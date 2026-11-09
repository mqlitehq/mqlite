//go:build mqlite_perf_diag && mqlite_latency_repro

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/mqlitehq/mqlite"
	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/server"
)

// Add latency only to the pinned engine's one-statement renew UPDATE. The
// original real SQLite/Hrana fixture remains unchanged at 200ms per HTTP call.
// This is a synthetic latency vector, not a claim about its network component.
type renewDelayVectorTransport struct {
	next  http.RoundTripper
	extra time.Duration
	calls atomic.Int64
}

func (t *renewDelayVectorTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if requestContainsRenewUpdate(r) {
		t.calls.Add(1)
		timer := time.NewTimer(t.extra)
		defer timer.Stop()
		select {
		case <-r.Context().Done():
			if r.Body != nil {
				_ = r.Body.Close()
			}
			return nil, r.Context().Err()
		case <-timer.C:
		}
	}
	return t.next.RoundTrip(r)
}
func requestContainsRenewUpdate(r *http.Request) bool {
	if r.GetBody == nil {
		return false
	}
	copy, err := r.GetBody()
	if err != nil {
		return false
	}
	defer copy.Close()
	var pipeline struct {
		Requests []sqliteHranaRequest `json:"requests"`
	}
	if json.NewDecoder(io.LimitReader(copy, 1<<20)).Decode(&pipeline) != nil {
		return false
	}
	renew := func(sql string) bool {
		return strings.HasPrefix(strings.ToUpper(strings.Join(strings.Fields(sql), " ")), "UPDATE MESSAGES SET LOCKED_UNTIL=MAX(")
	}
	for _, request := range pipeline.Requests {
		if request.Type == "execute" && renew(request.Stmt.SQL) {
			return true
		}
		if request.Type == "batch" {
			for _, step := range request.Batch.Steps {
				if renew(step.Stmt.SQL) {
					return true
				}
			}
		}
	}
	return false
}
func TestRenewDelayVectorClassifiesOnlyRenewSQL(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		want bool
	}{{"UPDATE messages SET locked_until=MAX(locked_until, ?) WHERE id=? RETURNING id", true}, {"UPDATE messages SET state='locked',locked_until=? WHERE id=? RETURNING id", false}, {"DELETE FROM messages WHERE id=?", false}, {"SELECT 1", false}} {
		body, _ := json.Marshal(map[string]any{"requests": []any{map[string]any{"type": "execute", "stmt": map[string]any{"sql": tc.sql}}}})
		request, _ := http.NewRequest(http.MethodPost, "http://synthetic.invalid/v2/pipeline", strings.NewReader(string(body)))
		if requestContainsRenewUpdate(request) != tc.want {
			t.Fatal("latency vector changed a non-renewal request")
		}
		original, err := io.ReadAll(request.Body)
		if err != nil || string(original) != string(body) {
			t.Fatal("latency classification consumed the driver request")
		}
	}
}

func TestRenewDelayVectorClosesCanceledRequestBody(t *testing.T) {
	request, err := http.NewRequest(http.MethodPost, "http://synthetic.invalid/v2/pipeline", strings.NewReader(`{"requests":[{"type":"execute","stmt":{"sql":"UPDATE messages SET locked_until=MAX(locked_until, ?) WHERE id=? RETURNING id"}}]}`))
	if err != nil {
		t.Fatal("construct synthetic request")
	}
	body := &providerErrorBody{}
	request.Body = body
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request = request.WithContext(ctx)
	reached := false
	vector := renewDelayVectorTransport{extra: time.Second, next: providerRoundTripFunc(func(*http.Request) (*http.Response, error) {
		reached = true
		return nil, errors.New("unexpected fixture call")
	})}
	_, err = vector.RoundTrip(request)
	if !errors.Is(err, context.Canceled) || reached || body.closeCalls != 1 {
		t.Fatal("canceled latency wrapper did not close its request body before returning")
	}
}

func TestHranaSeedRenewDelayVectorThreeSecondLease(t *testing.T) {
	for _, sdk := range []string{"native", "low-floor"} {
		t.Run(sdk, func(t *testing.T) {
			f := newSQLiteHranaFixture(t)
			original := http.DefaultTransport
			base, ok := original.(*http.Transport)
			if !ok {
				t.Fatal("unexpected default transport")
			}
			vector := &renewDelayVectorTransport{next: base, extra: 100 * time.Millisecond}
			provider, err := newProviderTrace(f.dsn, vector)
			if err != nil {
				t.Fatal("provider trace setup")
			}
			http.DefaultTransport = provider
			defer func() { http.DefaultTransport = original }()
			eng, err := engine.Open(context.Background(), engine.Options{DB: f.dsn, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
			if err != nil {
				t.Fatal("engine open")
			}
			engineClosed := false
			defer func() {
				if engineClosed {
					return
				}
				if err := eng.Close(); err != nil {
					t.Error("engine close failed")
				}
				ctx, cancel := context.WithTimeout(context.Background(), joinBudget)
				defer cancel()
				if !waitProviderQuiet(ctx, provider.Snapshot, 200*time.Millisecond, 25*time.Millisecond).ObservedQuiet {
					t.Error("provider did not become quiet")
				}
			}()
			broker := httptest.NewServer(server.New(eng, nil).Handler())
			defer broker.Close()
			transport := base.Clone()
			defer transport.CloseIdleConnections()
			trace := &hranaSeedTrace{origin: provider.origin, next: transport}
			client, err := openAdapter(context.Background(), sdk, broker.URL, &http.Client{Transport: trace})
			if err != nil {
				t.Fatal("SDK open")
			}
			defer client.close()
			const queue = "seed_only"
			if err := client.createWithLock(context.Background(), queue, time.Hour, 3*time.Second); err != nil {
				t.Fatal("create queue")
			}
			messages := make([]mq.OutMessage, 4)
			for i := range messages {
				messages[i] = inputFor(fmt.Sprintf("seed/%d", i), 256)
			}
			ack, err := client.send(context.Background(), queue, messages)
			if err != nil || checkACK(ack, 4) != nil {
				t.Fatal("seed send")
			}
			f.setDelay(200 * time.Millisecond)
			provider.SetPhase("seed3s-renew300/" + sdk + "/receive")
			trace.add("receiver_start", 0, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
			defer cancel()
			var stopRequested atomic.Bool
			var started, finished, lost, errorsSeen atomic.Int64
			failureSeen := make(chan struct{}, 1)
			finishedAll := make(chan struct{})
			runDone := make(chan error, 1)
			go func() {
				runDone <- client.runReceiver(ctx, queue, func(call context.Context, m *delivery) error {
					started.Add(1)
					trace.add("handler_start", m.Sequence, nil)
					timer := time.NewTimer(6 * time.Second)
					defer timer.Stop()
					select {
					case <-call.Done():
						if !stopRequested.Load() {
							lost.Add(1)
							trace.add("handler_context_lost", m.Sequence, call.Err())
						} else {
							trace.add("handler_stopped_by_test", m.Sequence, nil)
						}
						return call.Err()
					case <-timer.C:
						trace.add("handler_success", m.Sequence, nil)
						if finished.Add(1) == 4 {
							close(finishedAll)
						}
						return nil
					}
				}, func(err error) {
					errorsSeen.Add(1)
					trace.add("receiver_error", 0, err)
					select {
					case failureSeen <- struct{}{}:
					default:
					}
				})
			}()
			var early error
			select {
			case <-failureSeen:
			case <-finishedAll:
			case early = <-runDone:
			case <-ctx.Done():
				early = ctx.Err()
			}
			// On a healthy run, allow the real Receiver to finish its own settlement.
			if finished.Load() == 4 && errorsSeen.Load() == 0 {
				until := time.Now().Add(15 * time.Second)
				for time.Now().Before(until) {
					s, e := client.stats(ctx, queue)
					if e == nil && s.Total == 0 {
						break
					}
					select {
					case <-failureSeen:
						until = time.Now()
					case <-time.After(100 * time.Millisecond):
					}
				}
			}
			// Stop after the first SDK error to isolate this probe; this planned
			// cancellation is distinct from a handler losing its own lease.
			trace.add("test_requests_stop", 0, nil)
			stopRequested.Store(true)
			cancel()
			joined := false
			if early == nil {
				select {
				case early = <-runDone:
					joined = true
				case <-time.After(joinBudget):
				}
			} else {
				joined = true
			}
			f.setDelay(0)
			stats, statsErr := eng.Stats(context.Background(), queue)
			broker.Close()
			closeErr := eng.Close()
			engineClosed = true
			quietCtx, quietCancel := context.WithTimeout(context.Background(), joinBudget)
			quiet := waitProviderQuiet(quietCtx, provider.Snapshot, 200*time.Millisecond, 25*time.Millisecond)
			quietCancel()
			trace.mu.Lock()
			events := append([]hranaSeedEvent(nil), trace.events...)
			rpc := append([]hranaSeedRPC(nil), trace.rpc...)
			trace.mu.Unlock()
			encoded, err := json.Marshal(struct {
				SDK                             string `json:"sdk"`
				DelayMS                         int    `json:"provider_delay_ms"`
				RenewExtraMS                    int    `json:"renew_update_extra_ms"`
				RenewExtraCalls                 int64  `json:"renew_extra_calls"`
				Started, Finished, Lost, Errors int64
				Joined                          bool
				RunFailure                      *failure              `json:"run_failure"`
				Events                          []hranaSeedEvent      `json:"events"`
				RPC                             []hranaSeedRPC        `json:"broker_rpc"`
				Provider                        providerTraceSnapshot `json:"provider"`
				Stats                           engine.Metrics        `json:"stats"`
				StatsFailure                    *failure              `json:"stats_failure"`
				CloseFailure                    *failure              `json:"close_failure"`
				Quiet                           quietResult           `json:"provider_quiet"`
			}{sdk, 200, 100, vector.calls.Load(), started.Load(), finished.Load(), lost.Load(), errorsSeen.Load(), joined, safeFailure(early), events, rpc, renewVectorSnapshot(provider), stats, safeFailure(statsErr), safeFailure(closeErr), quiet})
			if err != nil {
				t.Fatal("safe trace encoding")
			}
			t.Log(string(encoded))
			if !joined || !errors.Is(early, context.Canceled) || started.Load() != 4 || finished.Load() != 4 || lost.Load() != 0 || errorsSeen.Load() != 0 || vector.calls.Load() == 0 || statsErr != nil || stats.Total != 0 || closeErr != nil || !quiet.ObservedQuiet {
				t.Fatal("valid 3s managed leases failed before all four seed handlers completed and settled")
			}
		})
	}
}

// Keep all mixed work, message identity checks and timing/budget settings. Queued
// real new Sends preserve the contended admission period seen in the remote run.
func TestHranaMixed68RenewDelayVectorThreeSecondLease(t *testing.T) {
	for index, sdk := range []string{"native", "low-floor"} {
		t.Run(sdk, func(t *testing.T) {
			fixture := newSQLiteHranaFixture(t)
			fixture.setDelay(200 * time.Millisecond)
			original := http.DefaultTransport
			base, ok := original.(*http.Transport)
			if !ok {
				t.Fatal("unexpected default transport")
			}
			vector := &renewDelayVectorTransport{next: base, extra: 100 * time.Millisecond}
			provider, err := newProviderTrace(fixture.dsn, vector)
			if err != nil {
				t.Fatal("fixture provider trace")
			}
			http.DefaultTransport = provider
			defer func() { http.DefaultTransport = original }()
			var plan casePlan
			for _, candidate := range plans("mixed") {
				if candidate.SDK == sdk && candidate.LockSeconds == 3 {
					plan = candidate
				}
			}
			if plan.Count != 68 || plan.Batch != 16 || plan.Concurrency != 4 || !plan.Background {
				t.Fatal("mixed profile changed")
			}
			queue := queueName("fixture_mixed68", index, sdk)
			owner := backend{Alias: "fixture", dsn: fixture.dsn}
			owned := []ownedQueue{{Backend: owner.Alias, Case: plan.ID, Queue: queue}}
			ctx, cancel := context.WithTimeout(context.Background(), caseBudget)
			defer cancel()
			result := runCase(ctx, owner, plan, queue, provider, &caseProgress{book: newBook()}, base, owned, index == 0)
			result.Provider.BodyTimingMeaning = renewVectorSnapshot(provider).BodyTimingMeaning
			encoded, err := json.Marshal(struct {
				FixtureOnly     bool       `json:"fixture_only"`
				OrdinaryMS      int        `json:"ordinary_provider_ms"`
				RenewExtraMS    int        `json:"renew_update_extra_ms"`
				RenewExtraCalls int64      `json:"renew_extra_calls"`
				Result          caseResult `json:"case"`
			}{true, 200, 100, vector.calls.Load(), result})
			if err != nil {
				t.Fatal("safe fixture result encode")
			}
			t.Log(string(encoded))
			if !result.Passed || result.Blocked || !result.OwnerReleased || vector.calls.Load() == 0 || !result.Workload.valid(68, result.FinalStats) {
				t.Fatal("unchanged mixed68 vector did not meet the complete reliability oracle")
			}
		})
	}
}

// This vector is below providerTrace rather than its optional synthetic-delay
// hook: its HTTP/TTFB include the wrapper delay and server fixture delay.
func renewVectorSnapshot(provider *providerTrace) providerTraceSnapshot {
	snapshot := provider.Snapshot()
	snapshot.BodyTimingMeaning = "Body time includes client read scheduling and ends at EOF, read error or Close; HTTP/TTFB include this fixture's synthetic latency: 200ms server delay and 100ms extra before renewal transport. A canceled request may never reach the server."
	return snapshot
}
