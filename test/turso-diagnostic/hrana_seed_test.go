//go:build mqlite_perf_diag

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
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/mqlitehq/mqlite"
	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/server"
)

type hranaSeedEvent struct {
	Phase    string   `json:"phase"`
	Sequence int64    `json:"sequence,omitempty"`
	AtMS     float64  `json:"at_ms"`
	Failure  *failure `json:"failure,omitempty"`
}
type hranaSeedRPC struct {
	Method                     string    `json:"method"`
	StartMS                    float64   `json:"start_ms"`
	HeadersMS                  float64   `json:"headers_ms"`
	Status                     int       `json:"status"`
	Failure                    *failure  `json:"failure,omitempty"`
	ServerClockDeltaAtHeaderMS int64     `json:"server_clock_delta_at_header_ms"`
	ServerTimeHeaderObserved   bool      `json:"server_time_header_observed"`
	Sequences                  []int64   `json:"sequences,omitempty"`
	LockedRemainingAtEOFMS     []float64 `json:"locked_remaining_at_eof_ms,omitempty"`
}
type hranaSeedTrace struct {
	origin time.Time
	next   http.RoundTripper
	mu     sync.Mutex
	events []hranaSeedEvent
	rpc    []hranaSeedRPC
}

func (t *hranaSeedTrace) add(phase string, seq int64, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = append(t.events, hranaSeedEvent{phase, seq, providerMillis(time.Since(t.origin)), safeFailure(err)})
}
func (t *hranaSeedTrace) RoundTrip(r *http.Request) (*http.Response, error) {
	start := providerMillis(time.Since(t.origin))
	resp, err := t.next.RoundTrip(r)
	status := 0
	clockDelta := int64(0)
	serverHeaderObserved := false
	if resp != nil {
		status = resp.StatusCode
		if serverMS, e := strconv.ParseInt(resp.Header.Get("X-Mqlite-Server-Time-Ms"), 10, 64); e == nil {
			clockDelta = time.Now().UnixMilli() - serverMS
			serverHeaderObserved = true
		}
	}
	method := brokerMethod(r.URL.Path)
	var sequences []int64
	if (method == "RenewBatch" || method == "Complete" || method == "CompleteBatch") && r.GetBody != nil {
		if copy, err := r.GetBody(); err == nil {
			var request struct {
				Sequence int64 `json:"seq_number"`
				Messages []struct {
					Sequence int64 `json:"seq_number"`
				} `json:"messages"`
			}
			if json.NewDecoder(io.LimitReader(copy, 64<<10)).Decode(&request) == nil {
				if request.Sequence > 0 {
					sequences = append(sequences, request.Sequence)
				}
				for _, m := range request.Messages {
					sequences = append(sequences, m.Sequence)
				}
			}
			_ = copy.Close()
		}
	}
	t.mu.Lock()
	index := len(t.rpc)
	t.rpc = append(t.rpc, hranaSeedRPC{Method: method, StartMS: start, HeadersMS: providerMillis(time.Since(t.origin)), Status: status, Failure: safeFailure(err), ServerClockDeltaAtHeaderMS: clockDelta, ServerTimeHeaderObserved: serverHeaderObserved, Sequences: sequences})
	t.mu.Unlock()
	if method == "Receive" && resp != nil && resp.Body != nil && status == 200 {
		resp.Body = &observedSDKBody{ReadCloser: resp.Body, done: func(data []byte) {
			var response struct {
				Messages []struct {
					LockedUntilMS int64 `json:"locked_until_ms"`
				} `json:"messages"`
			}
			if json.Unmarshal(data, &response) != nil {
				return
			}
			remaining := make([]float64, len(response.Messages))
			now := time.Now()
			for i, m := range response.Messages {
				remaining[i] = float64(time.UnixMilli(m.LockedUntilMS).Sub(now)) / float64(time.Millisecond)
			}
			t.mu.Lock()
			t.rpc[index].LockedRemainingAtEOFMS = remaining
			t.mu.Unlock()
		}}
	}
	return resp, err
}

// This intentionally requires healthy four-way managed handling of a valid 3s
// queue over real SQL/Hrana. It is a regression probe, not a latency simulation
// inside the SDK. Provider latency is the same for all provider RPCs.
func TestHranaSeedOnlyThreeSecondLeaseOverProviderLatency(t *testing.T) {
	for _, sdk := range []string{"native", "low-floor"} {
		t.Run(sdk, func(t *testing.T) {
			f := newSQLiteHranaFixture(t)
			original := http.DefaultTransport
			base, ok := original.(*http.Transport)
			if !ok {
				t.Fatal("unexpected default transport")
			}
			provider, err := newProviderTrace(f.dsn, base)
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
			provider.SetPhase("seed3s/" + sdk + "/receive")
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
			}{sdk, 200, started.Load(), finished.Load(), lost.Load(), errorsSeen.Load(), joined, safeFailure(early), events, rpc, provider.Snapshot(), stats, safeFailure(statsErr), safeFailure(closeErr), quiet})
			if err != nil {
				t.Fatal("safe trace encoding")
			}
			t.Log(string(encoded))
			if !joined || !errors.Is(early, context.Canceled) || started.Load() != 4 || finished.Load() != 4 || lost.Load() != 0 || errorsSeen.Load() != 0 || statsErr != nil || stats.Total != 0 || closeErr != nil || !quiet.ObservedQuiet {
				t.Fatal("valid 3s managed leases failed before all four seed handlers completed and settled")
			}
		})
	}
}
