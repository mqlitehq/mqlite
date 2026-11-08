//go:build mqlite_perf_diag

package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mq "github.com/mqlitehq/mqlite"
	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/server"
	"github.com/mqlitehq/mqlite/wire"
)

func TestQueuedNewWriteCoverageUsesSameAcknowledgedRequest(t *testing.T) {
	const caseID = "mixed_native_lock30"
	operation := caseID + "-mixed_new_send_b16-1"
	insert := providerHTTPEvent{ID: 20, BrokerRequestID: 7, StartedMS: 8, FinishedMS: 9, Finished: true, BodyEOF: true, HTTPStatus: 200, ErrorClass: "none", ProtocolSuccess: true, SQLTags: []string{"message_insert"}}
	send := brokerEvent{OperationID: operation, BrokerRequestID: 7, Method: "Send", Status: 200, StartedMS: .5, ServerReceived: true, ServerReceivedMS: 1, HeadersEndMS: 10}
	renewals := []observationInterval{{5, 6}}
	ops := []operationSample{{ID: operation, Phase: "mixed_new_send_b16"}}
	// Urgent Renew finishes before INSERT executes: zero SQL intersection is
	// expected, while the pending real new Send still constitutes competition.
	if mixedWriteCoverage([]providerHTTPEvent{insert}, 0, renewals) != 0 {
		t.Fatal("fixture must not overlap SQL execution")
	}
	if got := mixedQueuedWriteCoverage([]providerHTTPEvent{insert}, []brokerEvent{send}, 0, renewals, caseID, ops); len(got) != 1 || got[0].BrokerRequestID != 7 || got[0].InsertProviderEventID != 20 {
		t.Fatal("same pending acknowledged write was not proved")
	}
	for _, name := range []string{"dedup_read", "other_request_insert", "no_context", "unverified_request", "no_overlap", "failed_insert", "truncated_insert", "insert_outside_request", "wrong_operation", "unacknowledged_api", "failed_send"} {
		t.Run(name, func(t *testing.T) {
			e, b := insert, send
			rs := renewals
			operations := ops
			switch name {
			case "dedup_read":
				e.SQLTags = []string{"dedup_lookup"}
			case "other_request_insert":
				e.BrokerRequestID = 8
			case "no_context":
				e.BrokerRequestID = 0
			case "unverified_request":
				b.ServerReceived = false
			case "no_overlap":
				rs = []observationInterval{{11, 12}}
			case "failed_insert":
				e.ProtocolSuccess = false
			case "truncated_insert":
				e.ResponseInspectionTruncated = true
			case "insert_outside_request":
				e.FinishedMS = 11
			case "wrong_operation":
				b.OperationID = caseID + "-dedup_replay-1"
			case "unacknowledged_api":
				operations = nil
			case "failed_send":
				b.Status = 503
			}
			if len(mixedQueuedWriteCoverage([]providerHTTPEvent{e}, []brokerEvent{b}, 0, rs, caseID, operations)) != 0 {
				t.Fatal("unrelated or incomplete evidence accepted")
			}
		})
	}
}

func TestProviderCorrelationDoesNotTrustUnverifiedContextOrHeaders(t *testing.T) {
	for _, unverified := range []bool{false, true} {
		trace, err := newProviderTrace("https://synthetic.invalid", providerRoundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(strictExecuteEnvelope(strictStatementFixture)))}, nil
		}))
		if err != nil {
			t.Fatal(err)
		}
		req := providerTestRequest(t, "https://synthetic.invalid", strictExecuteRequest)
		req.Header.Set(diagnosticRequestHeader, "123")
		if unverified {
			req = req.WithContext(context.WithValue(req.Context(), brokerRequestContextKey{}, uint64(123)))
		}
		resp, err := trace.RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		event := trace.Snapshot().Events[0]
		if event.BrokerRequestID != 0 {
			t.Fatal("unverified correlation was guessed")
		}
	}
}

func TestLoopbackCorrelationRejectsUnregisteredAndMismatchedRequests(t *testing.T) {
	for _, bad := range []string{"unregistered", "operation", "path", "method"} {
		t.Run(bad, func(t *testing.T) {
			base := http.DefaultTransport.(*http.Transport).Clone()
			defer base.CloseIdleConnections()
			trace := &brokerTrace{origin: time.Now()}
			var called atomic.Int64
			broker := httptest.NewServer(trace.loopbackHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called.Add(1); w.WriteHeader(200) })))
			defer broker.Close()
			req, _ := http.NewRequest(http.MethodPost, broker.URL+wire.PathSend, strings.NewReader(`{}`))
			req = req.WithContext(context.WithValue(req.Context(), operationKey{}, "correlation-send-1"))
			var resp *http.Response
			var err error
			if bad == "unregistered" {
				req.Header.Set(diagnosticHeader, "correlation-send-1")
				req.Header.Set(diagnosticRequestHeader, "123")
				resp, err = base.RoundTrip(req)
			} else {
				trace.next = providerRoundTripFunc(func(r *http.Request) (*http.Response, error) {
					altered := r.Clone(r.Context())
					altered.Header = r.Header.Clone()
					switch bad {
					case "operation":
						altered.Header.Set(diagnosticHeader, "different-operation")
					case "path":
						copy := *r.URL
						copy.Path = wire.PathReceive
						altered.URL = &copy
					case "method":
						altered.Method = http.MethodGet
					}
					return base.RoundTrip(altered)
				})
				resp, err = trace.RoundTrip(req)
			}
			if err != nil {
				t.Fatal("loopback request failed")
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != 400 || called.Load() != 0 {
				t.Fatal("unverified loopback request reached provider-capable handler")
			}
		})
	}
}

// Real SDK -> HTTP broker -> Engine -> pinned HTTP driver -> real SQLite/Hrana.
// Concurrency demonstrates each provider INSERT retains its own verified ID.
func TestConcurrentLoopbackRequestsPropagateIDsThroughRealSQLProvider(t *testing.T) {
	for _, sdk := range []string{"native", "low-floor"} {
		t.Run(sdk, func(t *testing.T) {
			fixture := newSQLiteHranaFixture(t)
			original := http.DefaultTransport
			base, ok := original.(*http.Transport)
			if !ok {
				t.Fatal("unexpected transport")
			}
			provider, err := newProviderTrace(fixture.dsn, base)
			if err != nil {
				t.Fatal(err)
			}
			http.DefaultTransport = provider
			defer func() { http.DefaultTransport = original }()
			eng, err := engine.Open(context.Background(), engine.Options{DB: fixture.dsn, DisableBackground: true, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
			if err != nil {
				t.Fatal("engine open")
			}
			defer func() {
				if eng.Close() != nil {
					t.Error("engine close")
				}
				ctx, cancel := context.WithTimeout(context.Background(), joinBudget)
				defer cancel()
				if !waitProviderQuiet(ctx, provider.Snapshot, 200*time.Millisecond, 25*time.Millisecond).ObservedQuiet {
					t.Error("provider remained active")
				}
			}()
			transport := base.Clone()
			defer transport.CloseIdleConnections()
			trace := &brokerTrace{origin: provider.origin, next: transport}
			broker := httptest.NewServer(trace.loopbackHandler(server.New(eng, nil).Handler()))
			defer broker.Close()
			a, err := openAdapter(context.Background(), sdk, broker.URL, &http.Client{Transport: trace})
			if err != nil {
				t.Fatal("SDK open")
			}
			defer a.close()
			if err := a.createWithLock(context.Background(), "correlation", time.Hour, 30*time.Second); err != nil {
				t.Fatal("queue create")
			}
			var wg sync.WaitGroup
			errs := make(chan error, 4)
			for i := 0; i < 4; i++ {
				i := i
				wg.Add(1)
				go func() {
					defer wg.Done()
					ctx := context.WithValue(context.Background(), operationKey{}, fmt.Sprintf("correlation-send-%d", i))
					seqs, err := a.send(ctx, "correlation", []mq.OutMessage{inputFor(fmt.Sprintf("correlation/%d", i), 256)})
					if err == nil {
						err = checkACK(seqs, 1)
					}
					errs <- err
				}()
			}
			wg.Wait()
			for i := 0; i < 4; i++ {
				if <-errs != nil {
					t.Fatal("concurrent Send failed")
				}
			}
			requests, dropped := trace.snapshot()
			if dropped != 0 {
				t.Fatal("broker trace dropped")
			}
			sends := map[uint64]brokerEvent{}
			for _, request := range requests {
				if request.Method == "Send" {
					if request.BrokerRequestID == 0 || !request.ServerReceived || request.Status != 200 || request.ServerReceivedMS < request.StartedMS || request.HeadersEndMS < request.ServerReceivedMS {
						t.Fatal("missing verified broker timings")
					}
					sends[request.BrokerRequestID] = request
				}
			}
			if len(sends) != 4 {
				t.Fatal("HTTP request IDs are not unique")
			}
			matched := map[uint64]bool{}
			for _, event := range provider.Snapshot().Events {
				if !successfulMessageInsert(event, 0) {
					continue
				}
				request, ok := sends[event.BrokerRequestID]
				if !ok || event.StartedMS < request.ServerReceivedMS || event.FinishedMS > request.HeadersEndMS {
					t.Fatal("real provider INSERT did not inherit its own verified request context")
				}
				matched[event.BrokerRequestID] = true
			}
			if len(matched) != 4 {
				t.Fatal("missing real provider INSERT correlations")
			}
			// Reusing the retired ID must not be accepted by the broker wrapper.
			for id, request := range sends {
				replay, _ := http.NewRequest(http.MethodPost, broker.URL+wire.PathSend, strings.NewReader(`{}`))
				replay.Header.Set(diagnosticHeader, request.OperationID)
				replay.Header.Set(diagnosticRequestHeader, strconv.FormatUint(id, 10))
				response, err := transport.RoundTrip(replay)
				if err != nil {
					t.Fatal("retired ID probe failed")
				}
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
				if response.StatusCode != 400 {
					t.Fatal("retired request ID accepted")
				}
				break
			}
		})
	}
}
