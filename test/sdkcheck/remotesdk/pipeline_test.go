package remotesdk_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	broker "github.com/mqlitehq/mqlite"
	sdk "github.com/mqlitehq/mqlite-go"
	"github.com/mqlitehq/mqlite/server"
	"github.com/mqlitehq/mqlite/wire"
)

func TestSQLiteInFlightBrokerRestart(t *testing.T) {
	exerciseInFlightRestart(t, "file:"+filepath.Join(t.TempDir(), "restart.db"), "")
}

func TestTursoInFlightBrokerRestart(t *testing.T) {
	dsn := os.Getenv("MQLITE_TEST_DB")
	if dsn == "" {
		t.Skip("set MQLITE_TEST_DB for the dedicated restart test")
	}
	if dsn != "libsql://mqlite-verify-jiba.aws-ap-northeast-1.turso.io" || os.Getenv("MQLITE_TEST_DB_AUTH_TOKEN") == "" {
		t.Fatal("restart test requires the dedicated verification database and token")
	}
	exerciseInFlightRestart(t, dsn, os.Getenv("MQLITE_TEST_DB_AUTH_TOKEN"))
}

// Both adapters run the same lifecycle vector through the real Embedded.Serve
// drain path. Closing/reopening the engine keeps the database and HTTP address;
// the independent clients and their blocked handlers survive the whole restart.
func exerciseInFlightRestart(t *testing.T, dsn, databaseToken string) {
	for _, sdkName := range []string{"main", "remote-only"} {
		t.Run(sdkName, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			check := func(stage string, err error) {
				t.Helper()
				if err != nil {
					// Provider errors may contain endpoint/credential text. Log only
					// type and stable categories, never raw errors or the DSN.
					t.Fatalf("restart stage=%s type=%T canceled=%t deadline=%t", stage, err,
						errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded))
				}
			}
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			check("reserve address", err)
			addr := ln.Addr().String()
			check("release address", ln.Close())
			const token = "inflight-restart-local-test-token"
			queue := fmt.Sprintf("inflight-restart-%d-%d", os.Getpid(), time.Now().UnixNano())
			var current *broker.Embedded
			var serveCancel context.CancelFunc
			var serveDone chan error
			start := func() {
				var err error
				current, err = broker.OpenEmbedded(ctx, dsn, broker.WithBrokerMode(), broker.WithDBAuthToken(databaseToken),
					broker.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
				check("open broker", err)
				serveCtx, stop := context.WithCancel(ctx)
				serveCancel = stop
				ready := make(chan struct{})
				serveDone = make(chan error, 1)
				instance, done := current, serveDone
				go func() {
					done <- instance.Serve(serveCtx, addr, broker.WithTokens(token), broker.WithReady(func() { close(ready) }))
				}()
				select {
				case <-ready:
				case err := <-serveDone:
					serveDone = nil
					check("serve before ready", err)
					t.Fatal("Serve exited before ready")
				case <-ctx.Done():
					t.Fatal("restart broker readiness timed out")
				}
			}
			stop := func() {
				if serveDone != nil {
					serveCancel()
					select {
					case err := <-serveDone:
						serveDone = nil
						check("serve shutdown", err)
					case <-time.After(10 * time.Second):
						t.Fatal("restart Serve shutdown exceeded test budget")
					}
				}
				if current != nil {
					check("engine close", current.Close())
					current = nil
				}
			}
			defer stop()
			start()
			ledger := &restartTransport{base: http.DefaultTransport.(*http.Transport).Clone(), queue: queue, completed: make(map[int64]bool)}
			defer ledger.base.CloseIdleConnections()
			httpClient := &http.Client{Transport: ledger}
			var create func() error
			var send func(string) (int64, error)
			var run func(context.Context, func(context.Context, int64, int, []byte) error, func(error)) error
			var cleanup func(context.Context) error
			var closeClient func() error
			if sdkName == "main" {
				client, err := broker.Open(ctx, "http://"+addr, broker.WithToken(token), broker.WithHTTPClient(httpClient))
				check("main client open", err)
				closeClient = client.Close
				create = func() error {
					return client.CreateQueue(ctx, queue, broker.QueueConfig{LockDuration: 30 * time.Second, MaxDeliveryCount: 1})
				}
				send = func(body string) (int64, error) {
					return client.SendOne(ctx, queue, broker.OutMessage{Body: []byte(body)})
				}
				cleanup = func(c context.Context) error {
					_, err := client.DeleteQueueWithOptions(c, queue, broker.DeleteQueueOpts{Force: true})
					return err
				}
				run = func(c context.Context, handler func(context.Context, int64, int, []byte) error, notify func(error)) error {
					return client.Receiver(queue, broker.WithAutoRenew(), broker.WithConcurrency(3), broker.WithErrorHandler(notify)).Run(c,
						func(h context.Context, m *broker.Message) error {
							return handler(h, m.SequenceNumber, m.DeliveryCount, m.Body)
						})
				}
			} else {
				client, err := sdk.Open(ctx, "http://"+addr, sdk.WithToken(token), sdk.WithHTTPClient(httpClient))
				check("remote-only client open", err)
				closeClient = client.Close
				create = func() error {
					return client.CreateQueue(ctx, queue, sdk.QueueConfig{LockDuration: 30 * time.Second, MaxDeliveryCount: 1})
				}
				send = func(body string) (int64, error) {
					return client.SendOne(ctx, queue, sdk.OutMessage{Body: []byte(body)})
				}
				cleanup = func(c context.Context) error {
					_, err := client.DeleteQueueWithOptions(c, queue, sdk.DeleteQueueOpts{Force: true})
					return err
				}
				run = func(c context.Context, handler func(context.Context, int64, int, []byte) error, notify func(error)) error {
					return client.Receiver(queue, sdk.WithAutoRenew(), sdk.WithConcurrency(3), sdk.WithErrorHandler(notify)).Run(c,
						func(h context.Context, m *sdk.Message) error {
							return handler(h, m.SequenceNumber, m.DeliveryCount, m.Body)
						})
				}
			}
			defer func() { check("client close", closeClient()) }()
			check("create queue", create())
			defer func() {
				c, done := context.WithTimeout(context.Background(), 15*time.Second)
				defer done()
				check("own queue cleanup", cleanup(c))
			}()
			sent := make(map[int64]string)
			for _, body := range []string{"restart-work-0", "restart-work-1"} {
				seq, err := send(body)
				check("send", err)
				if seq <= 0 || sent[seq] != "" {
					t.Fatal("Send returned an invalid or duplicate sequence")
				}
				sent[seq] = body
			}
			var mu sync.Mutex
			seen := make(map[int64]int)
			invalid, lockLost, notifications := false, 0, 0
			entered, release := make(chan struct{}, 2), make(chan struct{})
			runCtx, cancelRun := context.WithCancel(ctx)
			runDone := make(chan error, 1)
			go func() {
				runDone <- run(runCtx, func(h context.Context, seq int64, delivery int, body []byte) error {
					mu.Lock()
					seen[seq]++
					want, exists := sent[seq]
					invalid = invalid || !exists || delivery != 1 || seen[seq] != 1 || string(body) != want
					mu.Unlock()
					select {
					case entered <- struct{}{}:
					default:
					}
					select {
					case <-release:
						return nil
					case <-h.Done():
						return h.Err()
					}
				}, func(err error) {
					mu.Lock()
					defer mu.Unlock()
					notifications++
					if errors.Is(err, broker.ErrLockLost) || errors.Is(err, sdk.ErrLockLost) {
						lockLost++
					}
				})
			}()
			joined := false
			defer func() {
				cancelRun()
				if !joined {
					select {
					case <-runDone:
					case <-time.After(10 * time.Second):
						t.Error("restart receiver cleanup exceeded budget")
					}
				}
			}()
			for i := 0; i < 2; i++ {
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal("handlers did not enter before restart")
				}
			}
			stop()
			// Deliberate, bounded broker unavailability; the client Run is unchanged.
			timer := time.NewTimer(500 * time.Millisecond)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				t.Fatal("restart deadline")
			}
			start()
			stats, err := current.Stats(ctx, queue)
			check("stats after reopen", err)
			if stats.Locked != 2 || stats.DeadLettered != 0 {
				t.Fatalf("reopen revoked live leases locked=%d dlq=%d", stats.Locked, stats.DeadLettered)
			}
			close(release)
			ticker := time.NewTicker(20 * time.Millisecond)
			defer ticker.Stop()
			for ledger.count() != 2 {
				select {
				case err := <-runDone:
					joined = true
					check("receiver exited during restart", err)
					t.Fatal("Receiver returned before both confirmations")
				case <-ctx.Done():
					t.Fatal("restart settlements did not confirm")
				case <-ticker.C:
				}
			}
			cancelRun()
			select {
			case err := <-runDone:
				joined = true
				if !errors.Is(err, context.Canceled) {
					check("receiver terminal result", err)
					t.Fatal("Receiver did not return caller cancellation")
				}
			case <-time.After(10 * time.Second):
				t.Fatal("restart Receiver did not join")
			}
			stats, err = current.Stats(ctx, queue)
			check("final stats", err)
			mu.Lock()
			defer mu.Unlock()
			if invalid || len(seen) != 2 || lockLost != 0 || stats.Total != 0 || stats.DeadLettered != 0 {
				t.Fatalf("restart ledger invalid=%t handled=%d complete=%d lock_lost=%d total=%d dlq=%d", invalid, len(seen), ledger.count(), lockLost, stats.Total, stats.DeadLettered)
			}
			for seq := range sent {
				if seen[seq] != 1 || !ledger.confirmed(seq) {
					t.Fatal("restart per-message confirmation mismatch")
				}
			}
			t.Logf("restart sdk=%s sent=2 handlers=2 confirmed_complete=2 lock_lost=0 notifications=%d final_total=0 dlq=0", sdkName, notifications)
		})
	}
}

// Count successful Complete responses received by the actual SDK, independently
// from handler returns and the final queue snapshot. No retries are added here.
type restartTransport struct {
	base      *http.Transport
	queue     string
	mu        sync.Mutex
	completed map[int64]bool
}

func (r *restartTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := r.base.RoundTrip(request)
	if err != nil || request.URL.Path != wire.PathComplete || response.StatusCode != http.StatusOK {
		return response, err
	}
	data, readErr := io.ReadAll(io.LimitReader(response.Body, 4096))
	_ = response.Body.Close()
	response.Body = io.NopCloser(bytes.NewReader(data))
	var input wire.SettleRequest
	if request.GetBody == nil {
		return response, nil
	}
	body, bodyErr := request.GetBody()
	if bodyErr != nil {
		return response, nil
	}
	decodeErr := json.NewDecoder(io.LimitReader(body, 4096)).Decode(&input)
	_ = body.Close()
	var result wire.SettleResponse
	if readErr == nil && decodeErr == nil && input.Queue == r.queue && input.SeqNumber > 0 && json.Unmarshal(data, &result) == nil && result.Ok {
		r.mu.Lock()
		r.completed[input.SeqNumber] = true
		r.mu.Unlock()
	}
	return response, nil
}

func (r *restartTransport) count() int { r.mu.Lock(); defer r.mu.Unlock(); return len(r.completed) }
func (r *restartTransport) confirmed(seq int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.completed[seq]
}

func TestTursoHTTPBrokerDelivery(t *testing.T) {
	dsn := os.Getenv("MQLITE_TEST_DB")
	if dsn == "" {
		t.Skip("set MQLITE_TEST_DB for the dedicated remote SDK pipeline test")
	}
	if dsn != "libsql://mqlite-verify-jiba.aws-ap-northeast-1.turso.io" {
		t.Fatal("remote SDK pipeline refused: not the dedicated verification database")
	}
	token := os.Getenv("MQLITE_TEST_DB_AUTH_TOKEN")
	if token == "" {
		t.Fatal("remote SDK pipeline requires the dedicated database token")
	}
	exerciseHTTPBrokerDelivery(t, "turso", dsn, token)
}

func TestSQLiteHTTPBrokerDelivery(t *testing.T) {
	exerciseHTTPBrokerDelivery(t, "sqlite", ":memory:", "")
}

// This deliberately follows the main SDK's HTTP-pipeline ledger, while every
// client operation and managed handler uses the independent remote-only SDK.
func exerciseHTTPBrokerDelivery(t *testing.T, backend, dsn, databaseToken string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	fail := func(stage string, err error) {
		t.Helper()
		if err == nil {
			return
		}
		// Never print provider errors: their text may contain secrets or URLs.
		status := 0
		var responseError *sdk.HTTPError
		if errors.As(err, &responseError) && responseError != nil {
			status = responseError.StatusCode
		}
		t.Fatalf("remote SDK pipeline stage=%s failed type=%T canceled=%t deadline=%t outcome_unknown=%t lock_lost=%t permission_denied=%t not_found=%t http_status=%d", stage, err,
			errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded), errors.Is(err, sdk.ErrOutcomeUnknown),
			errors.Is(err, sdk.ErrLockLost), errors.Is(err, sdk.ErrPermissionDenied), errors.Is(err, sdk.ErrNotFound), status)
	}
	eng, err := broker.OpenEmbedded(ctx, dsn, broker.WithDBAuthToken(databaseToken), broker.WithBrokerMode(),
		broker.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	fail("broker open", err)
	defer func() {
		if err := eng.Close(); err != nil {
			t.Error("remote SDK pipeline engine close failed")
		}
	}()
	const total = 24
	const brokerToken = "remote-sdk-pipeline-local-test-token"
	queue := fmt.Sprintf("remote-sdk-pipeline-%d-%d", os.Getpid(), time.Now().UnixNano())
	var mu sync.Mutex
	completed := make(map[int64]bool, total)
	var renewals, notifications int
	allCompleted := make(chan struct{})
	base := server.New(eng.Engine(), []string{brokerToken}).Handler()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Queue string `json:"queue"`
			Seq   int64  `json:"seq_number"`
		}
		if r.URL.Path == wire.PathComplete {
			data, readErr := io.ReadAll(r.Body)
			_ = r.Body.Close()
			if readErr != nil || json.Unmarshal(data, &request) != nil {
				http.Error(w, "invalid test completion request", http.StatusBadRequest)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(data))
		}
		recorder := httptest.NewRecorder()
		base.ServeHTTP(recorder, r)
		for name, values := range recorder.Header() {
			w.Header()[name] = values
		}
		w.WriteHeader(recorder.Code)
		written, writeErr := w.Write(recorder.Body.Bytes())
		if recorder.Code != http.StatusOK || writeErr != nil || written != recorder.Body.Len() {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case wire.PathComplete:
			var response wire.SettleResponse
			if json.Unmarshal(recorder.Body.Bytes(), &response) == nil && response.Ok && request.Queue == queue && !completed[request.Seq] {
				completed[request.Seq] = true
				if len(completed) == total {
					close(allCompleted)
				}
			}
		case wire.PathRenewBatch:
			var response wire.RenewBatchResponse
			if json.Unmarshal(recorder.Body.Bytes(), &response) == nil {
				for _, result := range response.Results {
					if result.Ok && result.LockedUntilMs > 0 {
						renewals++
					}
				}
			}
		}
	}))
	defer ts.Close()
	client, err := sdk.Open(ctx, ts.URL, sdk.WithToken(brokerToken))
	fail("client open", err)
	defer client.Close()
	fail("create own queue", client.CreateQueue(ctx, queue, sdk.QueueConfig{LockDuration: 30 * time.Second, MaxDeliveryCount: 1}))
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		if _, err := client.DeleteQueueWithOptions(cleanup, queue, sdk.DeleteQueueOpts{Force: true}); err != nil {
			t.Error("remote SDK pipeline own-queue cleanup failed")
		}
	}()
	type expected struct {
		seq  int64
		body []byte
	}
	sent := make(map[string]expected, total)
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("binary-%02d", i)
		body := []byte{byte(i), 0, 255, 128, byte(i ^ 127)}
		seq, err := client.SendOne(ctx, queue, sdk.OutMessage{MessageID: id, Body: body})
		fail("send", err)
		if seq <= 0 {
			t.Fatal("remote SDK pipeline Send returned an invalid sequence")
		}
		sent[id] = expected{seq: seq, body: body}
	}
	runCtx, stopReceiver := context.WithCancel(ctx)
	seen := make(map[string]int, total)
	invalidDelivery := false
	runDone := make(chan error, 1)
	go func() {
		runDone <- client.Receiver(queue, sdk.WithConcurrency(4), sdk.WithAutoRenew(),
			sdk.WithErrorHandler(func(error) { mu.Lock(); notifications++; mu.Unlock() })).
			Run(runCtx, func(handlerCtx context.Context, message *sdk.Message) error {
				want, exists := sent[message.MessageID]
				mu.Lock()
				seen[message.MessageID]++
				valid := exists && message.SequenceNumber == want.seq && bytes.Equal(message.Body, want.body) &&
					message.DeliveryCount == 1 && seen[message.MessageID] == 1
				invalidDelivery = invalidDelivery || !valid
				mu.Unlock()
				if !valid {
					return errors.New("remote SDK pipeline delivery validation failed")
				}
				if message.MessageID == "binary-00" {
					// Observe a real renewal at the 30s lease's first 10s point.
					timer := time.NewTimer(11 * time.Second)
					defer timer.Stop()
					select {
					case <-timer.C:
					case <-handlerCtx.Done():
						return handlerCtx.Err()
					}
				}
				return nil
			})
	}()
	joined := false
	defer func() {
		stopReceiver()
		if !joined {
			select {
			case <-runDone:
			case <-time.After(10 * time.Second):
				t.Error("remote SDK pipeline Receiver did not stop within cleanup budget")
			}
		}
	}()
	select {
	case <-allCompleted:
	case err := <-runDone:
		joined = true
		fail("Receiver stopped before all completions", err)
		t.Fatal("remote SDK pipeline Receiver ended before all completions")
	case <-ctx.Done():
		t.Fatal("remote SDK pipeline deadline reached before all completions")
	}
	stopReceiver()
	select {
	case err = <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("remote SDK pipeline Receiver exceeded its shutdown budget")
	}
	joined = true
	if !errors.Is(err, context.Canceled) {
		fail("Receiver terminal result", err)
		t.Fatal("remote SDK pipeline Receiver did not report cancellation")
	}
	stats, err := client.Stats(ctx, queue)
	fail("final stats", err)
	mu.Lock()
	defer mu.Unlock()
	if invalidDelivery || len(seen) != total || len(completed) != total || renewals == 0 ||
		stats.Total != 0 || stats.Active != 0 || stats.Locked != 0 || stats.DeadLettered != 0 {
		t.Fatalf("remote SDK pipeline accounting failed invalid=%t handled=%d complete=%d renewals=%d total=%d locked=%d dlq=%d",
			invalidDelivery, len(seen), len(completed), renewals, stats.Total, stats.Locked, stats.DeadLettered)
	}
	for id, want := range sent {
		if seen[id] != 1 || !completed[want.seq] {
			t.Fatal("remote SDK pipeline per-message handler/Complete accounting failed")
		}
	}
	t.Logf("remote SDK pipeline backend=%s sent=%d handled=%d confirmed_complete=%d confirmed_renewals=%d notifications=%d final_total=%d dlq=%d",
		backend, len(sent), len(seen), len(completed), renewals, notifications, stats.Total, stats.DeadLettered)
}
