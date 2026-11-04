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
	exerciseInFlightRestart(t, "file:"+filepath.Join(t.TempDir(), "restart.db"), "", 30*time.Second)
}

func TestTursoInFlightBrokerRestart(t *testing.T) {
	dsn := os.Getenv("MQLITE_TEST_DB")
	if dsn == "" {
		t.Skip("set MQLITE_TEST_DB for the dedicated restart test")
	}
	if dsn != "libsql://mqlite-verify-jiba.aws-ap-northeast-1.turso.io" || os.Getenv("MQLITE_TEST_DB_AUTH_TOKEN") == "" {
		t.Fatal("restart test requires the dedicated verification database and token")
	}
	// A measured remote reopen took 35.375s, exceeding the default 30s lease.
	// Keep that expiry result as a separate boundary observation; this vector
	// specifically tests preservation while a lease is still demonstrably live.
	exerciseInFlightRestart(t, dsn, os.Getenv("MQLITE_TEST_DB_AUTH_TOKEN"), 2*time.Minute)
}

// Both adapters run the same lifecycle vector through the real Embedded.Serve
// drain path. Closing/reopening the engine keeps the database and HTTP address;
// the independent clients and their blocked handlers survive the whole restart.
func exerciseInFlightRestart(t *testing.T, dsn, databaseToken string, lockDuration time.Duration) {
	for _, sdkName := range []string{"main", "remote-only"} {
		t.Run(sdkName, func(t *testing.T) {
			started := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			phase := func(stage string) {
				deadline, _ := ctx.Deadline()
				t.Logf("restart phase=%s elapsed_ms=%d remaining_budget_ms=%d context_expired=%t", stage,
					time.Since(started).Milliseconds(), time.Until(deadline).Milliseconds(), ctx.Err() != nil)
			}
			phase("case_begin")
			defer phase("case_end_after_cleanup")
			check := func(stage string, err error) {
				t.Helper()
				if err != nil {
					phase("failure_" + stage)
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
				phase("engine_open_begin")
				current, err = broker.OpenEmbedded(ctx, dsn, broker.WithBrokerMode(), broker.WithDBAuthToken(databaseToken),
					broker.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
				check("open broker", err)
				phase("engine_open_done")
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
					phase("serve_ready")
				case err := <-serveDone:
					serveDone = nil
					check("serve before ready", err)
					t.Fatal("Serve exited before ready")
				case <-ctx.Done():
					phase("serve_readiness_deadline")
					t.Fatal("restart broker readiness timed out")
				}
			}
			stop := func() {
				if serveDone != nil {
					phase("serve_stop_begin")
					serveCancel()
					select {
					case err := <-serveDone:
						serveDone = nil
						check("serve shutdown", err)
						phase("serve_stop_done")
					case <-time.After(10 * time.Second):
						phase("serve_stop_deadline")
						t.Fatal("restart Serve shutdown exceeded test budget")
					}
				}
				if current != nil {
					phase("engine_close_begin")
					check("engine close", current.Close())
					phase("engine_close_done")
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
			var run func(context.Context, func(context.Context, int64, int, []byte, time.Time) error, func(error)) error
			var cleanup func(context.Context) error
			var closeClient func() error
			if sdkName == "main" {
				client, err := broker.Open(ctx, "http://"+addr, broker.WithToken(token), broker.WithHTTPClient(httpClient))
				check("main client open", err)
				closeClient = client.Close
				create = func() error {
					return client.CreateQueue(ctx, queue, broker.QueueConfig{LockDuration: lockDuration, MaxDeliveryCount: 1})
				}
				send = func(body string) (int64, error) {
					return client.SendOne(ctx, queue, broker.OutMessage{Body: []byte(body)})
				}
				cleanup = func(c context.Context) error {
					_, err := client.DeleteQueueWithOptions(c, queue, broker.DeleteQueueOpts{Force: true})
					return err
				}
				run = func(c context.Context, handler func(context.Context, int64, int, []byte, time.Time) error, notify func(error)) error {
					return client.Receiver(queue, broker.WithAutoRenew(), broker.WithConcurrency(3), broker.WithErrorHandler(notify)).Run(c,
						func(h context.Context, m *broker.Message) error {
							return handler(h, m.SequenceNumber, m.DeliveryCount, m.Body, m.LockedUntil)
						})
				}
			} else {
				client, err := sdk.Open(ctx, "http://"+addr, sdk.WithToken(token), sdk.WithHTTPClient(httpClient))
				check("remote-only client open", err)
				closeClient = client.Close
				create = func() error {
					return client.CreateQueue(ctx, queue, sdk.QueueConfig{LockDuration: lockDuration, MaxDeliveryCount: 1})
				}
				send = func(body string) (int64, error) {
					return client.SendOne(ctx, queue, sdk.OutMessage{Body: []byte(body)})
				}
				cleanup = func(c context.Context) error {
					_, err := client.DeleteQueueWithOptions(c, queue, sdk.DeleteQueueOpts{Force: true})
					return err
				}
				run = func(c context.Context, handler func(context.Context, int64, int, []byte, time.Time) error, notify func(error)) error {
					return client.Receiver(queue, sdk.WithAutoRenew(), sdk.WithConcurrency(3), sdk.WithErrorHandler(notify)).Run(c,
						func(h context.Context, m *sdk.Message) error {
							return handler(h, m.SequenceNumber, m.DeliveryCount, m.Body, m.LockedUntil)
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
			initialLeases := make(map[int64]int64)
			initialClock := current.Engine()
			liveInitialLeases := func(stage string, serverNow int64) bool {
				mu.Lock()
				defer mu.Unlock()
				live := len(initialLeases) == len(sent)
				for seq, deadline := range initialLeases {
					remaining := deadline - serverNow
					t.Logf("restart lease_stage=%s seq=%d initial_lease_deadline_ms=%d server_time_ms=%d known_remaining_ms=%d elapsed_ms=%d",
						stage, seq, deadline, serverNow, remaining, time.Since(started).Milliseconds())
					live = live && remaining > 0
				}
				return live
			}
			invalid, lockLost, notifications := false, 0, 0
			entered, release := make(chan struct{}, 2), make(chan struct{})
			runCtx, cancelRun := context.WithCancel(ctx)
			runDone := make(chan error, 1)
			go func() {
				runDone <- run(runCtx, func(h context.Context, seq int64, delivery int, body []byte, lockedUntil time.Time) error {
					mu.Lock()
					seen[seq]++
					if _, exists := initialLeases[seq]; !exists {
						initialLeases[seq] = lockedUntil.UnixMilli()
					}
					initialDeadline := initialLeases[seq]
					want, exists := sent[seq]
					invalid = invalid || !exists || delivery != 1 || seen[seq] != 1 || string(body) != want
					mu.Unlock()
					serverNow := initialClock.ServerTimeMs()
					t.Logf("restart handler_entered seq=%d initial_lease_deadline_ms=%d delivery_lease_deadline_ms=%d server_time_ms=%d known_remaining_ms=%d elapsed_ms=%d",
						seq, initialDeadline, lockedUntil.UnixMilli(), serverNow, initialDeadline-serverNow, time.Since(started).Milliseconds())
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
					notifications++
					count := notifications
					if errors.Is(err, broker.ErrLockLost) || errors.Is(err, sdk.ErrLockLost) {
						lockLost++
					}
					mu.Unlock()
					t.Logf("restart notification number=%d elapsed_ms=%d diagnostic=%+v", count, time.Since(started).Milliseconds(), pipelineErrorDiagnostic(err))
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
			restartBegan := time.Now()
			phase("restart_begin_before_stop")
			liveInitialLeases("restart_begin", current.Engine().ServerTimeMs())
			stop()
			phase("restart_stop_done")
			// Deliberate, bounded broker unavailability; the client Run is unchanged.
			timer := time.NewTimer(500 * time.Millisecond)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				phase("restart_pause_deadline")
				t.Fatal("restart deadline")
			}
			phase("restart_fixed_pause_done")
			start()
			actualDowntime := time.Since(restartBegan)
			t.Logf("restart actual_downtime_ms=%d configured_pause_ms=500 configured_lock_ms=%d elapsed_ms=%d",
				actualDowntime.Milliseconds(), lockDuration.Milliseconds(), time.Since(started).Milliseconds())
			liveAtReady := liveInitialLeases("restart_ready", current.Engine().ServerTimeMs())
			stats, err := current.Stats(ctx, queue)
			check("stats after reopen", err)
			t.Logf("restart reopened_stats locked=%d dlq=%d total=%d actual_downtime_ms=%d elapsed_ms=%d",
				stats.Locked, stats.DeadLettered, stats.Total, actualDowntime.Milliseconds(), time.Since(started).Milliseconds())
			liveAfterStats := liveInitialLeases("reopen_stats_done", current.Engine().ServerTimeMs())
			if !liveAtReady || !liveAfterStats {
				t.Fatal("expired lease precondition; cannot assert preservation of a live lease (initial deadline no longer provides a live-lease bound)")
			}
			if stats.Locked != 2 || stats.DeadLettered != 0 {
				t.Fatalf("reopen live-lease preservation failed locked=%d dlq=%d", stats.Locked, stats.DeadLettered)
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
	original := response.Body
	data, readErr := io.ReadAll(io.LimitReader(original, 4097))
	tail := io.Reader(original)
	if readErr != nil {
		tail = pipelineReadError{readErr}
	}
	response.Body = pipelineReplayBody{Reader: io.MultiReader(bytes.NewReader(data), tail), Closer: original}
	if readErr != nil || len(data) > 4096 {
		return response, nil
	}
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

func TestRestartAcknowledgementPreservesReadError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "32")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer ts.Close()
	base := http.DefaultTransport.(*http.Transport).Clone()
	defer base.CloseIdleConnections()
	ledger := &restartTransport{base: base, queue: "control", completed: make(map[int64]bool)}
	body, err := json.Marshal(wire.SettleRequest{Queue: "control", SeqNumber: 1, LockToken: "local-fixture"})
	if err != nil {
		t.Fatal("cannot marshal control request")
	}
	request, err := http.NewRequest(http.MethodPost, ts.URL+wire.PathComplete, bytes.NewReader(body))
	if err != nil {
		t.Fatal("cannot create control request")
	}
	response, err := (&http.Client{Transport: ledger}).Do(request)
	if err != nil {
		t.Fatal("truncated response did not return headers")
	}
	_, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if !errors.Is(readErr, io.ErrUnexpectedEOF) || ledger.count() != 0 {
		t.Fatal("restart observer hid a truncated response error or counted its acknowledgement")
	}
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

// Observe what the client actually reads, not what a server writer buffered.
// Reading a small copy must preserve the SDK's original bytes, errors and Close.
type pipelineAckTransport struct {
	base      http.RoundTripper
	queue     string
	mu        sync.Mutex
	expected  map[int64]bool
	completed map[int64]bool
	done      chan struct{}
	once      sync.Once
	onAck     func(int64)
}

type pipelineReplayBody struct {
	io.Reader
	io.Closer
}

type pipelineReadError struct{ err error }

func (r pipelineReadError) Read([]byte) (int, error) { return 0, r.err }

func (r *pipelineAckTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := r.base.RoundTrip(request)
	if err != nil || request.URL.Path != wire.PathComplete || response.StatusCode != http.StatusOK {
		return response, err
	}
	original := response.Body
	data, readErr := io.ReadAll(io.LimitReader(original, 4097))
	tail := io.Reader(original)
	if readErr != nil {
		tail = pipelineReadError{readErr}
	}
	response.Body = pipelineReplayBody{Reader: io.MultiReader(bytes.NewReader(data), tail), Closer: original}
	if readErr != nil || len(data) > 4096 || request.GetBody == nil {
		return response, nil
	}
	body, bodyErr := request.GetBody()
	if bodyErr != nil {
		return response, nil
	}
	inputData, inputErr := io.ReadAll(io.LimitReader(body, 4097))
	_ = body.Close()
	var input wire.SettleRequest
	var result wire.SettleResponse
	if inputErr != nil || len(inputData) > 4096 || json.Unmarshal(inputData, &input) != nil ||
		input.Queue != r.queue || input.SeqNumber <= 0 || json.Unmarshal(data, &result) != nil || !result.Ok {
		return response, nil
	}
	r.mu.Lock()
	added := r.expected[input.SeqNumber] && !r.completed[input.SeqNumber]
	if added {
		r.completed[input.SeqNumber] = true
		if len(r.completed) == len(r.expected) && len(r.expected) > 0 {
			r.once.Do(func() { close(r.done) })
		}
	}
	r.mu.Unlock()
	if added && r.onAck != nil {
		r.onAck(input.SeqNumber)
	}
	return response, nil
}

func (r *pipelineAckTransport) count() int { r.mu.Lock(); defer r.mu.Unlock(); return len(r.completed) }
func (r *pipelineAckTransport) confirmed(seq int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.completed[seq]
}

func TestPipelineClientAcknowledgementControls(t *testing.T) {
	for _, mode := range []string{"complete", "truncated", "lost", "missing-ok", "false-ok", "wrong-queue", "unexpected-sequence"} {
		t.Run(mode, func(t *testing.T) {
			generated := make(chan struct{}, 1)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				body := `{"ok":true}`
				generated <- struct{}{}
				if mode == "lost" {
					connection, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error("local loss control could not hijack")
						return
					}
					_ = connection.Close()
					return
				}
				if mode == "missing-ok" {
					body = `{}`
				}
				if mode == "false-ok" {
					body = `{"ok":false}`
				}
				if mode == "truncated" {
					w.Header().Set("Content-Length", fmt.Sprint(len(body)+10))
				}
				_, _ = io.WriteString(w, body)
			}))
			defer ts.Close()
			base := http.DefaultTransport.(*http.Transport).Clone()
			defer base.CloseIdleConnections()
			ledger := &pipelineAckTransport{base: base, queue: "control", expected: map[int64]bool{1: true}, completed: map[int64]bool{}, done: make(chan struct{})}
			queue, seq := "control", int64(1)
			if mode == "wrong-queue" {
				queue = "other"
			}
			if mode == "unexpected-sequence" {
				seq = 2
			}
			body, _ := json.Marshal(wire.SettleRequest{Queue: queue, SeqNumber: seq, LockToken: "local-fixture"})
			request, _ := http.NewRequest(http.MethodPost, ts.URL+wire.PathComplete, bytes.NewReader(body))
			response, requestErr := (&http.Client{Transport: ledger}).Do(request)
			var readErr error
			if response != nil {
				_, readErr = io.ReadAll(response.Body)
				_ = response.Body.Close()
			}
			select {
			case <-generated:
			default:
				t.Fatal("control server did not generate a response")
			}
			if mode == "complete" {
				if requestErr != nil || readErr != nil || ledger.count() != 1 || !ledger.confirmed(1) {
					t.Fatal("valid client acknowledgement was not recorded")
				}
				select {
				case <-ledger.done:
				default:
					t.Fatal("valid acknowledgement did not release completion barrier")
				}
			} else {
				if ledger.count() != 0 {
					t.Fatal("unconfirmed client acknowledgement was recorded")
				}
				select {
				case <-ledger.done:
					t.Fatal("unconfirmed response released completion barrier")
				default:
				}
				if mode == "truncated" && readErr == nil {
					t.Fatal("observer hid the truncated response read error")
				}
				if mode == "lost" && requestErr == nil {
					t.Fatal("observer hid the lost response transport error")
				}
			}
		})
	}
}

// Only types, fixed categories and status codes cross this diagnostic boundary.
// Provider Error() text can contain credentials, SQL or endpoint addresses.
type pipelineErrorFields struct {
	Type             string
	UnwrapTypes      []string
	HTTPStatus       int
	Canceled         bool
	Deadline         bool
	OutcomeUnknown   bool
	LockLost         bool
	PermissionDenied bool
	Unauthenticated  bool
	NotFound         bool
	InvalidArgument  bool
}

func pipelineErrorDiagnostic(err error) pipelineErrorFields {
	fields := pipelineErrorFields{
		Type: fmt.Sprintf("%T", err), Canceled: errors.Is(err, context.Canceled),
		Deadline: errors.Is(err, context.DeadlineExceeded), OutcomeUnknown: errors.Is(err, sdk.ErrOutcomeUnknown) || errors.Is(err, broker.ErrOutcomeUnknown),
		LockLost: errors.Is(err, sdk.ErrLockLost) || errors.Is(err, broker.ErrLockLost), PermissionDenied: errors.Is(err, sdk.ErrPermissionDenied) || errors.Is(err, broker.ErrPermissionDenied),
		Unauthenticated: errors.Is(err, sdk.ErrUnauthenticated) || errors.Is(err, broker.ErrUnauthenticated), NotFound: errors.Is(err, sdk.ErrNotFound) || errors.Is(err, broker.ErrNotFound),
		InvalidArgument: errors.Is(err, sdk.ErrInvalidArgument) || errors.Is(err, broker.ErrInvalidArgument),
	}
	var responseError *sdk.HTTPError
	if errors.As(err, &responseError) && responseError != nil {
		fields.HTTPStatus = responseError.StatusCode
	} else {
		var mainResponseError *broker.HTTPError
		if errors.As(err, &mainResponseError) && mainResponseError != nil {
			fields.HTTPStatus = mainResponseError.StatusCode
		}
	}
	for cause, depth := errors.Unwrap(err), 0; cause != nil && depth < 6; cause, depth = errors.Unwrap(cause), depth+1 {
		fields.UnwrapTypes = append(fields.UnwrapTypes, fmt.Sprintf("%T", cause))
	}
	return fields
}

func TestPipelineErrorDiagnosticPrivacy(t *testing.T) {
	const secret = "https://private.invalid/db?token=PRIVATE_TOKEN_SENTINEL SQL=SELECT_PRIVATE_SENTINEL"
	err := fmt.Errorf("%s: %w", secret, errors.Join(&sdk.HTTPError{StatusCode: 503}, sdk.ErrLockLost,
		sdk.ErrOutcomeUnknown, context.Canceled, context.DeadlineExceeded))
	fields := pipelineErrorDiagnostic(err)
	data, marshalErr := json.Marshal(fields)
	data = append(data, []byte(fmt.Sprintf("%+v", fields))...)
	if marshalErr != nil || bytes.Contains(data, []byte("private.invalid")) || bytes.Contains(data, []byte("PRIVATE_TOKEN_SENTINEL")) ||
		bytes.Contains(data, []byte("SELECT_PRIVATE_SENTINEL")) || !fields.LockLost || !fields.OutcomeUnknown ||
		!fields.Canceled || !fields.Deadline || fields.HTTPStatus != 503 {
		t.Fatal("pipeline diagnostic privacy or category control failed")
	}
	plain := pipelineErrorDiagnostic(errors.New(secret))
	data, marshalErr = json.Marshal(plain)
	data = append(data, []byte(fmt.Sprintf("%+v", plain))...)
	if marshalErr != nil || bytes.Contains(data, []byte("private.invalid")) || bytes.Contains(data, []byte("PRIVATE_TOKEN_SENTINEL")) ||
		bytes.Contains(data, []byte("SELECT_PRIVATE_SENTINEL")) || plain.HTTPStatus != 0 || plain.LockLost || plain.Canceled {
		t.Fatal("pipeline diagnostic negative control failed")
	}
	mainFields := pipelineErrorDiagnostic(fmt.Errorf("%s: %w", secret, errors.Join(&broker.HTTPError{StatusCode: 409}, broker.ErrLockLost)))
	data, marshalErr = json.Marshal(mainFields)
	if marshalErr != nil || bytes.Contains(data, []byte("PRIVATE_TOKEN_SENTINEL")) || mainFields.HTTPStatus != 409 || !mainFields.LockLost {
		t.Fatal("main SDK diagnostic privacy or category control failed")
	}
}

// This deliberately follows the main SDK's HTTP-pipeline ledger, while every
// client operation and managed handler uses the independent remote-only SDK.
func exerciseHTTPBrokerDelivery(t *testing.T, backend, dsn, databaseToken string) {
	t.Helper()
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	mark := func(stage string, seq int64) {
		deadline, _ := ctx.Deadline()
		t.Logf("remote SDK pipeline phase=%s seq=%d elapsed_ms=%d remaining_budget_ms=%d context_expired=%t",
			stage, seq, time.Since(started).Milliseconds(), time.Until(deadline).Milliseconds(), ctx.Err() != nil)
	}
	var snapshot func(string)
	fail := func(stage string, err error) {
		t.Helper()
		if err == nil {
			return
		}
		if snapshot != nil {
			snapshot(stage)
		}
		t.Fatalf("remote SDK pipeline stage=%s diagnostic=%+v", stage, pipelineErrorDiagnostic(err))
	}
	mark("broker_open_begin", 0)
	eng, err := broker.OpenEmbedded(ctx, dsn, broker.WithDBAuthToken(databaseToken), broker.WithBrokerMode(),
		broker.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	fail("broker open", err)
	mark("broker_open_done", 0)
	defer func() {
		if err := eng.Close(); err != nil {
			t.Error("remote SDK pipeline engine close failed")
		}
	}()
	const total = 24
	const brokerToken = "remote-sdk-pipeline-local-test-token"
	queue := fmt.Sprintf("remote-sdk-pipeline-%d-%d", os.Getpid(), time.Now().UnixNano())
	var mu sync.Mutex
	serverCompleted := make(map[int64]bool, total)
	var serverAbandons int
	var renewals, notifications int
	completeFailures, renewFailures := make(map[int]int), make(map[int]int)
	base := server.New(eng.Engine(), []string{brokerToken}).Handler()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Queue string `json:"queue"`
			Seq   int64  `json:"seq_number"`
		}
		if r.URL.Path == wire.PathComplete || r.URL.Path == wire.PathAbandon {
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
			mu.Lock()
			if r.URL.Path == wire.PathComplete {
				completeFailures[recorder.Code]++
			} else if r.URL.Path == wire.PathRenewBatch {
				renewFailures[recorder.Code]++
			}
			mu.Unlock()
			return
		}
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case wire.PathComplete, wire.PathAbandon:
			var response wire.SettleResponse
			if json.Unmarshal(recorder.Body.Bytes(), &response) == nil && response.Ok && request.Queue == queue {
				if r.URL.Path == wire.PathComplete {
					serverCompleted[request.Seq] = true
					mark("server_complete_response_buffered", request.Seq)
				} else {
					serverAbandons++
					mark("server_abandon_response_buffered", request.Seq)
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
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 64
	if transport.MaxIdleConns < 64 {
		transport.MaxIdleConns = 64
	}
	defer transport.CloseIdleConnections()
	acks := &pipelineAckTransport{base: transport, queue: queue, expected: make(map[int64]bool, total), completed: make(map[int64]bool, total), done: make(chan struct{}), onAck: func(seq int64) { mark("client_complete_acknowledged", seq) }}
	client, err := sdk.Open(ctx, ts.URL, sdk.WithToken(brokerToken), sdk.WithHTTPClient(&http.Client{Transport: acks}))
	fail("client open", err)
	defer client.Close()
	fail("create own queue", client.CreateQueue(ctx, queue, sdk.QueueConfig{LockDuration: 30 * time.Second, MaxDeliveryCount: 1}))
	mark("queue_created", 0)
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
		acks.mu.Lock()
		acks.expected[seq] = true
		acks.mu.Unlock()
	}
	mark("serial_sends_finished", 0)
	runCtx, stopReceiver := context.WithCancel(ctx)
	seen := make(map[string]int, total)
	invalidDelivery := false
	snapshot = func(stage string) {
		mu.Lock()
		calls := 0
		for _, count := range seen {
			calls += count
		}
		clientCount := acks.count()
		t.Logf("remote SDK pipeline snapshot stage=%s elapsed_ms=%d context_expired=%t cleanup_phase=before_own_queue_cleanup sent=%d seen_unique=%d handler_calls=%d client_confirmed_complete=%d server_buffered_complete=%d server_buffered_abandon=%d server_buffered_renewals=%d notifications=%d invalid_delivery=%t unconfirmed=%d complete_response_failures_by_status=%v renew_response_failures_by_status=%v",
			stage, time.Since(started).Milliseconds(), ctx.Err() != nil, len(sent), len(seen), calls, clientCount, len(serverCompleted), serverAbandons, renewals, notifications, invalidDelivery,
			len(sent)-clientCount, completeFailures, renewFailures)
		mu.Unlock()
		// The Run context may already be expired. Probe with a fresh, independently
		// bounded context before queue cleanup; DLQ/locked rows are not lost rows.
		probe, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		stats, statsErr := client.Stats(probe, queue)
		mark("failure_stats_before_own_queue_cleanup", 0)
		if statsErr != nil {
			t.Logf("remote SDK pipeline snapshot_stats stage=%s diagnostic=%+v", stage, pipelineErrorDiagnostic(statsErr))
			return
		}
		t.Logf("remote SDK pipeline snapshot_stats stage=%s active=%d locked=%d deferred=%d scheduled=%d dlq=%d total=%d",
			stage, stats.Active, stats.Locked, stats.Deferred, stats.Scheduled, stats.DeadLettered, stats.Total)
	}
	runDone := make(chan error, 1)
	go func() {
		mark("receiver_begin", 0)
		runDone <- client.Receiver(queue, sdk.WithConcurrency(4), sdk.WithAutoRenew(),
			sdk.WithErrorHandler(func(err error) {
				mu.Lock()
				notifications++
				count := notifications
				mu.Unlock()
				t.Logf("remote SDK pipeline notification number=%d diagnostic=%+v", count, pipelineErrorDiagnostic(err))
			})).
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
					mark("slow_handler_begin", message.SequenceNumber)
					// Observe a real renewal at the 30s lease's first 10s point.
					timer := time.NewTimer(11 * time.Second)
					defer timer.Stop()
					select {
					case <-timer.C:
						mark("slow_handler_return_nil", message.SequenceNumber)
					case <-handlerCtx.Done():
						mark("slow_handler_return_context_error", message.SequenceNumber)
						t.Logf("remote SDK pipeline slow_handler diagnostic=%+v", pipelineErrorDiagnostic(handlerCtx.Err()))
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
	case <-acks.done:
	case err := <-runDone:
		joined = true
		fail("Receiver stopped before all completions", err)
		snapshot("Receiver returned nil before all completions")
		t.Fatal("remote SDK pipeline Receiver ended before all completions")
	case <-ctx.Done():
		snapshot("completion deadline")
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
	if invalidDelivery || len(seen) != total || acks.count() != total || renewals == 0 ||
		stats.Total != 0 || stats.Active != 0 || stats.Locked != 0 || stats.DeadLettered != 0 {
		t.Fatalf("remote SDK pipeline accounting failed invalid=%t handled=%d complete=%d renewals=%d total=%d locked=%d dlq=%d",
			invalidDelivery, len(seen), acks.count(), renewals, stats.Total, stats.Locked, stats.DeadLettered)
	}
	for id, want := range sent {
		if seen[id] != 1 || !acks.confirmed(want.seq) {
			t.Fatal("remote SDK pipeline per-message handler/Complete accounting failed")
		}
	}
	t.Logf("remote SDK pipeline backend=%s sent=%d handled=%d client_confirmed_complete=%d server_buffered_complete=%d server_buffered_abandon=%d server_buffered_renewals=%d notifications=%d final_total=%d dlq=%d",
		backend, len(sent), len(seen), acks.count(), len(serverCompleted), serverAbandons, renewals, notifications, stats.Total, stats.DeadLettered)
}
