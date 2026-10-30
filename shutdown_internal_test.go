package mqlite

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mqlitehq/mqlite/internal/receivegate"
	"github.com/mqlitehq/mqlite/server"
	"github.com/mqlitehq/mqlite/wire"
)

// withReceiveGateForTest exposes the internal parked boundary without adding a
// production Serve option. The test waits on the actual engine long-poll state;
// a timing sleep could pass before any Receive reached the broker.
func withReceiveGateForTest(gate *receivegate.Gate) ServeOption {
	return func(c *serveConfig) { c.receiveGate = gate }
}

func TestReceiveDrainWireContract(t *testing.T) {
	ctx := context.Background()
	eng, err := OpenEmbedded(ctx, ":memory:", WithoutBackground())
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	if err := eng.CreateQueue(ctx, "q", QueueConfig{}); err != nil {
		t.Fatal(err)
	}

	doReceive := func(gate *receivegate.Gate, requestCtx context.Context) *httptest.ResponseRecorder {
		srv := server.New(eng.eng, nil)
		srv.CORS = "*"
		h := receiveDrainHandler(srv.Handler(), gate)
		req := httptest.NewRequest(http.MethodPost, wire.PathReceive,
			strings.NewReader(`{"queue":"q","wait_time_ms":20000,"receive_attempt_id":"attempt"}`)).WithContext(requestCtx)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	// Before a first round, an AttemptID may refer to a prior committed response.
	// Fence it with retryable 503 rather than an empty success.
	draining := receivegate.New()
	draining.Stop()
	rec := doReceive(draining, ctx)
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("first-round drain = %d Retry-After=%q; want 503/1; body=%s",
			rec.Code, rec.Header().Get("Retry-After"), rec.Body.String())
	}
	if exposed := rec.Header().Get("Access-Control-Expose-Headers"); !strings.Contains(exposed, "Retry-After") {
		t.Fatalf("drain CORS expose headers = %q, want Retry-After", exposed)
	}
	var body wire.ErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Code != "unavailable" {
		t.Fatalf("first-round drain code = %q, want unavailable", body.Code)
	}

	// A caller-canceled request retains the existing 499 contract; the gate never
	// rewrites request cancellation as broker drain.
	requestCtx, cancelRequest := context.WithCancel(ctx)
	cancelRequest()
	clientCanceled := doReceive(receivegate.New(), requestCtx)
	if clientCanceled.Code != 499 {
		t.Fatalf("caller-canceled Receive = %d, want 499; body=%s", clientCanceled.Code, clientCanceled.Body.String())
	}
}

func TestServeDrainBarrierKeepsPostStopMessageActive(t *testing.T) {
	ctx := context.Background()
	eng, err := OpenEmbedded(ctx, "file:"+filepath.Join(t.TempDir(), "drain.db"), WithBrokerMode())
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	if err := eng.CreateQueue(ctx, "q", QueueConfig{}); err != nil {
		t.Fatal(err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}

	gate := receivegate.New()
	serveCtx, cancelServe := context.WithCancel(ctx)
	defer cancelServe()
	ready := make(chan struct{})
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- eng.Serve(serveCtx, addr,
			withReceiveGateForTest(gate),
			WithReady(func() { close(ready) }),
		)
	}()
	select {
	case <-ready:
	case err := <-serveDone:
		t.Fatalf("broker exited before ready: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("broker did not become ready")
	}

	client, err := Open(ctx, "http://"+addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	receiveCtx, cancelReceive := context.WithCancel(ctx)
	defer cancelReceive()
	type receiveResult struct {
		messages []*Message
		err      error
	}
	receiveDone := make(chan receiveResult, 1)
	go func() {
		messages, err := client.Receive(receiveCtx, "q", RecvOpts{Wait: 20 * time.Second})
		receiveDone <- receiveResult{messages: messages, err: err}
	}()
	select {
	case <-gate.FirstParked():
	case result := <-receiveDone:
		t.Fatalf("long poll returned before parking: %v", result.err)
	case <-time.After(5 * time.Second):
		t.Fatal("long poll did not reach the broker wait boundary")
	}

	cancelServe()
	select {
	case <-gate.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not establish the receive drain barrier")
	}
	if _, err := eng.SendOne(ctx, "q", OutMessage{Body: []byte("after-barrier")}); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-receiveDone:
		if result.err != nil || len(result.messages) != 0 {
			t.Fatalf("drained long poll = %+v, %v; want empty success", result.messages, result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("long poll did not wake after the drain barrier")
	}
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("clean shutdown returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after draining the parked receive")
	}
	peeked, err := eng.Peek(ctx, "q", PeekOpts{Max: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(peeked) != 1 || string(peeked[0].Body) != "after-barrier" {
		t.Fatalf("message sent after the drain barrier was claimed: %+v", peeked)
	}
}

func TestReceiverSurvivesBrokerRestart(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "restart.db")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}

	startBroker := func(eng *Embedded, opts ...ServeOption) (context.CancelFunc, <-chan error) {
		serveCtx, cancel := context.WithCancel(ctx)
		ready := make(chan struct{})
		done := make(chan error, 1)
		opts = append(opts, WithReady(func() { close(ready) }))
		go func() { done <- eng.Serve(serveCtx, addr, opts...) }()
		select {
		case <-ready:
		case err := <-done:
			cancel()
			t.Fatalf("broker exited before ready: %v", err)
		case <-time.After(5 * time.Second):
			cancel()
			t.Fatal("broker did not become ready")
		}
		return cancel, done
	}
	waitBroker := func(done <-chan error) {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("broker shutdown returned %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("broker did not shut down promptly")
		}
	}

	first, err := OpenEmbedded(ctx, "file:"+dbPath, WithBrokerMode())
	if err != nil {
		t.Fatal(err)
	}
	firstClosed := false
	defer func() {
		if !firstClosed {
			_ = first.Close()
		}
	}()
	if err := first.CreateQueue(ctx, "restart-jobs", QueueConfig{LockDuration: 30 * time.Second, MaxDeliveryCount: 1}); err != nil {
		t.Fatal(err)
	}
	cancelFirst, firstDone := startBroker(first)
	firstRunning := true
	defer func() {
		if firstRunning {
			cancelFirst()
			waitBroker(firstDone)
		}
	}()

	cli, err := Open(ctx, "http://"+addr)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	receiverCtx, cancelReceiver := context.WithCancel(ctx)
	defer cancelReceiver()
	handled := make(chan string, 2)
	handlerStarted := make(chan struct{}, 1)
	releaseHandler := make(chan struct{})
	var inFlightCalls atomic.Int32
	observed := make(chan error, 32)
	runDone := make(chan error, 1)
	go func() {
		runDone <- cli.Receiver("restart-jobs", WithAutoRenew(), WithErrorHandler(func(err error) {
			select {
			case observed <- err:
			default:
			}
		})).Run(receiverCtx, func(handlerCtx context.Context, msg *Message) error {
			body := string(msg.Body)
			if body == "in-flight" {
				inFlightCalls.Add(1)
				select {
				case handlerStarted <- struct{}{}:
				default:
				}
				select {
				case <-releaseHandler:
				case <-handlerCtx.Done():
					return handlerCtx.Err()
				}
			}
			handled <- body
			return nil
		})
	}()

	if _, err := cli.SendOne(ctx, "restart-jobs", OutMessage{Body: []byte("in-flight")}); err != nil {
		t.Fatal(err)
	}
	// Stop the broker only after the delivery is inside user code. The handler is
	// not an HTTP request; graceful HTTP drain alone cannot account for its lease.
	select {
	case <-handlerStarted:
	case err := <-runDone:
		t.Fatalf("Receiver exited before handler started: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Receiver did not start the in-flight handler")
	}
	cancelFirst()
	waitBroker(firstDone)
	firstRunning = false
	select {
	case err := <-runDone:
		t.Fatalf("Receiver exited during broker restart: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	firstClosed = true

	second, err := OpenEmbedded(ctx, "file:"+dbPath, WithBrokerMode())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	metrics, err := second.Stats(ctx, "restart-jobs")
	if err != nil {
		t.Fatal(err)
	}
	if metrics.Locked != 1 || metrics.Active != 0 || metrics.DeadLettered != 0 {
		t.Fatalf("broker restart revoked the in-flight lease: %+v", metrics)
	}
	cancelSecond, secondDone := startBroker(second)
	secondRunning := true
	defer func() {
		if secondRunning {
			cancelSecond()
			waitBroker(secondDone)
		}
	}()
	close(releaseHandler)
	select {
	case body := <-handled:
		if body != "in-flight" {
			t.Fatalf("first handled body = %q, want in-flight", body)
		}
	case err := <-runDone:
		t.Fatalf("Receiver exited before completing the preserved lease: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight handler did not return after broker restart")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		metrics, err = second.Stats(ctx, "restart-jobs")
		if err != nil {
			t.Fatal(err)
		}
		if metrics.Total == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("preserved delivery was not completed: %+v", metrics)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := inFlightCalls.Load(); got != 1 {
		t.Fatalf("in-flight handler calls = %d, want exactly one", got)
	}

	if _, err := cli.SendOne(ctx, "restart-jobs", OutMessage{Body: []byte("after-restart")}); err != nil {
		t.Fatal(err)
	}
	select {
	case body := <-handled:
		if body != "after-restart" {
			t.Fatalf("handled body = %q, want after-restart", body)
		}
	case err := <-runDone:
		t.Fatalf("Receiver exited before consuming after restart: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Receiver did not resume after broker restart")
	}

	// Connection-refused during the short restart is transient and allowed. The
	// broker drain itself must not surface as cancellation or revoke the lease.
	for {
		select {
		case err := <-observed:
			if errors.Is(err, ErrLockLost) || errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "HTTP 499") ||
				strings.Contains(err.Error(), "mqlite: canceled") {
				t.Fatalf("broker restart invalidated Receiver state: %v", err)
			}
		default:
			goto observedDrained
		}
	}

observedDrained:
	cancelReceiver()
	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Receiver cancellation returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Receiver did not stop after caller cancellation")
	}
	cancelSecond()
	waitBroker(secondDone)
	secondRunning = false
}
