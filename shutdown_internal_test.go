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
		h := receiveDrainHandler(server.New(eng.eng, nil).Handler(), gate)
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

	first, err := OpenEmbedded(ctx, "file:"+dbPath)
	if err != nil {
		t.Fatal(err)
	}
	firstClosed := false
	defer func() {
		if !firstClosed {
			_ = first.Close()
		}
	}()
	if err := first.CreateQueue(ctx, "restart-jobs", QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	firstGate := receivegate.New()
	cancelFirst, firstDone := startBroker(first, withReceiveGateForTest(firstGate))
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
	handled := make(chan string, 1)
	observed := make(chan error, 32)
	runDone := make(chan error, 1)
	go func() {
		runDone <- cli.Receiver("restart-jobs", WithAutoRenew(), WithErrorHandler(func(err error) {
			select {
			case observed <- err:
			default:
			}
		})).Run(receiverCtx, func(_ context.Context, msg *Message) error {
			handled <- string(msg.Body)
			return nil
		})
	}()

	// Synchronize on the real engine wait boundary: the first empty claim round
	// completed, the notifier is registered, and this Receive is now parked.
	select {
	case <-firstGate.FirstParked():
	case err := <-runDone:
		t.Fatalf("Receiver exited before parking: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Receiver did not reach the broker long-poll wait")
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

	second, err := OpenEmbedded(ctx, "file:"+dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	cancelSecond, secondDone := startBroker(second)
	secondRunning := true
	defer func() {
		if secondRunning {
			cancelSecond()
			waitBroker(secondDone)
		}
	}()

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
	// broker drain itself must not surface as the fatal H1 cancellation.
	for {
		select {
		case err := <-observed:
			if errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "HTTP 499") ||
				strings.Contains(err.Error(), "mqlite: canceled") {
				t.Fatalf("broker shutdown surfaced as cancellation: %v", err)
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
