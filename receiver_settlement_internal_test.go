package mqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/server"
	"github.com/mqlitehq/mqlite/wire"
)

func TestSettlementHandoffJoinsCanceledRenewAndRetainsConfirmedLease(t *testing.T) {
	f := &fakeSource{}
	message := &Message{SequenceNumber: 1, LockedUntil: time.Now().Add(500 * time.Millisecond), queue: "q", s: f}
	secondStarted, secondCanceled, allowSecond := make(chan struct{}), make(chan struct{}), make(chan struct{})
	thirdStarted, completeStarted := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(allowSecond) }) }
	defer release()
	var calls int32
	f.renewDeadlineFn = func(ctx context.Context) (time.Time, error) {
		switch atomic.AddInt32(&calls, 1) {
		case 1:
			return time.Now().Add(time.Second), nil
		case 2:
			close(secondStarted)
			<-ctx.Done()
			close(secondCanceled)
			<-allowSecond
			return time.Now().Add(time.Hour), nil
		default:
			close(thirdStarted)
			return time.Now().Add(2 * time.Second), nil
		}
	}
	f.completeFn = func(context.Context) error {
		close(completeStarted)
		select {
		case <-thirdStarted:
			return nil
		case <-time.After(2 * time.Second):
			return ErrInvalidArgument
		}
	}
	var observed int32
	r := newReceiver(f, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) {
		atomic.AddInt32(&observed, 1)
		t.Errorf("handoff error: %v", err)
	})})
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.process(context.Background(), message, func(context.Context, *Message) error { <-secondStarted; return nil }, func() {}, func() bool { return false }, func(error, bool) {}, false)
	}()
	select {
	case <-secondCanceled:
	case <-time.After(3 * time.Second):
		t.Fatal("handling renewal did not observe handoff cancellation")
	}
	select {
	case <-completeStarted:
		t.Fatal("settlement ran before the handling renewal joined")
	case <-time.After(20 * time.Millisecond):
	}
	release()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("settlement did not use the last confirmed lease after handoff")
	}
	if atomic.LoadInt32(&calls) != 3 || atomic.LoadInt32(&observed) != 0 {
		t.Fatal("handoff lost the confirmed lease or accepted a canceled late renewal")
	}
}

func TestHandlingRenewalGoexitCancelsHandlerAndSkipsSettlement(t *testing.T) {
	f := &fakeSource{}
	message := &Message{SequenceNumber: 1, LockedUntil: time.Now().Add(500 * time.Millisecond), queue: "q", s: f}
	var goexited int32
	f.renewDeadlineFn = func(context.Context) (time.Time, error) {
		atomic.StoreInt32(&goexited, 1)
		runtime.Goexit()
		return time.Time{}, nil
	}
	observed := make(chan error, 4)
	r := newReceiver(f, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) { observed <- err })})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.process(ctx, message, func(handlerCtx context.Context, _ *Message) error { <-handlerCtx.Done(); return handlerCtx.Err() }, func() {}, func() bool { return false }, func(error, bool) {}, false)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handling heartbeat Goexit did not cancel and join its handler")
	}
	if atomic.LoadInt32(&goexited) != 1 {
		t.Fatal("test did not exercise transport Goexit")
	}
	select {
	case err := <-observed:
		if !errors.Is(err, ErrLockLost) {
			t.Fatalf("aborted heartbeat=%v, want LockLost", err)
		}
	default:
		t.Fatal("aborted handling heartbeat was silent")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.completeCalls != 0 || f.abandonCalls != 0 {
		t.Fatal("aborted heartbeat authorized settlement")
	}
}

func TestSettlementRenewalGoexitDoesNotStrandJoin(t *testing.T) {
	f := &fakeSource{}
	message := &Message{SequenceNumber: 1, LockedUntil: time.Now().Add(500 * time.Millisecond), queue: "q", s: f}
	started := make(chan struct{})
	f.renewDeadlineFn = func(context.Context) (time.Time, error) { close(started); runtime.Goexit(); return time.Time{}, nil }
	f.completeFn = func(context.Context) error { <-started; return nil }
	r := newReceiver(f, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) { t.Errorf("unexpected error: %v", err) })})
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.process(context.Background(), message, func(context.Context, *Message) error { return nil }, func() {}, func() bool { return false }, func(error, bool) {}, false)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("heartbeat Goexit stranded settlement join")
	}
}

func TestSettlementHeartbeatPermanentFailureStopsRunBeforeNotification(t *testing.T) {
	f := &fakeSource{}
	message := &Message{SequenceNumber: 1, LockedUntil: time.Now().Add(500 * time.Millisecond), queue: "q", s: f}
	notifyEntered, releaseNotify := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseNotify) }) }
	defer release()
	var receiveContext context.Context
	f.receiveFn = func(ctx context.Context, call int, _ string) ([]*Message, error) {
		if call == 1 {
			receiveContext = ctx
			return []*Message{message}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	f.renewDeadlineFn = func(context.Context) (time.Time, error) { return time.Time{}, ErrPermissionDenied }
	f.completeFn = func(context.Context) error { <-notifyEntered; return nil }
	var notifications int32
	r := newReceiver(f, "q", []ReceiverOption{WithAutoRenew(), WithConcurrency(1), WithErrorHandler(func(err error) {
		if !errors.Is(err, ErrPermissionDenied) {
			t.Errorf("unexpected error: %v", err)
		}
		if receiveContext == nil || receiveContext.Err() == nil {
			t.Error("Run was not stopped before the permanent renewal notification")
		}
		atomic.AddInt32(&notifications, 1)
		close(notifyEntered)
		<-releaseNotify
	})})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx, func(context.Context, *Message) error { return nil }) }()
	select {
	case <-notifyEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("settlement heartbeat did not report its permanent failure")
	}
	f.mu.Lock()
	calls := f.calls
	f.mu.Unlock()
	select {
	case <-done:
		t.Fatal("Run returned before the notifying heartbeat joined")
	case <-time.After(20 * time.Millisecond):
	}
	f.mu.Lock()
	later := f.calls
	f.mu.Unlock()
	if later != calls {
		t.Fatal("claim started after permanent renewal failure")
	}
	release()
	select {
	case err := <-done:
		if !errors.Is(err, ErrPermissionDenied) {
			t.Fatalf("Run=%v, want original permanent renewal failure", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not join after notification returned")
	}
	if atomic.LoadInt32(&notifications) != 1 {
		t.Fatal("permanent heartbeat failure was not reported exactly once")
	}
}

func TestSettlementPanicAndGoexitJoinHeartbeat(t *testing.T) {
	for _, goexit := range []bool{false, true} {
		name := "panic"
		if goexit {
			name = "Goexit"
		}
		t.Run(name, func(t *testing.T) {
			f := &fakeSource{}
			message := &Message{SequenceNumber: 1, LockedUntil: time.Now().Add(500 * time.Millisecond), queue: "q", s: f}
			started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			f.renewDeadlineFn = func(ctx context.Context) (time.Time, error) {
				close(started)
				<-ctx.Done()
				close(canceled)
				<-release
				return time.Time{}, ctx.Err()
			}
			f.completeFn = func(context.Context) error {
				<-started
				if goexit {
					runtime.Goexit()
				}
				panic("controlled settlement panic")
			}
			r := newReceiver(f, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) { t.Errorf("unexpected error: %v", err) })})
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer func() { _ = recover() }()
				r.process(context.Background(), message, func(context.Context, *Message) error { return nil }, func() {}, func() bool { return false }, func(error, bool) {}, false)
			}()
			select {
			case <-canceled:
			case <-time.After(3 * time.Second):
				t.Fatal("settlement unwind did not cancel its heartbeat")
			}
			select {
			case <-done:
				t.Fatal("settlement unwind returned before its heartbeat exited")
			case <-time.After(20 * time.Millisecond):
			}
			unblock()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("settlement unwind failed to join heartbeat")
			}
		})
	}
}

func TestSettlementRenewalLossIsResolvedByFinalSettlement(t *testing.T) {
	for _, outcome := range []string{"confirmed", "replayed", "lock lost", "unknown"} {
		t.Run(outcome, func(t *testing.T) {
			f := &fakeSource{}
			message := &Message{SequenceNumber: 1, LockedUntil: time.Now().Add(500 * time.Millisecond), queue: "q", s: f}
			renewLost := make(chan struct{})
			var once sync.Once
			f.renewDeadlineFn = func(context.Context) (time.Time, error) {
				once.Do(func() { close(renewLost) })
				return time.Time{}, ErrLockLost
			}
			attempts := 0
			complete := func(context.Context) error {
				attempts++
				select {
				case <-renewLost:
				case <-time.After(2 * time.Second):
					return ErrInvalidArgument
				}
				switch outcome {
				case "confirmed":
					return nil
				case "replayed":
					if attempts == 1 {
						return errors.New("lost committed settlement acknowledgement")
					}
					return nil
				case "lock lost":
					return ErrLockLost
				default:
					return errors.New("settlement response unavailable")
				}
			}
			f.completeFn = complete
			var mu sync.Mutex
			var observed []error
			r := newReceiver(f, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) {
				mu.Lock()
				observed = append(observed, err)
				mu.Unlock()
			})})
			if outcome == "unknown" {
				r.settlement.totalTimeout = 350 * time.Millisecond
				r.settlement.attemptTimeout = 100 * time.Millisecond
			}
			handlers := 0
			r.process(context.Background(), message, func(context.Context, *Message) error { handlers++; return nil }, func() {}, func() bool { return false }, func(error, bool) { t.Error("ownership loss must not stop Run") }, false)
			select {
			case <-renewLost:
			default:
				t.Fatal("settlement did not overlap an ownership-losing heartbeat")
			}
			mu.Lock()
			defer mu.Unlock()
			if handlers != 1 {
				t.Fatal("handler repeated")
			}
			if outcome == "confirmed" || outcome == "replayed" {
				if outcome == "replayed" && attempts != 2 {
					t.Fatal("settlement did not replay after the lost acknowledgement")
				}
				if len(observed) != 0 {
					t.Fatalf("own settlement loss was reported: %v", observed)
				}
				return
			}
			if len(observed) != 1 {
				t.Fatalf("observed %d errors, want the single final settlement outcome", len(observed))
			}
			if outcome == "lock lost" {
				if !errors.Is(observed[0], ErrLockLost) {
					t.Fatalf("final error = %v, want lock lost", observed[0])
				}
			} else if !errors.Is(observed[0], ErrOutcomeUnknown) || errors.Is(observed[0], ErrLockLost) {
				t.Fatalf("unconfirmed settlement lost Unknown identity: %v", observed[0])
			}
		})
	}
}

func writeReceiverSettlementResponse(w http.ResponseWriter, rec *httptest.ResponseRecorder) {
	for key, values := range rec.Header() {
		w.Header()[key] = append([]string(nil), values...)
	}
	w.WriteHeader(rec.Code)
	_, _ = w.Write(rec.Body.Bytes())
}

type receiverSettlementBody struct {
	io.ReadCloser
	data []byte
	done func()
}

func (b *receiverSettlementBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.data = append(b.data, p[:n]...)
	if err == io.EOF && b.done != nil {
		var response wire.SettleResponse
		if json.Unmarshal(b.data, &response) == nil && response.Ok {
			b.done()
		}
		b.done = nil
	}
	return n, err
}

type receiverSettlementTransport struct {
	next http.RoundTripper
	ack  func()
}

func (t receiverSettlementTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	response, err := t.next.RoundTrip(req)
	if err == nil && response != nil && response.StatusCode == http.StatusOK && req.URL.Path == wire.PathComplete {
		response.Body = &receiverSettlementBody{ReadCloser: response.Body, done: t.ack}
	}
	return response, err
}

// The delay is before the real engine's fenced Complete, not in the response
// after it committed. Stopping renewal at handler return loses this live token.
func TestReceiverRenewsUntilRealBrokerSettlementConfirmed(t *testing.T) {
	for _, cancelDuringSettlement := range []bool{false, true} {
		name := "normal"
		if cancelDuringSettlement {
			name = "Run canceled during settlement"
		}
		t.Run(name, func(t *testing.T) {
			eng, err := engine.Open(context.Background(), engine.Options{DB: ":memory:", DisableBackground: true})
			if err != nil {
				t.Fatal(err)
			}
			defer eng.Close()
			const queue = "settlement-renewal"
			if err = eng.CreateQueue(context.Background(), queue, engine.QueueConfig{LockDurationMs: 1000, MaxDeliveryCount: 1}); err != nil {
				t.Fatal(err)
			}
			if _, err = eng.SendOne(context.Background(), queue, engine.OutMessage{Body: []byte("payload")}); err != nil {
				t.Fatal(err)
			}
			completeStarted := make(chan struct{})
			var completeOnce sync.Once
			var renewCalls atomic.Int64
			handler := server.New(eng, nil).Handler()
			broker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.URL.Path == wire.PathComplete {
					completeOnce.Do(func() { close(completeStarted) })
					timer := time.NewTimer(1250 * time.Millisecond)
					defer timer.Stop()
					select {
					case <-timer.C:
					case <-req.Context().Done():
						return
					}
				}
				if req.URL.Path == wire.PathRenewBatch || req.URL.Path == wire.PathRenew {
					renewCalls.Add(1)
				}
				handler.ServeHTTP(w, req)
			}))
			defer broker.Close()
			ack := make(chan struct{})
			var ackOnce sync.Once
			client, err := Open(context.Background(), broker.URL, WithHTTPClient(&http.Client{Transport: receiverSettlementTransport{
				next: broker.Client().Transport,
				ack:  func() { ackOnce.Do(func() { close(ack) }) },
			}}))
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			ctx, cancel := context.WithCancel(context.Background())
			var handlers atomic.Int64
			observed := make(chan error, 8)
			runDone := make(chan error, 1)
			go func() {
				runDone <- client.Receiver(queue, WithAutoRenew(), WithConcurrency(1), WithErrorHandler(func(err error) {
					observed <- err
				})).Run(ctx, func(context.Context, *Message) error { handlers.Add(1); return nil })
			}()
			defer func() {
				cancel()
				select {
				case err := <-runDone:
					if !errors.Is(err, context.Canceled) {
						t.Errorf("Run = %v, want context.Canceled", err)
					}
				case <-time.After(5 * time.Second):
					t.Error("Run did not join settlement and renewal")
				}
			}()
			select {
			case <-completeStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("Complete did not start")
			}
			if cancelDuringSettlement {
				cancel()
			}
			select {
			case <-ack:
			case err := <-observed:
				t.Fatalf("live token lost while Complete was waiting: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("Complete did not reach a successful response EOF")
			}
			if handlers.Load() != 1 || renewCalls.Load() == 0 {
				t.Fatalf("handlers=%d renewals=%d", handlers.Load(), renewCalls.Load())
			}
			stats, err := eng.Stats(context.Background(), queue)
			if err != nil || stats.Total != 0 || stats.DeadLettered != 0 {
				t.Fatalf("final queue=%+v err=%v", stats, err)
			}
			select {
			case err := <-observed:
				t.Fatalf("unexpected receiver error: %v", err)
			default:
			}
		})
	}
}

func TestReceiverCoalescesRealPostCommitRenewalLoss(t *testing.T) {
	for _, dropResponse := range []bool{false, true} {
		name := "delayed acknowledgement"
		if dropResponse {
			name = "lost acknowledgement exact replay"
		}
		t.Run(name, func(t *testing.T) {
			eng, err := engine.Open(context.Background(), engine.Options{DB: ":memory:", DisableBackground: true})
			if err != nil {
				t.Fatal(err)
			}
			defer eng.Close()
			const queue = "post-commit-renewal"
			if err = eng.CreateQueue(context.Background(), queue, engine.QueueConfig{LockDurationMs: 1000, MaxDeliveryCount: 1}); err != nil {
				t.Fatal(err)
			}
			if _, err = eng.SendOne(context.Background(), queue, engine.OutMessage{Body: []byte("payload")}); err != nil {
				t.Fatal(err)
			}
			renewLost := make(chan struct{})
			var lostOnce sync.Once
			var requestsMu sync.Mutex
			var requests [][]byte
			handler := server.New(eng, nil).Handler()
			broker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.URL.Path == wire.PathRenewBatch {
					// Exercise the legacy fenced Renew route's actual HTTP 409 as
					// well as the SDK's established batch-capability fallback.
					http.NotFound(w, req)
					return
				}
				if req.URL.Path != wire.PathComplete && req.URL.Path != wire.PathRenew {
					handler.ServeHTTP(w, req)
					return
				}
				call := 0
				if req.URL.Path == wire.PathComplete {
					body, err := io.ReadAll(req.Body)
					if err != nil {
						http.Error(w, "request read failed", 400)
						return
					}
					req.Body = io.NopCloser(bytes.NewReader(body))
					requestsMu.Lock()
					requests = append(requests, body)
					call = len(requests)
					requestsMu.Unlock()
				}
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req) // the real settlement commits here
				if req.URL.Path == wire.PathRenew && rec.Code == http.StatusConflict {
					lostOnce.Do(func() { close(renewLost) })
				}
				if call == 1 && rec.Code == http.StatusOK {
					select {
					case <-renewLost:
					case <-time.After(2 * time.Second):
					}
					if dropResponse {
						conn, _, err := w.(http.Hijacker).Hijack()
						if err == nil {
							_ = conn.Close()
						}
						return
					}
				}
				writeReceiverSettlementResponse(w, rec)
			}))
			defer broker.Close()
			ack := make(chan struct{})
			var ackOnce sync.Once
			client, err := Open(context.Background(), broker.URL, WithHTTPClient(&http.Client{Transport: receiverSettlementTransport{
				next: broker.Client().Transport, ack: func() { ackOnce.Do(func() { close(ack) }) },
			}}))
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			ctx, cancel := context.WithCancel(context.Background())
			observed := make(chan error, 8)
			var handled atomic.Int64
			done := make(chan error, 1)
			go func() {
				done <- client.Receiver(queue, WithAutoRenew(), WithConcurrency(1), WithErrorHandler(func(err error) { observed <- err })).Run(ctx, func(context.Context, *Message) error { handled.Add(1); return nil })
			}()
			defer func() {
				cancel()
				select {
				case err := <-done:
					if !errors.Is(err, context.Canceled) {
						t.Errorf("Run = %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Error("Run did not join")
				}
			}()
			select {
			case <-ack:
			case err := <-observed:
				t.Fatalf("own committed settlement misreported: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("settlement acknowledgement did not arrive")
			}
			select {
			case <-renewLost:
			default:
				t.Fatal("test did not exercise a post-commit Renew 409")
			}
			requestsMu.Lock()
			defer requestsMu.Unlock()
			want := 1
			if dropResponse {
				want = 2
			}
			if len(requests) != want || handled.Load() != 1 {
				t.Fatalf("Complete calls=%d handlers=%d", len(requests), handled.Load())
			}
			for _, request := range requests[1:] {
				if !bytes.Equal(requests[0], request) {
					t.Fatal("settlement replay changed the exact request")
				}
			}
			stats, err := eng.Stats(context.Background(), queue)
			if err != nil || stats.Total != 0 {
				t.Fatalf("queue=%+v err=%v", stats, err)
			}
			select {
			case err := <-observed:
				t.Fatalf("unexpected renewal/settlement error: %v", err)
			default:
			}
		})
	}
}
