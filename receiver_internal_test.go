package mqlite

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mqlitehq/mqlite/engine"
)

// fakeSource is a receiveSource that records the attempt id of every receiveOne
// call and can fail the first failN calls, to exercise the Receiver's same-id retry.
type fakeSource struct {
	mu          sync.Mutex
	attempts    []string
	calls       int
	failN       int
	batch       []*Message
	recvErr     error // if set, receiveOne always returns this (permanent-error test)
	completeErr error // if set, complete returns this (settle-error test)
	unlimited   bool  // if set, every receiveOne returns a fresh single-message batch
}

func (f *fakeSource) receiveOne(ctx context.Context, queue string, max int, waitMs int64, mode engine.ReceiveMode, attemptID string) ([]*Message, error) {
	f.mu.Lock()
	f.calls++
	n := f.calls
	f.attempts = append(f.attempts, attemptID)
	f.mu.Unlock()
	switch {
	case f.recvErr != nil:
		return nil, f.recvErr
	case f.unlimited:
		return []*Message{{SequenceNumber: int64(n), Body: []byte("x"), queue: queue, s: f}}, nil
	case n <= f.failN:
		return nil, errors.New("simulated transient receive error")
	case n == f.failN+1:
		return f.batch, nil
	default:
		<-ctx.Done() // quiesce after the batch is delivered; no busy-spin
		return nil, ctx.Err()
	}
}

func (f *fakeSource) complete(ctx context.Context, queue string, seq int64, token string) error {
	return f.completeErr
}
func (f *fakeSource) abandon(ctx context.Context, queue string, seq int64, token string, delayMs int64) error {
	return nil
}
func (f *fakeSource) reject(ctx context.Context, queue string, seq int64, token, reason, desc string) error {
	return nil
}
func (f *fakeSource) deferMsg(ctx context.Context, queue string, seq int64, token string) error {
	return nil
}
func (f *fakeSource) renew(ctx context.Context, queue string, seq int64, token string) error {
	return nil
}

// MQLITE-8: a transient receive error must be retried ONCE with the SAME attempt
// id, so the broker's idempotent-receive machinery replays the lost batch instead
// of claiming new messages (no double-delivery).
func TestReceiverRetriesWithSameAttemptID(t *testing.T) {
	f := &fakeSource{failN: 1}
	f.batch = []*Message{{SequenceNumber: 1, Body: []byte("x"), queue: "q", s: f}}

	// Generous deadline: the receiver only needs one 500ms retry backoff, but a
	// loaded CI runner under -race can starve the goroutine well past a few seconds.
	// A correct receiver still fires in <1s locally; this only absorbs CI jitter.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var once sync.Once
	got := make(chan struct{})
	go func() {
		_ = newReceiver(f, "q", nil).Run(ctx, func(context.Context, *Message) error {
			once.Do(func() { close(got); cancel() })
			return nil
		})
	}()

	select {
	case <-got:
	case <-ctx.Done():
		// The handler cancels ctx the instant it fires, so ctx.Done() can become
		// ready in the same scheduling tick as got closing — select then picks a
		// ready case at random and occasionally reads a real delivery as a timeout
		// (the actual flake, which a wider deadline alone can't fix). got is only
		// ever closed by the handler, so re-check it before declaring failure.
		select {
		case <-got:
		default:
			t.Fatal("handler never received the replayed batch")
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.attempts) < 2 {
		t.Fatalf("a failed receive must be retried; got %d call(s)", len(f.attempts))
	}
	if f.attempts[0] == "" {
		t.Fatal("receive attempt id must be non-empty")
	}
	if f.attempts[0] != f.attempts[1] {
		t.Fatalf("retry must reuse the same attempt id: %q != %q", f.attempts[0], f.attempts[1])
	}
}

// MQLITE-77: a permanent receive error (bad token / missing queue) must stop the loop and
// be returned from Run — not retried forever with Run reporting nothing — and it is also
// handed to the error handler.
func TestReceiverPermanentReceiveErrorReturns(t *testing.T) {
	f := &fakeSource{recvErr: ErrUnauthenticated}
	var observed error
	r := newReceiver(f, "q", []ReceiverOption{WithErrorHandler(func(e error) { observed = e })})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := r.Run(ctx, func(context.Context, *Message) error { return nil })
	if !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("Run must return the permanent receive error, got %v", err)
	}
	if !errors.Is(observed, ErrUnauthenticated) {
		t.Fatalf("error handler must see the error, got %v", observed)
	}
}

// MQLITE-77: a permanent settle failure (here a bad token surfaced from Complete) must stop
// Run and reach the error handler, rather than being silently dropped.
func TestReceiverPermanentSettleErrorStopsRun(t *testing.T) {
	f := &fakeSource{completeErr: ErrUnauthenticated}
	f.batch = []*Message{{SequenceNumber: 1, Body: []byte("x"), queue: "q", s: f}}
	var mu sync.Mutex
	var observed error
	r := newReceiver(f, "q", []ReceiverOption{WithErrorHandler(func(e error) {
		mu.Lock()
		observed = e
		mu.Unlock()
	})})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := r.Run(ctx, func(context.Context, *Message) error { return nil })
	if !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("a permanent settle error must stop Run, got %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !errors.Is(observed, ErrUnauthenticated) {
		t.Fatalf("error handler must see the settle error, got %v", observed)
	}
}

// MQLITE-76: the receiver reserves worker capacity BEFORE claiming, so with concurrency 1 it
// does not claim a second message while the only worker is busy (which would let that
// message's lock expire, queued, before it ever runs).
func TestReceiverReservesCapacityBeforeClaim(t *testing.T) {
	f := &fakeSource{unlimited: true}
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go newReceiver(f, "q", []ReceiverOption{WithConcurrency(1)}).Run(ctx, func(context.Context, *Message) error {
		entered <- struct{}{}
		<-release // hold the single worker
		return nil
	})
	<-entered // the first message is being handled; the only worker slot is taken
	// The loop is now blocked reserving a slot, so it cannot claim more. This window would
	// catch a regression to claim-before-capacity (calls would climb past 1).
	time.Sleep(250 * time.Millisecond)
	f.mu.Lock()
	calls := f.calls
	f.mu.Unlock()
	close(release)
	if calls != 1 {
		t.Fatalf("receiver claimed %d batches while its only worker was busy; must reserve capacity first (want 1)", calls)
	}
}

// Each receive round must use a fresh, unique attempt id so distinct batches are
// not collapsed by idempotent-receive dedup.
func TestNewAttemptIDUnique(t *testing.T) {
	seen := make(map[string]bool, 1000)
	for i := 0; i < 1000; i++ {
		id := newAttemptID()
		if id == "" {
			t.Fatal("attempt id must be non-empty")
		}
		if seen[id] {
			t.Fatalf("attempt id collided: %s", id)
		}
		seen[id] = true
	}
}

// ─── broker http.Server hardening (review F9 / MQLITE-64) ──────────────────────

// Serve's http.Server must carry the Slowloris/keep-alive hardening defaults.
// White-box: newHTTPServer is unexported, and these fields are set nowhere else.
func TestServeHTTPServerHardening(t *testing.T) {
	hs := newHTTPServer(":0", nil)
	if hs.ReadHeaderTimeout <= 0 {
		t.Fatal("ReadHeaderTimeout must be set (Slowloris)")
	}
	if hs.IdleTimeout <= 0 {
		t.Fatal("IdleTimeout must be set (dead keep-alive reclaim)")
	}
	if hs.ReadTimeout != 0 || hs.WriteTimeout != 0 {
		t.Fatal("Read/WriteTimeout must stay 0: Receive long-polls up to 20s; bodies are bounded by server.MaxBodyBytes instead")
	}
}
