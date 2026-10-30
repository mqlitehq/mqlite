package mqlite

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/wire"
)

// A positive age bound must NEVER convert to 0 ms (which the engine reads as unbounded) —
// only an exact zero (no bound) does. Guards the sub-millisecond truncation that let
// `purge-dlq --older-than 1ns` delete everything (review 2026-07-12 round-2 B2).
func TestOlderThanMs(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want int64
	}{
		{0, 0},
		{-time.Nanosecond, -1},
		{-time.Hour, -1},
		{time.Nanosecond, 1},
		{999999 * time.Nanosecond, 1},
		{time.Millisecond, 1},
		{1500 * time.Microsecond, 1},
		{time.Second, 1000},
	}
	for _, c := range cases {
		if got := olderThanMs(c.d); got != c.want {
			t.Errorf("olderThanMs(%s) = %d, want %d", c.d, got, c.want)
		}
	}
}

// fakeSource is a receiveSource that records the attempt id of every receiveOne
// call and can fail the first failN calls, to exercise the Receiver's same-id retry.
type fakeSource struct {
	mu                    sync.Mutex
	attempts              []string
	calls                 int
	completeCalls         int
	abandonCalls          int
	renewCalls            int
	deadlineRenewCalls    int
	failN                 int
	batch                 []*Message
	recvErr               error // if set, receiveOne always returns this (permanent-error test)
	recvErrAfterBatch     error // if set, the receive AFTER the batch returns this instead of quiescing
	abandonErr            error
	renewErr              error
	completeErr           error // if set, complete returns this (settle-error test)
	unlimited             bool  // if set, every receiveOne returns a fresh single-message batch
	completeFn            func(context.Context) error
	abandonFn             func(context.Context) error
	renewFn               func(context.Context) error
	renewDeadlineFn       func(context.Context) (time.Time, error)
	renewResponseRTT      time.Duration
	renewCoarseClock      bool
	renewClockUnavailable bool
	receiveFn             func(context.Context, int, string) ([]*Message, error)
}

func (f *fakeSource) receiveOne(ctx context.Context, queue string, max int, waitMs int64, mode engine.ReceiveMode, attemptID string) ([]*Message, error) {
	f.mu.Lock()
	f.calls++
	n := f.calls
	f.attempts = append(f.attempts, attemptID)
	fn := f.receiveFn
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, n, attemptID)
	}
	switch {
	case f.recvErr != nil:
		return nil, f.recvErr
	case f.unlimited:
		return []*Message{{SequenceNumber: int64(n), Body: []byte("x"), LockedUntil: time.Now().Add(30 * time.Second), queue: queue, s: f}}, nil
	case n <= f.failN:
		return nil, errors.New("simulated transient receive error")
	case n == f.failN+1:
		return f.batch, nil
	case n >= f.failN+2 && f.recvErrAfterBatch != nil:
		// every receive after the batch — including the loop's one transient retry — fails
		return nil, f.recvErrAfterBatch
	default:
		<-ctx.Done() // quiesce after the batch is delivered; no busy-spin
		return nil, ctx.Err()
	}
}

func (f *fakeSource) complete(ctx context.Context, queue string, seq int64, token string) error {
	f.mu.Lock()
	f.completeCalls++
	fn, err := f.completeFn, f.completeErr
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx)
	}
	return err
}
func (f *fakeSource) abandon(ctx context.Context, queue string, seq int64, token string, delayMs int64) error {
	f.mu.Lock()
	f.abandonCalls++
	fn, err := f.abandonFn, f.abandonErr
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx)
	}
	return err
}
func (f *fakeSource) reject(ctx context.Context, queue string, seq int64, token, reason, desc string) error {
	return nil
}
func (f *fakeSource) deferMsg(ctx context.Context, queue string, seq int64, token string) error {
	return nil
}
func (f *fakeSource) renew(ctx context.Context, queue string, seq int64, token string) error {
	f.mu.Lock()
	f.renewCalls++
	fn, err := f.renewFn, f.renewErr
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx)
	}
	return err
}

// renewDeadline is the managed Receiver's optional renewal path. Production
// clients can use a response carrying the deadline actually committed by the
// broker while Message.Renew keeps its legacy error-only public signature.
func (f *fakeSource) renewDeadline(ctx context.Context, queue string, seq int64, token string) (renewalLease, error) {
	f.mu.Lock()
	f.renewCalls++
	f.deadlineRenewCalls++
	fn, err := f.renewDeadlineFn, f.renewErr
	rtt, coarse, unavailable := f.renewResponseRTT, f.renewCoarseClock, f.renewClockUnavailable
	f.mu.Unlock()
	if fn != nil {
		deadline, err := fn(ctx)
		return renewalLease{deadline: deadline, responseRTT: rtt, coarseClock: coarse, clockUnavailable: unavailable}, err
	}
	return renewalLease{responseRTT: rtt, coarseClock: coarse, clockUnavailable: unavailable}, err
}

// MQLITE-8: a transient receive error must be retried ONCE with the SAME attempt
// id, so the broker's idempotent-receive machinery replays the lost batch instead
// of claiming new messages (no double-delivery).
func TestReceiverRetriesWithSameAttemptID(t *testing.T) {
	f := &fakeSource{failN: 1}
	f.batch = []*Message{{SequenceNumber: 1, Body: []byte("x"), LockedUntil: time.Now().Add(30 * time.Second), queue: "q", s: f}}

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

func TestReceiverRetriesBrokerCanceledReceiveWithSameAttemptID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	f := &fakeSource{}
	f.receiveFn = func(callCtx context.Context, call int, attemptID string) ([]*Message, error) {
		switch call {
		case 1:
			return nil, &HTTPError{StatusCode: 499, cause: fmt.Errorf("mqlite: canceled: %w", context.Canceled)}
		case 2:
			return []*Message{{SequenceNumber: 1, LockedUntil: time.Now().Add(time.Second), queue: "q", s: f}}, nil
		default:
			<-callCtx.Done()
			return nil, callCtx.Err()
		}
	}
	handled := 0
	err := newReceiver(f, "q", nil).Run(ctx, func(context.Context, *Message) error {
		handled++
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want caller cancellation after recovered delivery", err)
	}
	if handled != 1 {
		t.Fatalf("handler calls = %d, want one", handled)
	}
	f.mu.Lock()
	attempts := append([]string(nil), f.attempts...)
	f.mu.Unlock()
	if len(attempts) < 2 || attempts[0] == "" || attempts[0] != attempts[1] {
		t.Fatalf("499 replay attempts = %q, want the same non-empty AttemptID", attempts)
	}
}

func TestReceiverDoesNotRetryBrokerCanceledResponseAfterCallerCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	f := &fakeSource{}
	f.receiveFn = func(context.Context, int, string) ([]*Message, error) {
		cancel()
		return nil, &HTTPError{StatusCode: 499, cause: fmt.Errorf("mqlite: canceled: %w", context.Canceled)}
	}
	err := newReceiver(f, "q", nil).Run(ctx, func(context.Context, *Message) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want caller context cancellation", err)
	}
	f.mu.Lock()
	calls := f.calls
	f.mu.Unlock()
	if calls != 1 {
		t.Fatalf("Receive calls after caller cancellation = %d, want one", calls)
	}
}

func TestReceiverReportsMissingBrokerTimeOnceBeforeDispatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	f := &fakeSource{}
	deadline := time.Now().Add(time.Second)
	f.batch = []*Message{
		{SequenceNumber: 1, LockedUntil: deadline, localLeaseClockUnavailable: true, queue: "q", s: f},
		{SequenceNumber: 2, LockedUntil: deadline, localLeaseClockUnavailable: true, queue: "q", s: f},
	}
	var observed []error
	handlerCalls := 0
	err := newReceiver(f, "q", []ReceiverOption{
		WithConcurrency(2),
		WithErrorHandler(func(err error) {
			observed = append(observed, err)
			cancel()
		}),
	}).Run(ctx, func(context.Context, *Message) error {
		handlerCalls++
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want cancellation from diagnostic observer", err)
	}
	if handlerCalls != 0 {
		t.Fatalf("handler calls = %d, want zero after observer canceled Run", handlerCalls)
	}
	if len(observed) != 1 || observed[0].Error() != degradedClockError {
		t.Fatalf("observed diagnostics = %v, want one degraded-clock warning", observed)
	}
}

func TestReceiverReportsTimeHeadersStrippedFromRenewal(t *testing.T) {
	for _, tc := range []struct {
		name        string
		deadline    time.Duration
		responseRTT time.Duration
		wantHandler bool
	}{
		{name: "initial renewal", deadline: 100 * time.Millisecond, responseRTT: 60 * time.Millisecond},
		{name: "background renewal", deadline: 300 * time.Millisecond, wantHandler: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			f := &fakeSource{renewResponseRTT: tc.responseRTT, renewClockUnavailable: true}
			deadline := time.Now().Add(tc.deadline)
			f.batch = []*Message{{
				SequenceNumber: 1, LockedUntil: deadline, localLockedUntil: deadline,
				localLeaseResponseRTT: tc.responseRTT, queue: "q", s: f,
			}}
			f.renewDeadlineFn = func(context.Context) (time.Time, error) {
				return time.Now().Add(time.Second), nil
			}
			var observed []error
			handlerCalled := false
			err := newReceiver(f, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) {
				observed = append(observed, err)
				cancel()
			})}).Run(ctx, func(hctx context.Context, _ *Message) error {
				handlerCalled = true
				<-hctx.Done()
				return hctx.Err()
			})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Run = %v, want cancellation from diagnostic observer", err)
			}
			if handlerCalled != tc.wantHandler {
				t.Fatalf("handler called = %v, want %v", handlerCalled, tc.wantHandler)
			}
			if len(observed) != 1 || observed[0].Error() != degradedClockError {
				t.Fatalf("observed diagnostics = %v, want one degraded-clock warning", observed)
			}
		})
	}
}

func TestReceiverNeverTrustsAheadWireDeadlineWithoutBrokerTime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	f := &fakeSource{renewClockUnavailable: true}
	f.batch = []*Message{{
		SequenceNumber: 1,
		// Simulate a broker clock 100s ahead with a real 30s lease. Neither time
		// header survived the intermediary, so this apparent 130s horizon is not a
		// local ownership deadline.
		LockedUntil:                time.Now().Add(130 * time.Second),
		localLeaseClockUnavailable: true,
		queue:                      "q",
		s:                          f,
	}}
	var timesMu sync.Mutex
	var renewTimes []time.Time
	secondRenew := make(chan struct{})
	f.renewDeadlineFn = func(context.Context) (time.Time, error) {
		timesMu.Lock()
		renewTimes = append(renewTimes, time.Now())
		if len(renewTimes) == 2 {
			close(secondRenew)
		}
		timesMu.Unlock()
		return time.Now().Add(130 * time.Second), nil
	}
	f.completeFn = func(context.Context) error {
		cancel()
		return nil
	}
	var observed []error
	handlerCalls := 0
	err := newReceiver(f, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) {
		observed = append(observed, err)
	})}).Run(ctx, func(hctx context.Context, _ *Message) error {
		handlerCalls++
		select {
		case <-secondRenew:
			return nil
		case <-hctx.Done():
			return hctx.Err()
		}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want cancellation after completion", err)
	}
	if handlerCalls != 1 {
		t.Fatalf("handler calls = %d, want one", handlerCalls)
	}
	if len(observed) != 1 || observed[0].Error() != degradedClockError {
		t.Fatalf("observed diagnostics = %v, want one degraded-clock warning", observed)
	}
	timesMu.Lock()
	times := append([]time.Time(nil), renewTimes...)
	timesMu.Unlock()
	if len(times) < 2 {
		t.Fatalf("renew calls = %d, want initial confirmation plus degraded probe", len(times))
	}
	if elapsed := times[1].Sub(times[0]); elapsed < 150*time.Millisecond || elapsed > time.Second {
		t.Fatalf("degraded renewal interval = %s, want bounded cadence rather than apparent 130s lease", elapsed)
	}
	f.mu.Lock()
	completeCalls, abandonCalls := f.completeCalls, f.abandonCalls
	f.mu.Unlock()
	if completeCalls != 1 || abandonCalls != 0 {
		t.Fatalf("complete/abandon calls = %d/%d, want 1/0", completeCalls, abandonCalls)
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
	f.batch = []*Message{{SequenceNumber: 1, Body: []byte("x"), LockedUntil: time.Now().Add(30 * time.Second), queue: "q", s: f}}
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

// Settlement may overlap the next handler, but the Receiver must keep the
// combined handler-plus-settlement population bounded. With concurrency one,
// two blocked settlements fill the 2C total-inflight budget: the second handler
// must already have run, while a third Receive must not start.
func TestReceiverOverlapsSettlementWithNextHandlerWithinBound(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	releaseSettles := make(chan struct{})
	var cleanupOnce sync.Once
	cleanup := func() {
		cancel()
		cleanupOnce.Do(func() { close(releaseSettles) })
	}
	defer cleanup()

	f := &fakeSource{}
	thirdReceive := make(chan struct{})
	var thirdOnce sync.Once
	f.receiveFn = func(callCtx context.Context, call int, _ string) ([]*Message, error) {
		if call <= 2 {
			return []*Message{{SequenceNumber: int64(call), LockedUntil: time.Now().Add(30 * time.Second), queue: "q", s: f}}, nil
		}
		thirdOnce.Do(func() { close(thirdReceive) })
		<-callCtx.Done()
		return nil, callCtx.Err()
	}
	settlesStarted := make(chan struct{}, 2)
	f.completeFn = func(settleCtx context.Context) error {
		settlesStarted <- struct{}{}
		select {
		case <-releaseSettles:
			return nil
		case <-settleCtx.Done():
			return settleCtx.Err()
		}
	}
	handlersStarted := make(chan int64, 2)
	runDone := make(chan error, 1)
	go func() {
		runDone <- newReceiver(f, "q", []ReceiverOption{WithConcurrency(1)}).
			Run(ctx, func(_ context.Context, message *Message) error {
				handlersStarted <- message.SequenceNumber
				return nil
			})
	}()

	fail := func(format string, args ...any) {
		cleanup()
		select {
		case <-runDone:
		case <-time.After(time.Second):
		}
		t.Fatalf(format, args...)
	}
	waitHandler := func(want int64) {
		t.Helper()
		select {
		case got := <-handlersStarted:
			if got != want {
				fail("handler sequence = %d, want %d", got, want)
			}
		case <-time.After(time.Second):
			fail("handler %d did not start while the prior settlement was blocked", want)
		}
	}
	waitSettle := func(number int) {
		t.Helper()
		select {
		case <-settlesStarted:
		case <-time.After(time.Second):
			fail("settlement %d did not start", number)
		}
	}

	waitHandler(1)
	waitSettle(1)
	waitHandler(2)
	waitSettle(2)
	select {
	case <-thirdReceive:
		fail("a third Receive started while the 2C total-inflight budget was full")
	case <-time.After(150 * time.Millisecond):
	}

	// Race shutdown with the two settlements releasing total capacity. Even if
	// capacity wins reservePaired's select, Run must re-check cancellation before
	// issuing a third Receive, release both reservations, and join the workers.
	cancel()
	cleanupOnce.Do(func() { close(releaseSettles) })
	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not join settlements after release")
	}
	f.mu.Lock()
	receives := f.calls
	f.mu.Unlock()
	if receives != 2 {
		t.Fatalf("receive calls = %d, want two within the 2C bound", receives)
	}
}

// A transport can race cancellation and still decode a successful batch. Once
// Run is canceled, that batch must be dropped and both reservations released;
// user code must never be entered with a delivery claimed after shutdown.
func TestReceiverDropsBatchReturnedAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	receiveStarted := make(chan struct{})
	f := &fakeSource{}
	f.receiveFn = func(callCtx context.Context, _ int, _ string) ([]*Message, error) {
		close(receiveStarted)
		<-callCtx.Done()
		return []*Message{{SequenceNumber: 1, LockedUntil: time.Now().Add(30 * time.Second), queue: "q", s: f}}, nil
	}
	handlerCalled := make(chan struct{}, 1)
	runDone := make(chan error, 1)
	go func() {
		runDone <- newReceiver(f, "q", nil).Run(ctx, func(context.Context, *Message) error {
			handlerCalled <- struct{}{}
			return nil
		})
	}()
	<-receiveStarted
	cancel()
	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run hung while dropping a post-cancellation batch")
	}
	select {
	case <-handlerCalled:
		t.Fatal("handler ran for a batch returned after cancellation")
	default:
	}
}

// A small prefetch tightens the total pipeline bound rather than always using
// 2C. With C=2 and prefetch=1, three blocked settlements fill C+batch and a
// fourth Receive must wait.
func TestReceiverSmallPrefetchTightensTotalInflightBound(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	releaseSettles := make(chan struct{})
	var cleanupOnce sync.Once
	cleanup := func() {
		cancel()
		cleanupOnce.Do(func() { close(releaseSettles) })
	}
	defer cleanup()

	f := &fakeSource{}
	fourthReceive := make(chan struct{})
	var fourthOnce sync.Once
	f.receiveFn = func(callCtx context.Context, call int, _ string) ([]*Message, error) {
		if call <= 3 {
			return []*Message{{SequenceNumber: int64(call), LockedUntil: time.Now().Add(30 * time.Second), queue: "q", s: f}}, nil
		}
		fourthOnce.Do(func() { close(fourthReceive) })
		<-callCtx.Done()
		return nil, callCtx.Err()
	}
	settlesStarted := make(chan struct{}, 3)
	f.completeFn = func(settleCtx context.Context) error {
		settlesStarted <- struct{}{}
		select {
		case <-releaseSettles:
			return nil
		case <-settleCtx.Done():
			return settleCtx.Err()
		}
	}
	runDone := make(chan error, 1)
	go func() {
		runDone <- newReceiver(f, "q", []ReceiverOption{WithConcurrency(2), WithPrefetch(1)}).
			Run(ctx, func(context.Context, *Message) error { return nil })
	}()

	for settlement := 1; settlement <= 3; settlement++ {
		select {
		case <-settlesStarted:
		case <-time.After(time.Second):
			cleanup()
			t.Fatalf("settlement %d did not start", settlement)
		}
	}
	select {
	case <-fourthReceive:
		cleanup()
		t.Fatal("a fourth Receive exceeded the C+prefetch total-inflight bound")
	case <-time.After(150 * time.Millisecond):
	}
	cancel()
	cleanupOnce.Do(func() { close(releaseSettles) })
	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not join the bounded settlement pipeline")
	}
	f.mu.Lock()
	receives := f.calls
	f.mu.Unlock()
	if receives != 3 {
		t.Fatalf("receive calls = %d, want C+prefetch = 3", receives)
	}
}

func TestReceiverGoexitJoinsAutoRenewBeforeReleasingPipelineSlots(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	renewStarted := make(chan struct{})
	renewCanceled := make(chan struct{})
	allowRenewExit := make(chan struct{})
	renewExited := make(chan struct{})
	var renewOnce sync.Once
	f := &fakeSource{}
	f.receiveFn = func(callCtx context.Context, call int, _ string) ([]*Message, error) {
		switch call {
		case 1:
			return []*Message{{SequenceNumber: 1, LockedUntil: time.Now().Add(40 * time.Millisecond), queue: "q", s: f}}, nil
		case 2:
			return []*Message{{SequenceNumber: 2, LockedUntil: time.Now().Add(time.Hour), queue: "q", s: f}}, nil
		default:
			<-callCtx.Done()
			return nil, callCtx.Err()
		}
	}
	f.renewDeadlineFn = func(renewCtx context.Context) (time.Time, error) {
		renewOnce.Do(func() { close(renewStarted) })
		<-renewCtx.Done()
		close(renewCanceled)
		<-allowRenewExit
		close(renewExited)
		return time.Time{}, renewCtx.Err()
	}
	secondStarted := make(chan struct{})
	runDone := make(chan error, 1)
	go func() {
		runDone <- newReceiver(f, "q", []ReceiverOption{WithConcurrency(1), WithAutoRenew()}).
			Run(ctx, func(_ context.Context, message *Message) error {
				if message.SequenceNumber == 1 {
					select {
					case <-renewStarted:
					case <-time.After(2 * time.Second):
						cancel()
						return errors.New("renew did not start")
					}
					runtime.Goexit()
				}
				close(secondStarted)
				cancel()
				return nil
			})
	}()
	select {
	case <-renewCanceled:
	case <-time.After(2 * time.Second):
		cancel()
		close(allowRenewExit)
		<-runDone
		t.Fatal("handler Goexit did not cancel auto-renew")
	}
	select {
	case <-secondStarted:
		cancel()
		close(allowRenewExit)
		<-runDone
		t.Fatal("handler Goexit released pipeline capacity before auto-renew joined")
	case <-time.After(100 * time.Millisecond):
	}
	close(allowRenewExit)
	select {
	case <-secondStarted:
	case <-time.After(2 * time.Second):
		cancel()
		<-runDone
		t.Fatal("pipeline did not continue after auto-renew exited")
	}
	if err := <-runDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	select {
	case <-renewExited:
	default:
		t.Fatal("Run returned before Goexit auto-renew joined")
	}
	f.mu.Lock()
	renews, completes := f.renewCalls, f.completeCalls
	f.mu.Unlock()
	if renews != 1 || completes != 1 {
		t.Fatalf("renew/complete calls = %d/%d, want 1/1", renews, completes)
	}
}

// MQLITE-77: an expected ErrLockLost on settle (the message was redelivered) is NOT fatal —
// the loop keeps consuming and Run ends only on ctx cancellation — but it is still observed.
func TestReceiverLockLostSettleIsNotFatal(t *testing.T) {
	f := &fakeSource{completeErr: ErrLockLost}
	f.batch = []*Message{{SequenceNumber: 1, Body: []byte("x"), LockedUntil: time.Now().Add(30 * time.Second), queue: "q", s: f}}
	var mu sync.Mutex
	var observed error
	r := newReceiver(f, "q", []ReceiverOption{WithErrorHandler(func(e error) {
		mu.Lock()
		observed = e
		mu.Unlock()
	})})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := r.Run(ctx, func(context.Context, *Message) error { return nil })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ErrLockLost settle must be non-fatal; Run should end on ctx, got %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !errors.Is(observed, ErrLockLost) {
		t.Fatalf("observer should still see the lost-lock settle error, got %v", observed)
	}
	if f.completeCalls != 1 {
		t.Fatalf("lock_lost settlement calls = %d, want one without retry", f.completeCalls)
	}
}

// A clean shutdown (ctx cancelled) still ends Run with ctx.Err(), never a spurious fatal —
// the contract this change is careful to preserve.
func TestReceiverGracefulShutdownReturnsCtxErr(t *testing.T) {
	f := &fakeSource{}
	f.batch = []*Message{{SequenceNumber: 1, Body: []byte("x"), LockedUntil: time.Now().Add(30 * time.Second), queue: "q", s: f}}
	r := newReceiver(f, "q", nil)
	ctx, cancel := context.WithCancel(context.Background())
	handled := make(chan struct{}, 1)
	errc := make(chan error, 1)
	go func() {
		errc <- r.Run(ctx, func(context.Context, *Message) error {
			select {
			case handled <- struct{}{}:
			default:
			}
			return nil
		})
	}()
	<-handled // a message was processed
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("graceful shutdown must return context.Canceled, got %v", err)
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

func TestReceiverBackoffGrowsCapsAndJitters(t *testing.T) {
	want := []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second}
	for step, expected := range want {
		if got := exponentialBackoff(receiveBackoffBase, receiveBackoffMax, step); got != expected {
			t.Fatalf("backoff step %d = %s, want %s", step, got, expected)
		}
	}
	for i := 0; i < 32; i++ {
		got := jitterDuration(time.Second)
		if got < 800*time.Millisecond || got > 1200*time.Millisecond {
			t.Fatalf("jitter = %s, want within 800ms..1.2s", got)
		}
	}
}

func TestAutoRenewDelayUsesOneThirdAndStaysInsideShortLease(t *testing.T) {
	for _, tc := range []struct {
		name      string
		remaining time.Duration
		want      time.Duration
		tolerance time.Duration
	}{
		{name: "just over one second", remaining: 1010 * time.Millisecond, want: 337 * time.Millisecond, tolerance: 30 * time.Millisecond},
		{name: "minimum cadence", remaining: 120 * time.Millisecond, want: 50 * time.Millisecond, tolerance: 5 * time.Millisecond},
		{name: "near boundary", remaining: 40 * time.Millisecond, want: 20 * time.Millisecond, tolerance: 10 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := autoRenewDelay(time.Now().Add(tc.remaining))
			if got < tc.want-tc.tolerance || got > tc.want+tc.tolerance {
				t.Fatalf("autoRenewDelay(%s) = %s, want %s ± %s", tc.remaining, got, tc.want, tc.tolerance)
			}
		})
	}
}

func TestRenewalDelayDoesNotStartPastWatchdogHeadroom(t *testing.T) {
	state := renewalState{deadline: time.Now().Add(20 * time.Millisecond)}
	horizon := time.Until(state.deadline.Add(-replayLeaseSafety))
	delay := renewalDelay(state)
	if delay != 0 {
		t.Fatalf("20ms lease renewal delay = %s with watchdog horizon %s, want immediate attempt", delay, horizon)
	}
	state.deadlineUntrusted = true
	if delay := renewalDelay(state); delay != renewUntrustedDelay {
		t.Fatalf("untrusted renewal delay = %s, want %s", delay, renewUntrustedDelay)
	}
}

func TestRenewalScheduleReservesObservedRoundTripHeadroom(t *testing.T) {
	state := renewalState{
		deadline:    time.Now().Add(600 * time.Millisecond),
		responseRTT: 400 * time.Millisecond,
	}
	if !needsInitialRenewalGate(state) {
		t.Fatal("tight 600ms horizon with 400ms RTT must renew before handler dispatch")
	}
	if delay := renewalDelay(state); delay != 0 {
		t.Fatalf("tight high-RTT renewal delay = %s, want immediate", delay)
	}

	state = renewalState{
		deadline:    time.Now().Add(time.Second),
		responseRTT: 100 * time.Millisecond,
	}
	if needsInitialRenewalGate(state) {
		t.Fatal("one-second horizon with 100ms RTT has enough dispatch headroom")
	}
	delay := renewalDelay(state)
	if delay < 300*time.Millisecond || delay > 350*time.Millisecond {
		t.Fatalf("comfortable high-RTT renewal delay = %s, want normal one-third cadence", delay)
	}
}

func TestDateShortLowerBoundUsesDegradedWatchdog(t *testing.T) {
	state := newRenewalState(time.Now().Add(500*time.Millisecond), 150*time.Millisecond, true, false)
	if !state.deadlineUntrusted || !needsInitialRenewalGate(state) {
		t.Fatalf("coarse short state = %+v, want synchronous confirmation in degraded mode", state)
	}
	ctx, cancel := initialRenewalAttemptContext(context.Background(), state)
	defer cancel()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("degraded initial renewal has no watchdog")
	}
	remaining := time.Until(deadline)
	if remaining < renewDefaultAttemptTimeout-100*time.Millisecond || remaining > renewDefaultAttemptTimeout+100*time.Millisecond {
		t.Fatalf("degraded initial watchdog = %s, want %s", remaining, renewDefaultAttemptTimeout)
	}
}

func TestUnavailableClockDoesNotMisclassifyShortLeaseAsExact(t *testing.T) {
	state := newRenewalState(time.Now().Add(40*time.Millisecond), 2*time.Millisecond, false, true)
	if state.leaseTooShort {
		t.Fatal("a no-header absolute timestamp must not be treated as an exact pathological lease")
	}
}

func TestExactLeaseInsideSafetyWindowRemainsTooShort(t *testing.T) {
	for _, tc := range []struct {
		name      string
		remaining time.Duration
	}{
		{name: "one millisecond", remaining: time.Millisecond},
		{name: "already expired", remaining: -time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deadline := time.Now().Add(tc.remaining)
			state := newRenewalState(deadline, time.Millisecond, false, false)
			if !state.leaseTooShort {
				t.Fatalf("new exact state = %+v, want too-short even inside the safety window", state)
			}

			state = renewalState{deadlineUntrusted: true}.acceptRenewal(renewalLease{
				deadline: deadline, responseRTT: time.Millisecond,
			}, time.Now())
			if !state.leaseTooShort {
				t.Fatalf("accepted exact state = %+v, want too-short even inside the safety window", state)
			}
		})
	}
}

func TestInitialRenewalAttemptContextCapsTrustedLease(t *testing.T) {
	ctx, cancel := initialRenewalAttemptContext(context.Background(), renewalState{
		deadline: time.Now().Add(time.Hour),
	})
	defer cancel()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("initial renewal context has no deadline")
	}
	remaining := time.Until(deadline)
	if remaining < renewMaxInitialAttemptTimeout-100*time.Millisecond || remaining > renewMaxInitialAttemptTimeout+100*time.Millisecond {
		t.Fatalf("initial renewal watchdog = %s, want cap %s", remaining, renewMaxInitialAttemptTimeout)
	}
}

func TestClientMapsBrokerDeadlineOntoLocalMonotonicClock(t *testing.T) {
	now := time.Now()
	serverNow := now.Add(-2 * time.Second)
	header := make(http.Header)
	header.Set(wire.ServerTimeHeader, strconv.FormatInt(serverNow.UnixMilli(), 10))
	observation := observeServerTime(header, now.Add(-150*time.Millisecond), now)
	deadline, ok := observation.localLeaseDeadline(serverNow.Add(time.Second))
	if !ok {
		t.Fatal("server-time observation was not retained")
	}
	remaining := time.Until(deadline)
	if remaining < 800*time.Millisecond || remaining > 900*time.Millisecond {
		t.Fatalf("mapped remaining lease = %s, want conservative ~849ms", remaining)
	}
}

func TestClientRejectsPathologicallyShortConfirmedLease(t *testing.T) {
	now := time.Now()
	serverNow := now.Add(-2 * time.Second)
	header := make(http.Header)
	header.Set(wire.ServerTimeHeader, strconv.FormatInt(serverNow.UnixMilli(), 10))
	observation := observeServerTime(header, now.Add(-2*time.Millisecond), now)
	deadline, ok := observation.localLeaseDeadline(serverNow.Add(40 * time.Millisecond))
	if !ok {
		t.Fatal("server-time observation was not retained")
	}
	remaining := time.Until(deadline)
	if remaining < 30*time.Millisecond || remaining > 40*time.Millisecond {
		t.Fatalf("mapped short lease = %s, want about 37ms", remaining)
	}
	state := renewalState{deadlineUntrusted: true}.acceptRenewal(renewalLease{deadline: deadline, responseRTT: observation.responseRTT}, time.Now())
	if state.deadlineUntrusted {
		t.Fatal("exact short broker lease must not be confused with coarse/unavailable clock mode")
	}
	if !state.leaseTooShort {
		t.Fatal("confirmed lease below the minimum interval must fail closed instead of hot-looping")
	}
	if delay := renewalDelay(state); delay != 0 {
		t.Fatalf("pathological short-lease delay = %s, want immediate fail-closed decision", delay)
	}
}

func TestClientClockSkewDoesNotManufactureTinyLease(t *testing.T) {
	now := time.Now()
	serverNow := now.Add(-35 * time.Second)
	for _, tc := range []struct {
		name   string
		header http.Header
	}{
		{name: "exact", header: http.Header{wire.ServerTimeHeader: []string{strconv.FormatInt(serverNow.UnixMilli(), 10)}}},
		{name: "date fallback", header: http.Header{"Date": []string{serverNow.UTC().Format(http.TimeFormat)}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observation := observeServerTime(tc.header, now.Add(-20*time.Millisecond), now)
			deadline, ok := observation.localLeaseDeadline(serverNow.Add(30 * time.Second))
			if !ok {
				t.Fatal("server-time observation was not retained")
			}
			remaining := time.Until(deadline)
			if remaining < 28*time.Second || remaining > 30*time.Second {
				t.Fatalf("mapped 30s lease = %s, want a normal lease despite 35s clock skew", remaining)
			}
			state := newRenewalState(deadline, observation.responseRTT, observation.coarseClock, false)
			if state.deadlineUntrusted || renewalDelay(state) < 9*time.Second {
				t.Fatalf("mapped state = %+v delay=%s, want trusted non-spinning cadence", state, renewalDelay(state))
			}
		})
	}
}

func TestClientLongPollNeverExtendsLeaseFromLocalWallClock(t *testing.T) {
	// Date is only precise to a second. With a long request RTT, the local clock
	// can fall inside that large uncertainty interval even when it is behind the
	// broker. Treating local wall time as authoritative would then manufacture a
	// longer lease. The conservative server-time upper bound must instead make
	// this deliberately ambiguous lease unusable and force a broker Renew fence.
	now := time.Now()
	serverFloor := now.Truncate(time.Second)
	header := make(http.Header)
	header.Set("Date", serverFloor.UTC().Format(http.TimeFormat))
	observation := observeServerTime(header, now.Add(-10*time.Second), now)
	deadline, ok := observation.localLeaseDeadline(serverFloor.Add(2 * time.Second))
	if !ok {
		t.Fatal("Date observation was not retained")
	}
	if deadline.After(time.Now()) {
		t.Fatalf("ambiguous long-poll deadline = %s, must not be extended past now", deadline)
	}
}

func TestClientLongPollObservationDoesNotContaminateShortRenewal(t *testing.T) {
	now := time.Now()
	serverNow := now.Add(-2 * time.Second)
	header := make(http.Header)
	header.Set(wire.ServerTimeHeader, strconv.FormatInt(serverNow.UnixMilli(), 10))

	longPoll := observeServerTime(header, now.Add(-10*time.Second), now)
	if deadline, ok := longPoll.localLeaseDeadline(serverNow.Add(time.Second)); !ok || deadline.After(now) {
		t.Fatalf("long-poll deadline = %s, %v; want unusable", deadline, ok)
	}

	// A Renew response carries its own short-RTT sample. It must recover a normal
	// lease immediately rather than inherit the long poll's transport padding.
	shortRenew := observeServerTime(header, now.Add(-20*time.Millisecond), now)
	deadline, ok := shortRenew.localLeaseDeadline(serverNow.Add(time.Second))
	if !ok {
		t.Fatal("short-renew observation was not retained")
	}
	remaining := time.Until(deadline)
	if remaining < 900*time.Millisecond || remaining > time.Second {
		t.Fatalf("short-renew remaining lease = %s, want about 979ms", remaining)
	}
}

func TestReceiverRenewStartsBeforeWatchdog(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	f := &fakeSource{}
	// Keep the behavioral test comfortably above coarse Windows race-detector
	// scheduling granularity. TestRenewalDelayLeavesWatchdogHeadroom pins the
	// exact 20ms arithmetic without relying on sub-10ms goroutine scheduling.
	f.batch = []*Message{{SequenceNumber: 1, LockedUntil: time.Now().Add(200 * time.Millisecond), queue: "q", s: f}}
	renewed := make(chan struct{})
	var once sync.Once
	f.renewDeadlineFn = func(context.Context) (time.Time, error) {
		once.Do(func() { close(renewed) })
		return time.Now().Add(200 * time.Millisecond), nil
	}
	f.abandonFn = func(context.Context) error {
		cancel()
		return nil
	}
	var observed []error
	err := newReceiver(f, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) {
		observed = append(observed, err)
		cancel()
	})}).Run(ctx, func(hctx context.Context, _ *Message) error {
		select {
		case <-renewed:
			return errors.New("handler failed after renewal")
		case <-hctx.Done():
			return hctx.Err()
		}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled after Abandon", err)
	}
	if len(observed) != 0 {
		t.Fatalf("observed errors = %v, want none", observed)
	}
	f.mu.Lock()
	renews := f.renewCalls
	completeCalls, abandonCalls := f.completeCalls, f.abandonCalls
	f.mu.Unlock()
	if renews != 1 || completeCalls != 0 || abandonCalls != 1 {
		t.Fatalf("renew/complete/abandon calls = %d/%d/%d, want 1/0/1", renews, completeCalls, abandonCalls)
	}
}

func TestReceiverHighRTTRenewalCompletesBeforeConservativeHorizon(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	f := &fakeSource{renewResponseRTT: 120 * time.Millisecond}
	deadline := time.Now().Add(180 * time.Millisecond)
	f.batch = []*Message{{
		SequenceNumber: 1, LockedUntil: deadline, localLockedUntil: deadline,
		localLeaseResponseRTT: 120 * time.Millisecond, queue: "q", s: f,
	}}
	f.renewDeadlineFn = func(renewCtx context.Context) (time.Time, error) {
		timer := time.NewTimer(120 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-renewCtx.Done():
			return time.Time{}, renewCtx.Err()
		case <-timer.C:
			return time.Now().Add(180 * time.Millisecond), nil
		}
	}
	f.completeFn = func(context.Context) error {
		cancel()
		return nil
	}
	handlerCompleted := false
	var observed []error
	err := newReceiver(f, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) {
		observed = append(observed, err)
	})}).Run(ctx, func(hctx context.Context, _ *Message) error {
		timer := time.NewTimer(500 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-hctx.Done():
			return hctx.Err()
		case <-timer.C:
			handlerCompleted = true
			return nil
		}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want cancellation after Complete", err)
	}
	if !handlerCompleted {
		t.Fatal("high-RTT conservative lease canceled the handler")
	}
	if len(observed) != 0 {
		t.Fatalf("observed errors = %v, want none", observed)
	}
	f.mu.Lock()
	renews, completes := f.renewCalls, f.completeCalls
	f.mu.Unlock()
	if renews < 3 || completes != 1 {
		t.Fatalf("renew/complete calls = %d/%d, want at least 3/1", renews, completes)
	}
}

func TestReceiverHighRTTInitialRenewalHonorsConservativeHorizon(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	f := &fakeSource{renewResponseRTT: 120 * time.Millisecond}
	deadline := time.Now().Add(180 * time.Millisecond)
	f.batch = []*Message{{
		SequenceNumber: 1, LockedUntil: deadline, localLockedUntil: deadline,
		localLeaseResponseRTT: 120 * time.Millisecond, queue: "q", s: f,
	}}
	f.renewDeadlineFn = func(renewCtx context.Context) (time.Time, error) {
		<-renewCtx.Done()
		return time.Time{}, renewCtx.Err()
	}
	handlerCalls := 0
	var observed []error
	started := time.Now()
	err := newReceiver(f, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) {
		observed = append(observed, err)
		if errors.Is(err, ErrLockLost) {
			cancel()
		}
	})}).Run(ctx, func(context.Context, *Message) error {
		handlerCalls++
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want cancellation after watchdog", err)
	}
	elapsed := time.Since(started)
	if elapsed < 120*time.Millisecond || elapsed > 750*time.Millisecond {
		t.Fatalf("trusted high-RTT watchdog fired after %s, want the 180ms lease horizon", elapsed)
	}
	if handlerCalls != 0 {
		t.Fatalf("handler calls = %d, want zero while initial renewal is unconfirmed", handlerCalls)
	}
	if len(observed) != 1 || !errors.Is(observed[0], ErrLockLost) {
		t.Fatalf("observed errors = %v, want one ErrLockLost", observed)
	}
}

func TestReceiverLongPollLowerBoundCanBeRenewedBeforeDispatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	f := &fakeSource{renewResponseRTT: 20 * time.Millisecond}
	// The full eight-second long poll is conservatively charged as transport
	// uncertainty, so a fresh five-second broker lease has an expired local lower
	// bound. This is not proof that the broker token expired: one bounded Renew may
	// establish a useful horizon before any user work is dispatched.
	f.batch = []*Message{{
		SequenceNumber:        1,
		LockedUntil:           time.Now().Add(5 * time.Second),
		localLockedUntil:      time.Now().Add(-3 * time.Second),
		localLeaseResponseRTT: 8 * time.Second,
		queue:                 "q", s: f,
	}}
	f.renewDeadlineFn = func(renewCtx context.Context) (time.Time, error) {
		timer := time.NewTimer(20 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
			return time.Now().Add(5 * time.Second), nil
		case <-renewCtx.Done():
			return time.Time{}, renewCtx.Err()
		}
	}
	f.completeFn = func(context.Context) error {
		cancel()
		return nil
	}
	handlerCalls := 0
	var observed []error
	err := newReceiver(f, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) {
		observed = append(observed, err)
	})}).Run(ctx, func(context.Context, *Message) error {
		handlerCalls++
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want cancellation after completion", err)
	}
	if handlerCalls != 1 || len(observed) != 0 {
		t.Fatalf("handler calls/diagnostics = %d/%v, want 1/none", handlerCalls, observed)
	}
	f.mu.Lock()
	renews, completes, abandons := f.renewCalls, f.completeCalls, f.abandonCalls
	f.mu.Unlock()
	if renews != 1 || completes != 1 || abandons != 0 {
		t.Fatalf("renew/complete/abandon = %d/%d/%d, want 1/1/0", renews, completes, abandons)
	}
}

func TestReceiverFailsClosedBeforeDispatchForPathologicalLease(t *testing.T) {
	for _, tc := range []struct {
		name      string
		remaining time.Duration
	}{
		{name: "forty milliseconds", remaining: 40 * time.Millisecond},
		{name: "one millisecond", remaining: time.Millisecond},
		{name: "already expired", remaining: -time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			f := &fakeSource{renewResponseRTT: time.Millisecond}
			deadline := time.Now().Add(tc.remaining)
			f.batch = []*Message{{
				SequenceNumber: 1, LockedUntil: deadline, localLockedUntil: deadline,
				localLeaseResponseRTT: time.Millisecond, queue: "q", s: f,
			}}
			f.renewDeadlineFn = func(context.Context) (time.Time, error) {
				return time.Now().Add(tc.remaining), nil
			}
			handlerCalls := 0
			var observed []error
			err := newReceiver(f, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) {
				observed = append(observed, err)
				if errors.Is(err, ErrLockLost) {
					cancel()
				}
			})}).Run(ctx, func(context.Context, *Message) error {
				handlerCalls++
				return nil
			})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Run = %v, want cancellation after deterministic lock-loss diagnostic", err)
			}
			if handlerCalls != 0 {
				t.Fatalf("handler calls = %d, want zero for an unsafe confirmed lease", handlerCalls)
			}
			if len(observed) != 1 || !errors.Is(observed[0], ErrLockLost) {
				t.Fatalf("observed errors = %v, want one ErrLockLost", observed)
			}
			f.mu.Lock()
			renews, completes, abandons := f.renewCalls, f.completeCalls, f.abandonCalls
			f.mu.Unlock()
			if renews != 1 || completes != 0 || abandons != 0 {
				t.Fatalf("renew/complete/abandon calls = %d/%d/%d, want 1/0/0", renews, completes, abandons)
			}
		})
	}
}

func TestReceiverLegacyRenewalFailsClosedForPathologicalExactLease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	f := &fakeSource{}
	deadline := time.Now().Add(40 * time.Millisecond)
	f.batch = []*Message{{
		SequenceNumber: 1, LockedUntil: deadline, localLockedUntil: deadline,
		queue: "q", s: f,
	}}
	f.renewDeadlineFn = func(context.Context) (time.Time, error) {
		return time.Time{}, ErrUnsupported
	}
	f.renewFn = func(context.Context) error { return nil }
	handlerCalls := 0
	var observed []error
	err := newReceiver(f, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) {
		observed = append(observed, err)
		if errors.Is(err, ErrLockLost) {
			cancel()
		}
	})}).Run(ctx, func(context.Context, *Message) error {
		handlerCalls++
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want cancellation after lock-loss diagnostic", err)
	}
	if handlerCalls != 0 {
		t.Fatalf("handler calls = %d, want zero for a legacy short lease", handlerCalls)
	}
	if len(observed) != 1 || !errors.Is(observed[0], ErrLockLost) {
		t.Fatalf("observed errors = %v, want one ErrLockLost", observed)
	}
	f.mu.Lock()
	renews, deadlineRenews := f.renewCalls, f.deadlineRenewCalls
	completeCalls, abandonCalls := f.completeCalls, f.abandonCalls
	f.mu.Unlock()
	if renews != 2 || deadlineRenews != 1 || completeCalls != 0 || abandonCalls != 0 {
		t.Fatalf("renew/deadline/complete/abandon = %d/%d/%d/%d, want 2/1/0/0", renews, deadlineRenews, completeCalls, abandonCalls)
	}
}

func TestReceiverMissingRenewalClockDoesNotEraseExactShortFence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	f := &fakeSource{renewClockUnavailable: true}
	deadline := time.Now().Add(40 * time.Millisecond)
	f.batch = []*Message{{
		SequenceNumber: 1, LockedUntil: deadline, localLockedUntil: deadline,
		queue: "q", s: f,
	}}
	// The raw wall-clock value looks generous, but the response no longer has a
	// broker-time observation. It cannot erase the exact short fence learned from
	// the Receive response.
	f.renewDeadlineFn = func(context.Context) (time.Time, error) {
		return time.Now().Add(130 * time.Second), nil
	}
	handlerCalls := 0
	var observed []error
	err := newReceiver(f, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) {
		observed = append(observed, err)
		if errors.Is(err, ErrLockLost) {
			cancel()
		}
	})}).Run(ctx, func(context.Context, *Message) error {
		handlerCalls++
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want cancellation after lock-loss diagnostic", err)
	}
	if handlerCalls != 0 {
		t.Fatalf("handler calls = %d, want zero", handlerCalls)
	}
	if len(observed) != 2 || observed[0].Error() != degradedClockError || !errors.Is(observed[1], ErrLockLost) {
		t.Fatalf("observed errors = %v, want degraded-clock warning then ErrLockLost", observed)
	}
	f.mu.Lock()
	renews, completes, abandons := f.renewCalls, f.completeCalls, f.abandonCalls
	f.mu.Unlock()
	if renews != 1 || completes != 0 || abandons != 0 {
		t.Fatalf("renew/complete/abandon = %d/%d/%d, want 1/0/0", renews, completes, abandons)
	}
}

func TestManagedRetryClassifiesHTTPStatus(t *testing.T) {
	for _, tc := range []struct {
		status int
		want   bool
	}{
		{307, false},
		{400, false},
		{408, true},
		{429, true},
		{499, true},
		{500, true},
		{503, true},
	} {
		err := &HTTPError{StatusCode: tc.status}
		if got := retryableManagedError(err); got != tc.want {
			t.Errorf("HTTP %d retryable = %v, want %v", tc.status, got, tc.want)
		}
	}
	if !retryableManagedError(errors.New("connection reset")) {
		t.Error("transport failure must remain retryable")
	}
	if !retryableManagedError(context.DeadlineExceeded) {
		t.Error("request timeout must remain retryable while the controlling context is live")
	}
	if retryableManagedError(context.Canceled) {
		t.Error("a local context cancellation without an HTTP response must not be retried")
	}
	if err := (&HTTPError{StatusCode: 499, cause: context.Canceled}); !retryableManagedError(err) {
		t.Error("an HTTP 499 response must be retryable even when its body reports cancellation")
	}
	if err := (&HTTPError{StatusCode: 503, cause: ErrPermissionDenied}); !retryableManagedError(err) {
		t.Error("HTTP 503 must be retryable even when an untrusted body maps to permission_denied")
	}
	if err := (&HTTPError{StatusCode: 400, cause: errors.New("mqlite: internal")}); retryableManagedError(err) {
		t.Error("HTTP 400 must not be retryable even when its body claims an internal error")
	}
}

func TestSettlementExhaustionPreservesHTTPStatusClassification(t *testing.T) {
	r := newReceiver(nil, "q", nil)
	for _, tc := range []struct {
		name      string
		err       error
		wantFatal bool
	}{
		{name: "503 overrides permission body", err: &HTTPError{StatusCode: 503, cause: ErrPermissionDenied}},
		{name: "403 permission is fatal", err: &HTTPError{StatusCode: 403, cause: ErrPermissionDenied}, wantFatal: true},
		{name: "embedded permission is fatal", err: ErrPermissionDenied, wantFatal: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fatal := false
			r.settleErr(tc.err, func() bool { return false }, func(error, bool) { fatal = true })
			if fatal != tc.wantFatal {
				t.Fatalf("fatal = %v, want %v for %v", fatal, tc.wantFatal, tc.err)
			}
		})
	}
}

func TestReceiverReturnsNonRetryableHTTPError(t *testing.T) {
	httpErr := &HTTPError{StatusCode: 307}
	f := &fakeSource{recvErr: httpErr}
	var observed error
	err := newReceiver(f, "q", []ReceiverOption{WithErrorHandler(func(err error) { observed = err })}).
		Run(context.Background(), func(context.Context, *Message) error { return nil })
	if !errors.Is(err, httpErr) || !errors.Is(observed, httpErr) {
		t.Fatalf("Run/observed = %v/%v, want HTTP redirect error", err, observed)
	}
	f.mu.Lock()
	calls := f.calls
	f.mu.Unlock()
	if calls != 1 {
		t.Fatalf("non-retryable HTTP receive calls = %d, want one", calls)
	}
}

func TestReceiverCachesUnsupportedDeadlineRenewal(t *testing.T) {
	f := &fakeSource{}
	f.renewDeadlineFn = func(context.Context) (time.Time, error) {
		return time.Time{}, ErrUnsupported
	}
	m := &Message{SequenceNumber: 1, queue: "q", lockToken: "token", s: f}
	r := newReceiver(f, "q", nil)
	for i := 0; i < 2; i++ {
		if _, err := r.renewMessage(context.Background(), m); err != nil {
			t.Fatalf("renew %d: %v", i+1, err)
		}
	}
	f.mu.Lock()
	deadlineCalls, allCalls := f.deadlineRenewCalls, f.renewCalls
	f.mu.Unlock()
	if deadlineCalls != 1 || allCalls != 3 {
		t.Fatalf("deadline/all renew calls = %d/%d, want 1/3", deadlineCalls, allCalls)
	}
}

func TestEmbeddedReceiverMapsInjectedClockLeaseToLocalTimeline(t *testing.T) {
	const brokerNow = int64(1_600_000_000_000)
	ctx := context.Background()
	embedded, err := OpenEmbedded(ctx, ":memory:", WithClock(func() int64 { return brokerNow }), WithoutBackground())
	if err != nil {
		t.Fatal(err)
	}
	defer embedded.Close()
	if err := embedded.CreateQueue(ctx, "q", QueueConfig{LockDuration: 2 * time.Second}); err != nil {
		t.Fatal(err)
	}
	if _, err := embedded.SendOne(ctx, "q", OutMessage{Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	messages, err := embedded.Receive(ctx, "q")
	if err != nil || len(messages) != 1 {
		t.Fatalf("Receive = %d messages, %v", len(messages), err)
	}
	message := messages[0]
	if want := time.UnixMilli(brokerNow + 2_000); !message.LockedUntil.Equal(want) {
		t.Errorf("public LockedUntil = %v, want broker epoch %v", message.LockedUntil, want)
	}
	remaining := time.Until(message.localLockedUntil)
	if remaining < 1500*time.Millisecond || remaining > 2100*time.Millisecond {
		t.Errorf("local lease horizon = %s, want about 2s", remaining)
	}

	var observed []error
	handlerCalls := 0
	receiver := embedded.Receiver("q", WithAutoRenew(), WithErrorHandler(func(err error) {
		observed = append(observed, err)
	}))
	receiver.process(ctx, message, func(context.Context, *Message) error {
		handlerCalls++
		return nil
	}, func() {}, func() bool { return false }, func(err error, _ bool) {
		t.Errorf("Receiver stopped: %v", err)
	}, false)
	if handlerCalls != 1 {
		t.Errorf("handler calls = %d, want one", handlerCalls)
	}
	if len(observed) != 0 {
		t.Errorf("Receiver diagnostics = %v, want none", observed)
	}
}

func TestEmbeddedReceiverUsesCommittedRenewDeadline(t *testing.T) {
	now := int64(1_800_000_000_000)
	ctx := context.Background()
	embedded, err := OpenEmbedded(ctx, ":memory:", WithClock(func() int64 { return now }), WithoutBackground())
	if err != nil {
		t.Fatal(err)
	}
	defer embedded.Close()
	if err := embedded.CreateQueue(ctx, "q", QueueConfig{LockDuration: 2 * time.Second}); err != nil {
		t.Fatal(err)
	}
	if _, err := embedded.SendOne(ctx, "q", OutMessage{Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	messages, err := embedded.Receive(ctx, "q")
	if err != nil || len(messages) != 1 {
		t.Fatalf("Receive = %d messages, %v", len(messages), err)
	}
	original := messages[0].LockedUntil
	now += 500
	receiver := embedded.Receiver("q")
	lease, err := receiver.renewMessage(ctx, messages[0])
	if err != nil {
		t.Fatal(err)
	}
	remaining := time.Until(lease.deadline)
	if remaining < 1500*time.Millisecond || remaining > 2100*time.Millisecond {
		t.Fatalf("committed local deadline = %v (%s remaining), want about 2s", lease.deadline, remaining)
	}
	if !messages[0].LockedUntil.Equal(original) {
		t.Fatalf("renewal mutated public LockedUntil = %v, want original broker epoch %v", messages[0].LockedUntil, original)
	}
	if err := messages[0].Complete(ctx); err != nil {
		t.Fatal(err)
	}
}

// Settlement receipts make an exact Complete or Abandon replay safe. The managed
// Receiver must replay a transiently failed settlement before reporting it.
func TestReceiverRetriesIdempotentSettlement(t *testing.T) {
	for _, operation := range []string{"complete", "abandon"} {
		t.Run(operation, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			f := &fakeSource{}
			f.batch = []*Message{{SequenceNumber: 1, LockedUntil: time.Now().Add(30 * time.Second), queue: "q", s: f}}
			var attempts int
			settle := func(context.Context) error {
				attempts++
				if attempts == 1 {
					return context.DeadlineExceeded
				}
				cancel()
				return nil
			}
			if operation == "complete" {
				f.completeFn = settle
			} else {
				f.abandonFn = settle
			}
			var observed int
			r := newReceiver(f, "q", []ReceiverOption{WithErrorHandler(func(error) {
				observed++
				cancel() // makes the old one-shot implementation fail promptly
			})})
			err := r.Run(ctx, func(context.Context, *Message) error {
				if operation == "abandon" {
					return errors.New("handler failed")
				}
				return nil
			})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Run = %v, want context.Canceled after successful replay", err)
			}
			if attempts != 2 || observed != 0 {
				t.Fatalf("%s attempts=%d observed=%d, want 2/0", operation, attempts, observed)
			}
		})
	}
}

// A short outage must not permanently disable auto-renewal while the original
// lease still leaves time for a safe retry.
func TestReceiverRenewRecoversWithinLease(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		firstErr error
	}{
		{name: "transport timeout", firstErr: context.DeadlineExceeded},
		{name: "invalid successful acknowledgement", firstErr: &ambiguousAcknowledgementError{
			operation: "RenewBatch",
			cause:     &invalidSuccessResponseError{cause: errors.New("missing required ok")},
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			f := &fakeSource{}
			recovered := make(chan struct{})
			f.renewDeadlineFn = func(context.Context) (time.Time, error) {
				f.mu.Lock()
				calls := f.renewCalls
				f.mu.Unlock()
				if calls == 1 {
					return time.Time{}, testCase.firstErr
				}
				if calls == 2 {
					close(recovered)
				}
				return time.Now().Add(4 * time.Second), nil
			}
			f.batch = []*Message{{SequenceNumber: 1, LockedUntil: time.Now().Add(1300 * time.Millisecond), queue: "q", s: f}}
			var observed int
			r := newReceiver(f, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(error) {
				observed++
				cancel() // the old implementation reports and stops after failure one
			})})
			err := r.Run(ctx, func(hctx context.Context, _ *Message) error {
				select {
				case <-recovered:
					cancel()
					return nil
				case <-hctx.Done():
					return hctx.Err()
				}
			})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Run = %v, want context.Canceled", err)
			}
			f.mu.Lock()
			renews := f.renewCalls
			f.mu.Unlock()
			if renews != 2 || observed != 0 {
				t.Fatalf("renew calls=%d observed=%d, want 2/0", renews, observed)
			}
		})
	}
}

// Exhausting the known lease cancels only this message, reports one ErrLockLost,
// and skips a terminal settle that can no longer be authoritative.
func TestReceiverRenewExhaustionCancelsOnlyMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	f := &fakeSource{renewErr: errors.New("renew transport unavailable"), renewDeadlineFn: func(context.Context) (time.Time, error) {
		return time.Time{}, errors.New("renew transport unavailable")
	}}
	f.batch = []*Message{{SequenceNumber: 1, LockedUntil: time.Now().Add(1300 * time.Millisecond), queue: "q", s: f}}
	var observed []error
	r := newReceiver(f, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) {
		observed = append(observed, err)
		if errors.Is(err, ErrLockLost) {
			cancel()
		}
	})})
	handlerCanceled := make(chan struct{})
	err := r.Run(ctx, func(hctx context.Context, _ *Message) error {
		<-hctx.Done()
		close(handlerCanceled)
		return hctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want cancellation after lock loss", err)
	}
	select {
	case <-handlerCanceled:
	default:
		t.Fatal("lock loss did not cancel the per-message handler context")
	}
	if len(observed) != 1 || !errors.Is(observed[0], ErrLockLost) {
		t.Fatalf("observed errors = %v, want one ErrLockLost", observed)
	}
	f.mu.Lock()
	completeCalls, abandonCalls := f.completeCalls, f.abandonCalls
	f.mu.Unlock()
	if completeCalls != 0 || abandonCalls != 0 {
		t.Fatalf("settlement after lost renewal: complete=%d abandon=%d, want 0/0", completeCalls, abandonCalls)
	}
}

// A successful renewal is authoritative proof that the broker still owned the
// token when it committed the renewal. An absolute deadline that looks expired
// locally can instead mean the client clock is ahead of the broker; rejecting
// that acknowledgement would consume every delivery without invoking user code.
func TestReceiverDispatchesAfterSuccessfulRenewalAcrossClockSkew(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	f := &fakeSource{renewCoarseClock: true, renewDeadlineFn: func(context.Context) (time.Time, error) {
		return time.Now().Add(-2 * time.Second), nil
	}}
	f.batch = []*Message{{SequenceNumber: 1, LockedUntil: time.Now().Add(-time.Second), localLeaseCoarseClock: true, queue: "q", s: f}}
	f.completeFn = func(context.Context) error {
		cancel()
		return nil
	}
	var observed []error
	r := newReceiver(f, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) {
		observed = append(observed, err)
		cancel()
	})})
	handlerCalls := 0
	err := r.Run(ctx, func(context.Context, *Message) error {
		handlerCalls++
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled after settlement", err)
	}
	if handlerCalls != 1 {
		t.Fatalf("handler calls = %d, want one", handlerCalls)
	}
	if len(observed) != 0 {
		t.Fatalf("observed errors = %v, want none", observed)
	}
	f.mu.Lock()
	renews, completeCalls, abandonCalls := f.renewCalls, f.completeCalls, f.abandonCalls
	f.mu.Unlock()
	if renews < 1 || completeCalls != 1 || abandonCalls != 0 {
		t.Fatalf("calls renew/complete/abandon = %d/%d/%d, want >=1/1/0", renews, completeCalls, abandonCalls)
	}
}

func TestReceiverClockSkewRenewalAllowsCrossZoneRTT(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	f := &fakeSource{renewCoarseClock: true}
	f.batch = []*Message{{SequenceNumber: 1, LockedUntil: time.Now().Add(-time.Second), localLeaseCoarseClock: true, queue: "q", s: f}}
	f.renewDeadlineFn = func(renewCtx context.Context) (time.Time, error) {
		timer := time.NewTimer(150 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
			return time.Now().Add(-2 * time.Second), nil
		case <-renewCtx.Done():
			return time.Time{}, renewCtx.Err()
		}
	}
	f.completeFn = func(context.Context) error {
		cancel()
		return nil
	}
	var observed []error
	handlerCompleted := false
	err := newReceiver(f, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) {
		observed = append(observed, err)
		cancel()
	})}).Run(ctx, func(hctx context.Context, _ *Message) error {
		timer := time.NewTimer(500 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
			handlerCompleted = true
			return nil
		case <-hctx.Done():
			return hctx.Err()
		}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled after settlement", err)
	}
	if !handlerCompleted {
		t.Fatal("150ms renewal RTT canceled the handler")
	}
	if len(observed) != 0 {
		t.Fatalf("observed errors = %v, want none", observed)
	}
	f.mu.Lock()
	renews, completeCalls := f.renewCalls, f.completeCalls
	f.mu.Unlock()
	if renews < 2 || completeCalls != 1 {
		t.Fatalf("renew/complete calls = %d/%d, want >=2/1", renews, completeCalls)
	}
}

func TestReceiverContinuesRenewingAcrossClockSkew(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	f := &fakeSource{renewCoarseClock: true}
	f.batch = []*Message{{SequenceNumber: 1, LockedUntil: time.Now().Add(-time.Second), localLeaseCoarseClock: true, queue: "q", s: f}}
	var timesMu sync.Mutex
	var renewTimes []time.Time
	thirdRenew := make(chan struct{})
	f.renewDeadlineFn = func(context.Context) (time.Time, error) {
		timesMu.Lock()
		renewTimes = append(renewTimes, time.Now())
		if len(renewTimes) == 3 {
			close(thirdRenew)
		}
		timesMu.Unlock()
		return time.Now().Add(-2 * time.Second), nil
	}
	f.completeFn = func(context.Context) error {
		cancel()
		return nil
	}
	var observed []error
	err := newReceiver(f, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) {
		observed = append(observed, err)
		cancel()
	})}).Run(ctx, func(hctx context.Context, _ *Message) error {
		select {
		case <-thirdRenew:
			return nil
		case <-hctx.Done():
			return hctx.Err()
		}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled after settlement", err)
	}
	if len(observed) != 0 {
		t.Fatalf("observed errors = %v, want none", observed)
	}
	timesMu.Lock()
	times := append([]time.Time(nil), renewTimes...)
	timesMu.Unlock()
	if len(times) != 3 {
		t.Fatalf("renew calls = %d, want three", len(times))
	}
	if elapsed := times[2].Sub(times[0]); elapsed < 150*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("three skew-mode renewals took %s, want bounded non-spinning cadence", elapsed)
	}
	f.mu.Lock()
	completeCalls, abandonCalls := f.completeCalls, f.abandonCalls
	f.mu.Unlock()
	if completeCalls != 1 || abandonCalls != 0 {
		t.Fatalf("complete/abandon calls = %d/%d, want 1/0", completeCalls, abandonCalls)
	}
}

func TestReceiverClockSkewModeFailsClosedOnTransientRenewalFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	f := &fakeSource{renewCoarseClock: true}
	f.batch = []*Message{{SequenceNumber: 1, LockedUntil: time.Now().Add(-time.Second), localLeaseCoarseClock: true, queue: "q", s: f}}
	call := 0
	f.renewDeadlineFn = func(context.Context) (time.Time, error) {
		call++
		if call == 1 {
			return time.Now().Add(-2 * time.Second), nil
		}
		return time.Time{}, errors.New("renew transport unavailable")
	}
	handlerCanceled := make(chan struct{})
	var observed []error
	err := newReceiver(f, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) {
		observed = append(observed, err)
		if errors.Is(err, ErrLockLost) {
			cancel()
		}
	})}).Run(ctx, func(hctx context.Context, _ *Message) error {
		<-hctx.Done()
		close(handlerCanceled)
		return hctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled after lock loss", err)
	}
	select {
	case <-handlerCanceled:
	default:
		t.Fatal("untrusted renewal failure did not cancel the handler")
	}
	if len(observed) != 1 || !errors.Is(observed[0], ErrLockLost) {
		t.Fatalf("observed errors = %v, want one ErrLockLost", observed)
	}
	f.mu.Lock()
	renews := f.renewCalls
	completeCalls, abandonCalls := f.completeCalls, f.abandonCalls
	f.mu.Unlock()
	if renews != 2 || completeCalls != 0 || abandonCalls != 0 {
		t.Fatalf("renew/complete/abandon calls = %d/%d/%d, want 2/0/0", renews, completeCalls, abandonCalls)
	}
}

func TestReceiverTrustedRenewWatchdogCancelsHungHandler(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	f := &fakeSource{}
	f.batch = []*Message{{SequenceNumber: 1, LockedUntil: time.Now().Add(300 * time.Millisecond), queue: "q", s: f}}
	f.renewDeadlineFn = func(renewCtx context.Context) (time.Time, error) {
		<-renewCtx.Done()
		return time.Time{}, renewCtx.Err()
	}
	handlerCanceled := make(chan struct{})
	started := time.Now()
	var observed []error
	err := newReceiver(f, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) {
		observed = append(observed, err)
		if errors.Is(err, ErrLockLost) {
			cancel()
		}
	})}).Run(ctx, func(hctx context.Context, _ *Message) error {
		<-hctx.Done()
		close(handlerCanceled)
		return hctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled after renewal watchdog", err)
	}
	elapsed := time.Since(started)
	if elapsed < 240*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("trusted renewal watchdog fired after %s, want near the 300ms lease horizon", elapsed)
	}
	select {
	case <-handlerCanceled:
	default:
		t.Fatal("trusted renewal watchdog did not cancel the handler")
	}
	if len(observed) != 1 || !errors.Is(observed[0], ErrLockLost) {
		t.Fatalf("observed errors = %v, want one ErrLockLost", observed)
	}
	f.mu.Lock()
	renews := f.renewCalls
	completeCalls, abandonCalls := f.completeCalls, f.abandonCalls
	f.mu.Unlock()
	if renews != 1 || completeCalls != 0 || abandonCalls != 0 {
		t.Fatalf("renew/complete/abandon calls = %d/%d/%d, want 1/0/0", renews, completeCalls, abandonCalls)
	}
}

func TestReceiverUntrustedRenewWatchdogCancelsHungHandler(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	f := &fakeSource{renewCoarseClock: true}
	f.batch = []*Message{{SequenceNumber: 1, LockedUntil: time.Now().Add(-time.Second), localLeaseCoarseClock: true, queue: "q", s: f}}
	call := 0
	f.renewDeadlineFn = func(renewCtx context.Context) (time.Time, error) {
		call++
		if call == 1 {
			return time.Now().Add(-2 * time.Second), nil
		}
		<-renewCtx.Done()
		return time.Time{}, renewCtx.Err()
	}
	handlerCanceled := make(chan struct{})
	started := time.Now()
	var observed []error
	err := newReceiver(f, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) {
		observed = append(observed, err)
		if errors.Is(err, ErrLockLost) {
			cancel()
		}
	})}).Run(ctx, func(hctx context.Context, _ *Message) error {
		<-hctx.Done()
		close(handlerCanceled)
		return hctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled after skew-mode watchdog", err)
	}
	elapsed := time.Since(started)
	if elapsed < time.Second || elapsed > 2*time.Second {
		t.Fatalf("untrusted renewal watchdog fired after %s, want a 250ms probe plus bounded 1s request", elapsed)
	}
	select {
	case <-handlerCanceled:
	default:
		t.Fatal("untrusted renewal watchdog did not cancel the handler")
	}
	if len(observed) != 1 || !errors.Is(observed[0], ErrLockLost) {
		t.Fatalf("observed errors = %v, want one ErrLockLost", observed)
	}
	f.mu.Lock()
	renews := f.renewCalls
	completeCalls, abandonCalls := f.completeCalls, f.abandonCalls
	f.mu.Unlock()
	if renews != 2 || completeCalls != 0 || abandonCalls != 0 {
		t.Fatalf("renew/complete/abandon calls = %d/%d/%d, want 2/0/0", renews, completeCalls, abandonCalls)
	}
}

func TestReceiverInitialRenewalHasWatchdog(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	f := &fakeSource{renewCoarseClock: true}
	f.batch = []*Message{{SequenceNumber: 1, LockedUntil: time.Now().Add(-time.Second), queue: "q", s: f}}
	f.renewDeadlineFn = func(renewCtx context.Context) (time.Time, error) {
		<-renewCtx.Done()
		return time.Time{}, renewCtx.Err()
	}
	handlerCalls := 0
	var observed []error
	started := time.Now()
	err := newReceiver(f, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) {
		observed = append(observed, err)
		cancel()
	})}).Run(ctx, func(context.Context, *Message) error {
		handlerCalls++
		return nil
	})
	elapsed := time.Since(started)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled after watchdog", err)
	}
	if elapsed < 900*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("initial renewal watchdog fired after %s, want about 1s", elapsed)
	}
	if handlerCalls != 0 || len(observed) != 1 || !errors.Is(observed[0], ErrLockLost) {
		t.Fatalf("handler calls=%d observed=%v, want 0 and one ErrLockLost", handlerCalls, observed)
	}
}

func TestReceiverReplayInitialRenewalUsesMappedLeaseWatchdog(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	f := &fakeSource{}
	f.receiveFn = func(callCtx context.Context, call int, _ string) ([]*Message, error) {
		switch call {
		case 1:
			return nil, errors.New("lost receive response")
		case 2:
			return []*Message{{
				SequenceNumber:   1,
				LockedUntil:      time.Now().Add(-time.Second),
				localLockedUntil: time.Now().Add(10 * time.Second),
				queue:            "q",
				s:                f,
			}}, nil
		default:
			<-callCtx.Done()
			return nil, callCtx.Err()
		}
	}
	f.renewDeadlineFn = func(renewCtx context.Context) (time.Time, error) {
		timer := time.NewTimer(1100 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
			return time.Now().Add(10 * time.Second), nil
		case <-renewCtx.Done():
			return time.Time{}, renewCtx.Err()
		}
	}
	f.completeFn = func(context.Context) error {
		cancel()
		return nil
	}
	var observed []error
	handlers := 0
	err := newReceiver(f, "q", []ReceiverOption{WithErrorHandler(func(err error) {
		observed = append(observed, err)
		cancel()
	})}).Run(ctx, func(context.Context, *Message) error {
		handlers++
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if handlers != 1 || len(observed) != 0 {
		t.Fatalf("handler calls=%d observed=%v, want 1 and none", handlers, observed)
	}
	f.mu.Lock()
	renews, completeCalls := f.renewCalls, f.completeCalls
	f.mu.Unlock()
	if renews != 1 || completeCalls != 1 {
		t.Fatalf("renew/complete calls = %d/%d, want 1/1", renews, completeCalls)
	}
}

// Complete must wait until an in-flight Renew has observed cancellation and
// exited, avoiding a false post-completion lock_lost report.
func TestReceiverJoinsRenewBeforeSettlement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	renewStarted := make(chan struct{})
	renewExited := make(chan struct{})
	var once sync.Once
	f := &fakeSource{}
	renew := func(ctx context.Context) error {
		once.Do(func() { close(renewStarted) })
		<-ctx.Done()
		close(renewExited)
		return ctx.Err()
	}
	f.renewFn = renew
	f.renewDeadlineFn = func(ctx context.Context) (time.Time, error) { return time.Time{}, renew(ctx) }
	settledBeforeJoin := false
	f.completeFn = func(context.Context) error {
		select {
		case <-renewExited:
		default:
			settledBeforeJoin = true
		}
		cancel()
		return nil
	}
	f.batch = []*Message{{SequenceNumber: 1, LockedUntil: time.Now().Add(1300 * time.Millisecond), queue: "q", s: f}}
	err := newReceiver(f, "q", []ReceiverOption{WithAutoRenew()}).Run(ctx, func(hctx context.Context, _ *Message) error {
		select {
		case <-renewStarted:
			return nil
		case <-hctx.Done():
			return hctx.Err()
		}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if settledBeforeJoin {
		t.Fatal("Complete started before the in-flight Renew exited")
	}
}

// A successful renewal reschedules from the committed deadline, not the stale
// interval calculated from the original Receive response.
func TestReceiverRenewReschedulesFromCommittedDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	firstRenew := make(chan struct{})
	secondRenew := make(chan struct{})
	f := &fakeSource{}
	recordRenew := func() time.Time {
		f.mu.Lock()
		calls := f.renewCalls
		f.mu.Unlock()
		switch calls {
		case 1:
			close(firstRenew)
		case 2:
			close(secondRenew)
		}
		return time.Now().Add(4 * time.Second)
	}
	f.renewFn = func(context.Context) error { recordRenew(); return nil }
	f.renewDeadlineFn = func(context.Context) (time.Time, error) { return recordRenew(), nil }
	f.batch = []*Message{{SequenceNumber: 1, LockedUntil: time.Now().Add(1300 * time.Millisecond), queue: "q", s: f}}
	err := newReceiver(f, "q", []ReceiverOption{WithAutoRenew()}).Run(ctx, func(hctx context.Context, _ *Message) error {
		select {
		case <-firstRenew:
		case <-hctx.Done():
			return hctx.Err()
		}
		timer := time.NewTimer(1200 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-secondRenew:
			cancel()
			return errors.New("renewal kept the stale one-second ticker")
		case <-timer.C:
			cancel()
			return nil
		}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	f.mu.Lock()
	renews := f.renewCalls
	f.mu.Unlock()
	if renews != 1 {
		t.Fatalf("renew calls = %d, want one before handler return", renews)
	}
}

// More than two lost Receive responses still replay the exact same broker claim:
// the idempotency key survives every backoff.
func TestReceiverKeepsAttemptIDAcrossBackoff(t *testing.T) {
	f := &fakeSource{failN: 3}
	f.batch = []*Message{{SequenceNumber: 1, LockedUntil: time.Now().Add(30 * time.Second), queue: "q", s: f}}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	err := newReceiver(f, "q", nil).Run(ctx, func(context.Context, *Message) error {
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	f.mu.Lock()
	attempts := append([]string(nil), f.attempts...)
	f.mu.Unlock()
	if len(attempts) < 4 {
		t.Fatalf("receive calls = %d, want at least four", len(attempts))
	}
	for i := 1; i < 4; i++ {
		if attempts[i] != attempts[0] {
			t.Fatalf("attempt changed across backoff: %q then %q at call %d", attempts[0], attempts[i], i+1)
		}
	}
}

// A replay can arrive after its original lease expired. It must not enter user
// code; the Receiver starts a fresh receive round instead.
func TestReceiverDropsExpiredAttemptReplay(t *testing.T) {
	testReceiverDropsStaleAttemptReplay(t, time.Now().Add(-time.Second))
}

func TestReceiverDropsZeroDeadlineAttemptReplay(t *testing.T) {
	testReceiverDropsStaleAttemptReplay(t, time.Time{})
}

// A non-zero replay deadline cannot prove freshness against a client wall clock:
// a fast clock makes a live token look expired, while a slow clock makes a stale
// token look live. Let the broker's fenced Renew decide in auto-renew mode.
func TestReceiverAutoRenewFencesAttemptReplayRegardlessOfLocalDeadline(t *testing.T) {
	for _, tc := range []struct {
		name         string
		renewErr     error
		wantHandlers int
		wantComplete int
	}{
		{name: "broker confirms token", wantHandlers: 1, wantComplete: 1},
		{name: "broker rejects token", renewErr: ErrLockLost},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			f := &fakeSource{}
			f.receiveFn = func(callCtx context.Context, call int, _ string) ([]*Message, error) {
				switch call {
				case 1:
					return nil, errors.New("lost receive response")
				case 2:
					// A slow client clock can make an actually stale replay look
					// comfortably live. Replay status, not this timestamp, must force
					// the broker-side renewal fence.
					return []*Message{{SequenceNumber: 1, LockedUntil: time.Now().Add(time.Hour), queue: "q", s: f}}, nil
				default:
					<-callCtx.Done()
					return nil, callCtx.Err()
				}
			}
			f.renewDeadlineFn = func(context.Context) (time.Time, error) {
				if tc.renewErr != nil {
					return time.Time{}, tc.renewErr
				}
				return time.Now().Add(time.Hour), nil
			}
			f.completeFn = func(context.Context) error {
				cancel()
				return nil
			}
			handlers := 0
			var observed []error
			err := newReceiver(f, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) {
				observed = append(observed, err)
				if errors.Is(err, ErrLockLost) {
					cancel()
				}
			})}).Run(ctx, func(context.Context, *Message) error {
				handlers++
				return nil
			})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Run = %v, want context.Canceled", err)
			}
			if handlers != tc.wantHandlers {
				t.Fatalf("handler calls = %d, want %d", handlers, tc.wantHandlers)
			}
			f.mu.Lock()
			completeCalls := f.completeCalls
			attempts := append([]string(nil), f.attempts...)
			f.mu.Unlock()
			if completeCalls != tc.wantComplete {
				t.Fatalf("complete calls = %d, want %d", completeCalls, tc.wantComplete)
			}
			if len(attempts) < 2 || attempts[0] != attempts[1] {
				t.Fatalf("attempt sequence = %q, want exact replay before renewal fence", attempts)
			}
			if tc.renewErr == nil && len(observed) != 0 {
				t.Fatalf("observed errors = %v, want none", observed)
			}
			if tc.renewErr != nil && (len(observed) != 1 || !errors.Is(observed[0], ErrLockLost)) {
				t.Fatalf("observed errors = %v, want one ErrLockLost", observed)
			}
		})
	}
}

func TestReceiverReplayWithoutAutoRenewUsesBrokerFenceAcrossClockSkew(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	f := &fakeSource{renewCoarseClock: true}
	f.receiveFn = func(callCtx context.Context, call int, _ string) ([]*Message, error) {
		switch call {
		case 1:
			return nil, errors.New("lost receive response")
		case 2:
			// A fast client clock makes a live replay look expired locally. The
			// broker renewal fence, not that wall-clock comparison, decides.
			return []*Message{{SequenceNumber: 1, LockedUntil: time.Now().Add(-time.Second), localLeaseCoarseClock: true, queue: "q", s: f}}, nil
		default:
			<-callCtx.Done()
			return nil, callCtx.Err()
		}
	}
	f.renewDeadlineFn = func(context.Context) (time.Time, error) {
		return time.Now().Add(-time.Second), nil
	}
	f.completeFn = func(context.Context) error {
		cancel()
		return nil
	}
	handlers := 0
	var observed []error
	err := newReceiver(f, "q", []ReceiverOption{WithErrorHandler(func(err error) {
		observed = append(observed, err)
		cancel()
	})}).Run(ctx, func(context.Context, *Message) error {
		handlers++
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if handlers != 1 || len(observed) != 0 {
		t.Fatalf("handler calls=%d observed=%v, want 1 and none", handlers, observed)
	}
	f.mu.Lock()
	renews, completeCalls := f.renewCalls, f.completeCalls
	f.mu.Unlock()
	if renews != 1 || completeCalls != 1 {
		t.Fatalf("renew/complete calls = %d/%d, want 1/1", renews, completeCalls)
	}
}

func TestReceiverReplayLegacyRenewalKeepsUntrustedCadence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	f := &fakeSource{}
	f.receiveFn = func(callCtx context.Context, call int, _ string) ([]*Message, error) {
		switch call {
		case 1:
			return nil, errors.New("lost receive response")
		case 2:
			// Simulate a slow client clock: this stale replay appears to have a
			// huge local horizon, which cannot be reused after a legacy Renew.
			return []*Message{{SequenceNumber: 1, LockedUntil: time.Now().Add(time.Hour), queue: "q", s: f}}, nil
		default:
			<-callCtx.Done()
			return nil, callCtx.Err()
		}
	}
	f.renewDeadlineFn = func(context.Context) (time.Time, error) {
		return time.Time{}, ErrUnsupported
	}
	var timesMu sync.Mutex
	var legacyRenewTimes []time.Time
	thirdLegacyRenew := make(chan struct{})
	f.renewFn = func(context.Context) error {
		timesMu.Lock()
		legacyRenewTimes = append(legacyRenewTimes, time.Now())
		if len(legacyRenewTimes) == 3 {
			close(thirdLegacyRenew)
		}
		timesMu.Unlock()
		return nil
	}
	f.completeFn = func(context.Context) error {
		cancel()
		return nil
	}
	var observed []error
	err := newReceiver(f, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) {
		observed = append(observed, err)
		cancel()
	})}).Run(ctx, func(hctx context.Context, _ *Message) error {
		select {
		case <-thirdLegacyRenew:
			return nil
		case <-hctx.Done():
			return hctx.Err()
		}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled after settlement", err)
	}
	if len(observed) != 0 {
		t.Fatalf("observed errors = %v, want none", observed)
	}
	timesMu.Lock()
	times := append([]time.Time(nil), legacyRenewTimes...)
	timesMu.Unlock()
	if len(times) != 3 {
		t.Fatalf("legacy Renew calls = %d, want three", len(times))
	}
	if elapsed := times[2].Sub(times[0]); elapsed < 150*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("three legacy skew-mode renewals took %s, want bounded non-spinning cadence", elapsed)
	}
	f.mu.Lock()
	deadlineCalls := f.deadlineRenewCalls
	completeCalls, abandonCalls := f.completeCalls, f.abandonCalls
	f.mu.Unlock()
	if deadlineCalls != 1 || completeCalls != 1 || abandonCalls != 0 {
		t.Fatalf("deadline-renew/complete/abandon calls = %d/%d/%d, want 1/1/0", deadlineCalls, completeCalls, abandonCalls)
	}
}

func TestReceiverDropsZeroDeadlineOnFirstSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	f := &fakeSource{}
	f.receiveFn = func(callCtx context.Context, call int, _ string) ([]*Message, error) {
		switch call {
		case 1:
			return []*Message{{SequenceNumber: 1, queue: "q", s: f}}, nil
		case 2:
			return []*Message{{SequenceNumber: 2, LockedUntil: time.Now().Add(30 * time.Second), queue: "q", s: f}}, nil
		default:
			<-callCtx.Done()
			return nil, callCtx.Err()
		}
	}
	var handled []int64
	err := newReceiver(f, "q", nil).Run(ctx, func(_ context.Context, message *Message) error {
		handled = append(handled, message.SequenceNumber)
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if len(handled) != 1 || handled[0] != 2 {
		t.Fatalf("handled seqs = %v, want only the valid second delivery", handled)
	}
}

func TestReceiverFilterCancellationDropsRemainingBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := &fakeSource{}
	f.batch = []*Message{
		{SequenceNumber: 1, queue: "q", s: f},
		{SequenceNumber: 2, LockedUntil: time.Now().Add(30 * time.Second), queue: "q", s: f},
	}
	handlerCalled := make(chan struct{}, 1)
	err := newReceiver(f, "q", []ReceiverOption{
		WithConcurrency(2),
		WithErrorHandler(func(error) { cancel() }),
	}).Run(ctx, func(context.Context, *Message) error {
		handlerCalled <- struct{}{}
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled from filter observer", err)
	}
	select {
	case <-handlerCalled:
		t.Fatal("filter observer canceled Run but a remaining message reached the handler")
	default:
	}
}

func TestReceiverKeepsNearExpiryOnFirstSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	f := &fakeSource{}
	f.batch = []*Message{{SequenceNumber: 1, LockedUntil: time.Now().Add(time.Millisecond), queue: "q", s: f}}
	err := newReceiver(f, "q", nil).Run(ctx, func(context.Context, *Message) error {
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled after preserving first response", err)
	}
	f.mu.Lock()
	completeCalls := f.completeCalls
	f.mu.Unlock()
	if completeCalls != 1 {
		t.Fatalf("near-expiry first response complete calls = %d, want one", completeCalls)
	}
}

func TestReceiverConfirmsExpiredInitialLeaseBeforeHandler(t *testing.T) {
	for _, tc := range []struct {
		name              string
		legacy            bool
		wantRenewCalls    int
		wantDeadlineCalls int
	}{
		{name: "deadline-bearing", wantRenewCalls: 1, wantDeadlineCalls: 1},
		{name: "legacy fallback", legacy: true, wantRenewCalls: 2, wantDeadlineCalls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			f := &fakeSource{}
			f.batch = []*Message{{SequenceNumber: 1, LockedUntil: time.Now().Add(-time.Millisecond), queue: "q", s: f}}
			renewStarted := make(chan struct{})
			allowRenew := make(chan struct{})
			f.renewDeadlineFn = func(renewCtx context.Context) (time.Time, error) {
				if tc.legacy {
					return time.Time{}, ErrUnsupported
				}
				close(renewStarted)
				select {
				case <-allowRenew:
					return time.Now().Add(time.Minute), nil
				case <-renewCtx.Done():
					return time.Time{}, renewCtx.Err()
				}
			}
			f.renewFn = func(renewCtx context.Context) error {
				close(renewStarted)
				select {
				case <-allowRenew:
					return nil
				case <-renewCtx.Done():
					return renewCtx.Err()
				}
			}

			handlerStarted := make(chan struct{})
			done := make(chan error, 1)
			go func() {
				done <- newReceiver(f, "q", []ReceiverOption{WithAutoRenew()}).Run(ctx, func(context.Context, *Message) error {
					close(handlerStarted)
					cancel()
					return nil
				})
			}()
			select {
			case <-renewStarted:
			case <-time.After(2 * time.Second):
				t.Fatal("initial renewal did not start")
			}
			select {
			case <-handlerStarted:
				t.Fatal("handler started before the initial renewal was confirmed")
			case <-time.After(100 * time.Millisecond):
			}
			close(allowRenew)
			select {
			case <-handlerStarted:
			case <-time.After(2 * time.Second):
				t.Fatal("handler did not start after the initial renewal was confirmed")
			}
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("Run = %v, want context.Canceled", err)
			}
			f.mu.Lock()
			renewCalls, deadlineCalls := f.renewCalls, f.deadlineRenewCalls
			completeCalls, abandonCalls := f.completeCalls, f.abandonCalls
			f.mu.Unlock()
			if renewCalls != tc.wantRenewCalls || deadlineCalls != tc.wantDeadlineCalls {
				t.Fatalf("renew/deadline calls = %d/%d, want %d/%d", renewCalls, deadlineCalls, tc.wantRenewCalls, tc.wantDeadlineCalls)
			}
			if completeCalls != 1 || abandonCalls != 0 {
				t.Fatalf("complete/abandon calls = %d/%d, want 1/0", completeCalls, abandonCalls)
			}
		})
	}
}

func TestReceiverInitialRenewalFailureDoesNotDispatch(t *testing.T) {
	for _, tc := range []struct {
		name       string
		renewErr   error
		wantRun    error
		wantNotify error
		forbid     error
	}{
		{name: "transient becomes lock lost", renewErr: errors.New("renew transport unavailable"), wantRun: context.Canceled, wantNotify: ErrLockLost},
		{name: "gateway body stays transient", renewErr: &HTTPError{StatusCode: 503, cause: ErrPermissionDenied}, wantRun: context.Canceled, wantNotify: ErrLockLost, forbid: ErrPermissionDenied},
		{name: "permanent stops receiver", renewErr: ErrPermissionDenied, wantRun: ErrPermissionDenied, wantNotify: ErrPermissionDenied},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			f := &fakeSource{}
			f.batch = []*Message{{SequenceNumber: 1, LockedUntil: time.Now().Add(-time.Millisecond), queue: "q", s: f}}
			f.renewDeadlineFn = func(context.Context) (time.Time, error) { return time.Time{}, tc.renewErr }
			var observed []error
			handlerCalls := 0
			err := newReceiver(f, "q", []ReceiverOption{
				WithAutoRenew(),
				WithErrorHandler(func(err error) {
					observed = append(observed, err)
					if errors.Is(err, ErrLockLost) {
						cancel()
					}
				}),
			}).Run(ctx, func(context.Context, *Message) error {
				handlerCalls++
				return nil
			})
			if !errors.Is(err, tc.wantRun) {
				t.Fatalf("Run = %v, want %v", err, tc.wantRun)
			}
			if len(observed) != 1 || !errors.Is(observed[0], tc.wantNotify) {
				t.Fatalf("observed = %v, want one %v", observed, tc.wantNotify)
			}
			if tc.forbid != nil && errors.Is(observed[0], tc.forbid) {
				t.Fatalf("observed = %v, must not retain misleading %v classification", observed, tc.forbid)
			}
			f.mu.Lock()
			renewCalls := f.renewCalls
			completeCalls, abandonCalls := f.completeCalls, f.abandonCalls
			f.mu.Unlock()
			if handlerCalls != 0 || renewCalls != 1 || completeCalls != 0 || abandonCalls != 0 {
				t.Fatalf("handler/renew/complete/abandon calls = %d/%d/%d/%d, want 0/1/0/0", handlerCalls, renewCalls, completeCalls, abandonCalls)
			}
		})
	}
}

func TestReceiverCanceledDuringInitialRenewalDoesNotDispatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	f := &fakeSource{}
	f.batch = []*Message{{SequenceNumber: 1, LockedUntil: time.Now().Add(-time.Millisecond), queue: "q", s: f}}
	renewStarted := make(chan struct{})
	allowRenew := make(chan struct{})
	f.renewDeadlineFn = func(context.Context) (time.Time, error) {
		close(renewStarted)
		<-allowRenew // Deliberately ignore cancellation like a misbehaving transport.
		return time.Now().Add(time.Minute), nil
	}
	handlerStarted := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- newReceiver(f, "q", []ReceiverOption{WithAutoRenew()}).Run(ctx, func(context.Context, *Message) error {
			close(handlerStarted)
			return nil
		})
	}()
	select {
	case <-renewStarted:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("initial renewal did not start")
	}
	cancel()
	close(allowRenew)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after cancellation during initial renewal")
	}
	select {
	case <-handlerStarted:
		t.Fatal("handler started from a renewal acknowledgement returned after cancellation")
	default:
	}
	f.mu.Lock()
	renewCalls := f.renewCalls
	completeCalls, abandonCalls := f.completeCalls, f.abandonCalls
	f.mu.Unlock()
	if renewCalls != 1 || completeCalls != 0 || abandonCalls != 0 {
		t.Fatalf("renew/complete/abandon calls = %d/%d/%d, want 1/0/0", renewCalls, completeCalls, abandonCalls)
	}
}

func testReceiverDropsStaleAttemptReplay(t *testing.T, staleDeadline time.Time) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	f := &fakeSource{}
	if !staleDeadline.IsZero() {
		f.renewDeadlineFn = func(context.Context) (time.Time, error) {
			return time.Time{}, ErrLockLost
		}
	}
	f.receiveFn = func(ctx context.Context, call int, _ string) ([]*Message, error) {
		switch call {
		case 1, 2:
			return nil, errors.New("lost receive response")
		case 3:
			return []*Message{{SequenceNumber: 1, LockedUntil: staleDeadline, queue: "q", s: f}}, nil
		case 4:
			return []*Message{{SequenceNumber: 2, LockedUntil: time.Now().Add(30 * time.Second), queue: "q", s: f}}, nil
		default:
			<-ctx.Done()
			return nil, ctx.Err()
		}
	}
	var handled []int64
	err := newReceiver(f, "q", nil).Run(ctx, func(_ context.Context, message *Message) error {
		handled = append(handled, message.SequenceNumber)
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if len(handled) != 1 || handled[0] != 2 {
		t.Fatalf("handled seqs = %v, want [2]", handled)
	}
	f.mu.Lock()
	attempts := append([]string(nil), f.attempts...)
	f.mu.Unlock()
	if len(attempts) < 4 || attempts[0] != attempts[1] || attempts[1] != attempts[2] || attempts[3] == attempts[2] {
		t.Fatalf("attempt sequence = %q, want same ID through replay then a fresh round", attempts)
	}
}

// A broken source must not return more messages than the Receiver reserved:
// extra workers would otherwise block forever releasing slots never acquired.
func TestReceiverRejectsBatchAboveReservedCapacity(t *testing.T) {
	f := &fakeSource{}
	f.batch = []*Message{
		{SequenceNumber: 1, LockedUntil: time.Now().Add(30 * time.Second), queue: "q", s: f},
		{SequenceNumber: 2, LockedUntil: time.Now().Add(30 * time.Second), queue: "q", s: f},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	errCh := make(chan error, 1)
	handled := 0
	go func() {
		errCh <- newReceiver(f, "q", []ReceiverOption{WithConcurrency(1)}).Run(ctx, func(context.Context, *Message) error {
			handled++
			return nil
		})
	}()
	select {
	case err := <-errCh:
		if err == nil || !strings.Contains(err.Error(), "returned 2 messages") {
			t.Fatalf("Run = %v, want oversized receive-batch protocol error", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run hung after source returned more messages than reserved")
	}
	if handled != 0 {
		t.Fatalf("handlers called = %d, want zero", handled)
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
	// ReadTimeout bounds the whole request read (slow-drip body); it only covers
	// reading the request, so it doesn't touch a Receive long-poll (that wait + the
	// response come after). WriteTimeout MUST stay 0 — it would cap the response and
	// break the 20s long-poll (MQLITE-88).
	if hs.ReadTimeout <= 0 {
		t.Fatal("ReadTimeout must be set (slow-drip body); it doesn't affect the long-poll")
	}
	if hs.WriteTimeout != 0 {
		t.Fatal("WriteTimeout must stay 0: it would cap the response and break the 20s Receive long-poll")
	}
}

// A broker that does not serve one of the receiver's operations is a PERMANENT incompatibility:
// no amount of retrying makes the route appear. Before ErrUnsupported existed such a 404 arrived
// as ErrNotFound, which isPermanent already caught — so introducing the new sentinel without
// classifying it would have quietly turned a clean "this broker is too old" into an infinite
// retry loop (codex).
func TestUnsupportedIsPermanent(t *testing.T) {
	if !isPermanent(ErrUnsupported) {
		t.Error("ErrUnsupported must stop the receive loop: retrying cannot make the route exist")
	}
	// And it stays permanent when wrapped, which is how the client returns it.
	if !isPermanent(fmt.Errorf("%w: /mqlite.v1.QueueService/RenewBatch", ErrUnsupported)) {
		t.Error("a wrapped ErrUnsupported must still be permanent")
	}
	// Sanity: a transient failure is still transient.
	if isPermanent(errors.New("connection reset")) {
		t.Error("a transient error must not stop the loop")
	}
}

// A closed embedded engine surfaces ErrClosed from Receive. Run must treat it as permanent and
// return, not retry the error handler every 500ms forever after Embedded.Close (round-8).
func TestReceiverReturnsOnEngineClosed(t *testing.T) {
	if !isPermanent(ErrClosed) {
		t.Fatal("ErrClosed must be classified permanent so Run stops on engine shutdown")
	}
	f := &fakeSource{recvErr: ErrClosed}
	var observed error
	r := newReceiver(f, "q", []ReceiverOption{WithErrorHandler(func(e error) { observed = e })})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := r.Run(ctx, func(context.Context, *Message) error { return nil })
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("Run must return ErrClosed once the engine is closed, got %v", err)
	}
	if !errors.Is(observed, ErrClosed) {
		t.Fatalf("error handler must see ErrClosed, got %v", observed)
	}
}

// The other side of the shutdown-ordering coin (round-8, codex): an ErrClosed captured while the
// Run context was LIVE — the engine was closed under a running receiver — must keep precedence even
// if the caller cancels AFTERWARDS, while a worker still holds wg.Wait open. A late ctx check in
// done() would misfile that genuine engine-close as "just cancellation"; the ordering is therefore
// recorded at capture time, and this test pins it.
//
// The sequencing is deterministic, no sleeps: receive #1 delivers a message (the handler parks on
// its own ctx); receive #2 returns ErrClosed → fail() captures it with the caller context LIVE and
// cancels the run context — which is the handler's own ctx, so the handler wakes ONLY after the
// capture is complete, cancels the caller, and returns. done() then reads a record that says
// "captured before cancellation" and must report ErrClosed.
func TestErrClosedCapturedBeforeCancelKeepsPrecedence(t *testing.T) {
	f := &fakeSource{
		batch:             []*Message{{SequenceNumber: 1, Body: []byte("x"), LockedUntil: time.Now().Add(30 * time.Second), queue: "q"}},
		recvErrAfterBatch: ErrClosed,
	}
	f.batch[0].s = f
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := newReceiver(f, "q", []ReceiverOption{WithConcurrency(2)}).
		Run(ctx, func(hctx context.Context, _ *Message) error {
			<-hctx.Done() // wakes when fail() cancels the run ctx — strictly AFTER the capture
			cancel()      // NOW the caller cancels — after the ErrClosed was already recorded
			return nil
		})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("an ErrClosed captured while the context was live must keep precedence over a LATER "+
			"cancellation; got %v", err)
	}
}

// The tightest version of the ordering hole (round-8, codex): between OBSERVING a permanent
// ErrClosed and recording it, Run used to pass through notify — and the user's error handler may
// itself cancel the context. Sampling the ordering inside fail() then read "after cancel" and Run
// reported context.Canceled for an engine-close that provably came first. The ordering is now
// sampled at observation, before notify, so a cancellation FROM the error handler cannot rewrite
// it. Fully deterministic: the cancel happens synchronously inside the handler.
func TestErrorHandlerCancelDoesNotRewriteOrdering(t *testing.T) {
	f := &fakeSource{recvErr: ErrClosed} // every receive: the engine is closed, context live
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := newReceiver(f, "q", []ReceiverOption{
		WithErrorHandler(func(error) { cancel() }), // the observer reacts by cancelling Run
	}).Run(ctx, func(context.Context, *Message) error { return nil })
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("ErrClosed was observed while the context was live; a cancellation issued BY the "+
			"error handler must not rewrite that ordering. got %v", err)
	}
}

// Permission revocation must terminate every receiver path, including the renew
// goroutine and both handler outcomes, without a same-attempt transient retry.
// A Complete/Abandon failure can race one already-pipelined fresh Receive after
// its handler slot is released; cancellation must stop that long-poll.
func TestPermanentRenewalStopsRunBeforeHandlerCancellation(t *testing.T) {
	var order []string
	r := newReceiver(&fakeSource{}, "q", []ReceiverOption{WithErrorHandler(func(error) {
		order = append(order, "notify")
	})})
	outcome := r.stopRenewal(
		func() { order = append(order, "cancel-handler") },
		ErrPermissionDenied,
		func() bool {
			order = append(order, "sample-caller")
			return false
		},
		func(err error, afterCancel bool) {
			if !errors.Is(err, ErrPermissionDenied) || afterCancel {
				t.Fatalf("fail = (%v, %v), want permission denied before caller cancellation", err, afterCancel)
			}
			order = append(order, "fail-run")
		},
	)
	if !outcome.skipSettlement {
		t.Fatal("permanent renewal failure must skip settlement")
	}
	if got, want := strings.Join(order, ","), "sample-caller,fail-run,cancel-handler,notify"; got != want {
		t.Fatalf("permanent renewal shutdown order = %q, want %q", got, want)
	}
}

func TestReceiverPermissionDeniedStopsEveryPath(t *testing.T) {
	for _, operation := range []string{"receive", "complete", "abandon", "renew"} {
		t.Run(operation, func(t *testing.T) {
			denied := fmt.Errorf("permission revoked: %w", ErrPermissionDenied)
			f := &fakeSource{}
			f.batch = []*Message{{SequenceNumber: 1, Body: []byte("x"), queue: "q", s: f, LockedUntil: time.Now().Add(time.Second)}}
			switch operation {
			case "receive":
				f.recvErr = denied
			case "complete":
				f.completeErr = denied
			case "abandon":
				f.abandonErr = denied
			case "renew":
				f.renewErr = denied
			}
			var observed bool
			r := newReceiver(f, "q", []ReceiverOption{WithAutoRenew(), WithErrorHandler(func(err error) {
				if errors.Is(err, ErrPermissionDenied) {
					observed = true
				}
			})})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := r.Run(ctx, func(ctx context.Context, _ *Message) error {
				if operation == "renew" {
					<-ctx.Done()
					return ctx.Err()
				}
				if operation == "abandon" {
					return errors.New("handler failed")
				}
				return nil
			})
			if !errors.Is(err, ErrPermissionDenied) || !observed {
				t.Fatalf("Run = %v, observed = %v", err, observed)
			}
			f.mu.Lock()
			calls := f.calls
			attempts := append([]string(nil), f.attempts...)
			f.mu.Unlock()
			if operation == "receive" || operation == "renew" {
				if calls != 1 {
					t.Fatalf("%s permission failure receive calls = %d, want one", operation, calls)
				}
			} else if calls < 1 || calls > 2 {
				t.Fatalf("%s permission failure receive calls = %d, want at most one pipelined receive", operation, calls)
			}
			if calls == 2 && attempts[0] == attempts[1] {
				t.Fatalf("%s permission failure retried the same receive attempt %q", operation, attempts[0])
			}
		})
	}
}
