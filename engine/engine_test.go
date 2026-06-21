package engine

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func testEngine(t *testing.T) (*Engine, *int64) {
	t.Helper()
	return testEngineAt(t, filepath.Join(t.TempDir(), "mq.db"))
}

func testEngineAt(t *testing.T, path string) (*Engine, *int64) {
	t.Helper()
	var ms int64 = 1_700_000_000_000
	e, err := Open(context.Background(), Options{
		DB:                "file:" + path,
		Now:               func() int64 { return atomic.LoadInt64(&ms) },
		DisableBackground: true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e, &ms
}

func advance(ms *int64, d time.Duration) { atomic.AddInt64(ms, d.Milliseconds()) }

func mustQueue(t *testing.T, e *Engine, name string, cfg QueueConfig) {
	t.Helper()
	if err := e.CreateQueue(context.Background(), name, cfg); err != nil {
		t.Fatalf("create queue %s: %v", name, err)
	}
}

func recvOne(t *testing.T, e *Engine, q string) *Message {
	t.Helper()
	ms, err := e.Receive(context.Background(), q, ReceiveOptions{})
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if len(ms) == 0 {
		return nil
	}
	return ms[0]
}

// I-core: send -> receive (Peek-Lock) -> complete; metrics reflect each step.
func TestSendReceiveComplete(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "orders", QueueConfig{})

	seq, err := e.SendOne(ctx, "orders", OutMessage{Body: []byte("hello")})
	if err != nil || seq != 1 {
		t.Fatalf("send seq=%d err=%v (want 1, nil)", seq, err)
	}
	m := recvOne(t, e, "orders")
	if m == nil || string(m.Body) != "hello" || m.DeliveryCount != 1 {
		t.Fatalf("receive bad: %+v", m)
	}
	// while locked, another receive returns nothing.
	if got := recvOne(t, e, "orders"); got != nil {
		t.Fatalf("expected no message while locked, got seq=%d", got.SeqNumber)
	}
	if err := e.Complete(ctx, "orders", m.SeqNumber, m.LockToken); err != nil {
		t.Fatalf("complete: %v", err)
	}
	mt, _ := e.Stats(ctx, "orders")
	if mt.Total != 0 {
		t.Fatalf("expected empty queue after complete, got %+v", mt)
	}
}

// I-fencing: settling with a stale/foreign token must safely fail (LockLost).
func TestFencingTokenSafety(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{})
	e.SendOne(ctx, "q", OutMessage{Body: []byte("x")})

	m := recvOne(t, e, "q")
	if err := e.Complete(ctx, "q", m.SeqNumber, "wrong-token"); !errors.Is(err, ErrLockLost) {
		t.Fatalf("expected ErrLockLost for bad token, got %v", err)
	}
	if err := e.Complete(ctx, "q", m.SeqNumber, m.LockToken); err != nil {
		t.Fatalf("complete with right token: %v", err)
	}
}

// I-retry: Abandon redelivers; exceeding max delivery count dead-letters.
func TestAbandonAndDeadLetter(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{MaxDeliveryCount: 2})
	e.SendOne(ctx, "q", OutMessage{Body: []byte("x")})

	m := recvOne(t, e, "q") // delivery 1
	if err := e.Abandon(ctx, "q", m.SeqNumber, m.LockToken, 0); err != nil {
		t.Fatal(err)
	}
	m = recvOne(t, e, "q") // delivery 2
	if m.DeliveryCount != 2 {
		t.Fatalf("delivery_count=%d want 2", m.DeliveryCount)
	}
	if err := e.Abandon(ctx, "q", m.SeqNumber, m.LockToken, 0); err != nil {
		t.Fatal(err)
	}
	// now over max -> dead-lettered, not re-deliverable.
	if got := recvOne(t, e, "q"); got != nil {
		t.Fatalf("expected DLQ, but received seq=%d", got.SeqNumber)
	}
	mt, _ := e.Stats(ctx, "q")
	if mt.DeadLettered != 1 {
		t.Fatalf("expected 1 dead-lettered, got %+v", mt)
	}
}

// I-visibility: an expired lock is reclaimed by the reaper and redelivered.
func TestVisibilityTimeoutReaper(t *testing.T) {
	ctx := context.Background()
	e, ms := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{LockDurationMs: 1000, MaxDeliveryCount: 10})
	e.SendOne(ctx, "q", OutMessage{Body: []byte("x")})

	m := recvOne(t, e, "q")
	if got := recvOne(t, e, "q"); got != nil {
		t.Fatal("should be locked")
	}
	advance(ms, 2*time.Second) // lock expires
	e.RunMaintenanceOnce(ctx)
	m2 := recvOne(t, e, "q")
	if m2 == nil || m2.SeqNumber != m.SeqNumber || m2.DeliveryCount != 2 {
		t.Fatalf("expected redelivery after reap, got %+v", m2)
	}
}

// The claim path is the indexable hot path (state='active' only); an expired
// lock is NOT reclaimed by claim — the reaper returns it to active (§8.8).
func TestExpiredLockReclaimedByReaper(t *testing.T) {
	ctx := context.Background()
	e, ms := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{LockDurationMs: 1000, MaxDeliveryCount: 10})
	e.SendOne(ctx, "q", OutMessage{Body: []byte("x")})

	m := recvOne(t, e, "q")
	advance(ms, 2*time.Second) // lock expires
	if got := recvOne(t, e, "q"); got != nil {
		t.Fatalf("claim must not reclaim an expired lock directly, got seq=%d", got.SeqNumber)
	}
	e.RunMaintenanceOnce(ctx) // reaper returns it to active
	m2 := recvOne(t, e, "q")
	if m2 == nil || m2.SeqNumber != m.SeqNumber || m2.DeliveryCount != 2 {
		t.Fatalf("expected reaper redelivery with count 2, got %+v", m2)
	}
}

// §8.2 guard: an expired lock already at max delivery is NOT reclaimed by claim;
// it waits for the reaper to dead-letter it (no over-delivery).
func TestExpiredLockAtMaxNotReclaimed(t *testing.T) {
	ctx := context.Background()
	e, ms := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{LockDurationMs: 1000, MaxDeliveryCount: 1})
	e.SendOne(ctx, "q", OutMessage{Body: []byte("x")})

	_ = recvOne(t, e, "q") // delivery_count now 1 == max
	advance(ms, 2*time.Second)
	if got := recvOne(t, e, "q"); got != nil {
		t.Fatalf("at-max expired lock must not be reclaimed by claim, got seq=%d", got.SeqNumber)
	}
	e.RunMaintenanceOnce(ctx) // reaper dead-letters it
	mt, _ := e.Stats(ctx, "q")
	if mt.DeadLettered != 1 {
		t.Fatalf("reaper should have dead-lettered, got %+v", mt)
	}
}

// I-schedule: scheduled messages are invisible until activation time.
func TestScheduled(t *testing.T) {
	ctx := context.Background()
	e, ms := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{})
	at := atomic.LoadInt64(ms) + (5 * time.Second).Milliseconds()
	if _, err := e.Schedule(ctx, "q", OutMessage{Body: []byte("later")}, at); err != nil {
		t.Fatal(err)
	}
	if got := recvOne(t, e, "q"); got != nil {
		t.Fatal("scheduled message should be invisible")
	}
	advance(ms, 6*time.Second)
	e.RunMaintenanceOnce(ctx)
	if got := recvOne(t, e, "q"); got == nil {
		t.Fatal("scheduled message should be active after time")
	}
}

// I-dedup: in-window duplicates are silently dropped; conflicting body errors.
func TestDedup(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{DedupWindowMs: (10 * time.Minute).Milliseconds()})

	s1, _ := e.SendOne(ctx, "q", OutMessage{Body: []byte("a"), MessageID: "id1"})
	s2, _ := e.SendOne(ctx, "q", OutMessage{Body: []byte("a"), MessageID: "id1"})
	if s1 != s2 {
		t.Fatalf("duplicate should return original seq: %d vs %d", s1, s2)
	}
	mt, _ := e.Stats(ctx, "q")
	if mt.Active != 1 {
		t.Fatalf("dedup should keep depth at 1, got %d", mt.Active)
	}
	_, err := e.SendOne(ctx, "q", OutMessage{Body: []byte("DIFFERENT"), MessageID: "id1"})
	if !errors.Is(err, ErrDedupConflict) {
		t.Fatalf("expected DedupConflict for same id different body, got %v", err)
	}
}

// I-order: same SessionId is strictly in-order; only one in-flight per group.
func TestSessionOrdering(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{})
	e.SendOne(ctx, "q", OutMessage{Body: []byte("1"), GroupID: "order-7"})
	e.SendOne(ctx, "q", OutMessage{Body: []byte("2"), GroupID: "order-7"})

	m1 := recvOne(t, e, "q")
	if m1 == nil || string(m1.Body) != "1" {
		t.Fatalf("first should be msg 1, got %+v", m1)
	}
	// second message of the same group must NOT be delivered while first in-flight.
	if got := recvOne(t, e, "q"); got != nil {
		t.Fatalf("group head blocked: should get nothing, got %q", got.Body)
	}
	if err := e.Complete(ctx, "q", m1.SeqNumber, m1.LockToken); err != nil {
		t.Fatal(err)
	}
	m2 := recvOne(t, e, "q")
	if m2 == nil || string(m2.Body) != "2" {
		t.Fatalf("after completing 1, should get 2, got %+v", m2)
	}
}

// Independent groups proceed in parallel even if one group is busy.
func TestSessionParallelGroups(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{})
	e.SendOne(ctx, "q", OutMessage{Body: []byte("a1"), GroupID: "A"})
	e.SendOne(ctx, "q", OutMessage{Body: []byte("b1"), GroupID: "B"})

	m1 := recvOne(t, e, "q") // locks head of group A
	m2 := recvOne(t, e, "q") // group B head still available
	if m1 == nil || m2 == nil || m1.GroupID == m2.GroupID {
		t.Fatalf("expected one from each group, got %v / %v", m1, m2)
	}
}

// I-topic: publishing to a topic fans out one copy per subscription; filters apply.
func TestTopicFanoutAndFilter(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	if err := e.Subscribe(ctx, "events", "all", nil); err != nil {
		t.Fatal(err)
	}
	if err := e.Subscribe(ctx, "events", "paid", &Filter{Expr: `subject startsWith "payment."`}); err != nil {
		t.Fatal(err)
	}
	e.SendOne(ctx, "events", OutMessage{Body: []byte("o"), Subject: "order.created"})
	e.SendOne(ctx, "events", OutMessage{Body: []byte("p"), Subject: "payment.captured"})

	// "all" gets both; "paid" only the payment.* one.
	all, _ := e.Stats(ctx, "all")
	paid, _ := e.Stats(ctx, "paid")
	if all.Active != 2 {
		t.Fatalf("subscription all should have 2, got %d", all.Active)
	}
	if paid.Active != 1 {
		t.Fatalf("subscription paid should have 1 (filtered), got %d", paid.Active)
	}
}

// I-defer: deferred messages are retrievable only by seq.
func TestDeferAndReceiveDeferred(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{})
	e.SendOne(ctx, "q", OutMessage{Body: []byte("d")})
	m := recvOne(t, e, "q")
	if err := e.Defer(ctx, "q", m.SeqNumber, m.LockToken); err != nil {
		t.Fatal(err)
	}
	if got := recvOne(t, e, "q"); got != nil {
		t.Fatal("deferred message must not be returned by normal receive")
	}
	got, err := e.ReceiveDeferred(ctx, "q", m.SeqNumber)
	if err != nil || len(got) != 1 || string(got[0].Body) != "d" {
		t.Fatalf("receive deferred: %v %+v", err, got)
	}
}

// I-redrive: dead-lettered messages move back to active in place.
func TestRedrive(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{MaxDeliveryCount: 1})
	e.SendOne(ctx, "q", OutMessage{Body: []byte("x")})
	m := recvOne(t, e, "q")
	e.Reject(ctx, "q", m.SeqNumber, m.LockToken, "AppRequested", "bad")

	moved, err := e.Redrive(ctx, "q", RedriveOptions{})
	if err != nil || moved != 1 {
		t.Fatalf("redrive moved=%d err=%v", moved, err)
	}
	if got := recvOne(t, e, "q"); got == nil {
		t.Fatal("redriven message should be receivable")
	}
}

// I-crash: after a restart, orphaned locks are reclaimed (single-broker, §4.4).
func TestCrashRecovery(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "mq.db")
	e1, _ := testEngineAt(t, path)
	mustQueue(t, e1, "q", QueueConfig{})
	e1.SendOne(ctx, "q", OutMessage{Body: []byte("x")})
	m := recvOne(t, e1, "q") // locked
	if m == nil {
		t.Fatal("expected a message")
	}
	_ = e1.Close()

	// reopen: the locked orphan must be reclaimed to active.
	e2, _ := testEngineAt(t, path)
	if got := recvOne(t, e2, "q"); got == nil {
		t.Fatal("after restart the locked message should be active again")
	}
}

// I-outbox: Tx commits business write + enqueue atomically; rollback enqueues nothing.
func TestTxAtomicEnqueue(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{})

	// success path
	err := e.Tx(ctx, func(tx *EngineTx) error {
		if _, err := tx.SQL().ExecContext(ctx,
			`CREATE TABLE IF NOT EXISTS biz(id INTEGER PRIMARY KEY)`); err != nil {
			return err
		}
		if _, err := tx.SQL().ExecContext(ctx, `INSERT INTO biz(id) VALUES (1)`); err != nil {
			return err
		}
		_, err := tx.SendOne("q", OutMessage{Body: []byte("evt")})
		return err
	})
	if err != nil {
		t.Fatalf("tx: %v", err)
	}
	if recvOne(t, e, "q") == nil {
		t.Fatal("committed tx should have enqueued")
	}

	// rollback path: returning an error must enqueue nothing.
	wantErr := errors.New("boom")
	err = e.Tx(ctx, func(tx *EngineTx) error {
		if _, err := tx.SendOne("q", OutMessage{Body: []byte("ghost")}); err != nil {
			return err
		}
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected boom, got %v", err)
	}
	if got := recvOne(t, e, "q"); got != nil {
		t.Fatalf("rolled-back tx must not enqueue, got %q", got.Body)
	}
}

// Receive-and-delete removes the message immediately (at-most-once).
func TestReceiveAndDelete(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{})
	e.SendOne(ctx, "q", OutMessage{Body: []byte("x")})
	ms, err := e.Receive(ctx, "q", ReceiveOptions{Mode: ReceiveAndDelete})
	if err != nil || len(ms) != 1 {
		t.Fatalf("receive-and-delete: %v %d", err, len(ms))
	}
	mt, _ := e.Stats(ctx, "q")
	if mt.Total != 0 {
		t.Fatalf("message should be gone, got %+v", mt)
	}
}
