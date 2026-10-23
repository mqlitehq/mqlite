package engine

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"slices"
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

// Crash recovery applies the same bound as the reaper (MQLITE-58): an orphaned
// lock whose message already spent its LAST allowed attempt dead-letters on Open
// with MaxDeliveryCountExceeded instead of being redelivered an (max+1)th time —
// max_delivery_count bounds deliveries, not deliveries-without-a-crash. A message
// still under the bound is redelivered as before.
func TestCrashRecoveryRespectsMaxDelivery(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "mq.db")

	e1, _ := testEngineAt(t, path)
	mustQueue(t, e1, "last", QueueConfig{MaxDeliveryCount: 1})
	mustQueue(t, e1, "spare", QueueConfig{MaxDeliveryCount: 10})
	e1.SendOne(ctx, "last", OutMessage{Body: []byte("x")})
	e1.SendOne(ctx, "spare", OutMessage{Body: []byte("y")})
	if m := recvOne(t, e1, "last"); m == nil || m.DeliveryCount != 1 {
		t.Fatalf("claim last (count=1=max): %+v", m)
	}
	if m := recvOne(t, e1, "spare"); m == nil || m.DeliveryCount != 1 {
		t.Fatalf("claim spare (count=1<max): %+v", m)
	}
	_ = e1.Close() // crash stand-in: both locks never settled

	e2, _ := testEngineAt(t, path) // Open runs recovery
	// Over the bound → dead-lettered, never delivered again.
	if got := recvOne(t, e2, "last"); got != nil {
		t.Fatalf("recovery must not redeliver past max_delivery_count, got %q", got.Body)
	}
	pk, _ := e2.Peek(ctx, "last", PeekOptions{State: StateDeadLettered})
	if len(pk) != 1 || pk[0].DeadLetterReason != ReasonMaxDeliveryCount {
		t.Fatalf("expected 1 DLQ msg with MaxDeliveryCountExceeded, got %+v", pk)
	}
	// Under the bound → redelivered normally (claim bumps the count again).
	if m := recvOne(t, e2, "spare"); m == nil || m.DeliveryCount != 2 {
		t.Fatalf("under-bound message should redeliver with count=2, got %+v", m)
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

// ─── fixed subscription lifetime: data-plane fencing ────────────────────────

func mustDataPlaneSubscriptionTTL(t *testing.T, e *Engine, topic, name string, ttlMs int64) int64 {
	t.Helper()
	if err := e.SubscribeWithOptions(context.Background(), topic, name, nil, SubscribeOptions{TTLMs: ttlMs}); err != nil {
		t.Fatalf("subscribe %s: %v", name, err)
	}
	var expires int64
	if err := e.db.queryRowScan(context.Background(), []any{&expires},
		`SELECT expires_at FROM queues WHERE name=?`, name); err != nil {
		t.Fatalf("read %s expiration: %v", name, err)
	}
	return expires
}

// Expiry is a logical forced deletion at the exact stored millisecond. This
// matrix deliberately does not run maintenance: every path must fence itself,
// rather than relying on the background transition to deleting.
func TestSubscriptionExpiryFencesDataPlaneAtBoundary(t *testing.T) {
	ctx := context.Background()
	e, clock := testEngine(t)
	expires := mustDataPlaneSubscriptionTTL(t, e, "events", "ephemeral", 1_000)

	seqs, err := e.Send(ctx, "ephemeral",
		OutMessage{Body: []byte("defer")},
		OutMessage{Body: []byte("reject")},
		OutMessage{Body: []byte("complete-before-expiry")},
		OutMessage{Body: []byte("complete")},
		OutMessage{Body: []byte("abandon")},
		OutMessage{Body: []byte("reject-at-expiry")},
		OutMessage{Body: []byte("defer-at-expiry")})
	if err != nil {
		t.Fatal(err)
	}
	locked, err := e.Receive(ctx, "ephemeral", ReceiveOptions{MaxMessages: len(seqs), AttemptID: "request-1"})
	if err != nil || len(locked) != len(seqs) {
		t.Fatalf("claim=%d, %v", len(locked), err)
	}
	for _, m := range locked {
		if m.LockedUntilMs != expires {
			t.Fatalf("lease %d ends at %d, want subscription expiry %d", m.SeqNumber, m.LockedUntilMs, expires)
		}
	}
	var attemptExpires int64
	if err := e.db.queryRowScan(ctx, []any{&attemptExpires},
		`SELECT expires_at FROM receive_attempts WHERE queue=? AND attempt_id=?`, "ephemeral", "request-1"); err != nil {
		t.Fatal(err)
	}
	if attemptExpires != expires {
		t.Fatalf("receive-attempt expires=%d, want subscription expiry %d", attemptExpires, expires)
	}
	if err := e.Defer(ctx, "ephemeral", locked[0].SeqNumber, locked[0].LockToken); err != nil {
		t.Fatal(err)
	}
	if err := e.Reject(ctx, "ephemeral", locked[1].SeqNumber, locked[1].LockToken, "fixture", ""); err != nil {
		t.Fatal(err)
	}
	if err := e.Complete(ctx, "ephemeral", locked[2].SeqNumber, locked[2].LockToken); err != nil {
		t.Fatal(err)
	}
	scheduled, err := e.Schedule(ctx, "ephemeral", OutMessage{Body: []byte("scheduled")}, expires+10_000)
	if err != nil {
		t.Fatal(err)
	}

	atomic.StoreInt64(clock, expires) // equality is expired

	for name, call := range map[string]func() error{
		"send": func() error {
			_, err := e.SendOne(ctx, "ephemeral", OutMessage{Body: []byte("late")})
			return err
		},
		"schedule": func() error {
			_, err := e.Schedule(ctx, "ephemeral", OutMessage{Body: []byte("late")}, expires+10_000)
			return err
		},
		"publish": func() error {
			_, err := e.SendOne(ctx, "events", OutMessage{Body: []byte("late")})
			return err
		},
		"schedule publish": func() error {
			_, err := e.Schedule(ctx, "events", OutMessage{Body: []byte("late")}, expires+10_000)
			return err
		},
		"receive": func() error {
			_, err := e.Receive(ctx, "ephemeral", ReceiveOptions{})
			return err
		},
		"receive attempt replay": func() error {
			_, err := e.Receive(ctx, "ephemeral", ReceiveOptions{AttemptID: "request-1"})
			return err
		},
		"receive deferred": func() error {
			_, err := e.ReceiveDeferred(ctx, "ephemeral", locked[0].SeqNumber)
			return err
		},
		"peek": func() error {
			_, err := e.Peek(ctx, "ephemeral", PeekOptions{})
			return err
		},
		"stats": func() error {
			_, err := e.Stats(ctx, "ephemeral")
			return err
		},
		"cancel":  func() error { return e.Cancel(ctx, "ephemeral", scheduled) },
		"redrive": func() error { _, err := e.Redrive(ctx, "ephemeral", RedriveOptions{}); return err },
		"purge":   func() error { _, err := e.Purge(ctx, "ephemeral", RedriveOptions{}); return err },
		"renew":   func() error { return e.Renew(ctx, "ephemeral", locked[3].SeqNumber, locked[3].LockToken) },
		"renew batch": func() error {
			_, err := e.RenewBatch(ctx, "ephemeral", []SettleItem{{SeqNumber: locked[3].SeqNumber, LockToken: locked[3].LockToken}})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := call(); !errors.Is(err, ErrQueueNotFound) {
				t.Fatalf("at expiry got %v, want ErrQueueNotFound", err)
			}
		})
	}

	settles := []struct {
		name string
		idx  int
		call func(*Message) error
	}{
		{"complete", 3, func(m *Message) error { return e.Complete(ctx, "ephemeral", m.SeqNumber, m.LockToken) }},
		{"abandon", 4, func(m *Message) error { return e.Abandon(ctx, "ephemeral", m.SeqNumber, m.LockToken, 0) }},
		{"reject", 5, func(m *Message) error { return e.Reject(ctx, "ephemeral", m.SeqNumber, m.LockToken, "late", "") }},
		{"defer", 6, func(m *Message) error { return e.Defer(ctx, "ephemeral", m.SeqNumber, m.LockToken) }},
	}
	for _, tc := range settles {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(locked[tc.idx]); !errors.Is(err, ErrLockLost) {
				t.Fatalf("at expiry got %v, want ErrLockLost", err)
			}
			var state string
			if err := e.db.queryRowScan(ctx, []any{&state}, `SELECT state FROM messages WHERE id=?`, locked[tc.idx].SeqNumber); err != nil {
				t.Fatal(err)
			}
			if state != string(StateLocked) {
				t.Fatalf("expired settle changed state to %q", state)
			}
		})
	}

	// A receipt created while the subscription was live cannot authorize a replay
	// after its lifetime ends.
	if err := e.Complete(ctx, "ephemeral", locked[2].SeqNumber, locked[2].LockToken); !errors.Is(err, ErrLockLost) {
		t.Fatalf("expired receipt replay=%v, want ErrLockLost", err)
	}
	batch, err := e.CompleteBatch(ctx, "ephemeral", []SettleItem{
		{SeqNumber: locked[2].SeqNumber, LockToken: locked[2].LockToken},
		{SeqNumber: locked[3].SeqNumber, LockToken: locked[3].LockToken},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 2 || batch[0].Ok || batch[1].Ok {
		t.Fatalf("expired CompleteBatch vouched for a mutation or receipt: %+v", batch)
	}
}

func TestSubscriptionDeadlinesSaturateNearMaxInt64(t *testing.T) {
	ctx := context.Background()
	e, clock := testEngine(t)
	created := int64(math.MaxInt64 - 5)
	atomic.StoreInt64(clock, created)
	expires := mustDataPlaneSubscriptionTTL(t, e, "events", "ephemeral", 5)
	if expires != math.MaxInt64 {
		t.Fatalf("subscription expiry=%d, want MaxInt64", expires)
	}
	if got := boundedDeadlineMs(created, 10, 0); got != math.MaxInt64 {
		t.Fatalf("saturated deadline=%d, want MaxInt64", got)
	}
	if got := boundedDeadlineMs(created, 10, math.MaxInt64-1); got != math.MaxInt64-1 {
		t.Fatalf("bounded deadline=%d, want subscription limit", got)
	}

	seqs, err := e.Send(ctx, "ephemeral",
		OutMessage{Body: []byte("deferred")},
		OutMessage{Body: []byte("renew")},
		OutMessage{Body: []byte("renew-batch")},
		OutMessage{Body: []byte("complete-batch")})
	if err != nil || len(seqs) != 4 {
		t.Fatalf("send=%v, %v", seqs, err)
	}
	msgs, err := e.Receive(ctx, "ephemeral", ReceiveOptions{MaxMessages: 4, AttemptID: "near-max"})
	if err != nil || len(msgs) != 4 {
		t.Fatalf("receive=%d, %v", len(msgs), err)
	}
	for _, msg := range msgs {
		if msg.LockedUntilMs != math.MaxInt64 {
			t.Fatalf("message %d lock=%d, want MaxInt64", msg.SeqNumber, msg.LockedUntilMs)
		}
	}
	var attemptExpiry int64
	if err := e.db.queryRowScan(ctx, []any{&attemptExpiry},
		`SELECT expires_at FROM receive_attempts WHERE queue='ephemeral' AND attempt_id='near-max'`); err != nil {
		t.Fatal(err)
	}
	if attemptExpiry != math.MaxInt64 {
		t.Fatalf("attempt expiry=%d, want MaxInt64", attemptExpiry)
	}

	if err := e.Defer(ctx, "ephemeral", msgs[0].SeqNumber, msgs[0].LockToken); err != nil {
		t.Fatal(err)
	}
	deferred, err := e.ReceiveDeferred(ctx, "ephemeral", msgs[0].SeqNumber)
	if err != nil || len(deferred) != 1 || deferred[0].LockedUntilMs != math.MaxInt64 {
		t.Fatalf("deferred lease=%v, %v", deferred, err)
	}
	if err := e.Renew(ctx, "ephemeral", msgs[1].SeqNumber, msgs[1].LockToken); err != nil {
		t.Fatal(err)
	}
	renewed, err := e.RenewBatch(ctx, "ephemeral", []SettleItem{{SeqNumber: msgs[2].SeqNumber, LockToken: msgs[2].LockToken}})
	if err != nil || len(renewed) != 1 || !renewed[0].Ok || renewed[0].LockedUntilMs != math.MaxInt64 {
		t.Fatalf("renew batch=%+v, %v", renewed, err)
	}
	completed, err := e.CompleteBatch(ctx, "ephemeral", []SettleItem{{SeqNumber: msgs[3].SeqNumber, LockToken: msgs[3].LockToken}})
	if err != nil || len(completed) != 1 || !completed[0].Ok {
		t.Fatalf("complete batch=%+v, %v", completed, err)
	}
	var minReceiptExpiry int64
	if err := e.db.queryRowScan(ctx, []any{&minReceiptExpiry},
		`SELECT MIN(expires_at) FROM settlement_receipts WHERE queue='ephemeral'`); err != nil {
		t.Fatal(err)
	}
	if minReceiptExpiry != math.MaxInt64 {
		t.Fatalf("settlement receipt expiry=%d, want MaxInt64", minReceiptExpiry)
	}
}

func TestReceiveLongPollDeadlineSaturatesNearMaxInt64(t *testing.T) {
	e, clock := testEngine(t)
	mustQueue(t, e, "permanent", QueueConfig{})
	atomic.StoreInt64(clock, math.MaxInt64-5)
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	go func() {
		time.Sleep(time.Millisecond)
		atomic.StoreInt64(clock, math.MaxInt64)
	}()
	msgs, err := e.Receive(ctx, "permanent", ReceiveOptions{WaitMs: 20})
	if err != nil || len(msgs) != 0 {
		t.Fatalf("near-MaxInt64 long poll=%v, %v", msgs, err)
	}
}

func TestSubscriptionExpiryFiltersTopicRoster(t *testing.T) {
	ctx := context.Background()
	e, clock := testEngine(t)
	expires := mustDataPlaneSubscriptionTTL(t, e, "events", "temporary", 1_000)
	if err := e.Subscribe(ctx, "events", "permanent", nil); err != nil {
		t.Fatal(err)
	}
	atomic.StoreInt64(clock, expires)
	if _, err := e.SendOne(ctx, "events", OutMessage{Body: []byte("only-live")}); err != nil {
		t.Fatalf("publish to mixed roster: %v", err)
	}
	var temporary, permanent int
	if err := e.db.queryRowScan(ctx, []any{&temporary}, `SELECT COUNT(*) FROM messages WHERE queue='temporary'`); err != nil {
		t.Fatal(err)
	}
	if err := e.db.queryRowScan(ctx, []any{&permanent}, `SELECT COUNT(*) FROM messages WHERE queue='permanent'`); err != nil {
		t.Fatal(err)
	}
	if temporary != 0 || permanent != 1 {
		t.Fatalf("fan-out temporary/permanent=%d/%d, want 0/1", temporary, permanent)
	}
}

func TestDirectBatchRefreshesSubscriptionExpiryAtEachEffect(t *testing.T) {
	for _, operation := range []string{"send", "schedule"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			e, _ := testEngine(t)
			expires := mustDataPlaneSubscriptionTTL(t, e, "events", "temporary", 1_000)
			var reads int64
			liveReads := int64(2) // route resolution + first message effect
			if operation == "schedule" {
				liveReads++ // public scheduled-time validation
			}
			e.now = func() int64 {
				if atomic.AddInt64(&reads, 1) <= liveReads {
					return expires - 1
				}
				return expires
			}
			messages := []OutMessage{{Body: []byte("first")}, {Body: []byte("second")}}
			var err error
			if operation == "send" {
				_, err = e.Send(ctx, "temporary", messages...)
			} else {
				_, err = e.ScheduleBatch(ctx, "temporary", messages, expires+10_000)
			}
			if !errors.Is(err, ErrQueueNotFound) {
				t.Fatalf("mid-batch expiry=%v, want ErrQueueNotFound", err)
			}
			var count int
			if err := e.db.queryRowScan(ctx, []any{&count},
				`SELECT COUNT(*) FROM messages WHERE queue='temporary'`); err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				t.Fatalf("atomic batch retained %d messages after target expiry", count)
			}
		})
	}
}

func TestTopicFanoutRefreshesSubscriptionExpiryAtEachEffect(t *testing.T) {
	for _, operation := range []string{"send", "schedule"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			e, _ := testEngine(t)
			if err := e.Subscribe(ctx, "events", "a-permanent", nil); err != nil {
				t.Fatal(err)
			}
			expires := mustDataPlaneSubscriptionTTL(t, e, "events", "b-temporary", 1_000)
			var reads int64
			liveReads := int64(1) // publish/roster time; temporary target fence sees expiry
			if operation == "schedule" {
				liveReads++ // public scheduled-time validation
			}
			e.now = func() int64 {
				if atomic.AddInt64(&reads, 1) <= liveReads {
					return expires - 1
				}
				return expires
			}
			var err error
			if operation == "send" {
				_, err = e.SendOne(ctx, "events", OutMessage{Body: []byte("fanout")})
			} else {
				_, err = e.Schedule(ctx, "events", OutMessage{Body: []byte("fanout")}, expires+10_000)
			}
			if err != nil {
				t.Fatalf("topic %s across target expiry: %v", operation, err)
			}
			var permanent, temporary int
			if err := e.db.queryRowScan(ctx, []any{&permanent},
				`SELECT COUNT(*) FROM messages WHERE queue='a-permanent'`); err != nil {
				t.Fatal(err)
			}
			if err := e.db.queryRowScan(ctx, []any{&temporary},
				`SELECT COUNT(*) FROM messages WHERE queue='b-temporary'`); err != nil {
				t.Fatal(err)
			}
			if permanent != 1 || temporary != 0 {
				t.Fatalf("fanout permanent/temporary=%d/%d, want 1/0", permanent, temporary)
			}
		})
	}
}

func TestEngineTxRefreshesSubscriptionExpiryAtEachTarget(t *testing.T) {
	t.Run("direct", func(t *testing.T) {
		ctx := context.Background()
		e, _ := testEngine(t)
		expires := mustDataPlaneSubscriptionTTL(t, e, "events", "temporary", 1_000)
		var reads int64
		e.now = func() int64 {
			if atomic.AddInt64(&reads, 1) == 1 { // route resolution is still live
				return expires - 1
			}
			return expires // the direct effect is rejected
		}
		err := e.Tx(ctx, func(tx *EngineTx) error {
			_, err := tx.SendOne("temporary", OutMessage{Body: []byte("late")})
			return err
		})
		if !errors.Is(err, ErrQueueNotFound) {
			t.Fatalf("transactional direct send=%v, want ErrQueueNotFound", err)
		}
		var count int
		if err := e.db.queryRowScan(ctx, []any{&count},
			`SELECT COUNT(*) FROM messages WHERE queue='temporary'`); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("transactional direct send inserted %d expired messages", count)
		}
	})

	t.Run("topic fanout", func(t *testing.T) {
		ctx := context.Background()
		e, _ := testEngine(t)
		if err := e.Subscribe(ctx, "events", "a-permanent", nil); err != nil {
			t.Fatal(err)
		}
		expires := mustDataPlaneSubscriptionTTL(t, e, "events", "b-temporary", 1_000)
		var reads int64
		e.now = func() int64 {
			if atomic.AddInt64(&reads, 1) == 1 { // shared publish/roster time
				return expires - 1
			}
			return expires
		}
		if err := e.Tx(ctx, func(tx *EngineTx) error {
			_, err := tx.SendOne("events", OutMessage{Body: []byte("fanout")})
			return err
		}); err != nil {
			t.Fatal(err)
		}
		var permanent, temporary int
		if err := e.db.queryRowScan(ctx, []any{&permanent},
			`SELECT COUNT(*) FROM messages WHERE queue='a-permanent'`); err != nil {
			t.Fatal(err)
		}
		if err := e.db.queryRowScan(ctx, []any{&temporary},
			`SELECT COUNT(*) FROM messages WHERE queue='b-temporary'`); err != nil {
			t.Fatal(err)
		}
		if permanent != 1 || temporary != 0 {
			t.Fatalf("transactional fanout permanent/temporary=%d/%d, want 1/0", permanent, temporary)
		}
	})
}

func TestTopicFanoutUsesOnePublishTimestamp(t *testing.T) {
	for _, mode := range []string{"send", "transaction"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			e, _ := testEngine(t)
			for _, name := range []string{"a", "b"} {
				if err := e.Subscribe(ctx, "events", name, &Filter{Expr: `enqueued_at.Hour() == 0`}); err != nil {
					t.Fatal(err)
				}
			}
			var reads int64
			e.now = func() int64 {
				// The first target would see midnight and the second 01:00 if fan-out
				// sampled the publish clock per target.
				if atomic.AddInt64(&reads, 1) <= 2 {
					return 0
				}
				return int64(time.Hour / time.Millisecond)
			}
			if mode == "send" {
				if _, err := e.SendOne(ctx, "events", OutMessage{Body: []byte("same publish")}); err != nil {
					t.Fatal(err)
				}
			} else if err := e.Tx(ctx, func(tx *EngineTx) error {
				_, err := tx.SendOne("events", OutMessage{Body: []byte("same publish")})
				return err
			}); err != nil {
				t.Fatal(err)
			}
			rows, err := e.db.sql.QueryContext(ctx, `SELECT queue,enqueued_at FROM messages ORDER BY queue`)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			var queues []string
			var timestamps []int64
			for rows.Next() {
				var queue string
				var enqueuedAt int64
				if err := rows.Scan(&queue, &enqueuedAt); err != nil {
					t.Fatal(err)
				}
				queues = append(queues, queue)
				timestamps = append(timestamps, enqueuedAt)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(queues, []string{"a", "b"}) {
				t.Fatalf("time-dependent filters routed to %v, want both subscriptions", queues)
			}
			if len(timestamps) != 2 || timestamps[0] != timestamps[1] {
				t.Fatalf("fan-out enqueued_at=%v, want one shared publish timestamp", timestamps)
			}
		})
	}
}

func TestEngineTxSendUsesCallTimeForSubscriptionExpiry(t *testing.T) {
	ctx := context.Background()
	e, clock := testEngine(t)
	expires := mustDataPlaneSubscriptionTTL(t, e, "events", "temporary", 1_000)
	err := e.Tx(ctx, func(tx *EngineTx) error {
		atomic.StoreInt64(clock, expires)
		_, err := tx.SendOne("temporary", OutMessage{Body: []byte("late")})
		return err
	})
	if !errors.Is(err, ErrQueueNotFound) {
		t.Fatalf("SendOne after an open transaction crossed expiry=%v", err)
	}
	var count int
	if err := e.db.queryRowScan(ctx, []any{&count}, `SELECT COUNT(*) FROM messages WHERE queue='temporary'`); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("expired transactional send inserted %d messages", count)
	}
}

func TestSendRefreshesSubscriptionExpiryAfterWriterWait(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	e, clock := testEngine(t)
	expires := mustDataPlaneSubscriptionTTL(t, e, "events", "temporary", 1_000)
	held, err := e.db.sql.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	before := e.db.sql.Stats().WaitCount
	done := make(chan error, 1)
	go func() {
		_, err := e.SendOne(ctx, "temporary", OutMessage{Body: []byte("late")})
		done <- err
	}()
	for e.db.sql.Stats().WaitCount == before {
		select {
		case err := <-done:
			t.Fatalf("send returned before waiting for writer: %v", err)
		case <-ctx.Done():
			t.Fatal("send never waited for writer")
		case <-time.After(time.Millisecond):
		}
	}
	atomic.StoreInt64(clock, expires)
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrQueueNotFound) {
			t.Fatalf("send admitted with stale pre-wait time: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("send did not finish after writer release")
	}
}

func TestSubscriptionExpiryBoundsLongPoll(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	e, clock := testEngine(t)
	expires := mustDataPlaneSubscriptionTTL(t, e, "events", "temporary", 75)
	go func() {
		time.Sleep(20 * time.Millisecond)
		atomic.StoreInt64(clock, expires)
	}()
	started := time.Now()
	_, err := e.Receive(ctx, "temporary", ReceiveOptions{WaitMs: 2_000})
	if !errors.Is(err, ErrQueueNotFound) {
		t.Fatalf("long poll at expiry=%v, want ErrQueueNotFound", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("long poll slept past subscription expiry: %v", elapsed)
	}
}

func TestRedriveDoesNotReinsertStaleCandidates(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "source", QueueConfig{})
	mustQueue(t, e, "target", QueueConfig{})
	var id int64
	if err := e.db.queryRowScan(ctx, []any{&id},
		`INSERT INTO messages(queue,state,enqueued_at,body) VALUES('source','dead_lettered',1,X'01') RETURNING id`); err != nil {
		t.Fatal(err)
	}
	if _, err := e.db.exec(ctx, `DELETE FROM messages WHERE id=?`, id); err != nil {
		t.Fatal(err)
	}
	moved, err := e.moveBatch(ctx, "target", "source", true, []dlqRec{{id: id}})
	if err != nil || moved != 0 {
		t.Fatalf("stale cross-queue move=%d, %v", moved, err)
	}
	var targetRows int
	if err := e.db.queryRowScan(ctx, []any{&targetRows}, `SELECT COUNT(*) FROM messages WHERE queue='target'`); err != nil {
		t.Fatal(err)
	}
	if targetRows != 0 {
		t.Fatalf("stale candidate was resurrected into target: %d rows", targetRows)
	}

	if err := e.db.queryRowScan(ctx, []any{&id},
		`INSERT INTO messages(queue,state,delivery_count,enqueued_at,body) VALUES('source','dead_lettered',7,1,X'02') RETURNING id`); err != nil {
		t.Fatal(err)
	}
	if _, err := e.db.exec(ctx, `UPDATE messages SET state='active' WHERE id=?`, id); err != nil {
		t.Fatal(err)
	}
	moved, err = e.moveBatch(ctx, "", "source", false, []dlqRec{{id: id}})
	if err != nil || moved != 0 {
		t.Fatalf("stale same-queue move=%d, %v", moved, err)
	}
	var state string
	var deliveries int
	if err := e.db.queryRowScan(ctx, []any{&state, &deliveries},
		`SELECT state,delivery_count FROM messages WHERE id=?`, id); err != nil {
		t.Fatal(err)
	}
	if state != string(StateActive) || deliveries != 7 {
		t.Fatalf("stale same-queue candidate was rewritten: state=%s delivery_count=%d", state, deliveries)
	}
}

func TestRateLimitedRedriveStopsWhenSubscriptionExpiresBetweenChunks(t *testing.T) {
	for _, expiringQueue := range []string{"source", "target"} {
		t.Run(expiringQueue, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			e, clock := testEngine(t)
			var expires int64
			if expiringQueue == "source" {
				expires = mustDataPlaneSubscriptionTTL(t, e, "source-topic", "source", 1_000)
				mustQueue(t, e, "target", QueueConfig{})
			} else {
				mustQueue(t, e, "source", QueueConfig{})
				expires = mustDataPlaneSubscriptionTTL(t, e, "target-topic", "target", 1_000)
			}
			for i := 0; i < 2; i++ {
				if _, err := e.db.exec(ctx,
					`INSERT INTO messages(queue,state,enqueued_at,body) VALUES('source','dead_lettered',1,X'01')`); err != nil {
					t.Fatal(err)
				}
			}
			expirySet := make(chan error, 1)
			go func() {
				for {
					var targetRows int
					if err := e.db.queryRowScan(ctx, []any{&targetRows},
						`SELECT COUNT(*) FROM messages WHERE queue='target'`); err != nil {
						expirySet <- err
						return
					}
					if targetRows == 1 {
						// Observe the committed first chunk before crossing the fixed
						// lifetime. This makes the inter-chunk boundary deterministic.
						atomic.StoreInt64(clock, expires)
						expirySet <- nil
						return
					}
					select {
					case <-ctx.Done():
						expirySet <- ctx.Err()
						return
					case <-time.After(time.Millisecond):
					}
				}
			}()
			moved, err := e.Redrive(ctx, "source", RedriveOptions{Target: "target", RatePerSec: 1})
			if setErr := <-expirySet; setErr != nil {
				t.Fatalf("advance subscription expiry: %v", setErr)
			}
			if moved != 1 || !errors.Is(err, ErrQueueNotFound) {
				t.Fatalf("redrive across %s expiry moved=%d err=%v, want 1 then ErrQueueNotFound", expiringQueue, moved, err)
			}
			var sourceRows, targetRows int
			if err := e.db.queryRowScan(ctx, []any{&sourceRows}, `SELECT COUNT(*) FROM messages WHERE queue='source'`); err != nil {
				t.Fatal(err)
			}
			if err := e.db.queryRowScan(ctx, []any{&targetRows}, `SELECT COUNT(*) FROM messages WHERE queue='target'`); err != nil {
				t.Fatal(err)
			}
			if sourceRows != 1 || targetRows != 1 {
				t.Fatalf("source/target rows=%d/%d, want 1/1", sourceRows, targetRows)
			}
		})
	}
}

func TestRedriveThrottleWaitIsCanceledByClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	e, _ := testEngine(t)

	// Freeze the injected clock before the throttle timer is created. Close can
	// publish its shutdown signal first, so this assertion contains no scheduler-
	// or storage-latency bound.
	entered := make(chan struct{})
	resume := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(resume)
		}
	}()
	var nowCalls int32
	e.now = func() int64 {
		if atomic.AddInt32(&nowCalls, 1) == 1 {
			close(entered)
			<-resume
		}
		return 1
	}

	waitDone := make(chan error, 1)
	go func() { waitDone <- e.waitRedriveThrottle(ctx, 10_000) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("redrive throttle wait did not start")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- e.Close() }()
	select {
	case <-e.closed:
	case <-ctx.Done():
		t.Fatal("Close did not publish the engine shutdown signal")
	}
	close(resume)
	released = true

	select {
	case err := <-waitDone:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("redrive throttle wait after Close = %v, want ErrClosed", err)
		}
	case <-ctx.Done():
		t.Fatal("redrive throttle ignored engine Close")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Close did not finish")
	}
}

func TestRedriveThrottleHandlesFarFutureSubscriptionExpiry(t *testing.T) {
	e, _ := testEngine(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := e.waitRedriveThrottle(ctx, math.MaxInt64); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("far-future expiry wait=%v, want context deadline", err)
	}
}
