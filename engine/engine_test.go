package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
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
		DB:                     "file:" + path,
		Now:                    func() int64 { return atomic.LoadInt64(&ms) },
		DisableBackground:      true,
		ReclaimUnexpiredLeases: true,
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

func TestQueueLockDurationValidation(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)

	for _, lock := range []int64{-1, 1, 999, 3_600_001, 9_223_372_036_854_775_807} {
		name := fmt.Sprintf("invalid-%d", lock)
		err := e.CreateQueue(ctx, name, QueueConfig{LockDurationMs: lock})
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("lock duration %d: error = %v, want ErrInvalidArgument", lock, err)
		}
	}
	queues, err := e.ListQueues(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(queues) != 0 {
		t.Fatalf("invalid configurations created queues: %+v", queues)
	}

	mustQueue(t, e, "default", QueueConfig{})
	mustQueue(t, e, "minimum", QueueConfig{LockDurationMs: 1_000})
	mustQueue(t, e, "maximum", QueueConfig{LockDurationMs: 3_600_000})
	for _, lock := range []int64{999, 3_600_001, 9_223_372_036_854_775_807} {
		if err := e.CreateQueue(ctx, "default", QueueConfig{LockDurationMs: lock}); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("invalid reconfiguration %d = %v, want ErrInvalidArgument", lock, err)
		}
	}
	queues, err = e.ListQueues(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]int64, len(queues))
	for _, q := range queues {
		got[q.Name] = q.LockDurationMs
	}
	if got["default"] != 30_000 || got["minimum"] != 1_000 || got["maximum"] != 3_600_000 {
		t.Fatalf("stored lock durations = %v, want default=30000 minimum=1000 maximum=3600000", got)
	}
}

func TestLegacyOutOfRangeQueuesRemainReadable(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy-lock-range.db")
	e, err := Open(ctx, Options{DB: "file:" + path, DisableBackground: true})
	if err != nil {
		t.Fatal(err)
	}
	mustQueue(t, e, "legacy", QueueConfig{})
	mustQueue(t, e, "legacy-long", QueueConfig{})
	if err := e.Subscribe(ctx, "events", "legacy-sub", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := e.db.exec(ctx, `UPDATE queues SET lock_duration_ms=50 WHERE name IN ('legacy','legacy-sub')`); err != nil {
		t.Fatal(err)
	}
	if _, err := e.db.exec(ctx, `UPDATE queues SET lock_duration_ms=7200000 WHERE name='legacy-long'`); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(ctx, Options{DB: "file:" + path, DisableBackground: true})
	if err != nil {
		t.Fatalf("open legacy database: %v", err)
	}
	defer reopened.Close()
	if err := reopened.Subscribe(ctx, "events", "legacy-sub", nil); err != nil {
		t.Fatalf("update legacy subscription without reconfiguring its queue: %v", err)
	}
	assertLegacyLocks := func() {
		t.Helper()
		queues, err := reopened.ListQueues(ctx)
		if err != nil {
			t.Fatal(err)
		}
		got := make(map[string]int64)
		for _, q := range queues {
			if q.Name == "legacy" || q.Name == "legacy-sub" || q.Name == "legacy-long" {
				got[q.Name] = q.LockDurationMs
			}
		}
		if len(got) != 3 || got["legacy"] != 50 || got["legacy-sub"] != 50 || got["legacy-long"] != 7_200_000 {
			t.Fatalf("legacy lock durations = %v, want short queues at 50ms and long queue at 7200000ms", got)
		}
	}
	assertLegacyLocks()
	if _, err := reopened.SendOne(ctx, "legacy", OutMessage{Body: []byte("still usable")}); err != nil {
		t.Fatal(err)
	}
	msgs, err := reopened.Receive(ctx, "legacy", ReceiveOptions{Mode: ReceiveAndDelete})
	if err != nil || len(msgs) != 1 {
		t.Fatalf("use legacy queue: messages=%d error=%v", len(msgs), err)
	}
	if err := reopened.CreateQueue(ctx, "legacy", QueueConfig{LockDurationMs: 50}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("legacy short-lock reconfiguration = %v, want ErrInvalidArgument", err)
	}
	if err := reopened.CreateQueue(ctx, "legacy-long", QueueConfig{LockDurationMs: 7_200_000}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("legacy long-lock reconfiguration = %v, want ErrInvalidArgument", err)
	}
	assertLegacyLocks()
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

// An exclusive embedded owner may explicitly reclaim live orphaned locks after a restart.
func TestExplicitCrashRecovery(t *testing.T) {
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
func TestExplicitCrashRecoveryRespectsMaxDelivery(t *testing.T) {
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

// Safe startup recovery retains an unexpired token by default so a delivery can
// be replayed, renewed and completed instead of being duplicated or dead-lettered.
func TestOpenPreservesUnexpiredLeaseByDefault(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "mq.db")

	e1, clock := testEngineAt(t, path)
	mustQueue(t, e1, "q", QueueConfig{MaxDeliveryCount: 1})
	if _, err := e1.SendOne(ctx, "q", OutMessage{Body: []byte("in-flight")}); err != nil {
		t.Fatal(err)
	}
	claimed, err := e1.Receive(ctx, "q", ReceiveOptions{AttemptID: "restart-attempt"})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: messages=%+v err=%v", claimed, err)
	}
	want := claimed[0]
	if err := e1.Close(); err != nil {
		t.Fatal(err)
	}

	e2, err := Open(ctx, Options{
		DB:                "file:" + path,
		Now:               func() int64 { return atomic.LoadInt64(clock) },
		DisableBackground: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	stats, err := e2.Stats(ctx, "q")
	if err != nil {
		t.Fatal(err)
	}
	if stats.Locked != 1 || stats.Active != 0 || stats.DeadLettered != 0 {
		t.Fatalf("preserved lease stats = %+v, want one locked message", stats)
	}
	replayed, err := e2.Receive(ctx, "q", ReceiveOptions{AttemptID: "restart-attempt"})
	if err != nil || len(replayed) != 1 {
		t.Fatalf("attempt replay: messages=%+v err=%v", replayed, err)
	}
	if got := replayed[0]; got.SeqNumber != want.SeqNumber || got.LockToken != want.LockToken || got.DeliveryCount != want.DeliveryCount {
		t.Fatalf("replayed delivery = %+v, want seq=%d token=%q delivery=%d", got, want.SeqNumber, want.LockToken, want.DeliveryCount)
	}
	renewed, err := e2.RenewBatch(ctx, "q", []SettleItem{{SeqNumber: want.SeqNumber, LockToken: want.LockToken}})
	if err != nil || len(renewed) != 1 || !renewed[0].Ok {
		t.Fatalf("renew preserved lease = %+v, %v", renewed, err)
	}
	if err := e2.Complete(ctx, "q", want.SeqNumber, want.LockToken); err != nil {
		t.Fatalf("complete preserved lease: %v", err)
	}
	stats, err = e2.Stats(ctx, "q")
	if err != nil || stats.Total != 0 {
		t.Fatalf("stats after complete = %+v, %v; want empty queue", stats, err)
	}
}

// Default startup recovery preserves only a live lease. The exact expiry boundary retains
// the existing recovery rule: below the delivery limit it returns to active,
// while the final allowed delivery moves to the DLQ.
func TestOpenReclaimsExpiredLeasesByDefault(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "mq.db")

	e1, clock := testEngineAt(t, path)
	mustQueue(t, e1, "last", QueueConfig{LockDurationMs: 1_000, MaxDeliveryCount: 1})
	mustQueue(t, e1, "spare", QueueConfig{LockDurationMs: 1_000, MaxDeliveryCount: 2})
	if _, err := e1.SendOne(ctx, "last", OutMessage{}); err != nil {
		t.Fatal(err)
	}
	if _, err := e1.SendOne(ctx, "spare", OutMessage{}); err != nil {
		t.Fatal(err)
	}
	if recvOne(t, e1, "last") == nil || recvOne(t, e1, "spare") == nil {
		t.Fatal("expected both messages to be locked")
	}
	if err := e1.Close(); err != nil {
		t.Fatal(err)
	}
	advance(clock, time.Second) // locked_until == now is expired.

	e2, err := Open(ctx, Options{
		DB:                "file:" + path,
		Now:               func() int64 { return atomic.LoadInt64(clock) },
		DisableBackground: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	last, err := e2.Stats(ctx, "last")
	if err != nil || last.DeadLettered != 1 || last.Locked != 0 {
		t.Fatalf("last-attempt recovery = %+v, %v", last, err)
	}
	spare, err := e2.Stats(ctx, "spare")
	if err != nil || spare.Active != 1 || spare.Locked != 0 {
		t.Fatalf("under-limit recovery = %+v, %v", spare, err)
	}
	if got := recvOne(t, e2, "spare"); got == nil || got.DeliveryCount != 2 {
		t.Fatalf("recovered delivery = %+v, want delivery count 2", got)
	}
}

func TestOpenRejectsConflictingLeaseRecoveryOptions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "must-not-open.db")
	e, err := Open(context.Background(), Options{
		DB:                      "file:" + path,
		DisableBackground:       true,
		PreserveUnexpiredLeases: true, //nolint:staticcheck // compatibility test intentionally exercises the deprecated field
		ReclaimUnexpiredLeases:  true,
	})
	if e != nil || !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("conflicting recovery options = engine %v, error %v; want ErrInvalidArgument", e, err)
	}
	for _, name := range []string{path, path + ".lock"} {
		if _, statErr := os.Stat(name); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("conflicting recovery options touched %s: %v", name, statErr)
		}
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

// Go-native benchmarks for the embedded engine (no HTTP) — run with:
//
//	go test -bench=. -benchmem ./engine
//	go test -bench=BenchmarkSendBatch64 -benchtime=3s ./engine
//
// These measure the in-process SDK path the same way real embedded users drive it,
// and complement the container-based throughput/disk-IO matrix in test/bench/. Each
// reports a msg/s metric alongside the standard ns/op + allocs.

func benchEngine(b *testing.B) *Engine {
	b.Helper()
	e, err := Open(context.Background(), Options{
		DB:                "file:" + filepath.Join(b.TempDir(), "mq.db"),
		DisableBackground: true,
	})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	b.Cleanup(func() { _ = e.Close() })
	return e
}

func BenchmarkSendOne(b *testing.B) {
	ctx := context.Background()
	e := benchEngine(b)
	_ = e.CreateQueue(ctx, "q", QueueConfig{})
	body := make([]byte, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := e.SendOne(ctx, "q", OutMessage{Body: body}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msg/s")
}

func BenchmarkSendBatch64(b *testing.B) {
	ctx := context.Background()
	e := benchEngine(b)
	_ = e.CreateQueue(ctx, "q", QueueConfig{})
	const batch = 64
	msgs := make([]OutMessage, batch)
	for i := range msgs {
		msgs[i] = OutMessage{Body: make([]byte, 256)}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := e.Send(ctx, "q", msgs...); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N*batch)/b.Elapsed().Seconds(), "msg/s")
}

// benchmarkTopicFanout exercises routing plus enqueue for a stable topic roster.
// Queue configuration is carried by the roster JOIN; an accidental per-subscription
// metadata lookup makes this path visibly regress as fanout grows, especially on a
// remote store where every lookup is a round trip.
func benchmarkTopicFanout(b *testing.B, subscriptions int, subscriptionTTLMs int64) {
	ctx := context.Background()
	e := benchEngine(b)
	for i := 0; i < subscriptions; i++ {
		name := fmt.Sprintf("sub-%02d", i)
		var err error
		if subscriptionTTLMs > 0 {
			err = e.SubscribeWithOptions(ctx, "topic", name, nil, SubscribeOptions{TTLMs: subscriptionTTLMs})
		} else {
			err = e.Subscribe(ctx, "topic", name, nil)
		}
		if err != nil {
			b.Fatal(err)
		}
	}
	body := make([]byte, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := e.SendOne(ctx, "topic", OutMessage{Body: body}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "publish/s")
	b.ReportMetric(float64(b.N*subscriptions)/b.Elapsed().Seconds(), "delivery/s")
}

func BenchmarkTopicFanout1(b *testing.B)   { benchmarkTopicFanout(b, 1, 0) }
func BenchmarkTopicFanout32(b *testing.B)  { benchmarkTopicFanout(b, 32, 0) }
func BenchmarkTopicFanout256(b *testing.B) { benchmarkTopicFanout(b, 256, 0) }

// BenchmarkTopicFanoutTTL32 isolates the per-effect expiry fence for fixed-life
// subscriptions. Its one-hour deadlines remain live for the whole benchmark.
func BenchmarkTopicFanoutTTL32(b *testing.B) { benchmarkTopicFanout(b, 32, 3_600_000) }

// BenchmarkSubscriptionCreateDelete measures serialized, one-use subscription
// churn with low and high retained metadata cardinality. Seed setup is untimed.
// Each lifecycle uses a fresh name and deletes an empty subscription, which is the
// safe request/reply cleanup case without an ABA name reuse race.
func BenchmarkSubscriptionCreateDelete(b *testing.B) {
	for _, inventory := range []int{100, 10_000} {
		b.Run(fmt.Sprintf("Retained%d", inventory), func(b *testing.B) {
			ctx := context.Background()
			e, err := Open(ctx, Options{
				DB:                "file:" + filepath.Join(b.TempDir(), "mq.db"),
				DisableBackground: true,
				MaxSubscriptions:  inventory + 1,
			})
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			b.Cleanup(func() { _ = e.Close() })
			if _, err := e.db.exec(ctx, `WITH RECURSIVE n(i) AS (
				SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<?
			) INSERT INTO queues(name,kind,created_at,updated_at)
			  SELECT printf('retained-%05d',i),'subscription',1,1 FROM n`, inventory-1); err != nil {
				b.Fatal(err)
			}
			if _, err := e.db.exec(ctx, `
				INSERT INTO subscriptions(topic,subscription,created_at)
				SELECT 'retained',name,1 FROM queues WHERE kind='subscription'`); err != nil {
				b.Fatal(err)
			}

			names := make([]string, b.N)
			for i := range names {
				names[i] = fmt.Sprintf("temporary-%08d", i)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for _, name := range names {
				if err := e.Subscribe(ctx, "replies", name, nil); err != nil {
					b.Fatal(err)
				}
				result, err := e.DeleteSubscriptionWithOptions(ctx, "replies", name, DeleteOptions{})
				if err != nil {
					b.Fatal(err)
				}
				if !result.Deleted {
					b.Fatalf("empty subscription %q remained in state %q", name, result.State)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "lifecycle/s")
		})
	}
}

// BenchmarkListSubscriptionsPage measures the first 100-entry page together with
// its exact snapshot total at representative inventories. Setup is untimed. The
// result is page latency; it does not claim that creating 10,000 subscriptions is
// a low-latency operation.
func BenchmarkListSubscriptionsPage(b *testing.B) {
	for _, inventory := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("Inventory%d/Page100", inventory), func(b *testing.B) {
			ctx := context.Background()
			e := benchEngine(b)
			if _, err := e.db.exec(ctx, `WITH RECURSIVE n(i) AS (
				SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<?
			) INSERT INTO queues(name,kind,created_at,updated_at)
			  SELECT printf('sub-%05d',i),'subscription',1,1 FROM n`, inventory-1); err != nil {
				b.Fatal(err)
			}
			if _, err := e.db.exec(ctx, `
				INSERT INTO subscriptions(topic,subscription,created_at)
				SELECT 'inventory',name,1 FROM queues WHERE kind='subscription'`); err != nil {
				b.Fatal(err)
			}

			options := ListSubscriptionsOptions{Topic: "inventory", Limit: defaultSubscriptionPageSize}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				page, err := e.ListSubscriptionsWithOptions(ctx, options)
				if err != nil {
					b.Fatal(err)
				}
				if page.Total != inventory || len(page.Subscriptions) != 100 {
					b.Fatalf("page total/items = %d/%d, want %d/100", page.Total, len(page.Subscriptions), inventory)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "page/s")
		})
	}
}

// BenchmarkSubscriptionExpiryNoDue measures the once-per-second discovery pass
// at the default metadata ceiling when every fixed lifetime is still in the
// future. This is the steady-state cost paid by a broker that uses temporary
// subscriptions but has no expiry work to perform.
func BenchmarkSubscriptionExpiryNoDue(b *testing.B) {
	ctx := context.Background()
	e := benchEngine(b)
	now := e.now()
	const inventory = 10_000
	if _, err := e.db.exec(ctx, `WITH RECURSIVE n(i) AS (
		SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<?
	) INSERT INTO queues(name,kind,lifecycle_state,expires_at,created_at,updated_at)
	  SELECT printf('ttl-%05d',i),'subscription','active',?,?,? FROM n`,
		inventory-1, now+3_600_000, now, now); err != nil {
		b.Fatal(err)
	}
	if _, err := e.db.exec(ctx, `INSERT INTO subscriptions(topic,subscription,created_at)
		SELECT 'temporary',name,created_at FROM queues WHERE kind='subscription'`); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		changed, pending, err := e.transitionExpiredSubscriptionsPass(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if changed != 0 || pending {
			b.Fatalf("no-due pass changed=%d pending=%t", changed, pending)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "pass/s")
}

// BenchmarkSubscriptionExpiryBatch1000 measures one full indexed transition
// transaction. Reactivating the fixed roster is untimed; physical cleanup is
// covered by the lifecycle benchmarks because expired subscriptions enter that
// same deletion path after this transition.
func BenchmarkSubscriptionExpiryBatch1000(b *testing.B) {
	ctx := context.Background()
	e := benchEngine(b)
	now := e.now()
	const inventory = subscriptionExpiryBatch
	if _, err := e.db.exec(ctx, `WITH RECURSIVE n(i) AS (
		SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<?
	) INSERT INTO queues(name,kind,lifecycle_state,expires_at,created_at,updated_at)
	  SELECT printf('due-%04d',i),'subscription','active',?,?,? FROM n`,
		inventory-1, now, now-1, now-1); err != nil {
		b.Fatal(err)
	}
	if _, err := e.db.exec(ctx, `INSERT INTO subscriptions(topic,subscription,created_at)
		SELECT 'temporary',name,created_at FROM queues WHERE kind='subscription'`); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		if _, err := e.db.exec(ctx, `UPDATE queues
			SET lifecycle_state='active',delete_started_at=0,updated_at=?
			WHERE kind='subscription'`, now-1); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		changed, pending, err := e.transitionExpiredSubscriptionsPass(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if changed != inventory || !pending {
			b.Fatalf("full due pass changed=%d pending=%t", changed, pending)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N*inventory)/b.Elapsed().Seconds(), "subscription/s")
}

func BenchmarkReceiveComplete(b *testing.B) {
	ctx := context.Background()
	e := benchEngine(b)
	_ = e.CreateQueue(ctx, "q", QueueConfig{LockDurationMs: 600_000, MaxDeliveryCount: 100})
	body := make([]byte, 256)
	for i := 0; i < b.N; i++ { // prefill (untimed)
		if _, err := e.SendOne(ctx, "q", OutMessage{Body: body}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for got := 0; got < b.N; {
		msgs, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 64})
		if err != nil {
			b.Fatal(err)
		}
		for _, m := range msgs {
			if err := e.Complete(ctx, "q", m.SeqNumber, m.LockToken); err != nil {
				b.Fatal(err)
			}
			got++
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msg/s")
}

// BenchmarkDrainConcurrent measures the path the MQLITE-50 fix actually changed: many
// consumers draining one queue with Receive(batch)+CompleteBatch. The claim is a write,
// so all workers serialize on the single writer — this is where N-commits-per-Receive
// used to collapse. The msg/s metric is the regression signal: prefill b.N, drain with
// `workers` goroutines, report throughput. (Single-threaded dequeue is BenchmarkReceiveComplete.)
func BenchmarkDrainConcurrent(b *testing.B) {
	ctx := context.Background()
	e := benchEngine(b)
	_ = e.CreateQueue(ctx, "q", QueueConfig{LockDurationMs: 600_000, MaxDeliveryCount: 100})
	body := make([]byte, 256)
	for i := 0; i < b.N; i++ { // prefill (untimed)
		if _, err := e.SendOne(ctx, "q", OutMessage{Body: body}); err != nil {
			b.Fatal(err)
		}
	}
	const workers = 16
	var mu sync.Mutex
	var benchErr error
	fail := func(err error) {
		mu.Lock()
		if benchErr == nil {
			benchErr = err
		}
		mu.Unlock()
	}
	b.ReportAllocs()
	b.ResetTimer()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				msgs, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 50})
				if err != nil {
					fail(err)
					return
				}
				if len(msgs) == 0 {
					return // drained
				}
				items := make([]SettleItem, len(msgs))
				for i, m := range msgs {
					items[i] = SettleItem{SeqNumber: m.SeqNumber, LockToken: m.LockToken}
				}
				if _, err := e.CompleteBatch(ctx, "q", items); err != nil {
					fail(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
	if benchErr != nil {
		b.Fatal(benchErr)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msg/s")
}

func BenchmarkE2E(b *testing.B) {
	ctx := context.Background()
	e := benchEngine(b)
	_ = e.CreateQueue(ctx, "q", QueueConfig{LockDurationMs: 600_000, MaxDeliveryCount: 100})
	body := make([]byte, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := e.SendOne(ctx, "q", OutMessage{Body: body}); err != nil {
			b.Fatal(err)
		}
		msgs, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 1})
		if err != nil {
			b.Fatal(err)
		}
		for _, m := range msgs {
			if err := e.Complete(ctx, "q", m.SeqNumber, m.LockToken); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msg/s")
}

func eachLocalStore(t *testing.T, fn func(t *testing.T, dsn string)) {
	t.Helper()
	for _, dsn := range []string{":memory:", "file:" + filepath.Join(t.TempDir(), "mq.db")} {
		name := "memory"
		if dsn != ":memory:" {
			name = "file"
		}
		t.Run(name, func(t *testing.T) { fn(t, dsn) })
	}
}

// A caller who has ALREADY given up must not start work: nothing runs, so nothing can commit.
func TestPreCancelledNeverExecutes(t *testing.T) {
	eachLocalStore(t, func(t *testing.T, dsn string) {
		ctx := context.Background()
		e, err := Open(ctx, Options{DB: dsn, DisableBackground: true})
		if err != nil {
			t.Fatal(err)
		}
		defer e.Close()
		mustQueue(t, e, "q", QueueConfig{})

		dead, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := e.SendOne(dead, "q", OutMessage{Body: []byte("must-not-exist")}); err == nil {
			t.Error("a pre-cancelled Send returned success")
		}
		if m, _ := e.Stats(ctx, "q"); m.Total != 0 {
			t.Errorf("a pre-cancelled Send wrote %d message(s)", m.Total)
		}
	})
}

// A caller WAITING for the single writer must honour its own deadline — and must not commit after
// it. This is the regression the round-4 review caught in the first attempt at this fix (a write
// that waited out the holder and committed 500ms after the client had gone). It must never return.
func TestCancelWhileWaitingWritesNothing(t *testing.T) {
	eachLocalStore(t, func(t *testing.T, dsn string) {
		ctx := context.Background()
		e, err := Open(ctx, Options{DB: dsn, DisableBackground: true})
		if err != nil {
			t.Fatal(err)
		}
		defer e.Close()
		mustQueue(t, e, "q", QueueConfig{})

		held := make(chan struct{})
		go func() {
			_ = e.Tx(ctx, func(tx *EngineTx) error {
				close(held)
				time.Sleep(500 * time.Millisecond) // occupy the single writer
				return nil
			})
		}()
		<-held

		start := time.Now()
		short, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
		_, err = e.SendOne(short, "q", OutMessage{Body: []byte("must-not-commit")})
		took := time.Since(start)
		cancel()

		if err == nil {
			t.Error("a Send that timed out while queued returned success")
		}
		if took > 300*time.Millisecond {
			t.Errorf("a queued Send ignored its 80ms deadline (took %s)", took)
		}
		time.Sleep(600 * time.Millisecond) // the holder finishes; nothing may appear afterwards
		if m, _ := e.Stats(ctx, "q"); m.Total != 0 {
			t.Errorf("the timed-out Send committed LATE (total=%d)", m.Total)
		}
	})
}

// Cancelling EXECUTING statements — the renewer does this by design on every receive — must leave
// the database usable. Interrupting a local statement leaks the connection: the file then stays
// locked (SQLITE_BUSY, permanently) and `:memory:` is destroyed outright ("no such table:
// messages"). Both were reproducible; ~40% of runs before the fix.
func TestCancelStormLeavesTheDatabaseUsable(t *testing.T) {
	eachLocalStore(t, func(t *testing.T, dsn string) {
		ctx := context.Background()
		e, err := Open(ctx, Options{DB: dsn, DisableBackground: true})
		if err != nil {
			t.Fatal(err)
		}
		defer e.Close()
		mustQueue(t, e, "q", QueueConfig{LockDurationMs: 60_000})
		seed := make([]OutMessage, 64)
		for i := range seed {
			seed[i] = OutMessage{Body: []byte("m")}
		}
		if _, err := e.Send(ctx, "q", seed...); err != nil {
			t.Fatal(err)
		}
		msgs, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 64})
		if err != nil || len(msgs) != 64 {
			t.Fatalf("receive: %v n=%d", err, len(msgs))
		}
		items := make([]SettleItem, len(msgs))
		for i, m := range msgs {
			items[i] = SettleItem{SeqNumber: m.SeqNumber, LockToken: m.LockToken}
		}

		// Reads, writes, batches and transactions, all interrupted mid-flight.
		for i := 0; i < 200; i++ {
			cctx, cancel := context.WithTimeout(context.Background(), time.Duration(i%5)*100*time.Microsecond)
			_, _ = e.RenewBatch(cctx, "q", items)                             // set-based write
			_, _ = e.SendOne(cctx, "q", OutMessage{Body: []byte("x")})        // transaction
			_ = e.Complete(cctx, "q", items[0].SeqNumber, items[0].LockToken) // settle
			_, _ = e.Peek(cctx, "q", PeekOptions{Max: 10})                    // read
			_ = e.Tx(cctx, func(tx *EngineTx) error { _, e := tx.SendOne("q", OutMessage{Body: []byte("t")}); return e })
			cancel()
		}

		if _, err := e.Stats(ctx, "q"); err != nil {
			t.Fatalf("the database is unusable after cancellations: %v", err)
		}
		if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte("after")}); err != nil {
			t.Fatalf("cannot write after cancellations: %v", err)
		}
		if _, err := e.Peek(ctx, "q", PeekOptions{Max: 1}); err != nil {
			t.Fatalf("cannot read after cancellations: %v", err)
		}
	})
}

// Two `:memory:` engines are still two separate databases.
func TestMemoryEnginesStayIsolated(t *testing.T) {
	ctx := context.Background()
	a, err := Open(ctx, Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := Open(ctx, Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	mustQueue(t, a, "only-in-a", QueueConfig{})
	if _, err := b.Stats(ctx, "only-in-a"); err == nil {
		t.Error("a queue created in one :memory: engine is visible in another")
	}
}

// A transaction cancelled BETWEEN its statements must roll back. The statements are protected from
// interruption; the transaction is not. Committing here would persist work the caller had already
// abandoned — "a statement already executing finishes" is not "a transaction already begun
// commits" (codex, round-5 follow-up).
func TestTxCancelledBetweenStatementsRollsBack(t *testing.T) {
	eachLocalStore(t, func(t *testing.T, dsn string) {
		ctx := context.Background()
		e, err := Open(ctx, Options{DB: dsn, DisableBackground: true})
		if err != nil {
			t.Fatal(err)
		}
		defer e.Close()
		mustQueue(t, e, "q", QueueConfig{})

		cctx, cancel := context.WithCancel(context.Background())
		err = e.Tx(cctx, func(tx *EngineTx) error {
			if _, err := tx.SendOne("q", OutMessage{Body: []byte("first")}); err != nil {
				return err
			}
			cancel() // the caller gives up, mid-callback, between statements
			if _, err := tx.SendOne("q", OutMessage{Body: []byte("second")}); err != nil {
				return err
			}
			return nil
		})
		if err == nil {
			t.Error("a transaction cancelled mid-callback reported success")
		}
		if m, _ := e.Stats(ctx, "q"); m.Total != 0 {
			t.Errorf("a cancelled transaction committed %d message(s) — it must roll back", m.Total)
		}
	})
}

// The storm above completes the same message over and over, so after the first success every later
// Complete is a no-op and its WRITE is never actually interrupted — a hole codex found in my own
// acceptance test. This one gives every iteration a fresh, genuinely-locked message, so the fenced
// settle write is the thing being cancelled.
func TestCancelledSettleWritesLeaveTheDatabaseUsable(t *testing.T) {
	eachLocalStore(t, func(t *testing.T, dsn string) {
		ctx := context.Background()
		e, err := Open(ctx, Options{DB: dsn, DisableBackground: true})
		if err != nil {
			t.Fatal(err)
		}
		defer e.Close()
		mustQueue(t, e, "q", QueueConfig{LockDurationMs: 600_000, MaxDeliveryCount: 100})

		for i := 0; i < 150; i++ {
			if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte("m")}); err != nil {
				t.Fatalf("send %d: %v", i, err)
			}
			msgs, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 1})
			if err != nil || len(msgs) != 1 {
				t.Fatalf("receive %d: %v n=%d", i, err, len(msgs))
			}
			m := msgs[0]

			// Cancel while the fenced settle write is in flight. Each verb, in turn.
			cctx, cancel := context.WithTimeout(context.Background(), time.Duration(i%4)*80*time.Microsecond)
			switch i % 4 {
			case 0:
				_ = e.Complete(cctx, "q", m.SeqNumber, m.LockToken)
			case 1:
				_ = e.Abandon(cctx, "q", m.SeqNumber, m.LockToken, 0)
			case 2:
				_ = e.Reject(cctx, "q", m.SeqNumber, m.LockToken, ReasonAppRequested, "")
			case 3:
				_ = e.Defer(cctx, "q", m.SeqNumber, m.LockToken)
			}
			cancel()
		}

		if _, err := e.Stats(ctx, "q"); err != nil {
			t.Fatalf("the database is unusable after cancelled settle writes: %v", err)
		}
		if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte("after")}); err != nil {
			t.Fatalf("cannot write after cancelled settles: %v", err)
		}
	})
}

// The window between "the wait returned" and "the statement starts" is real: past that point
// cancellation is deliberately gone, so a caller who gives up THERE would otherwise have a
// destructive operation run on its behalf anyway. Nothing may BEGIN for a caller who has given up
// (codex, round-5 follow-up).
//
// Cancel the caller while it is queued behind the writer, then let the writer go: the operation
// must not run, whatever the scheduling.
func TestCancelInTheHandoffWindowStartsNothing(t *testing.T) {
	eachLocalStore(t, func(t *testing.T, dsn string) {
		ctx := context.Background()
		e, err := Open(ctx, Options{DB: dsn, DisableBackground: true})
		if err != nil {
			t.Fatal(err)
		}
		defer e.Close()
		mustQueue(t, e, "q", QueueConfig{})
		if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte("keep-me")}); err != nil {
			t.Fatal(err)
		}
		msgs, _ := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 1})
		if len(msgs) != 1 {
			t.Fatal("receive")
		}
		if err := e.Reject(ctx, "q", msgs[0].SeqNumber, msgs[0].LockToken, ReasonAppRequested, ""); err != nil {
			t.Fatal(err) // one dead letter to purge
		}

		// Hold the writer, queue a destructive Purge behind it, and cancel that caller while it waits.
		release := make(chan struct{})
		held := make(chan struct{})
		go func() {
			_ = e.Tx(ctx, func(tx *EngineTx) error {
				close(held)
				<-release
				return nil
			})
		}()
		<-held

		cctx, cancel := context.WithCancel(context.Background())

		// Cancel the caller EXACTLY in the handoff window: the connection is in hand, the statement
		// has not started. Cancelling any earlier is uninteresting — the wait itself would fail.
		afterConnAcquired = func() { cancel() }
		defer func() { afterConnAcquired = nil }()

		done := make(chan error, 1)
		go func() {
			_, perr := e.Purge(cctx, "q", RedriveOptions{})
			done <- perr
		}()
		time.Sleep(50 * time.Millisecond) // queued behind the writer
		close(release)                    // the writer lets go; the purge acquires the conn, then dies

		if err := <-done; err == nil {
			t.Error("a Purge whose caller had given up ran anyway")
		}
		if m, _ := e.Stats(ctx, "q"); m.DeadLettered != 1 {
			t.Errorf("dead_lettered=%d — the cancelled Purge destroyed data on behalf of a caller who was gone", m.DeadLettered)
		}
	})
}

// A transaction whose caller has given up must stop ISSUING statements — not grind through the
// rest of the closure and roll back at the end.
//
// The statements inside a local transaction deliberately run on an uninterruptible context, which
// is what keeps an in-flight write from wedging the database. The trap is that "uninterruptible"
// used to mean the closure kept going: a cancelled caller's 256-message Send still executed all
// 256 inserts. The rule for mqlite-owned statements is per-statement, and *txn enforces it:
// a statement already executing finishes; the next one does not begin. Raw EngineTx.SQL calls
// bypass these guards; the public boundary is pinned in sdk_test.go (MQLITE-103).
func TestCancelledTransactionStopsIssuingStatements(t *testing.T) {
	statements := []struct {
		name string
		run  func(context.Context, *txn) error
	}{
		{"ExecContext", func(ctx context.Context, tx *txn) error {
			_, err := tx.ExecContext(ctx, `INSERT INTO meta(key,value) VALUES ('probe2','1')`)
			return err
		}},
		{"QueryContext", func(ctx context.Context, tx *txn) error {
			rows, err := tx.QueryContext(ctx, `INSERT INTO meta(key,value) VALUES ('probe2','1') RETURNING value`)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
			}
			return rows.Err()
		}},
		{"QueryRowContext", func(ctx context.Context, tx *txn) error {
			var value string
			return tx.QueryRowContext(ctx, `INSERT INTO meta(key,value) VALUES ('probe2','1') RETURNING value`).Scan(&value)
		}},
	}
	var want, got []string
	for _, statement := range statements {
		want = append(want, statement.name)
	}
	want = append(want, "SQL") // The raw escape hatch is intentionally outside the guard.
	txType := reflect.TypeOf((*txn)(nil))
	for i := 0; i < txType.NumMethod(); i++ {
		got = append(got, txType.Method(i).Name)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("txn methods = %v, want %v; extend the cancellation checks for new methods", got, want)
	}
	eachLocalStore(t, func(t *testing.T, dsn string) {
		ctx := context.Background()
		e, err := Open(ctx, Options{DB: dsn, DisableBackground: true})
		if err != nil {
			t.Fatal(err)
		}
		defer e.Close()
		mustQueue(t, e, "q", QueueConfig{LockDurationMs: 60_000, MaxDeliveryCount: 10})

		for _, statement := range statements {
			t.Run(statement.name, func(t *testing.T) {
				cctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var second error
				txErr := e.inTx(cctx, func(ec context.Context, tx *txn) error {
					if _, err := tx.ExecContext(ec, `INSERT INTO meta(key,value) VALUES ('probe','1')`); err != nil {
						t.Fatalf("the FIRST statement runs normally: %v", err)
					}
					cancel() // the caller gives up, mid-transaction
					second = statement.run(ec, tx)
					return second
				})
				if !errors.Is(second, context.Canceled) {
					t.Fatalf("the statement AFTER the cancellation must not be issued at all; got err=%v", second)
				}
				if !errors.Is(txErr, context.Canceled) {
					t.Fatalf("inTx must surface the cancellation, got %v", txErr)
				}

				// ...and the transaction rolled back, so not even the first statement survives.
				var n int
				if err := e.db.queryRowScan(context.Background(), []any{&n},
					`SELECT COUNT(*) FROM meta WHERE key IN ('probe','probe2')`); err != nil {
					t.Fatal(err)
				}
				if n != 0 {
					t.Fatalf("a cancelled transaction committed %d row(s) — it must roll back entirely", n)
				}
			})
		}

		// The database is still perfectly usable: nothing was interrupted, so nothing leaked.
		if _, err := e.SendOne(context.Background(), "q", OutMessage{Body: []byte("after")}); err != nil {
			t.Fatalf("the store is unusable after a cancelled transaction: %v", err)
		}
	})
}

// A panicking transaction callback must not take the writer down with it.
//
// Engine.Tx runs USER code. If it panics, unwinding hits withConn's `defer conn.Close()` — and
// Close BLOCKS while the transaction is still open. Without a deferred rollback the panic never
// even reaches the caller: the goroutine parks inside Close, and the process's one local writer is
// wedged for good (codex, round-6). Deadlocks do not announce themselves, so this test is written
// to fail by TIMING OUT rather than to hang the suite.
func TestPanickingTransactionDoesNotWedgeTheWriter(t *testing.T) {
	eachLocalStore(t, func(t *testing.T, dsn string) {
		ctx := context.Background()
		e, err := Open(ctx, Options{DB: dsn, DisableBackground: true})
		if err != nil {
			t.Fatal(err)
		}
		defer e.Close()
		mustQueue(t, e, "q", QueueConfig{LockDurationMs: 60_000, MaxDeliveryCount: 10})

		panicked := make(chan any, 1)
		go func() {
			defer func() { panicked <- recover() }()
			_ = e.inTx(ctx, func(ec context.Context, tx *txn) error {
				if _, err := tx.ExecContext(ec, `INSERT INTO meta(key,value) VALUES ('boom','1')`); err != nil {
					t.Errorf("setup statement: %v", err)
				}
				panic("the callback blew up")
			})
		}()

		select {
		case got := <-panicked:
			if got != "the callback blew up" {
				t.Fatalf("the panic must reach the caller unchanged, got %v", got)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("the panic never surfaced: the connection is still held by an open transaction " +
				"and Close is blocked on it — the single writer is wedged")
		}

		// The writer survived, and the panicking transaction committed nothing.
		done := make(chan error, 1)
		go func() {
			_, serr := e.SendOne(context.Background(), "q", OutMessage{Body: []byte("after")})
			done <- serr
		}()
		select {
		case serr := <-done:
			if serr != nil {
				t.Fatalf("the store is unusable after a panicking transaction: %v", serr)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("the writer is wedged after a panicking transaction")
		}

		var n int
		if err := e.db.queryRowScan(context.Background(), []any{&n},
			`SELECT COUNT(*) FROM meta WHERE key='boom'`); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Fatalf("a panicking transaction committed its work (%d row(s)) — it must roll back", n)
		}
	})
}

// Reclamation is a multi-statement sequence pinned to one connection, so the per-statement rule has
// to hold there too: a cancelled Compact must stop starting SQL rather than run the vacuum and a
// second checkpoint for a caller who has gone (codex, round-6).
func TestCancelledReclaimStopsStartingStatements(t *testing.T) {
	eachLocalStore(t, func(t *testing.T, dsn string) {
		ctx := context.Background()
		e, err := Open(ctx, Options{DB: dsn, DisableBackground: true})
		if err != nil {
			t.Fatal(err)
		}
		defer e.Close()
		mustQueue(t, e, "q", QueueConfig{LockDurationMs: 60_000, MaxDeliveryCount: 10})

		cctx, cancel := context.WithCancel(context.Background())
		// Cancel with the sequence ALREADY RUNNING — after the first checkpoint. Cancelling sooner
		// proves nothing: withConn's pre-start check would refuse the whole callback and the guards
		// under test would never be reached.
		afterFirstCheckpoint = func() { cancel() }
		defer func() { afterFirstCheckpoint = nil }()

		if err := e.reclaimPages(cctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("a reclaim cancelled mid-sequence must stop starting statements, not run the\n"+
				"vacuum and a second checkpoint anyway; got err=%v", err)
		}
		afterFirstCheckpoint = nil

		// Nothing was interrupted, so nothing leaked: the store still works.
		if _, err := e.SendOne(context.Background(), "q", OutMessage{Body: []byte("after")}); err != nil {
			t.Fatalf("the store is unusable after a cancelled reclaim: %v", err)
		}
	})
}

// The last two windows where work could still BEGIN for a caller who had already gone. Both are
// after withConn's checks have passed, so both need a look of their own (codex, round-6).
func TestNothingBeginsInTheLastWindowBeforeAStatement(t *testing.T) {
	eachLocalStore(t, func(t *testing.T, dsn string) {
		ctx := context.Background()
		e, err := Open(ctx, Options{DB: dsn, DisableBackground: true})
		if err != nil {
			t.Fatal(err)
		}
		defer e.Close()
		mustQueue(t, e, "q", QueueConfig{LockDurationMs: 60_000, MaxDeliveryCount: 10})

		// 1. While buildArgs runs. It reads the clock, so it can be preempted — and a renewer that
		//    has been told to stop must not go on to extend the lease it was about to renew.
		t.Run("while the arguments are being built", func(t *testing.T) {
			cctx, cancel := context.WithCancel(context.Background())
			_, err := e.db.execFresh(cctx, `INSERT INTO meta(key,value) VALUES (?,?)`, func() []any {
				cancel() // the caller gives up exactly here
				return []any{"fresh", "1"}
			})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("a statement whose caller gave up while its arguments were being built must\n"+
					"not run; got err=%v", err)
			}
			var n int
			if err := e.db.queryRowScan(context.Background(), []any{&n},
				`SELECT COUNT(*) FROM meta WHERE key='fresh'`); err != nil {
				t.Fatal(err)
			}
			if n != 0 {
				t.Fatal("it ran anyway")
			}
		})

		// 2. Between BeginTx and the callback. fn is PUBLIC code — an Engine.Tx callback — and once
		//    it starts it holds the single writer until it returns.
		t.Run("between BeginTx and the callback", func(t *testing.T) {
			cctx, cancel := context.WithCancel(context.Background())
			afterBeginTx = func() { cancel() }
			defer func() { afterBeginTx = nil }()

			ran := false
			err := e.inTx(cctx, func(ec context.Context, tx *txn) error {
				ran = true
				return nil
			})
			afterBeginTx = nil
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("inTx must surface the cancellation, got %v", err)
			}
			if ran {
				t.Fatal("the callback ran for a caller who had already given up — and it holds the\n" +
					"single writer for as long as it wants to")
			}
		})

		if _, err := e.SendOne(context.Background(), "q", OutMessage{Body: []byte("after")}); err != nil {
			t.Fatalf("the store is unusable afterwards: %v", err)
		}
	})
}

// Concurrency invariants — the properties that make a queue a queue, asserted under real parallelism.
//
// Everything else in this suite tests what the engine SAYS when you ask it something. These tests
// watch what it DOES while many consumers hammer it at once, and they are here because that is
// precisely where this codebase has no eyes: every P1 found in review rounds 4-6 lived in behaviour
// that was observable but unobserved.
//
// The invariants below are the ones whose violation is unrecoverable for a user — a message handled
// twice at the same time, a message handled after it was completed, a message that simply vanishes.
// They are checked by an oracle that watches the deliveries, not by asking the engine to grade
// itself, because an engine with a bug will happily report that everything is fine.

// exclusivity is the oracle. It records who holds what, and screams the moment two consumers hold
// the same message at once — which is the failure a lock-based queue exists to prevent.
//
// It models the lease, because exclusivity IS the lease: a consumer owns a message only until
// locked_until, and a redelivery after that instant is not a violation, it is the documented
// at-least-once contract doing its job. The first version of this oracle ignored expiry and duly
// "found" dozens of violations that were the reaper working correctly — a reminder that an oracle
// which does not model the spec just measures its own misunderstanding.
//
// Holders are keyed by LOCK TOKEN, not by consumer: after a redelivery the same consumer may hold
// the same seq under a new token, and the old holder's late settle must not be mistaken for the new
// one's.
type holder struct {
	consumer    int
	token       string
	lockedUntil int64 // epoch ms; past this, the holder owns nothing
}

type exclusivity struct {
	mu         sync.Mutex
	held       map[int64]holder
	done       map[int64]bool // completed: a delivery after this is a settled message coming back
	deliveries map[int64]int

	violations []string
}

func newExclusivity() *exclusivity {
	return &exclusivity{held: map[int64]holder{}, done: map[int64]bool{}, deliveries: map[int64]int{}}
}

func (x *exclusivity) fail(format string, a ...any) {
	x.violations = append(x.violations, fmt.Sprintf(format, a...))
}

// claim records a delivery, the instant a consumer receives a message.
func (x *exclusivity) claim(seq int64, consumer int, token string, lockedUntil, now int64) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.deliveries[seq]++
	if prev, busy := x.held[seq]; busy && prev.lockedUntil > now {
		x.fail("seq %d was delivered to consumer %d while consumer %d STILL HELD A LIVE LOCK on it "+
			"(%dms of lease left) — two consumers are processing the same message at the same time",
			seq, consumer, prev.consumer, prev.lockedUntil-now)
		return
	}
	if x.done[seq] {
		x.fail("seq %d was delivered to consumer %d AFTER it had been completed "+
			"— a settled message came back", seq, consumer)
		return
	}
	x.held[seq] = holder{consumer: consumer, token: token, lockedUntil: lockedUntil}
}

// renewed extends the lease the oracle believes in, so a renewing holder does not look expired.
func (x *exclusivity) renewed(seq int64, token string, lockedUntil int64) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if h, ok := x.held[seq]; ok && h.token == token {
		h.lockedUntil = lockedUntil
		x.held[seq] = h
	}
}

// settled records the outcome of a settle by the holder of `token`. It is a no-op when the message
// has already moved on to a newer token — that holder lost its lease and owns nothing.
func (x *exclusivity) settled(seq int64, token string, completed bool) {
	x.mu.Lock()
	defer x.mu.Unlock()
	h, ok := x.held[seq]
	if !ok || h.token != token {
		return // stale holder; the engine will have told it ErrLockLost
	}
	delete(x.held, seq)
	if completed {
		x.done[seq] = true
	}
}

func (x *exclusivity) completedCount() int {
	x.mu.Lock()
	defer x.mu.Unlock()
	return len(x.done)
}

func (x *exclusivity) report(t *testing.T) {
	t.Helper()
	x.mu.Lock()
	defer x.mu.Unlock()
	if len(x.violations) == 0 {
		return
	}
	for i, v := range x.violations {
		if i == 8 {
			t.Errorf("... and %d more", len(x.violations)-8)
			break
		}
		t.Errorf("INVARIANT VIOLATED: %s", v)
	}
	t.FailNow()
}

// TestConcurrentConsumersNeverShareAMessage runs many consumers against one queue, in parallel, for
// real, and asserts the three invariants a user cannot work around:
//
//  1. EXCLUSIVE DELIVERY — while a consumer holds a lock, nobody else may be handed that message.
//  2. NO ZOMBIE DELIVERY — a completed message is never delivered again.
//  3. NO LOSS — every message sent is eventually completed. Not "most": every one.
//
// The consumers do not behave: they abandon, defer, renew, let locks expire on purpose, and settle
// with the wrong token. Locks are short and the reaper is running, so expiry races the handler —
// which is the exact window where a real system double-delivers.
func TestConcurrentConsumersNeverShareAMessage(t *testing.T) {
	eachLocalStore(t, func(t *testing.T, dsn string) {
		const (
			messages      = 400
			consumers     = 8
			drainConsumer = -1
		)
		ctx := context.Background()
		e, err := Open(ctx, Options{DB: dsn})
		if err != nil {
			t.Fatal(err)
		}
		defer e.Close()

		// A legacy short lease with the reaper live: expiry genuinely races the handler. New
		// configurations reject this value, but existing databases retain it across upgrades,
		// so their exclusivity guarantee remains part of the compatibility surface.
		mustQueue(t, e, "q", QueueConfig{LockDurationMs: 1_000, MaxDeliveryCount: 100})
		if _, err := e.db.exec(ctx, `UPDATE queues SET lock_duration_ms=300 WHERE name='q'`); err != nil {
			t.Fatal(err)
		}

		for i := 0; i < messages; i++ {
			if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte(fmt.Sprintf("m%d", i))}); err != nil {
				t.Fatal(err)
			}
		}

		x := newExclusivity()
		var completed int64
		deadline := time.Now().Add(20 * time.Second)

		var wg sync.WaitGroup
		for c := 0; c < consumers; c++ {
			wg.Add(1)
			go func(consumer int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(int64(consumer) + 1))
				for atomic.LoadInt64(&completed) < messages && time.Now().Before(deadline) {
					msgs, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 1 + rng.Intn(4), WaitMs: 50})
					if err != nil {
						t.Errorf("consumer %d: receive: %v", consumer, err)
						return
					}
					for _, m := range msgs {
						x.claim(m.SeqNumber, consumer, m.LockToken, m.LockedUntilMs, time.Now().UnixMilli())
					}
					for _, m := range msgs {
						switch rng.Intn(10) {
						case 0, 1:
							// Give it back. Somebody else must be able to take it — but only after
							// we have let go.
							x.settled(m.SeqNumber, m.LockToken, false)
							if err := e.Abandon(ctx, "q", m.SeqNumber, m.LockToken, 0); err != nil &&
								!errors.Is(err, ErrLockLost) {
								t.Errorf("consumer %d: abandon: %v", consumer, err)
							}
						case 2:
							// OVERRUN the lease and then try to settle. Expiry itself fences the
							// holder, even if the reaper has not reclaimed the row yet.
							time.Sleep(time.Duration(400+rng.Intn(200)) * time.Millisecond)
							err := e.Complete(ctx, "q", m.SeqNumber, m.LockToken)
							x.settled(m.SeqNumber, m.LockToken, false)
							if !errors.Is(err, ErrLockLost) {
								t.Errorf("consumer %d: late complete: %v, want ErrLockLost", consumer, err)
								return
							}
						case 3:
							// Renew, then complete: the lease must hold across the renewal.
							// The request's start gives a conservative lease bound. Adding the
							// duration after a slow response could invent lease time never granted.
							started := time.Now().UnixMilli()
							if err := e.Renew(ctx, "q", m.SeqNumber, m.LockToken); err == nil {
								x.renewed(m.SeqNumber, m.LockToken, started+300)
							} else if !errors.Is(err, ErrLockLost) {
								t.Errorf("consumer %d: renew: %v", consumer, err)
							}
							fallthrough
						default:
							err := e.Complete(ctx, "q", m.SeqNumber, m.LockToken)
							x.settled(m.SeqNumber, m.LockToken, err == nil)
							if err == nil {
								atomic.AddInt64(&completed, 1)
							} else if !errors.Is(err, ErrLockLost) {
								t.Errorf("consumer %d: complete: %v", consumer, err)
							}
						}
					}
				}
			}(c)
		}
		wg.Wait()

		x.report(t) // invariants 1 and 2
		if x.completedCount() != messages {
			// The randomized phase may stop with a valid short lease still in flight.
			// Give the reaper and one well-behaved consumer a bounded convergence window
			// before deciding that a message is lost or permanently stuck.
			drainDeadline := time.Now().Add(5 * time.Second)
			for x.completedCount() < messages && time.Now().Before(drainDeadline) {
				msgs, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 1, WaitMs: 50})
				if err != nil {
					t.Fatalf("drain receive: %v", err)
				}
				if len(msgs) == 0 {
					continue
				}
				m := msgs[0]
				x.claim(m.SeqNumber, drainConsumer, m.LockToken, m.LockedUntilMs, time.Now().UnixMilli())
				err = e.Complete(ctx, "q", m.SeqNumber, m.LockToken)
				x.settled(m.SeqNumber, m.LockToken, err == nil)
				if err == nil {
					atomic.AddInt64(&completed, 1)
				} else if !errors.Is(err, ErrLockLost) {
					t.Fatalf("drain complete: %v", err)
				}
			}
			x.report(t) // the convergence consumer obeys the same exclusivity oracle
		}

		// Invariant 3: nothing was lost. Every message sent came out the other side.
		if got := x.completedCount(); got != messages {
			var stuck []int64
			for seq := int64(1); seq <= messages; seq++ {
				if !x.done[seq] {
					stuck = append(stuck, seq)
					if len(stuck) == 10 {
						break
					}
				}
			}
			m, _ := e.Stats(context.Background(), "q")
			t.Fatalf("DRAIN TIMEOUT: %d of %d messages were completed after the convergence window.\n"+
				"  still in the queue: active=%d locked=%d dead=%d deferred=%d\n"+
				"  not completed (first few): %v\n"+
				"  every sent message must remain recoverable and converge to completion.",
				got, messages, m.Active, m.Locked, m.DeadLettered, m.Deferred, stuck)
		}
	})
}

// TestConcurrentSettlementPicksExactlyOneWinner is the exclusivity invariant reduced to its sharpest
// form: two consumers, one message, a lock that has just expired. Both of them try to settle it.
//
// Exactly the new, still-live holder may win. The fake clock is held still while both calls race:
// if both leases expired, refusing both would be correct and would say nothing about exclusivity.
//
// It runs many rounds because this is a race: a single round proves nothing.
func TestConcurrentSettlementPicksExactlyOneWinner(t *testing.T) {
	eachLocalStore(t, func(t *testing.T, dsn string) {
		ctx := context.Background()
		var now atomic.Int64
		now.Store(1_700_000_000_000)
		e, err := Open(ctx, Options{DB: dsn, Now: now.Load, DisableBackground: true})
		if err != nil {
			t.Fatal(err)
		}
		defer e.Close()
		mustQueue(t, e, "q", QueueConfig{LockDurationMs: 1_000, MaxDeliveryCount: 100})

		const rounds = 60
		for r := 0; r < rounds; r++ {
			if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte("x")}); err != nil {
				t.Fatal(err)
			}
			first, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 1})
			if err != nil || len(first) != 1 {
				t.Fatalf("round %d: first receive: %v (%d)", r, err, len(first))
			}
			a := first[0]

			// Let the lease lapse and hand the message to a second consumer. Now TWO tokens exist
			// for one message: A's, which is stale, and B's, which is live.
			now.Store(a.LockedUntilMs)
			e.RunMaintenanceOnce(ctx)
			second, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 1})
			if err != nil || len(second) != 1 {
				t.Fatalf("round %d: the reaper did not release the expired lock: %v (%d)", r, err, len(second))
			}
			b := second[0]
			if b.SeqNumber != a.SeqNumber {
				t.Fatalf("round %d: expected the same message back, got %d and %d", r, a.SeqNumber, b.SeqNumber)
			}
			if a.LockToken == b.LockToken {
				t.Fatalf("round %d: the redelivery reused the lock token — the fence is not a fence", r)
			}

			// Both consumers finish at the same instant and both try to acknowledge.
			var wins int64
			var staleWon int64
			var wg sync.WaitGroup
			for _, m := range []*Message{a, b} {
				wg.Add(1)
				go func(m *Message, stale bool) {
					defer wg.Done()
					err := e.Complete(ctx, "q", m.SeqNumber, m.LockToken)
					switch {
					case err == nil:
						atomic.AddInt64(&wins, 1)
						if stale {
							atomic.AddInt64(&staleWon, 1)
						}
					case errors.Is(err, ErrLockLost): // the honest answer for the stale holder
					default:
						t.Errorf("round %d: unexpected error: %v", r, err)
					}
				}(m, m.LockToken == a.LockToken)
			}
			wg.Wait()

			if wins != 1 {
				t.Fatalf("round %d: %d of 2 racing consumers were told their Complete succeeded.\n"+
					"  exactly one may win. Telling both they succeeded means the message was\n"+
					"  processed twice and both acknowledgements were accepted.", r, wins)
			}
			// And "exactly one" is not enough on its own: the winner must be the RIGHT one. A queue
			// that lets the STALE holder settle has no fence at all — it merely has a race, and the
			// live holder (which is the one actually processing the message) is told it lost a lock
			// it still held. Counting winners alone cannot see this; asking WHO won can.
			if staleWon != 0 {
				t.Fatalf("round %d: the settle was accepted from the holder of the EXPIRED lock.\n"+
					"  the message had already been reassigned, and its new owner — the one actually\n"+
					"  processing it — was told ErrLockLost. lock_token is not fencing anything.", r)
			}
			// And it really is gone.
			left, err := e.Peek(ctx, "q", PeekOptions{Max: 10})
			if err != nil {
				t.Fatal(err)
			}
			if len(left) != 0 {
				t.Fatalf("round %d: the message survived a successful Complete (%d left)", r, len(left))
			}
		}
	})
}
