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

// Expiration is enforced without waiting for maintenance, across every public
// data path. Existing receipts and an artificially long message lease cannot
// authorize an effect after the subscription itself has expired.
func TestSubscriptionExpiryFencesDataPlane(t *testing.T) {
	e, clock := testEngine(t)
	ctx := context.Background()
	sub, err := e.SubscribeWithOptions(ctx, "events", "temporary", nil, SubscribeOptions{
		TTLMs: 1000, RequestID: "expiry-data-plane", RequestExpiresAtMs: e.now() + 5000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Send(ctx, "events", OutMessage{Body: []byte("completed")}, OutMessage{Body: []byte("deferred")}, OutMessage{Body: []byte("locked")}); err != nil {
		t.Fatal(err)
	}
	msgs, err := e.Receive(ctx, sub.Name, ReceiveOptions{MaxMessages: 3, Generation: sub.Generation})
	if err != nil || len(msgs) != 3 {
		t.Fatalf("receive: %v %v", msgs, err)
	}
	for _, m := range msgs {
		if m.LockedUntilMs != sub.ExpiresAtMs {
			t.Fatalf("lease %d outlives subscription %d", m.LockedUntilMs, sub.ExpiresAtMs)
		}
	}
	if err := e.Complete(ctx, sub.Name, msgs[0].SeqNumber, msgs[0].LockToken); err != nil {
		t.Fatal(err)
	}
	if err := e.Defer(ctx, sub.Name, msgs[1].SeqNumber, msgs[1].LockToken); err != nil {
		t.Fatal(err)
	}
	if _, err := e.db.exec(ctx, `UPDATE messages SET locked_until=? WHERE id=?`, e.now()+60000, msgs[2].SeqNumber); err != nil {
		t.Fatal(err)
	}
	advance(clock, time.Second)
	item := SettleItem{SeqNumber: msgs[2].SeqNumber, LockToken: msgs[2].LockToken}
	for _, op := range deadlineOperations {
		t.Run(op, func(t *testing.T) {
			err := callDeadlineOperation(ctx, e, op, sub.Name, item)
			if !errors.Is(err, ErrLockLost) && !errors.Is(err, ErrQueueNotFound) {
				t.Fatalf("expired subscription allowed %s: %v", op, err)
			}
		})
	}
	if err := e.Complete(ctx, sub.Name, msgs[0].SeqNumber, msgs[0].LockToken); !errors.Is(err, ErrLockLost) {
		t.Fatalf("expired subscription replayed receipt: %v", err)
	}
	for name, call := range map[string]func() error{
		"publish": func() error { _, err := e.SendOne(ctx, "events", OutMessage{}); return err },
		"direct": func() error {
			_, err := e.SendOne(ctx, sub.Name, OutMessage{TargetGeneration: sub.Generation})
			return err
		},
		"schedule": func() error { _, err := e.Schedule(ctx, "events", OutMessage{}, e.now()+1000); return err },
		"transaction": func() error {
			return e.Tx(ctx, func(tx *EngineTx) error { _, err := tx.SendOne("events", OutMessage{}); return err })
		},
		"receive": func() error {
			_, err := e.Receive(ctx, sub.Name, ReceiveOptions{Generation: sub.Generation})
			return err
		},
		"deferred": func() error {
			_, err := e.ReceiveDeferredWithGeneration(ctx, sub.Name, sub.Generation, msgs[1].SeqNumber)
			return err
		},
		"purge":   func() error { _, err := e.Purge(ctx, sub.Name, RedriveOptions{}); return err },
		"redrive": func() error { _, err := e.Redrive(ctx, sub.Name, RedriveOptions{}); return err },
	} {
		t.Run(name, func(t *testing.T) {
			if err := call(); !errors.Is(err, ErrQueueNotFound) {
				t.Fatalf("expired operation: %v", err)
			}
		})
	}
	if got, err := e.Peek(ctx, sub.Name, PeekOptions{}); err != nil || len(got) != 0 {
		t.Fatalf("expired subscription remained browsable: %v %v", got, err)
	}
	var retained int
	if err := e.db.queryRowScan(ctx, []any{&retained}, `SELECT COUNT(*) FROM messages WHERE queue=?`, sub.Name); err != nil || retained != 2 {
		t.Fatalf("expired operations modified retained data: %d %v", retained, err)
	}
}

func TestTemporarySubscriptionRequiresIncarnation(t *testing.T) {
	e, _ := testEngine(t)
	ctx := context.Background()
	sub, err := e.SubscribeWithOptions(ctx, "events", "temporary", nil, SubscribeOptions{
		TTLMs: 1000, RequestID: "incarnation", RequestExpiresAtMs: e.now() + 5000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.SendOne(ctx, sub.Name, OutMessage{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("unfenced send: %v", err)
	}
	if err := e.Tx(ctx, func(tx *EngineTx) error { _, err := tx.SendOne(sub.Name, OutMessage{}); return err }); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("unfenced transactional send: %v", err)
	}
	if _, err := e.Receive(ctx, sub.Name, ReceiveOptions{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("unfenced receive: %v", err)
	}
	if _, err := e.ReceiveDeferred(ctx, sub.Name, 1); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("unfenced deferred receive: %v", err)
	}
	if _, err := e.SendOne(ctx, sub.Name, OutMessage{TargetGeneration: sub.Generation}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.SendOne(ctx, "events", OutMessage{TargetGeneration: sub.Generation}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("topic accepted direct-target generation: %v", err)
	}
}

func TestSubscriptionLongPollExpiryWithoutMaintenance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	e, err := Open(ctx, Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	sub, err := e.SubscribeWithOptions(ctx, "events", "temporary", nil, SubscribeOptions{
		TTLMs: 50, RequestID: "long-poll", RequestExpiresAtMs: e.now() + 5000,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.Receive(ctx, sub.Name, ReceiveOptions{Generation: sub.Generation, WaitMs: 20000})
	if !errors.Is(err, ErrQueueNotFound) {
		t.Fatalf("TTL did not wake a long poll: %v", err)
	}
	e.note.mu.Lock()
	defer e.note.mu.Unlock()
	if len(e.note.chans) != 0 {
		t.Fatalf("expired receiver retained %d notifier names", len(e.note.chans))
	}
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

// A selected DLQ ID is not ownership: purge, another redrive and receive can
// change it before this redrive obtains the writer. Only current DLQ rows move.
func TestRedriveRechecksStaleCandidates(t *testing.T) {
	for _, cross := range []bool{false, true} {
		name := "same-queue"
		if cross {
			name = "cross-queue"
		}
		t.Run(name, func(t *testing.T) {
			e, _ := testEngine(t)
			ctx := context.Background()
			mustQueue(t, e, "src", QueueConfig{})
			mustQueue(t, e, "dst", QueueConfig{})
			ids, err := e.Send(ctx, "src", OutMessage{Body: []byte("purged")}, OutMessage{Body: []byte("claimed")}, OutMessage{Body: []byte("remaining"), CorrelationID: "correlation", ReplyTo: "replies", Subject: "subject", Properties: map[string]string{"key": "value"}})
			if err != nil {
				t.Fatal(err)
			}
			messages, err := e.Receive(ctx, "src", ReceiveOptions{MaxMessages: 3})
			if err != nil || len(messages) != 3 {
				t.Fatalf("setup receive=%v err=%v", messages, err)
			}
			for _, m := range messages {
				if err := e.Reject(ctx, "src", m.SeqNumber, m.LockToken, "test", ""); err != nil {
					t.Fatal(err)
				}
			}
			source, err := e.loadQueue(ctx, "src")
			if err != nil {
				t.Fatal(err)
			}
			target, err := e.loadQueue(ctx, "dst")
			if err != nil {
				t.Fatal(err)
			}
			if n, err := e.Purge(ctx, "src", RedriveOptions{Max: 1}); err != nil || n != 1 {
				t.Fatalf("purge=%d err=%v", n, err)
			}
			if n, err := e.Redrive(ctx, "src", RedriveOptions{Max: 1}); err != nil || n != 1 {
				t.Fatalf("competing redrive=%d err=%v", n, err)
			}
			locked := recvOne(t, e, "src")
			if locked == nil || locked.SeqNumber != ids[1] {
				t.Fatalf("competing claim=%+v", locked)
			}
			moved, err := e.moveBatch(ctx, "dst", "src", cross, ids, source.generation, target.generation)
			if err != nil || moved != 1 {
				t.Fatalf("stale redrive moved=%d err=%v", moved, err)
			}
			// Resetting or deleting the concurrently claimed row would lose this lease.
			if err := e.Complete(ctx, "src", locked.SeqNumber, locked.LockToken); err != nil {
				t.Fatalf("stale redrive changed live lease: %v", err)
			}
			destination := "src"
			if cross {
				destination = "dst"
			}
			got, err := e.Receive(ctx, destination, ReceiveOptions{MaxMessages: 10})
			if err != nil || len(got) != 1 {
				t.Fatalf("remaining receive=%v err=%v", got, err)
			}
			if string(got[0].Body) != "remaining" || got[0].CorrelationID != "correlation" || got[0].ReplyTo != "replies" || got[0].Subject != "subject" || got[0].Properties["key"] != "value" {
				t.Fatalf("wrong row/metadata revived: %+v", got[0])
			}
			if !cross && got[0].SeqNumber != ids[2] {
				t.Fatal("same-queue redrive changed sequence")
			}
			src, err := e.Stats(ctx, "src")
			if err != nil {
				t.Fatal(err)
			}
			if src.DeadLettered != 0 || (cross && src.Total != 0) {
				t.Fatalf("source leftovers=%+v", src)
			}
		})
	}
}

// Public throttled calls release the writer between chunks. Rows purged in that
// interval must not be resurrected, and the returned count must describe effects.
func TestRedriveConcurrentPurgeAndCancel(t *testing.T) {
	for _, cancelDuringThrottle := range []bool{false, true} {
		name := "purge"
		if cancelDuringThrottle {
			name = "cancel"
		}
		t.Run(name, func(t *testing.T) {
			e, _ := testEngine(t)
			base := context.Background()
			mustQueue(t, e, "src", QueueConfig{})
			mustQueue(t, e, "dst", QueueConfig{})
			deadLetterN(t, e, "src", 3)
			ctx, cancel := context.WithCancel(base)
			defer cancel()
			type result struct {
				moved int
				err   error
			}
			done := make(chan result, 1)
			go func() {
				n, err := e.Redrive(ctx, "src", RedriveOptions{Target: "dst", RatePerSec: 1})
				done <- result{n, err}
			}()
			deadline := time.Now().Add(5 * time.Second)
			for {
				stats, err := e.Stats(base, "dst")
				if err != nil {
					t.Fatal(err)
				}
				if stats.Active == 1 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("first redrive chunk did not commit")
				}
				time.Sleep(time.Millisecond)
			}
			if cancelDuringThrottle {
				cancel()
				select {
				case res := <-done:
					if res.moved != 1 || !errors.Is(res.err, context.Canceled) {
						t.Fatalf("cancel result=%+v", res)
					}
				case <-time.After(500 * time.Millisecond):
					t.Fatal("throttle ignored cancellation")
				}
				src, err := e.Stats(base, "src")
				if err != nil || src.DeadLettered != 2 {
					t.Fatalf("cancel source=%+v err=%v", src, err)
				}
			} else {
				if n, err := e.Purge(base, "src", RedriveOptions{}); err != nil || n != 2 {
					t.Fatalf("concurrent purge=%d err=%v", n, err)
				}
				select {
				case res := <-done:
					if res.moved != 1 || res.err != nil {
						t.Fatalf("purge result=%+v", res)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("redrive did not finish")
				}
				dst, err := e.Stats(base, "dst")
				if err != nil || dst.Active != 1 {
					t.Fatalf("purged rows resurrected: %+v err=%v", dst, err)
				}
			}
		})
	}
}

// Internal SQL chunks must not turn the unthrottled public operation into
// partial commits: a failure in a later chunk restores every original DLQ row.
func TestRedriveLargeMoveRollsBackTogether(t *testing.T) {
	e, _ := testEngine(t)
	ctx := context.Background()
	mustQueue(t, e, "src", QueueConfig{})
	mustQueue(t, e, "dst", QueueConfig{})
	if err := e.inTx(ctx, func(ctx context.Context, tx *txn) error {
		for i := 0; i < 1001; i++ {
			subject := "ok"
			if i == 1000 {
				subject = "fail"
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO messages(queue,state,enqueued_at,subject,body) VALUES ('src','dead_lettered',?,?,?)`, e.now(), subject, []byte("body")); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.db.exec(ctx, `CREATE TRIGGER fail_redrive BEFORE INSERT ON messages WHEN NEW.queue='dst' AND NEW.subject='fail' BEGIN SELECT RAISE(ABORT,'redrive test failure'); END`); err != nil {
		t.Fatal(err)
	}
	if moved, err := e.Redrive(ctx, "src", RedriveOptions{Target: "dst"}); err == nil || moved != 0 {
		t.Fatalf("failed move=%d err=%v", moved, err)
	}
	src, err := e.Stats(ctx, "src")
	if err != nil {
		t.Fatal(err)
	}
	dst, err := e.Stats(ctx, "dst")
	if err != nil {
		t.Fatal(err)
	}
	if src.DeadLettered != 1001 || dst.Total != 0 {
		t.Fatalf("partial commit: src=%+v dst=%+v", src, dst)
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
