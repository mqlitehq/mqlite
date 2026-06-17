package engine

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A dedup conflict inside a SendBatch must skip only the offending message, not
// roll back the whole batch (§11 / Bug-4).
func TestSendBatchDedupConflictSkipsOne(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{DedupWindowMs: 60_000})

	seqs, err := e.SendBatch(ctx, "q", []OutMessage{
		{MessageID: "A", Body: []byte("one")},
		{MessageID: "A", Body: []byte("TWO-different-body")}, // conflict: same id, diff body
		{MessageID: "B", Body: []byte("three")},
	})
	if err != nil {
		t.Fatalf("SendBatch must not fail the whole batch on one conflict: %v", err)
	}
	if len(seqs) != 3 || seqs[0] == 0 || seqs[2] == 0 {
		t.Fatalf("good messages must commit, got seqs=%v", seqs)
	}
	if seqs[1] != 0 {
		t.Fatalf("conflicting message must be skipped (seq 0), got %d", seqs[1])
	}
	mm, _ := e.GetQueueMetrics(ctx, "q")
	if mm.Active != 2 {
		t.Fatalf("want 2 enqueued (A,B), got active=%d", mm.Active)
	}

	// single Send still surfaces the conflict as an error.
	if _, err := e.Send(ctx, "q", OutMessage{MessageID: "A", Body: []byte("yet-another")}); !errors.Is(err, ErrDedupConflict) {
		t.Fatalf("single Send conflict must return ErrDedupConflict, got %v", err)
	}
}

// Idempotent receive: retrying Receive with the same attempt id replays the same
// batch (same lock token, no delivery_count increment) — a dropped Receive
// response must not burn a delivery or hand out a different message (§11 cx-port).
func TestIdempotentReceive(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{})
	e.Send(ctx, "q", OutMessage{Body: []byte("first")})
	e.Send(ctx, "q", OutMessage{Body: []byte("second")})

	first, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 1, AttemptID: "A1"})
	if err != nil || len(first) != 1 {
		t.Fatalf("first receive: n=%d err=%v", len(first), err)
	}
	m1 := first[0]
	if m1.DeliveryCount != 1 {
		t.Fatalf("delivery_count want 1, got %d", m1.DeliveryCount)
	}

	// retry with the SAME attempt id -> replay, identical token, dc unchanged.
	replay, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 1, AttemptID: "A1"})
	if err != nil || len(replay) != 1 {
		t.Fatalf("replay receive: n=%d err=%v", len(replay), err)
	}
	if replay[0].SeqNumber != m1.SeqNumber || replay[0].LockToken != m1.LockToken {
		t.Fatalf("replay must return same msg+token: got seq=%d tok=%s want seq=%d tok=%s",
			replay[0].SeqNumber, replay[0].LockToken, m1.SeqNumber, m1.LockToken)
	}
	if replay[0].DeliveryCount != 1 {
		t.Fatalf("replay must NOT burn delivery_count, got %d", replay[0].DeliveryCount)
	}

	// a different attempt id claims the next message (not a replay).
	next, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 1, AttemptID: "A2"})
	if err != nil || len(next) != 1 || next[0].SeqNumber == m1.SeqNumber {
		t.Fatalf("different attempt id must claim a new message, got %+v err=%v", next, err)
	}

	// the replayed token still settles the message exactly once.
	if err := e.Complete(ctx, "q", replay[0].SeqNumber, replay[0].LockToken); err != nil {
		t.Fatalf("complete via replayed token: %v", err)
	}
}

// deadLetterN sends n messages to q and dead-letters all of them.
func deadLetterN(t *testing.T, e *Engine, q string, n int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		e.Send(ctx, q, OutMessage{Body: []byte("x")})
	}
	for i := 0; i < n; i++ {
		m := recvOne(t, e, q)
		if m == nil {
			t.Fatalf("expected a message to dead-letter")
		}
		if err := e.DeadLetter(ctx, q, m.SeqNumber, m.LockToken, "test", ""); err != nil {
			t.Fatalf("deadletter: %v", err)
		}
	}
}

// Cross-queue redrive moves the whole set atomically into the target, and the
// source DLQ ends empty. (§11 Bug-3: was per-row tx.)
func TestCrossQueueRedrive(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "src", QueueConfig{})
	mustQueue(t, e, "dst", QueueConfig{})
	deadLetterN(t, e, "src", 5)

	moved, err := e.Redrive(ctx, "src", RedriveOptions{Target: "dst"})
	if err != nil || moved != 5 {
		t.Fatalf("cross-queue redrive moved=%d err=%v (want 5, nil)", moved, err)
	}
	src, _ := e.GetQueueMetrics(ctx, "src")
	dst, _ := e.GetQueueMetrics(ctx, "dst")
	if src.DeadLettered != 0 || dst.Active != 5 {
		t.Fatalf("after move: src.dlq=%d dst.active=%d (want 0, 5)", src.DeadLettered, dst.Active)
	}
}

// RatePerSec chunks the move but still relocates everything.
func TestRedriveRatePerSec(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "src", QueueConfig{})
	mustQueue(t, e, "dst", QueueConfig{})
	deadLetterN(t, e, "src", 3)

	moved, err := e.Redrive(ctx, "src", RedriveOptions{Target: "dst", RatePerSec: 2}) // 2 chunks
	if err != nil || moved != 3 {
		t.Fatalf("rate-limited redrive moved=%d err=%v (want 3, nil)", moved, err)
	}
	dst, _ := e.GetQueueMetrics(ctx, "dst")
	if dst.Active != 3 {
		t.Fatalf("dst.active=%d (want 3)", dst.Active)
	}
}

// PurgeDeadLetter permanently deletes DLQ messages.
func TestPurgeDeadLetter(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{})
	deadLetterN(t, e, "q", 4)

	purged, err := e.PurgeDeadLetter(ctx, "q", RedriveOptions{Max: 3})
	if err != nil || purged != 3 {
		t.Fatalf("purge max=3 got=%d err=%v", purged, err)
	}
	purged, err = e.PurgeDeadLetter(ctx, "q", RedriveOptions{}) // purge the rest
	if err != nil || purged != 1 {
		t.Fatalf("purge rest got=%d err=%v", purged, err)
	}
	mm, _ := e.GetQueueMetrics(ctx, "q")
	if mm.DeadLettered != 0 || mm.Total != 0 {
		t.Fatalf("after purge dlq=%d total=%d (want 0,0)", mm.DeadLettered, mm.Total)
	}
}

// Idempotent settle: a client retrying a Complete whose ack was lost must get
// success (receipt), but a genuinely lost lock (message reclaimed) must still
// fail with ErrLockLost. This is the difference §11 calls out.
func TestSettleIdempotentVsLockLost(t *testing.T) {
	ctx := context.Background()
	e, ms := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{LockDurationMs: 1000, MaxDeliveryCount: 50})

	// --- idempotent replay: double Complete with the SAME token succeeds twice.
	e.Send(ctx, "q", OutMessage{Body: []byte("a")})
	m := recvOne(t, e, "q")
	if err := e.Complete(ctx, "q", m.SeqNumber, m.LockToken); err != nil {
		t.Fatalf("first complete: %v", err)
	}
	if err := e.Complete(ctx, "q", m.SeqNumber, m.LockToken); err != nil {
		t.Fatalf("retry of already-completed token must be idempotent success, got %v", err)
	}

	// --- genuine lock-loss: lock expires, message reclaimed (new token), the
	// stale token's Complete must be ErrLockLost (no receipt for it).
	e.Send(ctx, "q", OutMessage{Body: []byte("b")})
	m1 := recvOne(t, e, "q")
	advance(ms, 2*time.Second) // past the 1s lock
	e.RunMaintenanceOnce(ctx)  // reaper returns it to active
	m2 := recvOne(t, e, "q")
	if m2 == nil || m2.LockToken == m1.LockToken {
		t.Fatalf("expected redelivery with a fresh token; m2=%v", m2)
	}
	if err := e.Complete(ctx, "q", m1.SeqNumber, m1.LockToken); !errors.Is(err, ErrLockLost) {
		t.Fatalf("stale token after reclaim must be ErrLockLost, got %v", err)
	}
	// the legitimate holder still completes fine.
	if err := e.Complete(ctx, "q", m2.SeqNumber, m2.LockToken); err != nil {
		t.Fatalf("legit complete: %v", err)
	}
}
