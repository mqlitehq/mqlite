package engine

import (
	"context"
	"errors"
	"testing"
)

// CompleteBatch settles a received batch in one transaction: all valid items
// succeed, a stale token comes back Ok=false (not fatal), and a replay of
// already-settled tokens is idempotently Ok=true.
func TestCompleteBatch(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{LockDurationMs: 600_000, MaxDeliveryCount: 10})

	for i := 0; i < 5; i++ {
		if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte{byte(i)}}); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	msgs, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 5})
	if err != nil || len(msgs) != 5 {
		t.Fatalf("receive: got %d (err %v)", len(msgs), err)
	}

	items := make([]SettleItem, 0, 6)
	for _, m := range msgs {
		items = append(items, SettleItem{SeqNumber: m.SeqNumber, LockToken: m.LockToken})
	}
	items = append(items, SettleItem{SeqNumber: msgs[0].SeqNumber, LockToken: "stale"}) // bogus

	res, err := e.CompleteBatch(ctx, "q", items)
	if err != nil {
		t.Fatalf("CompleteBatch: %v", err)
	}
	ok := 0
	for _, r := range res {
		if r.Ok {
			ok++
		}
	}
	if ok != 5 {
		t.Fatalf("want 5 settled, got %d (%+v)", ok, res)
	}
	if res[5].Ok {
		t.Fatal("stale-token item must be Ok=false, not fatal to the batch")
	}
	if mt, _ := e.Stats(ctx, "q"); mt.Total != 0 {
		t.Fatalf("queue should be empty after batch complete, total=%d", mt.Total)
	}
	// The lifetime completed counter sees the 5 real deletes — the stale-token item
	// removed nothing, so it must not count. (MQLITE-54)
	if c := e.CompletedCounts()["q"]; c != 5 {
		t.Fatalf("completed counter = %d, want 5 (stale token must not count)", c)
	}

	// idempotent replay: the same tokens still have live receipts → Ok=true
	res2, err := e.CompleteBatch(ctx, "q", items[:5])
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	for _, r := range res2 {
		if !r.Ok {
			t.Fatalf("idempotent replay should be Ok=true (%+v)", res2)
		}
	}
	// Replay deletes nothing, so the counter must stay at 5, not double to 10.
	if c := e.CompletedCounts()["q"]; c != 5 {
		t.Fatalf("idempotent replay double-counted: completed = %d, want 5", c)
	}
}

// TestCompletedCounter pins the single-Complete path of the lifetime completed
// counter: it counts real completions, ignores idempotent replays and stale
// tokens (which delete nothing), and is isolated per queue. (MQLITE-54)
func TestCompletedCounter(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{LockDurationMs: 600_000, MaxDeliveryCount: 10})
	mustQueue(t, e, "other", QueueConfig{LockDurationMs: 600_000, MaxDeliveryCount: 10})

	for i := 0; i < 3; i++ {
		if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte{byte(i)}}); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	msgs, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 3})
	if err != nil || len(msgs) != 3 {
		t.Fatalf("receive: got %d (err %v)", len(msgs), err)
	}
	for _, m := range msgs[:2] { // complete two of three
		if err := e.Complete(ctx, "q", m.SeqNumber, m.LockToken); err != nil {
			t.Fatalf("complete: %v", err)
		}
	}
	if c := e.CompletedCounts()["q"]; c != 2 {
		t.Fatalf("completed = %d, want 2", c)
	}
	// Replaying an already-completed message (lost-response) must not double-count.
	if err := e.Complete(ctx, "q", msgs[0].SeqNumber, msgs[0].LockToken); err != nil {
		t.Fatalf("replay complete: %v", err)
	}
	if c := e.CompletedCounts()["q"]; c != 2 {
		t.Fatalf("replay double-counted: completed = %d, want 2", c)
	}
	// A stale token removes nothing → ErrLockLost, and must not bump the counter.
	if err := e.Complete(ctx, "q", 999, "nope"); !errors.Is(err, ErrLockLost) {
		t.Fatalf("stale complete: err = %v, want ErrLockLost", err)
	}
	if c := e.CompletedCounts()["q"]; c != 2 {
		t.Fatalf("stale token bumped the counter: completed = %d, want 2", c)
	}
	// Per-queue isolation: "other" saw no completions.
	if c := e.CompletedCounts()["other"]; c != 0 {
		t.Fatalf("cross-queue leak: other = %d, want 0", c)
	}
}
