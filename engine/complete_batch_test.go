package engine

import (
	"context"
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
}
