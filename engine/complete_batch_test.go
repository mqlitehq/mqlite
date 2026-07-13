package engine

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"
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

// ─── RenewBatch (MQLITE-97) ────────────────────────────────────────────────────

// RenewBatch extends a whole batch's leases in one transaction, fencing each item on its own
// lock token: a valid item is renewed, a stale token is simply not (Ok=false, not an error),
// and an item whose lock was never held stays untouched.
func TestRenewBatch(t *testing.T) {
	ctx := context.Background()
	e, ms := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{LockDurationMs: 10_000, MaxDeliveryCount: 10})

	for i := 0; i < 5; i++ {
		if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte{byte(i)}}); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	msgs, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 5})
	if err != nil || len(msgs) != 5 {
		t.Fatalf("receive: got %d (err %v)", len(msgs), err)
	}
	before := msgs[0].LockedUntilMs

	advance(ms, 3*time.Second) // the lease is ticking down

	items := make([]SettleItem, 0, 7)
	for _, m := range msgs {
		items = append(items, SettleItem{SeqNumber: m.SeqNumber, LockToken: m.LockToken})
	}
	items = append(items, SettleItem{SeqNumber: msgs[0].SeqNumber, LockToken: "stale"}) // wrong token
	items = append(items, SettleItem{SeqNumber: msgs[1].SeqNumber, LockToken: ""})      // no token

	res, err := e.RenewBatch(ctx, "q", items)
	if err != nil {
		t.Fatalf("RenewBatch: %v", err)
	}
	if len(res) != len(items) {
		t.Fatalf("results = %d, want one per item (%d)", len(res), len(items))
	}
	for i, r := range res {
		wantOk := i < 5 // only the five real (seq, token) pairs
		if r.Ok != wantOk {
			t.Errorf("item %d (seq %d): Ok=%v, want %v", i, r.SeqNumber, r.Ok, wantOk)
		}
		if r.SeqNumber != items[i].SeqNumber {
			t.Errorf("item %d: result seq %d does not line up with the request (%d)", i, r.SeqNumber, items[i].SeqNumber)
		}
	}

	// Every lease really moved forward — a renewal that reports Ok but doesn't extend the lock
	// is worse than useless.
	peeked, err := e.Peek(ctx, "q", PeekOptions{State: StateLocked, Max: 10})
	if err != nil || len(peeked) != 5 {
		t.Fatalf("peek locked: got %d (err %v)", len(peeked), err)
	}
	for _, p := range peeked {
		if p.LockedUntilMs <= before {
			t.Errorf("seq %d: locked_until %d did not advance past %d", p.SeqNumber, p.LockedUntilMs, before)
		}
	}
}

// A wrong token must never renew somebody else's lock — the fencing that makes Peek-Lock safe.
// The set-based UPDATE matches (id, lock_token) as a pair, so a real seq with the wrong token
// matches nothing; a bug that matched on id alone would extend a lease the caller does not hold.
func TestRenewBatchFencesOnToken(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{LockDurationMs: 10_000, MaxDeliveryCount: 10})
	if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	msgs, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 1})
	if err != nil || len(msgs) != 1 {
		t.Fatalf("receive: %v n=%d", err, len(msgs))
	}
	held := msgs[0].LockedUntilMs

	res, err := e.RenewBatch(ctx, "q", []SettleItem{{SeqNumber: msgs[0].SeqNumber, LockToken: "not-my-token"}})
	if err != nil {
		t.Fatalf("RenewBatch: %v", err)
	}
	if res[0].Ok {
		t.Error("a wrong lock token must NOT renew the lease")
	}
	p, err := e.Peek(ctx, "q", PeekOptions{State: StateLocked, Max: 1})
	if err != nil || len(p) != 1 {
		t.Fatalf("peek: %v n=%d", err, len(p))
	}
	if p[0].LockedUntilMs != held {
		t.Errorf("the lease moved (%d -> %d) despite a wrong token — fencing is broken",
			held, p[0].LockedUntilMs)
	}
}

// The whole reason RenewBatch exists: it must cost a FIXED number of SQL statements, not one
// per message. Against a remote Turso store each statement is a Hrana round trip, so an
// O(N) implementation reintroduces at the DB layer exactly the latency we removed from the
// client — and a 256-message renewal could then outlast the very lease it is renewing.
func TestRenewBatchIsSetBased(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{LockDurationMs: 30_000, MaxDeliveryCount: 10})
	const n = 256 // the engine's maximum receive
	for i := 0; i < n; i++ {
		if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte("m")}); err != nil {
			t.Fatal(err)
		}
	}
	msgs, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: n})
	if err != nil || len(msgs) != n {
		t.Fatalf("receive: got %d (err %v)", len(msgs), err)
	}
	items := make([]SettleItem, n)
	for i, m := range msgs {
		items[i] = SettleItem{SeqNumber: m.SeqNumber, LockToken: m.LockToken}
	}
	res, err := e.RenewBatch(ctx, "q", items)
	if err != nil {
		t.Fatalf("RenewBatch(%d): %v", n, err)
	}
	for _, r := range res {
		if !r.Ok {
			t.Fatalf("seq %d was not renewed in a full-size batch", r.SeqNumber)
		}
	}
}

// CompleteBatch is set-based too (MQLITE-97): a full-size batch must settle in a fixed number of
// statements, and each item must still be fenced on its OWN token. The duplicate-seq case is the
// trap: a seq passed twice, once with the real token and once with a stale one, must report
// Ok only for the real pair — a result keyed by sequence number alone would let the stale item
// inherit the other's success and tell the caller a message was settled that never was.
func TestCompleteBatchSetBasedAndFenced(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{LockDurationMs: 600_000, MaxDeliveryCount: 10})
	const n = 256 // the engine's maximum receive
	for i := 0; i < n; i++ {
		if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte("m")}); err != nil {
			t.Fatal(err)
		}
	}
	msgs, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: n})
	if err != nil || len(msgs) != n {
		t.Fatalf("receive: got %d (err %v)", len(msgs), err)
	}

	items := make([]SettleItem, 0, n+2)
	for _, m := range msgs {
		items = append(items, SettleItem{SeqNumber: m.SeqNumber, LockToken: m.LockToken})
	}
	items = append(items, SettleItem{SeqNumber: msgs[0].SeqNumber, LockToken: "stale"}) // same seq, wrong token
	items = append(items, SettleItem{SeqNumber: msgs[1].SeqNumber, LockToken: ""})      // no token

	res, err := e.CompleteBatch(ctx, "q", items)
	if err != nil {
		t.Fatalf("CompleteBatch(%d): %v", len(items), err)
	}
	for i, r := range res {
		wantOk := i < n
		if r.Ok != wantOk {
			t.Errorf("item %d (seq %d): Ok=%v, want %v", i, r.SeqNumber, r.Ok, wantOk)
		}
	}
	if m, _ := e.Stats(ctx, "q"); m.Total != 0 {
		t.Errorf("total=%d after completing the whole batch, want 0", m.Total)
	}

	// Re-completing with the SAME tokens is an idempotent success (the settle receipts), while a
	// wrong token is still fenced — the contract the set-based rewrite must preserve.
	again, err := e.CompleteBatch(ctx, "q", items)
	if err != nil {
		t.Fatalf("CompleteBatch replay: %v", err)
	}
	for i, r := range again {
		wantOk := i < n // the same-token replays; the stale/empty ones stay false
		if r.Ok != wantOk {
			t.Errorf("replay item %d (seq %d): Ok=%v, want %v", i, r.SeqNumber, r.Ok, wantOk)
		}
	}
}

// A batch far larger than one statement's bind-parameter budget must still work. The pinned
// SQLite build caps a statement at 32,766 parameters and a (seq, token) pair costs two, so an
// unchunked set-based settle hard-fails somewhere above ~16k items — on batches the previous
// item-by-item loop handled fine, and which sit well inside the HTTP body limit. 8,192 items is
// the size codex flagged; it also crosses the 512-item chunk boundary many times over.
func TestBatchSettleBeyondBindParameterLimit(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{LockDurationMs: 600_000, MaxDeliveryCount: 10})

	const n = 8192
	seed := make([]OutMessage, n)
	for i := range seed {
		seed[i] = OutMessage{Body: []byte("m")}
	}
	if _, err := e.Send(ctx, "q", seed...); err != nil { // one transaction: the fixture is not the test
		t.Fatal(err)
	}

	// Receive caps at 256 per call, so claim the batch in rounds and settle it all at once.
	items := make([]SettleItem, 0, n)
	for len(items) < n {
		msgs, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 256})
		if err != nil {
			t.Fatalf("receive: %v", err)
		}
		if len(msgs) == 0 {
			t.Fatalf("receive returned nothing at %d/%d claimed", len(items), n)
		}
		for _, m := range msgs {
			items = append(items, SettleItem{SeqNumber: m.SeqNumber, LockToken: m.LockToken})
		}
	}

	res, err := e.RenewBatch(ctx, "q", items)
	if err != nil {
		t.Fatalf("RenewBatch(%d): %v", n, err)
	}
	for _, r := range res {
		if !r.Ok {
			t.Fatalf("seq %d was not renewed in an %d-item batch", r.SeqNumber, n)
		}
	}

	res, err = e.CompleteBatch(ctx, "q", items)
	if err != nil {
		t.Fatalf("CompleteBatch(%d): %v", n, err)
	}
	for _, r := range res {
		if !r.Ok {
			t.Fatalf("seq %d was not completed in an %d-item batch", r.SeqNumber, n)
		}
	}
	if m, _ := e.Stats(ctx, "q"); m.Total != 0 {
		t.Errorf("total=%d after settling every message, want 0", m.Total)
	}
}

// A receipt vouches for a TOKEN, so replay detection must only consider receipts that existed
// BEFORE this batch ran. If the lookup happens after the batch writes its own receipts, then a
// batch carrying (wrongSeq, T) alongside the valid (liveSeq, T) sees the receipt it just wrote
// for T and reports Ok for the wrong pair too — claiming a message settled that matched no row
// at all. Same fencing hole as keying results by seq alone, one level over (codex).
func TestCompleteBatchDoesNotVouchForAPairWithItsOwnReceipt(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{LockDurationMs: 600_000, MaxDeliveryCount: 10})
	for i := 0; i < 2; i++ {
		if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte("m")}); err != nil {
			t.Fatal(err)
		}
	}
	msgs, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 2})
	if err != nil || len(msgs) != 2 {
		t.Fatalf("receive: %v n=%d", err, len(msgs))
	}
	live, other := msgs[0], msgs[1]

	// (other.seq, live.token) is a mismatched pair: that token does not fence that row.
	items := []SettleItem{
		{SeqNumber: other.SeqNumber, LockToken: live.LockToken}, // must NOT settle
		{SeqNumber: live.SeqNumber, LockToken: live.LockToken},  // settles, writing a receipt for the token
	}
	res, err := e.CompleteBatch(ctx, "q", items)
	if err != nil {
		t.Fatalf("CompleteBatch: %v", err)
	}
	if res[0].Ok {
		t.Error("a (seq, token) pair that matched no row was reported settled — the batch's own receipt vouched for it")
	}
	if !res[1].Ok {
		t.Error("the valid pair should have settled")
	}
	// And the mismatched item's message is untouched: still there, still locked.
	if m, _ := e.Stats(ctx, "q"); m.Total != 1 || m.Locked != 1 {
		t.Errorf("total=%d locked=%d, want 1/1 — only the valid pair may be deleted", m.Total, m.Locked)
	}
}

// Both batch operations must survive a batch far past a single statement's bind-parameter
// budget. The 8,192-message test above only trips CompleteBatch: its receipt INSERT binds four
// parameters per row, while RenewBatch binds two per pair and so stays under the 32,766 cap
// until ~16k items — meaning RenewBatch's chunking was NOT actually pinned (codex).
//
// A synthetic item list is the cheap way to pin it: the rows need not exist. Nothing matches, so
// every item is correctly Ok=false — but the statements are still built and executed at full
// width, which is exactly what the bind limit cares about.
func TestBatchSettleBindLimitWithSyntheticItems(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{LockDurationMs: 600_000, MaxDeliveryCount: 10})

	const n = 20_000 // > 16,382 pairs: unchunked, RenewBatch alone would exceed the limit
	items := make([]SettleItem, n)
	for i := range items {
		items[i] = SettleItem{SeqNumber: int64(i + 1), LockToken: "tok-" + strconv.Itoa(i)}
	}

	res, err := e.RenewBatch(ctx, "q", items)
	if err != nil {
		t.Fatalf("RenewBatch(%d): %v", n, err)
	}
	for _, r := range res {
		if r.Ok {
			t.Fatalf("seq %d: nothing exists to renew, so no item may report Ok", r.SeqNumber)
		}
	}

	res, err = e.CompleteBatch(ctx, "q", items)
	if err != nil {
		t.Fatalf("CompleteBatch(%d): %v", n, err)
	}
	for _, r := range res {
		if r.Ok {
			t.Fatalf("seq %d: nothing exists to complete, so no item may report Ok", r.SeqNumber)
		}
	}
}
