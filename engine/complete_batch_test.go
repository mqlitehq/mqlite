package engine

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
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

	// Renewal is capped at one statement (MaxRenewBatch): a lease renewed by an earlier statement
	// could expire while a later one is still running, so a multi-statement renewal cannot honestly
	// say which leases still hold. It refuses rather than lie.
	if _, err := e.RenewBatch(ctx, "q", items); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("RenewBatch(%d) must be refused as over the cap, got %v", n, err)
	}
	// The first MaxRenewBatch of them renew fine in one call.
	res, err := e.RenewBatch(ctx, "q", items[:MaxRenewBatch])
	if err != nil {
		t.Fatalf("RenewBatch(%d): %v", MaxRenewBatch, err)
	}
	for _, r := range res {
		if !r.Ok {
			t.Fatalf("seq %d was not renewed in a full-size renewal batch", r.SeqNumber)
		}
	}

	// Completion is TERMINAL, so it may span statements — and must still handle the whole batch.
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

	// RenewBatch is capped at one statement, so it never reaches the bind limit at all — it
	// refuses first, which is the honest answer (see MaxRenewBatch).
	if _, err := e.RenewBatch(ctx, "q", items); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("RenewBatch(%d) must be refused as over the cap, got %v", n, err)
	}
	// At exactly the cap it is one full-width statement — the bind-limit edge that matters for it.
	if _, err := e.RenewBatch(ctx, "q", items[:MaxRenewBatch]); err != nil {
		t.Fatalf("RenewBatch at the cap (%d): %v", MaxRenewBatch, err)
	}

	// CompleteBatch chunks, so it must survive a batch far past ONE statement's bind budget.
	res, err := e.CompleteBatch(ctx, "q", items)
	if err != nil {
		t.Fatalf("CompleteBatch(%d): %v", n, err)
	}
	for _, r := range res {
		if r.Ok {
			t.Fatalf("seq %d: nothing exists to complete, so no item may report Ok", r.SeqNumber)
		}
	}
}

// The lease deadline must be measured against the clock of the write that ACTUALLY LANDS, not
// computed once before a retry loop. A remote retry backs off for up to hundreds of
// milliseconds, so a deadline fixed beforehand can commit a lock that has already expired —
// while RETURNING still reports Ok, and the reaper reclaims the message at once (codex).
//
// The clock is advanced between the call and the write, standing in for that backoff: whatever
// happens in between, the committed deadline must be in the future relative to the clock at
// write time, for both Renew and RenewBatch.
func TestRenewDeadlineMeasuredAtWriteTime(t *testing.T) {
	ctx := context.Background()
	e, ms := testEngine(t)
	const lockMs = 30_000
	mustQueue(t, e, "q", QueueConfig{LockDurationMs: lockMs, MaxDeliveryCount: 10})
	for i := 0; i < 2; i++ {
		if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte("m")}); err != nil {
			t.Fatal(err)
		}
	}
	msgs, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 2})
	if err != nil || len(msgs) != 2 {
		t.Fatalf("receive: %v n=%d", err, len(msgs))
	}

	// Stand in for a retry's backoff: time passes before the write lands.
	advance(ms, 20*time.Second)
	writeTime := atomic.LoadInt64(ms)

	if err := e.Renew(ctx, "q", msgs[0].SeqNumber, msgs[0].LockToken); err != nil {
		t.Fatalf("Renew: %v", err)
	}
	res, err := e.RenewBatch(ctx, "q", []SettleItem{{SeqNumber: msgs[1].SeqNumber, LockToken: msgs[1].LockToken}})
	if err != nil || !res[0].Ok {
		t.Fatalf("RenewBatch: %v ok=%v", err, res[0].Ok)
	}

	locked, err := e.Peek(ctx, "q", PeekOptions{State: StateLocked, Max: 10})
	if err != nil || len(locked) != 2 {
		t.Fatalf("peek locked: %v n=%d", err, len(locked))
	}
	for _, p := range locked {
		// A deadline computed before the elapsed time would land at writeTime-20s+lockMs.
		if want := writeTime + lockMs; p.LockedUntilMs != want {
			t.Errorf("seq %d: locked_until=%d, want %d — the deadline was not measured at write time",
				p.SeqNumber, p.LockedUntilMs, want)
		}
		if p.LockedUntilMs <= writeTime {
			t.Errorf("seq %d: committed a lease already expired at write time", p.SeqNumber)
		}
	}
}

// Two promises RenewBatch has to keep, both of them about the moment it ANSWERS (codex):
//
//  1. A renewal only ever EXTENDS a lease. Two renewals can race — a retry, a second consumer
//     process — and the loser must not pull a lock back in by writing its older deadline.
//  2. Ok means the lease is live. If the write itself outlives the lease (a slow remote store and
//     a short lock), the deadline it just committed is already spent and the reaper may take the
//     message at any moment. Reporting Ok there would make the caller settle a message it no
//     longer holds.
func TestRenewBatchNeverShortensAndNeverLiesAboutADeadLease(t *testing.T) {
	ctx := context.Background()
	e, ms := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{LockDurationMs: 10_000, MaxDeliveryCount: 10})
	if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte("m")}); err != nil {
		t.Fatal(err)
	}
	msgs, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 1})
	if err != nil || len(msgs) != 1 {
		t.Fatalf("receive: %v n=%d", err, len(msgs))
	}
	item := SettleItem{SeqNumber: msgs[0].SeqNumber, LockToken: msgs[0].LockToken}

	// 1. A renewal at t+5s pushes the lease to t+15s. A LATER renewal that (because the clock went
	//    backwards, or a retry replayed an older deadline) would write t+10s must not shorten it.
	advance(ms, 5*time.Second)
	if _, err := e.RenewBatch(ctx, "q", []SettleItem{item}); err != nil {
		t.Fatal(err)
	}
	locked, _ := e.Peek(ctx, "q", PeekOptions{State: StateLocked, Max: 1})
	if len(locked) != 1 {
		t.Fatal("peek locked")
	}
	pushedTo := locked[0].LockedUntilMs

	advance(ms, -4*time.Second) // a stale/racing renewal computing an OLDER deadline
	if _, err := e.RenewBatch(ctx, "q", []SettleItem{item}); err != nil {
		t.Fatal(err)
	}
	locked, _ = e.Peek(ctx, "q", PeekOptions{State: StateLocked, Max: 1})
	if locked[0].LockedUntilMs < pushedTo {
		t.Errorf("the lease was SHORTENED: %d -> %d; a renewal may only ever extend it",
			pushedTo, locked[0].LockedUntilMs)
	}

	// 2. Now let the lease actually expire. A renewal that lands after it is gone matches no row
	//    (the reaper cleared the token) or writes a dead deadline — either way it must NOT say Ok.
	advance(ms, time.Hour)
	e.RunMaintenanceOnce(ctx) // the reaper reclaims the expired lock
	res, err := e.RenewBatch(ctx, "q", []SettleItem{item})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Ok {
		t.Error("RenewBatch reported Ok for a lease that no longer exists")
	}
}

// The deadline is verified AFTER the statement completes — that is the only way to know whether
// the lease it just wrote is actually live. When the write itself outlives the lock (a slow remote
// store, a short lease), the row is updated but its new deadline is already spent, and the reaper
// may take the message at any moment. RenewBatch must say Ok=false there, not hand the caller a
// lock it does not hold.
//
// A clock that jumps forward between reads stands in for that slow write: the deadline is computed
// at one instant and checked at a much later one, exactly as it would be across a slow round trip.
func TestRenewBatchRefusesToClaimALeaseTheWriteOutlived(t *testing.T) {
	ctx := context.Background()
	var ms int64 = 1_700_000_000_000
	e, err := Open(ctx, Options{
		DB: ":memory:", DisableBackground: true,
		Now: func() int64 { return atomic.LoadInt64(&ms) },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	mustQueue(t, e, "q", QueueConfig{LockDurationMs: 1000, MaxDeliveryCount: 10}) // a 1s lease
	if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte("m")}); err != nil {
		t.Fatal(err)
	}
	msgs, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 1})
	if err != nil || len(msgs) != 1 {
		t.Fatalf("receive: %v n=%d", err, len(msgs))
	}
	item := SettleItem{SeqNumber: msgs[0].SeqNumber, LockToken: msgs[0].LockToken}

	// From here on, every clock read jumps an hour: whatever deadline the write computes, it is
	// long gone by the time the statement finishes.
	e.now = func() int64 { return atomic.AddInt64(&ms, time.Hour.Milliseconds()) }

	res, err := e.RenewBatch(ctx, "q", []SettleItem{item})
	if err != nil {
		t.Fatalf("RenewBatch: %v", err)
	}
	if res[0].Ok {
		t.Error("RenewBatch claimed Ok for a lease that was already expired when the write completed — the caller would settle a message it no longer holds")
	}
}

// ─── settlement receipts are VERB-SPECIFIC (round-4 §3) ────────────────────────

// A receipt vouches for the verb that WROTE it, not merely for the token.
//
// Receipts make a lost settle response replayable: the same verb, same token, same success. They
// are not a licence for a DIFFERENT verb to claim that success. Abandon(T) returns a message to
// `active` — and used to leave a receipt that a later Complete(T) read as "already completed",
// telling the caller the message was gone while it sat in the queue waiting for somebody else.
// At-least-once permits redelivery; it does not permit a successful Complete for a message
// Complete never removed.
//
// The full 4×4: only the diagonal — the same verb replayed — may succeed, and every cell asserts
// the message's ACTUAL state, not just the return value.
func TestSettlementReceiptsAreVerbSpecific(t *testing.T) {
	type verb struct {
		name  string
		call  func(e *Engine, ctx context.Context, seq int64, tok string) error
		state State // where this verb leaves the message
	}
	verbs := []verb{
		{"Complete", func(e *Engine, ctx context.Context, s int64, tk string) error {
			return e.Complete(ctx, "q", s, tk)
		}, ""}, // gone
		{"Abandon", func(e *Engine, ctx context.Context, s int64, tk string) error {
			return e.Abandon(ctx, "q", s, tk, 0)
		}, StateActive},
		{"Reject", func(e *Engine, ctx context.Context, s int64, tk string) error {
			return e.Reject(ctx, "q", s, tk, ReasonAppRequested, "")
		}, StateDeadLettered},
		{"Defer", func(e *Engine, ctx context.Context, s int64, tk string) error {
			return e.Defer(ctx, "q", s, tk)
		}, StateDeferred},
	}

	for _, first := range verbs {
		for _, second := range verbs {
			t.Run(first.name+"_then_"+second.name, func(t *testing.T) {
				ctx := context.Background()
				e, _ := testEngine(t)
				mustQueue(t, e, "q", QueueConfig{LockDurationMs: 600_000, MaxDeliveryCount: 10})
				if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte("m")}); err != nil {
					t.Fatal(err)
				}
				msgs, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 1})
				if err != nil || len(msgs) != 1 {
					t.Fatalf("receive: %v n=%d", err, len(msgs))
				}
				seq, tok := msgs[0].SeqNumber, msgs[0].LockToken

				if err := first.call(e, ctx, seq, tok); err != nil {
					t.Fatalf("%s: %v", first.name, err)
				}

				err = second.call(e, ctx, seq, tok)
				same := first.name == second.name
				if same && err != nil {
					t.Errorf("replaying %s with the same token must be an idempotent success, got %v",
						second.name, err)
				}
				if !same && !errors.Is(err, ErrLockLost) {
					t.Errorf("%s after %s returned %v — a receipt written by %s must not vouch for %s; the message is still %q",
						second.name, first.name, err, first.name, second.name, first.state)
				}

				// Whatever was returned, the message must still be where the FIRST verb left it.
				m, err := e.Stats(ctx, "q")
				if err != nil {
					t.Fatal(err)
				}
				got := map[State]int64{
					StateActive: m.Active, StateDeadLettered: m.DeadLettered, StateDeferred: m.Deferred,
				}
				if first.state == "" { // Complete removed it
					if m.Total != 0 {
						t.Errorf("total=%d after %s+%s, want 0 — the message must stay completed", m.Total, first.name, second.name)
					}
					return
				}
				if got[first.state] != 1 || m.Total != 1 {
					t.Errorf("after %s+%s the message is not %s (total=%d active=%d dead=%d deferred=%d)",
						first.name, second.name, first.state, m.Total, m.Active, m.DeadLettered, m.Deferred)
				}
			})
		}
	}
}

// CompleteBatch carries the same rule: only a COMPLETION may vouch for a completion. A token
// abandoned earlier must come back ok=false, not a false success for a message still in the queue.
func TestCompleteBatchReceiptIsVerbSpecific(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{LockDurationMs: 600_000, MaxDeliveryCount: 10})
	if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte("m")}); err != nil {
		t.Fatal(err)
	}
	msgs, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 1})
	if err != nil || len(msgs) != 1 {
		t.Fatalf("receive: %v n=%d", err, len(msgs))
	}
	seq, tok := msgs[0].SeqNumber, msgs[0].LockToken

	if err := e.Abandon(ctx, "q", seq, tok, 0); err != nil { // back to active, receipt "abandoned"
		t.Fatal(err)
	}
	res, err := e.CompleteBatch(ctx, "q", []SettleItem{{SeqNumber: seq, LockToken: tok}})
	if err != nil {
		t.Fatalf("CompleteBatch: %v", err)
	}
	if res[0].Ok {
		t.Error("CompleteBatch reported ok for a token that was ABANDONED — the message is still in the queue, waiting to be handed to somebody else")
	}
	if m, _ := e.Stats(ctx, "q"); m.Active != 1 || m.Total != 1 {
		t.Errorf("active=%d total=%d, want 1/1 — the abandoned message must still be there", m.Active, m.Total)
	}

	// And a genuine completion still replays as an idempotent success.
	msgs, err = e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 1})
	if err != nil || len(msgs) != 1 {
		t.Fatalf("re-receive: %v n=%d", err, len(msgs))
	}
	item := SettleItem{SeqNumber: msgs[0].SeqNumber, LockToken: msgs[0].LockToken}
	if res, err := e.CompleteBatch(ctx, "q", []SettleItem{item}); err != nil || !res[0].Ok {
		t.Fatalf("CompleteBatch: %v ok=%v", err, res[0].Ok)
	}
	if res, err := e.CompleteBatch(ctx, "q", []SettleItem{item}); err != nil || !res[0].Ok {
		t.Fatalf("replaying the SAME completion must stay an idempotent success: %v ok=%v", err, res[0].Ok)
	}
}

// A receipt vouches for ONE REQUEST — this queue, this seq, this token, this verb. Not for a token.
//
// Binding the verb (round-4) closed Abandon(T)→Complete(T). It left the deeper hole: the receipt
// still said nothing about WHICH MESSAGE it settled, so `Complete(seqB, tokenA)` found tokenA's
// completion receipt and reported success for a message it never touched — in the same queue, in
// the batch path, and even across queues. A settle is a claim about one message; its receipt must
// be too (round-5 §3).
func TestReceiptsAreBoundToTheirMessage(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{LockDurationMs: 600_000, MaxDeliveryCount: 10})
	mustQueue(t, e, "other", QueueConfig{LockDurationMs: 600_000, MaxDeliveryCount: 10})

	for i := 0; i < 2; i++ {
		if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte("m")}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.SendOne(ctx, "other", OutMessage{Body: []byte("m")}); err != nil {
		t.Fatal(err)
	}
	msgs, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 2})
	if err != nil || len(msgs) != 2 {
		t.Fatalf("receive: %v n=%d", err, len(msgs))
	}
	elsewhere, err := e.Receive(ctx, "other", ReceiveOptions{MaxMessages: 1})
	if err != nil || len(elsewhere) != 1 {
		t.Fatalf("receive other: %v n=%d", err, len(elsewhere))
	}
	a, b := msgs[0], msgs[1]

	// A is completed for real: that writes a receipt for (q, seqA, tokenA, completed).
	if err := e.Complete(ctx, "q", a.SeqNumber, a.LockToken); err != nil {
		t.Fatal(err)
	}

	// 1. A's receipt must not settle B.
	if err := e.Complete(ctx, "q", b.SeqNumber, a.LockToken); !errors.Is(err, ErrLockLost) {
		t.Errorf("Complete(seqB, tokenA) = %v, want ErrLockLost — A's receipt says nothing about B", err)
	}
	// 2. Nor through the batch path.
	res, err := e.CompleteBatch(ctx, "q", []SettleItem{{SeqNumber: b.SeqNumber, LockToken: a.LockToken}})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Ok {
		t.Error("CompleteBatch(seqB, tokenA) reported ok — B was never touched and is still locked")
	}
	// 3. Nor in another queue.
	if err := e.Complete(ctx, "other", elsewhere[0].SeqNumber, a.LockToken); !errors.Is(err, ErrLockLost) {
		t.Errorf("Complete(otherQueue, tokenA) = %v, want ErrLockLost", err)
	}

	// B and the other queue's message are untouched — still locked, still there.
	if m, _ := e.Stats(ctx, "q"); m.Locked != 1 || m.Total != 1 {
		t.Errorf("queue q: locked=%d total=%d, want 1/1 — B must be exactly where it was", m.Locked, m.Total)
	}
	if m, _ := e.Stats(ctx, "other"); m.Locked != 1 || m.Total != 1 {
		t.Errorf("queue other: locked=%d total=%d, want 1/1", m.Locked, m.Total)
	}

	// And the genuine replay — the SAME request — is still an idempotent success.
	if err := e.Complete(ctx, "q", a.SeqNumber, a.LockToken); err != nil {
		t.Errorf("replaying the same Complete must stay an idempotent success, got %v", err)
	}
}
