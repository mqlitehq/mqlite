package engine

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"sort"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// MQLITE-114: the full exported settlement surface shares the same lease boundary.
// Pin the inventory against settle.go so adding a method requires extending this matrix.
var deadlineOperations = []string{"Abandon", "Complete", "CompleteBatch", "Defer", "Reject", "Renew", "RenewBatch"}

func TestSettlementDeadlineSurface(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "settle.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var methods []string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && fn.Recv != nil && ast.IsExported(fn.Name.Name) && fn.Name.Name != "CompletedCounts" {
			methods = append(methods, fn.Name.Name)
		}
	}
	sort.Strings(methods)
	if !reflect.DeepEqual(methods, deadlineOperations) {
		t.Fatalf("settlement surface changed: got %v, matrix covers %v", methods, deadlineOperations)
	}
}

// Normalize a one-item batch's documented Ok=false to the single-operation sentinel.
// Result shape and lease metadata remain checked rather than reduced to a loose bool.
func callDeadlineOperation(ctx context.Context, e *Engine, op, queue string, item SettleItem) error {
	switch op {
	case "Complete":
		return e.Complete(ctx, queue, item.SeqNumber, item.LockToken)
	case "Abandon":
		return e.Abandon(ctx, queue, item.SeqNumber, item.LockToken, 500)
	case "Reject":
		return e.Reject(ctx, queue, item.SeqNumber, item.LockToken, "deadline", "test")
	case "Defer":
		return e.Defer(ctx, queue, item.SeqNumber, item.LockToken)
	case "Renew":
		return e.Renew(ctx, queue, item.SeqNumber, item.LockToken)
	case "CompleteBatch", "RenewBatch":
		var result []SettleResult
		var err error
		if op == "CompleteBatch" {
			result, err = e.CompleteBatch(ctx, queue, []SettleItem{item})
		} else {
			result, err = e.RenewBatch(ctx, queue, []SettleItem{item})
		}
		if err != nil {
			return err
		}
		if len(result) != 1 || result[0].SeqNumber != item.SeqNumber ||
			((op == "CompleteBatch" || !result[0].Ok) && result[0].LockedUntilMs != 0) {
			return fmt.Errorf("invalid %s result: %+v", op, result)
		}
		if !result[0].Ok {
			return ErrLockLost
		}
		return nil
	default:
		return fmt.Errorf("uncovered settlement operation %q", op)
	}
}

func deadlineFixture(t *testing.T, dsn string) (*Engine, *atomic.Int64) {
	t.Helper()
	clock := new(atomic.Int64)
	clock.Store(100_000)
	e, err := Open(context.Background(), Options{DB: dsn, Now: clock.Load, DisableBackground: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	mustQueue(t, e, "q", QueueConfig{LockDurationMs: 1_000})
	return e, clock
}

func deadlineLock(t *testing.T, e *Engine) SettleItem {
	t.Helper()
	if _, err := e.SendOne(context.Background(), "q", OutMessage{Body: []byte("kept")}); err != nil {
		t.Fatal(err)
	}
	m := recvOne(t, e, "q")
	if m == nil {
		t.Fatal("missing fixture lease")
	}
	return SettleItem{SeqNumber: m.SeqNumber, LockToken: m.LockToken}
}

func deadlineAfterWriterWait(t *testing.T, e *Engine, advanceClock func(), call func() error) error {
	t.Helper()
	var result error
	claimAfterWriterWait(t, e, advanceClock, func() ([]*Message, error) {
		result = call()
		return nil, nil // the operation's ErrLockLost is an expected result, not a helper error
	})
	return result
}

// Both local stores, every operation, and both direct and queued admission paths
// must treat exactly locked_until as expired. Maintenance never runs in this test.
func TestSettlementLeaseDeadline(t *testing.T) {
	for _, op := range deadlineOperations {
		for _, wait := range []bool{false, true} {
			for _, delta := range []int64{-1, 0, 1} {
				t.Run(fmt.Sprintf("%s/wait=%t/deadline%+d", op, wait, delta), func(t *testing.T) {
					eachLocalStore(t, func(t *testing.T, dsn string) {
						ctx := context.Background()
						e, clock := deadlineFixture(t, dsn)
						item := deadlineLock(t, e)
						before, err := e.Peek(ctx, "q", PeekOptions{Max: 10})
						if err != nil || len(before) != 1 {
							t.Fatalf("snapshot: %+v, %v", before, err)
						}
						at := before[0].LockedUntilMs + delta
						call := func() error { return callDeadlineOperation(ctx, e, op, "q", item) }
						if wait {
							err = deadlineAfterWriterWait(t, e, func() { clock.Store(at) }, call)
						} else {
							clock.Store(at)
							err = call()
						}
						if delta < 0 {
							if err != nil {
								t.Fatalf("live lease was refused: %v", err)
							}
						} else if !errors.Is(err, ErrLockLost) {
							t.Fatalf("unreaped expired lease must fail: got %v", err)
						}
						after, err := e.Peek(ctx, "q", PeekOptions{Max: 10})
						if err != nil {
							t.Fatal(err)
						}
						if delta >= 0 && !reflect.DeepEqual(before, after) {
							t.Fatalf("expired lease changed message identity/state: before=%+v after=%+v", before, after)
						}
						terminal := op != "Renew" && op != "RenewBatch"
						var receipts int64
						if err := e.db.queryRowScan(ctx, []any{&receipts}, `SELECT COUNT(*) FROM settlement_receipts`); err != nil {
							t.Fatal(err)
						}
						wantReceipts := int64(0)
						if delta < 0 && terminal {
							wantReceipts = 1
						}
						if receipts != wantReceipts {
							t.Fatalf("receipts=%d, want %d", receipts, wantReceipts)
						}
						wantCompleted := uint64(0)
						if delta < 0 && (op == "Complete" || op == "CompleteBatch") {
							wantCompleted = 1
							if len(after) != 0 {
								t.Fatal("successful completion retained the message")
							}
						} else if delta < 0 {
							if len(after) != 1 || after[0].SeqNumber != item.SeqNumber || after[0].DeliveryCount != 1 {
								t.Fatalf("unexpected post-settlement identity/delivery: %+v", after)
							}
							want := map[string]State{"Abandon": StateScheduled, "Reject": StateDeadLettered, "Defer": StateDeferred, "Renew": StateLocked, "RenewBatch": StateLocked}[op]
							if after[0].State != want || (!terminal && after[0].LockedUntilMs != at+1_000) ||
								(op == "Abandon" && after[0].VisibleAtMs != at+500) {
								t.Fatalf("wrong live effect at write time %d: %+v", at, after[0])
							}
						}
						if e.CompletedCounts()["q"] != wantCompleted {
							t.Fatalf("completed count=%d, want %d", e.CompletedCounts()["q"], wantCompleted)
						}
					})
				})
			}
		}
	}
}

func TestSettlementReceiptTimeAfterWriterAdmission(t *testing.T) {
	for _, op := range []string{"Abandon", "Complete", "CompleteBatch", "Defer", "Reject"} {
		t.Run(op, func(t *testing.T) {
			eachLocalStore(t, func(t *testing.T, dsn string) {
				ctx := context.Background()
				e, clock := deadlineFixture(t, dsn)
				item := deadlineLock(t, e)
				call := func() error { return callDeadlineOperation(ctx, e, op, "q", item) }
				if err := deadlineAfterWriterWait(t, e, func() { clock.Add(500) }, call); err != nil {
					t.Fatal(err)
				}
				var created, expires int64
				if err := e.db.queryRowScan(ctx, []any{&created, &expires},
					`SELECT created_at, expires_at FROM settlement_receipts WHERE queue='q' AND seq_number=?`, item.SeqNumber); err != nil {
					t.Fatal(err)
				}
				if created != clock.Load() || expires != created+settlementTTLMs {
					t.Fatalf("receipt lifetime predates admission: created=%d expires=%d now=%d", created, expires, clock.Load())
				}
				// Enter before receipt expiry, but obtain the writer exactly at expiry.
				// The old message lease is irrelevant to the live receipt replay.
				clock.Store(expires - 1)
				if err := call(); err != nil {
					t.Fatalf("live receipt after original lease: %v", err)
				}
				if err := deadlineAfterWriterWait(t, e, func() { clock.Store(expires) }, call); !errors.Is(err, ErrLockLost) {
					t.Fatalf("receipt expired during admission wait must fail: %v", err)
				}
			})
		})
	}
}

func TestBatchSettlementMixedLeaseDeadlines(t *testing.T) {
	for _, op := range []string{"CompleteBatch", "RenewBatch"} {
		t.Run(op, func(t *testing.T) {
			eachLocalStore(t, func(t *testing.T, dsn string) {
				ctx := context.Background()
				e, clock := deadlineFixture(t, dsn)
				expired := deadlineLock(t, e)
				clock.Add(500)
				live := deadlineLock(t, e)
				receipt := deadlineLock(t, e)
				if err := e.Complete(ctx, "q", receipt.SeqNumber, receipt.LockToken); err != nil {
					t.Fatal(err)
				}
				mustQueue(t, e, "other", QueueConfig{LockDurationMs: 1_000})
				if _, err := e.SendOne(ctx, "other", OutMessage{Body: []byte("other")}); err != nil {
					t.Fatal(err)
				}
				other := recvOne(t, e, "other")
				if other == nil {
					t.Fatal("missing other queue fixture")
				}
				clock.Add(500) // expired is exactly at its deadline; live still has 500 ms
				before, err := e.Peek(ctx, "q", PeekOptions{Max: 10})
				if err != nil || len(before) != 2 {
					t.Fatalf("before: %+v, %v", before, err)
				}
				otherBefore, err := e.Peek(ctx, "other", PeekOptions{Max: 10})
				if err != nil {
					t.Fatal(err)
				}
				pattern := []SettleItem{live, expired, {SeqNumber: live.SeqNumber, LockToken: "wrong"}, live,
					{SeqNumber: live.SeqNumber}, {SeqNumber: other.SeqNumber, LockToken: other.LockToken},
					{SeqNumber: 999_999, LockToken: "missing"}, receipt}
				var items []SettleItem
				for len(items) < MaxRenewBatch {
					items = append(items, pattern...)
				}
				if op == "CompleteBatch" {
					items = append(items, live) // duplicate of a successful earlier chunk
				}
				var result []SettleResult
				if op == "CompleteBatch" {
					result, err = e.CompleteBatch(ctx, "q", items)
				} else {
					result, err = e.RenewBatch(ctx, "q", items)
				}
				if err != nil || len(result) != len(items) {
					t.Fatalf("batch: %+v, %v", result, err)
				}
				for i, got := range result {
					want := i%len(pattern) == 0 || i%len(pattern) == 3 ||
						(op == "CompleteBatch" && i%len(pattern) == 7)
					until := int64(0)
					if op == "RenewBatch" && want {
						until = clock.Load() + 1_000
					}
					if got.SeqNumber != items[i].SeqNumber || got.Ok != want || got.LockedUntilMs != until {
						t.Fatalf("item %d: %+v, want ok=%t until=%d", i, got, want, until)
					}
				}
				after, err := e.Peek(ctx, "q", PeekOptions{Max: 10})
				if err != nil || len(after) == 0 || !reflect.DeepEqual(before[0], after[0]) {
					t.Fatalf("expired row changed without maintenance: before=%+v after=%+v err=%v", before, after, err)
				}
				otherAfter, err := e.Peek(ctx, "other", PeekOptions{Max: 10})
				if err != nil || !reflect.DeepEqual(otherBefore, otherAfter) {
					t.Fatal("wrong queue identity changed an unrelated message")
				}
				wantCompleted := uint64(1) // the receipt fixture was completed once
				if op == "CompleteBatch" {
					wantCompleted++
				}
				if e.CompletedCounts()["q"] != wantCompleted {
					t.Fatalf("duplicate/invalid items inflated completed count: %v", e.CompletedCounts())
				}
			})
		})
	}
}

func TestCompleteBatchRefreshesTimeBetweenChunks(t *testing.T) {
	for _, path := range []string{"new_effect", "receipt_replay"} {
		t.Run(path, func(t *testing.T) {
			eachLocalStore(t, func(t *testing.T, dsn string) {
				ctx := context.Background()
				e, clock := deadlineFixture(t, dsn)
				first, last := deadlineLock(t, e), deadlineLock(t, e)
				boundary := clock.Load() + 1_000
				if path == "receipt_replay" {
					for _, item := range []SettleItem{first, last} {
						if err := e.Complete(ctx, "q", item.SeqNumber, item.LockToken); err != nil {
							t.Fatal(err)
						}
					}
					boundary = clock.Load() + settlementTTLMs
				}
				items := make([]SettleItem, settleChunk+1)
				for i := range items {
					items[i] = SettleItem{SeqNumber: 999_999, LockToken: "wrong"}
				}
				items[0], items[settleChunk] = first, last
				// Each call represents one statement admission. No real sleep or reaper:
				// the first relevant chunk is just before expiry, the second exactly at it.
				if path == "new_effect" {
					clock.Store(boundary - 2)
				} else {
					clock.Store(boundary - 4) // two DELETE chunks precede the receipt reads
				}
				e.now = func() int64 { return clock.Add(1) }
				result, err := e.CompleteBatch(ctx, "q", items)
				e.now = clock.Load
				if err != nil || len(result) != len(items) {
					t.Fatalf("batch: %+v, %v", result, err)
				}
				for i, got := range result {
					if got.Ok != (i == 0) || got.SeqNumber != items[i].SeqNumber || got.LockedUntilMs != 0 {
						t.Fatalf("chunk item %d crossed its deadline incorrectly: %+v", i, got)
					}
				}
				if path == "new_effect" {
					remaining, err := e.Peek(ctx, "q", PeekOptions{Max: 10})
					if err != nil || len(remaining) != 1 || remaining[0].SeqNumber != last.SeqNumber ||
						remaining[0].State != StateLocked || remaining[0].LockedUntilMs != boundary || remaining[0].DeliveryCount != 1 {
						t.Fatalf("expired final chunk must remain unchanged: %+v, %v", remaining, err)
					}
					var created, expires int64
					if err := e.db.queryRowScan(ctx, []any{&created, &expires},
						`SELECT created_at, expires_at FROM settlement_receipts WHERE seq_number=?`, first.SeqNumber); err != nil {
						t.Fatal(err)
					}
					if created <= boundary || expires != created+settlementTTLMs {
						t.Fatalf("receipt insertion reused an earlier chunk's time: created=%d expires=%d boundary=%d", created, expires, boundary)
					}
				}
			})
		})
	}
}

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

// A receipt vouches for the exact request. A batch carrying (wrongSeq, T) alongside the valid
// (liveSeq, T) must never use the valid pair's receipt to report success for the wrong pair.
// The token-only lookup previously did that after writing its own receipts — claiming a
// message settled that matched no row. Both receipt and result matching must keep the full identity.
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
		{SeqNumber: live.SeqNumber, LockToken: live.LockToken},  // settles, writing a receipt for this Complete request
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

	// 2. Expiry itself fences renewal, even while the unreaped row retains its token.
	advance(ms, time.Hour)
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

	// The old lease is still live when the write begins. Only the following read,
	// after RETURNING reaches EOF, crosses the newly committed deadline.
	var reads int
	e.now = func() int64 {
		reads++
		if reads == 1 {
			return atomic.AddInt64(&ms, 500)
		}
		return atomic.AddInt64(&ms, time.Hour.Milliseconds())
	}

	res, err := e.RenewBatch(ctx, "q", []SettleItem{item})
	if err != nil {
		t.Fatalf("RenewBatch: %v", err)
	}
	if res[0].Ok {
		t.Error("RenewBatch claimed Ok for a lease that was already expired when the write completed — the caller would settle a message it no longer holds")
	}
	var committed int64
	if err := e.db.queryRowScan(ctx, []any{&committed}, `SELECT locked_until FROM messages WHERE id=?`, item.SeqNumber); err != nil {
		t.Fatal(err)
	}
	if committed != msgs[0].LockedUntilMs+500 {
		t.Fatalf("the write must have extended the live lease before its response became stale: %d", committed)
	}
}

// ─── settlement receipts are VERB-SPECIFIC (round-4 §3) ────────────────────────

// A receipt vouches for the verb that WROTE it, not merely for the token.
//
// Receipts make a lost settle response replayable: the same request has the same success. They
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

// ─── receipt identity: the ARGUMENTS ──────────────────────────────────────────

// MQLITE-104: cross every receipt writer with every reader and vary each identity
// field independently. In particular, changing only the queue preserves the seq
// and token needed to expose a missing queue predicate in the batch receipt lookup.
func TestSettlementReceiptIdentityMatrix(t *testing.T) {
	type operation struct {
		name, verb, reason, desc string
		delay                    int64
		batch                    bool
	}
	ops := []operation{
		{name: "Complete", verb: "completed"},
		{name: "CompleteBatch", verb: "completed", batch: true},
		{name: "Abandon", verb: "abandoned"},
		{name: "AbandonDelayed", verb: "abandoned", delay: 5_000},
		{name: "Reject", verb: "dead_lettered", reason: "PoisonMessage", desc: "first failure"},
		{name: "RejectOtherReason", verb: "dead_lettered", reason: "OtherReason", desc: "first failure"},
		{name: "RejectOtherDescription", verb: "dead_lettered", reason: "PoisonMessage", desc: "other failure"},
		{name: "Defer", verb: "deferred"},
	}
	ctx := context.Background()
	for _, first := range ops {
		t.Run(first.name, func(t *testing.T) {
			e, clock := testEngine(t)
			created := atomic.LoadInt64(clock)
			for _, q := range []string{"q", "other"} {
				mustQueue(t, e, q, QueueConfig{LockDurationMs: 600_000, MaxDeliveryCount: 10})
				for i := 0; i < 2; i++ {
					if _, err := e.SendOne(ctx, q, OutMessage{Body: []byte("m")}); err != nil {
						t.Fatal(err)
					}
				}
			}
			msgs, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 2})
			if err != nil || len(msgs) != 2 {
				t.Fatalf("receive: n=%d err=%v", len(msgs), err)
			}
			if other, err := e.Receive(ctx, "other", ReceiveOptions{MaxMessages: 2}); err != nil || len(other) != 2 {
				t.Fatalf("receive other: n=%d err=%v", len(other), err)
			}
			a, b := msgs[0], msgs[1]
			call := func(t *testing.T, op operation, q string, seq int64, token string) bool {
				t.Helper()
				if op.batch {
					res, err := e.CompleteBatch(ctx, q, []SettleItem{{SeqNumber: seq, LockToken: token}})
					if err != nil || len(res) != 1 {
						t.Fatalf("CompleteBatch: results=%v err=%v", res, err)
					}
					if res[0].SeqNumber != seq || res[0].LockedUntilMs != 0 {
						t.Fatalf("unexpected batch result: %+v", res[0])
					}
					return res[0].Ok
				}
				var err error
				switch op.verb {
				case "completed":
					err = e.Complete(ctx, q, seq, token)
				case "abandoned":
					err = e.Abandon(ctx, q, seq, token, op.delay)
				case "dead_lettered":
					err = e.Reject(ctx, q, seq, token, op.reason, op.desc)
				case "deferred":
					err = e.Defer(ctx, q, seq, token)
				}
				if err != nil && !errors.Is(err, ErrLockLost) {
					t.Fatalf("%s: %v", op.name, err)
				}
				return err == nil
			}
			if !call(t, first, "q", a.SeqNumber, a.LockToken) {
				t.Fatal("initial settlement lost its live lock")
			}
			snapshot := func(t *testing.T) map[string][]*PeekedMessage {
				t.Helper()
				out := make(map[string][]*PeekedMessage)
				for _, q := range []string{"q", "other"} {
					var err error
					out[q], err = e.Peek(ctx, q, PeekOptions{Max: 10})
					if err != nil {
						t.Fatal(err)
					}
				}
				return out
			}
			before, completed := snapshot(t), e.CompletedCounts()
			identities := []struct {
				name, queue, token string
				seq                int64
			}{
				{"exact", "q", a.LockToken, a.SeqNumber},
				{"wrong_queue", "other", a.LockToken, a.SeqNumber},
				{"wrong_seq", "q", a.LockToken, b.SeqNumber},
				{"wrong_token", "q", b.LockToken, a.SeqNumber},
				{"empty_token", "q", "", a.SeqNumber},
			}
			for _, timing := range []struct {
				name  string
				delta int64
				live  bool
			}{
				{"initial", 0, true},
				{"lease_expired", 600_001, true},
				{"receipt_before", settlementTTLMs - 1, true},
				{"receipt_exact", settlementTTLMs, false},
				{"receipt_after", settlementTTLMs + 1, false},
			} {
				atomic.StoreInt64(clock, created+timing.delta)
				for _, second := range ops {
					for _, id := range identities {
						t.Run(timing.name+"/"+second.name+"/"+id.name, func(t *testing.T) {
							want := timing.live && id.name == "exact" && first.verb == second.verb &&
								first.delay == second.delay && first.reason == second.reason && first.desc == second.desc
							if got := call(t, second, id.queue, id.seq, id.token); got != want {
								t.Errorf("receipt replay ok=%v, want %v", got, want)
							}
							if after := snapshot(t); !reflect.DeepEqual(after, before) {
								t.Errorf("receipt replay changed messages: before=%+v after=%+v", before, after)
							}
							if after := e.CompletedCounts(); !reflect.DeepEqual(after, completed) {
								t.Errorf("receipt replay changed completed counts: before=%v after=%v", completed, after)
							}
						})
					}
				}
			}
		})
	}
}

// A receipt vouches for a REQUEST. Same message, same token, same verb — but a different delay or
// a different dead-letter reason is a DIFFERENT request, and it must not inherit the first one's
// success. It used to: Abandon(T, 60s) after Abandon(T, 5s) returned nil while the message quietly
// kept the 5s delay, so a client's backoff was silently discarded and the message came back early
// (round-6 §3). Reject's reason/description had the same hole — the operator would read the wrong
// text out of the DLQ and be told the write had succeeded.
func TestReceiptsAreBoundToTheirArguments(t *testing.T) {
	ctx := context.Background()
	e, err := Open(ctx, Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	mustQueue(t, e, "q", QueueConfig{LockDurationMs: 60_000, MaxDeliveryCount: 10})

	lock := func() *Message {
		t.Helper()
		if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte("x")}); err != nil {
			t.Fatal(err)
		}
		got, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 1})
		if err != nil || len(got) != 1 {
			t.Fatalf("receive: %v (%d)", err, len(got))
		}
		return got[0]
	}

	t.Run("abandon delay", func(t *testing.T) {
		m := lock()
		if err := e.Abandon(ctx, "q", m.SeqNumber, m.LockToken, 5_000); err != nil {
			t.Fatalf("first abandon: %v", err)
		}
		// The SAME request, replayed (a lost response): idempotent success.
		if err := e.Abandon(ctx, "q", m.SeqNumber, m.LockToken, 5_000); err != nil {
			t.Fatalf("replaying the same Abandon must be an idempotent success, got %v", err)
		}
		// A DIFFERENT delay is a different request on a message this caller no longer holds.
		err := e.Abandon(ctx, "q", m.SeqNumber, m.LockToken, 60_000)
		if !errors.Is(err, ErrLockLost) {
			t.Fatalf("Abandon with a CHANGED delay must be ErrLockLost — it cannot report success while\n"+
				"keeping the first call's 5s delay. got err=%v", err)
		}
		// And the message really does still carry the delay the first call asked for.
		p, err := e.Peek(ctx, "q", PeekOptions{FromSeq: m.SeqNumber, Max: 1})
		if err != nil || len(p) != 1 {
			t.Fatalf("peek: %v", err)
		}
		if want := m.EnqueuedAtMs; p[0].VisibleAtMs > want+30_000 {
			t.Fatalf("visible_at moved to the second call's delay — the false success wrote through")
		}
	})

	t.Run("reject reason and description", func(t *testing.T) {
		for _, tc := range []struct{ name, reason, desc string }{
			{"changed reason", "OtherReason", "boom"},
			{"changed description", "PoisonMessage", "something else"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				m := lock()
				if err := e.Reject(ctx, "q", m.SeqNumber, m.LockToken, "PoisonMessage", "boom"); err != nil {
					t.Fatalf("first reject: %v", err)
				}
				if err := e.Reject(ctx, "q", m.SeqNumber, m.LockToken, "PoisonMessage", "boom"); err != nil {
					t.Fatalf("replaying the same Reject must be an idempotent success, got %v", err)
				}
				if err := e.Reject(ctx, "q", m.SeqNumber, m.LockToken, tc.reason, tc.desc); !errors.Is(err, ErrLockLost) {
					t.Fatalf("Reject with a CHANGED %s must be ErrLockLost, not a success that keeps the\n"+
						"original text in the DLQ. got err=%v", tc.name, err)
				}
				p, err := e.Peek(ctx, "q", PeekOptions{FromSeq: m.SeqNumber, Max: 1})
				if err != nil || len(p) != 1 {
					t.Fatalf("peek: %v", err)
				}
				if p[0].DeadLetterReason != "PoisonMessage" || p[0].DeadLetterDescription != "boom" {
					t.Fatalf("the DLQ text changed under a call that was supposed to fail: reason=%q desc=%q",
						p[0].DeadLetterReason, p[0].DeadLetterDescription)
				}
			})
		}
	})

	// Reject defaults an empty reason to ReasonAppRequested BEFORE the receipt is keyed, so the two
	// spellings of the same request stay the same request.
	t.Run("the default reason is not a different request", func(t *testing.T) {
		m := lock()
		if err := e.Reject(ctx, "q", m.SeqNumber, m.LockToken, "", ""); err != nil {
			t.Fatalf("first reject: %v", err)
		}
		if err := e.Reject(ctx, "q", m.SeqNumber, m.LockToken, ReasonAppRequested, ""); err != nil {
			t.Fatalf("Reject(\"\") and Reject(ReasonAppRequested) are the SAME request; the replay must\n"+
				"succeed. got %v", err)
		}
	})

	// Complete and Defer take no arguments that change what they do, so nothing about them may be
	// argument-sensitive — their receipts stay plain replays.
	t.Run("argument-free verbs still replay", func(t *testing.T) {
		m := lock()
		if err := e.Complete(ctx, "q", m.SeqNumber, m.LockToken); err != nil {
			t.Fatal(err)
		}
		if err := e.Complete(ctx, "q", m.SeqNumber, m.LockToken); err != nil {
			t.Fatalf("replaying Complete must stay an idempotent success, got %v", err)
		}
	})
}
