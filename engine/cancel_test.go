package engine

// Cancellation acceptance suite (round-5 §2.4). Two properties must hold TOGETHER, and every
// earlier attempt at this bug sacrificed one to get the other:
//
//   1. a cancelled caller stops waiting and creates NO post-timeout mutation;
//   2. cancelling an in-flight operation does not erase or wedge the database.
//
// Both are asserted on a file store AND on `:memory:`, because they failed differently on each.

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

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
