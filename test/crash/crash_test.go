//go:build crash_injection

// Package crash is the crash-injection layer: it starts a real process doing real work against a
// file-backed engine, KILLS it hard (SIGKILL / TerminateProcess — no cleanup, no flush, no defers),
// restarts, and checks that what survived is consistent.
//
// It lives under test/ and not as an engine _test.go for one unavoidable reason: a killed process
// cannot be an in-package test. The harness re-execs THIS test binary with a role in the
// environment, so the worker is the same code under test, launched fresh each cycle and shot in the
// head mid-transaction.
//
// What it can and cannot prove, stated honestly so no one reads more into a green run than is there:
//
//   - It proves APPLICATION-LEVEL recovery: a transaction torn by a kill is atomic (all or nothing),
//     orphaned locks are reset on restart, and nothing already committed is lost or duplicated. That
//     is the contract engine.Open and the transactional outbox actually promise.
//   - It does NOT prove power-loss durability. A hard kill does not lose data the OS has already
//     accepted; only a power cut or kernel panic can, and that needs fault-injecting the filesystem,
//     which is out of scope here. So this is "the process died", not "the machine died".
package crash

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mqlitehq/mqlite"
	"github.com/mqlitehq/mqlite/engine"
)

// The worker role and the DB path are handed to the re-exec'd child through the environment.
const (
	roleEnv = "MQLITE_CRASH_ROLE"
	dbEnv   = "MQLITE_CRASH_DB"
)

// TestCrashWorkerEntrypoint is the re-exec target. Run normally (no role in the env) it is a no-op
// skip; run by the harness it becomes a worker that hammers the DB until it is killed. It never
// returns on its own — the whole point is that the harness ends it abruptly.
func TestCrashWorkerEntrypoint(t *testing.T) {
	role := os.Getenv(roleEnv)
	if role == "" {
		t.Skip("worker entrypoint — driven by the crash harness, not run directly")
	}
	db := os.Getenv(dbEnv)
	ctx := context.Background()

	e := openWithRetry(ctx, db) // the previous worker's advisory lock releases as it dies
	defer e.Close()
	ensureQueue(ctx, e, "q")

	switch role {
	case "producer":
		runProducer(ctx, e)
	case "locker":
		runLocker(ctx, e)
	default:
		fmt.Fprintf(os.Stderr, "unknown crash role %q\n", role)
		os.Exit(2)
	}
}

// runProducer commits, as fast as it can, a transaction that does two writes at once: a business
// row in `orders` and the matching queue message. This is the transactional-outbox contract — the
// row and the message commit together or not at all — and killing it mid-commit is the sharpest way
// to test that "together or not at all" survives a crash. It loops forever; the harness kills it.
func runProducer(ctx context.Context, e *mqlite.Embedded) {
	if err := e.Tx(ctx, func(tx *engine.EngineTx) error {
		_, err := tx.SQL().ExecContext(tx.Context(),
			`CREATE TABLE IF NOT EXISTS orders (oid INTEGER PRIMARY KEY)`)
		return err
	}); err != nil {
		fail("create orders", err)
	}
	for {
		if err := e.Tx(ctx, func(tx *engine.EngineTx) error {
			var next int64
			if err := tx.SQL().QueryRowContext(tx.Context(),
				`SELECT COALESCE(MAX(oid),0)+1 FROM orders`).Scan(&next); err != nil {
				return err
			}
			if _, err := tx.SQL().ExecContext(tx.Context(),
				`INSERT INTO orders(oid) VALUES(?)`, next); err != nil {
				return err
			}
			// The message body IS the order id: the oracle later matches the two sets exactly.
			// Inside a Tx the enqueue takes engine.OutMessage (the engine's own type).
			_, err := tx.SendOne("q", engine.OutMessage{Body: []byte(strconv.FormatInt(next, 10))})
			return err
		}); err != nil {
			// A kill lands as a closed DB / cancelled ctx; anything else is a real defect.
			if isShutdown(err) {
				return
			}
			fail("producer tx", err)
		}
	}
}

// runLocker receives messages (taking short leases, with the reaper live) and mostly just holds
// them, so it is usually killed WHILE holding locks. It never completes anything: no message ever
// leaves the queue, which makes the recovery oracle exact — after a restart the queue must still
// hold every message, and none may be stuck `locked` (Open resets orphaned locks to active).
func runLocker(ctx context.Context, e *mqlite.Embedded) {
	for {
		msgs, err := e.Receive(ctx, "q", mqlite.RecvOpts{Max: 5, Wait: 100 * time.Millisecond})
		if err != nil {
			if isShutdown(err) {
				return
			}
			fail("locker receive", err)
		}
		for _, m := range msgs {
			// Occasionally give one back; otherwise hold it and let the kill catch us mid-lease.
			if m.SequenceNumber%3 == 0 {
				if err := m.Abandon(ctx); err != nil &&
					!errors.Is(err, mqlite.ErrLockLost) && !isShutdown(err) {
					fail("locker abandon", err)
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// ─── the crash oracle ─────────────────────────────────────────────────────────

// TestCrashOutboxAtomicity kills a producer over and over, mid-transaction, then asserts the outbox
// invariant across every crash: the set of business rows equals the set of queue messages. A torn
// commit must leave NEITHER (rolled back atomically) — never an order without its message, never a
// message without its order. That equality is the whole promise of the same-DB transactional
// enqueue, tested at the one moment it is hardest to keep.
func TestCrashOutboxAtomicity(t *testing.T) {
	db := dbPath(t)
	killLoop(t, db, "producer", 8)

	ctx := context.Background()
	e := openWithRetry(ctx, db)
	defer e.Close()

	orders := readOrders(t, e)
	msgs := readMessageBodies(t, e, "q")

	if len(orders) == 0 {
		t.Fatal("no orders survived — the producer never committed anything; the harness is not exercising the code")
	}
	// Every order has its message and vice versa. Report the asymmetry, not just the count.
	var ordersNoMsg, msgNoOrder []int64
	for oid := range orders {
		if !msgs[oid] {
			ordersNoMsg = append(ordersNoMsg, oid)
		}
	}
	for body := range msgs {
		if !orders[body] {
			msgNoOrder = append(msgNoOrder, body)
		}
	}
	if len(ordersNoMsg) > 0 || len(msgNoOrder) > 0 {
		t.Fatalf("OUTBOX ATOMICITY VIOLATED after crash recovery (%d orders, %d messages):\n"+
			"  orders whose message is missing: %v\n"+
			"  messages whose order is missing: %v\n"+
			"  a transaction torn by the kill committed one write without the other — the outbox is not atomic.",
			len(orders), len(msgs), trim(ordersNoMsg), trim(msgNoOrder))
	}
	t.Logf("outbox intact across 8 crashes: %d orders, each with exactly its message", len(orders))
}

// TestCrashRecoveryResetsOrphanedLocks seeds a fixed set of messages, then kills a consumer that is
// holding locks, repeatedly. After each restart Open must reset every orphaned `locked` row back to
// `active` (single-broker crash recovery), and — because the consumer never completes anything —
// not one of the seeded messages may be lost.
func TestCrashRecoveryResetsOrphanedLocks(t *testing.T) {
	const seeded = 200
	db := dbPath(t)

	ctx := context.Background()
	// Seed once, cleanly, before any killing.
	func() {
		e := openWithRetry(ctx, db)
		defer e.Close()
		ensureQueue(ctx, e, "q")
		bodies := make([]mqlite.OutMessage, seeded)
		for i := range bodies {
			bodies[i] = mqlite.OutMessage{Body: []byte(strconv.Itoa(i))}
		}
		if _, err := e.Send(ctx, "q", bodies...); err != nil {
			t.Fatal(err)
		}
	}()

	killLoop(t, db, "locker", 6)

	// Reopen and check the recovery contract.
	e := openWithRetry(ctx, db)
	defer e.Close()

	// Open just ran crash recovery. No lock may have survived it.
	m, err := e.Stats(ctx, "q")
	if err != nil {
		t.Fatal(err)
	}
	if m.Locked != 0 {
		t.Fatalf("%d message(s) are still `locked` after restart — crash recovery did not reset the "+
			"orphaned locks, so they are stranded until their lease expires (or forever, if the reaper "+
			"is off).", m.Locked)
	}
	// Nothing completed, so nothing may be gone: every seeded message is still here.
	if int(m.Total) != seeded {
		t.Fatalf("MESSAGE LOSS across crash recovery: %d of %d messages remain "+
			"(active=%d locked=%d deferred=%d dead=%d). Nothing was ever completed; none should be gone.",
			m.Total, seeded, m.Active, m.Locked, m.Deferred, m.DeadLettered)
	}
	// And they are usable: a clean receive of the whole set still works after everything.
	got, err := e.Receive(ctx, "q", mqlite.RecvOpts{Max: seeded})
	if err != nil {
		t.Fatalf("the queue is unusable after crash recovery: %v", err)
	}
	if len(got) != seeded {
		t.Fatalf("expected to receive all %d recovered messages, got %d", seeded, len(got))
	}
}

// ─── harness plumbing ─────────────────────────────────────────────────────────

// killLoop runs `cycles` rounds of: launch a worker (a re-exec of this test binary), let it run a
// short, randomised while so the kill lands mid-flight, then KILL it hard and reap it. Process.Kill
// is a hard kill on every platform (SIGKILL on unix, TerminateProcess on Windows) — no signal
// handler, no deferred Close, exactly a crash.
func killLoop(t *testing.T, db, role string, cycles int) {
	t.Helper()
	for i := 0; i < cycles; i++ {
		cmd := exec.Command(os.Args[0], "-test.run=^TestCrashWorkerEntrypoint$", "-test.v=false")
		cmd.Env = append(os.Environ(), roleEnv+"="+role, dbEnv+"="+db)
		var out strings.Builder
		cmd.Stdout, cmd.Stderr = &out, &out
		if err := cmd.Start(); err != nil {
			t.Fatalf("cycle %d: start worker: %v", i, err)
		}
		// Vary the uptime so kills land at different points in the transaction stream. No
		// Math.rand needed: derive it from the cycle index, which is enough spread here.
		time.Sleep(time.Duration(60+40*i) * time.Millisecond)
		_ = cmd.Process.Kill()
		err := cmd.Wait() // reap it, so its advisory file lock is released before the next open
		if err == nil {
			// A worker that exits 0 on its own never got killed — it should loop until we kill it.
			t.Fatalf("cycle %d: worker exited cleanly; it was meant to be killed mid-work.\n%s", i, out.String())
		}
		if body := out.String(); strings.Contains(body, "CRASH-WORKER-FAIL") {
			t.Fatalf("cycle %d: the worker reported a defect before we killed it:\n%s", i, body)
		}
	}
}

// openWithRetry opens the embedded engine, retrying briefly on ErrDBLocked: a just-killed worker's
// OS advisory lock can take a moment to be released after the process is reaped.
func openWithRetry(ctx context.Context, db string) *mqlite.Embedded {
	var last error
	for i := 0; i < 50; i++ {
		e, err := mqlite.OpenEmbedded(ctx, "file:"+db)
		if err == nil {
			return e
		}
		last = err
		if !errors.Is(err, mqlite.ErrDBLocked) {
			fail("open", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	fail("open (still locked after retries)", last)
	return nil
}

func ensureQueue(ctx context.Context, e *mqlite.Embedded, name string) {
	qs, err := e.ListQueues(ctx)
	if err != nil {
		fail("list queues", err)
	}
	for _, q := range qs {
		if q.Name == name {
			return
		}
	}
	// Short lease so, in the locker role, expiry races the handler and the reaper is doing real work.
	if err := e.CreateQueue(ctx, name, mqlite.QueueConfig{LockDuration: 500 * time.Millisecond, MaxDeliveryCount: 1000}); err != nil {
		fail("create queue", err)
	}
}

func readOrders(t *testing.T, e *mqlite.Embedded) map[int64]bool {
	t.Helper()
	ids := map[int64]bool{}
	err := e.Tx(context.Background(), func(tx *engine.EngineTx) error {
		rows, err := tx.SQL().QueryContext(tx.Context(), `SELECT oid FROM orders`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var oid int64
			if err := rows.Scan(&oid); err != nil {
				return err
			}
			ids[oid] = true
		}
		return rows.Err()
	})
	if err != nil {
		t.Fatalf("read orders: %v", err)
	}
	return ids
}

func readMessageBodies(t *testing.T, e *mqlite.Embedded, queue string) map[int64]bool {
	t.Helper()
	bodies := map[int64]bool{}
	var from int64
	for {
		page, err := e.Peek(context.Background(), queue, mqlite.PeekOpts{From: from, Max: 1000})
		if err != nil {
			t.Fatalf("peek: %v", err)
		}
		if len(page) == 0 {
			break
		}
		for _, p := range page {
			n, err := strconv.ParseInt(string(p.Body), 10, 64)
			if err != nil {
				t.Fatalf("message body %q is not an order id: %v", p.Body, err)
			}
			bodies[n] = true
			if p.SequenceNumber >= from {
				from = p.SequenceNumber + 1
			}
		}
	}
	return bodies
}

func dbPath(t *testing.T) string {
	t.Helper()
	return t.TempDir() + "/crash.db"
}

func isShutdown(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "closed") || strings.Contains(s, "closing")
}

// fail is how a worker reports a defect: a marker the harness greps for, then a nonzero exit. It
// cannot use *testing.T — it runs in the re-exec'd child, whose test failures the parent never sees.
func fail(what string, err error) {
	fmt.Fprintf(os.Stderr, "CRASH-WORKER-FAIL: %s: %v\n", what, err)
	os.Stdout.Sync()
	os.Stderr.Sync()
	os.Exit(3)
}

func trim(xs []int64) []int64 {
	if len(xs) > 12 {
		return xs[:12]
	}
	return xs
}
