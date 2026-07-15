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
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mqlitehq/mqlite"
	"github.com/mqlitehq/mqlite/engine"
)

// readyMarker is printed by a worker once it is doing the thing the kill is meant to interrupt — the
// producer after its first commit, the locker once it holds a lock. The harness waits for it before
// killing, so a slow CI runner can never let the kill land before any real work started (which would
// pass the recovery assertions vacuously). It is a line on its own so a bufio.Scanner sees it whole.
const readyMarker = "CRASH-WORKER-READY"

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
	committed := false
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
		if !committed { // one real commit is on disk — safe to be killed now
			committed = true
			ready()
		}
	}
}

// runLocker receives messages (taking short leases, with the reaper live) and mostly just holds
// them, so it is usually killed WHILE holding locks. It never completes anything: no message ever
// leaves the queue, which makes the recovery oracle exact — after a restart the queue must still
// hold every message, and none may be stuck `locked` (Open resets orphaned locks to active).
func runLocker(ctx context.Context, e *mqlite.Embedded) {
	holding := 0
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
			} else {
				holding++
			}
		}
		if holding > 0 { // at least one lock is held — killing us now actually exercises recovery
			ready()
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// ready announces, exactly once, that the worker is now doing the thing the kill is meant to
// interrupt. The harness blocks on this line before it kills, so a slow start can never make the
// recovery assertions pass without any work having happened.
var readyOnce sync.Once

func ready() {
	readyOnce.Do(func() {
		fmt.Println(readyMarker)
		_ = os.Stdout.Sync()
	})
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
	msgs := readMessageBodies(t, e, "q") // body -> how many messages carry it

	if len(orders) == 0 {
		t.Fatal("no orders survived — the producer never committed anything; the harness is not exercising the code")
	}
	// The invariant is a BIJECTION, and multiplicity is load-bearing: because a rolled-back order
	// frees its id, a message-only torn commit followed by id reuse produces order N with TWO
	// messages N. Counting distinct bodies would collapse those two and hide the very tear this test
	// exists to catch (codex), so the message side is a count, and every order must have exactly one.
	var ordersNoMsg, ordersDupMsg, msgNoOrder []int64
	for oid := range orders {
		switch msgs[oid] {
		case 1: // the healthy case
		case 0:
			ordersNoMsg = append(ordersNoMsg, oid)
		default:
			ordersDupMsg = append(ordersDupMsg, oid) // a second message for a reused id
		}
	}
	total := 0
	for body, n := range msgs {
		total += n
		if !orders[body] {
			msgNoOrder = append(msgNoOrder, body)
		}
	}
	if len(ordersNoMsg) > 0 || len(ordersDupMsg) > 0 || len(msgNoOrder) > 0 {
		t.Fatalf("OUTBOX ATOMICITY VIOLATED after crash recovery (%d orders, %d messages):\n"+
			"  orders whose message is missing:    %v\n"+
			"  orders with MORE THAN ONE message:  %v  (an id was reused after a message-only torn commit)\n"+
			"  messages whose order is missing:    %v\n"+
			"  a transaction torn by the kill committed one write without the other — the outbox is not atomic.",
			len(orders), total, trim(ordersNoMsg), trim(ordersDupMsg), trim(msgNoOrder))
	}
	t.Logf("outbox intact across 8 crashes: %d orders, each with exactly one message", len(orders))
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

// killLoop runs `cycles` rounds of: launch a worker (a re-exec of this test binary), WAIT until it
// signals it is really doing work, let it run a little longer so the kill lands at a varying point,
// then KILL it hard and reap it. Process.Kill is a hard kill on every platform (SIGKILL on unix,
// TerminateProcess on Windows) — no signal handler, no deferred Close, exactly a crash.
//
// The classification at the end is the part that gives the whole suite teeth: a worker that PANICS,
// trips the race detector, or exits nonzero on its own must be a hard failure, not a crash cycle
// counted as success. Ignoring the kill/Wait error (they look identical) would let exactly those go
// unnoticed (codex).
func killLoop(t *testing.T, db, role string, cycles int) {
	t.Helper()
	for i := 0; i < cycles; i++ {
		cmd := exec.Command(os.Args[0], "-test.run=^TestCrashWorkerEntrypoint$", "-test.v=false")
		cmd.Env = append(os.Environ(), roleEnv+"="+role, dbEnv+"="+db)

		// One pipe carries both streams; a goroutine drains it into `out` and flags readiness. The
		// parent closes its write end after Start so the reader sees EOF once the child is gone.
		pr, pw, err := os.Pipe()
		if err != nil {
			t.Fatalf("cycle %d: pipe: %v", i, err)
		}
		cmd.Stdout, cmd.Stderr = pw, pw
		if err := cmd.Start(); err != nil {
			t.Fatalf("cycle %d: start worker: %v", i, err)
		}
		_ = pw.Close()

		ready := make(chan struct{})
		drained := make(chan struct{})
		var mu sync.Mutex
		var out strings.Builder
		go func() {
			sc := bufio.NewScanner(pr)
			readyClosed := false
			for sc.Scan() {
				line := sc.Text()
				mu.Lock()
				out.WriteString(line)
				out.WriteByte('\n')
				mu.Unlock()
				if !readyClosed && strings.Contains(line, readyMarker) {
					readyClosed = true
					close(ready)
				}
			}
			close(drained)
		}()

		select {
		case <-ready:
		case <-time.After(20 * time.Second):
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			<-drained
			t.Fatalf("cycle %d: worker never signalled it was doing work (role %q). The kill would land\n"+
				"before any real work started, so recovery would be tested vacuously.\n%s", i, role, snapshot(&mu, &out))
		}

		// It is genuinely working now. Let it run a little longer, varying by cycle so kills land at
		// different points in the transaction stream, then crash it.
		time.Sleep(time.Duration(30+30*i) * time.Millisecond)
		_ = cmd.Process.Kill()
		waitErr := cmd.Wait() // reap it, so its advisory file lock is released before the next open
		<-drained             // all of the worker's output is now in `out`

		body := snapshot(&mu, &out)
		switch {
		case strings.Contains(body, "CRASH-WORKER-FAIL"):
			t.Fatalf("cycle %d: the worker reported a defect before we killed it:\n%s", i, body)
		case strings.Contains(body, "panic:") || strings.Contains(body, "DATA RACE") || strings.Contains(body, "race detected"):
			t.Fatalf("cycle %d: the worker panicked or tripped the race detector:\n%s", i, body)
		case waitErr == nil:
			t.Fatalf("cycle %d: worker exited 0 on its own; it must loop until the harness kills it.\n%s", i, body)
		case exitedOnItsOwn(cmd.ProcessState):
			// A killed process is signalled (unix) or has the terminate exit code (windows). Any
			// other nonzero exit means the worker died on its own without a marker — surface it.
			t.Fatalf("cycle %d: worker exited on its own (%v) instead of being killed:\n%s", i, cmd.ProcessState, body)
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

// readMessageBodies returns body-id -> count. The count matters: two messages with the same body is
// itself an atomicity violation the caller must be able to see (see TestCrashOutboxAtomicity).
func readMessageBodies(t *testing.T, e *mqlite.Embedded, queue string) map[int64]int {
	t.Helper()
	bodies := map[int64]int{}
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
			bodies[n]++
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

func snapshot(mu *sync.Mutex, out *strings.Builder) string {
	mu.Lock()
	defer mu.Unlock()
	return out.String()
}

// exitedOnItsOwn reports whether the process terminated other than by our kill. A killed process is
// signalled on unix (Exited()==false); on Windows TerminateProcess yields exit code 1. Anything else
// that "Exited" is a self-inflicted death (a panic is 2; the worker's own fail() is 3).
func exitedOnItsOwn(ps *os.ProcessState) bool {
	if ps == nil || !ps.Exited() {
		return false // signalled → we killed it
	}
	if runtime.GOOS == "windows" && ps.ExitCode() == 1 {
		return false // TerminateProcess
	}
	return true
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
