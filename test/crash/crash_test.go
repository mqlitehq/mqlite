//go:build crash_injection

// Package crash is the crash-injection layer: it starts a real process doing real work against a
// file-backed engine, KILLS it hard (SIGKILL / TerminateProcess — no cleanup, no flush, no defers),
// restarts, and checks that what survived is consistent.
//
// It lives under test/ and not as an engine _test.go for one unavoidable reason: a killed process
// cannot be an in-package test. The harness re-execs THIS test binary with a role in the
// environment, so the worker is the same code under test, launched fresh each cycle and shot in the
// head — for the producer, precisely while a transaction is open.
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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mqlitehq/mqlite"
	"github.com/mqlitehq/mqlite/engine"
)

// Markers a worker prints on stdout. Each is a whole line so a bufio.Scanner sees it intact.
const (
	// readyMarker: the locker holds at least one lock — killing it now exercises recovery.
	readyMarker = "CRASH-WORKER-READY"
	// inTxMarker: the producer has written the order half of a transaction and is holding it OPEN,
	// message not yet written. Killing on THIS is what guarantees a kill lands inside a transaction —
	// the one moment the outbox's atomicity is actually on the line.
	inTxMarker = "CRASH-WORKER-INTX"
	// ackPrefix begins the line for every committed order nonce. The harness collects these and
	// requires every acknowledged nonce to survive recovery (conformance 15.3).
	ackPrefix = "CRASH-WORKER-ACK"
)

// parseAck reads the nonce out of a "CRASH-WORKER-ACK <nonce>" line.
func parseAck(line string) (string, bool) {
	return strings.CutPrefix(line, ackPrefix+" ")
}

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

// runProducer tests the transactional outbox across a crash. It first commits a run of ordinary
// transactions — each writing a business row AND its message together — so there is durable,
// acknowledged state for the no-loss oracle. Then it opens ONE more transaction, writes only the
// order half, and HOLDS it open while announcing inTxMarker, so the harness can kill it at the exact
// instant the atomicity guarantee is on the line: order written, message not, process dying.
//
// Order identity is a per-PROCESS nonce (this process's start time + a counter), not a value derived
// from the database like MAX(oid)+1. That matters for the no-loss check: if a crash lost a whole
// acknowledged transaction, a db-derived id would be recomputed and reused by the next worker,
// silently taking the lost one's place. A process-unique nonce can never be regenerated, so the loss
// stays visible (codex).
func runProducer(ctx context.Context, e *mqlite.Embedded) {
	if err := e.Tx(ctx, func(tx *engine.EngineTx) error {
		_, err := tx.SQL().ExecContext(tx.Context(),
			`CREATE TABLE IF NOT EXISTS orders (nonce TEXT PRIMARY KEY)`)
		return err
	}); err != nil {
		fail("create orders", err)
	}

	prefix := strconv.FormatInt(time.Now().UnixNano(), 10) // unique to THIS process
	counter := 0
	nextNonce := func() string { counter++; return prefix + "-" + strconv.Itoa(counter) }

	// A run of ordinary committed transactions: durable state, and acks the oracle will require to
	// survive the crash.
	for n := 0; n < 40; n++ {
		nonce := nextNonce()
		if err := e.Tx(ctx, func(tx *engine.EngineTx) error {
			if _, err := tx.SQL().ExecContext(tx.Context(),
				`INSERT INTO orders(nonce) VALUES(?)`, nonce); err != nil {
				return err
			}
			// The message body IS the order nonce, so the oracle can match the two sets exactly.
			_, err := tx.SendOne("q", engine.OutMessage{Body: []byte(nonce)})
			return err
		}); err != nil {
			// No benign error: SIGKILL cannot surface as a Go error, and nothing else cancels this
			// ctx or closes this engine, so any error is a real defect (codex).
			fail("producer tx", err)
		}
		fmt.Println(ackPrefix, nonce) // this nonce is durably committed
	}

	// Now the coordinated kill point: open a transaction, write the order, and hold it OPEN. The
	// message write and commit never run — the harness kills us here. If the outbox is one
	// transaction, recovery rolls back BOTH halves; a dual-write would leave the order behind.
	held := nextNonce()
	_ = e.Tx(ctx, func(tx *engine.EngineTx) error {
		if _, err := tx.SQL().ExecContext(tx.Context(),
			`INSERT INTO orders(nonce) VALUES(?)`, held); err != nil {
			return err
		}
		fmt.Println(inTxMarker) // order written, message NOT yet — kill me now
		_ = os.Stdout.Sync()
		time.Sleep(60 * time.Second) // hold the transaction open until the harness kills the process
		_, err := tx.SendOne("q", engine.OutMessage{Body: []byte(held)})
		return err
	})
	fail("producer was not killed while holding a transaction open", errors.New("still alive"))
}

// runLocker receives messages (short leases, reaper OFF) and mostly just holds them, so it is killed
// WHILE holding locks. It never completes anything: no message ever leaves the queue, which makes
// the recovery oracle exact — after a restart the queue must still hold every message, and none may
// be stuck `locked` (Open resets orphaned locks).
func runLocker(ctx context.Context, e *mqlite.Embedded) {
	holding := 0
	for {
		msgs, err := e.Receive(ctx, "q", mqlite.RecvOpts{Max: 5, Wait: 100 * time.Millisecond})
		if err != nil {
			fail("locker receive", err) // no benign error — see runProducer
		}
		for _, m := range msgs {
			// Occasionally give one back; otherwise hold it and let the kill catch us mid-lease.
			if m.SequenceNumber%3 == 0 {
				if err := m.Abandon(ctx); err != nil && !errors.Is(err, mqlite.ErrLockLost) {
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

// ready announces, exactly once, that the locker now holds a lock. The harness blocks on it before
// killing, so a slow start can never make the recovery assertions pass without any lock held.
var readyOnce sync.Once

func ready() {
	readyOnce.Do(func() {
		fmt.Println(readyMarker)
		_ = os.Stdout.Sync()
	})
}

// ─── the crash oracle ─────────────────────────────────────────────────────────

// TestCrashOutboxAtomicity kills a producer eight times, each time WHILE it holds a transaction open
// between its two writes, then asserts the outbox invariant across every crash: business rows and
// queue messages are in exact bijection. The held transaction must roll back whole — never an order
// without its message, never a message without its order — and every previously-acknowledged commit
// must still be there.
func TestCrashOutboxAtomicity(t *testing.T) {
	db := dbPath(t)
	acked := killLoop(t, db, "producer", inTxMarker, 8) // nonces the producer said it committed

	ctx := context.Background()
	e := openWithRetry(ctx, db)
	defer e.Close()

	orders := readOrderNonces(t, e)
	msgs := readMessageBodies(t, e, "q") // body -> how many messages carry it

	if len(orders) == 0 {
		t.Fatal("no orders survived — the producer never committed anything; the harness is not exercising the code")
	}
	// The invariant is a bijection, and multiplicity is load-bearing: two messages for one order is
	// itself a tear. So the message side is a count, and every order must have exactly one.
	var ordersNoMsg, ordersDupMsg, msgNoOrder []string
	for nonce := range orders {
		switch msgs[nonce] {
		case 1:
		case 0:
			ordersNoMsg = append(ordersNoMsg, nonce)
		default:
			ordersDupMsg = append(ordersDupMsg, nonce)
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
			"  orders with MORE THAN ONE message:  %v\n"+
			"  messages whose order is missing:    %v\n"+
			"  a transaction torn by the kill committed one write without the other — the outbox is not atomic.",
			len(orders), total, trim(ordersNoMsg), trim(ordersDupMsg), trim(msgNoOrder))
	}

	// No loss of an ACKNOWLEDGED commit (conformance 15.3). The bijection only proves orders and
	// messages agree with each other; if a whole transaction vanished after e.Tx returned success,
	// both halves disappear together and the sets stay equal. The producer's acks — nonces that can
	// never be regenerated — are the external witness: every one must still be here.
	var lostAcked []string
	for nonce := range acked {
		if !orders[nonce] {
			lostAcked = append(lostAcked, nonce)
		}
	}
	if len(lostAcked) > 0 {
		t.Fatalf("MESSAGE LOSS across crash recovery: %d of %d acknowledged commits vanished after a "+
			"restart (first few: %v). A transaction that returned success must survive the process dying.",
			len(lostAcked), len(acked), trim(lostAcked))
	}
	if len(acked) == 0 {
		t.Fatal("the producer acknowledged no commits — the harness is not exercising the code")
	}
	t.Logf("outbox intact across 8 in-transaction crashes: %d orders in bijection with their messages; all %d acked commits survived",
		len(orders), len(acked))
}

// TestCrashRecoveryResetsOrphanedLocks seeds a fixed set of messages, then kills a consumer that is
// holding locks, repeatedly. After each restart Open must reset every orphaned `locked` row (to
// `active`, or to `dead_lettered` if it was on its last permitted delivery — the reaper's rule), and
// — because the consumer never completes anything — not one of the seeded messages may be lost.
func TestCrashRecoveryResetsOrphanedLocks(t *testing.T) {
	const seeded = 200
	db := dbPath(t)

	ctx := context.Background()
	// Seed once, cleanly, before any killing. MaxDeliveryCount is high, so an orphaned lock always
	// recovers to `active` here (never dead-lettered) — which keeps the no-loss check exact.
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

	_ = killLoop(t, db, "locker", readyMarker, 6) // the locker emits no acks

	// Reopen and check the recovery contract.
	e := openWithRetry(ctx, db)
	defer e.Close()

	m, err := e.Stats(ctx, "q")
	if err != nil {
		t.Fatal(err)
	}
	// Open just ran crash recovery. No lock may have survived it.
	if m.Locked != 0 {
		t.Fatalf("%d message(s) are still `locked` after restart — crash recovery did not reset the "+
			"orphaned locks, so they are stranded until their lease expires (or forever, the reaper is off).", m.Locked)
	}
	// Nothing completed, so nothing may be gone. A bare count can be fooled — losing body N while
	// duplicating body M keeps the total at 200 — so check the IDENTITIES and multiplicities. The
	// seed bodies are exactly 0..199 and none were dead-lettered (high MaxDeliveryCount), so every
	// one must come back active exactly once (codex).
	got, err := e.Receive(ctx, "q", mqlite.RecvOpts{Max: seeded})
	if err != nil {
		t.Fatalf("the queue is unusable after crash recovery: %v", err)
	}
	seen := map[int64]int{}
	for _, msg := range got {
		n, perr := strconv.ParseInt(string(msg.Body), 10, 64)
		if perr != nil {
			t.Fatalf("recovered a message whose body %q is not a seed id: %v", msg.Body, perr)
		}
		seen[n]++
	}
	var missing, duped []int64
	for i := int64(0); i < seeded; i++ {
		switch seen[i] {
		case 1:
		case 0:
			missing = append(missing, i)
		default:
			duped = append(duped, i)
		}
	}
	if len(missing) > 0 || len(duped) > 0 || int(m.Total) != seeded || len(got) != seeded {
		t.Fatalf("crash recovery did not preserve the seeded set exactly (received %d, Stats.Total %d, "+
			"of %d seeded):\n"+
			"  missing bodies:    %v\n"+
			"  duplicated bodies: %v\n"+
			"  nothing was ever completed, so every seed id 0..%d must come back exactly once.",
			len(got), m.Total, seeded, trim(missing), trim(duped), seeded-1)
	}
}

// ─── harness plumbing ─────────────────────────────────────────────────────────

// killLoop runs `cycles` rounds of: launch a worker (a re-exec of this test binary), WAIT for it to
// print killOn — the exact moment we want to crash it at (a producer holding a transaction open, a
// locker holding a lock) — then KILL it hard and reap it. Killing on a marker rather than a timer is
// what makes the crash land where the invariant is actually on the line (codex).
//
// The classification afterwards gives the suite its teeth: only the SIGKILL we injected counts. A
// worker that panics, trips the race detector, exits on its own, or dies from some other signal is a
// hard failure, never a crash cycle counted as success.
func killLoop(t *testing.T, db, role, killOn string, cycles int) map[string]bool {
	t.Helper()
	acked := map[string]bool{}
	for i := 0; i < cycles; i++ {
		cmd := exec.Command(os.Args[0], "-test.run=^TestCrashWorkerEntrypoint$", "-test.v=false")
		cmd.Env = append(os.Environ(), roleEnv+"="+role, dbEnv+"="+db,
			// Under -race a clean test exit sleeps 1s (atexit_sleep_ms) before the process actually
			// dies; a kill landing in that window would look like our SIGKILL and mask an unexpected
			// clean return. Zero it so a clean exit is immediate and gets caught by Success() below.
			"GORACE=atexit_sleep_ms=0")

		// One pipe carries both streams; a goroutine drains it into `out`, records acks, and flags
		// the kill signal. The parent closes its write end after Start so the reader EOFs once the
		// child is gone.
		pr, pw, err := os.Pipe()
		if err != nil {
			t.Fatalf("cycle %d: pipe: %v", i, err)
		}
		cmd.Stdout, cmd.Stderr = pw, pw
		if err := cmd.Start(); err != nil {
			t.Fatalf("cycle %d: start worker: %v", i, err)
		}
		_ = pw.Close()

		signal := make(chan struct{})
		drained := make(chan struct{})
		var mu sync.Mutex
		var out strings.Builder
		go func() {
			sc := bufio.NewScanner(pr)
			signalled := false
			for sc.Scan() {
				line := sc.Text()
				mu.Lock()
				out.WriteString(line)
				out.WriteByte('\n')
				if nonce, ok := parseAck(line); ok {
					acked[nonce] = true // this nonce was durably committed before the crash
				}
				mu.Unlock()
				if !signalled && strings.Contains(line, killOn) {
					signalled = true
					close(signal)
				}
			}
			close(drained)
		}()

		select {
		case <-signal:
		case <-time.After(20 * time.Second):
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			<-drained
			t.Fatalf("cycle %d: worker never reached its kill point %q (role %q). The crash would land\n"+
				"before the invariant was on the line, so recovery would be tested vacuously.\n%s",
				i, killOn, role, snapshot(&mu, &out))
		}

		_ = cmd.Process.Kill()
		_ = cmd.Wait() // reap it, so its advisory file lock is released before the next open
		<-drained      // all of the worker's output is now in `out`

		body := snapshot(&mu, &out)
		switch {
		case strings.Contains(body, "CRASH-WORKER-FAIL"):
			t.Fatalf("cycle %d: the worker reported a defect before we killed it:\n%s", i, body)
		case strings.Contains(body, "panic:") || strings.Contains(body, "DATA RACE") || strings.Contains(body, "race detected"):
			t.Fatalf("cycle %d: the worker panicked or tripped the race detector:\n%s", i, body)
		case cmd.ProcessState != nil && cmd.ProcessState.Success():
			t.Fatalf("cycle %d: worker exited 0 on its own; it must run until the harness kills it.\n%s", i, body)
		case !killedByUs(cmd.ProcessState):
			// Any signal makes Exited()==false, so "it was signalled" is NOT enough — it must be OUR
			// SIGKILL. A real SIGSEGV/SIGABRT or a nonzero self-exit falls here.
			t.Fatalf("cycle %d: worker did not die from the injected kill (%v) — it crashed on its own:\n%s",
				i, cmd.ProcessState, body)
		}
	}
	return acked
}

// openWithRetry opens the embedded engine, retrying briefly on ErrDBLocked: a just-killed worker's
// OS advisory lock can take a moment to be released after the process is reaped.
func openWithRetry(ctx context.Context, db string) *mqlite.Embedded {
	var last error
	for i := 0; i < 50; i++ {
		// WithoutBackground: the reaper (1s tick) must NOT run. With short leases it could clear an
		// orphaned lock before the kill or before the oracle reads it, letting the recovery test pass
		// even if Open's crash recovery is broken — so only Open may reset a lock here (codex).
		e, err := mqlite.OpenEmbedded(ctx, "file:"+db, mqlite.WithoutBackground())
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
	if err := e.CreateQueue(ctx, name, mqlite.QueueConfig{LockDuration: 500 * time.Millisecond, MaxDeliveryCount: 1000}); err != nil {
		fail("create queue", err)
	}
}

func readOrderNonces(t *testing.T, e *mqlite.Embedded) map[string]bool {
	t.Helper()
	ids := map[string]bool{}
	err := e.Tx(context.Background(), func(tx *engine.EngineTx) error {
		rows, err := tx.SQL().QueryContext(tx.Context(), `SELECT nonce FROM orders`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var nonce string
			if err := rows.Scan(&nonce); err != nil {
				return err
			}
			ids[nonce] = true
		}
		return rows.Err()
	})
	if err != nil {
		t.Fatalf("read orders: %v", err)
	}
	return ids
}

// readMessageBodies returns body -> count. The count matters: two messages with the same body is
// itself an atomicity violation the caller must be able to see.
func readMessageBodies(t *testing.T, e *mqlite.Embedded, queue string) map[string]int {
	t.Helper()
	bodies := map[string]int{}
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
			bodies[string(p.Body)]++
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

// fail is how a worker reports a defect: a marker the harness greps for, then a nonzero exit. It
// cannot use *testing.T — it runs in the re-exec'd child, whose test failures the parent never sees.
func fail(what string, err error) {
	fmt.Fprintf(os.Stderr, "CRASH-WORKER-FAIL: %s: %v\n", what, err)
	_ = os.Stdout.Sync()
	_ = os.Stderr.Sync()
	os.Exit(3)
}

func trim[T any](xs []T) []T {
	if len(xs) > 12 {
		return xs[:12]
	}
	return xs
}
