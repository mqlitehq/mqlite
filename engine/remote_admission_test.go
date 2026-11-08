package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func waitRemoteQueue(t *testing.T, w *remoteWriter, normal, urgent int) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		w.mu.Lock()
		n, u := w.normal.Len(), w.urgent.Len()
		w.mu.Unlock()
		if n == normal && u == urgent {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("queue has normal=%d urgent=%d, want %d/%d", n, u, normal, urgent)
		default:
			runtime.Gosched()
		}
	}
}

func TestRemoteWriterFIFOAndBoundedLeasePriority(t *testing.T) {
	w := &remoteWriter{}
	first, err := w.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.close)
	order := make(chan string, 12)
	releases := make(chan func(), 12)
	queue := func(id string, urgent bool, n, u int) {
		ctx := context.Background()
		if urgent {
			ctx = leaseWriteContext(ctx)
		}
		go func() {
			release, err := w.acquire(ctx)
			if err != nil {
				return
			}
			order <- id
			releases <- release
		}()
		waitRemoteQueue(t, w, n, u)
	}
	queue("n0", false, 1, 0)
	queue("n1", false, 2, 0)
	for i := 0; i < 6; i++ {
		queue(fmt.Sprintf("u%d", i), true, 2, i+1)
	}
	first()
	for _, want := range []string{"u0", "u1", "u2", "u3", "n0", "u4", "u5", "n1"} {
		select {
		case got := <-order:
			if got != want {
				t.Fatalf("grant %s, want %s", got, want)
			}
			(<-releases)()
		case <-time.After(3 * time.Second):
			t.Fatalf("grant %s did not arrive", want)
		}
	}
}

func TestRemoteWriterCanceledGrantDoesNotLeakPermit(t *testing.T) {
	for i := 0; i < 100; i++ {
		w := &remoteWriter{}
		release, _ := w.acquire(context.Background())
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			p, err := w.acquire(ctx)
			if p != nil {
				p()
			}
			done <- err
		}()
		waitRemoteQueue(t, w, 1, 0)
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-start; cancel() }()
		go func() { defer wg.Done(); <-start; release() }()
		close(start)
		wg.Wait()
		<-done
		nextCtx, stop := context.WithTimeout(context.Background(), time.Second)
		next, err := w.acquire(nextCtx)
		stop()
		if err != nil {
			t.Fatalf("iteration %d leaked a canceled grant: %v", i, err)
		}
		next()
		w.close()
	}
}

func TestRemoteWriterCloseWakesBothQueuesAndRejectsLaterWork(t *testing.T) {
	w := &remoteWriter{}
	release, _ := w.acquire(context.Background())
	defer release()
	done := make(chan error, 2)
	for _, ctx := range []context.Context{context.Background(), leaseWriteContext(context.Background())} {
		go func(ctx context.Context) { _, err := w.acquire(ctx); done <- err }(ctx)
	}
	waitRemoteQueue(t, w, 1, 1)
	w.close()
	w.close()
	for i := 0; i < 2; i++ {
		if err := <-done; !errors.Is(err, ErrClosed) {
			t.Fatalf("pending work after Close: %v", err)
		}
	}
	if _, err := w.acquire(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("new work after Close: %v", err)
	}
}

func TestRemoteWriterCancellationGrantAndCloseRace(t *testing.T) {
	for i := 0; i < 100; i++ {
		w := &remoteWriter{}
		release, _ := w.acquire(context.Background())
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			p, err := w.acquire(ctx)
			if p != nil {
				p()
			}
			done <- err
		}()
		waitRemoteQueue(t, w, 1, 0)
		var wg sync.WaitGroup
		start := make(chan struct{})
		for _, action := range []func(){release, cancel, w.close} {
			wg.Add(1)
			go func(action func()) { defer wg.Done(); <-start; action() }(action)
		}
		close(start)
		wg.Wait()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, ErrClosed) {
			t.Fatalf("iteration %d: unexpected race result %v", i, err)
		}
		w.mu.Lock()
		leaked := w.held || w.normal.Len() != 0 || w.urgent.Len() != 0
		w.mu.Unlock()
		if leaked {
			t.Fatalf("iteration %d: canceled/closed grant leaked ownership", i)
		}
	}
}

type admissionFixture struct {
	active, maximum   atomic.Int64
	execs, begins     atomic.Int64
	commits           atomic.Int64
	commitErr         error
	firstWrite        chan struct{}
	allowFirst        chan struct{}
	firstOnce         sync.Once
	allowFirstOnce    sync.Once
	commitStarted     chan struct{}
	allowCommit       chan struct{}
	commitOnce        sync.Once
	allowCommitOnce   sync.Once
	rollbackStarted   chan struct{}
	allowRollback     chan struct{}
	rollbackOnce      sync.Once
	allowRollbackOnce sync.Once
	overlap           chan struct{}
	overlapOnce       sync.Once
	read              chan struct{}
	failFirstExec     bool
	queryMu           sync.Mutex
	execQueries       []string
}

func (f *admissionFixture) unblock() {
	if f.allowFirst != nil {
		f.allowFirstOnce.Do(func() { close(f.allowFirst) })
	}
	if f.allowCommit != nil {
		f.allowCommitOnce.Do(func() { close(f.allowCommit) })
	}
	if f.allowRollback != nil {
		f.allowRollbackOnce.Do(func() { close(f.allowRollback) })
	}
	if f.overlap != nil {
		f.overlapOnce.Do(func() { close(f.overlap) })
	}
}

func (f *admissionFixture) enter() {
	n := f.active.Add(1)
	for old := f.maximum.Load(); n > old && !f.maximum.CompareAndSwap(old, n); old = f.maximum.Load() {
	}
	if n > 1 && f.overlap != nil {
		f.overlapOnce.Do(func() { close(f.overlap) })
	}
}

func (f *admissionFixture) pause() {
	if f.firstWrite != nil {
		f.firstOnce.Do(func() { close(f.firstWrite); <-f.allowFirst })
	}
	if f.overlap != nil {
		<-f.overlap
	}
}

type admissionConnector struct{ f *admissionFixture }

func (c admissionConnector) Connect(context.Context) (driver.Conn, error) {
	return &admissionConn{f: c.f}, nil
}
func (admissionConnector) Driver() driver.Driver { return nil }

type admissionConn struct {
	f     *admissionFixture
	inTxn atomic.Bool
}

func (*admissionConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unused prepare") }
func (*admissionConn) Close() error                        { return nil }
func (c *admissionConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}
func (c *admissionConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	c.f.enter()
	c.f.begins.Add(1)
	c.inTxn.Store(true)
	c.f.pause()
	return &admissionTx{c: c}, nil
}
func (c *admissionConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	n := c.f.execs.Add(1)
	c.f.queryMu.Lock()
	c.f.execQueries = append(c.f.execQueries, query)
	c.f.queryMu.Unlock()
	if !c.inTxn.Load() {
		c.f.enter()
		defer c.f.active.Add(-1)
	}
	c.f.pause()
	if n == 1 && c.f.failFirstExec {
		return nil, sqliteBusyEvidence{}
	}
	return driver.RowsAffected(1), nil
}
func (c *admissionConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	if !strings.HasPrefix(q, "SELECT") && !c.inTxn.Load() {
		c.f.enter()
		c.f.pause()
		return &admissionRows{release: func() { c.f.active.Add(-1) }}, nil
	}
	if c.f.read != nil {
		select {
		case c.f.read <- struct{}{}:
		default:
		}
	}
	return &admissionRows{}, nil
}

type admissionTx struct{ c *admissionConn }

func (t *admissionTx) finish() {
	if t.c.inTxn.Swap(false) {
		t.c.f.active.Add(-1)
	}
}
func (t *admissionTx) Commit() error {
	t.c.f.commits.Add(1)
	if t.c.f.commitStarted != nil {
		t.c.f.commitOnce.Do(func() { close(t.c.f.commitStarted); <-t.c.f.allowCommit })
	}
	t.finish()
	return t.c.f.commitErr
}
func (t *admissionTx) Rollback() error {
	if t.c.f.rollbackStarted != nil {
		t.c.f.rollbackOnce.Do(func() { close(t.c.f.rollbackStarted); <-t.c.f.allowRollback })
	}
	t.finish()
	return nil
}

type admissionRows struct {
	done    bool
	once    sync.Once
	release func()
}

func (*admissionRows) Columns() []string { return []string{"value"} }
func (r *admissionRows) Close() error {
	r.once.Do(func() {
		if r.release != nil {
			r.release()
		}
	})
	return nil
}
func (r *admissionRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0] = int64(1)
	return nil
}

func admissionTestDB(t *testing.T, f *admissionFixture) *db {
	t.Helper()
	sdb := sql.OpenDB(admissionConnector{f})
	sdb.SetMaxOpenConns(4)
	d := &db{sql: sdb, remote: true}
	t.Cleanup(func() { f.unblock(); _ = d.close() })
	return d
}

func TestRemoteMutationEntrypointsDoNotOverlap(t *testing.T) {
	f := &admissionFixture{firstWrite: make(chan struct{}), allowFirst: make(chan struct{})}
	d := admissionTestDB(t, f)
	e := fakeRemoteEngine(d)
	ctx := context.Background()
	work := []func() error{
		func() error { _, err := d.exec(ctx, "UPDATE t SET x=1"); return err },
		func() error {
			_, err := d.execFresh(ctx, "UPDATE t SET x=?", func() []any { return []any{1} })
			return err
		},
		func() error {
			return d.queryFreshWrite(ctx, "UPDATE t SET x=1 RETURNING x", func() []any { return nil }, func(r *sql.Rows) error {
				for r.Next() {
				}
				return r.Err()
			})
		},
		func() error {
			return e.inTx(ctx, func(c context.Context, tx *txn) error { _, err := tx.ExecContext(c, "UPDATE t SET x=1"); return err })
		},
		func() error {
			return d.queryFreshWrite(ctx, "WITH incoming AS (SELECT 1) UPDATE t SET x=1 RETURNING x", func() []any { return nil }, func(r *sql.Rows) error {
				for r.Next() {
				}
				return r.Err()
			})
		},
	}
	var wg sync.WaitGroup
	errs := make(chan error, 24)
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); errs <- work[i%len(work)]() }(i)
	}
	<-f.firstWrite
	waitRemoteQueue(t, &d.writer, 23, 0)
	f.unblock()
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("guarded SQL writers: max=%d", f.maximum.Load())
	if got := f.maximum.Load(); got != 1 {
		t.Fatalf("remote mutations overlapped across %d SQL leases", got)
	}
}

func TestRemoteMutationFixtureDetectsUnguardedOverlap(t *testing.T) {
	// Calibrate the same driver and pool against raw SQL calls that bypass the
	// engine. A max of one here would make the guarded comparison uninformative.
	f := &admissionFixture{overlap: make(chan struct{})}
	d := admissionTestDB(t, f)
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = d.sql.ExecContext(context.Background(), "UPDATE t SET x=1") }()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		f.unblock()
		<-done
		t.Fatal("negative control could not acquire two independent SQL connections")
	}
	t.Logf("unguarded SQL negative control: max=%d", f.maximum.Load())
	if f.maximum.Load() < 2 {
		t.Fatal("negative control failed to expose independent SQL writers")
	}
}

func TestRemoteWriterRetryBackoffDoesNotHoldAdmission(t *testing.T) {
	f := &admissionFixture{firstWrite: make(chan struct{}), allowFirst: make(chan struct{}), failFirstExec: true}
	d := admissionTestDB(t, f)
	first := make(chan error, 1)
	go func() { _, err := d.exec(context.Background(), "UPDATE first"); first <- err }()
	<-f.firstWrite
	lease := make(chan error, 1)
	go func() { _, err := d.exec(leaseWriteContext(context.Background()), "UPDATE lease"); lease <- err }()
	waitRemoteQueue(t, &d.writer, 0, 1)
	f.unblock()
	if err := <-lease; err != nil {
		t.Fatal(err)
	}
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	f.queryMu.Lock()
	defer f.queryMu.Unlock()
	if strings.Join(f.execQueries, ",") != "UPDATE first,UPDATE lease,UPDATE first" {
		t.Fatalf("retry held admission across backoff: %v", f.execQueries)
	}
}

func TestRemoteWriterCancellationWaitsOutsideSQLPool(t *testing.T) {
	f := &admissionFixture{}
	d := admissionTestDB(t, f)
	release, _ := d.writer.acquire(context.Background())
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var built atomic.Bool
	done := make(chan error, 1)
	go func() {
		_, err := d.execFresh(ctx, "UPDATE t", func() []any { built.Store(true); return nil })
		done <- err
	}()
	waitRemoteQueue(t, &d.writer, 1, 0)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("wait cancellation: %v", err)
	}
	if built.Load() || f.execs.Load() != 0 || d.sql.Stats().OpenConnections != 0 {
		t.Fatal("canceled queue entry acquired SQL or sampled statement arguments")
	}
}

func TestRemoteLeaseMutationAPIsUseUrgentAdmission(t *testing.T) {
	for _, name := range []string{"renew", "renew_batch", "complete", "complete_batch", "abandon", "reject", "defer"} {
		t.Run(name, func(t *testing.T) {
			f := &admissionFixture{}
			d := admissionTestDB(t, f)
			e := fakeRemoteEngine(d)
			e.qcache["q"] = queueRow{name: "q", kind: "queue", lifecycleState: "active", lockDurationMs: 1000}
			release, _ := d.writer.acquire(context.Background())
			defer release()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				var err error
				switch name {
				case "renew":
					err = e.Renew(ctx, "q", 1, "token")
				case "renew_batch":
					_, err = e.RenewBatch(ctx, "q", []SettleItem{{SeqNumber: 1, LockToken: "token"}})
				case "complete":
					err = e.Complete(ctx, "q", 1, "token")
				case "complete_batch":
					_, err = e.CompleteBatch(ctx, "q", []SettleItem{{SeqNumber: 1, LockToken: "token"}})
				case "abandon":
					err = e.Abandon(ctx, "q", 1, "token", 0)
				case "reject":
					err = e.Reject(ctx, "q", 1, "token", "reason", "description")
				case "defer":
					err = e.Defer(ctx, "q", 1, "token")
				}
				done <- err
			}()
			waitRemoteQueue(t, &d.writer, 0, 1)
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled lease mutation: %v", err)
			}
			if f.execs.Load() != 0 || f.begins.Load() != 0 {
				t.Fatal("queued lease mutation executed SQL")
			}
		})
	}
}

func TestRemoteReturningScanPanicAndGoexitReleaseAdmission(t *testing.T) {
	for _, exit := range []bool{false, true} {
		t.Run(fmt.Sprintf("goexit=%v", exit), func(t *testing.T) {
			f := &admissionFixture{}
			d := admissionTestDB(t, f)
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer func() { _ = recover() }()
				_ = d.queryFreshWrite(context.Background(), "UPDATE t RETURNING x", func() []any { return nil }, func(*sql.Rows) error {
					if exit {
						runtime.Goexit()
					}
					panic("controlled scan panic")
				})
			}()
			<-done
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if _, err := d.exec(ctx, "UPDATE t"); err != nil {
				t.Fatal(err)
			}
			if f.active.Load() != 0 {
				t.Fatal("RETURNING scan left its SQL write active")
			}
		})
	}
}

func TestRemoteWriterPreservesReadsAndCloseWakesQueuedWrites(t *testing.T) {
	f := &admissionFixture{firstWrite: make(chan struct{}), allowFirst: make(chan struct{}), read: make(chan struct{}, 1)}
	d := admissionTestDB(t, f)
	first := make(chan error, 1)
	go func() { _, err := d.exec(context.Background(), "UPDATE t SET x=1"); first <- err }()
	<-f.firstWrite
	second := make(chan error, 1)
	go func() { _, err := d.exec(context.Background(), "UPDATE t SET x=2"); second <- err }()
	waitRemoteQueue(t, &d.writer, 1, 0)
	readCtx, cancelRead := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelRead()
	if err := d.queryRowScan(readCtx, []any{new(int64)}, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-f.read:
	default:
		t.Fatal("read did not use a spare pool connection")
	}
	d.beginClosing()
	if err := <-second; !errors.Is(err, ErrClosed) {
		t.Fatalf("queued write not closed: %v", err)
	}
	if f.execs.Load() != 1 {
		t.Fatal("queued write issued SQL after closing admission")
	}
	f.unblock()
	if err := <-first; err != nil {
		t.Fatal(err)
	}
}

func TestRemoteTransactionPanicGoexitAndUnknownCommitReleaseAdmission(t *testing.T) {
	for _, mode := range []string{"panic", "goexit", "commit_eof"} {
		t.Run(mode, func(t *testing.T) {
			f := &admissionFixture{}
			if mode == "commit_eof" {
				f.commitErr = io.EOF
			}
			d := admissionTestDB(t, f)
			e := fakeRemoteEngine(d)
			done := make(chan struct{})
			var calls atomic.Int64
			go func() {
				defer close(done)
				defer func() { _ = recover() }()
				err := e.inTx(context.Background(), func(context.Context, *txn) error {
					calls.Add(1)
					switch mode {
					case "panic":
						panic("controlled panic")
					case "goexit":
						runtime.Goexit()
					}
					return nil
				})
				if mode == "commit_eof" && !errors.Is(err, ErrOutcomeUnknown) {
					t.Errorf("commit EOF lost unknown identity: %v", err)
				}
			}()
			<-done
			if calls.Load() != 1 {
				t.Fatalf("callback replayed %d times", calls.Load())
			}
			if mode == "commit_eof" && f.commits.Load() != 1 {
				t.Fatal("unknown COMMIT was retried")
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if _, err := d.exec(ctx, "UPDATE t SET x=3"); err != nil {
				t.Fatalf("admission leaked after %s: %v", mode, err)
			}
		})
	}
}

func TestRemoteUnknownCommitRetainsAdmissionUntilDriverReturns(t *testing.T) {
	f := &admissionFixture{commitErr: io.EOF, commitStarted: make(chan struct{}), allowCommit: make(chan struct{})}
	d := admissionTestDB(t, f)
	e := fakeRemoteEngine(d)
	committed := make(chan error, 1)
	go func() { committed <- e.inTx(context.Background(), func(context.Context, *txn) error { return nil }) }()
	<-f.commitStarted
	next := make(chan error, 1)
	go func() { _, err := d.exec(context.Background(), "UPDATE next"); next <- err }()
	waitRemoteQueue(t, &d.writer, 1, 0)
	if f.execs.Load() != 0 || f.active.Load() != 1 {
		t.Fatal("an unresolved COMMIT lost its writer ownership")
	}
	f.unblock()
	if err := <-committed; !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("commit result=%v, want outcome unknown", err)
	}
	if err := <-next; err != nil {
		t.Fatal(err)
	}
	if f.begins.Load() != 1 || f.commits.Load() != 1 || f.maximum.Load() != 1 {
		t.Fatalf("unknown commit replayed or overlapped: begins=%d commits=%d max=%d", f.begins.Load(), f.commits.Load(), f.maximum.Load())
	}
}

func TestRemoteCanceledTransactionWaitsForAsynchronousRollback(t *testing.T) {
	f := &admissionFixture{rollbackStarted: make(chan struct{}), allowRollback: make(chan struct{})}
	d := admissionTestDB(t, f)
	e := fakeRemoteEngine(d)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	callbackStarted, returnCallback := make(chan struct{}), make(chan struct{})
	var returnOnce sync.Once
	finishCallback := func() { returnOnce.Do(func() { close(returnCallback) }) }
	t.Cleanup(finishCallback)
	first := make(chan error, 1)
	go func() {
		first <- e.inTx(ctx, func(context.Context, *txn) error {
			close(callbackStarted)
			<-returnCallback
			return nil
		})
	}()
	select {
	case <-callbackStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("transaction callback did not start")
	}
	next := make(chan error, 1)
	go func() { _, err := d.exec(context.Background(), "UPDATE next"); next <- err }()
	waitRemoteQueue(t, &d.writer, 1, 0)
	// Force database/sql's awaitDone goroutine to own Rollback first. Its Tx
	// is already marked done, but the driver transaction and SQL lease are live.
	cancel()
	select {
	case <-f.rollbackStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("cancellation did not start the asynchronous driver rollback")
	}
	finishCallback()
	var firstErr error
	firstReturned := false
	select {
	case firstErr = <-first:
		firstReturned = true
		t.Errorf("transaction returned before driver rollback finished: %v", firstErr)
	case <-time.After(50 * time.Millisecond):
	}
	var nextErr error
	nextReturned := false
	select {
	case nextErr = <-next:
		nextReturned = true
		t.Errorf("next writer passed admission before driver rollback finished: %v", nextErr)
	case <-time.After(50 * time.Millisecond):
	}
	closed := make(chan error, 1)
	go func() { closed <- d.close() }()
	if !nextReturned {
		select {
		case nextErr = <-next:
			nextReturned = true
		case <-time.After(3 * time.Second):
			t.Fatal("Close did not wake the queued writer")
		}
		if !errors.Is(nextErr, ErrClosed) {
			t.Errorf("queued writer after Close: %v", nextErr)
		}
	}
	var closeErr error
	closeReturned := false
	select {
	case closeErr = <-closed:
		closeReturned = true
		t.Errorf("Close returned while the driver rollback still held its SQL lease: %v", closeErr)
	case <-time.After(50 * time.Millisecond):
	}
	f.unblock()
	if !firstReturned {
		select {
		case firstErr = <-first:
		case <-time.After(3 * time.Second):
			t.Fatal("transaction did not finish after driver rollback returned")
		}
	}
	if !errors.Is(firstErr, context.Canceled) || errors.Is(firstErr, ErrOutcomeUnknown) {
		t.Errorf("cancellation identity changed: %v", firstErr)
	}
	if !closeReturned {
		select {
		case closeErr = <-closed:
		case <-time.After(3 * time.Second):
			t.Fatal("Close did not finish after the SQL lease returned")
		}
	}
	if closeErr != nil {
		t.Errorf("Close: %v", closeErr)
	}
	if f.maximum.Load() != 1 || f.execs.Load() != 0 || f.active.Load() != 0 {
		t.Errorf("rollback overlapped a queued write: max=%d execs=%d active=%d", f.maximum.Load(), f.execs.Load(), f.active.Load())
	}
}
