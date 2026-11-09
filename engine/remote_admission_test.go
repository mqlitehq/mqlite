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
	waitRemoteQueues(t, w, normal, urgent, 0)
}

func waitRemoteQueues(t *testing.T, w *remoteWriter, normal, urgent, renew int) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		w.mu.Lock()
		n, u, r := w.normal.Len(), w.urgent.Len(), w.renew.Len()
		w.mu.Unlock()
		if n == normal && u == urgent && r == renew {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("queue has normal=%d urgent=%d renew=%d, want %d/%d/%d", n, u, r, normal, urgent, renew)
		default:
			runtime.Gosched()
		}
	}
}

func waitPendingRenewalCalls(t *testing.T, w *remoteWriter, calls int) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		w.mu.Lock()
		pending := 0
		for element := w.renew.Front(); element != nil; element = element.Next() {
			waiter := element.Value.(*remoteWriteWaiter)
			if waiter.renewal == nil {
				pending++
				continue
			}
			for _, member := range waiter.renewal.members {
				if member.active && member.ctx.Err() == nil {
					pending++
				}
			}
		}
		w.mu.Unlock()
		if pending == calls {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("pending renewal calls=%d, want %d", pending, calls)
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

func TestRemoteWriterRenewalFIFOAndBothLowerPrioritiesProgress(t *testing.T) {
	w := &remoteWriter{}
	first, err := w.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.close)
	order := make(chan string, 40)
	releases := make(chan func(), 40)
	queue := func(id string, ctx context.Context, n, u, r int) {
		go func() {
			release, err := w.acquire(ctx)
			if err != nil {
				return
			}
			order <- id
			releases <- release
		}()
		waitRemoteQueues(t, w, n, u, r)
	}
	for i := 0; i < 2; i++ {
		queue(fmt.Sprintf("n%d", i), context.Background(), i+1, 0, 0)
	}
	for i := 0; i < 6; i++ {
		queue(fmt.Sprintf("s%d", i), leaseWriteContext(context.Background()), 2, i+1, 0)
	}
	for i := 0; i < 26; i++ {
		queue(fmt.Sprintf("r%d", i), renewWriteContext(context.Background()), 2, 6, i+1)
	}
	var want []string
	for group := 0; group < 5; group++ {
		for i := 0; i < 4; i++ {
			want = append(want, fmt.Sprintf("r%d", 4*group+i))
		}
		if group < 4 {
			want = append(want, fmt.Sprintf("s%d", group))
		} else {
			want = append(want, "n0")
		}
	}
	want = append(want, "r20", "r21", "r22", "r23", "s4", "r24", "r25", "s5", "n1")
	first()
	for i, next := range want {
		select {
		case got := <-order:
			if got != next {
				t.Fatalf("grant %d=%s, want %s: renewals must not consume settlement fairness", i, got, next)
			}
			(<-releases)()
		case <-time.After(3 * time.Second):
			t.Fatalf("grant %s did not progress", next)
		}
	}
}

func TestRemoteWriterUncontendedRenewalCountsTowardBoundedBurst(t *testing.T) {
	w := &remoteWriter{}
	first, err := w.acquire(renewWriteContext(context.Background()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.close)
	order := make(chan string, 6)
	releases := make(chan func(), 6)
	queue := func(id string, ctx context.Context, n, r int) {
		go func() {
			release, err := w.acquire(ctx)
			if err != nil {
				return
			}
			order <- id
			releases <- release
		}()
		waitRemoteQueues(t, w, n, 0, r)
	}
	queue("normal", context.Background(), 1, 0)
	for i := 0; i < 5; i++ {
		queue(fmt.Sprintf("r%d", i), renewWriteContext(context.Background()), 1, i+1)
	}
	first()
	for _, want := range []string{"r0", "r1", "r2", "normal", "r3", "r4"} {
		if got := <-order; got != want {
			t.Fatalf("grant=%s want=%s", got, want)
		}
		(<-releases)()
	}
}

func admissionPriorityContext(ctx context.Context, priority int) context.Context {
	switch priority {
	case 1:
		return leaseWriteContext(ctx)
	case 2:
		return renewWriteContext(ctx)
	default:
		return ctx
	}
}

func waitRemotePriority(t *testing.T, w *remoteWriter, priority int) {
	t.Helper()
	counts := [3]int{}
	counts[priority] = 1
	waitRemoteQueues(t, w, counts[0], counts[1], counts[2])
}

func TestRemoteWriterCanceledGrantDoesNotLeakPermit(t *testing.T) {
	for i := 0; i < 100; i++ {
		w := &remoteWriter{}
		release, _ := w.acquire(context.Background())
		ctx, cancel := context.WithCancel(context.Background())
		ctx = admissionPriorityContext(ctx, i%3)
		done := make(chan error, 1)
		go func() {
			p, err := w.acquire(ctx)
			if p != nil {
				p()
			}
			done <- err
		}()
		waitRemotePriority(t, w, i%3)
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

func TestRemoteWriterCloseWakesAllQueuesAndRejectsLaterWork(t *testing.T) {
	w := &remoteWriter{}
	release, _ := w.acquire(context.Background())
	defer release()
	done := make(chan error, 3)
	for _, ctx := range []context.Context{context.Background(), leaseWriteContext(context.Background()), renewWriteContext(context.Background())} {
		go func(ctx context.Context) { _, err := w.acquire(ctx); done <- err }(ctx)
	}
	waitRemoteQueues(t, w, 1, 1, 1)
	w.close()
	w.close()
	for i := 0; i < 3; i++ {
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
		ctx = admissionPriorityContext(ctx, i%3)
		done := make(chan error, 1)
		go func() {
			p, err := w.acquire(ctx)
			if p != nil {
				p()
			}
			done <- err
		}()
		waitRemotePriority(t, w, i%3)
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
		leaked := w.held || w.normal.Len() != 0 || w.urgent.Len() != 0 || w.renew.Len() != 0
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
	returning         func(context.Context, string, []driver.NamedValue) (driver.Rows, error)
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
func (c *admissionConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	if !strings.HasPrefix(q, "SELECT") && !c.inTxn.Load() {
		c.f.enter()
		c.f.pause()
		if c.f.returning != nil {
			rows, err := c.f.returning(ctx, q, args)
			if err != nil {
				c.f.active.Add(-1)
				return nil, err
			}
			return &admissionRows{inner: rows, release: func() { c.f.active.Add(-1) }}, nil
		}
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
	done     bool
	once     sync.Once
	release  func()
	inner    driver.Rows
	closeErr error
}

func (r *admissionRows) Columns() []string {
	if r.inner != nil {
		return r.inner.Columns()
	}
	return []string{"value"}
}
func (r *admissionRows) Close() error {
	r.once.Do(func() {
		if r.inner != nil {
			r.closeErr = r.inner.Close()
		}
		if r.release != nil {
			r.release()
		}
	})
	return r.closeErr
}
func (r *admissionRows) Next(dest []driver.Value) error {
	if r.inner != nil {
		return r.inner.Next(dest)
	}
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

// MQLITE-175: calls already waiting behind a writer should share one renewal
// statement, while retaining every caller's original shape and token fence.
func TestRemotePendingRenewalsShareStatementAndPreserveMasks(t *testing.T) {
	f := &admissionFixture{}
	var statements atomic.Int64
	f.returning = func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
		if ctx.Value(renewalCallerContextKey{}) != nil {
			return nil, errors.New("shared SQL inherited one caller's context value")
		}
		if !strings.Contains(query, "UPDATE messages SET locked_until") {
			return nil, fmt.Errorf("unexpected statement: %s", query)
		}
		statements.Add(1)
		until := args[0].Value.(int64)
		var values [][]driver.Value
		for i := 4; i < len(args); i += 2 {
			if args[i+1].Value == "valid" {
				values = append(values, []driver.Value{args[i].Value, args[i+1].Value, until})
			}
		}
		return &valueRows{values: values}, nil
	}
	d := admissionTestDB(t, f)
	d.dsn = "libsql://fixture.invalid" // The injected Connector never performs network I/O.
	e := fakeRemoteEngine(d)
	e.qcache["q"] = queueRow{name: "q", kind: "queue", lifecycleState: "active", lockDurationMs: 3000}
	release, err := d.writer.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	requests := [][]SettleItem{
		{{SeqNumber: 1, LockToken: "valid"}, {SeqNumber: 1, LockToken: "stale"}, {SeqNumber: 19}},
		{{SeqNumber: 2, LockToken: "valid"}, {SeqNumber: 1, LockToken: "valid"}, {SeqNumber: 999, LockToken: "stale"}},
	}
	type answer struct {
		items []SettleResult
		err   error
	}
	answers := []chan answer{make(chan answer, 1), make(chan answer, 1)}
	for i := range requests {
		go func(i int) {
			ctx := context.WithValue(context.Background(), renewalCallerContextKey{}, i)
			items, err := e.RenewBatch(ctx, "q", requests[i])
			answers[i] <- answer{items: items, err: err}
		}(i)
		waitPendingRenewalCalls(t, &d.writer, i+1)
	}
	release()
	for i, ch := range answers {
		select {
		case got := <-ch:
			if got.err != nil || len(got.items) != len(requests[i]) {
				t.Fatalf("caller %d: result=%v err=%v", i, got.items, got.err)
			}
			for j, result := range got.items {
				wantOK := requests[i][j].LockToken == "valid"
				if result.SeqNumber != requests[i][j].SeqNumber || result.Ok != wantOK ||
					(result.Ok && result.LockedUntilMs != 4000) || (!result.Ok && result.LockedUntilMs != 0) {
					t.Errorf("caller %d item %d: result=%+v", i, j, result)
				}
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("caller %d did not return", i)
		}
	}
	if got := statements.Load(); got != 1 {
		t.Errorf("pending compatible renewals issued %d statements, want one", got)
	}
	if f.active.Load() != 0 || f.maximum.Load() != 1 {
		t.Errorf("SQL ownership: active=%d maximum=%d", f.active.Load(), f.maximum.Load())
	}
	assertRenewalLogicalWrites(t, d, 2, 0)
}

type renewalCallerContextKey struct{}

func assertRenewalLogicalWrites(t *testing.T, d *db, ok, canceled uint64) {
	t.Helper()
	got := make(map[string]uint64)
	for _, operation := range d.storage.snapshot(d.sql.Stats()).Operations {
		if operation.Operation == "write" && operation.Duration.Count != 0 {
			got[operation.Outcome+"/"+operation.ErrorCode] += operation.Duration.Count
		}
	}
	if got["ok/"] != ok || got["error/canceled"] != canceled {
		t.Errorf("logical write observations=%v, want ok=%d canceled=%d", got, ok, canceled)
	}
	delete(got, "ok/")
	delete(got, "error/canceled")
	if len(got) != 0 {
		t.Errorf("unexpected aggregate or extra logical write observations: %v", got)
	}
}

func renewalAdmissionRows(query string, args []driver.NamedValue) (driver.Rows, error) {
	if !strings.Contains(query, "UPDATE messages SET locked_until") || len(args) < 6 || len(args)%2 != 0 {
		return nil, fmt.Errorf("unexpected renewal statement or argument count: %s (%d)", query, len(args))
	}
	var values [][]driver.Value
	for i := 4; i < len(args); i += 2 {
		if args[i+1].Value == "valid" {
			values = append(values, []driver.Value{args[i].Value, args[i+1].Value, args[0].Value})
		}
	}
	return &valueRows{values: values}, nil
}

type pendingRenewalAnswer struct {
	items []SettleResult
	err   error
}

func startPendingRenewal(e *Engine, ctx context.Context, queue string, items []SettleItem) <-chan pendingRenewalAnswer {
	result := make(chan pendingRenewalAnswer, 1)
	go func() {
		items, err := e.RenewBatch(ctx, queue, items)
		result <- pendingRenewalAnswer{items: items, err: err}
	}()
	return result
}

func receivePendingRenewal(t *testing.T, ch <-chan pendingRenewalAnswer) pendingRenewalAnswer {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(3 * time.Second):
		t.Fatal("renewal caller did not return")
		return pendingRenewalAnswer{}
	}
}

func TestRemotePendingRenewalCancellationPreservesOtherCaller(t *testing.T) {
	for canceled := 0; canceled < 2; canceled++ {
		t.Run(fmt.Sprintf("canceled_caller_%d", canceled), func(t *testing.T) {
			f := &admissionFixture{}
			var statements atomic.Int64
			var submitted []int64
			f.returning = func(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
				statements.Add(1)
				for i := 4; i < len(args); i += 2 {
					submitted = append(submitted, args[i].Value.(int64))
				}
				return renewalAdmissionRows(query, args)
			}
			d := admissionTestDB(t, f)
			d.dsn = "libsql://fixture.invalid"
			e := fakeRemoteEngine(d)
			e.qcache["q"] = queueRow{name: "q", kind: "queue", lifecycleState: "active", lockDurationMs: 3000}
			release, err := d.writer.acquire(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			contexts := make([]context.Context, 2)
			cancel := make([]context.CancelFunc, 2)
			for i := range contexts {
				contexts[i], cancel[i] = context.WithCancel(context.Background())
				defer cancel[i]()
			}
			requests := [][]SettleItem{
				{{SeqNumber: 1, LockToken: "valid"}, {SeqNumber: 2, LockToken: "valid"}},
				{{SeqNumber: 2, LockToken: "valid"}, {SeqNumber: 3, LockToken: "valid"}},
			}
			answers := make([]<-chan pendingRenewalAnswer, 2)
			for i := range answers {
				answers[i] = startPendingRenewal(e, contexts[i], "q", requests[i])
				waitPendingRenewalCalls(t, &d.writer, i+1)
			}
			cancel[canceled]()
			abandoned := receivePendingRenewal(t, answers[canceled])
			if !errors.Is(abandoned.err, context.Canceled) || errors.Is(abandoned.err, ErrOutcomeUnknown) {
				t.Fatalf("canceled caller changed error identity: %v", abandoned.err)
			}
			if f.active.Load() != 0 || d.sql.Stats().OpenConnections != 0 {
				t.Fatal("pending cancellation occupied a SQL connection")
			}
			release()
			peer := receivePendingRenewal(t, answers[1-canceled])
			if peer.err != nil || len(peer.items) != 2 || !peer.items[0].Ok || !peer.items[1].Ok {
				t.Fatalf("live peer inherited cancellation: result=%v err=%v", peer.items, peer.err)
			}
			for i, item := range peer.items {
				if item.SeqNumber != requests[1-canceled][i].SeqNumber {
					t.Errorf("live peer result order changed: %v", peer.items)
				}
			}
			want := []int64{1, 2}
			if canceled == 0 {
				want = []int64{2, 3}
			}
			seen := make(map[int64]bool)
			for _, id := range submitted {
				seen[id] = true
			}
			if statements.Load() != 1 || len(submitted) != 2 || len(seen) != 2 || !seen[want[0]] || !seen[want[1]] {
				t.Errorf("canceled sole pairs retained or shared pair lost: SQL=%d pairs=%v want=%v", statements.Load(), submitted, want)
			}
			if f.active.Load() != 0 || f.maximum.Load() != 1 {
				t.Errorf("SQL ownership: active=%d maximum=%d", f.active.Load(), f.maximum.Load())
			}
			assertRenewalLogicalWrites(t, d, 1, 1)
		})
	}
}

func TestRemotePendingRenewalsAllCanceledDoNotAcquireSQL(t *testing.T) {
	f := &admissionFixture{}
	var statements atomic.Int64
	f.returning = func(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
		statements.Add(1)
		return renewalAdmissionRows(query, args)
	}
	d := admissionTestDB(t, f)
	d.dsn = "libsql://fixture.invalid"
	e := fakeRemoteEngine(d)
	e.qcache["q"] = queueRow{name: "q", kind: "queue", lifecycleState: "active", lockDurationMs: 3000}
	release, err := d.writer.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var answers []<-chan pendingRenewalAnswer
	var cancellations []context.CancelFunc
	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cancellations = append(cancellations, cancel)
		answers = append(answers, startPendingRenewal(e, ctx, "q", []SettleItem{{SeqNumber: int64(i + 1), LockToken: "valid"}}))
		waitPendingRenewalCalls(t, &d.writer, i+1)
	}
	for _, cancel := range cancellations {
		cancel()
	}
	for _, answer := range answers {
		if result := receivePendingRenewal(t, answer); !errors.Is(result.err, context.Canceled) {
			t.Errorf("canceled caller: %v", result.err)
		}
	}
	release()
	if err := d.close(); err != nil {
		t.Fatal(err)
	}
	if statements.Load() != 0 || f.active.Load() != 0 || d.sql.Stats().OpenConnections != 0 {
		t.Errorf("abandoned pending work touched SQL: statements=%d active=%d connections=%d", statements.Load(), f.active.Load(), d.sql.Stats().OpenConnections)
	}
}

func TestRemoteExecutingRenewalCancellationKeepsPhysicalOwnerUntilDriverReturns(t *testing.T) {
	started, canceled, finish := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var finishOnce sync.Once
	unblock := func() { finishOnce.Do(func() { close(finish) }) }
	defer unblock()
	f := &admissionFixture{}
	var statements atomic.Int64
	f.returning = func(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
		statements.Add(1)
		close(started)
		<-ctx.Done()
		close(canceled)
		// Model a driver that has seen cancellation but has not returned its SQL
		// lease. Cancellation alone is not proof that ownership has ended.
		<-finish
		return nil, ctx.Err()
	}
	d := admissionTestDB(t, f)
	d.dsn = "libsql://fixture.invalid"
	e := fakeRemoteEngine(d)
	e.qcache["q"] = queueRow{name: "q", kind: "queue", lifecycleState: "active", lockDurationMs: 3000}
	release, err := d.writer.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var answers []<-chan pendingRenewalAnswer
	var cancellations []context.CancelFunc
	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cancellations = append(cancellations, cancel)
		answers = append(answers, startPendingRenewal(e, ctx, "q", []SettleItem{{SeqNumber: int64(i + 1), LockToken: "valid"}}))
		waitPendingRenewalCalls(t, &d.writer, i+1)
	}
	release()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("shared SQL did not start")
	}
	for _, cancel := range cancellations {
		cancel()
	}
	for _, answer := range answers {
		if result := receivePendingRenewal(t, answer); !errors.Is(result.err, context.Canceled) || errors.Is(result.err, ErrOutcomeUnknown) {
			t.Errorf("canceled member changed context identity: %v", result.err)
		}
	}
	select {
	case <-canceled:
	case <-time.After(3 * time.Second):
		t.Fatal("all canceled members did not cancel their shared SQL")
	}
	if f.active.Load() != 1 || d.sql.Stats().InUse != 1 {
		t.Errorf("caller return released a live SQL lease: active=%d in_use=%d", f.active.Load(), d.sql.Stats().InUse)
	}
	next := make(chan error, 1)
	go func() { _, err := d.exec(context.Background(), "UPDATE after_canceled_renewal"); next <- err }()
	waitRemoteQueues(t, &d.writer, 1, 0, 0)
	closed := make(chan error, 1)
	go func() { closed <- d.close() }()
	select {
	case err := <-next:
		if !errors.Is(err, ErrClosed) {
			t.Errorf("queued writer after Close: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not wake the queued writer")
	}
	if f.execs.Load() != 0 {
		t.Error("another writer ran before the canceled SQL released its lease")
	}
	select {
	case err := <-closed:
		t.Fatalf("Close returned before the admitted group joined: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	unblock()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not finish after the driver returned")
	}
	if statements.Load() != 1 || f.active.Load() != 0 || f.maximum.Load() != 1 || d.sql.Stats().InUse != 0 {
		t.Errorf("physical ownership leaked or overlapped: statements=%d active=%d maximum=%d in_use=%d", statements.Load(), f.active.Load(), f.maximum.Load(), d.sql.Stats().InUse)
	}
}

func TestRemoteExecutingRenewalMemberCancellationDoesNotCancelPeerSQL(t *testing.T) {
	for abandoned := 0; abandoned < 2; abandoned++ {
		t.Run(fmt.Sprintf("canceled_caller_%d", abandoned), func(t *testing.T) {
			started, finish := make(chan struct{}), make(chan struct{})
			var finishOnce sync.Once
			unblock := func() { finishOnce.Do(func() { close(finish) }) }
			defer unblock()
			f := &admissionFixture{}
			var statements atomic.Int64
			f.returning = func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
				statements.Add(1)
				close(started)
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-finish:
					return renewalAdmissionRows(query, args)
				}
			}
			d := admissionTestDB(t, f)
			d.dsn = "libsql://fixture.invalid"
			e := fakeRemoteEngine(d)
			e.qcache["q"] = queueRow{name: "q", kind: "queue", lifecycleState: "active", lockDurationMs: 3000}
			release, err := d.writer.acquire(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			var answers []<-chan pendingRenewalAnswer
			var cancellations []context.CancelFunc
			for i := 0; i < 2; i++ {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				cancellations = append(cancellations, cancel)
				answers = append(answers, startPendingRenewal(e, ctx, "q", []SettleItem{{SeqNumber: int64(i + 1), LockToken: "valid"}}))
				waitPendingRenewalCalls(t, &d.writer, i+1)
			}
			release()
			select {
			case <-started:
			case <-time.After(3 * time.Second):
				t.Fatal("shared SQL did not start")
			}
			cancellations[abandoned]()
			if result := receivePendingRenewal(t, answers[abandoned]); !errors.Is(result.err, context.Canceled) {
				t.Errorf("canceled member: %v", result.err)
			}
			unblock()
			peer := receivePendingRenewal(t, answers[1-abandoned])
			if peer.err != nil || len(peer.items) != 1 || !peer.items[0].Ok || peer.items[0].SeqNumber != int64(2-abandoned) {
				t.Errorf("one caller canceled the peer SQL: result=%v err=%v", peer.items, peer.err)
			}
			if statements.Load() != 1 || f.active.Load() != 0 || f.maximum.Load() != 1 {
				t.Errorf("shared SQL ownership: statements=%d active=%d maximum=%d", statements.Load(), f.active.Load(), f.maximum.Load())
			}
			assertRenewalLogicalWrites(t, d, 1, 1)
		})
	}
}

func TestRemotePendingRenewalsDoNotMergeAcrossQueueFIFO(t *testing.T) {
	f := &admissionFixture{}
	var order []string
	f.returning = func(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
		order = append(order, args[3].Value.(string))
		return renewalAdmissionRows(query, args)
	}
	d := admissionTestDB(t, f)
	d.dsn = "libsql://fixture.invalid"
	e := fakeRemoteEngine(d)
	for _, name := range []string{"a", "b"} {
		e.qcache[name] = queueRow{name: name, kind: "queue", lifecycleState: "active", lockDurationMs: 3000}
	}
	release, err := d.writer.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var answers []<-chan pendingRenewalAnswer
	for i, queue := range []string{"a", "b", "a"} {
		answers = append(answers, startPendingRenewal(e, context.Background(), queue, []SettleItem{{SeqNumber: int64(i + 1), LockToken: "valid"}}))
		waitPendingRenewalCalls(t, &d.writer, i+1)
	}
	d.writer.mu.Lock()
	groups := d.writer.renewGroups
	d.writer.mu.Unlock()
	if groups > 2 {
		t.Errorf("pending renewal workers=%d, want at most two", groups)
	}
	release()
	for i, answer := range answers {
		result := receivePendingRenewal(t, answer)
		if result.err != nil || len(result.items) != 1 || !result.items[0].Ok || result.items[0].SeqNumber != int64(i+1) {
			t.Errorf("caller %d: result=%v err=%v", i, result.items, result.err)
		}
	}
	if fmt.Sprint(order) != "[a b a]" {
		t.Errorf("a later queue-a call crossed queue-b: order=%v", order)
	}
	if f.active.Load() != 0 || f.maximum.Load() != 1 {
		t.Errorf("FIFO writes overlapped or leaked: active=%d maximum=%d", f.active.Load(), f.maximum.Load())
	}
}

func TestRemotePendingRenewalsKeepConfigurationSnapshotsSeparate(t *testing.T) {
	for _, tc := range []struct {
		name         string
		durations    [2]int64
		expires      [2]int64
		wantDeadline [2]int64
	}{
		{name: "duration", durations: [2]int64{3000, 5000}, wantDeadline: [2]int64{4000, 6000}},
		{name: "resource_expiry", durations: [2]int64{3000, 3000}, expires: [2]int64{2000, 3500}, wantDeadline: [2]int64{2000, 3500}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &admissionFixture{}
			var statements atomic.Int64
			f.returning = func(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
				statements.Add(1)
				return renewalAdmissionRows(query, args)
			}
			d := admissionTestDB(t, f)
			d.dsn = "libsql://fixture.invalid"
			e := fakeRemoteEngine(d)
			release, err := d.writer.acquire(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			var answers []<-chan pendingRenewalAnswer
			for i := 0; i < 2; i++ {
				e.qmu.Lock()
				e.qcache["q"] = queueRow{name: "q", kind: "queue", lifecycleState: "active", lockDurationMs: tc.durations[i], expiresAtMs: tc.expires[i]}
				e.qmu.Unlock()
				answers = append(answers, startPendingRenewal(e, context.Background(), "q", []SettleItem{{SeqNumber: int64(i + 1), LockToken: "valid"}}))
				waitPendingRenewalCalls(t, &d.writer, i+1)
			}
			release()
			for i, answer := range answers {
				result := receivePendingRenewal(t, answer)
				if result.err != nil || len(result.items) != 1 || !result.items[0].Ok || result.items[0].LockedUntilMs != tc.wantDeadline[i] {
					t.Errorf("configuration snapshot %d: result=%v err=%v want_deadline=%d", i, result.items, result.err, tc.wantDeadline[i])
				}
			}
			if statements.Load() != 2 || f.active.Load() != 0 || f.maximum.Load() != 1 {
				t.Errorf("configuration snapshots merged or overlapped: count=%d active=%d maximum=%d", statements.Load(), f.active.Load(), f.maximum.Load())
			}
			assertRenewalLogicalWrites(t, d, 2, 0)
		})
	}
}

func TestRemoteRenewalSafeRetryRequeuesAndPrunesCanceledMember(t *testing.T) {
	bStarted, finishB := make(chan struct{}), make(chan struct{})
	var finishOnce sync.Once
	unblockB := func() { finishOnce.Do(func() { close(finishB) }) }
	defer unblockB()
	f := &admissionFixture{}
	var order []string
	var retryPairs []int64
	f.returning = func(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
		queue := args[3].Value.(string)
		order = append(order, queue)
		if len(order) == 1 {
			return nil, sqliteBusyEvidence{} // The typed rejection proves no write ran.
		}
		if queue == "b" {
			close(bStarted)
			<-finishB
		} else {
			for i := 4; i < len(args); i += 2 {
				retryPairs = append(retryPairs, args[i].Value.(int64))
			}
		}
		return renewalAdmissionRows(query, args)
	}
	d := admissionTestDB(t, f)
	d.dsn = "libsql://fixture.invalid"
	e := fakeRemoteEngine(d)
	for _, name := range []string{"a", "b"} {
		e.qcache[name] = queueRow{name: name, kind: "queue", lifecycleState: "active", lockDurationMs: 3000}
	}
	release, err := d.writer.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	leader := startPendingRenewal(e, ctx, "a", []SettleItem{{SeqNumber: 1, LockToken: "valid"}, {SeqNumber: 4, LockToken: "valid"}})
	waitPendingRenewalCalls(t, &d.writer, 1)
	peer := startPendingRenewal(e, context.Background(), "a", []SettleItem{{SeqNumber: 2, LockToken: "valid"}, {SeqNumber: 4, LockToken: "valid"}})
	waitPendingRenewalCalls(t, &d.writer, 2)
	other := startPendingRenewal(e, context.Background(), "b", []SettleItem{{SeqNumber: 3, LockToken: "valid"}})
	waitPendingRenewalCalls(t, &d.writer, 3)
	release()
	select {
	case <-bStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("safe retry retained its permit or jumped ahead of queued work")
	}
	cancel()
	if result := receivePendingRenewal(t, leader); !errors.Is(result.err, context.Canceled) || errors.Is(result.err, ErrOutcomeUnknown) {
		t.Errorf("canceled retry member: %v", result.err)
	}
	unblockB()
	if result := receivePendingRenewal(t, other); result.err != nil || len(result.items) != 1 || !result.items[0].Ok {
		t.Errorf("queued queue-b work failed: result=%v err=%v", result.items, result.err)
	}
	if result := receivePendingRenewal(t, peer); result.err != nil || len(result.items) != 2 || !result.items[0].Ok || !result.items[1].Ok {
		t.Errorf("live retry peer failed: result=%v err=%v", result.items, result.err)
	}
	seen := make(map[int64]bool)
	for _, id := range retryPairs {
		seen[id] = true
	}
	if fmt.Sprint(order) != "[a b a]" || len(retryPairs) != 2 || len(seen) != 2 || !seen[2] || !seen[4] {
		t.Errorf("retry did not requeue/prune sole pairs: order=%v retry_pairs=%v", order, retryPairs)
	}
	if f.active.Load() != 0 || f.maximum.Load() != 1 {
		t.Errorf("retry SQL overlapped or leaked: active=%d maximum=%d", f.active.Load(), f.maximum.Load())
	}
	assertRenewalLogicalWrites(t, d, 2, 1)
}

func TestRemoteAbandonedPendingRenewalDoesNotCancelFreshCaller(t *testing.T) {
	f := &admissionFixture{}
	var statements atomic.Int64
	var submitted []int64
	f.returning = func(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
		statements.Add(1)
		for i := 4; i < len(args); i += 2 {
			submitted = append(submitted, args[i].Value.(int64))
		}
		return renewalAdmissionRows(query, args)
	}
	d := admissionTestDB(t, f)
	d.dsn = "libsql://fixture.invalid"
	e := fakeRemoteEngine(d)
	e.qcache["q"] = queueRow{name: "q", kind: "queue", lifecycleState: "active", lockDurationMs: 3000}
	release, err := d.writer.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var answers []<-chan pendingRenewalAnswer
	var cancellations []context.CancelFunc
	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cancellations = append(cancellations, cancel)
		answers = append(answers, startPendingRenewal(e, ctx, "q", []SettleItem{{SeqNumber: int64(i + 1), LockToken: "valid"}}))
		waitPendingRenewalCalls(t, &d.writer, i+1)
	}
	for _, cancel := range cancellations {
		cancel()
	}
	for _, answer := range answers {
		if result := receivePendingRenewal(t, answer); !errors.Is(result.err, context.Canceled) {
			t.Errorf("abandoned member: %v", result.err)
		}
	}
	fresh := startPendingRenewal(e, context.Background(), "q", []SettleItem{{SeqNumber: 3, LockToken: "valid"}})
	waitPendingRenewalCalls(t, &d.writer, 1)
	release()
	result := receivePendingRenewal(t, fresh)
	if result.err != nil || len(result.items) != 1 || !result.items[0].Ok || result.items[0].SeqNumber != 3 {
		t.Errorf("fresh caller inherited abandoned group: result=%v err=%v", result.items, result.err)
	}
	if err := d.close(); err != nil {
		t.Fatal(err)
	}
	if statements.Load() != 1 || fmt.Sprint(submitted) != "[3]" || f.active.Load() != 0 || f.maximum.Load() != 1 {
		t.Errorf("abandoned pairs were dispatched: count=%d pairs=%v active=%d maximum=%d", statements.Load(), submitted, f.active.Load(), f.maximum.Load())
	}
	assertRenewalLogicalWrites(t, d, 1, 2)
}

// Force the scheduling window where the last caller cancels before the worker
// gets its first instruction. No runtime hook or scheduler timing is involved.
func TestRemoteCanceledRenewalBeforeWorkerStartsRetiresAdmissionTicket(t *testing.T) {
	for _, granted := range []bool{false, true} {
		t.Run(fmt.Sprintf("already_granted=%v", granted), func(t *testing.T) {
			f := &admissionFixture{}
			var renewalSQL atomic.Int64
			f.returning = func(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
				renewalSQL.Add(1)
				return renewalAdmissionRows(query, args)
			}
			d := admissionTestDB(t, f)
			d.dsn = "libsql://fixture.invalid"
			e := fakeRemoteEngine(d)
			release, err := d.writer.acquire(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			if !d.enter() {
				t.Fatal("could not register the admitted physical group")
			}
			ctx, attempt := startStorage(renewWriteContext(context.Background()), "write")
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			memberCtx, cancelMember := context.WithCancel(context.Background())
			cancelMember()
			_, memberAttempt := startStorage(memberCtx, "write")
			d.finishStorage(memberAttempt, context.Canceled)
			items := []SettleItem{{SeqNumber: 1, LockToken: "valid"}}
			bytes, _ := renewalMergeSize("q", items)
			g := &renewalGroup{
				e: e, queue: "q", config: queueRow{name: "q", kind: "queue", lifecycleState: "active", lockDurationMs: 3000},
				ctx: ctx, cancel: cancel, attempt: attempt, sealed: true, positions: 1, bytes: bytes,
				members: []*renewalMember{{ctx: memberCtx, items: items, done: make(chan struct{}), active: false, attempt: memberAttempt}},
			}
			p := &remoteWriteWaiter{ready: make(chan struct{}), queue: &d.writer.renew, renewal: g}
			g.waiter = p
			d.writer.mu.Lock()
			p.element = d.writer.renew.PushBack(p)
			d.writer.renewGroups++
			d.writer.mu.Unlock()
			// Only fixture containment on a failing implementation: after recording
			// the defect, do not leave a synthetic broken permit in test cleanup.
			defer func() {
				d.writer.mu.Lock()
				if p.element != nil {
					p.queue.Remove(p.element)
					p.element = nil
				}
				if p.granted && d.writer.held {
					d.writer.releaseLocked()
				}
				d.writer.mu.Unlock()
			}()
			if granted {
				release()
			}
			cancel()
			g.run()
			release()
			d.writer.mu.Lock()
			pending, held, groups := d.writer.renew.Len(), d.writer.held, d.writer.renewGroups
			d.writer.mu.Unlock()
			if pending != 0 || held || groups != 0 {
				t.Errorf("canceled startup left an admission ticket: pending=%d held=%v groups=%d", pending, held, groups)
			}
			ctxNext, cancelNext := context.WithTimeout(context.Background(), time.Second)
			defer cancelNext()
			if _, err := d.exec(ctxNext, "UPDATE after_canceled_group_startup"); err != nil {
				t.Errorf("next writer cannot acquire after the canceled group finished: %v", err)
			}
			closed := make(chan error, 1)
			go func() { closed <- d.close() }()
			select {
			case err := <-closed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("Close did not join the canceled startup owner")
			}
			if renewalSQL.Load() != 0 || f.active.Load() != 0 || d.sql.Stats().InUse != 0 {
				t.Errorf("canceled startup executed renewal SQL or leaked a lease: SQL=%d active=%d in_use=%d", renewalSQL.Load(), f.active.Load(), d.sql.Stats().InUse)
			}
		})
	}
}

func TestRemoteCoalescedRenewalPanicAndGoexitReleaseOwner(t *testing.T) {
	for _, goexit := range []bool{false, true} {
		t.Run(fmt.Sprintf("goexit=%v", goexit), func(t *testing.T) {
			f := &admissionFixture{}
			d := admissionTestDB(t, f)
			d.dsn = "libsql://fixture.invalid"
			e := fakeRemoteEngine(d)
			e.qcache["q"] = queueRow{name: "q", kind: "queue", lifecycleState: "active", lockDurationMs: 3000}
			originalPanic := &struct{ message string }{"controlled coalesced renewal panic"}
			var reads atomic.Int64
			e.now = func() int64 {
				if reads.Add(1) > 2 { // Both cached queue checks finish before the writer grant.
					if goexit {
						runtime.Goexit()
					}
					panic(originalPanic)
				}
				return 1000
			}
			release, err := d.writer.acquire(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			type outcome struct {
				returned bool
				panic    any
				err      error
			}
			answers := make(chan outcome, 2)
			for i := 0; i < 2; i++ {
				go func(i int) {
					result := outcome{}
					defer func() { result.panic = recover(); answers <- result }()
					_, result.err = e.RenewBatch(context.Background(), "q", []SettleItem{{SeqNumber: int64(i + 1), LockToken: "valid"}})
					result.returned = true
				}(i)
				waitPendingRenewalCalls(t, &d.writer, i+1)
			}
			release()
			for i := 0; i < 2; i++ {
				select {
				case result := <-answers:
					if goexit {
						if !result.returned || result.panic != nil || !errors.Is(result.err, ErrOutcomeUnknown) {
							t.Errorf("worker Goexit stranded or exited caller: %+v", result)
						}
					} else if result.returned || result.panic != originalPanic {
						t.Errorf("original panic was swallowed or changed: %+v", result)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("abnormal worker stranded a caller")
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if _, err := d.exec(ctx, "UPDATE after_coalesced_exit"); err != nil {
				t.Errorf("abnormal worker retained its writer permit: %v", err)
			}
			if err := d.close(); err != nil {
				t.Fatal(err)
			}
			d.writer.mu.Lock()
			groups := d.writer.renewGroups
			d.writer.mu.Unlock()
			if groups != 0 || f.active.Load() != 0 || d.sql.Stats().InUse != 0 {
				t.Errorf("abnormal worker did not join: groups=%d active=%d in_use=%d", groups, f.active.Load(), d.sql.Stats().InUse)
			}
		})
	}
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

func TestRemoteRenewalOvertakesQueuedSettlement(t *testing.T) {
	f := &admissionFixture{}
	d := admissionTestDB(t, f)
	e := fakeRemoteEngine(d)
	e.qcache["q"] = queueRow{name: "q", kind: "queue", lifecycleState: "active", lockDurationMs: 3000}
	release, _ := d.writer.acquire(context.Background())
	defer release()
	done := make(chan error, 2)
	go func() {
		_, err := d.exec(leaseWriteContext(context.Background()), "UPDATE queued_settlement")
		done <- err
	}()
	waitRemoteQueue(t, &d.writer, 0, 1)
	go func() { done <- e.Renew(context.Background(), "q", 1, "token") }()
	waitRemoteQueues(t, &d.writer, 0, 1, 1)
	release()
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	f.queryMu.Lock()
	defer f.queryMu.Unlock()
	if len(f.execQueries) != 2 || f.execQueries[0] == "UPDATE queued_settlement" {
		t.Fatalf("renewal did not overtake queued settlement: SQL=%v", f.execQueries)
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

func TestRemoteLeaseMutationAPIsUseDistinctAdmission(t *testing.T) {
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
			if name == "renew" || name == "renew_batch" {
				waitRemoteQueues(t, &d.writer, 0, 0, 1)
			} else {
				waitRemoteQueue(t, &d.writer, 0, 1)
			}
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

func TestRemoteRenewalAdmissionReleasedOnPanicAndGoexit(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, goexit := range []bool{false, true} {
			t.Run(fmt.Sprintf("batch=%v/goexit=%v", batch, goexit), func(t *testing.T) {
				f := &admissionFixture{}
				d := admissionTestDB(t, f)
				e := fakeRemoteEngine(d)
				e.qcache["q"] = queueRow{name: "q", kind: "queue", lifecycleState: "active", lockDurationMs: 3000}
				var clockCalls atomic.Int64
				e.now = func() int64 {
					if clockCalls.Add(1) == 1 {
						return 1 // the queue-liveness check precedes write admission
					}
					if goexit {
						runtime.Goexit()
					}
					panic("controlled renewal-clock panic")
				}
				release, _ := d.writer.acquire(context.Background())
				defer release()
				done := make(chan struct{})
				go func() {
					defer close(done)
					defer func() { _ = recover() }()
					if batch {
						_, _ = e.RenewBatch(context.Background(), "q", []SettleItem{{SeqNumber: 1, LockToken: "token"}})
					} else {
						_ = e.Renew(context.Background(), "q", 1, "token")
					}
				}()
				waitRemoteQueues(t, &d.writer, 0, 0, 1)
				release()
				<-done
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if _, err := d.exec(ctx, "UPDATE after_renewal_exit"); err != nil {
					t.Fatalf("renewal exit leaked admission: %v", err)
				}
			})
		}
	}
}

func TestLocalLeasePriorityContextsRemainUnchanged(t *testing.T) {
	e := &Engine{db: &db{}}
	ctx := context.Background()
	if e.leaseWriteContext(ctx) != ctx || e.renewWriteContext(ctx) != ctx {
		t.Fatal("local lease operations acquired a remote admission marker")
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
