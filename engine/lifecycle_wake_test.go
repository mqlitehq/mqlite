package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// lifecycleWakeDriver makes background lifecycle activity observable without a
// real database. Every query returns no rows, which is an idle lifecycle pass.
// Its counters are atomic because the worker and the test inspect them from
// different goroutines (including under -race).
type lifecycleWakeDriver struct {
	state *lifecycleWakeDriverState
}

type lifecycleWakeDriverState struct {
	beginCount    atomic.Int64
	commitCount   atomic.Int64
	rollbackCount atomic.Int64
	queryCount    atomic.Int64
	execCount     atomic.Int64

	blockFirstQuery atomic.Bool
	failQueries     atomic.Bool
	firstQueryOnce  sync.Once
	firstQuery      chan struct{}
	releaseFirst    chan struct{}
	commitNotify    chan struct{}
	rollbackNotify  chan struct{}
}

func newLifecycleWakeDriverState() *lifecycleWakeDriverState {
	return &lifecycleWakeDriverState{
		firstQuery:     make(chan struct{}),
		releaseFirst:   make(chan struct{}),
		commitNotify:   make(chan struct{}, 8),
		rollbackNotify: make(chan struct{}, 8),
	}
}

func (d lifecycleWakeDriver) Connect(context.Context) (driver.Conn, error) {
	return &lifecycleWakeConn{state: d.state}, nil
}

func (lifecycleWakeDriver) Driver() driver.Driver { return nil }

type lifecycleWakeConn struct {
	state *lifecycleWakeDriverState
}

func (*lifecycleWakeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not used by lifecycle wake tests")
}

func (*lifecycleWakeConn) Close() error { return nil }

func (c *lifecycleWakeConn) Begin() (driver.Tx, error) {
	c.state.beginCount.Add(1)
	return &lifecycleWakeTx{state: c.state}, nil
}

func (c *lifecycleWakeConn) QueryContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	c.state.queryCount.Add(1)
	if c.state.blockFirstQuery.CompareAndSwap(true, false) {
		c.state.firstQueryOnce.Do(func() { close(c.state.firstQuery) })
		select {
		case <-c.state.releaseFirst:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if c.state.failQueries.Load() {
		return nil, errors.New("synthetic lifecycle inventory failure")
	}
	return lifecycleWakeRows{}, nil
}

func (c *lifecycleWakeConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	c.state.execCount.Add(1)
	return driver.RowsAffected(1), nil
}

type lifecycleWakeTx struct {
	state *lifecycleWakeDriverState
}

func (t *lifecycleWakeTx) Commit() error {
	t.state.commitCount.Add(1)
	select {
	case t.state.commitNotify <- struct{}{}:
	default:
	}
	return nil
}

func (t *lifecycleWakeTx) Rollback() error {
	t.state.rollbackCount.Add(1)
	select {
	case t.state.rollbackNotify <- struct{}{}:
	default:
	}
	return nil
}

type lifecycleWakeRows struct{}

func (lifecycleWakeRows) Columns() []string         { return []string{"value"} }
func (lifecycleWakeRows) Close() error              { return nil }
func (lifecycleWakeRows) Next([]driver.Value) error { return io.EOF }

func newLifecycleWakeTestEngine(t *testing.T) (*Engine, *lifecycleWakeDriverState) {
	t.Helper()
	state := newLifecycleWakeDriverState()
	sdb := sql.OpenDB(lifecycleWakeDriver{state: state})
	sdb.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sdb.Close() })
	return &Engine{
		db:            &db{sql: sdb, remote: true},
		now:           func() int64 { return 1_700_000_000_000 },
		lifecycleWake: make(chan struct{}, 1),
	}, state
}

func waitLifecycleWorker(t *testing.T, e *Engine, cancel context.CancelFunc) {
	t.Helper()
	cancel()
	done := make(chan struct{})
	go func() {
		e.bgWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(750 * time.Millisecond):
		t.Fatal("lifecycle worker did not stop promptly after cancellation")
	}
}

func waitLifecycleCount(t *testing.T, count *atomic.Int64, notify <-chan struct{}, want int64) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for count.Load() < want {
		select {
		case <-notify:
		case <-deadline.C:
			t.Fatalf("lifecycle count=%d, want at least %d", count.Load(), want)
		}
	}
}

func waitUncertainLifecycleProbesAtMost(t *testing.T, e *Engine, want int32) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if got := e.uncertainLifecycleProbes.Load(); got <= want {
			return
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatalf("uncertain lifecycle probes=%d, want at most %d", e.uncertainLifecycleProbes.Load(), want)
		}
	}
}

func TestLifecycleSignalCoalescesWithoutBlocking(t *testing.T) {
	e := &Engine{lifecycleWake: make(chan struct{}, 1)}
	const callers = 128
	var wg sync.WaitGroup
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			e.signalLifecycle()
		}()
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("signalLifecycle blocked when its coalescing slot was full")
	}
	if got := len(e.lifecycleWake); got != 1 {
		t.Fatalf("coalesced wake count=%d, want 1", got)
	}
}

func TestLifecycleWorkerDoesNotPollStorageWhileIdle(t *testing.T) {
	e, state := newLifecycleWakeTestEngine(t)
	ctx, cancel := context.WithCancel(context.Background())
	e.startLifecycle(ctx)
	defer waitLifecycleWorker(t, e, cancel)

	// First prove the worker is scheduled and can complete a pass. Once that
	// single wake is drained, an old one-second polling loop would add another
	// transaction during the observation window.
	e.signalLifecycle()
	waitLifecycleCount(t, &state.commitCount, state.commitNotify, 1)
	baselineBegins := state.beginCount.Load()
	baselineQueries := state.queryCount.Load()
	time.Sleep(1500 * time.Millisecond)
	if got := state.beginCount.Load(); got != baselineBegins {
		t.Fatalf("idle lifecycle worker opened %d extra transactions", got-baselineBegins)
	}
	if got := state.queryCount.Load(); got != baselineQueries {
		t.Fatalf("idle lifecycle worker issued %d extra statements", got-baselineQueries)
	}
}

func TestLifecycleWorkerPreservesWakeDuringPass(t *testing.T) {
	e, state := newLifecycleWakeTestEngine(t)
	state.blockFirstQuery.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	e.startLifecycle(ctx)
	defer waitLifecycleWorker(t, e, cancel)

	e.signalLifecycle()
	select {
	case <-state.firstQuery:
	case <-time.After(2 * time.Second):
		t.Fatal("first lifecycle pass did not reach storage")
	}
	// A deletion can commit while this pass is already reading its inventory.
	// Bursts still coalesce, but at least one buffered wake must survive the pass.
	for i := 0; i < 128; i++ {
		e.signalLifecycle()
	}
	close(state.releaseFirst)
	waitLifecycleCount(t, &state.commitCount, state.commitNotify, 2)

	// Both passes are empty, so only the original wake and the coalesced in-flight
	// wake should have run.
	time.Sleep(25 * time.Millisecond)
	if got := state.beginCount.Load(); got != 2 {
		t.Fatalf("lifecycle passes=%d, want exactly 2", got)
	}
}

func TestLifecycleWorkerCancellationInterruptsRetryDelay(t *testing.T) {
	e, state := newLifecycleWakeTestEngine(t)
	state.failQueries.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	e.startLifecycle(ctx)
	defer func() {
		cancel()
		e.bgWG.Wait()
	}()
	e.signalLifecycle()
	waitLifecycleCount(t, &state.rollbackCount, state.rollbackNotify, 1)

	// A failed pass asks the worker to retry after its one-second pacing delay.
	// Prove it has not started a second pass before cancellation, then require
	// cancellation to interrupt the delay rather than making Close wait it out.
	time.Sleep(100 * time.Millisecond)
	if got := state.beginCount.Load(); got != 1 {
		t.Fatalf("failed lifecycle pass retried without pacing: begins=%d", got)
	}
	waitLifecycleWorker(t, e, cancel)
}

func TestUncertainLifecycleWakeSchedulesPacedEmptyProbe(t *testing.T) {
	e, state := newLifecycleWakeTestEngine(t)
	ctx, cancel := context.WithCancel(context.Background())
	e.startLifecycle(ctx)
	defer waitLifecycleWorker(t, e, cancel)

	e.signalUncertainLifecycle()
	waitLifecycleCount(t, &state.commitCount, state.commitNotify, 1)

	// An ordinary empty pass stops immediately. An outcome-unknown pass retains
	// reconciliation intent and waits before probing again, avoiding a hot loop.
	time.Sleep(750 * time.Millisecond)
	if got := state.commitCount.Load(); got != 1 {
		t.Fatalf("uncertain lifecycle retry was not paced: passes=%d before 750ms", got)
	}
	waitLifecycleCount(t, &state.commitCount, state.commitNotify, 2)
	waitUncertainLifecycleProbesAtMost(t, e, lifecycleUncertainPasses-2)
}

func TestUncertainLifecycleWorkerCancellationStopsProbeChain(t *testing.T) {
	e, state := newLifecycleWakeTestEngine(t)
	ctx, cancel := context.WithCancel(context.Background())
	e.startLifecycle(ctx)
	defer func() {
		cancel()
		e.bgWG.Wait()
	}()

	e.signalUncertainLifecycle()
	waitLifecycleCount(t, &state.commitCount, state.commitNotify, 1)
	waitUncertainLifecycleProbesAtMost(t, e, lifecycleUncertainPasses-1)
	waitLifecycleWorker(t, e, cancel)
	if got := state.commitCount.Load(); got != 1 {
		t.Fatalf("cancellation allowed another uncertain lifecycle pass: got %d", got)
	}
}

func TestConcurrentUncertainLifecycleMarksCoalesceWithoutLoss(t *testing.T) {
	e := &Engine{lifecycleWake: make(chan struct{}, 1)}
	const callers = 128
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			<-start
			e.signalUncertainLifecycle()
		}()
	}
	close(start)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent uncertain lifecycle markers blocked")
	}
	if got := e.uncertainLifecycleProbes.Load(); got != lifecycleUncertainPasses {
		t.Fatalf("uncertain probe budget=%d, want %d", got, lifecycleUncertainPasses)
	}
	if got := len(e.lifecycleWake); got != 1 {
		t.Fatalf("coalesced uncertain wake count=%d, want 1", got)
	}
	for i := int32(0); i < lifecycleUncertainPasses; i++ {
		if !e.takeUncertainLifecycleProbe() {
			t.Fatalf("uncertain probe %d/%d was lost", i+1, lifecycleUncertainPasses)
		}
	}
	if e.takeUncertainLifecycleProbe() {
		t.Fatal("uncertain probe budget underflowed")
	}

	// A marker racing the worker's consume is linearizable in either order: the
	// consumer leaves 29 when it wins last, or the marker restores 30 when it
	// wins last. A stale load/store implementation could erase the new window and
	// leave zero; repeat the overlap to make that regression visible under -race.
	for round := 0; round < 1000; round++ {
		e.uncertainLifecycleProbes.Store(1)
		select {
		case <-e.lifecycleWake:
		default:
		}
		start := make(chan struct{})
		var overlap sync.WaitGroup
		overlap.Add(2)
		go func() {
			defer overlap.Done()
			<-start
			e.signalUncertainLifecycle()
		}()
		go func() {
			defer overlap.Done()
			<-start
			e.takeUncertainLifecycleProbe()
		}()
		close(start)
		overlap.Wait()
		got := e.uncertainLifecycleProbes.Load()
		if got != lifecycleUncertainPasses && got != lifecycleUncertainPasses-1 {
			t.Fatalf("round %d: concurrent mark/take left %d probes, want %d or %d",
				round, got, lifecycleUncertainPasses-1, lifecycleUncertainPasses)
		}
	}
}

func TestDeleteOutcomeUnknownSchedulesLifecycleReconciliation(t *testing.T) {
	d, state := remoteDBFailingFirst(t, 0)
	state.commitErr = io.EOF
	e := fakeRemoteEngine(d)
	e.lifecycleWake = make(chan struct{}, 1)

	result, err := e.DeleteQueueWithOptions(context.Background(), "q", DeleteOptions{Force: true})
	if !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("delete result=%+v error=%v, want ErrOutcomeUnknown", result, err)
	}
	if got := e.uncertainLifecycleProbes.Load(); got != lifecycleUncertainPasses {
		t.Fatalf("delete outcome-unknown probe budget=%d, want %d", got, lifecycleUncertainPasses)
	}
	if got := len(e.lifecycleWake); got != 1 {
		t.Fatalf("delete outcome-unknown wake count=%d, want 1", got)
	}
}

func TestMaintenanceOutcomeUnknownSchedulesLifecycleReconciliation(t *testing.T) {
	d, state := remoteDBFailingFirst(t, 0)
	state.commitErr = io.EOF
	e := fakeRemoteEngine(d)
	e.lifecycleWake = make(chan struct{}, 1)

	if retry := e.maintainLifecyclePass(context.Background()); !retry {
		t.Fatal("maintenance outcome-unknown did not request a retry")
	}
	if got := e.uncertainLifecycleProbes.Load(); got != lifecycleUncertainPasses {
		t.Fatalf("maintenance outcome-unknown probe budget=%d, want %d", got, lifecycleUncertainPasses)
	}
	if got := len(e.lifecycleWake); got != 1 {
		t.Fatalf("maintenance outcome-unknown wake count=%d, want 1", got)
	}
}

func TestPendingDeleteWakesLifecycleWorker(t *testing.T) {
	ctx := context.Background()
	e, err := Open(ctx, Options{DB: ":memory:"})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	mustQueue(t, e, "pending", QueueConfig{})
	insertLifecycleMessages(t, e, "pending", lifecycleBatchRows+17, 1)

	result, err := e.DeleteQueueWithOptions(ctx, "pending", DeleteOptions{Force: true})
	if err != nil || result.Deleted || result.State != "deleting" {
		t.Fatalf("force delete=%+v, %v", result, err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, ok := queueLifecycleState(t, e, "pending"); !ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("event-driven lifecycle worker did not finish an accepted pending delete")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
