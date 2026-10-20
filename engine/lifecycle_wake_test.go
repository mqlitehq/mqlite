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
	case <-time.After(250 * time.Millisecond):
		t.Fatal("lifecycle worker did not stop promptly after cancellation")
	}
}

func waitLifecycleCount(t *testing.T, count *atomic.Int64, notify <-chan struct{}, want int64) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for count.Load() < want {
		select {
		case <-notify:
		case <-deadline.C:
			t.Fatalf("lifecycle count=%d, want at least %d", count.Load(), want)
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
	case <-time.After(250 * time.Millisecond):
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
	time.Sleep(1100 * time.Millisecond)
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
	case <-time.After(time.Second):
		t.Fatal("first lifecycle pass did not reach storage")
	}
	// A deletion can commit after this pass has taken its inventory snapshot.
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
	// Cancellation must interrupt that delay rather than making Close wait it out.
	waitLifecycleWorker(t, e, cancel)
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

	deadline := time.Now().Add(time.Second)
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
