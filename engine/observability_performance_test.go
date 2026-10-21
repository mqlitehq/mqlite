package engine

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestStatsQueryPlanUsesStateIndexes(t *testing.T) {
	e, _ := testEngine(t)
	q := "q"
	plan := explainPlan(t, e, queueMetricsSQL, q, q, q, q, q, q, q, int64(1))
	for _, index := range []string{
		"idx_msg_active", "idx_msg_locked_head", "idx_msg_deferred",
		"idx_msg_sched_head", "idx_msg_completed", "idx_msg_dlq",
	} {
		if !strings.Contains(plan, index) {
			t.Errorf("Stats no longer uses %s:\n%s", index, plan)
		}
	}
	if strings.Contains(plan, "SCAN messages") || strings.Contains(plan, "USE TEMP B-TREE") {
		t.Fatalf("Stats regained an unbounded message scan or sort:\n%s", plan)
	}
}

func TestGlobalObservationStateAggregationSemantics(t *testing.T) {
	e, clock := testEngine(t)
	ctx := context.Background()
	base := *clock

	for _, name := range []string{"mixed", "locked-only", "non-age", "empty", "deleting"} {
		mustQueue(t, e, name, QueueConfig{})
	}
	if err := e.Subscribe(ctx, "topic", "live-sub", nil); err != nil {
		t.Fatal(err)
	}
	if err := e.SubscribeWithOptions(ctx, "topic", "expired-sub", nil, SubscribeOptions{TTLMs: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.db.exec(ctx, `UPDATE queues SET lifecycle_state='deleting',delete_started_at=? WHERE name='deleting'`, base); err != nil {
		t.Fatal(err)
	}

	insert := func(queue, state string, enqueuedAt int64) {
		t.Helper()
		if _, err := e.db.exec(ctx,
			`INSERT INTO messages(queue,state,enqueued_at,body) VALUES(?,?,?,x'')`,
			queue, state, enqueuedAt); err != nil {
			t.Fatal(err)
		}
	}
	for _, row := range []struct {
		queue, state string
		enqueuedAt   int64
	}{
		{"mixed", "active", base - 100},
		{"mixed", "active", base - 300},
		{"mixed", "locked", base - 200},
		{"mixed", "locked", base - 400},
		{"mixed", "deferred", base - 10_000},
		{"mixed", "scheduled", base - 20_000},
		{"mixed", "completed", base - 30_000},
		{"mixed", "dead_lettered", base - 40_000},
		{"locked-only", "locked", base - 250},
		{"non-age", "deferred", base - 50_000},
		{"deleting", "active", base - 60_000},
		{"expired-sub", "active", base - 70_000},
	} {
		insert(row.queue, row.state, row.enqueuedAt)
	}
	advance(clock, time.Millisecond)

	rows, sampledAt, err := e.queueSnapshot(ctx, nil)
	if err != nil || sampledAt != *clock {
		t.Fatalf("snapshot sampled_at=%d want=%d err=%v", sampledAt, *clock, err)
	}
	got := make(map[string]QueueObservation, len(rows))
	for _, row := range rows {
		got[row.Queue] = row
	}
	if len(got) != 5 {
		t.Fatalf("active inventory=%+v", rows)
	}
	if _, ok := got["deleting"]; ok {
		t.Fatalf("deleting queue remained observable: %+v", rows)
	}
	if _, ok := got["expired-sub"]; ok {
		t.Fatalf("expired subscription remained observable: %+v", rows)
	}
	mixed := got["mixed"]
	if mixed.Active != 2 || mixed.Locked != 2 || mixed.Deferred != 1 || mixed.Scheduled != 1 ||
		mixed.DeadLettered != 1 || mixed.Total != 8 || mixed.Unexpected != 1 || mixed.OldestMessageAgeMs != 401 {
		t.Fatalf("mixed queue=%+v", mixed)
	}
	if locked := got["locked-only"]; locked.Total != 1 || locked.Locked != 1 || locked.OldestMessageAgeMs != 251 {
		t.Fatalf("locked-only queue=%+v", locked)
	}
	if nonAge := got["non-age"]; nonAge.Total != 1 || nonAge.Deferred != 1 || nonAge.OldestMessageAgeMs != 0 {
		t.Fatalf("non-age queue=%+v", nonAge)
	}
	if empty := got["empty"]; empty.Total != 0 || empty.OldestMessageAgeMs != 0 {
		t.Fatalf("empty queue=%+v", empty)
	}
	if sub := got["live-sub"]; sub.Kind != "subscription" || sub.Total != 0 {
		t.Fatalf("live subscription=%+v", sub)
	}
}

func TestLocalFileObservationReaderIsSeparateAndQueryOnly(t *testing.T) {
	e, _ := testEngine(t)
	ctx := context.Background()
	mustQueue(t, e, "wal-frame", QueueConfig{})
	if e.db.reader == nil || e.db.reader.Stats().MaxOpenConnections != 1 {
		t.Fatalf("local file observation reader = %#v, want one connection", e.db.reader)
	}
	if got := e.db.poolStats().MaxOpenConnections; got != 2 {
		t.Fatalf("aggregate pool max_open=%d, want writer+reader=2", got)
	}
	conn, err := e.db.reader.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var queryOnly int
	if err := conn.QueryRowContext(ctx, `PRAGMA query_only`).Scan(&queryOnly); err != nil || queryOnly != 1 {
		t.Fatalf("query_only=%d err=%v", queryOnly, err)
	}
	var one int
	if err := conn.QueryRowContext(ctx, `SELECT 1`).Scan(&one); err != nil || one != 1 {
		t.Fatalf("read-only observation SELECT=%d err=%v", one, err)
	}
	if _, err := conn.ExecContext(ctx, `CREATE TABLE observation_reader_must_not_write (id INTEGER)`); err == nil {
		t.Fatal("observation reader accepted a write")
	}
	var busy, logPages, checkpointed int
	if err := conn.QueryRowContext(ctx, `PRAGMA wal_checkpoint(PASSIVE)`).Scan(&busy, &logPages, &checkpointed); err == nil {
		t.Fatalf("read-only observation reader accepted a checkpoint: busy=%d log=%d checkpointed=%d", busy, logPages, checkpointed)
	}
	var queues int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM queues`).Scan(&queues); err != nil || queues != 1 {
		t.Fatalf("reader was unusable after rejected checkpoint: queues=%d err=%v", queues, err)
	}

	// Keep a read snapshot open. WAL must still let the sole writer commit; this
	// is the isolation that prevents a deep Observe from occupying its connection.
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var n int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- e.CreateQueue(ctx, "writer-progress", QueueConfig{}) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("writer was blocked by an open observation snapshot")
	}
}

func TestMemoryObservationStaysOnSingleConnection(t *testing.T) {
	e, err := Open(context.Background(), Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if e.db.reader != nil || e.db.poolStats().MaxOpenConnections != 1 {
		t.Fatalf("memory reader=%#v max_open=%d, want sole connection", e.db.reader, e.db.poolStats().MaxOpenConnections)
	}
	if runtime, _ := e.runtimeObservation(context.Background()); !runtime.ReadAvailable {
		t.Fatal("memory runtime probe did not fall back to the sole connection")
	}
}

func TestObserveCompletesWhileLocalWriterConnectionIsOccupied(t *testing.T) {
	e, _ := testEngine(t)
	ctx := context.Background()
	mustQueue(t, e, "q", QueueConfig{})

	// Occupy the sole writer connection without touching the observation reader.
	// Both the runtime SELECT 1 probe and the global gauge scan must still finish.
	writer, err := e.db.sql.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })

	done := make(chan ObservabilitySnapshot, 1)
	go func() { done <- e.Observability(ctx) }()
	select {
	case snapshot := <-done:
		if snapshot.Collection.State != "available" || !snapshot.Runtime.ReadAvailable || len(snapshot.Queues) != 1 {
			t.Fatalf("observation while writer occupied: %+v", snapshot)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Observe waited for the occupied writer connection")
	}
}

func TestGlobalObservationCacheKeepsAtomicFreshnessAndCopies(t *testing.T) {
	e, clock := testEngine(t)
	ctx := context.Background()
	mustQueue(t, e, "q", QueueConfig{})
	if _, err := e.SendOne(ctx, "q", OutMessage{}); err != nil {
		t.Fatal(err)
	}

	first, firstSample, firstEpoch, firstDuration, err := e.globalQueueSnapshot(ctx)
	if err != nil || len(first) != 1 || first[0].Active != 1 || firstSample != *clock {
		t.Fatalf("first snapshot=%+v sample=%d err=%v", first, firstSample, err)
	}
	first[0].Active = 999 // a caller must not be able to mutate the cached rows

	advance(clock, time.Second)
	if _, err := e.SendOne(ctx, "q", OutMessage{}); err != nil {
		t.Fatal(err)
	}
	cached, cachedSample, cachedEpoch, cachedDuration, err := e.globalQueueSnapshot(ctx)
	if err != nil || len(cached) != 1 || cached[0].Active != 1 || cachedSample != firstSample || cachedEpoch != firstEpoch || cachedDuration != firstDuration {
		t.Fatalf("cache changed or was mutated: rows=%+v sample=%d/%d epoch=%d/%d duration=%g/%g err=%v",
			cached, cachedSample, firstSample, cachedEpoch, firstEpoch, cachedDuration, firstDuration, err)
	}
	e.observation.mu.Lock()
	lastSuccess := e.observation.lastSuccess
	e.observation.mu.Unlock()
	if lastSuccess != firstSample {
		t.Fatalf("cache hit moved last success: %d, want %d", lastSuccess, firstSample)
	}

	stats, err := e.Stats(ctx, "q")
	if err != nil || stats.Active != 2 {
		t.Fatalf("Stats was cached with Observe: %+v err=%v", stats, err)
	}

	// A queue topology callback invalidates the cache even before its time TTL.
	advance(clock, time.Second)
	mustQueue(t, e, "q2", QueueConfig{})
	topology, topologySample, topologyEpoch, _, err := e.globalQueueSnapshot(ctx)
	if err != nil || len(topology) != 2 || topologySample != *clock || topologyEpoch != e.observationEpoch() {
		t.Fatalf("topology change reused stale cache: rows=%+v sample=%d epoch=%d current=%d err=%v",
			topology, topologySample, topologyEpoch, e.observationEpoch(), err)
	}
	if _, err := e.SendOne(ctx, "q", OutMessage{}); err != nil {
		t.Fatal(err)
	}

	// Expire the process-time TTL directly instead of making the test sleep ten
	// seconds. The next call must collect new rows and a new engine timestamp.
	e.observation.mu.Lock()
	e.observation.queueCache.expiresAt = time.Now().Add(-time.Second)
	e.observation.mu.Unlock()
	advance(clock, time.Second)
	fresh, freshSample, _, _, err := e.globalQueueSnapshot(ctx)
	if err != nil || freshSample != *clock || freshSample == topologySample || len(fresh) != 2 || fresh[0].Active != 3 {
		t.Fatalf("expired cache did not refresh: rows=%+v sample=%d clock=%d err=%v", fresh, freshSample, *clock, err)
	}
	e.observation.mu.Lock()
	lastSuccess = e.observation.lastSuccess
	e.observation.mu.Unlock()
	if lastSuccess != freshSample {
		t.Fatalf("fresh collection did not publish last success: %d, want %d", lastSuccess, freshSample)
	}
}

func queueObservationByName(rows []QueueObservation, name string) (QueueObservation, bool) {
	for _, row := range rows {
		if row.Queue == name {
			return row, true
		}
	}
	return QueueObservation{}, false
}

func TestLogicalDeleteInvalidatesGlobalObservationCache(t *testing.T) {
	e, _ := testEngine(t)
	ctx := context.Background()
	mustQueue(t, e, "keep", QueueConfig{})
	mustQueue(t, e, "victim", QueueConfig{})
	insertLifecycleMessages(t, e, "victim", lifecycleBatchRows+1, 1)

	primed, _, beforeEpoch, _, err := e.globalQueueSnapshot(ctx)
	if victim, ok := queueObservationByName(primed, "victim"); err != nil || !ok || victim.Active != lifecycleBatchRows+1 {
		t.Fatalf("primed snapshot victim=%+v present=%t err=%v", victim, ok, err)
	}
	result, err := e.DeleteQueueWithOptions(ctx, "victim", DeleteOptions{Force: true})
	if err != nil || result.Deleted || result.State != "deleting" {
		t.Fatalf("logical delete=%+v err=%v", result, err)
	}
	afterTransition := e.observationEpoch()
	if afterTransition <= beforeEpoch {
		t.Fatalf("logical deletion did not advance observation epoch: before=%d after=%d", beforeEpoch, afterTransition)
	}

	refreshed, _, refreshedEpoch, _, err := e.globalQueueSnapshot(ctx)
	if _, ok := queueObservationByName(refreshed, "victim"); err != nil || ok || refreshedEpoch != afterTransition {
		t.Fatalf("post-delete snapshot=%+v epoch=%d/%d err=%v", refreshed, refreshedEpoch, afterTransition, err)
	}
	if _, ok := queueObservationByName(refreshed, "keep"); !ok {
		t.Fatalf("post-delete snapshot lost live queue: %+v", refreshed)
	}
	if state, ok := queueLifecycleState(t, e, "victim"); !ok || state != "deleting" {
		t.Fatalf("deleting inventory state=(%q,%t)", state, ok)
	}
	if got := e.observationEpoch(); got != afterTransition {
		t.Fatalf("deleting ListQueues inventory reintroduced the retired dimension: got=%d want=%d", got, afterTransition)
	}

	// Metadata removal is physical cleanup, not another observable topology
	// transition. Its fallback forget is deliberately idempotent.
	e.maintainLifecycle(ctx)
	if _, ok := queueLifecycleState(t, e, "victim"); ok {
		t.Fatal("bounded follow-up did not finish victim cleanup")
	}
	if got := e.observationEpoch(); got != afterTransition {
		t.Fatalf("physical cleanup advanced observation epoch: got=%d want=%d", got, afterTransition)
	}
}

func TestLogicalTransitionsFenceInFlightObservationAndNewWaiter(t *testing.T) {
	for _, transition := range []string{"force_delete", "ttl_expiry"} {
		t.Run(transition, func(t *testing.T) {
			e, clock := testEngine(t)
			ctx := context.Background()
			mustQueue(t, e, "keep", QueueConfig{})
			if transition == "force_delete" {
				// Model an active resource discovered through storage rather than this
				// process's creation callback. Its transition must still fence the scan.
				if _, err := e.db.exec(ctx, `INSERT INTO queues(name,created_at,updated_at) VALUES('victim',1,1)`); err != nil {
					t.Fatal(err)
				}
				insertLifecycleMessages(t, e, "victim", lifecycleBatchRows+1, 1)
			} else if err := e.SubscribeWithOptions(ctx, "topic", "victim", nil, SubscribeOptions{TTLMs: 1000}); err != nil {
				t.Fatal(err)
			}

			collected := make(chan struct{})
			release := make(chan struct{})
			joined := make(chan struct{})
			var collectOnce, joinOnce sync.Once
			afterQueueSnapshotCollected = func() {
				collectOnce.Do(func() {
					close(collected)
					<-release
				})
			}
			afterGlobalQueueFlightJoin = func() { joinOnce.Do(func() { close(joined) }) }
			defer func() {
				afterQueueSnapshotCollected = nil
				afterGlobalQueueFlightJoin = nil
			}()

			type snapshotResult struct {
				rows  []QueueObservation
				epoch uint64
				err   error
			}
			leader := make(chan snapshotResult, 1)
			go func() {
				rows, _, epoch, _, err := e.globalQueueSnapshot(ctx)
				leader <- snapshotResult{rows: rows, epoch: epoch, err: err}
			}()
			select {
			case <-collected:
			case <-time.After(2 * time.Second):
				t.Fatal("observation did not reach the collected-snapshot boundary")
			}
			beforeTransition := e.observationEpoch()
			if transition == "force_delete" {
				result, err := e.DeleteQueueWithOptions(ctx, "victim", DeleteOptions{Force: true})
				if err != nil || result.Deleted || result.State != "deleting" {
					t.Fatalf("logical delete=%+v err=%v", result, err)
				}
			} else {
				advance(clock, time.Second)
				changed, _, err := e.transitionExpiredSubscriptionsPass(ctx)
				if err != nil || changed != 1 {
					t.Fatalf("TTL transition changed=%d err=%v", changed, err)
				}
			}
			currentEpoch := e.observationEpoch()
			if currentEpoch <= beforeTransition {
				t.Fatalf("%s did not advance observation epoch: before=%d after=%d", transition, beforeTransition, currentEpoch)
			}

			waiter := make(chan snapshotResult, 1)
			go func() {
				rows, _, epoch, _, err := e.globalQueueSnapshot(ctx)
				waiter <- snapshotResult{rows: rows, epoch: epoch, err: err}
			}()
			select {
			case <-joined:
			case <-time.After(2 * time.Second):
				t.Fatal("post-transition caller did not join the older flight")
			}
			close(release)

			old := <-leader
			if victim, ok := queueObservationByName(old.rows, "victim"); old.err != nil || !ok || old.epoch == currentEpoch {
				t.Fatalf("pre-transition flight victim=%+v present=%t epoch=%d/%d err=%v", victim, ok, old.epoch, currentEpoch, old.err)
			}
			fresh := <-waiter
			if _, ok := queueObservationByName(fresh.rows, "victim"); fresh.err != nil || ok || fresh.epoch != currentEpoch {
				t.Fatalf("post-transition waiter rows=%+v epoch=%d/%d err=%v", fresh.rows, fresh.epoch, currentEpoch, fresh.err)
			}
			if _, ok := queueObservationByName(fresh.rows, "keep"); !ok {
				t.Fatalf("post-transition waiter lost live queue: %+v", fresh.rows)
			}
			e.observation.mu.Lock()
			cached := e.observation.queueCache
			e.observation.mu.Unlock()
			if cached == nil || cached.epoch != currentEpoch {
				t.Fatalf("fresh inventory was not cached at current epoch: %+v", cached)
			}
		})
	}
}

func awaitGlobalObservationFlight(t *testing.T, e *Engine) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		e.observation.mu.Lock()
		inFlight := e.observation.queueFlight != nil
		e.observation.mu.Unlock()
		if inFlight {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("global observation refresh did not start")
}

func observationReadCount(e *Engine) uint64 {
	var count uint64
	for _, op := range e.db.storage.snapshot(e.db.poolStats()).Operations {
		if op.Operation == "read" {
			count += op.Duration.Count
		}
	}
	return count
}

func TestGlobalObservationSingleflightAndFailureIsNotCached(t *testing.T) {
	e, _ := testEngine(t)
	ctx := context.Background()
	mustQueue(t, e, "q", QueueConfig{})

	held, err := e.db.reader.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	heldOpen := true
	t.Cleanup(func() {
		if heldOpen {
			_ = held.Close()
		}
	})

	ownerCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	owner := make(chan error, 1)
	go func() {
		_, _, _, _, err := e.globalQueueSnapshot(ownerCtx)
		owner <- err
	}()
	awaitGlobalObservationFlight(t, e)

	joined := make(chan error, 1)
	go func() {
		rows, _, _, _, err := e.globalQueueSnapshot(ctx)
		if err == nil && len(rows) != 2 {
			err = errors.New("unexpected queue snapshot length")
		}
		joined <- err
	}()
	if err := <-owner; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("refresh owner error=%v, want deadline", err)
	}
	e.observation.mu.Lock()
	cachedFailure := e.observation.queueCache != nil
	e.observation.mu.Unlock()
	if cachedFailure {
		t.Fatal("failed global observation was cached")
	}
	// The live waiter retries after the timed-out leader. Change topology while
	// that retry is blocked: its captured fence is now old, so the completed scan
	// may satisfy this call but must not become the 10-second cache entry.
	awaitGlobalObservationFlight(t, e)
	mustQueue(t, e, "raced", QueueConfig{})

	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	heldOpen = false
	if err := <-joined; err != nil {
		t.Fatalf("live waiter inherited leader cancellation: %v", err)
	}
	e.observation.mu.Lock()
	cacheInstalled := e.observation.queueCache != nil
	e.observation.mu.Unlock()
	if cacheInstalled {
		t.Fatal("refresh that raced a topology change was cached")
	}
	rows, _, epoch, _, err := e.globalQueueSnapshot(ctx)
	if err != nil || len(rows) != 2 || epoch != e.observationEpoch() {
		t.Fatalf("post-race refresh rows=%+v epoch=%d current=%d err=%v", rows, epoch, e.observationEpoch(), err)
	}
}

func TestConcurrentGlobalObservationUsesOneScan(t *testing.T) {
	e, _ := testEngine(t)
	ctx := context.Background()
	mustQueue(t, e, "q", QueueConfig{})
	held, err := e.db.reader.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	before := observationReadCount(e)
	const callers = 24
	start := make(chan struct{})
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func() {
			<-start
			rows, _, _, _, err := e.globalQueueSnapshot(ctx)
			if err == nil && len(rows) != 1 {
				err = errors.New("unexpected queue snapshot length")
			}
			errs <- err
		}()
	}
	close(start)
	awaitGlobalObservationFlight(t, e)
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < callers; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if delta := observationReadCount(e) - before; delta != 1 {
		t.Fatalf("concurrent global observations executed %d scans, want 1", delta)
	}
}

func TestCloseDuringGlobalObservationRefresh(t *testing.T) {
	e, _ := testEngine(t)
	ctx := context.Background()
	mustQueue(t, e, "q", QueueConfig{})
	held, err := e.db.reader.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	waits := e.db.reader.Stats().WaitCount
	leader := make(chan error, 1)
	go func() {
		_, _, _, _, err := e.globalQueueSnapshot(ctx)
		leader <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for e.db.reader.Stats().WaitCount == waits && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if e.db.reader.Stats().WaitCount == waits {
		_ = held.Close()
		t.Fatal("global refresh was not admitted while waiting for the reader")
	}

	joined := make(chan error, 1)
	go func() {
		_, _, _, _, err := e.globalQueueSnapshot(ctx)
		joined <- err
	}()
	closed := make(chan error, 1)
	go func() { closed <- e.Close() }()
	awaitClosing(t, e)
	if _, _, _, _, err := e.globalQueueSnapshot(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("new global observation during Close=%v, want ErrClosed", err)
	}
	select {
	case err := <-joined:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("joined observation during Close=%v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		_ = held.Close()
		t.Fatal("joined observation did not wake when Close began")
	}
	select {
	case err := <-closed:
		t.Fatalf("Close returned before the admitted scan was released: %v", err)
	default:
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-leader; err != nil {
		t.Fatalf("admitted refresh failed during Close: %v", err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := e.globalQueueSnapshot(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("cached global observation after Close=%v, want ErrClosed", err)
	}
}

func TestObservationReaderWaitCancellationAndClose(t *testing.T) {
	e, _ := testEngine(t)
	ctx := context.Background()
	mustQueue(t, e, "q", QueueConfig{})
	held, err := e.db.reader.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = held.Close() })

	waiting, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	_, err = e.Stats(waiting, "q")
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("reader wait ignored cancellation: %v", err)
	}

	before := e.db.reader.Stats().WaitCount
	admitted := make(chan error, 1)
	go func() { _, err := e.Stats(ctx, "q"); admitted <- err }()
	deadline := time.Now().Add(2 * time.Second)
	for e.db.reader.Stats().WaitCount == before && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if e.db.reader.Stats().WaitCount == before {
		t.Fatal("Stats did not wait for the observation reader")
	}
	closed := make(chan error, 1)
	go func() { closed <- e.Close() }()
	awaitClosing(t, e)
	if _, err := e.Stats(ctx, "q"); !errors.Is(err, ErrClosed) {
		t.Fatalf("new Stats during Close = %v, want ErrClosed", err)
	}
	select {
	case err := <-closed:
		t.Fatalf("Close returned before admitted observation: %v", err)
	default:
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-admitted; err != nil {
		t.Fatalf("admitted Stats failed during Close: %v", err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
}

func seedObservationBenchmarkStates(b *testing.B, messages, queues int, mixed bool) *Engine {
	b.Helper()
	ctx := context.Background()
	e := benchEngine(b)
	if _, err := e.db.exec(ctx, `INSERT INTO queues(name,created_at,updated_at) VALUES('hot',1,1)`); err != nil {
		b.Fatal(err)
	}
	if _, err := e.db.exec(ctx, `WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<?)
		INSERT INTO queues(name,created_at,updated_at) SELECT printf('q%03d',i),1,1 FROM n`, queues-1); err != nil {
		b.Fatal(err)
	}
	if _, err := e.db.exec(ctx, `WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<?)
		INSERT INTO messages(queue,state,enqueued_at,body)
		SELECT printf('q%03d',i%?),
			CASE WHEN ?=0 THEN 'active' ELSE CASE i%6
				WHEN 0 THEN 'active' WHEN 1 THEN 'locked' WHEN 2 THEN 'deferred'
				WHEN 3 THEN 'scheduled' WHEN 4 THEN 'completed' ELSE 'dead_lettered' END END,
			1700000000000,x'00' FROM n`, messages-1, queues, mixed); err != nil {
		b.Fatal(err)
	}
	if _, err := e.db.exec(ctx, `ANALYZE`); err != nil {
		b.Fatal(err)
	}
	return e
}

func seedObservationBenchmark(b *testing.B, messages, queues int) *Engine {
	b.Helper()
	return seedObservationBenchmarkStates(b, messages, queues, false)
}

// BenchmarkObservationDeepBacklog keeps the 300k-row reproduction available
// without adding a large fixture to the repository.
func BenchmarkObservationDeepBacklog(b *testing.B) {
	const messages, queues = 300_000, 300
	b.Run("StatsSparseQueue", func(b *testing.B) {
		e := seedObservationBenchmark(b, messages, queues)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := e.Stats(context.Background(), "q000"); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("ObserveAllQueuesRefresh", func(b *testing.B) {
		e := seedObservationBenchmark(b, messages, queues)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			e.observation.mu.Lock()
			if e.observation.queueCache != nil {
				e.observation.queueCache.expiresAt = time.Time{}
			}
			e.observation.mu.Unlock()
			if s := e.Observability(context.Background()); s.Collection.State != "available" {
				b.Fatal(s.Collection)
			}
		}
	})
	b.Run("ObserveAllQueuesRefreshMixedStates", func(b *testing.B) {
		e := seedObservationBenchmarkStates(b, messages, queues, true)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			e.observation.mu.Lock()
			if e.observation.queueCache != nil {
				e.observation.queueCache.expiresAt = time.Time{}
			}
			e.observation.mu.Unlock()
			if s := e.Observability(context.Background()); s.Collection.State != "available" {
				b.Fatal(s.Collection)
			}
		}
	})
	b.Run("ObserveAllQueuesCacheHit", func(b *testing.B) {
		e := seedObservationBenchmark(b, messages, queues)
		if s := e.Observability(context.Background()); s.Collection.State != "available" {
			b.Fatal(s.Collection)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if s := e.Observability(context.Background()); s.Collection.State != "available" {
				b.Fatal(s.Collection)
			}
		}
	})
}

func BenchmarkSendDuringDeepObservation(b *testing.B) {
	for _, observing := range []bool{false, true} {
		name := "baseline"
		if observing {
			name = "continuous_observe"
		}
		b.Run(name, func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			e := seedObservationBenchmark(b, 300_000, 300)
			var observer sync.WaitGroup
			stopObserver := func() {
				cancel()
				observer.Wait()
			}
			b.Cleanup(stopObserver)
			if observing {
				observer.Add(1)
				go func() {
					defer observer.Done()
					for {
						select {
						case <-ctx.Done():
							return
						default:
							_ = e.Observability(ctx)
						}
					}
				}()
				refreshRunning := func() bool {
					e.observation.mu.Lock()
					inFlight := e.observation.queueFlight != nil
					e.observation.mu.Unlock()
					return inFlight && e.db.reader.Stats().InUse > 0
				}
				deadline := time.Now().Add(2 * time.Second)
				for !refreshRunning() && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}
				if !refreshRunning() {
					stopObserver()
					b.Fatal("observation scan did not start")
				}
			}
			body := []byte("x")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := e.SendOne(ctx, "hot", OutMessage{Body: body}); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msg/s")
		})
	}
}
