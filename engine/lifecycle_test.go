package engine

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func lifecycleRows(t *testing.T, e *Engine, table, queue string) int {
	t.Helper()
	var count int
	if err := e.db.queryRowScan(context.Background(), []any{&count},
		`SELECT COUNT(*) FROM `+table+` WHERE queue=?`, queue); err != nil {
		t.Fatalf("count %s/%s: %v", table, queue, err)
	}
	return count
}

func insertLifecycleMessages(tb testing.TB, e *Engine, queue string, count, bodyBytes int) {
	tb.Helper()
	ctx := context.Background()
	for count > 0 {
		batch := count
		if batch > 1000 {
			batch = 1000
		}
		if _, err := e.db.exec(ctx, `WITH RECURSIVE n(i) AS (
			SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<?
		) INSERT INTO messages(queue,state,enqueued_at,body)
		  SELECT ?,'active',1,zeroblob(?) FROM n`, batch, queue, bodyBytes); err != nil {
			tb.Fatalf("insert %d lifecycle messages for %s: %v", batch, queue, err)
		}
		count -= batch
	}
}

func queueLifecycleState(t *testing.T, e *Engine, name string) (string, bool) {
	t.Helper()
	queues, err := e.ListQueues(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range queues {
		if q.Name == name {
			return q.State, true
		}
	}
	return "", false
}

func disableForeignKeys(t *testing.T, e *Engine) {
	t.Helper()
	ctx := context.Background()
	if _, err := e.db.exec(ctx, `PRAGMA foreign_keys=OFF`); err != nil {
		t.Fatalf("disable foreign keys: %v", err)
	}
	var enabled int
	if err := e.db.queryRowScan(ctx, []any{&enabled}, `PRAGMA foreign_keys`); err != nil {
		t.Fatalf("read foreign_keys pragma: %v", err)
	}
	if enabled != 0 {
		t.Fatalf("foreign_keys=%d, want disabled regression environment", enabled)
	}
}

func subscriptionMetadataRows(t *testing.T, e *Engine, name string) (mappings, queues int) {
	t.Helper()
	ctx := context.Background()
	if err := e.db.queryRowScan(ctx, []any{&mappings},
		`SELECT COUNT(*) FROM subscriptions WHERE subscription=?`, name); err != nil {
		t.Fatalf("count subscription mapping %q: %v", name, err)
	}
	if err := e.db.queryRowScan(ctx, []any{&queues},
		`SELECT COUNT(*) FROM queues WHERE name=?`, name); err != nil {
		t.Fatalf("count subscription queue %q: %v", name, err)
	}
	return mappings, queues
}

func TestDeleteSubscriptionExactEmptyAndForceSemantics(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	if err := e.Subscribe(ctx, "events", "worker", &Filter{Expr: "true"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.loadQueue(ctx, "worker"); err != nil {
		t.Fatal(err)
	}
	_ = e.compiledFilter("worker", "true")
	wake, release := e.note.subscribe("worker")
	defer release()

	for _, state := range []State{StateActive, StateLocked, StateScheduled, StateDeferred, StateCompleted, StateDeadLettered} {
		if _, err := e.db.exec(ctx,
			`INSERT INTO messages(queue,state,enqueued_at,body) VALUES(?,?,1,?)`,
			"worker", string(state), []byte("discard")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.db.exec(ctx,
		`INSERT INTO dedup(queue,message_id,request_hash,seq_number,seen_at) VALUES('worker','id','hash',1,1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := e.db.exec(ctx,
		`INSERT INTO settlement_receipts(lock_token,queue,seq_number,operation,args,created_at,expires_at)
		 VALUES('token','worker',1,'completed','',1,9999999999999)`); err != nil {
		t.Fatal(err)
	}
	if _, err := e.db.exec(ctx,
		`INSERT INTO receive_attempts(queue,attempt_id,response,created_at,expires_at)
		 VALUES('worker','attempt','[]',1,9999999999999)`); err != nil {
		t.Fatal(err)
	}

	if err := e.DeleteSubscription(ctx, "wrong-topic", "worker"); !errors.Is(err, ErrSubscriptionNotFound) {
		t.Fatalf("wrong topic delete=%v, want ErrSubscriptionNotFound", err)
	}
	if err := e.DeleteSubscription(ctx, "events", "worker"); !errors.Is(err, ErrSubscriptionNotEmpty) {
		t.Fatalf("non-empty delete=%v, want ErrSubscriptionNotEmpty", err)
	}
	result, err := e.DeleteSubscriptionWithOptions(ctx, "events", "worker", DeleteOptions{Force: true})
	if err != nil || !result.Deleted || result.State != "deleted" {
		t.Fatalf("force delete result=%+v, error=%v", result, err)
	}

	for _, table := range []string{"messages", "dedup", "settlement_receipts", "receive_attempts"} {
		if got := lifecycleRows(t, e, table, "worker"); got != 0 {
			t.Fatalf("%s retained %d rows", table, got)
		}
	}
	var mappings, queues int
	if err := e.db.queryRowScan(ctx, []any{&mappings},
		`SELECT COUNT(*) FROM subscriptions WHERE topic='events' AND subscription='worker'`); err != nil {
		t.Fatal(err)
	}
	if err := e.db.queryRowScan(ctx, []any{&queues},
		`SELECT COUNT(*) FROM queues WHERE name='worker'`); err != nil {
		t.Fatal(err)
	}
	if mappings != 0 || queues != 0 {
		t.Fatalf("retained metadata: mappings=%d queues=%d", mappings, queues)
	}
	if _, err := e.SendOne(ctx, "events", OutMessage{}); !errors.Is(err, ErrQueueNotFound) {
		t.Fatalf("last subscription left topic routable: %v", err)
	}
	if _, err := e.SendOne(ctx, "worker", OutMessage{}); !errors.Is(err, ErrQueueNotFound) {
		t.Fatalf("deleted backing queue accepted send: %v", err)
	}
	if err := e.DeleteSubscription(ctx, "events", "worker"); !errors.Is(err, ErrSubscriptionNotFound) {
		t.Fatalf("repeat delete=%v, want ErrSubscriptionNotFound", err)
	}

	select {
	case <-wake:
	default:
		t.Fatal("subscription deletion did not wake a registered receiver")
	}
	e.qmu.RLock()
	_, queueCached := e.qcache["worker"]
	e.qmu.RUnlock()
	e.filterMu.Lock()
	_, filterCached := e.filterCache["worker"]
	e.filterMu.Unlock()
	e.note.mu.Lock()
	_, notifierRetained := e.note.chans["worker"]
	e.note.mu.Unlock()
	if queueCached || filterCached || notifierRetained {
		t.Fatalf("retired in-memory state remains: queue=%t filter=%t notifier=%t", queueCached, filterCached, notifierRetained)
	}

	if err := e.Subscribe(ctx, "empty-topic", "empty", nil); err != nil {
		t.Fatal(err)
	}
	if err := e.DeleteSubscription(ctx, "empty-topic", "empty"); err != nil {
		t.Fatalf("empty subscription delete: %v", err)
	}
}

func TestSubscriptionDeletionDoesNotDependOnForeignKeys(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	disableForeignKeys(t, e)

	if err := e.Subscribe(ctx, "empty-topic", "empty-sub", nil); err != nil {
		t.Fatal(err)
	}
	result, err := e.DeleteSubscriptionWithOptions(ctx, "empty-topic", "empty-sub", DeleteOptions{})
	if err != nil || !result.Deleted || result.State != "deleted" {
		t.Fatalf("empty delete with foreign keys off=%+v, error=%v", result, err)
	}
	if mappings, queues := subscriptionMetadataRows(t, e, "empty-sub"); mappings != 0 || queues != 0 {
		t.Fatalf("empty delete retained metadata with foreign keys off: mappings=%d queues=%d", mappings, queues)
	}

	if err := e.Subscribe(ctx, "forced-topic", "forced-sub", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := e.SendOne(ctx, "forced-topic", OutMessage{Body: []byte("discard")}); err != nil {
		t.Fatal(err)
	}
	if err := e.DeleteSubscription(ctx, "forced-topic", "forced-sub"); !errors.Is(err, ErrSubscriptionNotEmpty) {
		t.Fatalf("non-force delete with foreign keys off=%v, want ErrSubscriptionNotEmpty", err)
	}
	result, err = e.DeleteSubscriptionWithOptions(ctx, "forced-topic", "forced-sub", DeleteOptions{Force: true})
	if err != nil || !result.Deleted || result.State != "deleted" {
		t.Fatalf("forced delete with foreign keys off=%+v, error=%v", result, err)
	}
	if mappings, queues := subscriptionMetadataRows(t, e, "forced-sub"); mappings != 0 || queues != 0 {
		t.Fatalf("forced delete retained metadata with foreign keys off: mappings=%d queues=%d", mappings, queues)
	}
}

func TestSubscriptionBackgroundCleanupDoesNotDependOnForeignKeys(t *testing.T) {
	ctx := context.Background()
	e, err := Open(ctx, Options{DB: "file:" + filepath.Join(t.TempDir(), "background-foreign-keys-off.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	disableForeignKeys(t, e)
	if err := e.Subscribe(ctx, "background-topic", "background-sub", nil); err != nil {
		t.Fatal(err)
	}
	insertLifecycleMessages(t, e, "background-sub", lifecycleBatchRows+17, 1)
	result, err := e.DeleteSubscriptionWithOptions(ctx, "background-topic", "background-sub", DeleteOptions{Force: true})
	if err != nil || result.Deleted || result.State != "deleting" {
		t.Fatalf("bounded foreground delete=%+v, error=%v", result, err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		mappings, queues := subscriptionMetadataRows(t, e, "background-sub")
		if mappings == 0 && queues == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("background cleanup retained metadata with foreign keys off: mappings=%d queues=%d", mappings, queues)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestSubscriptionRestartCleanupDoesNotDependOnForeignKeys(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "restart-foreign-keys-off.db")
	opts := Options{DB: "file:" + path, DisableBackground: true}
	e, err := Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	disableForeignKeys(t, e)
	if err := e.Subscribe(ctx, "restart-topic", "restart-sub", nil); err != nil {
		t.Fatal(err)
	}
	insertLifecycleMessages(t, e, "restart-sub", lifecycleBatchRows+lifecyclePassRows+17, 1)
	result, err := e.DeleteSubscriptionWithOptions(ctx, "restart-topic", "restart-sub", DeleteOptions{Force: true})
	if err != nil || result.Deleted || result.State != "deleting" {
		t.Fatalf("bounded foreground delete=%+v, error=%v", result, err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e, err = Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if got := lifecycleRows(t, e, "messages", "restart-sub"); got != 17 {
		t.Fatalf("startup cleanup remaining=%d, want 17", got)
	}
	disableForeignKeys(t, e)
	e.RunMaintenanceOnce(ctx)
	if mappings, queues := subscriptionMetadataRows(t, e, "restart-sub"); mappings != 0 || queues != 0 {
		t.Fatalf("restart cleanup retained metadata with foreign keys off: mappings=%d queues=%d", mappings, queues)
	}
}

func TestSubscriptionFinalMetadataConsumesTwoRows(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	disableForeignKeys(t, e)
	if err := e.Subscribe(ctx, "topic", "sub", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := e.db.exec(ctx,
		`UPDATE queues SET lifecycle_state='deleting',delete_started_at=1 WHERE name='sub'`); err != nil {
		t.Fatal(err)
	}

	var firstDone bool
	var firstBudget cleanupBudget
	if err := e.inTx(ctx, func(ctx context.Context, tx *txn) error {
		firstBudget = cleanupBudget{rows: 1, bytes: lifecycleBatchBytes}
		var err error
		firstDone, err = e.cleanEntityTx(ctx, tx, "sub", "subscription", &firstBudget)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if firstDone || firstBudget.rows != 1 {
		t.Fatalf("one-row budget retired two metadata rows: done=%t budget=%+v", firstDone, firstBudget)
	}
	if state, ok := queueLifecycleState(t, e, "sub"); !ok || state != "deleting" {
		t.Fatalf("subscription reservation state=(%q,%t)", state, ok)
	}

	var secondDone bool
	var secondBudget cleanupBudget
	if err := e.inTx(ctx, func(ctx context.Context, tx *txn) error {
		secondBudget = cleanupBudget{rows: 2, bytes: lifecycleBatchBytes}
		var err error
		secondDone, err = e.cleanEntityTx(ctx, tx, "sub", "subscription", &secondBudget)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if !secondDone || secondBudget.rows != 0 {
		t.Fatalf("two-row metadata retirement: done=%t budget=%+v", secondDone, secondBudget)
	}
	var mappings int
	if err := e.db.queryRowScan(ctx, []any{&mappings},
		`SELECT COUNT(*) FROM subscriptions WHERE subscription='sub'`); err != nil || mappings != 0 {
		t.Fatalf("retained mappings=%d, error=%v", mappings, err)
	}
}

func TestSubscriptionMetadataRetirementMismatchRollsBack(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	disableForeignKeys(t, e)
	if err := e.Subscribe(ctx, "topic", "sub", nil); err != nil {
		t.Fatal(err)
	}

	// The mapping DELETE succeeds first, but the active queue does not match the
	// final deleting-row predicate. The returned error must roll the mapping back.
	err := e.inTx(ctx, func(ctx context.Context, tx *txn) error {
		done, removed, err := e.deleteEntityMetadataTx(ctx, tx, "sub", "subscription", false)
		if err == nil || done || removed != 0 {
			t.Fatalf("mismatched retirement: done=%t removed=%d error=%v", done, removed, err)
		}
		return err
	})
	if !errors.Is(err, ErrQueueNotFound) {
		t.Fatalf("mismatched retirement error=%v, want ErrQueueNotFound", err)
	}
	if mappings, queues := subscriptionMetadataRows(t, e, "sub"); mappings != 1 || queues != 1 {
		t.Fatalf("mismatched retirement committed partial metadata: mappings=%d queues=%d", mappings, queues)
	}
}

func TestSubscriptionDeletionBoundedRestartableAndNameReserved(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "subscription-lifecycle.db")
	now := int64(1_700_000_000_000)
	opts := Options{DB: "file:" + path, DisableBackground: true, Now: func() int64 { return now }}
	e, err := Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Subscribe(ctx, "topic", "large-sub", nil); err != nil {
		t.Fatal(err)
	}
	const rows = lifecycleBatchRows + lifecyclePassRows + 17
	insertLifecycleMessages(t, e, "large-sub", rows, 1)

	result, err := e.DeleteSubscriptionWithOptions(ctx, "topic", "large-sub", DeleteOptions{Force: true})
	if err != nil || result.Deleted || result.State != "deleting" {
		t.Fatalf("foreground delete=%+v, error=%v", result, err)
	}
	if got := lifecycleRows(t, e, "messages", "large-sub"); got != rows-lifecycleBatchRows {
		t.Fatalf("foreground remaining=%d, want %d", got, rows-lifecycleBatchRows)
	}
	if err := e.Subscribe(ctx, "topic", "large-sub", nil); !errors.Is(err, ErrNameConflict) {
		t.Fatalf("deleting subscription name was reusable: %v", err)
	}
	if _, err := e.SendOne(ctx, "topic", OutMessage{}); !errors.Is(err, ErrQueueNotFound) {
		t.Fatalf("topic routed to deleting subscription: %v", err)
	}
	page, err := e.ListSubscriptionsWithOptions(ctx, ListSubscriptionsOptions{Topic: "topic", IncludeDeleting: true})
	if err != nil || page.Total != 1 || len(page.Subscriptions) != 1 || page.Subscriptions[0].State != "deleting" {
		t.Fatalf("deleting inventory=%+v, error=%v", page, err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e, err = Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if got := lifecycleRows(t, e, "messages", "large-sub"); got != 17 {
		t.Fatalf("startup bounded cleanup remaining=%d, want 17", got)
	}
	if state, ok := queueLifecycleState(t, e, "large-sub"); !ok || state != "deleting" {
		t.Fatalf("restart lost reservation: state=(%q,%t)", state, ok)
	}
	e.RunMaintenanceOnce(ctx)
	if _, ok := queueLifecycleState(t, e, "large-sub"); ok {
		t.Fatal("follow-up maintenance did not finish subscription deletion")
	}
	if err := e.Subscribe(ctx, "topic", "large-sub", nil); err != nil {
		t.Fatalf("same-name recreation after physical cleanup: %v", err)
	}
}

func TestConcurrentTopicPublishAndForceDeleteSubscription(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	for i := 0; i < 24; i++ {
		topic := fmt.Sprintf("topic-%02d", i)
		name := fmt.Sprintf("sub-%02d", i)
		if err := e.Subscribe(ctx, topic, name, nil); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		var sendErr, deleteErr error
		var result DeleteResult
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, sendErr = e.SendOne(ctx, topic, OutMessage{Body: []byte("race")})
		}()
		go func() {
			defer wg.Done()
			<-start
			result, deleteErr = e.DeleteSubscriptionWithOptions(ctx, topic, name, DeleteOptions{Force: true})
		}()
		close(start)
		wg.Wait()
		if sendErr != nil && !errors.Is(sendErr, ErrQueueNotFound) {
			t.Fatalf("iteration %d send error=%v", i, sendErr)
		}
		if deleteErr != nil || !result.Deleted {
			t.Fatalf("iteration %d delete=%+v, error=%v", i, result, deleteErr)
		}
		if got := lifecycleRows(t, e, "messages", name); got != 0 {
			t.Fatalf("iteration %d left %d messages after deletion", i, got)
		}
		if _, err := e.SendOne(ctx, topic, OutMessage{}); !errors.Is(err, ErrQueueNotFound) {
			t.Fatalf("iteration %d topic remained routable: %v", i, err)
		}
	}
}

func TestConcurrentTopicPublishAndEmptyDeleteSubscription(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	for i := 0; i < 64; i++ {
		topic := fmt.Sprintf("empty-race-topic-%02d", i)
		name := fmt.Sprintf("empty-race-sub-%02d", i)
		if err := e.Subscribe(ctx, topic, name, nil); err != nil {
			t.Fatal(err)
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		var sendErr, deleteErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, sendErr = e.SendOne(ctx, topic, OutMessage{Body: []byte("must-not-be-lost")})
		}()
		go func() {
			defer wg.Done()
			<-start
			deleteErr = e.DeleteSubscription(ctx, topic, name)
		}()
		close(start)
		wg.Wait()

		switch {
		case sendErr == nil:
			if !errors.Is(deleteErr, ErrSubscriptionNotEmpty) {
				t.Fatalf("iteration %d: successful send paired with delete error %v, want ErrSubscriptionNotEmpty", i, deleteErr)
			}
			if got := lifecycleRows(t, e, "messages", name); got != 1 {
				t.Fatalf("iteration %d: successful send retained %d messages, want 1", i, got)
			}
			if mappings, queues := subscriptionMetadataRows(t, e, name); mappings != 1 || queues != 1 {
				t.Fatalf("iteration %d: rejected empty-delete changed metadata: mappings=%d queues=%d", i, mappings, queues)
			}
			if result, err := e.DeleteSubscriptionWithOptions(ctx, topic, name, DeleteOptions{Force: true}); err != nil || !result.Deleted {
				t.Fatalf("iteration %d: cleanup force delete=%+v, error=%v", i, result, err)
			}

		case deleteErr == nil:
			if !errors.Is(sendErr, ErrQueueNotFound) {
				t.Fatalf("iteration %d: successful delete paired with send error %v, want ErrQueueNotFound", i, sendErr)
			}
			if got := lifecycleRows(t, e, "messages", name); got != 0 {
				t.Fatalf("iteration %d: successful delete allowed %d messages", i, got)
			}
			if mappings, queues := subscriptionMetadataRows(t, e, name); mappings != 0 || queues != 0 {
				t.Fatalf("iteration %d: successful delete retained metadata: mappings=%d queues=%d", i, mappings, queues)
			}

		default:
			t.Fatalf("iteration %d: neither operation won: send=%v delete=%v", i, sendErr, deleteErr)
		}
	}
}

func TestSubscriptionLimitCountsDeletingAndAllowsFilterUpdate(t *testing.T) {
	ctx := context.Background()
	if e, err := Open(ctx, Options{DB: ":memory:", DisableBackground: true, MaxSubscriptions: -1}); !errors.Is(err, ErrInvalidArgument) || e != nil {
		t.Fatalf("negative max subscriptions: engine=%v error=%v", e, err)
	}
	e, err := Open(ctx, Options{
		DB:                "file:" + filepath.Join(t.TempDir(), "subscription-limit.db"),
		DisableBackground: true,
		MaxSubscriptions:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err := e.CreateQueue(ctx, "bare", QueueConfig{Kind: "subscription"}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("public orphan backing queue creation=%v", err)
	}
	// A pre-upgrade orphan can still be repaired through Subscribe, but attaching
	// it is a new mapping and therefore cannot bypass the configured capacity.
	if _, err := e.db.exec(ctx,
		`INSERT INTO queues(name,kind,created_at,updated_at) VALUES('legacy-orphan','subscription',1,1)`); err != nil {
		t.Fatal(err)
	}
	if err := e.Subscribe(ctx, "topic", "first", nil); err != nil {
		t.Fatal(err)
	}
	if err := e.Subscribe(ctx, "topic", "first", &Filter{Expr: "true"}); err != nil {
		t.Fatalf("filter update at capacity: %v", err)
	}
	if err := e.Subscribe(ctx, "legacy", "legacy-orphan", nil); !errors.Is(err, ErrSubscriptionLimit) {
		t.Fatalf("legacy orphan attachment bypassed capacity: %v", err)
	}
	insertLifecycleMessages(t, e, "first", lifecycleBatchRows+1, 1)
	result, err := e.DeleteSubscriptionWithOptions(ctx, "topic", "first", DeleteOptions{Force: true})
	if err != nil || result.Deleted || result.State != "deleting" {
		t.Fatalf("bounded delete=%+v, error=%v", result, err)
	}
	if err := e.Subscribe(ctx, "topic", "second", nil); !errors.Is(err, ErrSubscriptionLimit) {
		t.Fatalf("deleting subscription did not consume capacity: %v", err)
	}
	e.RunMaintenanceOnce(ctx)
	if err := e.Subscribe(ctx, "legacy", "legacy-orphan", nil); err != nil {
		t.Fatalf("cleanup did not release capacity for legacy orphan repair: %v", err)
	}
}

func TestConcurrentSubscriptionCreationCannotExceedLimit(t *testing.T) {
	ctx := context.Background()
	const limit = 8
	const contenders = 32
	e, err := Open(ctx, Options{
		DB:                "file:" + filepath.Join(t.TempDir(), "subscription-limit-race.db"),
		DisableBackground: true,
		MaxSubscriptions:  limit,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	start := make(chan struct{})
	results := make(chan error, contenders)
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		name := fmt.Sprintf("sub-%02d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- e.Subscribe(ctx, "topic", name, nil)
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	succeeded, limited := 0, 0
	for err := range results {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrSubscriptionLimit):
			limited++
		default:
			t.Fatalf("unexpected concurrent creation error: %v", err)
		}
	}
	if succeeded != limit || limited != contenders-limit {
		t.Fatalf("concurrent admission successes=%d limited=%d, want %d/%d", succeeded, limited, limit, contenders-limit)
	}
	page, err := e.ListSubscriptionsWithOptions(ctx, ListSubscriptionsOptions{Topic: "topic", Limit: maxSubscriptionPageSize})
	if err != nil || page.Total != limit || len(page.Subscriptions) != limit {
		t.Fatalf("retained subscription count=%+v, error=%v", page, err)
	}
}

func TestForceDeleteCleansAllMessageStatesAndAuxiliaryRows(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	for _, name := range []string{"q", "other"} {
		mustQueue(t, e, name, QueueConfig{})
		for _, state := range []State{StateActive, StateLocked, StateScheduled, StateDeferred, StateCompleted, StateDeadLettered} {
			if _, err := e.db.exec(ctx,
				`INSERT INTO messages(queue,state,enqueued_at,body) VALUES(?,?,1,?)`,
				name, string(state), []byte("retained")); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := e.db.exec(ctx,
			`INSERT INTO dedup(queue,message_id,request_hash,seq_number,seen_at) VALUES(?,?,?,?,1)`,
			name, "id", "hash", 1); err != nil {
			t.Fatal(err)
		}
		if _, err := e.db.exec(ctx,
			`INSERT INTO settlement_receipts(lock_token,queue,seq_number,operation,args,created_at,expires_at)
			 VALUES(?,?,1,'completed','',1,9999999999999)`, "token", name); err != nil {
			t.Fatal(err)
		}
		if _, err := e.db.exec(ctx,
			`INSERT INTO receive_attempts(queue,attempt_id,response,created_at,expires_at)
			 VALUES(?,?,'[]',1,9999999999999)`, name, "attempt"); err != nil {
			t.Fatal(err)
		}
	}

	result, err := e.DeleteQueueWithOptions(ctx, "q", DeleteOptions{Force: true})
	if err != nil || !result.Deleted || result.State != "deleted" {
		t.Fatalf("force delete: %+v, %v", result, err)
	}
	for _, table := range []string{"messages", "dedup", "settlement_receipts", "receive_attempts"} {
		if got := lifecycleRows(t, e, table, "q"); got != 0 {
			t.Fatalf("%s retained %d deleted rows", table, got)
		}
		want := 1
		if table == "messages" {
			want = 6
		}
		if got := lifecycleRows(t, e, table, "other"); got != want {
			t.Fatalf("%s crossed queue boundary: other=%d want=%d", table, got, want)
		}
	}
	if _, ok := queueLifecycleState(t, e, "q"); ok {
		t.Fatal("deleted queue metadata remains")
	}
}

func TestQueueDeletionBoundedRestartableAndNameReserved(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "lifecycle.db")
	now := int64(1_700_000_000_000)
	opts := Options{DB: "file:" + path, DisableBackground: true, Now: func() int64 { return now }}
	e, err := Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	mustQueue(t, e, "large", QueueConfig{})
	const rows = lifecycleBatchRows + lifecyclePassRows + 17
	insertLifecycleMessages(t, e, "large", rows, 1)

	result, err := e.DeleteQueueWithOptions(ctx, "large", DeleteOptions{Force: true})
	if err != nil || result.Deleted || result.State != "deleting" {
		t.Fatalf("foreground force delete: %+v, %v", result, err)
	}
	if got := lifecycleRows(t, e, "messages", "large"); got != rows-lifecycleBatchRows {
		t.Fatalf("foreground deleted outside %d-row budget: remaining=%d", lifecycleBatchRows, got)
	}
	if state, ok := queueLifecycleState(t, e, "large"); !ok || state != "deleting" {
		t.Fatalf("inventory state=(%q,%t), want deleting", state, ok)
	}
	if err := e.CreateQueue(ctx, "large", QueueConfig{}); !errors.Is(err, ErrNameConflict) {
		t.Fatalf("deleting name was reusable: %v", err)
	}
	if err := e.Subscribe(ctx, "topic", "large", nil); !errors.Is(err, ErrNameConflict) {
		t.Fatalf("deleting name was reusable as a subscription: %v", err)
	}
	if _, err := e.SendOne(ctx, "large", OutMessage{}); !errors.Is(err, ErrQueueNotFound) {
		t.Fatalf("deleting queue accepted send: %v", err)
	}
	if _, err := e.Receive(ctx, "large", ReceiveOptions{}); !errors.Is(err, ErrQueueNotFound) {
		t.Fatalf("deleting queue accepted receive: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	// Open resumes one globally bounded pass before crash recovery. Seventeen
	// rows deliberately remain, proving startup did not run unbounded cleanup.
	e, err = Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if got := lifecycleRows(t, e, "messages", "large"); got != 17 {
		t.Fatalf("startup pass remaining=%d, want 17", got)
	}
	if state, ok := queueLifecycleState(t, e, "large"); !ok || state != "deleting" {
		t.Fatalf("restart lost reservation: state=(%q,%t)", state, ok)
	}
	e.RunMaintenanceOnce(ctx)
	if _, ok := queueLifecycleState(t, e, "large"); ok {
		t.Fatal("bounded follow-up pass did not finish deletion")
	}
	mustQueue(t, e, "large", QueueConfig{LockDurationMs: 12345})
	if q, err := e.loadQueue(ctx, "large"); err != nil || q.lockDurationMs != 12345 {
		t.Fatalf("replacement queue: %+v, %v", q, err)
	}
}

func TestEmptyQueueDeletionBoundsAuxiliaryRows(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{})
	if _, err := e.db.exec(ctx, `WITH RECURSIVE n(i) AS (
		SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<2500
	) INSERT INTO receive_attempts(queue,attempt_id,response,created_at,expires_at)
	  SELECT 'q',printf('attempt-%d',i),'[]',1,9999999999999 FROM n`); err != nil {
		t.Fatal(err)
	}
	if err := e.DeleteQueue(ctx, "q"); err != nil {
		t.Fatalf("large auxiliary delete=%v, want accepted deletion", err)
	}
	if got := lifecycleRows(t, e, "receive_attempts", "q"); got != 1500 {
		t.Fatalf("foreground auxiliary remaining=%d, want 1500", got)
	}
	if state, ok := queueLifecycleState(t, e, "q"); !ok || state != "deleting" {
		t.Fatalf("pending state=(%q,%t)", state, ok)
	}
	e.maintainLifecycle(ctx)
	if _, ok := queueLifecycleState(t, e, "q"); ok {
		t.Fatal("auxiliary cleanup did not converge")
	}
}

func TestQueueDeletionCountsMetadataRowInRowBudget(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{})
	if _, err := e.db.exec(ctx, `WITH RECURSIVE n(i) AS (
		SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<?
	) INSERT INTO receive_attempts(queue,attempt_id,response,created_at,expires_at)
	  SELECT 'q',printf('attempt-%d',i),'[]',1,9999999999999 FROM n`, lifecycleBatchRows); err != nil {
		t.Fatal(err)
	}
	result, err := e.DeleteQueueWithOptions(ctx, "q", DeleteOptions{})
	if err != nil || result.Deleted || result.State != "deleting" {
		t.Fatalf("exact-cap delete=%+v, %v; metadata row must wait for the next pass", result, err)
	}
	if got := lifecycleRows(t, e, "receive_attempts", "q"); got != 0 {
		t.Fatalf("owned rows remaining=%d, want 0", got)
	}
	if state, ok := queueLifecycleState(t, e, "q"); !ok || state != "deleting" {
		t.Fatalf("metadata reservation state=(%q,%t)", state, ok)
	}
	e.maintainLifecycle(ctx)
	if _, ok := queueLifecycleState(t, e, "q"); ok {
		t.Fatal("metadata-only follow-up did not finish")
	}
}

func TestQueueDeletionByteBudgetAndOversizedProgress(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "metadata", QueueConfig{})
	columns := []string{"lock_token", "message_id", "correlation_id", "reply_to", "group_id", "content_type", "subject", "properties", "dead_letter_reason", "dead_letter_description"}
	// SQLite length(TEXT) counts characters. Each repeated unit is two
	// characters but three UTF-8 bytes, and the embedded NUL must count too.
	metadata := strings.Repeat("é\x00", (1<<20)/len(columns)/3+1)
	args := make([]any, len(columns))
	for i := range args {
		args[i] = metadata
	}
	for i := 0; i < 9; i++ {
		if _, err := e.db.exec(ctx, `INSERT INTO messages(queue,state,enqueued_at,body,`+strings.Join(columns, ",")+`)
			VALUES('metadata','active',1,x'',`+strings.TrimRight(strings.Repeat("?,", len(columns)), ",")+`)`, args...); err != nil {
			t.Fatal(err)
		}
	}
	result, err := e.DeleteQueueWithOptions(ctx, "metadata", DeleteOptions{Force: true})
	if err != nil || result.Deleted || result.State != "deleting" {
		t.Fatalf("metadata byte bound: %+v, %v", result, err)
	}
	rowBytes := int64(len("metadata") + len("active") + len(metadata)*len(columns))
	wantRemaining := 9 - int(lifecycleBatchBytes/rowBytes)
	if remaining := lifecycleRows(t, e, "messages", "metadata"); remaining != wantRemaining {
		t.Fatalf("metadata byte budget remaining=%d, want %d", remaining, wantRemaining)
	}
	e.maintainLifecycle(ctx)
	if _, ok := queueLifecycleState(t, e, "metadata"); ok {
		t.Fatal("metadata cleanup did not finish")
	}

	mustQueue(t, e, "oversized", QueueConfig{})
	if _, err := e.db.exec(ctx,
		`INSERT INTO messages(queue,state,enqueued_at,body) VALUES('oversized','active',1,zeroblob(?))`, lifecycleBatchBytes+1); err != nil {
		t.Fatal(err)
	}
	result, err = e.DeleteQueueWithOptions(ctx, "oversized", DeleteOptions{Force: true})
	if err != nil || result.Deleted || result.State != "deleting" {
		t.Fatalf("oversized deletion: %+v, %v", result, err)
	}
	if got := lifecycleRows(t, e, "messages", "oversized"); got != 0 {
		t.Fatalf("oversized row made no progress: %d remain", got)
	}
	e.maintainLifecycle(ctx)
	if _, ok := queueLifecycleState(t, e, "oversized"); ok {
		t.Fatal("oversized cleanup did not converge")
	}
}

func TestLifecycleMaintenanceGlobalBudgetsAndFairness(t *testing.T) {
	t.Run("row budget", func(t *testing.T) {
		ctx := context.Background()
		e, _ := testEngine(t)
		for _, name := range []string{"a", "b"} {
			mustQueue(t, e, name, QueueConfig{})
			insertLifecycleMessages(t, e, name, 3000, 1)
			if _, err := e.db.exec(ctx,
				`UPDATE queues SET lifecycle_state='deleting',delete_started_at=1 WHERE name=?`, name); err != nil {
				t.Fatal(err)
			}
		}
		e.maintainLifecycle(ctx)
		remaining := lifecycleRows(t, e, "messages", "a") + lifecycleRows(t, e, "messages", "b")
		if remaining != 6000-lifecyclePassRows {
			t.Fatalf("global pass removed %d rows, want %d", 6000-remaining, lifecyclePassRows)
		}
	})

	t.Run("metadata rows share global cap", func(t *testing.T) {
		ctx := context.Background()
		e, _ := testEngine(t)
		mustQueue(t, e, "00-heavy", QueueConfig{})
		insertLifecycleMessages(t, e, "00-heavy", 5000, 1)
		for i := 0; i < lifecyclePassResources-1; i++ {
			mustQueue(t, e, fmt.Sprintf("empty-%02d", i), QueueConfig{})
		}
		if _, err := e.db.exec(ctx,
			`UPDATE queues SET lifecycle_state='deleting',delete_started_at=1`); err != nil {
			t.Fatal(err)
		}
		physicalRows := func() int {
			t.Helper()
			var count int
			if err := e.db.queryRowScan(ctx, []any{&count},
				`SELECT (SELECT COUNT(*) FROM messages)+(SELECT COUNT(*) FROM queues)`); err != nil {
				t.Fatal(err)
			}
			return count
		}
		before := physicalRows()
		e.maintainLifecycle(ctx)
		if removed := before - physicalRows(); removed != lifecyclePassRows {
			t.Fatalf("mixed pass removed %d physical rows, want cap %d", removed, lifecyclePassRows)
		}
	})

	t.Run("resource cap", func(t *testing.T) {
		ctx := context.Background()
		e, _ := testEngine(t)
		for i := 0; i < lifecyclePassResources+1; i++ {
			name := fmt.Sprintf("q-%02d", i)
			mustQueue(t, e, name, QueueConfig{})
			if _, err := e.db.exec(ctx,
				`UPDATE queues SET lifecycle_state='deleting',delete_started_at=1 WHERE name=?`, name); err != nil {
				t.Fatal(err)
			}
		}
		e.maintainLifecycle(ctx)
		queues, err := e.ListQueues(ctx)
		if err != nil || len(queues) != 1 {
			t.Fatalf("resource cap left %d queues, want 1: %v", len(queues), err)
		}
		e.maintainLifecycle(ctx)
		if queues, err = e.ListQueues(ctx); err != nil || len(queues) != 0 {
			t.Fatalf("resource cursor did not wrap: %v, %v", queues, err)
		}
	})

	t.Run("oversized oldest cannot starve later queue", func(t *testing.T) {
		ctx := context.Background()
		path := filepath.Join(t.TempDir(), "fairness.db")
		opts := Options{DB: "file:" + path, DisableBackground: true}
		e, err := Open(ctx, opts)
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"huge", "small"} {
			mustQueue(t, e, name, QueueConfig{})
		}
		for i := 0; i < 2; i++ {
			if _, err := e.db.exec(ctx,
				`INSERT INTO messages(queue,state,enqueued_at,body) VALUES('huge','active',1,zeroblob(?))`, lifecycleBatchBytes+1); err != nil {
				t.Fatal(err)
			}
		}
		insertLifecycleMessages(t, e, "small", 1, 1)
		if _, err := e.db.exec(ctx,
			`UPDATE queues SET lifecycle_state='deleting',delete_started_at=CASE name WHEN 'huge' THEN 1 ELSE 2 END
			 WHERE name IN ('huge','small')`); err != nil {
			t.Fatal(err)
		}

		e.maintainLifecycle(ctx)
		if _, ok := queueLifecycleState(t, e, "small"); !ok {
			t.Fatal("test did not make the oldest oversized queue exhaust the first pass")
		}
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
		// The next startup pass must resume after the durable cursor, rather
		// than returning to the oldest oversized queue.
		e, err = Open(ctx, opts)
		if err != nil {
			t.Fatal(err)
		}
		defer e.Close()
		if _, ok := queueLifecycleState(t, e, "small"); ok {
			t.Fatal("persistent cursor let the oldest queue starve a later resource")
		}
		if _, ok := queueLifecycleState(t, e, "huge"); !ok {
			t.Fatal("large queue unexpectedly completed before proving fair rotation")
		}
	})
}

func TestForceDeleteConcurrentSendIsSerialized(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	for i := 0; i < 40; i++ {
		name := fmt.Sprintf("race-%d", i)
		mustQueue(t, e, name, QueueConfig{})
		start := make(chan struct{})
		sent := make(chan error, 1)
		deleted := make(chan struct {
			result DeleteResult
			err    error
		}, 1)
		go func() {
			<-start
			_, err := e.SendOne(ctx, name, OutMessage{Body: []byte("explicitly discardable")})
			sent <- err
		}()
		go func() {
			<-start
			result, err := e.DeleteQueueWithOptions(ctx, name, DeleteOptions{Force: true})
			deleted <- struct {
				result DeleteResult
				err    error
			}{result, err}
		}()
		close(start)
		sendErr := <-sent
		deletion := <-deleted
		if deletion.err != nil || !deletion.result.Deleted || deletion.result.State != "deleted" {
			t.Fatalf("iteration %d delete=%+v err=%v", i, deletion.result, deletion.err)
		}
		if sendErr != nil && !errors.Is(sendErr, ErrQueueNotFound) {
			t.Fatalf("iteration %d send returned %v", i, sendErr)
		}
		if got := lifecycleRows(t, e, "messages", name); got != 0 {
			t.Fatalf("iteration %d resurrected %d messages", i, got)
		}
		if _, ok := queueLifecycleState(t, e, name); ok {
			t.Fatalf("iteration %d resurrected queue metadata", i)
		}
	}
}

func TestRemoteSnapshotRetryRechecksParentAfterConcurrentDelete(t *testing.T) {
	ctx := context.Background()
	e, err := Open(ctx, Options{DB: filepath.Join(t.TempDir(), "snapshot-race.db"), DisableBackground: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	mustQueue(t, e, "q", QueueConfig{})

	// Exercise the remote retry policy against real SQLite snapshot semantics:
	// the first callback reads an active parent, another connection deletes it,
	// then the stale callback's write must fail and replay from the beginning.
	e.db.remote = true
	e.db.sql.SetMaxOpenConns(4)
	firstSnapshot := make(chan struct{})
	releaseFirst := make(chan struct{})
	txDone := make(chan error, 1)
	callbacks := 0
	go func() {
		txDone <- e.Tx(ctx, func(tx *EngineTx) error {
			callbacks++
			if callbacks == 1 {
				var state string
				if err := tx.tx.QueryRowContext(tx.ctx,
					`SELECT lifecycle_state FROM queues WHERE name='q'`).Scan(&state); err != nil {
					return err
				}
				if state != "active" {
					return fmt.Errorf("first snapshot state=%q, want active", state)
				}
				close(firstSnapshot)
				<-releaseFirst
			}
			_, err := tx.SendOne("q", OutMessage{Body: []byte("must not become orphaned")})
			return err
		})
	}()

	select {
	case <-firstSnapshot:
	case <-time.After(2 * time.Second):
		t.Fatal("transaction did not establish its first queue snapshot")
	}
	result, err := e.DeleteQueueWithOptions(ctx, "q", DeleteOptions{Force: true})
	if err != nil || !result.Deleted {
		close(releaseFirst)
		t.Fatalf("concurrent delete=%+v, %v", result, err)
	}
	close(releaseFirst)
	select {
	case err := <-txDone:
		if !errors.Is(err, ErrQueueNotFound) {
			t.Fatalf("retried send error=%v, want ErrQueueNotFound", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("snapshot-conflicted transaction did not finish")
	}
	if callbacks != 2 {
		t.Fatalf("transaction callbacks=%d, want one conflicted attempt plus one recheck", callbacks)
	}
	var orphans int
	if err := e.db.queryRowScan(ctx, []any{&orphans}, `SELECT COUNT(*) FROM messages m
		LEFT JOIN queues q ON q.name=m.queue WHERE q.name IS NULL`); err != nil || orphans != 0 {
		t.Fatalf("orphan messages=%d, error=%v", orphans, err)
	}
}

func TestDeletingQueueRejectsDataPlaneFromStaleCache(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{})
	if _, err := e.loadQueue(ctx, "q"); err != nil { // deliberately seed an active cache entry
		t.Fatal(err)
	}
	insert := func(state State, token string) int64 {
		t.Helper()
		res, err := e.db.exec(ctx, `INSERT INTO messages
			(queue,state,locked_until,lock_token,enqueued_at,body) VALUES('q',?,?,?,1,x'01')`,
			string(state), e.now()+60_000, nz(token))
		if err != nil {
			t.Fatal(err)
		}
		id, err := res.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	active := insert(StateActive, "")
	deferred := insert(StateDeferred, "")
	scheduled := insert(StateScheduled, "")
	dlq := insert(StateDeadLettered, "")
	locked := make([]int64, 7)
	for i := range locked {
		locked[i] = insert(StateLocked, fmt.Sprintf("lock-%d", i))
	}
	if _, err := e.db.exec(ctx,
		`UPDATE queues SET lifecycle_state='deleting',delete_started_at=? WHERE name='q'`, e.now()); err != nil {
		t.Fatal(err)
	}
	before := lifecycleRows(t, e, "messages", "q")
	wantNotFound := func(label string, err error) {
		t.Helper()
		if !errors.Is(err, ErrQueueNotFound) {
			t.Fatalf("%s=%v, want ErrQueueNotFound", label, err)
		}
	}
	_, err := e.SendOne(ctx, "q", OutMessage{})
	wantNotFound("send", err)
	_, err = e.Schedule(ctx, "q", OutMessage{}, e.now()+1000)
	wantNotFound("schedule", err)
	err = e.Tx(ctx, func(tx *EngineTx) error {
		_, err := tx.SendOne("q", OutMessage{})
		return err
	})
	wantNotFound("transactional send", err)
	_, err = e.Receive(ctx, "q", ReceiveOptions{})
	wantNotFound("receive", err)
	_, err = e.Receive(ctx, "q", ReceiveOptions{AttemptID: "attempt"})
	wantNotFound("idempotent receive", err)
	_, err = e.ReceiveDeferred(ctx, "q", deferred)
	wantNotFound("receive deferred", err)
	wantNotFound("cancel", e.Cancel(ctx, "q", scheduled))
	_, err = e.Peek(ctx, "q", PeekOptions{})
	wantNotFound("peek", err)
	_, err = e.Stats(ctx, "q")
	wantNotFound("stats", err)
	_, err = e.Redrive(ctx, "q", RedriveOptions{})
	wantNotFound("redrive", err)
	_, err = e.Purge(ctx, "q", RedriveOptions{})
	wantNotFound("purge", err)

	for label, settle := range map[string]func() error{
		"complete": func() error { return e.Complete(ctx, "q", locked[0], "lock-0") },
		"abandon":  func() error { return e.Abandon(ctx, "q", locked[1], "lock-1", 0) },
		"reject":   func() error { return e.Reject(ctx, "q", locked[2], "lock-2", "reason", "description") },
		"defer":    func() error { return e.Defer(ctx, "q", locked[3], "lock-3") },
		"renew":    func() error { return e.Renew(ctx, "q", locked[4], "lock-4") },
	} {
		if err := settle(); !errors.Is(err, ErrLockLost) {
			t.Fatalf("%s=%v, want ErrLockLost", label, err)
		}
	}
	batch, err := e.CompleteBatch(ctx, "q", []SettleItem{{SeqNumber: locked[5], LockToken: "lock-5"}})
	if err != nil || len(batch) != 1 || batch[0].Ok {
		t.Fatalf("complete batch=%+v, %v", batch, err)
	}
	batch, err = e.RenewBatch(ctx, "q", []SettleItem{{SeqNumber: locked[6], LockToken: "lock-6"}})
	if err != nil || len(batch) != 1 || batch[0].Ok {
		t.Fatalf("renew batch=%+v, %v", batch, err)
	}
	if got := lifecycleRows(t, e, "messages", "q"); got != before {
		t.Fatalf("rejected operations changed rows: before=%d after=%d", before, got)
	}
	if got := lifecycleRows(t, e, "settlement_receipts", "q"); got != 0 {
		t.Fatalf("rejected settlement wrote %d receipts", got)
	}
	for _, id := range []int64{active, deferred, scheduled, dlq} {
		var count int
		if err := e.db.queryRowScan(ctx, []any{&count}, `SELECT COUNT(*) FROM messages WHERE id=?`, id); err != nil || count != 1 {
			t.Fatalf("message %d changed or disappeared: count=%d err=%v", id, count, err)
		}
	}
}

func TestNotifierRetiresCanceledAndReplacedWaiters(t *testing.T) {
	n := newNotifier()
	for i := 0; i < 1000; i++ {
		_, release := n.subscribe(fmt.Sprintf("temporary-%d", i))
		release()
		release()
	}
	if len(n.chans) != 0 {
		t.Fatalf("canceled waiters retained %d names", len(n.chans))
	}
	old, releaseOld := n.subscribe("reused")
	n.notify("reused")
	select {
	case <-old:
	default:
		t.Fatal("old waiter was not notified")
	}
	fresh, releaseFresh := n.subscribe("reused")
	releaseOld()
	select {
	case <-fresh:
		t.Fatal("retired waiter closed a replacement channel")
	default:
	}
	n.notify("reused")
	select {
	case <-fresh:
	default:
		t.Fatal("replacement waiter lost its notification")
	}
	releaseFresh()
	if len(n.chans) != 0 {
		t.Fatalf("completed waiters retained %d names", len(n.chans))
	}
}

func TestNotifierReleaseKeepsOtherWaiters(t *testing.T) {
	n := newNotifier()
	first, releaseFirst := n.subscribe("q")
	second, releaseSecond := n.subscribe("q")
	if first != second {
		t.Fatal("concurrent waiters must share a notification")
	}
	releaseFirst()
	n.notify("q")
	select {
	case <-second:
	default:
		t.Fatal("one canceled waiter removed another waiter")
	}
	releaseSecond()
}

func TestObservabilityRetirementFencesLateCommit(t *testing.T) {
	o := observations{initialized: true, known: map[string]bool{"q": true}}
	o.commit(&txn{events: map[eventKey]uint64{{"q", "enqueued"}: 3}})
	late := &txn{events: map[eventKey]uint64{{"q", "completed"}: 2}}
	o.commit(&txn{forgottenQueues: map[string]bool{"q": true}})
	o.commit(&txn{observationEpoch: o.epoch, rememberedQueues: map[string]bool{"q": true}})
	o.commit(late)
	o.commit(&txn{observationEpoch: o.epoch, events: map[eventKey]uint64{{"q", "enqueued"}: 1}})
	if !o.known["q"] || len(o.known) != 1 {
		t.Fatalf("replacement inventory=%v", o.known)
	}
	if o.events[eventKey{"q", "enqueued"}] != 1 || o.events[eventKey{"q", "completed"}] != 0 {
		t.Fatalf("old lifetime contaminated replacement: %v", o.events)
	}
	if o.events[eventKey{"", "enqueued"}] != 3 || o.events[eventKey{"", "completed"}] != 2 {
		t.Fatalf("retired aggregate lost effects: %v", o.events)
	}
}

func TestObservabilityUniqueNameChurnRetainsOnlyAggregate(t *testing.T) {
	o := observations{initialized: true}
	for i := 0; i < 1000; i++ {
		name := fmt.Sprintf("request-%d", i)
		epoch := o.epoch
		o.commit(&txn{observationEpoch: epoch, rememberedQueues: map[string]bool{name: true}})
		o.commit(&txn{observationEpoch: o.epoch, events: map[eventKey]uint64{{name, "enqueued"}: 1}})
		o.commit(&txn{observationEpoch: o.epoch, forgottenQueues: map[string]bool{name: true}})
		o.commit(&txn{observationEpoch: epoch, events: map[eventKey]uint64{{name, "completed"}: 1}})
	}
	if len(o.known) != 0 || len(o.events) != 2 {
		t.Fatalf("retired names retained: known=%d events=%d", len(o.known), len(o.events))
	}
	if o.events[eventKey{"", "enqueued"}] != 1000 || o.events[eventKey{"", "completed"}] != 1000 {
		t.Fatalf("retired counts=%v", o.events)
	}
}

func TestObservabilityRetiredAggregateHasCompleteEventDomain(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "retired", QueueConfig{})
	if _, err := e.SendOne(ctx, "retired", OutMessage{Body: []byte("work")}); err != nil {
		t.Fatal(err)
	}
	result, err := e.DeleteQueueWithOptions(ctx, "retired", DeleteOptions{Force: true})
	if err != nil || !result.Deleted {
		t.Fatalf("delete=%+v, %v", result, err)
	}
	snapshot := e.Observability(ctx)
	if len(snapshot.Messages) != len(MessageEventNames()) {
		t.Fatalf("retired event domain is partial: %+v", snapshot.Messages)
	}
	for _, row := range snapshot.Messages {
		if row.Queue != "" {
			t.Fatalf("retired name retained: %+v", row)
		}
	}
}

func TestListQueuesReconcilesUnknownRetirementBeforeNameReuse(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "reused", QueueConfig{})
	if _, err := e.SendOne(ctx, "reused", OutMessage{Body: []byte("old lifetime")}); err != nil {
		t.Fatal(err)
	}

	// Model a remote delete commit whose acknowledgement was lost: the durable
	// rows changed, but commitAttempt could not publish forgetQueue in memory.
	if _, err := e.db.exec(ctx, `DELETE FROM messages WHERE queue='reused'`); err != nil {
		t.Fatal(err)
	}
	if _, err := e.db.exec(ctx, `DELETE FROM queues WHERE name='reused'`); err != nil {
		t.Fatal(err)
	}
	queues, err := e.ListQueues(ctx)
	if err != nil || len(queues) != 0 {
		t.Fatalf("reconciliation inventory=%+v, %v", queues, err)
	}

	mustQueue(t, e, "reused", QueueConfig{})
	if _, err := e.SendOne(ctx, "reused", OutMessage{Body: []byte("new lifetime")}); err != nil {
		t.Fatal(err)
	}
	snapshot := e.Observability(ctx)
	counts := make(map[eventKey]uint64)
	for _, counter := range snapshot.Messages {
		counts[eventKey{counter.Queue, counter.Event}] = counter.Count
	}
	if got := counts[eventKey{"", "enqueued"}]; got != 1 {
		t.Fatalf("retired enqueue count=%d, want 1", got)
	}
	if got := counts[eventKey{"reused", "enqueued"}]; got != 1 {
		t.Fatalf("replacement enqueue count=%d, want 1", got)
	}
}

func TestTopologyReconciliationRejectsStaleInventory(t *testing.T) {
	o := observations{initialized: true, known: map[string]bool{"existing": true}}
	collectionEpoch := o.epoch

	// A creation commits after inventory collection begins. Its topology callback
	// advances the epoch before the stale empty result is applied.
	o.commit(&txn{observationEpoch: o.epoch, rememberedQueues: map[string]bool{"created": true}})
	o.mu.Lock()
	o.reconcileTopologyLocked(map[string]bool{}, collectionEpoch)
	o.mu.Unlock()

	if !o.known["existing"] || !o.known["created"] || len(o.known) != 2 {
		t.Fatalf("stale inventory replaced newer topology: %v", o.known)
	}
}

func TestLifecycleCleanupQueriesUseQueuePrefixIndexes(t *testing.T) {
	e, _ := testEngine(t)
	for _, state := range lifecycleMessageStates {
		t.Run("messages "+state.name, func(t *testing.T) {
			plan := explainPlan(t, e, `SELECT rowid FROM messages WHERE queue=? AND state='`+state.name+`' ORDER BY id LIMIT ?`, "q", 1001)
			if !strings.Contains(plan, "SEARCH") || !strings.Contains(plan, state.index) || strings.Contains(plan, "SCAN") || strings.Contains(plan, "USE TEMP B-TREE") {
				t.Fatalf("%s cleanup no longer stops on queue-prefix index %s:\n%s", state.name, state.index, plan)
			}
		})
	}
	for _, tc := range []struct {
		name  string
		table string
		order string
		index string
	}{
		{"dedup", "dedup", "message_id", "sqlite_autoindex_dedup_1"},
		{"settlement receipts", "settlement_receipts", "seq_number,lock_token,operation,args", "sqlite_autoindex_settlement_receipts_1"},
		{"receive attempts", "receive_attempts", "attempt_id", "sqlite_autoindex_receive_attempts_1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := explainPlan(t, e, `SELECT rowid FROM `+tc.table+` WHERE queue=? ORDER BY `+tc.order+` LIMIT ?`, "q", 1001)
			if !strings.Contains(plan, "SEARCH") || !strings.Contains(plan, tc.index) || strings.Contains(plan, "SCAN") || strings.Contains(plan, "USE TEMP B-TREE") {
				t.Fatalf("cleanup no longer stops on the queue-prefix index %s:\n%s", tc.index, plan)
			}
		})
	}
	existencePlan := explainPlan(t, e, `SELECT EXISTS(`+messageExistenceUnion("?")+`)`, repeatedQueueArgs("q")...)
	for _, state := range lifecycleMessageStates {
		if !strings.Contains(existencePlan, state.index) {
			t.Fatalf("message existence check does not use %s:\n%s", state.index, existencePlan)
		}
	}
	if strings.Contains(existencePlan, "SCAN messages") || strings.Contains(existencePlan, "USE TEMP B-TREE") {
		t.Fatalf("message existence check became unbounded:\n%s", existencePlan)
	}
	plan := explainPlan(t, e, `SELECT name,delete_started_at FROM queues
		WHERE lifecycle_state='deleting' AND (delete_started_at,name)>(?,?)
		ORDER BY delete_started_at,name LIMIT ?`, int64(-1), "", lifecyclePassResources)
	if !strings.Contains(plan, "idx_queue_deleting") || strings.Contains(plan, "USE TEMP B-TREE") {
		t.Fatalf("resource selection no longer uses the deleting cursor index:\n%s", plan)
	}
}

func TestLifecycleResourceLimitIsBackendAware(t *testing.T) {
	if got := lifecycleResourceLimit(false); got != lifecyclePassResources {
		t.Fatalf("local resource limit=%d, want %d", got, lifecyclePassResources)
	}
	if got := lifecycleResourceLimit(true); got != 1 {
		t.Fatalf("remote resource limit=%d, want 1", got)
	}
}

func TestRemoteCommitPublishesRetirementBeforeReplacement(t *testing.T) {
	d, state := remoteDBFailingFirst(t, 0)
	d.sql.SetMaxOpenConns(4)
	e := &Engine{db: d, observation: observations{
		initialized: true,
		known:       map[string]bool{"q": true},
		events:      map[eventKey]uint64{{"q", "enqueued"}: 3},
	}}
	firstCommit := make(chan struct{})
	releaseAck := make(chan struct{})
	var release sync.Once
	unblock := func() { release.Do(func() { close(releaseAck) }) }
	defer unblock()
	commits := 0
	var commitsMu sync.Mutex
	state.onCommit = func() {
		commitsMu.Lock()
		commits++
		current := commits
		commitsMu.Unlock()
		if current == 1 {
			close(firstCommit)
			<-releaseAck
		}
	}
	deleted := make(chan error, 1)
	go func() {
		deleted <- e.inTx(context.Background(), func(_ context.Context, tx *txn) error {
			tx.forgetQueue("q")
			return nil
		})
	}()
	select {
	case <-firstCommit:
	case <-time.After(time.Second):
		t.Fatal("deletion did not reach commit")
	}
	created := make(chan error, 1)
	createReady := make(chan struct{})
	go func() {
		created <- e.inTx(context.Background(), func(_ context.Context, tx *txn) error {
			tx.rememberQueue("q")
			close(createReady)
			return nil
		})
	}()
	select {
	case <-createReady:
	case <-time.After(time.Second):
		t.Fatal("replacement callback was blocked")
	}
	if e.commitMu.TryLock() {
		e.commitMu.Unlock()
		t.Fatal("later commit could overtake retirement publication")
	}
	unblock()
	for _, done := range []chan error{deleted, created} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("commit publication did not finish")
		}
	}
	e.recordMessage("q", "enqueued", 1)
	if !e.observation.known["q"] || e.observation.events[eventKey{"q", "enqueued"}] != 1 || e.observation.events[eventKey{"", "enqueued"}] != 3 {
		t.Fatalf("replacement observation was retired: known=%v events=%v", e.observation.known, e.observation.events)
	}
}

func BenchmarkForceDeleteBoundedBatch(b *testing.B) {
	ctx := context.Background()
	e, err := Open(ctx, Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		b.Fatal(err)
	}
	defer e.Close()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		name := fmt.Sprintf("q-%d", i)
		if err := e.CreateQueue(ctx, name, QueueConfig{}); err != nil {
			b.Fatal(err)
		}
		insertLifecycleMessages(b, e, name, lifecycleBatchRows-1, 1)
		b.StartTimer()
		result, err := e.DeleteQueueWithOptions(ctx, name, DeleteOptions{Force: true})
		if err != nil || !result.Deleted {
			b.Fatalf("delete=%+v, %v", result, err)
		}
	}
}

// ─── indexed message-parent guards ──────────────────────────────────────────

func requireMessageParentFailure(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("operation succeeded without a message queue parent")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "foreign key constraint failed") {
		t.Fatalf("error=%v, want FOREIGN KEY constraint failed", err)
	}
}

func insertLifecycleTestMessage(t *testing.T, e *Engine, queue, state string) int64 {
	t.Helper()
	result, err := e.db.execUnobserved(context.Background(),
		`INSERT INTO messages(queue,state,enqueued_at,body) VALUES(?,?,1,x'')`, queue, state)
	if err != nil {
		t.Fatalf("insert %s message for %q: %v", state, queue, err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("message id for %q: %v", queue, err)
	}
	return id
}

func TestMessageQueueUpdateTriggerRejectsOrphan(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)

	mustQueue(t, e, "source", QueueConfig{})
	mustQueue(t, e, "destination", QueueConfig{})
	id := insertLifecycleTestMessage(t, e, "source", "active")
	_, err := e.db.execUnobserved(ctx, `UPDATE messages SET queue='missing' WHERE id=?`, id)
	requireMessageParentFailure(t, err)

	var queue string
	if err := e.db.queryRowScanUnobserved(ctx, []any{&queue}, `SELECT queue FROM messages WHERE id=?`, id); err != nil {
		t.Fatal(err)
	}
	if queue != "source" {
		t.Fatalf("failed child update moved message to %q", queue)
	}
	if _, err := e.db.execUnobserved(ctx, `UPDATE messages SET queue='destination' WHERE id=?`, id); err != nil {
		t.Fatalf("child update to an existing parent: %v", err)
	}
}

func TestMessageQueueTriggersRestrictParentRenameAndDelete(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "parent", QueueConfig{})
	id := insertLifecycleTestMessage(t, e, "parent", "active")

	_, err := e.db.execUnobserved(ctx, `UPDATE queues SET name='renamed' WHERE name='parent'`)
	requireMessageParentFailure(t, err)
	_, err = e.db.execUnobserved(ctx, `DELETE FROM queues WHERE name='parent'`)
	requireMessageParentFailure(t, err)

	if _, err := e.db.execUnobserved(ctx, `DELETE FROM messages WHERE id=?`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := e.db.execUnobserved(ctx, `UPDATE queues SET name='renamed' WHERE name='parent'`); err != nil {
		t.Fatalf("rename after clearing children: %v", err)
	}
	result, err := e.db.execUnobserved(ctx, `DELETE FROM queues WHERE name='renamed'`)
	if err != nil {
		t.Fatalf("delete after clearing children: %v", err)
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		t.Fatalf("cleared parent rows=%d, error=%v; want 1", rows, err)
	}
}

func TestEveryMessageStateRestrictsParentDeleteUntilCleared(t *testing.T) {
	if len(lifecycleMessageStates) != 6 {
		t.Fatalf("message state inventory=%d, want 6", len(lifecycleMessageStates))
	}
	ctx := context.Background()
	e, _ := testEngine(t)
	for _, state := range lifecycleMessageStates {
		state := state
		t.Run(state.name, func(t *testing.T) {
			name := "parent-" + state.name
			mustQueue(t, e, name, QueueConfig{})
			id := insertLifecycleTestMessage(t, e, name, state.name)

			_, err := e.db.execUnobserved(ctx, `DELETE FROM queues WHERE name=?`, name)
			requireMessageParentFailure(t, err)
			if _, err := e.db.execUnobserved(ctx, `DELETE FROM messages WHERE id=?`, id); err != nil {
				t.Fatal(err)
			}
			result, err := e.db.execUnobserved(ctx, `DELETE FROM queues WHERE name=?`, name)
			if err != nil {
				t.Fatalf("delete cleared %s parent: %v", state.name, err)
			}
			if rows, err := result.RowsAffected(); err != nil || rows != 1 {
				t.Fatalf("delete cleared %s parent rows=%d, error=%v; want 1", state.name, rows, err)
			}
		})
	}
}

func TestMessageParentDeleteStaysIndexedBesideDeepUnrelatedBacklog(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "backlog", QueueConfig{})
	mustQueue(t, e, "empty-target", QueueConfig{})

	const backlogRows = 30_000
	if _, err := e.db.execUnobserved(ctx, `WITH RECURSIVE n(i) AS (
		SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<?
	) INSERT INTO messages(queue,state,enqueued_at,body)
	  SELECT 'backlog', CASE i%6
	    WHEN 0 THEN 'active' WHEN 1 THEN 'locked' WHEN 2 THEN 'deferred'
	    WHEN 3 THEN 'scheduled' WHEN 4 THEN 'completed' ELSE 'dead_lettered' END,
	    1,x'' FROM n`, backlogRows); err != nil {
		t.Fatalf("seed unrelated backlog: %v", err)
	}
	if _, err := e.db.execUnobserved(ctx, `ANALYZE`); err != nil {
		t.Fatalf("analyze backlog: %v", err)
	}

	// This is the exact predicate embedded in both parent triggers. Pinning every
	// branch to its queue-prefix partial index is deterministic evidence that an
	// empty target does six bounded seeks instead of scanning the unrelated rows.
	plan := explainPlan(t, e, `SELECT EXISTS(`+messageExistenceUnion("?")+`)`, repeatedQueueArgs("empty-target")...)
	for _, state := range lifecycleMessageStates {
		if !strings.Contains(plan, state.index) {
			t.Errorf("parent guard for %s does not use %s:\n%s", state.name, state.index, plan)
		}
	}
	if strings.Contains(plan, "SCAN messages") || strings.Contains(plan, "USE TEMP B-TREE") {
		t.Fatalf("parent guard became backlog-dependent:\n%s", plan)
	}

	result, err := e.db.execUnobserved(ctx, `DELETE FROM queues WHERE name='empty-target'`)
	if err != nil {
		t.Fatalf("delete empty target beside deep backlog: %v", err)
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		t.Fatalf("empty target rows=%d, error=%v; want 1", rows, err)
	}
	var retained int
	if err := e.db.queryRowScanUnobserved(ctx, []any{&retained},
		`SELECT COUNT(*) FROM messages WHERE queue='backlog'`); err != nil || retained != backlogRows {
		t.Fatalf("unrelated backlog=%d, error=%v; want %d", retained, err, backlogRows)
	}
}

func TestSubscriptionChildIndexIsUniqueAndUsedForParentDelete(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	if err := e.Subscribe(ctx, "topic-a", "subscription-child", nil); err != nil {
		t.Fatal(err)
	}

	var unique int
	if err := e.db.queryRowScanUnobserved(ctx, []any{&unique},
		`SELECT "unique" FROM pragma_index_list('subscriptions') WHERE name='idx_subs_by_subscription'`); err != nil {
		t.Fatalf("inspect idx_subs_by_subscription: %v", err)
	}
	if unique != 1 {
		t.Fatalf("idx_subs_by_subscription unique=%d, want 1", unique)
	}
	if _, err := e.db.execUnobserved(ctx,
		`INSERT INTO subscriptions(topic,subscription,created_at) VALUES('topic-b','subscription-child',1)`); err == nil ||
		!strings.Contains(err.Error(), "UNIQUE constraint failed") {
		t.Fatalf("duplicate subscription child error=%v, want unique constraint", err)
	}

	plan := explainPlan(t, e, `DELETE FROM queues WHERE name=?`, "subscription-child")
	if !strings.Contains(plan, "SEARCH subscriptions USING COVERING INDEX idx_subs_by_subscription") || strings.Contains(plan, "SCAN subscriptions") {
		t.Fatalf("queue parent delete does not use the subscription child index:\n%s", plan)
	}

	result, err := e.db.execUnobserved(ctx, `DELETE FROM queues WHERE name='subscription-child'`)
	if err != nil {
		t.Fatalf("delete subscription parent: %v", err)
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		t.Fatalf("subscription parent rows=%d, error=%v; want 1", rows, err)
	}
	var children int
	if err := e.db.queryRowScanUnobserved(ctx, []any{&children},
		`SELECT COUNT(*) FROM subscriptions WHERE subscription='subscription-child'`); err != nil || children != 0 {
		t.Fatalf("cascaded subscription children=%d, error=%v; want 0", children, err)
	}
}

func TestMessageQueueTriggerInventory(t *testing.T) {
	e, _ := testEngine(t)
	want := []string{
		"trg_messages_queue_update",
		"trg_queues_messages_delete",
		"trg_queues_messages_rename",
	}
	for _, name := range want {
		var count int
		if err := e.db.queryRowScanUnobserved(context.Background(), []any{&count},
			`SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name=?`, name); err != nil || count != 1 {
			t.Errorf("trigger %s count=%d, error=%v; want 1", name, count, err)
		}
	}

	var physicalFKs int
	if err := e.db.queryRowScanUnobserved(context.Background(), []any{&physicalFKs},
		`SELECT COUNT(*) FROM pragma_foreign_key_list('messages')`); err != nil || physicalFKs != 0 {
		t.Fatalf("messages physical foreign keys=%d, error=%v; want 0", physicalFKs, err)
	}
	if testing.Verbose() {
		t.Logf("pinned %d message-parent guards without a per-insert trigger or duplicate full queue index", len(want))
	}
}
func TestExpiredSubscriptionTransitionAndEmptyBatchCleanup(t *testing.T) {
	ctx := context.Background()
	e, clock := testEngine(t)
	const count = 10_000
	now := *clock
	if _, err := e.db.exec(ctx, `WITH RECURSIVE n(i) AS (
		SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<?
	) INSERT INTO queues(name,kind,lifecycle_state,delete_started_at,expires_at,created_at,updated_at)
	  SELECT printf('ttl-%05d',i),'subscription','active',0,?,?,? FROM n`, count-1, now, now-1, now-1); err != nil {
		t.Fatal(err)
	}
	if _, err := e.db.exec(ctx, `INSERT INTO subscriptions(topic,subscription,created_at)
		SELECT 'burst',name,created_at FROM queues WHERE kind='subscription'`); err != nil {
		t.Fatal(err)
	}

	for pass := 1; pass <= 24; pass++ {
		changed, _, err := e.transitionExpiredSubscriptionsPass(ctx)
		if err != nil {
			t.Fatalf("transition pass %d: %v", pass, err)
		}
		e.maintainLifecycle(ctx)
		var retained int
		if err := e.db.queryRowScan(ctx, []any{&retained}, `SELECT COUNT(*) FROM subscriptions`); err != nil {
			t.Fatal(err)
		}
		if retained == 0 {
			if pass > 20 {
				t.Fatalf("10k empty expirations required %d cleanup passes, want at most 20", pass)
			}
			if changed == 0 && pass == 1 {
				t.Fatal("due transition did not find subscriptions")
			}
			return
		}
	}
	t.Fatal("10k empty expirations did not converge within 24 bounded passes")
}

func TestEmptySubscriptionBatchLeavesBudgetForLoadedResource(t *testing.T) {
	ctx := context.Background()
	e, clock := testEngine(t)
	if err := e.Subscribe(ctx, "topic", "heavy", nil); err != nil {
		t.Fatal(err)
	}
	insertLifecycleMessages(t, e, "heavy", lifecyclePassRows+1000, 1)
	if _, err := e.DeleteSubscriptionWithOptions(ctx, "topic", "heavy", DeleteOptions{Force: true}); err != nil {
		t.Fatal(err)
	}
	before := lifecycleRows(t, e, "messages", "heavy")
	// Thousands of earlier, nonempty reservations must not prevent the indexed
	// candidate scan from finding an empty batch later in lifecycle order.
	if _, err := e.db.exec(ctx, `WITH RECURSIVE n(i) AS (
		SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<2047
	) INSERT INTO queues(name,kind,lifecycle_state,delete_started_at,created_at,updated_at)
	  SELECT printf('blocked-%04d',i),'subscription','deleting',?,1,1 FROM n`, *clock+1); err != nil {
		t.Fatal(err)
	}
	if _, err := e.db.exec(ctx, `INSERT INTO subscriptions(topic,subscription,created_at)
		SELECT 'blocked',name,1 FROM queues WHERE name LIKE 'blocked-%'`); err != nil {
		t.Fatal(err)
	}
	if _, err := e.db.exec(ctx, `INSERT INTO messages(queue,state,enqueued_at,body)
		SELECT name,'active',1,x'01' FROM queues WHERE name LIKE 'blocked-%'`); err != nil {
		t.Fatal(err)
	}
	if _, err := e.db.exec(ctx, `WITH RECURSIVE n(i) AS (
		SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<?
	) INSERT INTO queues(name,kind,lifecycle_state,delete_started_at,created_at,updated_at)
	  SELECT printf('empty-%04d',i),'subscription','deleting',?,1,1 FROM n`, lifecycleEmptySubBatch-1, *clock+2); err != nil {
		t.Fatal(err)
	}
	if _, err := e.db.exec(ctx, `INSERT INTO subscriptions(topic,subscription,created_at)
		SELECT 'empty',name,1 FROM queues WHERE name LIKE 'empty-%'`); err != nil {
		t.Fatal(err)
	}
	e.maintainLifecycle(ctx)
	if after := lifecycleRows(t, e, "messages", "heavy"); after >= before {
		t.Fatalf("set-based empty cleanup starved loaded resource: messages %d -> %d", before, after)
	}
	var empty int
	if err := e.db.queryRowScan(ctx, []any{&empty}, `SELECT COUNT(*) FROM subscriptions WHERE topic='empty'`); err != nil || empty != 0 {
		t.Fatalf("empty batch retained=%d err=%v", empty, err)
	}
	plan := explainPlan(t, e, emptySubscriptionCandidatesSQL, lifecycleEmptySubBatch)
	if !strings.Contains(plan, "idx_queue_deleting") || strings.Contains(plan, "USE TEMP B-TREE") || strings.Contains(plan, "SCAN messages") {
		t.Fatalf("empty subscription candidate scan lost bounded indexes:\n%s", plan)
	}
}

func TestLifecycleWorkerFastContinuesFullEmptyBatches(t *testing.T) {
	ctx := context.Background()
	e, clock := testEngine(t)
	const count = lifecycleEmptySubBatch * 4
	if _, err := e.db.exec(ctx, `WITH RECURSIVE n(i) AS (
		SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<?
	) INSERT INTO queues(name,kind,lifecycle_state,delete_started_at,created_at,updated_at)
	  SELECT printf('fast-%04d',i),'subscription','deleting',?,1,1 FROM n`, count-1, *clock); err != nil {
		t.Fatal(err)
	}
	if _, err := e.db.exec(ctx, `INSERT INTO subscriptions(topic,subscription,created_at)
		SELECT 'fast',name,1 FROM queues WHERE name LIKE 'fast-%'`); err != nil {
		t.Fatal(err)
	}

	workerCtx, cancel := context.WithCancel(context.Background())
	e.startLifecycle(workerCtx)
	e.signalLifecycle()
	foreground, foregroundCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	if err := e.CreateQueue(foreground, "foreground", QueueConfig{}); err != nil {
		foregroundCancel()
		cancel()
		e.bgWG.Wait()
		t.Fatalf("foreground writer could not enter between bounded cleanup transactions: %v", err)
	}
	foregroundCancel()

	deadline := time.Now().Add(1500 * time.Millisecond)
	for {
		var retained int
		if err := e.db.queryRowScan(ctx, []any{&retained}, `SELECT COUNT(*) FROM subscriptions WHERE topic='fast'`); err != nil {
			cancel()
			e.bgWG.Wait()
			t.Fatal(err)
		}
		if retained == 0 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			e.bgWG.Wait()
			t.Fatalf("fast continuation retained %d subscriptions past deadline", retained)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	e.bgWG.Wait()
}

func TestSubscriptionExpiryWorkerFastContinuesFullBatches(t *testing.T) {
	ctx := context.Background()
	e, clock := testEngine(t)
	const count = subscriptionExpiryBatch*2 + 500
	if _, err := e.db.exec(ctx, `WITH RECURSIVE n(i) AS (
		SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<?
	) INSERT INTO queues(name,kind,lifecycle_state,expires_at,created_at,updated_at)
	  SELECT printf('due-%04d',i),'subscription','active',?,?,? FROM n`, count-1, *clock, *clock-1, *clock-1); err != nil {
		t.Fatal(err)
	}
	if _, err := e.db.exec(ctx, `INSERT INTO subscriptions(topic,subscription,created_at)
		SELECT 'due',name,created_at FROM queues WHERE name LIKE 'due-%'`); err != nil {
		t.Fatal(err)
	}

	workerCtx, cancel := context.WithCancel(context.Background())
	e.startSubscriptionExpiry(workerCtx)
	deadline := time.Now().Add(2 * time.Second)
	for {
		var active int
		if err := e.db.queryRowScan(ctx, []any{&active}, `SELECT COUNT(*) FROM queues
			WHERE name LIKE 'due-%' AND lifecycle_state='active'`); err != nil {
			cancel()
			e.bgWG.Wait()
			t.Fatal(err)
		}
		if active == 0 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			e.bgWG.Wait()
			t.Fatalf("fast expiry continuation retained %d active subscriptions", active)
		}
		time.Sleep(10 * time.Millisecond)
	}
	foreground, foregroundCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	if err := e.CreateQueue(foreground, "expiry-foreground", QueueConfig{}); err != nil {
		foregroundCancel()
		cancel()
		e.bgWG.Wait()
		t.Fatalf("expiry batches monopolized foreground writer: %v", err)
	}
	foregroundCancel()
	cancel()
	e.bgWG.Wait()
}

func TestSubscriptionExpiryFailureDoesNotRequestFastRetry(t *testing.T) {
	ctx := context.Background()
	e, clock := testEngine(t)
	if err := e.SubscribeWithOptions(ctx, "topic", "sub", nil, SubscribeOptions{TTLMs: 1}); err != nil {
		t.Fatal(err)
	}
	advance(clock, time.Millisecond)
	if _, err := e.db.exec(ctx, `CREATE TRIGGER fail_expiry_transition BEFORE UPDATE OF lifecycle_state ON queues
		WHEN OLD.name='sub' BEGIN SELECT RAISE(ABORT,'injected expiry transition failure'); END`); err != nil {
		t.Fatal(err)
	}
	changed, fastRetry, err := e.transitionExpiredSubscriptionsPass(ctx)
	if err == nil {
		t.Fatal("expiry transition unexpectedly succeeded")
	}
	if changed != 0 || fastRetry {
		t.Fatalf("failed transition changed=%d fast-retry=%t, want 0/false", changed, fastRetry)
	}
}

func TestEmptySubscriptionBatchRollsBackWithoutForeignKeys(t *testing.T) {
	ctx := context.Background()
	e, clock := testEngine(t)
	if err := e.SubscribeWithOptions(ctx, "topic", "sub", nil, SubscribeOptions{TTLMs: 1}); err != nil {
		t.Fatal(err)
	}
	advance(clock, time.Millisecond)
	if changed, _, err := e.transitionExpiredSubscriptionsPass(ctx); err != nil || changed != 1 {
		t.Fatalf("transition changed=%d err=%v", changed, err)
	}
	disableForeignKeys(t, e)
	if _, err := e.db.exec(ctx, `CREATE TRIGGER fail_expired_queue BEFORE DELETE ON queues
		WHEN OLD.name='sub' BEGIN SELECT RAISE(ABORT,'injected queue delete failure'); END`); err != nil {
		t.Fatal(err)
	}
	e.maintainLifecycle(ctx)
	if mappings, queues := subscriptionMetadataRows(t, e, "sub"); mappings != 1 || queues != 1 {
		t.Fatalf("failed set cleanup committed partial metadata mapping/queue=%d/%d", mappings, queues)
	}
	if _, err := e.db.exec(ctx, `DROP TRIGGER fail_expired_queue`); err != nil {
		t.Fatal(err)
	}
	e.maintainLifecycle(ctx)
	if mappings, queues := subscriptionMetadataRows(t, e, "sub"); mappings != 0 || queues != 0 {
		t.Fatalf("retry did not clean metadata mapping/queue=%d/%d", mappings, queues)
	}
}
