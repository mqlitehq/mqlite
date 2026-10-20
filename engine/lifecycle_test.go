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
	mustQueue(t, e, "subscription-child", QueueConfig{Kind: "subscription"})

	var unique int
	if err := e.db.queryRowScanUnobserved(ctx, []any{&unique},
		`SELECT "unique" FROM pragma_index_list('subscriptions') WHERE name='idx_subs_by_subscription'`); err != nil {
		t.Fatalf("inspect idx_subs_by_subscription: %v", err)
	}
	if unique != 1 {
		t.Fatalf("idx_subs_by_subscription unique=%d, want 1", unique)
	}
	if _, err := e.db.execUnobserved(ctx,
		`INSERT INTO subscriptions(topic,subscription,created_at) VALUES('topic-a','subscription-child',1)`); err != nil {
		t.Fatal(err)
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
