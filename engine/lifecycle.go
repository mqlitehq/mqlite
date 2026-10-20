package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	lifecycleBatchRows           = 1000
	lifecyclePassRows            = 4096
	lifecyclePassResources       = 32
	lifecycleBatchBytes    int64 = 8 << 20
	lifecycleCursorAtKey         = "lifecycle-cleanup-cursor-at"
	lifecycleCursorNameKey       = "lifecycle-cleanup-cursor-name"
)

var lifecycleMessageStates = [...]struct {
	name  string
	index string
}{
	{string(StateActive), "idx_msg_active"},
	{string(StateLocked), "idx_msg_locked_head"},
	{string(StateDeferred), "idx_msg_deferred"},
	{string(StateScheduled), "idx_msg_sched_head"},
	{string(StateCompleted), "idx_msg_completed"},
	{string(StateDeadLettered), "idx_msg_dlq"},
}

// messageExistenceUnion builds literal state predicates so SQLite can prove
// that each branch matches its queue-prefix partial index. queueExpr is either
// a bind parameter or the outer queues.name column; both are internal constants.
func messageExistenceUnion(queueExpr string) string {
	parts := make([]string, 0, len(lifecycleMessageStates))
	for _, state := range lifecycleMessageStates {
		parts = append(parts, `SELECT 1 FROM messages WHERE queue=`+queueExpr+` AND state='`+state.name+`'`)
	}
	return strings.Join(parts, ` UNION ALL `)
}

func repeatedQueueArgs(name string) []any {
	args := make([]any, len(lifecycleMessageStates))
	for i := range args {
		args[i] = name
	}
	return args
}

// DeleteOptions controls explicit destruction of a queue. Force discards every
// message state, including deliveries already in flight.
type DeleteOptions struct{ Force bool }

// DeleteResult distinguishes logical deletion from completed physical cleanup.
// A deleting queue cannot receive work and keeps its name until cleanup finishes.
type DeleteResult struct {
	Deleted bool   `json:"deleted"`
	State   string `json:"state"`
}

const queueSelectColumns = `name,kind,lock_duration_ms,max_delivery_count,default_ttl_ms,
	dead_letter_on_expire,dedup_window_ms,ordering_mode,dlq_max_age_ms,dlq_max_count,dlq_max_bytes,
	lifecycle_state`

type queueScanner interface{ Scan(...any) error }

func scanQueue(row queueScanner) (queueRow, error) {
	var q queueRow
	var dle int
	var ordering string
	err := row.Scan(&q.name, &q.kind, &q.lockDurationMs, &q.maxDeliveryCount, &q.defaultTTLMs,
		&dle, &q.dedupWindowMs, &ordering, &q.dlqMaxAgeMs, &q.dlqMaxCount, &q.dlqMaxBytes,
		&q.lifecycleState)
	if errors.Is(err, sql.ErrNoRows) {
		return queueRow{}, ErrQueueNotFound
	}
	q.deadLetterOnExp = dle != 0
	q.ordering = OrderingMode(ordering)
	return q, err
}

func (e *Engine) readQueueTx(ctx context.Context, tx *txn, name string) (queueRow, error) {
	return scanQueue(tx.QueryRowContext(ctx, `SELECT `+queueSelectColumns+` FROM queues WHERE name=?`, name))
}

func queueLive(q queueRow) error {
	if q.lifecycleState != "active" {
		return ErrQueueNotFound
	}
	return nil
}

// checkQueueLiveTx reads lifecycle metadata inside the operation's transaction.
// Cached configuration must never allow a deleting entity to act.
func (e *Engine) checkQueueLiveTx(ctx context.Context, tx *txn, name string) (queueRow, error) {
	q, err := e.readQueueTx(ctx, tx, name)
	if err != nil {
		return q, err
	}
	return q, queueLive(q)
}

func (e *Engine) invalidateLifecycle(name string) {
	e.qmu.Lock()
	delete(e.qcache, name)
	e.qepoch++
	e.qmu.Unlock()
	e.invalidateFilter(name)
	e.note.notify(name)
}

// DeleteQueueWithOptions logically retires one plain queue, then performs a
// bounded cleanup batch. Empty-only is the default; Force is durable permission
// to discard all retained messages. Maintenance resumes unfinished cleanup.
func (e *Engine) DeleteQueueWithOptions(ctx context.Context, name string, opts DeleteOptions) (DeleteResult, error) {
	if name == "" {
		return DeleteResult{}, fmt.Errorf("%w: queue name required", ErrInvalidArgument)
	}
	result := DeleteResult{}
	err := e.inTx(ctx, func(ctx context.Context, tx *txn) error {
		result = DeleteResult{}
		q, err := e.readQueueTx(ctx, tx, name)
		if err != nil {
			return err
		}
		if q.kind != "queue" {
			return fmt.Errorf("%w: DeleteQueue only supports plain queues, not subscriptions", ErrInvalidArgument)
		}
		if q.lifecycleState == "active" && !opts.Force {
			var occupied bool
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS(`+messageExistenceUnion("?")+`)`, repeatedQueueArgs(name)...).Scan(&occupied); err != nil {
				return err
			}
			if occupied {
				return ErrQueueNotEmpty
			}
		}
		if q.lifecycleState == "active" {
			if _, err := tx.ExecContext(ctx, `UPDATE queues SET lifecycle_state='deleting',delete_started_at=? WHERE name=? AND lifecycle_state='active'`, e.now(), name); err != nil {
				return err
			}
		}
		budget := cleanupBudget{rows: lifecycleBatchRows, bytes: lifecycleBatchBytes}
		done, err := e.cleanQueueTx(ctx, tx, name, &budget)
		if err != nil {
			return err
		}
		result.State = "deleting"
		result.Deleted = done
		if done {
			result.State = "deleted"
		}
		return nil
	})
	if err == nil || errors.Is(err, ErrOutcomeUnknown) {
		e.invalidateLifecycle(name)
	}
	if (err == nil && !result.Deleted) || errors.Is(err, ErrOutcomeUnknown) {
		e.signalLifecycle()
	}
	return result, err
}

type cleanupBudget struct {
	rows  int
	bytes int64
}

func lifecycleResourceLimit(remote bool) int {
	if remote {
		return 1
	}
	return lifecyclePassResources
}

// cleanQueueTx never cascades a message backlog. The queues row reserves the
// name until bounded deletion has emptied every queue-owned table.
func (e *Engine) cleanQueueTx(ctx context.Context, tx *txn, name string, budget *cleanupBudget) (bool, error) {
	if budget.rows <= 0 || budget.bytes <= 0 {
		return false, nil
	}
	// Empty resources are common (for example a request queue drained before
	// deletion). Retire one in a single round trip instead of probing all four
	// owned tables separately. The queue row remains the name reservation until
	// every table is empty.
	res, err := tx.ExecContext(ctx, `DELETE FROM queues
		WHERE name=? AND kind='queue' AND lifecycle_state='deleting'
		  AND NOT EXISTS (`+messageExistenceUnion("queues.name")+`)
		  AND NOT EXISTS (SELECT 1 FROM dedup WHERE queue=queues.name)
		  AND NOT EXISTS (SELECT 1 FROM settlement_receipts WHERE queue=queues.name)
		  AND NOT EXISTS (SELECT 1 FROM receive_attempts WHERE queue=queues.name)`, name)
	if err != nil {
		return false, err
	}
	removed, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if removed == 1 {
		budget.rows--
		tx.forgetQueue(name)
		return true, nil
	}

	// Count stored payload bytes, including metadata and UTF-8 text. SQLite's
	// length(TEXT) counts characters, so cast text to BLOB for byte accounting.
	// Index entries, row headers and WAL pages remain outside this estimate.
	type cleanupSource struct {
		name, size, order, state string
	}
	messageSize := `length(body)
			+COALESCE(length(CAST(queue AS BLOB)),0)
			+COALESCE(length(CAST(state AS BLOB)),0)
			+COALESCE(length(CAST(lock_token AS BLOB)),0)
			+COALESCE(length(CAST(message_id AS BLOB)),0)
			+COALESCE(length(CAST(correlation_id AS BLOB)),0)
			+COALESCE(length(CAST(reply_to AS BLOB)),0)
			+COALESCE(length(CAST(group_id AS BLOB)),0)
			+COALESCE(length(CAST(content_type AS BLOB)),0)
			+COALESCE(length(CAST(subject AS BLOB)),0)
			+COALESCE(length(CAST(properties AS BLOB)),0)
			+COALESCE(length(CAST(dead_letter_reason AS BLOB)),0)
			+COALESCE(length(CAST(dead_letter_description AS BLOB)),0)`
	sources := make([]cleanupSource, 0, len(lifecycleMessageStates)+3)
	for _, state := range lifecycleMessageStates {
		sources = append(sources, cleanupSource{"messages", messageSize, "id", state.name})
	}
	sources = append(sources,
		cleanupSource{"dedup", `length(CAST(queue AS BLOB))+length(CAST(message_id AS BLOB))
			+COALESCE(length(CAST(request_hash AS BLOB)),0)`, "message_id", ""},
		cleanupSource{"settlement_receipts", `length(CAST(queue AS BLOB))+length(CAST(lock_token AS BLOB))
			+length(CAST(operation AS BLOB))+length(CAST(args AS BLOB))`, "seq_number,lock_token,operation,args", ""},
		cleanupSource{"receive_attempts", `length(CAST(queue AS BLOB))+length(CAST(response AS BLOB))
			+length(CAST(attempt_id AS BLOB))`, "attempt_id", ""},
	)
	for _, table := range sources {
		if budget.rows <= 0 || budget.bytes <= 0 {
			return false, nil
		}
		// The order follows each table's queue-prefix index. LIMIT therefore bounds
		// reads and CPU as well as writes; ordering auxiliary rows by rowid would
		// scan or sort an unrelated multi-queue backlog before deleting one batch.
		// Read one sentinel past the write budget. That distinguishes a naturally
		// exhausted prefix from a full batch without an unbounded EXISTS probe.
		// Reads are still bounded to at most batch+1 rows.
		rowLimit := budget.rows
		query := `SELECT rowid,` + table.size + ` FROM ` + table.name + ` WHERE queue=?`
		if table.state != "" {
			query += ` AND state='` + table.state + `'`
		}
		query += ` ORDER BY ` + table.order + ` LIMIT ?`
		rows, err := tx.QueryContext(ctx, query, name, rowLimit+1)
		if err != nil {
			return false, err
		}
		var ids []any
		var usedBytes int64
		more := false
		for rows.Next() {
			var id, size int64
			if err := rows.Scan(&id, &size); err != nil {
				_ = rows.Close()
				return false, err
			}
			if len(ids) >= rowLimit {
				more = true
				break
			}
			// One oversized row may exceed the byte budget, ensuring progress.
			if len(ids) > 0 && (usedBytes >= budget.bytes || usedBytes+size > budget.bytes) {
				more = true
				break
			}
			ids = append(ids, id)
			usedBytes += size
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return false, err
		}
		_ = rows.Close()
		if len(ids) > 0 {
			placeholders := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
			if _, err := tx.ExecContext(ctx, `DELETE FROM `+table.name+` WHERE rowid IN (`+placeholders+`)`, ids...); err != nil {
				return false, err
			}
			budget.rows -= len(ids)
			budget.bytes -= usedBytes
		}
		if more {
			return false, nil
		}
	}
	// The metadata row counts toward the physical row cap. If the final owned
	// table consumed the last slot, leave the reservation for the next pass;
	// its empty-resource fast path will retire that one row.
	if budget.rows <= 0 {
		return false, nil
	}
	res, err = tx.ExecContext(ctx, `DELETE FROM queues WHERE name=? AND kind='queue' AND lifecycle_state='deleting'`, name)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if n != 1 {
		return false, ErrQueueNotFound
	}
	budget.rows--
	tx.forgetQueue(name)
	return true, nil
}

// maintainLifecycle applies one global row/byte/resource budget, so a burst of
// forced deletions cannot monopolize the single writer. Local stores process up
// to 32 resources per transaction. A remote store processes one: even bounded
// rows spread across many resources would otherwise turn into hundreds of
// sequential Hrana round trips while holding the write transaction. A durable
// composite cursor preserves fairness across passes in either mode. Within a
// local pass each selected queue receives a fair slice before unfinished queues
// reuse spare capacity.
func (e *Engine) maintainLifecyclePass(ctx context.Context) bool {
	type resource struct {
		name      string
		startedAt int64
	}
	var touched []string
	hadResources := false
	err := e.inTx(ctx, func(ctx context.Context, tx *txn) error {
		touched = nil
		hadResources = false
		resourceLimit := lifecycleResourceLimit(e.db.remote)
		cursorAt, cursorName := int64(math.MinInt64), ""
		var encoded string
		err := tx.QueryRowContext(ctx, `SELECT value FROM meta WHERE key=?`, lifecycleCursorAtKey).Scan(&encoded)
		if err == nil {
			if parsed, parseErr := strconv.ParseInt(encoded, 10, 64); parseErr == nil {
				cursorAt = parsed
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		err = tx.QueryRowContext(ctx, `SELECT value FROM meta WHERE key=?`, lifecycleCursorNameKey).Scan(&cursorName)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		resources := make([]resource, 0, resourceLimit)
		read := func(query string, args ...any) error {
			rows, err := tx.QueryContext(ctx, query, args...)
			if err != nil {
				return err
			}
			for rows.Next() {
				var r resource
				if err := rows.Scan(&r.name, &r.startedAt); err != nil {
					_ = rows.Close()
					return err
				}
				resources = append(resources, r)
			}
			err = rows.Err()
			if closeErr := rows.Close(); err == nil {
				err = closeErr
			}
			return err
		}
		if err := read(`SELECT name,delete_started_at FROM queues
			WHERE lifecycle_state='deleting' AND (delete_started_at,name)>(?,?)
			ORDER BY delete_started_at,name LIMIT ?`, cursorAt, cursorName, resourceLimit); err != nil {
			return err
		}
		if remaining := resourceLimit - len(resources); remaining > 0 {
			if err := read(`SELECT name,delete_started_at FROM queues
				WHERE lifecycle_state='deleting' AND (delete_started_at,name)<=(?,?)
				ORDER BY delete_started_at,name LIMIT ?`, cursorAt, cursorName, remaining); err != nil {
				return err
			}
		}
		hadResources = len(resources) > 0

		budget := cleanupBudget{rows: lifecyclePassRows, bytes: lifecycleBatchBytes}
		pending := resources
		var last resource
		for len(pending) > 0 && budget.rows > 0 && budget.bytes > 0 {
			next := make([]resource, 0, len(pending))
			used := false
			for i, r := range pending {
				if budget.rows <= 0 || budget.bytes <= 0 {
					break
				}
				remaining := len(pending) - i
				local := cleanupBudget{rows: budget.rows / remaining, bytes: budget.bytes / int64(remaining)}
				if local.rows < 1 {
					local.rows = 1
				}
				if local.bytes < 1 {
					local.bytes = 1
				}
				before := local
				done, err := e.cleanQueueTx(ctx, tx, r.name, &local)
				if err != nil {
					return err
				}
				rowsUsed, bytesUsed := before.rows-local.rows, before.bytes-local.bytes
				budget.rows -= rowsUsed
				budget.bytes -= bytesUsed
				used = used || rowsUsed > 0 || bytesUsed > 0 || done
				last = r
				touched = append(touched, r.name)
				if !done {
					next = append(next, r)
				}
			}
			if !used {
				break
			}
			pending = next
		}
		if last.name == "" {
			return nil
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO meta(key,value) VALUES(?,?),(?,?)
			ON CONFLICT(key) DO UPDATE SET value=excluded.value`,
			lifecycleCursorAtKey, strconv.FormatInt(last.startedAt, 10), lifecycleCursorNameKey, last.name)
		return err
	})
	if err != nil {
		e.maintenanceFailure(ctx, "resource_cleanup", err)
	}
	if err == nil || errors.Is(err, ErrOutcomeUnknown) {
		for _, name := range touched {
			e.invalidateLifecycle(name)
		}
	}
	// A selected resource may have completed in this pass. Returning true once
	// more deliberately permits one cheap confirmation pass and also covers more
	// resources than the selection cap. Any error is retried because a durable
	// deleting row may still exist (or an outcome-unknown commit may have landed).
	return err != nil || hadResources
}

// maintainLifecycle is the deterministic/manual wrapper used by tests and
// RunMaintenanceOnce. If work remains and a background worker exists, its
// nonblocking wake will continue the bounded cleanup; with background disabled,
// callers can invoke RunMaintenanceOnce again.
func (e *Engine) maintainLifecycle(ctx context.Context) {
	if e.maintainLifecyclePass(ctx) {
		e.signalLifecycle()
	}
}
