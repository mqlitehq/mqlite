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
	lifecycleBatchRows            = 1000
	lifecyclePassRows             = 4096
	lifecyclePassResources        = 32
	lifecycleEmptySubBatch        = 512
	subscriptionExpiryBatch       = 1000
	lifecycleBatchBytes     int64 = 8 << 20
	lifecycleCursorAtKey          = "lifecycle-cleanup-cursor-at"
	lifecycleCursorNameKey        = "lifecycle-cleanup-cursor-name"
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

var emptySubscriptionCandidatesSQL = `
	SELECT q.name
	  FROM queues q JOIN subscriptions s ON s.subscription=q.name
	 WHERE q.kind='subscription' AND q.lifecycle_state='deleting'
	   AND NOT EXISTS (` + messageExistenceUnion("q.name") + `)
	   AND NOT EXISTS (SELECT 1 FROM dedup WHERE queue=q.name)
	   AND NOT EXISTS (SELECT 1 FROM settlement_receipts WHERE queue=q.name)
	   AND NOT EXISTS (SELECT 1 FROM receive_attempts WHERE queue=q.name)
	 ORDER BY q.delete_started_at,q.name LIMIT ?`

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

// DeleteOptions controls explicit destruction of a queue or subscription. Force
// discards every message state, including deliveries already in flight.
type DeleteOptions struct{ Force bool }

// DeleteResult distinguishes logical deletion from completed physical cleanup.
// A deleting entity cannot receive work and keeps its name until cleanup finishes.
type DeleteResult struct {
	Deleted bool   `json:"deleted"`
	State   string `json:"state"`
}

const queueSelectColumns = `name,kind,lock_duration_ms,max_delivery_count,default_ttl_ms,
	dead_letter_on_expire,dedup_window_ms,ordering_mode,dlq_max_age_ms,dlq_max_count,dlq_max_bytes,
	lifecycle_state,expires_at`

type queueScanner interface{ Scan(...any) error }

func scanQueue(row queueScanner) (queueRow, error) {
	var q queueRow
	var dle int
	var ordering string
	err := row.Scan(&q.name, &q.kind, &q.lockDurationMs, &q.maxDeliveryCount, &q.defaultTTLMs,
		&dle, &q.dedupWindowMs, &ordering, &q.dlqMaxAgeMs, &q.dlqMaxCount, &q.dlqMaxBytes,
		&q.lifecycleState, &q.expiresAtMs)
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

func queueLiveAt(q queueRow, now int64) error {
	if q.lifecycleState != "active" || (q.expiresAtMs > 0 && q.expiresAtMs <= now) {
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
	return q, queueLiveAt(q, e.now())
}

func (e *Engine) invalidateLifecycle(name string) {
	e.qmu.Lock()
	delete(e.qcache, name)
	e.qepoch++
	e.qmu.Unlock()
	e.invalidateFilter(name)
	e.note.notify(name)
}

// transitionExpiredSubscriptionsPass makes due fixed-lifetime subscriptions
// durable members of the existing deletion pipeline. Correctness does not wait
// for this scan: queueLiveAt and every data-plane transaction fence at the
// absolute expiration. The indexed, bounded transition supplies restart-safe
// physical cleanup and preserves the name until that cleanup commits.
func (e *Engine) transitionExpiredSubscriptionsPass(ctx context.Context) (int, bool, error) {
	var names []string
	err := e.inTx(ctx, func(ctx context.Context, tx *txn) error {
		names = nil
		now := e.now()
		rows, err := tx.QueryContext(ctx, `
			UPDATE queues
			   SET lifecycle_state='deleting',delete_started_at=expires_at,updated_at=?
			 WHERE name IN (
			       SELECT name FROM queues
			        WHERE kind='subscription' AND lifecycle_state='active'
			          AND expires_at>0 AND expires_at<=?
			        ORDER BY expires_at,name LIMIT ?)
			RETURNING name`, now, now, subscriptionExpiryBatch)
		if err != nil {
			return err
		}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				_ = rows.Close()
				return err
			}
			names = append(names, name)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return err
		}
		return rows.Close()
	})
	if err == nil || errors.Is(err, ErrOutcomeUnknown) {
		for _, name := range names {
			e.invalidateLifecycle(name)
		}
	}
	if errors.Is(err, ErrOutcomeUnknown) {
		e.signalUncertainLifecycle()
	}
	// Only a full batch requests the 10 ms continuation. Errors fall back to the
	// worker's one-second ticker, avoiding a tight retry loop during an outage.
	return len(names), err == nil && len(names) == subscriptionExpiryBatch, err
}

func (e *Engine) runSubscriptionExpiryPass(ctx context.Context) bool {
	changed, pending, err := e.transitionExpiredSubscriptionsPass(ctx)
	if err != nil {
		e.maintenanceFailure(ctx, "transition_expired_subscriptions", err)
	}
	if changed > 0 || errors.Is(err, ErrOutcomeUnknown) {
		e.signalLifecycle()
	}
	return pending
}

func (e *Engine) transitionExpiredSubscriptions(ctx context.Context) {
	e.runSubscriptionExpiryPass(ctx)
}

// DeleteQueueWithOptions logically retires one plain queue, then performs a
// bounded cleanup batch. Empty-only is the default; Force is durable permission
// to discard all retained messages. Maintenance resumes unfinished cleanup.
func (e *Engine) DeleteQueueWithOptions(ctx context.Context, name string, opts DeleteOptions) (DeleteResult, error) {
	if name == "" {
		return DeleteResult{}, fmt.Errorf("%w: queue name required", ErrInvalidArgument)
	}
	return e.deleteEntity(ctx, "", name, "queue", opts)
}

// DeleteSubscription removes an empty subscription and its topic membership.
func (e *Engine) DeleteSubscription(ctx context.Context, topic, name string) error {
	_, err := e.DeleteSubscriptionWithOptions(ctx, topic, name, DeleteOptions{})
	return err
}

// DeleteSubscriptionWithOptions logically retires the exact topic/name mapping.
// Force is explicit permission to discard every retained message state.
func (e *Engine) DeleteSubscriptionWithOptions(ctx context.Context, topic, name string, opts DeleteOptions) (DeleteResult, error) {
	if topic == "" || name == "" {
		return DeleteResult{}, fmt.Errorf("%w: topic and subscription name required", ErrInvalidArgument)
	}
	return e.deleteEntity(ctx, topic, name, "subscription", opts)
}

func (e *Engine) deleteEntity(ctx context.Context, topic, name, kind string, opts DeleteOptions) (DeleteResult, error) {
	notFound := ErrQueueNotFound
	notEmpty := ErrQueueNotEmpty
	if kind == "subscription" {
		notFound = ErrSubscriptionNotFound
		notEmpty = ErrSubscriptionNotEmpty
	}
	result := DeleteResult{}
	err := e.inTx(ctx, func(ctx context.Context, tx *txn) error {
		result = DeleteResult{}
		q, err := e.readQueueTx(ctx, tx, name)
		if err != nil {
			if kind == "subscription" && errors.Is(err, ErrQueueNotFound) {
				return notFound
			}
			return err
		}
		if q.kind != kind {
			if kind == "subscription" {
				return notFound
			}
			return fmt.Errorf("%w: DeleteQueue only supports plain queues, not subscriptions", ErrInvalidArgument)
		}
		if kind == "subscription" {
			var mapped int
			err := tx.QueryRowContext(ctx,
				`SELECT 1 FROM subscriptions WHERE topic=? AND subscription=?`, topic, name).Scan(&mapped)
			if errors.Is(err, sql.ErrNoRows) {
				return notFound
			}
			if err != nil {
				return err
			}
		}
		now := e.now()
		ttlAuthorized := kind == "subscription" && q.expiresAtMs > 0 && q.expiresAtMs <= now
		if q.lifecycleState == "active" && !opts.Force && !ttlAuthorized {
			var occupied bool
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS(`+messageExistenceUnion("?")+`)`, repeatedQueueArgs(name)...).Scan(&occupied); err != nil {
				return err
			}
			if occupied {
				return notEmpty
			}
		}
		if q.lifecycleState == "active" {
			startedAt := now
			if ttlAuthorized {
				startedAt = q.expiresAtMs
			}
			if _, err := tx.ExecContext(ctx, `UPDATE queues SET lifecycle_state='deleting',delete_started_at=? WHERE name=? AND lifecycle_state='active'`, startedAt, name); err != nil {
				return err
			}
		}
		budget := cleanupBudget{rows: lifecycleBatchRows, bytes: lifecycleBatchBytes}
		done, err := e.cleanEntityTx(ctx, tx, name, kind, &budget)
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
	if errors.Is(err, ErrOutcomeUnknown) {
		e.signalUncertainLifecycle()
	} else if err == nil && !result.Deleted {
		e.signalLifecycle()
	}
	return result, err
}

// cleanEmptySubscriptionsTx retires a burst of empty deleting subscriptions in
// two set-based statements. This is the common path for temporary request/reply
// subscriptions, and keeps a simultaneous TTL wave from paying one transaction
// per name. Mapping rows are deleted first and backing queues second in the same
// transaction; exact row counts make the operation safe even when foreign keys
// are disabled, and any mismatch rolls the whole transaction back.
func (e *Engine) cleanEmptySubscriptionsTx(ctx context.Context, tx *txn, budget *cleanupBudget) ([]string, bool, error) {
	limit := lifecycleEmptySubBatch
	if byRows := budget.rows / 2; byRows < limit {
		limit = byRows
	}
	if limit <= 0 {
		return nil, false, nil
	}
	rows, err := tx.QueryContext(ctx, emptySubscriptionCandidatesSQL, limit)
	if err != nil {
		return nil, false, err
	}
	names := make([]string, 0, limit)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			return nil, false, err
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, false, err
	}
	if err := rows.Close(); err != nil {
		return nil, false, err
	}
	if len(names) == 0 {
		return nil, false, nil
	}
	args := make([]any, len(names))
	for i := range names {
		args[i] = names[i]
	}
	placeholders := strings.TrimRight(strings.Repeat("?,", len(names)), ",")
	mappingResult, err := tx.ExecContext(ctx,
		`DELETE FROM subscriptions WHERE subscription IN (`+placeholders+`)`, args...)
	if err != nil {
		return nil, false, err
	}
	mappingRows, err := mappingResult.RowsAffected()
	if err != nil {
		return nil, false, err
	}
	if mappingRows != int64(len(names)) {
		return nil, false, ErrSubscriptionNotFound
	}
	queueResult, err := tx.ExecContext(ctx, `DELETE FROM queues
		WHERE kind='subscription' AND lifecycle_state='deleting'
		  AND name IN (`+placeholders+`)`, args...)
	if err != nil {
		return nil, false, err
	}
	queueRows, err := queueResult.RowsAffected()
	if err != nil {
		return nil, false, err
	}
	if queueRows != int64(len(names)) {
		return nil, false, ErrQueueNotFound
	}
	budget.rows -= 2 * len(names)
	for _, name := range names {
		tx.forgetQueue(name)
	}
	return names, len(names) == limit, nil
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

// cleanEntityTx never cascades a message backlog. The queues row reserves the
// name until bounded deletion has emptied every entity-owned table. Subscription
// metadata is retired explicitly as mapping then backing queue in this transaction;
// remote libSQL deployments need not enforce SQLite foreign keys for correctness.
// A normal subscription therefore consumes two physical rows from the shared budget.
func (e *Engine) cleanEntityTx(ctx context.Context, tx *txn, name, kind string, budget *cleanupBudget) (bool, error) {
	if budget.rows <= 0 || budget.bytes <= 0 {
		return false, nil
	}
	metadataRows := 1
	if kind == "subscription" {
		metadataRows = 2
	}
	// Empty resources are common (for example a request queue drained before
	// deletion). Retire one directly instead of probing all four owned tables
	// separately. The queue row remains the name reservation until every table
	// is empty.
	if budget.rows >= metadataRows {
		done, removed, err := e.deleteEntityMetadataTx(ctx, tx, name, kind, true)
		if err != nil {
			return false, err
		}
		if done {
			budget.rows -= removed
			tx.forgetQueue(name)
			return true, nil
		}
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
	// Metadata counts toward the physical row cap. If the final owned table left
	// too few slots, keep the reservation for the next pass. A subscription needs
	// one slot for queues plus one for its explicit subscriptions row.
	if budget.rows < metadataRows {
		return false, nil
	}
	done, removed, err := e.deleteEntityMetadataTx(ctx, tx, name, kind, false)
	if err != nil {
		return false, err
	}
	if !done {
		return false, ErrQueueNotFound
	}
	budget.rows -= removed
	tx.forgetQueue(name)
	return true, nil
}

// deleteEntityMetadataTx performs the final metadata retirement without relying
// on a foreign-key cascade. When requireEmpty is true, the subscription mapping is
// removed only if its backing queue has no owned rows, so an unfinished fast-path
// probe cannot expose an orphan backing queue. Any failure after mapping deletion
// rolls the surrounding transaction back.
func (e *Engine) deleteEntityMetadataTx(ctx context.Context, tx *txn, name, kind string, requireEmpty bool) (bool, int, error) {
	removed := int64(0)
	if kind == "subscription" {
		query := `DELETE FROM subscriptions WHERE subscription=?`
		args := []any{name}
		if requireEmpty {
			query += ` AND EXISTS (
				SELECT 1 FROM queues
				WHERE name=? AND kind='subscription' AND lifecycle_state='deleting'
				  AND NOT EXISTS (` + messageExistenceUnion("queues.name") + `)
				  AND NOT EXISTS (SELECT 1 FROM dedup WHERE queue=queues.name)
				  AND NOT EXISTS (SELECT 1 FROM settlement_receipts WHERE queue=queues.name)
				  AND NOT EXISTS (SELECT 1 FROM receive_attempts WHERE queue=queues.name))`
			args = append(args, name)
		}
		res, err := tx.ExecContext(ctx, query, args...)
		if err != nil {
			return false, 0, err
		}
		removed, err = res.RowsAffected()
		if err != nil {
			return false, 0, err
		}
		if requireEmpty && removed == 0 {
			return false, 0, nil
		}
		if removed != 1 {
			return false, 0, ErrSubscriptionNotFound
		}
	}

	query := `DELETE FROM queues WHERE name=? AND kind=? AND lifecycle_state='deleting'`
	if requireEmpty && kind != "subscription" {
		query += `
			AND NOT EXISTS (` + messageExistenceUnion("queues.name") + `)
			AND NOT EXISTS (SELECT 1 FROM dedup WHERE queue=queues.name)
			AND NOT EXISTS (SELECT 1 FROM settlement_receipts WHERE queue=queues.name)
			AND NOT EXISTS (SELECT 1 FROM receive_attempts WHERE queue=queues.name)`
	}
	res, err := tx.ExecContext(ctx, query, name, kind)
	if err != nil {
		return false, 0, err
	}
	queueRows, err := res.RowsAffected()
	if err != nil {
		return false, 0, err
	}
	if queueRows == 0 {
		if removed != 0 {
			// Returning an error is essential: it rolls back the mapping DELETE.
			return false, 0, ErrQueueNotFound
		}
		return false, 0, nil
	}
	if queueRows != 1 {
		return false, 0, ErrQueueNotFound
	}
	if kind == "subscription" && removed != 1 {
		// A subscription is exactly two physical metadata rows. Treat a missing
		// mapping as corruption rather than committing a one-row retirement.
		return false, 0, ErrSubscriptionNotFound
	}
	return true, int(queueRows + removed), nil
}

// maintainLifecycle applies one global row/byte/resource budget, so a burst of
// forced deletions cannot monopolize the single writer. Each pass first retires
// a bounded set of empty subscription metadata, then spends the remaining budget
// on loaded resources. Local stores process up to 32 loaded resources per
// transaction. A remote store processes one: even bounded rows spread across
// many resources would otherwise turn into hundreds of sequential Hrana round
// trips while holding the write transaction. A durable composite cursor preserves
// fairness across passes. Within a local pass each selected queue receives a fair
// slice before unfinished queues reuse spare capacity.
type lifecyclePassOutcome struct {
	retry bool
	fast  bool
}

func (e *Engine) maintainLifecyclePassDetailed(ctx context.Context) lifecyclePassOutcome {
	type resource struct {
		name      string
		kind      string
		startedAt int64
	}
	var touched []string
	fastPathSelected := false
	fastPathFull := false
	hadResources := false
	err := e.inTx(ctx, func(ctx context.Context, tx *txn) error {
		touched = nil
		fastPathSelected = false
		fastPathFull = false
		hadResources = false
		budget := cleanupBudget{rows: lifecyclePassRows, bytes: lifecycleBatchBytes}
		fastNames, full, err := e.cleanEmptySubscriptionsTx(ctx, tx, &budget)
		if err != nil {
			return err
		}
		touched = append(touched, fastNames...)
		fastPathSelected = len(fastNames) > 0 || full
		fastPathFull = full
		resourceLimit := lifecycleResourceLimit(e.db.remote)
		cursorAt, cursorName := int64(math.MinInt64), ""
		var encoded string
		err = tx.QueryRowContext(ctx, `SELECT value FROM meta WHERE key=?`, lifecycleCursorAtKey).Scan(&encoded)
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
				if err := rows.Scan(&r.name, &r.kind, &r.startedAt); err != nil {
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
		if err := read(`SELECT name,kind,delete_started_at FROM queues
			WHERE lifecycle_state='deleting' AND (delete_started_at,name)>(?,?)
			ORDER BY delete_started_at,name LIMIT ?`, cursorAt, cursorName, resourceLimit); err != nil {
			return err
		}
		if remaining := resourceLimit - len(resources); remaining > 0 {
			if err := read(`SELECT name,kind,delete_started_at FROM queues
				WHERE lifecycle_state='deleting' AND (delete_started_at,name)<=(?,?)
				ORDER BY delete_started_at,name LIMIT ?`, cursorAt, cursorName, remaining); err != nil {
				return err
			}
		}
		hadResources = len(resources) > 0

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
				done, err := e.cleanEntityTx(ctx, tx, r.name, r.kind, &local)
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
	if errors.Is(err, ErrOutcomeUnknown) {
		// The cleanup commit may have landed even though its acknowledgement did
		// not. A different remote connection can briefly return an older empty
		// inventory, so keep probing through the bounded uncertainty window.
		e.signalUncertainLifecycle()
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
	return lifecyclePassOutcome{
		retry: err != nil || fastPathSelected || hadResources,
		fast:  err == nil && fastPathFull,
	}
}

func (e *Engine) maintainLifecyclePass(ctx context.Context) bool {
	return e.maintainLifecyclePassDetailed(ctx).retry
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
