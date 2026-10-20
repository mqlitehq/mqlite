package engine

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// dlqRec identifies a candidate discovered before a throttled move. Its state
// is rechecked in each move transaction; carrying a body snapshot here could
// resurrect a row another operation already purged or redrove.
type dlqRec struct{ id int64 }

// Redrive moves dead-lettered messages back to active for reprocessing (§11.2).
//
//   - Same-queue redrive is an in-place UPDATE preserving rowid (FIFO replay order).
//   - Cross-queue redrive re-INSERTs into the target (fresh rowid) and deletes the
//     originals. The whole move is ONE transaction (atomic): a crash mid-redrive
//     leaves every message either fully moved or fully in the DLQ — never half.
//   - RatePerSec>0 throttles the move into per-second chunks (each chunk atomic),
//     so draining a huge DLQ does not flood the target. RatePerSec<=0 moves
//     everything in a single atomic transaction.
//
// validate rejects negative bounds. A negative Max/OlderThanMs/RatePerSec would otherwise
// skip its `> 0` guard below and silently REMOVE the restriction — turning a bounded
// redrive/purge into an unrestricted destructive op (review 2026-07-12 P1-2). Rejected at
// this boundary so the CLI, the SDK, and a raw HTTP request are all covered.
func (o RedriveOptions) validate() error {
	switch {
	case o.Max < 0:
		return fmt.Errorf("%w: max must be >= 0 (got %d)", ErrInvalidArgument, o.Max)
	case o.OlderThanMs < 0:
		return fmt.Errorf("%w: older-than must be >= 0", ErrInvalidArgument)
	case o.RatePerSec < 0:
		return fmt.Errorf("%w: rate must be >= 0 (got %d)", ErrInvalidArgument, o.RatePerSec)
	}
	return nil
}

func (e *Engine) Redrive(ctx context.Context, dlq string, opts RedriveOptions) (int, error) {
	if err := opts.validate(); err != nil {
		return 0, err
	}
	sourceQueue, err := e.loadQueue(ctx, dlq)
	if err != nil {
		return 0, err
	}
	crossQueue := opts.Target != "" && opts.Target != dlq
	var targetQueue queueRow
	if crossQueue {
		targetQueue, err = e.loadQueue(ctx, opts.Target)
		if err != nil {
			return 0, err
		}
	}
	operationExpiresAt := sourceQueue.expiresAtMs
	if crossQueue && targetQueue.expiresAtMs > 0 &&
		(operationExpiresAt == 0 || targetQueue.expiresAtMs < operationExpiresAt) {
		operationExpiresAt = targetQueue.expiresAtMs
	}
	where := "queue=? AND state='dead_lettered'"
	if opts.OlderThanMs > 0 {
		where += " AND enqueued_at < ?"
	}
	limit := ""
	if opts.Max > 0 {
		limit = " LIMIT ?"
	}

	// Select a bounded candidate set up front. Every chunk revalidates these IDs
	// transactionally because a throttled remote operation can observe other work.
	selQuery := "SELECT id" +
		" FROM messages WHERE " + where + " ORDER BY id" + limit
	var recs []dlqRec
	if err := e.db.queryFresh(ctx, selQuery, func() []any {
		args := []any{dlq}
		if opts.OlderThanMs > 0 {
			// Build the cutoff only after acquiring a connection and again on
			// every remote retry. A delayed attempt must not use an older view
			// of a relative age boundary.
			args = append(args, e.now()-opts.OlderThanMs)
		}
		if opts.Max > 0 {
			args = append(args, opts.Max)
		}
		return args
	}, func(rows *sql.Rows) error {
		// A retry calls scan again, so discard any partial prior-attempt rows.
		recs = recs[:0]
		for rows.Next() {
			var r dlqRec
			if err := rows.Scan(&r.id); err != nil {
				return err
			}
			recs = append(recs, r)
		}
		return rows.Err()
	}); err != nil {
		return 0, err
	}
	if len(recs) == 0 {
		// The preflight may have raced the fixed expiration while the read was
		// waiting for a remote connection. Preserve the queue-not-found boundary
		// even for an otherwise empty redrive.
		if _, err := e.loadQueue(ctx, dlq); err != nil {
			return 0, err
		}
		if crossQueue {
			if _, err := e.loadQueue(ctx, opts.Target); err != nil {
				return 0, err
			}
		}
		return 0, nil
	}

	chunk := len(recs)
	if opts.RatePerSec > 0 && opts.RatePerSec < chunk {
		chunk = opts.RatePerSec
	}
	moved := 0
	for start := 0; start < len(recs); start += chunk {
		end := start + chunk
		if end > len(recs) {
			end = len(recs)
		}
		batch := recs[start:end]
		n, err := e.moveBatch(ctx, opts.Target, dlq, crossQueue, batch)
		if err != nil {
			return moved, err
		}
		moved += n
		if opts.RatePerSec > 0 && end < len(recs) {
			if err := e.waitRedriveThrottle(ctx, operationExpiresAt); err != nil {
				return moved, err
			}
		}
	}

	notifyQ := dlq
	if crossQueue {
		notifyQ = opts.Target
	}
	e.note.notify(notifyQ)
	return moved, nil
}

// waitRedriveThrottle keeps rate limiting interruptible and never parks beyond
// the fixed lifetime of either side of a cross-queue move. Each following chunk
// still rechecks both queue rows transactionally.
func (e *Engine) waitRedriveThrottle(ctx context.Context, expiresAtMs int64) error {
	wait := time.Second
	if expiresAtMs > 0 {
		remaining := expiresAtMs - e.now()
		if remaining <= 0 {
			return ErrQueueNotFound
		}
		// Convert only when the result is below the one-second cap. A valid
		// absolute lifetime can be near MaxInt64, where milliseconds converted
		// directly to time.Duration would overflow and make NewTimer panic.
		if remaining < int64(time.Second/time.Millisecond) {
			wait = time.Duration(remaining) * time.Millisecond
		}
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		if expiresAtMs > 0 && expiresAtMs <= e.now() {
			return ErrQueueNotFound
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-e.closed:
		return ErrClosed
	}
}

// moveBatch atomically redrives one batch of dead-lettered rows.
func (e *Engine) moveBatch(ctx context.Context, target, dlq string, crossQueue bool, batch []dlqRec) (int, error) {
	ids := make([]any, len(batch))
	ph := make([]string, len(batch))
	for i, r := range batch {
		ids[i] = r.id
		ph[i] = "?"
	}
	inClause := "(" + strings.Join(ph, ",") + ")"

	var moved int64
	err := e.inTx(ctx, func(ctx context.Context, tx *txn) error {
		moved = 0 // remote retries replay this closure
		sourceQueue, err := e.checkQueueLiveTx(ctx, tx, dlq)
		if err != nil {
			return err
		}
		var targetQueue queueRow
		if crossQueue {
			targetQueue, err = e.checkQueueLiveTx(ctx, tx, target)
			if err != nil {
				return err
			}
		}
		now := e.now()
		if err := queueLiveAt(sourceQueue, now); err != nil {
			return err
		}
		if crossQueue {
			if err := queueLiveAt(targetQueue, now); err != nil {
				return err
			}
		}
		if !crossQueue {
			updateArgs := append([]any{dlq}, ids...)
			res, err := tx.ExecContext(ctx, `
				UPDATE messages SET state='active', delivery_count=0, lock_token=NULL,
				    locked_until=0, visible_at=0, dead_letter_reason=NULL, dead_letter_description=NULL
				 WHERE queue=? AND state='dead_lettered' AND id IN `+inClause, updateArgs...)
			if err == nil {
				moved, err = res.RowsAffected()
				if err == nil {
					tx.record(dlq, "redriven", moved)
				}
			}
			return err
		}

		// Re-read candidates in this transaction. A throttled redrive deliberately
		// sleeps between chunks, so a snapshot captured before that sleep is not
		// authority to insert a message that has since left the source DLQ.
		insertArgs := append([]any{target, now, dlq}, ids...)
		res, err := tx.ExecContext(ctx, `
			INSERT INTO messages
			  (queue,state,visible_at,locked_until,lock_token,delivery_count,enqueued_at,expires_at,
			   message_id,correlation_id,reply_to,group_id,content_type,subject,properties,body)
			SELECT ?, 'active', 0, 0, NULL, 0, ?, 0,
			       message_id,correlation_id,reply_to,group_id,content_type,subject,properties,body
			  FROM messages
			 WHERE queue=? AND state='dead_lettered' AND id IN `+inClause+`
			 ORDER BY id`, insertArgs...)
		if err != nil {
			return err
		}
		moved, err = res.RowsAffected()
		if err != nil {
			return err
		}
		deleteArgs := append([]any{dlq}, ids...)
		res, err = tx.ExecContext(ctx,
			`DELETE FROM messages WHERE queue=? AND state='dead_lettered' AND id IN `+inClause,
			deleteArgs...)
		if err != nil {
			return err
		}
		removed, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if removed != moved {
			return fmt.Errorf("redrive source changed inside transaction: inserted %d, removed %d", moved, removed)
		}
		tx.record(dlq, "redriven", moved)
		tx.record(target, "enqueued", moved)
		return nil
	})
	return int(moved), err
}

// Purge permanently deletes dead-lettered messages from a queue's DLQ (§7.3).
// Use after a redrive, or to discard poison messages that will never be
// reprocessed. opts.Max / opts.OlderThanMs scope the purge; both zero purges the
// whole DLQ. Returns the number of rows deleted.
func (e *Engine) Purge(ctx context.Context, queue string, opts RedriveOptions) (int, error) {
	if err := opts.validate(); err != nil {
		return 0, err
	}
	if _, err := e.loadQueue(ctx, queue); err != nil {
		return 0, err
	}
	where := "queue=? AND state='dead_lettered'"
	if opts.OlderThanMs > 0 {
		where += " AND enqueued_at < ?"
	}
	query := `DELETE FROM messages WHERE ` + where
	if opts.Max > 0 {
		query = `DELETE FROM messages WHERE id IN (SELECT id FROM messages WHERE ` + where + ` ORDER BY id LIMIT ?)`
	}
	var n int64
	err := e.inTx(ctx, func(ctx context.Context, tx *txn) error {
		q, err := e.checkQueueLiveTx(ctx, tx, queue)
		if err != nil {
			return err
		}
		now := e.now()
		if err := queueLiveAt(q, now); err != nil {
			return err
		}
		args := []any{queue}
		if opts.OlderThanMs > 0 {
			args = append(args, now-opts.OlderThanMs)
		}
		if opts.Max > 0 {
			args = append(args, opts.Max)
		}
		res, err := tx.ExecContext(ctx, query, args...)
		if err != nil {
			return err
		}
		n, err = res.RowsAffected()
		if err == nil {
			tx.record(queue, "purged", n)
		}
		return err
	})
	if err != nil {
		return 0, err
	}
	return int(n), nil
}
