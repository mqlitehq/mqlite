package engine

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

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
	now := e.now()

	where := "queue=? AND state='dead_lettered'"
	args := []any{dlq}
	if opts.OlderThanMs > 0 {
		where += " AND enqueued_at < ?"
		args = append(args, now-opts.OlderThanMs)
	}
	limit := ""
	if opts.Max > 0 {
		limit = " LIMIT ?"
		args = append(args, opts.Max)
	}

	// Capture candidate IDs only. Other calls may purge, claim or redrive them
	// before a move transaction starts; every batch rechecks queue and DLQ state.
	selQuery := "SELECT id" +
		" FROM messages WHERE " + where + " ORDER BY id" + limit
	var ids []int64
	if err := e.db.queryRows(ctx, selQuery, func(rows *sql.Rows) error {
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				return err
			}
			ids = append(ids, id)
		}
		return rows.Err()
	}, args...); err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}

	chunk := len(ids)
	if opts.RatePerSec > 0 && opts.RatePerSec < chunk {
		chunk = opts.RatePerSec
	}
	moved := 0
	for start := 0; start < len(ids); start += chunk {
		end := start + chunk
		if end > len(ids) {
			end = len(ids)
		}
		batch := ids[start:end]
		n, err := e.moveBatch(ctx, opts.Target, dlq, crossQueue, batch, sourceQueue.generation, targetQueue.generation)
		if err != nil {
			return moved, err
		}
		moved += n
		if n > 0 {
			notifyQ := dlq
			if crossQueue {
				notifyQ = opts.Target
			}
			e.note.notify(notifyQ)
		}
		if opts.RatePerSec > 0 && end < len(ids) {
			timer := time.NewTimer(time.Second)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return moved, ctx.Err()
			}
		}
	}

	return moved, nil
}

// moveBatch atomically redrives a candidate ID set. IDs are advisory: a competing
// purge/redrive/claim may have removed or changed a row before writer admission.
func (e *Engine) moveBatch(ctx context.Context, target, dlq string, crossQueue bool, batch []int64, sourceGeneration, targetGeneration string) (int, error) {
	moved := int64(0)
	err := e.inTx(ctx, func(ctx context.Context, tx *txn) error {
		moved = 0 // A remote retry rolls back and repeats the whole transaction.
		if _, err := e.checkQueueLiveTx(ctx, tx, dlq, sourceGeneration); err != nil {
			return err
		}
		if crossQueue {
			if _, err := e.checkQueueLiveTx(ctx, tx, target, targetGeneration); err != nil {
				return err
			}
		}
		now := e.now()
		// Stay below SQLite's host-parameter limit without splitting the atomic
		// move into separate commits. SQL copies current bodies directly.
		const idsPerStatement = 1000
		for start := 0; start < len(batch); start += idsPerStatement {
			end := start + idsPerStatement
			if end > len(batch) {
				end = len(batch)
			}
			args := make([]any, 1, end-start+1)
			args[0] = dlq
			for _, id := range batch[start:end] {
				args = append(args, id)
			}
			predicate := "queue=? AND state='dead_lettered' AND id IN (" + strings.TrimRight(strings.Repeat("?,", end-start), ",") + ")"
			if !crossQueue {
				res, err := tx.ExecContext(ctx, `UPDATE messages SET state='active',delivery_count=0,lock_token=NULL,
                    locked_until=0,visible_at=0,dead_letter_reason=NULL,dead_letter_description=NULL
                    WHERE `+predicate, args...)
				if err != nil {
					return err
				}
				n, err := res.RowsAffected()
				if err != nil {
					return err
				}
				moved += n
				continue
			}
			insertArgs := append([]any{target, now}, args...)
			inserted, err := tx.ExecContext(ctx, `INSERT INTO messages
                (queue,state,visible_at,locked_until,lock_token,delivery_count,enqueued_at,expires_at,
                 message_id,correlation_id,reply_to,group_id,content_type,subject,properties,body)
                SELECT ?, 'active',0,0,NULL,0,?,0,
                    message_id,correlation_id,reply_to,group_id,content_type,subject,properties,body
                FROM messages WHERE `+predicate+` ORDER BY id`, insertArgs...)
			if err != nil {
				return err
			}
			n, err := inserted.RowsAffected()
			if err != nil {
				return err
			}
			deleted, err := tx.ExecContext(ctx, `DELETE FROM messages WHERE `+predicate, args...)
			if err != nil {
				return err
			}
			removed, err := deleted.RowsAffected()
			if err != nil {
				return err
			}
			if removed != n {
				return fmt.Errorf("redrive copied %d rows but removed %d", n, removed)
			}
			moved += n
		}
		tx.record(dlq, "redriven", moved)
		if crossQueue {
			tx.record(target, "enqueued", moved)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return int(moved), nil
}

// Purge permanently deletes dead-lettered messages from a queue's DLQ (§7.3).
// Use after a redrive, or to discard poison messages that will never be
// reprocessed. opts.Max / opts.OlderThanMs scope the purge; both zero purges the
// whole DLQ. Returns the number of rows deleted.
func (e *Engine) Purge(ctx context.Context, queue string, opts RedriveOptions) (int, error) {
	if err := opts.validate(); err != nil {
		return 0, err
	}
	q, err := e.loadQueue(ctx, queue)
	if err != nil {
		return 0, err
	}
	where := "queue=? AND state='dead_lettered'"
	args := []any{queue}
	if opts.OlderThanMs > 0 {
		where += " AND enqueued_at < ?"
		args = append(args, e.now()-opts.OlderThanMs)
	}
	query := `DELETE FROM messages WHERE ` + where
	if opts.Max > 0 {
		query = `DELETE FROM messages WHERE id IN (SELECT id FROM messages WHERE ` + where + ` ORDER BY id LIMIT ?)`
		args = append(args, opts.Max)
	}
	var n int64
	err = e.inTx(ctx, func(ctx context.Context, tx *txn) error {
		if _, err := e.checkQueueLiveTx(ctx, tx, queue, q.generation); err != nil {
			return err
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
