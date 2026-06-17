package engine

import (
	"context"
	"database/sql"
	"strings"
	"time"
)

// dlqRec is a dead-lettered row carried across a cross-queue move.
type dlqRec struct {
	id                                                         int64
	body                                                       []byte
	messageID, correlationID, sessionID, ctype, subject, props sql.NullString
}

// Redrive moves dead-lettered messages back to active for reprocessing (§11.2).
//
//   - Same-queue redrive is an in-place UPDATE preserving rowid (FIFO replay order).
//   - Cross-queue redrive re-INSERTs into the target (fresh rowid) and deletes the
//     originals. The whole move is ONE transaction (atomic): a crash mid-redrive
//     leaves every message either fully moved or fully in the DLQ — never half.
//   - RatePerSec>0 throttles the move into per-second chunks (each chunk atomic),
//     so draining a huge DLQ does not flood the target. RatePerSec<=0 moves
//     everything in a single atomic transaction.
func (e *Engine) Redrive(ctx context.Context, dlq string, opts RedriveOptions) (int, error) {
	if _, err := e.loadQueue(ctx, dlq); err != nil {
		return 0, err
	}
	crossQueue := opts.Target != "" && opts.Target != dlq
	if crossQueue {
		if _, err := e.loadQueue(ctx, opts.Target); err != nil {
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

	// Select the candidate set up front (single writer → the set is stable).
	selQuery := "SELECT id,body,message_id,correlation_id,session_id,content_type,subject,properties" +
		" FROM messages WHERE " + where + " ORDER BY id" + limit
	rows, err := e.db.query(ctx, selQuery, args...)
	if err != nil {
		return 0, err
	}
	var recs []dlqRec
	for rows.Next() {
		var r dlqRec
		if err := rows.Scan(&r.id, &r.body, &r.messageID, &r.correlationID, &r.sessionID,
			&r.ctype, &r.subject, &r.props); err != nil {
			rows.Close()
			return 0, err
		}
		recs = append(recs, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(recs) == 0 {
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
		if err := e.moveBatch(ctx, opts.Target, dlq, crossQueue, batch, now); err != nil {
			return moved, err
		}
		moved += len(batch)
		if opts.RatePerSec > 0 && end < len(recs) {
			time.Sleep(time.Second) // throttle to ~RatePerSec
		}
	}

	notifyQ := dlq
	if crossQueue {
		notifyQ = opts.Target
	}
	e.note.notify(notifyQ)
	return moved, nil
}

// moveBatch atomically redrives one batch of dead-lettered rows.
func (e *Engine) moveBatch(ctx context.Context, target, dlq string, crossQueue bool, batch []dlqRec, now int64) error {
	ids := make([]any, len(batch))
	ph := make([]string, len(batch))
	for i, r := range batch {
		ids[i] = r.id
		ph[i] = "?"
	}
	inClause := "(" + strings.Join(ph, ",") + ")"

	return e.inTx(ctx, func(tx *sql.Tx) error {
		if !crossQueue {
			_, err := tx.ExecContext(ctx, `
				UPDATE messages SET state='active', delivery_count=0, lock_token=NULL,
				    locked_until=0, visible_at=0, dead_letter_reason=NULL, dead_letter_description=NULL
				 WHERE id IN `+inClause, ids...)
			return err
		}
		for _, r := range batch {
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO messages
				  (queue,state,visible_at,locked_until,lock_token,delivery_count,enqueued_at,expires_at,
				   message_id,correlation_id,session_id,content_type,subject,properties,body)
				VALUES (?, 'active', 0, 0, NULL, 0, ?, 0, ?,?,?,?,?,?,?)`,
				target, now, r.messageID, r.correlationID, r.sessionID, r.ctype, r.subject, r.props, r.body); err != nil {
				return err
			}
		}
		_, err := tx.ExecContext(ctx, `DELETE FROM messages WHERE id IN `+inClause, ids...)
		return err
	})
}

// PurgeDeadLetter permanently deletes dead-lettered messages from a queue's DLQ
// (§7.3 PurgeDeadLetter). Use after a redrive, or to discard poison messages that
// will never be reprocessed. opts.Max / opts.OlderThanMs scope the purge; both
// zero purges the whole DLQ. Returns the number of rows deleted.
func (e *Engine) PurgeDeadLetter(ctx context.Context, queue string, opts RedriveOptions) (int, error) {
	if _, err := e.loadQueue(ctx, queue); err != nil {
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
	res, err := e.db.exec(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}
