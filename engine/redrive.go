package engine

import (
	"context"
	"database/sql"
)

// Redrive moves dead-lettered messages back to active for reprocessing (§11.2).
// Same-queue redrive is an in-place UPDATE preserving rowid (FIFO replay order).
// Cross-queue redrive must re-INSERT to take a fresh target rowid (and deletes
// the originals), because rowid is a global primary key.
func (e *Engine) Redrive(ctx context.Context, dlq string, opts RedriveOptions) (int, error) {
	if _, err := e.loadQueue(ctx, dlq); err != nil {
		return 0, err
	}
	now := e.now()
	crossQueue := opts.Target != "" && opts.Target != dlq
	if crossQueue {
		if _, err := e.loadQueue(ctx, opts.Target); err != nil {
			return 0, err
		}
	}

	// build the selection predicate shared by both paths.
	where := "queue=? AND state='dead_lettered'"
	args := []any{dlq}
	if opts.OlderThanMs > 0 {
		where += " AND enqueued_at < ?"
		args = append(args, now-opts.OlderThanMs)
	}
	limit := ""
	if opts.Max > 0 {
		limit = " LIMIT ?"
	}

	if !crossQueue {
		q := `UPDATE messages SET state='active', delivery_count=0, lock_token=NULL,
		           locked_until=0, visible_at=0, dead_letter_reason=NULL, dead_letter_description=NULL
		       WHERE id IN (SELECT id FROM messages WHERE ` + where + ` ORDER BY id` + limit + `)`
		a := append([]any{}, args...)
		if opts.Max > 0 {
			a = append(a, opts.Max)
		}
		res, err := e.db.exec(ctx, q, a...)
		if err != nil {
			return 0, err
		}
		n, _ := res.RowsAffected()
		if n > 0 {
			e.note.notify(dlq)
		}
		return int(n), nil
	}

	// cross-queue: select, re-INSERT into target with a new rowid, delete original.
	selA := append([]any{}, args...)
	if opts.Max > 0 {
		selA = append(selA, opts.Max)
	}
	rows, err := e.db.query(ctx, `
		SELECT id,body,message_id,correlation_id,session_id,content_type,subject,properties
		FROM messages WHERE `+where+` ORDER BY id`+limit, selA...)
	if err != nil {
		return 0, err
	}
	type rec struct {
		id                                                         int64
		body                                                       []byte
		messageID, correlationID, sessionID, ctype, subject, props sql.NullString
	}
	var recs []rec
	for rows.Next() {
		var r rec
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

	moved := 0
	for _, r := range recs {
		err := e.inTx(ctx, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO messages
				  (queue,state,visible_at,locked_until,lock_token,delivery_count,enqueued_at,expires_at,
				   message_id,correlation_id,session_id,content_type,subject,properties,body)
				VALUES (?, 'active', 0, 0, NULL, 0, ?, 0, ?,?,?,?,?,?,?)`,
				opts.Target, now, r.messageID, r.correlationID, r.sessionID, r.ctype, r.subject, r.props, r.body); err != nil {
				return err
			}
			_, err := tx.ExecContext(ctx, `DELETE FROM messages WHERE id=?`, r.id)
			return err
		})
		if err != nil {
			return moved, err
		}
		moved++
	}
	if moved > 0 {
		e.note.notify(opts.Target)
	}
	return moved, nil
}
