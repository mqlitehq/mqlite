package engine

import (
	"context"
	"database/sql"
)

// maxResponseBytes bounds the total body bytes a single Peek/Receive response accumulates.
// Their item caps (Peek 1000, Receive 256) times the 1 MiB default body allow a legal request
// to materialize ~1.3 GiB and OOM a small VM; this stops after the message that crosses the
// budget (so a single large message is still returned, and only claimed messages are locked —
// MQLITE-80). Peek pages past it with PeekOptions.FromSeq. A var so tests can lower it.
var maxResponseBytes int64 = 32 << 20 // 32 MiB

// Peek browses messages without locking or settling them (§7.3). Useful for
// triage and for recovering the seq_number of a deferred message.
func (e *Engine) Peek(ctx context.Context, queue string, opts PeekOptions) ([]*PeekedMessage, error) {
	max := opts.Max
	if max <= 0 {
		max = 32
	}
	if max > 1000 {
		max = 1000
	}
	args := []any{queue, opts.FromSeq}
	stateClause := ""
	if opts.State != "" {
		stateClause = " AND state=?"
		args = append(args, string(opts.State))
	}
	args = append(args, max)
	var out []*PeekedMessage
	var bytes int64
	err := e.db.queryRows(ctx, `
		SELECT id,state,body,message_id,group_id,correlation_id,reply_to,subject,content_type,
		       properties,delivery_count,enqueued_at,visible_at,expires_at,locked_until,
		       dead_letter_reason,dead_letter_description
		FROM messages WHERE queue=? AND id>=?`+stateClause+`
		ORDER BY id ASC LIMIT ?`, func(rows *sql.Rows) error {
		for rows.Next() {
			var p PeekedMessage
			var st string
			var messageID, groupID, correlationID, replyTo, subject, ctype, props, dlr, dld sql.NullString
			if err := rows.Scan(&p.SeqNumber, &st, &p.Body, &messageID, &groupID, &correlationID,
				&replyTo, &subject, &ctype, &props, &p.DeliveryCount, &p.EnqueuedAtMs, &p.VisibleAtMs, &p.ExpiresAtMs, &p.LockedUntilMs,
				&dlr, &dld); err != nil {
				return err
			}
			p.State = State(st)
			p.MessageID = messageID.String
			p.GroupID = groupID.String
			p.CorrelationID = correlationID.String
			p.ReplyTo = replyTo.String
			p.Subject = subject.String
			p.ContentType = ctype.String
			p.Properties = parseProps(props)
			p.DeadLetterReason = dlr.String
			p.DeadLetterDescription = dld.String
			out = append(out, &p)
			// Bound the response size; page past this with FromSeq (MQLITE-80).
			bytes += int64(len(p.Body))
			if bytes >= maxResponseBytes {
				break
			}
		}
		return rows.Err()
	}, args...)
	return out, err
}

// Stats returns pgmq-style counters for a queue (§7.3).
func (e *Engine) Stats(ctx context.Context, queue string) (Metrics, error) {
	if _, err := e.loadQueue(ctx, queue); err != nil {
		return Metrics{}, err
	}
	m := Metrics{Queue: queue}
	var oldest sql.NullInt64
	err := e.db.queryRowScan(ctx,
		[]any{&m.Active, &m.Locked, &m.Deferred, &m.Scheduled, &m.DeadLettered, &m.Total, &oldest}, `
		SELECT
		    COALESCE(SUM(CASE WHEN state='active'        THEN 1 ELSE 0 END),0),
		    COALESCE(SUM(CASE WHEN state='locked'        THEN 1 ELSE 0 END),0),
		    COALESCE(SUM(CASE WHEN state='deferred'      THEN 1 ELSE 0 END),0),
		    COALESCE(SUM(CASE WHEN state='scheduled'     THEN 1 ELSE 0 END),0),
		    COALESCE(SUM(CASE WHEN state='dead_lettered' THEN 1 ELSE 0 END),0),
		    COUNT(*),
		    MIN(CASE WHEN state IN ('active','locked') THEN enqueued_at END)
		FROM messages WHERE queue=?`, queue)
	if err != nil {
		return Metrics{}, err
	}
	if oldest.Valid {
		if age := e.now() - oldest.Int64; age > 0 {
			m.OldestMessageAgeMs = age
		}
	}
	return m, nil
}
