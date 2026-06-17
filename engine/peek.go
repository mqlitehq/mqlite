package engine

import (
	"context"
	"database/sql"
)

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
	rows, err := e.db.query(ctx, `
		SELECT id,state,body,message_id,session_id,correlation_id,subject,content_type,
		       properties,delivery_count,enqueued_at,visible_at,locked_until,
		       dead_letter_reason,dead_letter_description
		FROM messages WHERE queue=? AND id>=?`+stateClause+`
		ORDER BY id ASC LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*PeekedMessage
	for rows.Next() {
		var p PeekedMessage
		var st string
		var messageID, sessionID, correlationID, subject, ctype, props, dlr, dld sql.NullString
		if err := rows.Scan(&p.SeqNumber, &st, &p.Body, &messageID, &sessionID, &correlationID,
			&subject, &ctype, &props, &p.DeliveryCount, &p.EnqueuedAtMs, &p.VisibleAtMs, &p.LockedUntilMs,
			&dlr, &dld); err != nil {
			return nil, err
		}
		p.State = State(st)
		p.MessageID = messageID.String
		p.SessionID = sessionID.String
		p.CorrelationID = correlationID.String
		p.Subject = subject.String
		p.ContentType = ctype.String
		p.Properties = parseProps(props)
		p.DeadLetterReason = dlr.String
		p.DeadLetterDescription = dld.String
		out = append(out, &p)
	}
	return out, rows.Err()
}

// GetQueueMetrics returns pgmq-style counters for a queue (§7.3).
func (e *Engine) GetQueueMetrics(ctx context.Context, queue string) (Metrics, error) {
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
