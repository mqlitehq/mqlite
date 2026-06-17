package engine

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"time"
)

func randToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// claimSQL atomically locks the eligible head message (design §5.2 + §11.1).
// Only state='active' is claimable — this keeps the hot path on the partial
// idx_msg_active(queue,id) index (O(log n) even with a deep backlog). Expired
// locks are returned to 'active' by the reaper (§8.8), so visibility-timeout
// redelivery happens within the reaper interval rather than on the claim path.
// session_id IS NULL  -> the message is its own group (never group-blocked);
// otherwise the group head is released only when no earlier same-group message
// is still locked/deferred/scheduled (in-order, FIFO-per-group).
const claimSQL = `
UPDATE messages
   SET state='locked', locked_until=?, lock_token=?, delivery_count=delivery_count+1
 WHERE id = (
   SELECT m.id FROM messages m
    WHERE m.queue=? AND m.state='active'
      AND m.visible_at<=? AND (m.expires_at=0 OR m.expires_at>?)
      AND ( m.session_id IS NULL
         OR NOT EXISTS (
              SELECT 1 FROM messages b
               WHERE b.queue=m.queue AND b.session_id IS m.session_id
                 AND b.id < m.id
                 AND ( (b.state='locked' AND b.locked_until>?)
                    OR  b.state IN ('deferred','scheduled') ) ) )
    ORDER BY m.id ASC LIMIT 1)
RETURNING id, body, delivery_count, session_id, message_id, correlation_id,
          subject, content_type, properties, enqueued_at, locked_until`

// Receive claims up to opts.MaxMessages messages (Peek-Lock by default), with
// long-poll up to opts.WaitMs (clamped to 20s, §11.3).
func (e *Engine) Receive(ctx context.Context, queue string, opts ReceiveOptions) ([]*Message, error) {
	q, err := e.loadQueue(ctx, queue)
	if err != nil {
		return nil, err
	}
	max := opts.MaxMessages
	if max <= 0 {
		max = 1
	}
	if max > 256 {
		max = 256
	}
	wait := opts.WaitMs
	if wait < 0 {
		wait = 0
	}
	if wait > 20000 {
		wait = 20000
	}
	deadline := e.now() + wait

	for {
		msgs, err := e.claimRound(ctx, q, max, opts.Mode, opts.AttemptID)
		if err != nil {
			return nil, err
		}
		if len(msgs) > 0 || wait == 0 {
			return msgs, nil
		}
		// register a waiter, then re-check to avoid a lost wakeup.
		ch := e.note.wait(queue)
		msgs, err = e.claimRound(ctx, q, max, opts.Mode, opts.AttemptID)
		if err != nil {
			return nil, err
		}
		if len(msgs) > 0 {
			return msgs, nil
		}
		remaining := deadline - e.now()
		if remaining <= 0 {
			return nil, nil
		}
		timer := time.NewTimer(time.Duration(remaining) * time.Millisecond)
		select {
		case <-ch:
			timer.Stop()
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		}
	}
}

func (e *Engine) claimUpTo(ctx context.Context, q queueRow, max int, mode ReceiveMode) ([]*Message, error) {
	var out []*Message
	now := e.now()
	for i := 0; i < max; i++ {
		m, err := e.claimOne(ctx, q, now)
		if err != nil {
			return out, err
		}
		if m == nil {
			break
		}
		if mode == ReceiveAndDelete {
			if _, err := e.db.exec(ctx,
				`DELETE FROM messages WHERE id=? AND lock_token=?`, m.SeqNumber, m.LockToken); err != nil {
				return out, err
			}
			m.LockToken = "" // already removed; not settleable
		}
		out = append(out, m)
	}
	return out, nil
}

func (e *Engine) claimOne(ctx context.Context, q queueRow, now int64) (*Message, error) {
	token := randToken()
	lockUntil := now + q.lockDurationMs
	var (
		m                                                   Message
		sessionID, messageID, correlationID, subject, ctype sql.NullString
		props                                               sql.NullString
	)
	err := e.db.queryRowScan(ctx,
		[]any{&m.SeqNumber, &m.Body, &m.DeliveryCount, &sessionID, &messageID,
			&correlationID, &subject, &ctype, &props, &m.EnqueuedAtMs, &m.LockedUntilMs},
		claimSQL, lockUntil, token, q.name, now, now, now)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	m.LockToken = token
	m.SessionID = sessionID.String
	m.MessageID = messageID.String
	m.CorrelationID = correlationID.String
	m.Subject = subject.String
	m.ContentType = ctype.String
	m.Properties = parseProps(props)
	return &m, nil
}

// ReceiveDeferred locks previously-deferred messages by seq_number (§8.7).
func (e *Engine) ReceiveDeferred(ctx context.Context, queue string, seqs ...int64) ([]*Message, error) {
	q, err := e.loadQueue(ctx, queue)
	if err != nil {
		return nil, err
	}
	now := e.now()
	var out []*Message
	for _, seq := range seqs {
		token := randToken()
		lockUntil := now + q.lockDurationMs
		var (
			m                                                   Message
			sessionID, messageID, correlationID, subject, ctype sql.NullString
			props                                               sql.NullString
		)
		err := e.db.queryRowScan(ctx,
			[]any{&m.SeqNumber, &m.Body, &m.DeliveryCount, &sessionID, &messageID,
				&correlationID, &subject, &ctype, &props, &m.EnqueuedAtMs, &m.LockedUntilMs}, `
			UPDATE messages
			   SET state='locked', locked_until=?, lock_token=?, delivery_count=delivery_count+1
			 WHERE id=? AND queue=? AND state='deferred'
			RETURNING id, body, delivery_count, session_id, message_id, correlation_id,
			          subject, content_type, properties, enqueued_at, locked_until`,
			lockUntil, token, seq, queue)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return out, err
		}
		m.LockToken = token
		m.SessionID = sessionID.String
		m.MessageID = messageID.String
		m.CorrelationID = correlationID.String
		m.Subject = subject.String
		m.ContentType = ctype.String
		m.Properties = parseProps(props)
		out = append(out, &m)
	}
	return out, nil
}
