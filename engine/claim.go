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

// claimSQLFor picks the claim statement for a queue's ordering mode. strict_fifo
// uses the global head-of-line variant; standard and group_fifo share the
// per-group FIFO statement (their claim eligibility is identical — group_fifo
// only differs by requiring a GroupID at send time).
func claimSQLFor(ordering OrderingMode) string {
	if ordering == OrderStrictFIFO {
		return claimStrictSQL
	}
	return claimSQL
}

// claimSQL atomically locks the eligible head message (design §5.2 + §11.1).
// Only state='active' is claimable — this keeps the hot path on the partial
// idx_msg_active(queue,id) index (O(log n) even with a deep backlog). Expired
// locks are returned to 'active' by the reaper (§8.8), so visibility-timeout
// redelivery happens within the reaper interval rather than on the claim path.
// group_id IS NULL  -> the message is its own group (never group-blocked);
// otherwise the group head is released only when no earlier same-group message
// is still locked/deferred/scheduled (in-order, FIFO-per-group).
const claimSQL = `
UPDATE messages
   SET state='locked', locked_until=?, lock_token=?, delivery_count=delivery_count+1
 WHERE id = (
   SELECT m.id FROM messages m
    WHERE m.queue=? AND m.state='active'
      AND m.visible_at<=? AND (m.expires_at=0 OR m.expires_at>?)
      AND ( m.group_id IS NULL
         OR NOT (  -- MQLITE-22: one EXISTS per in-flight state, each a single
                   -- state= equality so SQLite seeks the covering index
                   -- idx_msg_group_inflight(queue,group_id,state,locked_until) by
                   -- its (queue,group_id,state) prefix instead of a backward rowid
                   -- scan. (group_id IS NULL short-circuits first, so these run
                   -- only for grouped messages.) Do NOT merge back into one
                   -- NOT EXISTS with state IN(...) / an OR of states: that plans as
                   -- a rowid scan, O(n) per candidate, O(n^2) to drain a deep
                   -- blocked backlog — the r1 incident, on the ordered path.
              EXISTS ( SELECT 1 FROM messages b
                        WHERE b.queue=m.queue AND b.group_id=m.group_id
                          AND b.state='deferred' AND b.id < m.id )
           OR EXISTS ( SELECT 1 FROM messages b
                        WHERE b.queue=m.queue AND b.group_id=m.group_id
                          AND b.state='scheduled' AND b.id < m.id )
           OR EXISTS ( SELECT 1 FROM messages b
                        WHERE b.queue=m.queue AND b.group_id=m.group_id
                          AND b.state='locked' AND b.locked_until>? AND b.id < m.id ) ) )
    ORDER BY m.id ASC LIMIT 1)
RETURNING id, body, delivery_count, group_id, message_id, correlation_id,
          reply_to, subject, content_type, properties, enqueued_at, locked_until`

// claimStrictSQL is the strict_fifo variant: identical to claimSQL except the
// per-group head condition is replaced by a *global* head-of-line block — a
// message is claimable only when no earlier id in the queue is still in flight
// (locked/deferred/scheduled), regardless of group. The whole queue therefore
// delivers strictly one-at-a-time in id order. Parameter layout is identical to
// claimSQL (lockUntil, token, queue, now, now, now) so claimOne just swaps the
// SQL string per the queue's ordering mode.
const claimStrictSQL = `
UPDATE messages
   SET state='locked', locked_until=?, lock_token=?, delivery_count=delivery_count+1
 WHERE id = (
   SELECT m.id FROM messages m
    WHERE m.queue=? AND m.state='active'
      AND m.visible_at<=? AND (m.expires_at=0 OR m.expires_at>?)
      AND NOT (  -- MQLITE-22: one EXISTS per in-flight state (single state=
                 -- equality each) so each seeks an index instead of a backward
                 -- rowid scan: deferred -> idx_msg_deferred(queue,id),
                 -- scheduled -> idx_msg_sched_head(queue,id), locked ->
                 -- idx_msg_locked_head(queue,id). Same reasoning as claimSQL; do
                 -- NOT collapse back.
              EXISTS ( SELECT 1 FROM messages b
                        WHERE b.queue=m.queue
                          AND b.state='deferred' AND b.id < m.id )
           OR EXISTS ( SELECT 1 FROM messages b
                        WHERE b.queue=m.queue
                          AND b.state='scheduled' AND b.id < m.id )
           OR EXISTS ( SELECT 1 FROM messages b
                        WHERE b.queue=m.queue
                          AND b.state='locked' AND b.locked_until>? AND b.id < m.id ) )
    ORDER BY m.id ASC LIMIT 1)
RETURNING id, body, delivery_count, group_id, message_id, correlation_id,
          reply_to, subject, content_type, properties, enqueued_at, locked_until`

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
		m                                                          Message
		groupID, messageID, correlationID, replyTo, subject, ctype sql.NullString
		props                                                      sql.NullString
	)
	err := e.db.queryRowScan(ctx,
		[]any{&m.SeqNumber, &m.Body, &m.DeliveryCount, &groupID, &messageID,
			&correlationID, &replyTo, &subject, &ctype, &props, &m.EnqueuedAtMs, &m.LockedUntilMs},
		claimSQLFor(q.ordering), lockUntil, token, q.name, now, now, now)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	m.LockToken = token
	m.GroupID = groupID.String
	m.MessageID = messageID.String
	m.CorrelationID = correlationID.String
	m.ReplyTo = replyTo.String
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
			m                                                          Message
			groupID, messageID, correlationID, replyTo, subject, ctype sql.NullString
			props                                                      sql.NullString
		)
		err := e.db.queryRowScan(ctx,
			[]any{&m.SeqNumber, &m.Body, &m.DeliveryCount, &groupID, &messageID,
				&correlationID, &replyTo, &subject, &ctype, &props, &m.EnqueuedAtMs, &m.LockedUntilMs}, `
			UPDATE messages
			   SET state='locked', locked_until=?, lock_token=?, delivery_count=delivery_count+1
			 WHERE id=? AND queue=? AND state='deferred'
			RETURNING id, body, delivery_count, group_id, message_id, correlation_id,
			          reply_to, subject, content_type, properties, enqueued_at, locked_until`,
			lockUntil, token, seq, queue)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return out, err
		}
		m.LockToken = token
		m.GroupID = groupID.String
		m.MessageID = messageID.String
		m.CorrelationID = correlationID.String
		m.ReplyTo = replyTo.String
		m.Subject = subject.String
		m.ContentType = ctype.String
		m.Properties = parseProps(props)
		out = append(out, &m)
	}
	return out, nil
}
