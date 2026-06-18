package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

// recvAttemptTTLFloorMs is the minimum lifetime of a receive-attempt record. The
// real TTL is max(queue lock duration, this) so a replay stays valid at least as
// long as the lock the client is holding.
const recvAttemptTTLFloorMs = 5 * 60 * 1000 // 5 min

// claimRound performs one claim attempt. Without an attempt id it is the plain
// (non-transactional, per-message) claim. With an attempt id the whole round —
// replay-check, claim, and record — runs in one transaction so a client retrying
// a Receive whose response was lost replays the SAME batch instead of claiming
// new messages and burning delivery_count.
func (e *Engine) claimRound(ctx context.Context, q queueRow, max int, mode ReceiveMode, attemptID string) ([]*Message, error) {
	if attemptID == "" {
		return e.claimUpTo(ctx, q, max, mode)
	}
	now := e.now()
	var out []*Message
	err := e.inTx(ctx, func(tx *sql.Tx) error {
		if msgs, ok, err := lookupAttempt(ctx, tx, q.name, attemptID, now); err != nil {
			return err
		} else if ok {
			out = msgs
			return nil
		}
		msgs, err := e.claimUpToTx(ctx, tx, q, max, mode, now)
		if err != nil {
			return err
		}
		if len(msgs) > 0 {
			ttl := q.lockDurationMs
			if ttl < recvAttemptTTLFloorMs {
				ttl = recvAttemptTTLFloorMs
			}
			if err := storeAttempt(ctx, tx, q.name, attemptID, msgs, now, now+ttl); err != nil {
				return err
			}
		}
		out = msgs
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func lookupAttempt(ctx context.Context, tx *sql.Tx, queue, attemptID string, now int64) ([]*Message, bool, error) {
	var blob string
	err := tx.QueryRowContext(ctx,
		`SELECT response FROM receive_attempts WHERE queue=? AND attempt_id=? AND expires_at>?`,
		queue, attemptID, now).Scan(&blob)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var msgs []*Message
	if err := json.Unmarshal([]byte(blob), &msgs); err != nil {
		return nil, false, err
	}
	return msgs, true, nil
}

func storeAttempt(ctx context.Context, tx *sql.Tx, queue, attemptID string, msgs []*Message, now, expires int64) error {
	blob, err := json.Marshal(msgs)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx,
		`INSERT OR REPLACE INTO receive_attempts(queue,attempt_id,response,created_at,expires_at)
		 VALUES (?,?,?,?,?)`, queue, attemptID, string(blob), now, expires)
	return err
}

// claimUpToTx is claimUpTo bound to an explicit transaction (idempotent receive).
func (e *Engine) claimUpToTx(ctx context.Context, tx *sql.Tx, q queueRow, max int, mode ReceiveMode, now int64) ([]*Message, error) {
	var out []*Message
	for i := 0; i < max; i++ {
		m, err := e.claimOneTx(ctx, tx, q, now)
		if err != nil {
			return out, err
		}
		if m == nil {
			break
		}
		if mode == ReceiveAndDelete {
			if _, err := tx.ExecContext(ctx,
				`DELETE FROM messages WHERE id=? AND lock_token=?`, m.SeqNumber, m.LockToken); err != nil {
				return out, err
			}
			m.LockToken = ""
		}
		out = append(out, m)
	}
	return out, nil
}

func (e *Engine) claimOneTx(ctx context.Context, tx *sql.Tx, q queueRow, now int64) (*Message, error) {
	token := randToken()
	lockUntil := now + q.lockDurationMs
	var (
		m                                                          Message
		groupID, messageID, correlationID, replyTo, subject, ctype sql.NullString
		props                                                      sql.NullString
	)
	err := tx.QueryRowContext(ctx, claimSQLFor(q.ordering), lockUntil, token, q.name, now, now, now).Scan(
		&m.SeqNumber, &m.Body, &m.DeliveryCount, &groupID, &messageID,
		&correlationID, &replyTo, &subject, &ctype, &props, &m.EnqueuedAtMs, &m.LockedUntilMs)
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
