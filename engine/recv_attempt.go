package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type attemptLookupState uint8

const (
	attemptMissing attemptLookupState = iota
	attemptReplayable
	attemptInvalidated
)

// recvAttemptTTLFloorMs is the minimum lifetime of a receive-attempt record. The
// real TTL is max(queue lock duration, this) so a replay stays valid at least as
// long as the lock the client is holding.
const recvAttemptTTLFloorMs = 5 * 60 * 1000 // 5 min

// receiveAttemptFormatVersion identifies the compact envelope stored in the
// existing receive_attempts.response column. Versioned JSON lets upgraded
// brokers replay pre-upgrade full-message arrays until they expire without a
// schema migration.
const receiveAttemptFormatVersion = 1

type storedReceiveAttempt struct {
	Version  int                  `json:"version"`
	Mode     ReceiveMode          `json:"mode"`
	Leases   []storedReceiveLease `json:"leases,omitempty"`
	Messages []*Message           `json:"messages,omitempty"`
}

type storedReceiveLease struct {
	SeqNumber     int64  `json:"seq_number"`
	LockToken     string `json:"lock_token"`
	LockedUntilMs int64  `json:"locked_until_ms"`
}

// claimRound performs one transactional claim attempt. With an attempt id the whole round —
// replay-check, lease validation, claim, and record — runs in one transaction so
// a client retrying a Receive whose response was lost replays the same still-owned
// batch, and can never receive a stale lock after recovery or lease expiry.
func (e *Engine) claimRound(ctx context.Context, q queueRow, max int, mode ReceiveMode, attemptID string) ([]*Message, error) {
	if attemptID == "" {
		return e.claimUpTo(ctx, q, max, mode)
	}
	var out []*Message
	err := e.inTx(ctx, func(ctx context.Context, tx *txn) error {
		// A remote transaction callback may be replayed after a definite failed
		// statement or commit. Never retain output or refreshed metadata from a
		// previous transaction attempt.
		out = nil
		current := q
		claimNow := e.now()
		first, err := e.claimFirstTx(ctx, tx, current, attemptID, claimNow)
		if err != nil {
			return err
		}
		var msgs []*Message
		if first != nil {
			msgs, err = e.claimUpToLiveTxFrom(ctx, tx, current, max, mode, first)
		} else {
			// The fenced UPDATE returning zero can mean replay, stale cached
			// configuration, an empty queue, or a deleted/expired queue. Classify
			// only after the UPDATE has established write intent, so these reads
			// cannot create a deferred read snapshot that later fails to upgrade.
			current, err = e.readQueueTx(ctx, tx, q.name)
			if err != nil {
				return err
			}
			if err := queueLiveAt(current, e.now()); err != nil {
				return err
			}
			// Use the same instant as the UPDATE guard. If a live attempt caused
			// that statement to return zero, advancing the clock here must not
			// turn the same transaction into an empty response.
			replay, state, lookupErr := lookupAttempt(ctx, tx, q.name, attemptID, mode, claimNow)
			if lookupErr != nil {
				return lookupErr
			}
			if state == attemptReplayable {
				out = replay
				return nil
			}
			// A stale lease-bearing attempt is removed in this transaction. Claim
			// immediately with the same attempt ID instead of returning an empty
			// response that could be mistaken for a definite replay result.
			if state == attemptMissing && claimConfigEqual(q, current) {
				return nil
			}
			msgs, err = e.claimUpToLiveTx(ctx, tx, current, max, mode)
		}
		if err != nil {
			return err
		}
		if len(msgs) > 0 {
			// Preserve the existing attempt lifetime for both modes. Peek-Lock
			// needs it to cover the lease; ReceiveAndDelete has historically used
			// the same queue-scoped replay window even though it grants no lease.
			ttl := current.lockDurationMs
			if ttl < recvAttemptTTLFloorMs {
				ttl = recvAttemptTTLFloorMs
			}
			now := e.now()
			if err := queueLiveAt(current, now); err != nil {
				return err
			}
			expires := boundedDeadlineMs(now, ttl, current.expiresAtMs)
			if err := storeAttempt(ctx, tx, current.name, attemptID, mode, msgs, now, expires); err != nil {
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

// claimConfigEqual compares only metadata consumed by the claim statement. A
// mismatch makes the first fenced UPDATE return zero so the caller can refresh
// and continue in the same write transaction. Other queue configuration does
// not affect ordering, lease calculation or subscription lifetime.
func claimConfigEqual(cached, current queueRow) bool {
	return cached.lockDurationMs == current.lockDurationMs &&
		cached.ordering == current.ordering &&
		cached.expiresAtMs == current.expiresAtMs
}

func lookupAttempt(ctx context.Context, tx *txn, queue, attemptID string, mode ReceiveMode, now int64) ([]*Message, attemptLookupState, error) {
	var blob string
	err := tx.QueryRowContext(ctx,
		`SELECT response FROM receive_attempts WHERE queue=? AND attempt_id=? AND expires_at>?`,
		queue, attemptID, now).Scan(&blob)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, attemptMissing, nil
	}
	if err != nil {
		return nil, attemptMissing, err
	}
	stored, err := decodeStoredReceiveAttempt(blob)
	if err != nil {
		return nil, attemptMissing, err
	}
	if stored.Mode != mode {
		if stored.Mode == PeekLock {
			return nil, attemptMissing, fmt.Errorf("%w: receive attempt was created for peek-lock mode", ErrInvalidArgument)
		}
		return nil, attemptMissing, fmt.Errorf("%w: receive attempt was created for receive-and-delete mode", ErrInvalidArgument)
	}
	if mode == ReceiveAndDelete {
		return stored.Messages, attemptReplayable, nil
	}

	// Peek-Lock stores only ownership coordinates. The message rows are immutable
	// while locked, so one set query both validates every token and reconstructs
	// the original response without duplicating bodies in receive_attempts.
	expected := make(map[int64]storedReceiveLease, len(stored.Leases))
	placeholders := make([]string, len(stored.Leases))
	args := make([]any, 0, len(stored.Leases)+1)
	args = append(args, queue)
	for i, lease := range stored.Leases {
		if lease.SeqNumber <= 0 || lease.LockToken == "" || lease.LockedUntilMs <= 0 {
			return nil, attemptMissing, errors.New("mqlite: stored receive attempt contains an invalid lease")
		}
		if _, duplicate := expected[lease.SeqNumber]; duplicate {
			return nil, attemptMissing, errors.New("mqlite: stored receive attempt contains a duplicate message")
		}
		expected[lease.SeqNumber] = lease
		placeholders[i] = "?"
		args = append(args, lease.SeqNumber)
	}
	rows, err := tx.QueryContext(ctx, `SELECT id,body,delivery_count,group_id,message_id,
		correlation_id,reply_to,subject,content_type,properties,enqueued_at,locked_until,lock_token FROM messages
		WHERE queue=? AND state='locked' AND id IN (`+strings.Join(placeholders, ",")+`)`, args...)
	if err != nil {
		return nil, attemptMissing, err
	}
	actual := make(map[int64]*Message, len(stored.Leases))
	for rows.Next() {
		var msg Message
		var groupID, messageID, correlationID, replyTo, subject, ctype, props sql.NullString
		if err := rows.Scan(&msg.SeqNumber, &msg.Body, &msg.DeliveryCount, &groupID, &messageID,
			&correlationID, &replyTo, &subject, &ctype, &props, &msg.EnqueuedAtMs, &msg.LockedUntilMs, &msg.LockToken); err != nil {
			_ = rows.Close()
			return nil, attemptMissing, err
		}
		msg.GroupID = groupID.String
		msg.MessageID = messageID.String
		msg.CorrelationID = correlationID.String
		msg.ReplyTo = replyTo.String
		msg.Subject = subject.String
		msg.ContentType = ctype.String
		msg.Properties = parseProps(props)
		actual[msg.SeqNumber] = &msg
	}
	if err := rows.Close(); err != nil {
		return nil, attemptMissing, err
	}
	if err := rows.Err(); err != nil {
		return nil, attemptMissing, err
	}
	valid := len(actual) == len(expected)
	if valid {
		for seq, lease := range expected {
			current, ok := actual[seq]
			if !ok || current.LockToken != lease.LockToken || current.LockedUntilMs <= now {
				valid = false
				break
			}
		}
	}
	if valid {
		msgs := make([]*Message, len(stored.Leases))
		for i, lease := range stored.Leases {
			msg := actual[lease.SeqNumber]
			// Preserve the deadline observed in the original response. A Renew may
			// have extended the row since then, but replay remains the same response.
			msg.LockedUntilMs = lease.LockedUntilMs
			msgs[i] = msg
		}
		return msgs, attemptReplayable, nil
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM receive_attempts WHERE queue=? AND attempt_id=?`, queue, attemptID); err != nil {
		return nil, attemptMissing, err
	}
	return nil, attemptInvalidated, nil
}

func decodeStoredReceiveAttempt(blob string) (storedReceiveAttempt, error) {
	trimmed := strings.TrimSpace(blob)
	if trimmed == "" {
		return storedReceiveAttempt{}, errors.New("mqlite: stored receive attempt has an empty response")
	}
	if trimmed[0] == '[' {
		// Before compact envelopes, both modes stored the complete []*Message.
		// Convert that representation in memory so live attempts survive upgrade.
		var msgs []*Message
		if err := json.Unmarshal([]byte(trimmed), &msgs); err != nil {
			return storedReceiveAttempt{}, err
		}
		if len(msgs) == 0 {
			return storedReceiveAttempt{}, errors.New("mqlite: stored receive attempt has an empty response")
		}
		tokenless, tokenBearing := true, true
		for _, msg := range msgs {
			if msg == nil {
				return storedReceiveAttempt{}, errors.New("mqlite: stored receive attempt contains a null message")
			}
			if msg.LockToken == "" {
				tokenBearing = false
			} else {
				tokenless = false
			}
		}
		if !tokenless && !tokenBearing {
			return storedReceiveAttempt{}, errors.New("mqlite: stored receive attempt mixes leased and deleted messages")
		}
		if tokenless {
			for _, msg := range msgs {
				if msg.LockedUntilMs != 0 {
					return storedReceiveAttempt{}, errors.New("mqlite: stored receive-and-delete attempt contains a lease deadline")
				}
			}
			return storedReceiveAttempt{Version: receiveAttemptFormatVersion, Mode: ReceiveAndDelete, Messages: msgs}, nil
		}
		leases := make([]storedReceiveLease, len(msgs))
		for i, msg := range msgs {
			leases[i] = storedReceiveLease{SeqNumber: msg.SeqNumber, LockToken: msg.LockToken, LockedUntilMs: msg.LockedUntilMs}
		}
		return storedReceiveAttempt{Version: receiveAttemptFormatVersion, Mode: PeekLock, Leases: leases}, nil
	}
	if trimmed[0] != '{' {
		return storedReceiveAttempt{}, errors.New("mqlite: stored receive attempt has an unknown format")
	}
	var stored storedReceiveAttempt
	if err := json.Unmarshal([]byte(trimmed), &stored); err != nil {
		return storedReceiveAttempt{}, err
	}
	if stored.Version != receiveAttemptFormatVersion {
		return storedReceiveAttempt{}, fmt.Errorf("mqlite: unsupported stored receive attempt version %d", stored.Version)
	}
	switch stored.Mode {
	case PeekLock:
		if len(stored.Leases) == 0 || len(stored.Messages) != 0 {
			return storedReceiveAttempt{}, errors.New("mqlite: stored peek-lock attempt has an invalid response")
		}
	case ReceiveAndDelete:
		if len(stored.Messages) == 0 || len(stored.Leases) != 0 {
			return storedReceiveAttempt{}, errors.New("mqlite: stored receive-and-delete attempt has an invalid response")
		}
		for _, msg := range stored.Messages {
			if msg == nil || msg.LockToken != "" || msg.LockedUntilMs != 0 {
				return storedReceiveAttempt{}, errors.New("mqlite: stored receive-and-delete attempt contains a lease")
			}
		}
	default:
		return storedReceiveAttempt{}, fmt.Errorf("mqlite: stored receive attempt has unknown mode %d", stored.Mode)
	}
	return stored, nil
}

func storeAttempt(ctx context.Context, tx *txn, queue, attemptID string, mode ReceiveMode, msgs []*Message, now, expires int64) error {
	stored := storedReceiveAttempt{Version: receiveAttemptFormatVersion, Mode: mode}
	if mode == ReceiveAndDelete {
		stored.Messages = msgs
	} else {
		stored.Leases = make([]storedReceiveLease, len(msgs))
		for i, msg := range msgs {
			stored.Leases[i] = storedReceiveLease{
				SeqNumber: msg.SeqNumber, LockToken: msg.LockToken, LockedUntilMs: msg.LockedUntilMs,
			}
		}
	}
	blob, err := json.Marshal(stored)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx,
		`INSERT OR REPLACE INTO receive_attempts(queue,attempt_id,response,created_at,expires_at)
		 VALUES (?,?,?,?,?)`, queue, attemptID, string(blob), now, expires)
	return err
}

// claimUpToTx is claimUpTo bound to an explicit transaction (idempotent receive).
func (e *Engine) claimUpToTx(ctx context.Context, tx *txn, q queueRow, max int, mode ReceiveMode) ([]*Message, error) {
	first, err := e.claimFirstTx(ctx, tx, q, "", e.now())
	if err != nil {
		return nil, err
	}
	if first != nil {
		return e.claimUpToLiveTxFrom(ctx, tx, q, max, mode, first)
	}
	// Zero rows needs a read to distinguish a genuinely empty queue from a
	// stale/deleting/expired one. The UPDATE above already made this a write
	// transaction, avoiding a deferred read-snapshot upgrade.
	current, err := e.checkQueueLiveTx(ctx, tx, q.name)
	if err != nil {
		return nil, err
	}
	if claimConfigEqual(q, current) {
		return nil, nil
	}
	return e.claimUpToLiveTx(ctx, tx, current, max, mode)
}

// claimUpToLiveTx claims a batch after its caller has already established a
// write transaction and loaded current queue configuration.
func (e *Engine) claimUpToLiveTx(ctx context.Context, tx *txn, q queueRow, max int, mode ReceiveMode) ([]*Message, error) {
	return e.claimUpToLiveTxFrom(ctx, tx, q, max, mode, nil)
}

// claimUpToLiveTxFrom continues a batch after an optional first claim. Only the
// first statement in the transaction needs the queue fence and write intent;
// the same transaction prevents lifecycle/configuration changes while the
// remaining messages use the smaller hot-path statement.
func (e *Engine) claimUpToLiveTxFrom(ctx context.Context, tx *txn, q queueRow, max int, mode ReceiveMode, first *Message) ([]*Message, error) {
	var out []*Message
	var bytes int64
	for i := 0; i < max; i++ {
		m := first
		first = nil
		if m == nil {
			var err error
			m, err = e.claimOneTx(ctx, tx, q)
			if err != nil {
				return out, err
			}
		}
		if m == nil {
			break
		}
		if mode == ReceiveAndDelete {
			if _, err := tx.ExecContext(ctx,
				`DELETE FROM messages WHERE id=? AND lock_token=?`, m.SeqNumber, m.LockToken); err != nil {
				return out, err
			}
			tx.record(q.name, "receive_deleted", 1)
			m.LockToken = ""
			m.LockedUntilMs = 0 // The row is gone; neither response nor attempt replay grants a lease.
		}
		out = append(out, m)
		// Stop once the response reaches the byte budget: we only ever claim messages we
		// return (no claim-then-truncate), so the un-claimed rest stays active for the next
		// Receive, and a single legal batch can't materialize gigabytes (MQLITE-80).
		bytes += int64(len(m.Body))
		if bytes >= maxResponseBytes {
			break
		}
	}
	return out, nil
}

// claimFirstTx is the first storage statement in both ordinary and AttemptID
// Receive transactions. It deliberately does not pre-read queue metadata: the
// fenced UPDATE proves the cached claim fields still describe a live queue, or
// returns no row for the caller to classify after write intent is established.
func (e *Engine) claimFirstTx(ctx context.Context, tx *txn, q queueRow, attemptID string, now int64) (*Message, error) {
	token := randToken()
	// Placeholder order follows the composed SQL fragments: lockUntil, token,
	// queue, queueNow, lockDuration, ordering, expiresAt, then (for AttemptID)
	// attemptID, attemptNow, followed by messageNow twice.
	args := []any{
		queueLockUntil(q, now), token,
		q.name, now, q.lockDurationMs, string(q.ordering), q.expiresAtMs,
	}
	if attemptID != "" {
		args = append(args, attemptID, now)
	}
	args = append(args, now, now)
	return e.claimRowTx(ctx, tx, q.name, token, firstClaimSQLFor(q.ordering, attemptID != ""), args...)
}

func (e *Engine) claimOneTx(ctx context.Context, tx *txn, q queueRow) (*Message, error) {
	// Read after acquiring the writer and for each item: a pool wait, retry, or
	// preceding claim must not consume this item's lease or reuse stale TTL eligibility.
	now := e.now()
	if err := queueLiveAt(q, now); err != nil {
		return nil, err
	}
	token := randToken()
	lockUntil := queueLockUntil(q, now)
	return e.claimRowTx(ctx, tx, q.name, token, claimSQLFor(q.ordering), lockUntil, token, q.name, now, now)
}

func (e *Engine) claimRowTx(ctx context.Context, tx *txn, queue, token, query string, args ...any) (*Message, error) {
	var (
		m                                                          Message
		groupID, messageID, correlationID, replyTo, subject, ctype sql.NullString
		props                                                      sql.NullString
	)
	err := tx.QueryRowContext(ctx, query, args...).Scan(
		&m.SeqNumber, &m.Body, &m.DeliveryCount, &groupID, &messageID,
		&correlationID, &replyTo, &subject, &ctype, &props, &m.EnqueuedAtMs, &m.LockedUntilMs)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	tx.record(queue, "delivered", 1)
	if m.DeliveryCount > 1 {
		tx.record(queue, "redelivered", 1)
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
