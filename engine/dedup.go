package engine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
)

func sha256hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// insertOne inserts a single message inside tx, applying dedup gating (§8.6).
// Returns (seq, deduped). When deduped==true the message was silently dropped
// and seq is the original message's seq_number.
func (e *Engine) insertOne(ctx context.Context, tx *sql.Tx, q queueRow, m OutMessage, atMs int64, forced State, now int64) (int64, bool, error) {
	visibleAt := now
	if forced == StateScheduled {
		visibleAt = atMs
	}
	ttl := m.TTLMs
	if ttl <= 0 {
		ttl = q.defaultTTLMs
	}
	var expiresAt int64
	if ttl > 0 {
		expiresAt = visibleAt + ttl // TTL anchored at visible_at (§8.7)
	}

	// dedup gate (only when the queue enabled a window).
	if q.dedupWindowMs > 0 {
		key := m.MessageID
		if key == "" {
			key = sha256hex(m.Body) // content-addressed key
		}
		reqHash := sha256hex(m.Body)
		windowStart := now - q.dedupWindowMs

		var existSeq int64
		var existHash sql.NullString
		err := tx.QueryRowContext(ctx,
			`SELECT seq_number, request_hash FROM dedup
			   WHERE queue=? AND message_id=? AND seen_at > ?`,
			q.name, key, windowStart).Scan(&existSeq, &existHash)
		switch {
		case err == nil:
			if existHash.Valid && existHash.String != reqHash {
				return 0, false, ErrDedupConflict // D23: same key, different body
			}
			return existSeq, true, nil // in-window duplicate -> silent drop
		case errors.Is(err, sql.ErrNoRows):
			// fall through to insert
		default:
			return 0, false, err
		}

		seq, err := e.rawInsert(ctx, tx, q.name, m, visibleAt, expiresAt, forced, now)
		if err != nil {
			return 0, false, err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO dedup (queue,message_id,request_hash,seq_number,seen_at)
			VALUES (?,?,?,?,?)
			ON CONFLICT(queue,message_id) DO UPDATE SET
			    request_hash=excluded.request_hash,
			    seq_number=excluded.seq_number,
			    seen_at=excluded.seen_at`,
			q.name, key, reqHash, seq, now); err != nil {
			return 0, false, err
		}
		return seq, false, nil
	}

	seq, err := e.rawInsert(ctx, tx, q.name, m, visibleAt, expiresAt, forced, now)
	return seq, false, err
}

func (e *Engine) rawInsert(ctx context.Context, tx *sql.Tx, queue string, m OutMessage, visibleAt, expiresAt int64, forced State, now int64) (int64, error) {
	props, err := propsJSON(m.Properties)
	if err != nil {
		return 0, err
	}
	body := m.Body
	if body == nil {
		body = []byte{} // body column is NOT NULL; nil -> empty blob
	}
	var seq int64
	err = tx.QueryRowContext(ctx, `
		INSERT INTO messages
		  (queue,state,visible_at,locked_until,lock_token,delivery_count,enqueued_at,expires_at,
		   message_id,correlation_id,reply_to,group_id,content_type,subject,properties,body)
		VALUES (?,?,?,0,NULL,0,?,?,?,?,?,?,?,?,?,?)
		RETURNING id`,
		queue, string(forced), visibleAt, now, expiresAt,
		nz(m.MessageID), nz(m.CorrelationID), nz(m.ReplyTo), nz(m.GroupID),
		nz(m.ContentType), nz(m.Subject), props, body,
	).Scan(&seq)
	return seq, err
}
