package engine

import (
	"context"
	"database/sql"
	"errors"
)

// settlementTTLMs bounds how long a settle receipt survives — long enough to
// cover any sane client retry window, short enough that the table stays tiny
// (the janitor sweeps expired rows).
const settlementTTLMs = 30 * 60 * 1000 // 30 min

func affected(res sql.Result) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrLockLost // wrong/expired token, or already settled
	}
	return nil
}

// settleOp runs a terminal settle (Complete/Abandon/Reject/Defer) idempotently
// under one transaction: it performs the fenced write (do), and
//   - rows affected > 0 → records a receipt keyed by lock_token, returns nil;
//   - rows affected = 0 but a live receipt exists → the client is retrying a
//     settle whose response was lost; returns nil (idempotent success);
//   - rows affected = 0 and no receipt → ErrLockLost (genuine fencing failure).
//
// This is the difference between "I already Completed this, the completion just
// got lost" (success) and "my lock expired and someone else has the message" (lost).
func (e *Engine) settleOp(ctx context.Context, token, op string, do func(tx *sql.Tx) (int64, error)) error {
	if token == "" {
		return ErrLockLost
	}
	now := e.now()
	return e.inTx(ctx, func(tx *sql.Tx) error {
		n, err := do(tx)
		if err != nil {
			return err
		}
		if n == 0 {
			var one int
			err := tx.QueryRowContext(ctx,
				`SELECT 1 FROM settlement_receipts WHERE lock_token=? AND expires_at>?`, token, now).Scan(&one)
			if err == nil {
				return nil // idempotent replay of an already-settled token
			}
			if errors.Is(err, sql.ErrNoRows) {
				return ErrLockLost
			}
			return err
		}
		_, err = tx.ExecContext(ctx,
			`INSERT OR REPLACE INTO settlement_receipts(lock_token,operation,created_at,expires_at)
			 VALUES (?,?,?,?)`, token, op, now, now+settlementTTLMs)
		return err
	})
}

// Complete removes a successfully-processed message (fencing on lock_token).
func (e *Engine) Complete(ctx context.Context, queue string, seq int64, token string) error {
	return e.settleOp(ctx, token, "completed", func(tx *sql.Tx) (int64, error) {
		res, err := tx.ExecContext(ctx,
			`DELETE FROM messages WHERE id=? AND queue=? AND lock_token=?`, seq, queue, token)
		if err != nil {
			return 0, err
		}
		return res.RowsAffected()
	})
}

// Abandon releases the lock and redelivers, or dead-letters if delivery_count
// has reached the queue's max (§8.5). delayMs applies exponential-backoff style
// re-visibility when requeued.
func (e *Engine) Abandon(ctx context.Context, queue string, seq int64, token string, delayMs int64) error {
	now := e.now()
	err := e.settleOp(ctx, token, "abandoned", func(tx *sql.Tx) (int64, error) {
		res, err := tx.ExecContext(ctx, `
			UPDATE messages SET
			    state = CASE WHEN delivery_count >= (SELECT max_delivery_count FROM queues WHERE name=messages.queue)
			                 THEN 'dead_lettered' ELSE 'active' END,
			    locked_until = 0,
			    lock_token   = NULL,
			    visible_at = CASE WHEN delivery_count >= (SELECT max_delivery_count FROM queues WHERE name=messages.queue)
			                      THEN visible_at ELSE ? END,
			    dead_letter_reason = CASE WHEN delivery_count >= (SELECT max_delivery_count FROM queues WHERE name=messages.queue)
			                              THEN 'MaxDeliveryCountExceeded' ELSE dead_letter_reason END
			 WHERE id=? AND queue=? AND lock_token=?`,
			now+delayMs, seq, queue, token)
		if err != nil {
			return 0, err
		}
		return res.RowsAffected()
	})
	if err != nil {
		return err
	}
	e.note.notify(queue)
	return nil
}

// Reject moves a locked message to the dead-letter state with a reason.
func (e *Engine) Reject(ctx context.Context, queue string, seq int64, token, reason, desc string) error {
	if reason == "" {
		reason = ReasonAppRequested
	}
	return e.settleOp(ctx, token, "dead_lettered", func(tx *sql.Tx) (int64, error) {
		res, err := tx.ExecContext(ctx, `
			UPDATE messages SET state='dead_lettered', locked_until=0, lock_token=NULL,
			    dead_letter_reason=?, dead_letter_description=?
			 WHERE id=? AND queue=? AND lock_token=?`,
			reason, nz(desc), seq, queue, token)
		if err != nil {
			return 0, err
		}
		return res.RowsAffected()
	})
}

// Defer sets a message aside; it is later retrieved by seq via ReceiveDeferred.
func (e *Engine) Defer(ctx context.Context, queue string, seq int64, token string) error {
	return e.settleOp(ctx, token, "deferred", func(tx *sql.Tx) (int64, error) {
		res, err := tx.ExecContext(ctx,
			`UPDATE messages SET state='deferred', locked_until=0, lock_token=NULL
			   WHERE id=? AND queue=? AND lock_token=?`, seq, queue, token)
		if err != nil {
			return 0, err
		}
		return res.RowsAffected()
	})
}

// Renew extends the lock lease by the queue's lock duration (§11.3).
func (e *Engine) Renew(ctx context.Context, queue string, seq int64, token string) error {
	q, err := e.loadQueue(ctx, queue)
	if err != nil {
		return err
	}
	res, err := e.db.exec(ctx,
		`UPDATE messages SET locked_until=? WHERE id=? AND queue=? AND lock_token=?`,
		e.now()+q.lockDurationMs, seq, queue, token)
	if err != nil {
		return err
	}
	return affected(res)
}

// SettleItem identifies one message to settle in a batch (fenced on LockToken).
type SettleItem struct {
	SeqNumber int64
	LockToken string
}

// SettleResult is the per-item outcome of a batch settle.
type SettleResult struct {
	SeqNumber int64
	Ok        bool // true = settled (or an idempotent replay of an already-settled token)
}

// CompleteBatch completes many messages in one transaction / one round-trip — the
// broker-side fix for the drain N+1 (Receive returns a batch, but settling it
// one-by-one is one HTTP call per message). Each item is fenced on its own
// lock_token and recorded idempotently, exactly like Complete; a per-item failure
// (expired/wrong token) returns Ok=false rather than failing the whole batch.
func (e *Engine) CompleteBatch(ctx context.Context, queue string, items []SettleItem) ([]SettleResult, error) {
	out := make([]SettleResult, len(items))
	now := e.now()
	err := e.inTx(ctx, func(tx *sql.Tx) error {
		for i, it := range items {
			out[i].SeqNumber = it.SeqNumber
			if it.LockToken == "" {
				continue // Ok stays false
			}
			res, err := tx.ExecContext(ctx,
				`DELETE FROM messages WHERE id=? AND queue=? AND lock_token=?`, it.SeqNumber, queue, it.LockToken)
			if err != nil {
				return err
			}
			n, _ := res.RowsAffected()
			if n > 0 {
				if _, err := tx.ExecContext(ctx,
					`INSERT OR REPLACE INTO settlement_receipts(lock_token,operation,created_at,expires_at)
					 VALUES (?,?,?,?)`, it.LockToken, "completed", now, now+settlementTTLMs); err != nil {
					return err
				}
				out[i].Ok = true
				continue
			}
			// rows=0: idempotent replay if a live receipt exists, else the lock is lost.
			var one int
			switch err := tx.QueryRowContext(ctx,
				`SELECT 1 FROM settlement_receipts WHERE lock_token=? AND expires_at>?`, it.LockToken, now).Scan(&one); {
			case err == nil:
				out[i].Ok = true // already completed (lost-response replay)
			case errors.Is(err, sql.ErrNoRows):
				// Ok stays false (lock lost)
			default:
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
