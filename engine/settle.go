package engine

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
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

// markCompleted bumps the per-queue lifetime "completed" counter by n. It is
// called only after a settle's transaction has committed AND actually removed
// rows (n>0), so an idempotent lost-response replay — which deletes nothing —
// never double-counts. (MQLITE-54)
func (e *Engine) markCompleted(queue string, n int64) {
	if n <= 0 {
		return
	}
	v, ok := e.processed.Load(queue)
	if !ok {
		v, _ = e.processed.LoadOrStore(queue, new(atomic.Uint64))
	}
	v.(*atomic.Uint64).Add(uint64(n))
}

// CompletedCounts snapshots the lifetime completed-message count per queue.
// In-process and rough (resets on restart); surfaced as mqlite_messages_completed_total.
func (e *Engine) CompletedCounts() map[string]uint64 {
	out := map[string]uint64{}
	e.processed.Range(func(k, v any) bool {
		out[k.(string)] = v.(*atomic.Uint64).Load()
		return true
	})
	return out
}

// Complete removes a successfully-processed message (fencing on lock_token).
func (e *Engine) Complete(ctx context.Context, queue string, seq int64, token string) error {
	var removed int64
	err := e.settleOp(ctx, token, "completed", func(tx *sql.Tx) (int64, error) {
		res, err := tx.ExecContext(ctx,
			`DELETE FROM messages WHERE id=? AND queue=? AND lock_token=?`, seq, queue, token)
		if err != nil {
			return 0, err
		}
		// Assignment (not +=) keeps this retry-safe: the remote inTx may replay
		// the closure, and we want the last attempt's count, not the sum.
		removed, err = res.RowsAffected()
		return removed, err
	})
	if err == nil {
		e.markCompleted(queue, removed)
	}
	return err
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

// RenewBatch extends the lock lease of many messages in ONE transaction, returning a per-item
// result so the caller can see exactly which leases still hold.
//
// Renewing a batch message-by-message does not scale: over a network each Renew is a separate
// round trip, so N messages × the link latency can easily exceed the lease itself and the locks
// expire *while they are being renewed* — a 64-message batch on a 50ms link needs 3.2s of
// renewals against a 2s lease and loses most of them (review round-3). One request, one
// transaction, one lease deadline for the whole batch.
//
// Like Renew, an item whose lock was already lost simply comes back Ok=false; it is not an
// error for the batch. Fencing is on LockToken, exactly as in CompleteBatch.
func (e *Engine) RenewBatch(ctx context.Context, queue string, items []SettleItem) ([]SettleResult, error) {
	q, err := e.loadQueue(ctx, queue)
	if err != nil {
		return nil, err
	}
	out := make([]SettleResult, len(items))
	err = e.inTx(ctx, func(tx *sql.Tx) error {
		for i := range out {
			out[i] = SettleResult{} // the remote inTx may replay the closure — never inherit Ok
		}
		until := e.now() + q.lockDurationMs
		for i, it := range items {
			out[i].SeqNumber = it.SeqNumber
			if it.LockToken == "" {
				continue // Ok stays false
			}
			res, err := tx.ExecContext(ctx,
				`UPDATE messages SET locked_until=? WHERE id=? AND queue=? AND lock_token=?`,
				until, it.SeqNumber, queue, it.LockToken)
			if err != nil {
				return err
			}
			if n, _ := res.RowsAffected(); n > 0 {
				out[i].Ok = true
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
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
	var removed int64 // messages actually deleted this commit (for the completed counter)
	err := e.inTx(ctx, func(tx *sql.Tx) error {
		removed = 0 // reset per attempt: the remote inTx may replay the closure
		for i := range out {
			out[i] = SettleResult{} // ditto: a replayed attempt must not inherit Ok flags
		}
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
				removed++
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
	e.markCompleted(queue, removed)
	return out, nil
}
