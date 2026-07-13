package engine

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
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
// It is SET-BASED, not a loop of single-row updates, and that is the whole point. Against a
// remote Turso/libSQL store every statement is its own Hrana round trip, so an N-statement
// renewal is N remote round trips — the same O(N) latency we just removed from the client,
// reintroduced one layer down. On a 256-message batch that can outlast even a 30-second lease,
// and because the new deadline is computed once, the transaction would commit leases that had
// already expired while it ran. Two statements, whatever the batch size.
func (e *Engine) RenewBatch(ctx context.Context, queue string, items []SettleItem) ([]SettleResult, error) {
	out := make([]SettleResult, len(items))
	for i, it := range items {
		out[i] = SettleResult{SeqNumber: it.SeqNumber}
	}
	// Only well-formed items can match; an empty token can never fence a row.
	rows := make([]SettleItem, 0, len(items))
	for _, it := range items {
		if it.LockToken != "" {
			rows = append(rows, it)
		}
	}
	if len(rows) == 0 {
		return out, nil
	}
	q, err := e.loadQueue(ctx, queue)
	if err != nil {
		return nil, err
	}

	// (id, lock_token) IN (VALUES (?,?), ...) — a row-value set, so one statement fences every
	// item on its own token exactly as the single-message Renew does.
	pairs := strings.Repeat(",(?,?)", len(rows))[1:]
	args := make([]any, 0, 2+2*len(rows))
	// Keyed by (seq, token), NOT by seq: a caller may pass the same sequence number twice with
	// different tokens, and a seq-keyed result would let the item holding a STALE token inherit
	// the Ok of the one holding the real lock — reporting a lease renewed that was never held.
	renewed := make(map[string]bool, len(rows))
	err = e.inTx(ctx, func(tx *sql.Tx) error {
		for k := range renewed { // the remote inTx may replay the closure — never inherit results
			delete(renewed, k)
		}
		args = args[:0]
		args = append(args, e.now()+q.lockDurationMs, queue)
		for _, it := range rows {
			args = append(args, it.SeqNumber, it.LockToken)
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE messages SET locked_until=? WHERE queue=? AND (id, lock_token) IN (VALUES `+pairs+`)`,
			args...); err != nil {
			return err
		}
		// Which items actually matched? The same row-value set, so this reports exactly the rows
		// the UPDATE touched — an item whose lock was lost or whose token is wrong is simply
		// absent (Ok stays false), which is Renew's contract, not an error.
		sel, err := tx.QueryContext(ctx,
			`SELECT id, lock_token FROM messages WHERE queue=? AND (id, lock_token) IN (VALUES `+pairs+`)`,
			args[1:]...)
		if err != nil {
			return err
		}
		defer sel.Close()
		for sel.Next() {
			var id int64
			var tok string
			if err := sel.Scan(&id, &tok); err != nil {
				return err
			}
			renewed[renewKey(id, tok)] = true
		}
		return sel.Err()
	})
	if err != nil {
		return nil, err
	}
	for i, it := range items {
		out[i].Ok = it.LockToken != "" && renewed[renewKey(it.SeqNumber, it.LockToken)]
	}
	return out, nil
}

// renewKey identifies a settle item by the PAIR that fences it. A lock token is opaque, so it
// is length-prefixed rather than concatenated — two different (seq, token) pairs must never
// collide into one key.
func renewKey(seq int64, token string) string {
	return strconv.FormatInt(seq, 10) + ":" + strconv.Itoa(len(token)) + ":" + token
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
// Like RenewBatch it is SET-BASED: a FIXED number of statements, not one (or two) per message.
// Against a remote Turso/libSQL store every statement is its own Hrana round trip, so the old
// item-by-item loop made a 256-message settle ~512 remote round trips — and this is the very
// RPC whose latency lets the batch's own locks expire while it runs (review round-3).
func (e *Engine) CompleteBatch(ctx context.Context, queue string, items []SettleItem) ([]SettleResult, error) {
	out := make([]SettleResult, len(items))
	for i, it := range items {
		out[i] = SettleResult{SeqNumber: it.SeqNumber}
	}
	rows := make([]SettleItem, 0, len(items)) // only a non-empty token can fence a row
	for _, it := range items {
		if it.LockToken != "" {
			rows = append(rows, it)
		}
	}
	if len(rows) == 0 {
		return out, nil
	}

	now := e.now()
	pairs := strings.Repeat(",(?,?)", len(rows))[1:]
	tokenList := strings.Repeat(",?", len(rows))[1:]
	settled := make(map[string]bool, len(rows)) // (seq, token) pairs this call actually deleted
	replayed := make(map[string]bool, len(rows))
	var removed int64

	err := e.inTx(ctx, func(tx *sql.Tx) error {
		// The remote inTx may replay this closure — never inherit a previous attempt's results.
		removed = 0
		for k := range settled {
			delete(settled, k)
		}
		for k := range replayed {
			delete(replayed, k)
		}

		pairArgs := make([]any, 0, 1+2*len(rows))
		pairArgs = append(pairArgs, queue)
		for _, it := range rows {
			pairArgs = append(pairArgs, it.SeqNumber, it.LockToken)
		}

		// 1. Which items still hold their lock? Reading BEFORE the delete is what tells the two
		//    zero-row cases apart per item — deleted-now vs already-gone — which the loop used to
		//    learn from each DELETE's RowsAffected.
		sel, err := tx.QueryContext(ctx,
			`SELECT id, lock_token FROM messages WHERE queue=? AND (id, lock_token) IN (VALUES `+pairs+`)`,
			pairArgs...)
		if err != nil {
			return err
		}
		for sel.Next() {
			var id int64
			var tok string
			if err := sel.Scan(&id, &tok); err != nil {
				sel.Close()
				return err
			}
			settled[renewKey(id, tok)] = true
		}
		if err := sel.Err(); err != nil {
			sel.Close()
			return err
		}
		sel.Close()

		if len(settled) > 0 {
			// 2. Delete exactly those, in one statement.
			res, err := tx.ExecContext(ctx,
				`DELETE FROM messages WHERE queue=? AND (id, lock_token) IN (VALUES `+pairs+`)`, pairArgs...)
			if err != nil {
				return err
			}
			removed, _ = res.RowsAffected()

			// 3. One receipt per settled token, in one statement — this is what makes a
			//    re-Complete with the SAME token an idempotent success rather than ErrLockLost.
			recArgs := make([]any, 0, 4*len(rows))
			var recVals strings.Builder
			for _, it := range rows {
				if !settled[renewKey(it.SeqNumber, it.LockToken)] {
					continue
				}
				if recVals.Len() > 0 {
					recVals.WriteByte(',')
				}
				recVals.WriteString("(?,?,?,?)")
				recArgs = append(recArgs, it.LockToken, "completed", now, now+settlementTTLMs)
			}
			if recVals.Len() > 0 {
				if _, err := tx.ExecContext(ctx,
					`INSERT OR REPLACE INTO settlement_receipts(lock_token,operation,created_at,expires_at)
					 VALUES `+recVals.String(), recArgs...); err != nil {
					return err
				}
			}
		}

		// 4. The items that matched no row: a live receipt means this is a replay of a settle
		//    whose response was lost (idempotent success); no receipt means the lock is lost.
		recheck := make([]any, 0, 1+len(rows))
		recheck = append(recheck, now)
		for _, it := range rows {
			recheck = append(recheck, it.LockToken)
		}
		rec, err := tx.QueryContext(ctx,
			`SELECT lock_token FROM settlement_receipts WHERE expires_at>? AND lock_token IN (`+tokenList+`)`,
			recheck...)
		if err != nil {
			return err
		}
		defer rec.Close()
		for rec.Next() {
			var tok string
			if err := rec.Scan(&tok); err != nil {
				return err
			}
			replayed[tok] = true
		}
		return rec.Err()
	})
	if err != nil {
		return nil, err
	}

	for i, it := range items {
		if it.LockToken == "" {
			continue // Ok stays false
		}
		// Settled now, or already settled earlier under the same token (lost-response replay).
		out[i].Ok = settled[renewKey(it.SeqNumber, it.LockToken)] || replayed[it.LockToken]
	}
	e.markCompleted(queue, removed)
	return out, nil
}
