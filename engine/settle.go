package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
	// execFresh, not exec: a remote retry backs off, so a deadline computed once — before the
	// loop — could commit a lease that has already expired by the time the write that actually
	// lands runs (MQLITE-97). It is measured against the clock of each attempt.
	res, err := e.db.execFresh(ctx,
		`UPDATE messages SET locked_until=? WHERE id=? AND queue=? AND lock_token=?`,
		func() []any { return []any{e.now() + q.lockDurationMs, seq, queue, token} })
	if err != nil {
		return err
	}
	return affected(res)
}

// settleChunk bounds how many (seq, token) pairs go into one set-based statement.
//
// The pinned SQLite build caps a statement at 32,766 bind parameters, and a pair costs two — so
// an unchunked set-based settle would HARD-FAIL somewhere above ~16k items, on batches the
// previous item-by-item loop handled fine and that sit well inside the HTTP body limit. Chunking
// keeps the round-trip count O(N/512) instead of O(N) — for any realistic batch (Receive caps at
// 256) that is a single statement — while a pathologically large batch degrades gracefully
// instead of rolling back the whole transaction.
const settleChunk = 512

// chunkPairs walks items in settleChunk-sized groups, handing each group to fn along with the
// pre-built `(?,?),(?,?)...` row-value placeholder list it needs.
func chunkPairs(items []SettleItem, fn func(group []SettleItem, placeholders string) error) error {
	for start := 0; start < len(items); start += settleChunk {
		end := start + settleChunk
		if end > len(items) {
			end = len(items)
		}
		group := items[start:end]
		if err := fn(group, strings.Repeat(",(?,?)", len(group))[1:]); err != nil {
			return err
		}
	}
	return nil
}

// pairArgs builds [queue, seq1, token1, seq2, token2, ...] for a row-value `IN (VALUES ...)`.
func pairArgs(queue string, group []SettleItem) []any {
	args := make([]any, 0, 1+2*len(group))
	args = append(args, queue)
	for _, it := range group {
		args = append(args, it.SeqNumber, it.LockToken)
	}
	return args
}

// fencible drops items that cannot match a row: an empty lock token never fences anything.
func fencible(items []SettleItem) []SettleItem {
	out := make([]SettleItem, 0, len(items))
	for _, it := range items {
		if it.LockToken != "" {
			out = append(out, it)
		}
	}
	return out
}

// settleKey identifies a settle item by the PAIR that fences it. Keying results by sequence
// number ALONE is a real bug, not a shortcut: a caller may pass the same seq twice — once with
// the live token, once with a stale one — and a seq-keyed result lets the stale item inherit the
// other's success, reporting a lease renewed (or a message settled) that never was. The token is
// opaque, so it is length-prefixed rather than concatenated: two different pairs must never
// collide into one key.
func settleKey(seq int64, token string) string {
	return strconv.FormatInt(seq, 10) + ":" + strconv.Itoa(len(token)) + ":" + token
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
// It is also SET-BASED rather than a loop of single-row updates, and that matters just as much:
// against a remote Turso/libSQL store every statement is its own Hrana round trip, so an
// N-statement renewal is N remote round trips — the same O(N) latency, reintroduced one layer
// down. Since the new deadline is computed once, a slow enough pass would commit leases that had
// already expired while it ran.
//
// Like Renew, an item whose lock was already lost simply comes back Ok=false; that is the
// contract, not an error for the batch. Fencing is on LockToken, exactly as in CompleteBatch.
// MaxRenewBatch is the most messages one RenewBatch call may renew.
//
// It is exactly one statement's worth, and that is a CONTRACT, not a tuning knob. RenewBatch
// promises that every Ok it returns means a live lease at the moment it returns. Across several
// statements that promise is unkeepable: the first chunk commits, later chunks run (each a
// network round trip on a remote store), and with a short lock duration the first chunk's lease
// can expire — and be reaped — before the call has even finished, while its result still says Ok.
// Renewing in one statement is what makes the answer honest.
//
// It is also not a limit anyone meets by accident: Receive hands out at most 256 messages, so a
// consumer holding a batch of leases never has more than that. A caller holding more should renew
// in several calls, each of which then reports honestly about its own.
const MaxRenewBatch = settleChunk

// RenewBatch extends the lock lease of many messages in ONE statement, returning a per-item
// result so the caller can see exactly which leases still hold.
//
// Renewing a batch message-by-message does not scale: over a network each Renew is a separate
// round trip, so N messages × the link latency can easily exceed the lease itself and the locks
// expire *while they are being renewed* — a 64-message batch on a 50ms link needs 3.2s of
// renewals against a 2s lease and loses most of them (review round-3). One request, one statement,
// one lease deadline for the whole batch.
//
// Deliberately NOT in a transaction. Renewal has no cross-item invariant to protect — each lease
// stands or falls on its own token — so a transaction buys nothing and costs two real things: the
// deadline would have to survive the COMMIT as well as the write, and `Tx.Commit` takes no
// context (the pinned libSQL driver sends it on context.Background()), so a stalled commit is
// UNCANCELLABLE — and `mqlite receive` waits for its renewal goroutine before closing the client,
// which would hang the command despite cancelling it.
//
// Like Renew, an item whose lock was already lost simply comes back Ok=false; that is the
// contract, not an error for the batch. Fencing is on LockToken, exactly as in CompleteBatch.
func (e *Engine) RenewBatch(ctx context.Context, queue string, items []SettleItem) ([]SettleResult, error) {
	if len(items) > MaxRenewBatch {
		return nil, fmt.Errorf("%w: renew at most %d messages per call (got %d) — a lease renewed by an earlier statement could expire before a later one finished, so a multi-statement renewal cannot honestly report which leases still hold",
			ErrInvalidArgument, MaxRenewBatch, len(items))
	}
	out := make([]SettleResult, len(items))
	for i, it := range items {
		out[i] = SettleResult{SeqNumber: it.SeqNumber}
	}
	rows := fencible(items)
	if len(rows) == 0 {
		return out, nil
	}
	q, err := e.loadQueue(ctx, queue)
	if err != nil {
		return nil, err
	}

	// ONE statement: the write and the report of what it touched are the same UPDATE ... RETURNING.
	// A separate follow-up SELECT would put another round trip between the deadline and its commit,
	// eating into the very lease it is trying to extend.
	renewed := make(map[string]bool, len(rows))
	pairs := strings.Repeat(",(?,?)", len(rows))[1:]
	args := pairArgs(queue, rows)
	err = e.db.queryFresh(ctx,
		`UPDATE messages SET locked_until=? WHERE queue=? AND (id, lock_token) IN (VALUES `+pairs+`)
		 RETURNING id, lock_token`,
		// The deadline is built per ATTEMPT, once a connection is already held — see queryFresh.
		// Computed any earlier, a backoff (or a wait for a free connection) could leave it in the
		// past by the time the successful attempt commits: the row would report Ok while the reaper
		// reclaimed it at once.
		func() []any { return append([]any{e.now() + q.lockDurationMs}, args...) },
		func(sel *sql.Rows) error {
			// An item whose lock was lost, or whose token is wrong, matches no row: it is simply
			// absent from the RETURNING set and its Ok stays false. That is Renew's contract, not
			// an error.
			for sel.Next() {
				var id int64
				var tok string
				if err := sel.Scan(&id, &tok); err != nil {
					return err
				}
				renewed[settleKey(id, tok)] = true
			}
			return sel.Err()
		})
	if err != nil {
		return nil, err
	}
	for i, it := range items {
		out[i].Ok = it.LockToken != "" && renewed[settleKey(it.SeqNumber, it.LockToken)]
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

// CompleteBatch completes many messages in one transaction / one round-trip — the broker-side fix
// for the drain N+1 (Receive returns a batch, but settling it one-by-one is one HTTP call per
// message). Each item is fenced on its own lock_token and recorded idempotently, exactly like
// Complete; a per-item failure (expired/wrong token) returns Ok=false rather than failing the
// whole batch.
//
// Like RenewBatch it is SET-BASED — a fixed number of statements, not one (or two) per message.
// Against a remote Turso/libSQL store every statement is its own round trip, so the old
// item-by-item loop made a 256-message settle ~512 remote round trips, on the very RPC whose
// latency lets the batch's own locks expire while it runs (review round-3).
//
// Unlike RenewBatch it may span several statements (it chunks), because completion is TERMINAL:
// a message deleted by an earlier chunk cannot un-delete itself while a later chunk runs. Renewal
// makes a claim about the future — "this lease is live" — which is why it must fit in one
// statement; completion only reports what already happened.
func (e *Engine) CompleteBatch(ctx context.Context, queue string, items []SettleItem) ([]SettleResult, error) {
	out := make([]SettleResult, len(items))
	for i, it := range items {
		out[i] = SettleResult{SeqNumber: it.SeqNumber}
	}
	rows := fencible(items)
	if len(rows) == 0 {
		return out, nil
	}

	now := e.now()
	settled := make(map[string]bool, len(rows))  // (seq, token) pairs this call actually deleted
	replayed := make(map[string]bool, len(rows)) // tokens with a live receipt: an earlier settle
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

		// 1. Which tokens ALREADY had a live receipt, before this batch touches anything? A
		//    receipt means an earlier settle of that token succeeded but its response was lost,
		//    so a re-Complete is an idempotent success rather than ErrLockLost.
		//
		//    This must run BEFORE step 4 inserts receipts of its own. Reading afterwards is a
		//    fencing hole: a batch carrying (wrongSeq, T) alongside the valid (liveSeq, T) would
		//    see the receipt THIS batch just wrote for T and report Ok for the wrong pair too —
		//    claiming a message settled that matched no row at all. Receipts are keyed by token,
		//    so only a pre-existing one may vouch for a pair. Chunked on the same budget — one
		//    bind parameter per token here, not two.
		for start := 0; start < len(rows); start += settleChunk {
			end := start + settleChunk
			if end > len(rows) {
				end = len(rows)
			}
			group := rows[start:end]
			args := make([]any, 0, 1+len(group))
			args = append(args, now)
			for _, it := range group {
				args = append(args, it.LockToken)
			}
			rec, err := tx.QueryContext(ctx,
				`SELECT lock_token FROM settlement_receipts WHERE expires_at>? AND lock_token IN (`+
					strings.Repeat(",?", len(group))[1:]+`)`, args...)
			if err != nil {
				return err
			}
			for rec.Next() {
				var tok string
				if err := rec.Scan(&tok); err != nil {
					rec.Close()
					return err
				}
				replayed[tok] = true
			}
			if err := rec.Err(); err != nil {
				rec.Close()
				return err
			}
			rec.Close()
		}

		err := chunkPairs(rows, func(group []SettleItem, pairs string) error {
			args := pairArgs(queue, group)

			// 2. Which items still hold their lock? Reading BEFORE the delete is what tells the
			//    two zero-row cases apart per item — deleted-just-now vs already-gone — which the
			//    old loop learned from each single DELETE's RowsAffected.
			sel, err := tx.QueryContext(ctx,
				`SELECT id, lock_token FROM messages WHERE queue=? AND (id, lock_token) IN (VALUES `+pairs+`)`,
				args...)
			if err != nil {
				return err
			}
			present := make([]SettleItem, 0, len(group))
			for sel.Next() {
				var id int64
				var tok string
				if err := sel.Scan(&id, &tok); err != nil {
					sel.Close()
					return err
				}
				settled[settleKey(id, tok)] = true
				present = append(present, SettleItem{SeqNumber: id, LockToken: tok})
			}
			if err := sel.Err(); err != nil {
				sel.Close()
				return err
			}
			sel.Close()
			if len(present) == 0 {
				return nil
			}

			// 3. Delete exactly those, in one statement.
			res, err := tx.ExecContext(ctx,
				`DELETE FROM messages WHERE queue=? AND (id, lock_token) IN (VALUES `+pairs+`)`, args...)
			if err != nil {
				return err
			}
			n, _ := res.RowsAffected()
			removed += n

			// 4. One receipt per settled token, in one statement — this is what makes a
			//    re-Complete with the SAME token an idempotent success instead of ErrLockLost.
			recArgs := make([]any, 0, 4*len(present))
			var vals strings.Builder
			for _, it := range present {
				if vals.Len() > 0 {
					vals.WriteByte(',')
				}
				vals.WriteString("(?,?,?,?)")
				recArgs = append(recArgs, it.LockToken, "completed", now, now+settlementTTLMs)
			}
			_, err = tx.ExecContext(ctx,
				`INSERT OR REPLACE INTO settlement_receipts(lock_token,operation,created_at,expires_at)
				 VALUES `+vals.String(), recArgs...)
			return err
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	for i, it := range items {
		if it.LockToken == "" {
			continue // Ok stays false
		}
		// Settled now, or already settled earlier under the same token (lost-response replay).
		out[i].Ok = settled[settleKey(it.SeqNumber, it.LockToken)] || replayed[it.LockToken]
	}
	e.markCompleted(queue, removed)
	return out, nil
}
