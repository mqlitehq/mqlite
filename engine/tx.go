package engine

import (
	"context"
	"database/sql"
	"fmt"
)

// EngineTx is a transaction handle for same-DB transactional enqueue (§4.5).
// Business-table writes and queue enqueues commit together: "business success
// ⇔ message enqueued" — a natural outbox with no distributed-commit dilemma.
// Only valid in embedded mode against a local file / single libSQL connection.
type EngineTx struct {
	e    *Engine
	tx   *txn
	ctx  context.Context
	now  int64
	woke map[string]bool
}

// SQL returns the underlying *sql.Tx so callers can run their business writes
// in the same transaction as the enqueue.
//
// These are YOUR statements: mqlite cannot see where one ends and the next begins, so it cannot
// stop issuing them on your behalf the way it does for its own (see txn in db.go). Two things
// follow, and both matter on a local store, where this transaction holds the process's ONE writer:
//
//   - Run them with Context(), never with the context you passed to Tx. Cancelling a statement
//     mid-flight is what wedges the database.
//   - Return promptly. The writer is blocked for as long as your callback runs, and cancelling
//     your context does not — cannot — cut that short. Do slow work (an HTTP call, a long compute)
//     BEFORE opening the transaction, not inside it.
func (t *EngineTx) SQL() *sql.Tx { return t.tx.SQL() }

// Context is the context your own statements inside this transaction should use:
//
//	tx.SQL().ExecContext(tx.Context(), `INSERT INTO orders_tbl(id) VALUES (1)`)
//
// Use it instead of the context you passed to Tx. On a LOCAL store it is deliberately not
// cancellable: interrupting a statement mid-transaction leaks the SQLite connection, which leaves
// the database locked (SQLITE_BUSY, permanently) — or, for `:memory:`, destroys it outright. The
// wait to enter the transaction still honours your context, and an already-cancelled caller never
// enters at all; a statement already running is allowed to finish, and the next one does not begin.
//
// There is no upper bound on how long "finish" takes — it is YOUR statement. On a remote store this
// IS your context, unchanged, and real statement cancellation still applies.
func (t *EngineTx) Context() context.Context { return t.ctx }

// SendOne enqueues a message inside the open transaction.
func (t *EngineTx) SendOne(queue string, m OutMessage) (int64, error) {
	if int64(len(m.Body)) > t.e.maxMsgBytes {
		return 0, fmt.Errorf("%w: %d > %d bytes", ErrMessageTooLarge, len(m.Body), t.e.maxMsgBytes)
	}
	targets, err := t.e.resolveTargetsTx(t.ctx, t.tx, queue)
	if err != nil {
		return 0, err
	}
	var last int64
	for _, tg := range targets {
		if !t.e.filterAccepts(tg, m, t.now, t.now) { // immediate send: visible_at == enqueued_at
			continue
		}
		q, err := t.e.loadQueueTx(t.ctx, t.tx, tg.name)
		if err != nil {
			return 0, err
		}
		if q.ordering == OrderGroupFIFO && m.GroupID == "" {
			return 0, ErrGroupRequired
		}
		seq, deduped, err := t.e.insertOne(t.ctx, t.tx, q, m, 0, StateActive, t.now)
		if err != nil {
			return 0, err
		}
		last = seq
		if !deduped {
			t.woke[tg.name] = true
		}
	}
	return last, nil
}

// Tx runs fn inside one transaction. If fn returns nil the transaction commits
// (and long-poll waiters for any written queue are notified); otherwise it rolls back.
//
// fn MAY RUN MORE THAN ONCE on a remote store. inTx replays the whole closure when a transaction
// fails on a retryable connection/busy error — the database work of the failed attempt rolled
// back, so the DATA is correct either way, but anything fn did OUTSIDE the transaction happened
// twice. That is the contract, and it is not hypothetical: an HTTP call, a charge, a counter in
// memory does not roll back with a SQL transaction (round-4 §5.2). Keep fn to transaction-bound
// work.
//
// Local file and :memory: stores never retry, so there fn runs zero or one time — zero when the
// caller was already cancelled before the transaction began.
func (e *Engine) Tx(ctx context.Context, fn func(*EngineTx) error) error {
	var woke map[string]bool
	err := e.inTx(ctx, func(ctx context.Context, tx *txn) error {
		et := &EngineTx{e: e, tx: tx, ctx: ctx, now: e.now(), woke: map[string]bool{}}
		if err := fn(et); err != nil {
			return err
		}
		woke = et.woke
		return nil
	})
	if err != nil {
		return err
	}
	for q := range woke {
		e.note.notify(q)
	}
	return nil
}

func (e *Engine) resolveTargetsTx(ctx context.Context, tx *txn, name string) ([]target, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT subscription, filter_json FROM subscriptions WHERE topic=? ORDER BY subscription`, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var subs []target
	for rows.Next() {
		var s string
		var fj sql.NullString
		if err := rows.Scan(&s, &fj); err != nil {
			return nil, err
		}
		subs = append(subs, e.targetFor(s, fj))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(subs) > 0 {
		return subs, nil
	}
	if _, err := e.loadQueueTx(ctx, tx, name); err != nil {
		return nil, err
	}
	return []target{{name: name}}, nil
}
