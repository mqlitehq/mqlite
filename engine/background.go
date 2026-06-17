package engine

import (
	"context"
	"database/sql"
	"time"
)

// startBackground launches the four maintenance loops (§8.8). All run as writes
// through the single connection, so they serialize behind foreground work.
func (e *Engine) startBackground(ctx context.Context) {
	// reaper at 1s bounds visibility-timeout redelivery to lock_duration + ≤1s
	// (expired locks are reclaimed here, not on the claim hot path, so claims stay
	// O(log n) on a deep backlog — see claim.go / the stress report).
	e.spawn(ctx, 1*time.Second, e.reapLocks)         // lock-expiry reaper
	e.spawn(ctx, 1*time.Second, e.activateScheduled) // scheduled -> active
	e.spawn(ctx, 10*time.Second, e.expireTTL)        // active TTL -> DLQ/discard
	e.spawn(ctx, 60*time.Second, e.cleanupDedup)     // drop out-of-window dedup rows
	e.spawn(ctx, 60*time.Second, e.cleanupExpiredAux) // drop expired settle/receive receipts
}

func (e *Engine) spawn(ctx context.Context, interval time.Duration, fn func(context.Context)) {
	e.bgWG.Add(1)
	go func() {
		defer e.bgWG.Done()
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				fn(ctx)
			}
		}
	}()
}

func (e *Engine) distinctQueues(ctx context.Context, where string, args ...any) []string {
	rows, err := e.db.query(ctx, `SELECT DISTINCT queue FROM messages WHERE `+where, args...)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var qs []string
	for rows.Next() {
		var q string
		if rows.Scan(&q) == nil {
			qs = append(qs, q)
		}
	}
	return qs
}

// reapLocks reclaims expired locks: redeliver, or dead-letter once over max (§5.2 reaper).
func (e *Engine) reapLocks(ctx context.Context) {
	now := e.now()
	qs := e.distinctQueues(ctx, `state='locked' AND locked_until<=?`, now)
	_, err := e.db.exec(ctx, `
		UPDATE messages SET
		    state = CASE WHEN delivery_count >= (SELECT max_delivery_count FROM queues WHERE name=messages.queue)
		                 THEN 'dead_lettered' ELSE 'active' END,
		    locked_until = 0, lock_token = NULL,
		    dead_letter_reason = CASE WHEN delivery_count >= (SELECT max_delivery_count FROM queues WHERE name=messages.queue)
		                              THEN 'MaxDeliveryCountExceeded' ELSE dead_letter_reason END
		 WHERE state='locked' AND locked_until<=?`, now)
	if err != nil {
		return
	}
	for _, q := range qs {
		e.note.notify(q)
	}
}

// activateScheduled flips scheduled messages whose visible_at has arrived.
func (e *Engine) activateScheduled(ctx context.Context) {
	now := e.now()
	qs := e.distinctQueues(ctx, `state='scheduled' AND visible_at<=?`, now)
	if _, err := e.db.exec(ctx,
		`UPDATE messages SET state='active' WHERE state='scheduled' AND visible_at<=?`, now); err != nil {
		return
	}
	for _, q := range qs {
		e.note.notify(q)
	}
}

// expireTTL is the authoritative TTL pass (§8.8): expired messages go to the DLQ
// (queues with dead_letter_on_expire=1) or are discarded.
func (e *Engine) expireTTL(ctx context.Context) {
	now := e.now()
	_, _ = e.db.exec(ctx, `
		UPDATE messages SET state='dead_lettered', dead_letter_reason='TTLExpired',
		    locked_until=0, lock_token=NULL
		 WHERE expires_at>0 AND expires_at<=? AND state IN ('active','locked','deferred')
		   AND queue IN (SELECT name FROM queues WHERE dead_letter_on_expire=1)`, now)
	_, _ = e.db.exec(ctx, `
		DELETE FROM messages
		 WHERE expires_at>0 AND expires_at<=? AND state IN ('active','locked','deferred','scheduled')
		   AND queue IN (SELECT name FROM queues WHERE dead_letter_on_expire=0)`, now)
}

// cleanupDedup drops dedup rows older than the widest configured window.
func (e *Engine) cleanupDedup(ctx context.Context) {
	var maxWindow sql.NullInt64
	if err := e.db.queryRowScan(ctx, []any{&maxWindow}, `SELECT MAX(dedup_window_ms) FROM queues`); err != nil {
		return
	}
	if !maxWindow.Valid || maxWindow.Int64 <= 0 {
		return
	}
	_, _ = e.db.exec(ctx, `DELETE FROM dedup WHERE seen_at < ?`, e.now()-maxWindow.Int64)
}

// cleanupExpiredAux drops expired settlement receipts and receive-attempt records.
// cx ships these idempotency tables without a sweeper (unbounded growth); mqlite
// retires them on a low-frequency pass so the feature comes with retention.
func (e *Engine) cleanupExpiredAux(ctx context.Context) {
	now := e.now()
	_, _ = e.db.exec(ctx, `DELETE FROM settlement_receipts WHERE expires_at < ?`, now)
	_, _ = e.db.exec(ctx, `DELETE FROM receive_attempts WHERE expires_at < ?`, now)
}

// RunMaintenanceOnce runs every maintenance pass synchronously. Tests with
// DisableBackground use this to drive time-based transitions deterministically.
func (e *Engine) RunMaintenanceOnce(ctx context.Context) {
	e.reapLocks(ctx)
	e.activateScheduled(ctx)
	e.expireTTL(ctx)
	e.cleanupDedup(ctx)
	e.cleanupExpiredAux(ctx)
}
