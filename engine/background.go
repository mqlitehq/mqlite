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
	e.spawn(ctx, 1*time.Second, e.reapLocks)          // lock-expiry reaper
	e.spawn(ctx, 1*time.Second, e.activateScheduled)  // scheduled -> active
	e.spawn(ctx, 10*time.Second, e.expireTTL)         // active TTL -> DLQ/discard
	e.spawn(ctx, 60*time.Second, e.cleanupDedup)      // drop out-of-window dedup rows
	e.spawn(ctx, 60*time.Second, e.cleanupExpiredAux) // drop expired settle/receive receipts
	// DLQ retention (MQLITE-21/29): always run when background loops are on. A
	// per-queue bound may exist even with no engine-global default, and the pass is
	// a cheap no-op when nothing is bounded (it just lists DLQ queues and skips).
	e.spawn(ctx, 60*time.Second, e.reapDLQ) // drop-oldest past age/count/bytes
	// free-page reclamation (MQLITE-53): hand deleted pages back to the OS so a
	// churning queue's file doesn't bloat — no manual stop-the-broker VACUUM.
	e.spawn(ctx, 60*time.Second, e.reclaimFreePages)
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
		e.log.Error("background: list affected queues failed", "err", err)
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
		e.log.Error("reaper: reclaim expired locks failed", "err", err)
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
		e.log.Error("scheduler: activate scheduled messages failed", "err", err)
		return
	}
	for _, q := range qs {
		e.note.notify(q)
	}
}

// expireTTL is the authoritative TTL pass (§8.8): expired messages go to the DLQ
// (queues with dead_letter_on_expire=1) or are discarded. Both branches MUST
// cover the same state set — 'scheduled' included, so a message whose TTL lapses
// before it ever activates is dead-lettered now, not only after activation
// (MQLITE-61; the discard branch always had it).
func (e *Engine) expireTTL(ctx context.Context) {
	now := e.now()
	if _, err := e.db.exec(ctx, `
		UPDATE messages SET state='dead_lettered', dead_letter_reason='TTLExpired',
		    locked_until=0, lock_token=NULL
		 WHERE expires_at>0 AND expires_at<=? AND state IN ('active','locked','deferred','scheduled')
		   AND queue IN (SELECT name FROM queues WHERE dead_letter_on_expire=1)`, now); err != nil {
		e.log.Error("ttl: dead-letter expired messages failed", "err", err)
	}
	if _, err := e.db.exec(ctx, `
		DELETE FROM messages
		 WHERE expires_at>0 AND expires_at<=? AND state IN ('active','locked','deferred','scheduled')
		   AND queue IN (SELECT name FROM queues WHERE dead_letter_on_expire=0)`, now); err != nil {
		e.log.Error("ttl: discard expired messages failed", "err", err)
	}
}

// cleanupDedup drops dedup rows older than the widest configured window.
func (e *Engine) cleanupDedup(ctx context.Context) {
	var maxWindow sql.NullInt64
	if err := e.db.queryRowScan(ctx, []any{&maxWindow}, `SELECT MAX(dedup_window_ms) FROM queues`); err != nil {
		e.log.Error("janitor: read max dedup window failed", "err", err)
		return
	}
	if !maxWindow.Valid || maxWindow.Int64 <= 0 {
		return
	}
	if _, err := e.db.exec(ctx, `DELETE FROM dedup WHERE seen_at < ?`, e.now()-maxWindow.Int64); err != nil {
		e.log.Error("janitor: prune dedup rows failed", "err", err)
	}
}

// cleanupExpiredAux drops expired settlement receipts and receive-attempt records.
// These idempotency tables would otherwise grow unbounded; mqlite retires them on a
// low-frequency pass so the feature ships with its own retention.
func (e *Engine) cleanupExpiredAux(ctx context.Context) {
	now := e.now()
	if _, err := e.db.exec(ctx, `DELETE FROM settlement_receipts WHERE expires_at < ?`, now); err != nil {
		e.log.Error("janitor: prune settlement receipts failed", "err", err)
	}
	if _, err := e.db.exec(ctx, `DELETE FROM receive_attempts WHERE expires_at < ?`, now); err != nil {
		e.log.Error("janitor: prune receive attempts failed", "err", err)
	}
}

// effectiveBound resolves a per-queue retention override against the engine default
// (MQLITE-29): a positive per-queue value wins; a negative value means "explicitly
// unbounded" (0); 0 inherits the engine default.
func effectiveBound(perQueue, engineDefault int64) int64 {
	switch {
	case perQueue > 0:
		return perQueue
	case perQueue < 0:
		return 0 // opt out of the default
	default:
		return engineDefault
	}
}

// reapDLQ enforces DLQ retention (MQLITE-21/29): drop-oldest dead letters past the
// effective age / count / body-byte bound, per queue, in bounded batches so the
// single writer never stalls. Each queue's bound is its own override if set, else the
// engine default (see effectiveBound). ONLY state='dead_lettered' rows are ever
// deleted — undelivered / in-flight / scheduled work is never touched. A queue with
// no effective bound is skipped; with no dead letters at all the pass is a no-op.
func (e *Engine) reapDLQ(ctx context.Context) {
	const batch = 1000
	const maxBatches = 100 // bound work per pass; the 60s cadence rate-limits the rest

	for _, q := range e.distinctQueues(ctx, `state='dead_lettered'`) {
		qr, err := e.loadQueue(ctx, q)
		if err != nil {
			e.log.Error("dlq-retention: load queue failed", "queue", q, "err", err)
			continue
		}
		maxAge := effectiveBound(qr.dlqMaxAgeMs, e.dlqMaxAgeMs)
		maxCount := effectiveBound(qr.dlqMaxCount, int64(e.dlqMaxCount))
		maxBytes := effectiveBound(qr.dlqMaxBytes, e.dlqMaxBytes)

		// age: drop dead letters enqueued before the cutoff.
		if maxAge > 0 {
			cutoff := e.now() - maxAge
			for i := 0; i < maxBatches; i++ {
				res, err := e.db.exec(ctx, `DELETE FROM messages WHERE id IN (
					SELECT id FROM messages WHERE queue=? AND state='dead_lettered' AND enqueued_at < ?
					ORDER BY id LIMIT ?)`, q, cutoff, batch)
				if err != nil {
					e.log.Error("dlq-retention: age purge failed", "queue", q, "err", err)
					break
				}
				if n, _ := res.RowsAffected(); n < batch {
					break
				}
			}
		}

		// count: the (maxCount+1)-th newest dead letter and everything older is surplus.
		if maxCount > 0 {
			var cutoffID sql.NullInt64
			if err := e.db.queryRowScan(ctx, []any{&cutoffID},
				`SELECT id FROM messages WHERE queue=? AND state='dead_lettered'
				 ORDER BY id DESC LIMIT 1 OFFSET ?`, q, maxCount); err != nil {
				e.log.Error("dlq-retention: count cutoff failed", "queue", q, "err", err)
			} else if cutoffID.Valid {
				e.purgeDLQUpToID(ctx, q, cutoffID.Int64, batch, maxBatches, "count")
			}
		}

		// bytes: newest->oldest, the first dead letter whose cumulative body bytes
		// exceed the cap (and everything older) is surplus.
		if maxBytes > 0 {
			var cutoffID sql.NullInt64
			if err := e.db.queryRowScan(ctx, []any{&cutoffID},
				`SELECT id FROM (
				     SELECT id, SUM(length(body)) OVER (ORDER BY id DESC) AS cum
				     FROM messages WHERE queue=? AND state='dead_lettered'
				 ) WHERE cum > ? ORDER BY id DESC LIMIT 1`, q, maxBytes); err != nil {
				e.log.Error("dlq-retention: bytes cutoff failed", "queue", q, "err", err)
			} else if cutoffID.Valid {
				e.purgeDLQUpToID(ctx, q, cutoffID.Int64, batch, maxBatches, "bytes")
			}
		}
	}
}

// purgeDLQUpToID drops dead letters with id <= cutoffID for one queue, in bounded
// batches. Shared by the count and bytes bounds (both compute a cutoff id, then
// delete it and everything older).
func (e *Engine) purgeDLQUpToID(ctx context.Context, q string, cutoffID int64, batch, maxBatches int, label string) {
	for i := 0; i < maxBatches; i++ {
		res, err := e.db.exec(ctx, `DELETE FROM messages WHERE id IN (
			SELECT id FROM messages WHERE queue=? AND state='dead_lettered' AND id <= ?
			ORDER BY id LIMIT ?)`, q, cutoffID, batch)
		if err != nil {
			e.log.Error("dlq-retention: "+label+" purge failed", "queue", q, "err", err)
			return
		}
		if n, _ := res.RowsAffected(); n < int64(batch) {
			return
		}
	}
}

// freePageReclaimMin is the freelist size (in pages) below which background reclamation
// skips the write — an incremental_vacuum is only worth a pass once enough pages free up.
const freePageReclaimMin = 256 // ~1 MiB at a 4 KiB page size

// reclaimFreePages hands freed pages back to the OS via PRAGMA incremental_vacuum once the
// freelist has grown. A churning queue deletes rows constantly; auto_vacuum=INCREMENTAL
// (set at DB creation, see resolveDSN) parks those pages on the freelist, and this returns
// them without a full VACUUM or a global lock — so the file doesn't bloat and no operator
// has to stop the broker to compact it (MQLITE-53). Local file DBs only: :memory: has no
// OS pages to return and remote Turso manages its own storage. Best-effort.
func (e *Engine) reclaimFreePages(ctx context.Context) {
	if e.db.remote {
		return
	}
	if _, ok := localFilePath(e.db.dsn); !ok {
		return // in-memory DB
	}
	var free int
	if err := e.db.queryRowScan(ctx, []any{&free}, `PRAGMA freelist_count`); err != nil {
		e.log.Error("janitor: read freelist_count failed", "err", err)
		return
	}
	if free < freePageReclaimMin {
		return
	}
	if err := e.reclaimPages(ctx); err != nil {
		e.log.Error("janitor: reclaim failed", "err", err)
	}
}

// reclaimPages returns free pages to the OS on a pinned connection so the steps share
// session state (splitting them across pooled exec() calls reclaims almost nothing). The
// sequence: (1) a TRUNCATE checkpoint flushes the WAL's freed pages onto the main-DB
// freelist; (2) incremental_vacuum moves them to the end and drops the page count — it frees
// one page per result step, so the rows MUST be drained to completion (a single Exec reclaims
// exactly one page); (3) a second TRUNCATE checkpoint hands the truncated tail back to the OS
// (in WAL mode the file only shrinks at a checkpoint). Single writer, so neither blocks.
// Shared by the background janitor and Compact(false) (MQLITE-78).
func (e *Engine) reclaimPages(ctx context.Context) error {
	conn, err := e.db.sql.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		return err
	}
	rows, err := conn.QueryContext(ctx, `PRAGMA incremental_vacuum`)
	if err != nil {
		return err
	}
	for rows.Next() {
		// each step frees one more page; iterating reclaims the whole freelist
	}
	if cerr := rows.Err(); cerr != nil {
		_ = rows.Close()
		return cerr
	}
	_ = rows.Close()
	_, err = conn.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`)
	return err
}

// RunMaintenanceOnce runs every maintenance pass synchronously. Tests with
// DisableBackground use this to drive time-based transitions deterministically.
func (e *Engine) RunMaintenanceOnce(ctx context.Context) {
	e.reapLocks(ctx)
	e.activateScheduled(ctx)
	e.expireTTL(ctx)
	e.cleanupDedup(ctx)
	e.cleanupExpiredAux(ctx)
	e.reapDLQ(ctx)
	e.reclaimFreePages(ctx)
}
