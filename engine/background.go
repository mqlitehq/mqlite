package engine

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"time"
)

// startBackground launches the maintenance loops (§8.8). All run as writes
// through the single connection, so they serialize behind foreground work.
func (e *Engine) startBackground(ctx context.Context) {
	e.startLifecycle(ctx)
	e.startSubscriptionExpiry(ctx)
	// reaper at 1s bounds visibility-timeout redelivery to lock_duration + ≤1s
	// (expired locks are reclaimed here, not on the claim hot path, so claims stay
	// O(log n) on a deep backlog — see claim.go / the stress report).
	e.spawn(ctx, 1*time.Second, func(ctx context.Context) { e.runMaintenance(ctx, "locks", e.reapLocks) })             // lock-expiry reaper
	e.spawn(ctx, 1*time.Second, func(ctx context.Context) { e.runMaintenance(ctx, "scheduled", e.activateScheduled) }) // scheduled -> active
	e.spawn(ctx, 10*time.Second, func(ctx context.Context) { e.runMaintenance(ctx, "ttl", e.expireTTL) })              // active TTL -> DLQ/discard
	e.spawn(ctx, 60*time.Second, func(ctx context.Context) { e.runMaintenance(ctx, "dedup", e.cleanupDedup) })         // drop out-of-window dedup rows
	e.spawn(ctx, 60*time.Second, func(ctx context.Context) { e.runMaintenance(ctx, "receipts", e.cleanupExpiredAux) }) // drop expired settle/receive receipts
	// DLQ retention (MQLITE-21/29): always run when background loops are on. A
	// per-queue bound may exist even with no engine-global default, and the pass is
	// a cheap no-op when nothing is bounded (it just lists DLQ queues and skips).
	e.spawn(ctx, 60*time.Second, func(ctx context.Context) { e.runMaintenance(ctx, "retention", e.reapDLQ) }) // drop-oldest past age/count/bytes
	// free-page reclamation (MQLITE-53): hand deleted pages back to the OS so a
	// churning queue's file doesn't bloat — no manual stop-the-broker VACUUM.
	e.spawn(ctx, 60*time.Second, func(ctx context.Context) { e.runMaintenance(ctx, "reclaim", e.reclaimFreePages) })
}

const lifecycleFastRetryDelay = 10 * time.Millisecond

// startSubscriptionExpiry detects newly due lifetimes once per second. A full
// batch continues after a short yield: every batch is its own transaction, so
// foreground work can enter between them, while a 10k simultaneous expiry does
// not inherit a ten-second physical-transition delay.
func (e *Engine) startSubscriptionExpiry(ctx context.Context) {
	e.bgWG.Add(1)
	go func() {
		defer e.bgWG.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			for {
				pending := false
				e.runMaintenance(ctx, "subscription_expiry", func(ctx context.Context) {
					pending = e.runSubscriptionExpiryPass(ctx)
				})
				if ctx.Err() != nil {
					return
				}
				if !pending {
					break
				}
				timer := time.NewTimer(lifecycleFastRetryDelay)
				select {
				case <-ctx.Done():
					if !timer.Stop() {
						<-timer.C
					}
					return
				case <-timer.C:
				}
			}
		}
	}()
}

// signalLifecycle records cleanup work without blocking the caller. A capacity-one
// channel coalesces bursts of deletions; each pass reports whether it must continue,
// so one wake is enough to drain every durable deleting resource. The channel is
// deliberately never closed: a concurrent outcome-unknown delete may signal while
// Close is cancelling the worker.
func (e *Engine) signalLifecycle() {
	select {
	case e.lifecycleWake <- struct{}{}:
	default:
	}
}

// A remote lifecycle commit acknowledgement can be lost before its durable state
// is visible to a new connection. One immediate inventory pass is therefore not
// enough after ErrOutcomeUnknown. Keep a bounded 30-second reconciliation window;
// this cost exists only after an ambiguous destructive request, never in the
// ordinary idle state. Concurrent ambiguous deletes extend (rather than multiply)
// the window and still share the capacity-one wake.
const lifecycleUncertainPasses int32 = 30

func (e *Engine) signalUncertainLifecycle() {
	for {
		current := e.uncertainLifecycleProbes.Load()
		if current >= lifecycleUncertainPasses || e.uncertainLifecycleProbes.CompareAndSwap(current, lifecycleUncertainPasses) {
			break
		}
	}
	e.signalLifecycle()
}

func (e *Engine) takeUncertainLifecycleProbe() bool {
	for {
		current := e.uncertainLifecycleProbes.Load()
		if current <= 0 {
			return false
		}
		if e.uncertainLifecycleProbes.CompareAndSwap(current, current-1) {
			return true
		}
	}
}

// startLifecycle runs cleanup only after durable work is discovered. A full
// metadata-only batch continues after a short yield; loaded-message cleanup and
// errors remain paced at one pass per second. Every pass releases the writer. A
// signal arriving during a pass remains buffered for the next outer iteration,
// which prevents a deletion committed after an empty inventory snapshot from
// being lost.
func (e *Engine) startLifecycle(ctx context.Context) {
	e.bgWG.Add(1)
	go func() {
		defer e.bgWG.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-e.lifecycleWake:
			}

			for {
				outcome := lifecyclePassOutcome{}
				e.runMaintenance(ctx, "lifecycle", func(ctx context.Context) {
					outcome = e.maintainLifecyclePassDetailed(ctx)
				})
				if ctx.Err() != nil {
					return
				}
				if e.takeUncertainLifecycleProbe() {
					outcome.retry = true
					outcome.fast = false
				}
				if !outcome.retry {
					break
				}

				delay := time.Second
				if outcome.fast {
					delay = lifecycleFastRetryDelay
				}
				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					if !timer.Stop() {
						<-timer.C
					}
					return
				case <-timer.C:
				}
			}
		}
	}()
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

func (e *Engine) distinctQueues(ctx context.Context, where string) []string {
	var qs []string
	if err := e.db.queryFresh(ctx, `SELECT DISTINCT messages.queue FROM messages JOIN queues ON queues.name=messages.queue
		WHERE queues.lifecycle_state='active' AND (queues.expires_at=0 OR queues.expires_at>?) AND `+where,
		func() []any {
			qs = qs[:0]
			return []any{e.now()}
		},
		func(rows *sql.Rows) error {
			for rows.Next() {
				var q string
				if err := rows.Scan(&q); err != nil {
					return err
				}
				qs = append(qs, q)
			}
			return rows.Err()
		}); err != nil {
		e.maintenanceFailure(ctx, "list_affected_queues", err)
		return nil
	}
	return qs
}

// reapLocks reclaims expired locks: redeliver, or dead-letter once over max (§5.2 reaper).
func (e *Engine) reapLocks(ctx context.Context) {
	qs, err := e.mutateMessagesFreshQueues(ctx, "lock_expired", `
		UPDATE messages SET
		    state = CASE WHEN delivery_count >= (SELECT max_delivery_count FROM queues WHERE name=messages.queue)
		                 THEN 'dead_lettered' ELSE 'active' END,
		    locked_until = 0, lock_token = NULL,
		    dead_letter_reason = CASE WHEN delivery_count >= (SELECT max_delivery_count FROM queues WHERE name=messages.queue)
		                              THEN 'MaxDeliveryCountExceeded' ELSE dead_letter_reason END
		 WHERE state='locked' AND locked_until<=?
		   AND queue IN (SELECT name FROM queues
		                 WHERE lifecycle_state='active' AND (expires_at=0 OR expires_at>?))`,
		func() []any { now := e.now(); return []any{now, now} })
	if err != nil {
		e.maintenanceFailure(ctx, "reclaim_locks", err)
		return
	}
	for _, q := range qs {
		e.note.notify(q)
	}
}

// activateScheduled flips scheduled messages whose visible_at has arrived.
func (e *Engine) activateScheduled(ctx context.Context) {
	qs, err := e.mutateMessagesFreshQueues(ctx, "activated",
		`UPDATE messages SET state='active' WHERE state='scheduled' AND visible_at<=?
		 AND queue IN (SELECT name FROM queues
		               WHERE lifecycle_state='active' AND (expires_at=0 OR expires_at>?))`,
		func() []any { now := e.now(); return []any{now, now} })
	if err != nil {
		e.maintenanceFailure(ctx, "activate_scheduled", err)
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
	if err := e.mutateMessagesFresh(ctx, "dead_lettered", `
			UPDATE messages SET state='dead_lettered', dead_letter_reason='TTLExpired',
		    locked_until=0, lock_token=NULL
		 WHERE expires_at>0 AND expires_at<=? AND state IN ('active','locked','deferred','scheduled')
		   AND queue IN (SELECT name FROM queues WHERE dead_letter_on_expire=1 AND lifecycle_state='active'
		                 AND (expires_at=0 OR expires_at>?))`,
		func() []any { now := e.now(); return []any{now, now} }); err != nil {
		e.maintenanceFailure(ctx, "expire_to_dlq", err)
	}
	if err := e.mutateMessagesFresh(ctx, "ttl_discarded", `
		DELETE FROM messages
		 WHERE expires_at>0 AND expires_at<=? AND state IN ('active','locked','deferred','scheduled')
		   AND queue IN (SELECT name FROM queues WHERE dead_letter_on_expire=0 AND lifecycle_state='active'
		                 AND (expires_at=0 OR expires_at>?))`,
		func() []any { now := e.now(); return []any{now, now} }); err != nil {
		e.maintenanceFailure(ctx, "expire_discard", err)
	}
}

// cleanupDedup drops dedup rows older than the widest configured window.
func (e *Engine) cleanupDedup(ctx context.Context) {
	var maxWindow sql.NullInt64
	if err := e.db.queryFresh(ctx, `SELECT MAX(dedup_window_ms) FROM queues
		WHERE lifecycle_state='active' AND (expires_at=0 OR expires_at>?)`,
		func() []any {
			maxWindow = sql.NullInt64{}
			return []any{e.now()}
		}, func(rows *sql.Rows) error {
			if !rows.Next() {
				return rows.Err()
			}
			return rows.Scan(&maxWindow)
		}); err != nil {
		e.maintenanceFailure(ctx, "read_dedup_window", err)
		return
	}
	if !maxWindow.Valid || maxWindow.Int64 <= 0 {
		return
	}
	if _, err := e.db.execFresh(ctx, `DELETE FROM dedup WHERE seen_at < ?
		AND queue IN (SELECT name FROM queues WHERE lifecycle_state='active'
		              AND (expires_at=0 OR expires_at>?))`, func() []any {
		now := e.now()
		return []any{now - maxWindow.Int64, now}
	}); err != nil {
		e.maintenanceFailure(ctx, "prune_dedup", err)
	}
}

// cleanupExpiredAux drops expired settlement receipts and receive-attempt records.
// These idempotency tables would otherwise grow unbounded; mqlite retires them on a
// low-frequency pass so the feature ships with its own retention.
func (e *Engine) cleanupExpiredAux(ctx context.Context) {
	if _, err := e.db.execFresh(ctx, `DELETE FROM settlement_receipts WHERE expires_at < ?
		AND queue IN (SELECT name FROM queues WHERE lifecycle_state='active'
		              AND (expires_at=0 OR expires_at>?))`, func() []any {
		now := e.now()
		return []any{now, now}
	}); err != nil {
		e.maintenanceFailure(ctx, "prune_settlement_receipts", err)
	}
	if _, err := e.db.execFresh(ctx, `DELETE FROM receive_attempts WHERE expires_at < ?
		AND queue IN (SELECT name FROM queues WHERE lifecycle_state='active'
		              AND (expires_at=0 OR expires_at>?))`, func() []any {
		now := e.now()
		return []any{now, now}
	}); err != nil {
		e.maintenanceFailure(ctx, "prune_receive_attempts", err)
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
			if errors.Is(err, ErrQueueNotFound) {
				continue // deleted after the active-queue inventory snapshot
			}
			e.maintenanceFailure(ctx, "load_retention_queue", err)
			continue
		}
		maxAge := effectiveBound(qr.dlqMaxAgeMs, e.dlqMaxAgeMs)
		maxCount := effectiveBound(qr.dlqMaxCount, int64(e.dlqMaxCount))
		maxBytes := effectiveBound(qr.dlqMaxBytes, e.dlqMaxBytes)

		// age: drop dead letters enqueued before the cutoff.
		if maxAge > 0 {
			for i := 0; i < maxBatches; i++ {
				epoch := e.observationEpoch()
				res, err := e.db.execFresh(ctx, `DELETE FROM messages WHERE id IN (
					SELECT id FROM messages WHERE queue=? AND state='dead_lettered' AND enqueued_at < ?
					  AND `+liveMessageQueueSQL+`
					ORDER BY id LIMIT ?)`, func() []any {
					now := e.now()
					return []any{q, now - maxAge, now, batch}
				})
				if err != nil {
					e.maintenanceFailure(ctx, "purge_retention_age", err)
					break
				}
				n, _ := res.RowsAffected()
				e.recordMessage(q, "retention_deleted", n, epoch)
				if n < batch {
					break
				}
			}
		}

		// count: the (maxCount+1)-th newest dead letter and everything older is surplus.
		//
		// A DLQ that is UNDER its cap has no such row, so the query returns sql.ErrNoRows. That
		// is the normal steady state — "nothing to purge" — not a failure. Logging it as an
		// ERROR made a healthy broker cry wolf every 60 seconds, and with the default cap of a
		// million dead letters that is very nearly every broker (review round-3 §3.2).
		if maxCount > 0 {
			var cutoffID sql.NullInt64
			err := e.db.queryFresh(ctx,
				`SELECT id FROM messages WHERE queue=? AND state='dead_lettered'
				   AND `+liveMessageQueueSQL+`
				 ORDER BY id DESC LIMIT 1 OFFSET ?`, func() []any {
					cutoffID = sql.NullInt64{}
					return []any{q, e.now(), maxCount}
				}, func(rows *sql.Rows) error {
					if !rows.Next() {
						if err := rows.Err(); err != nil {
							return err
						}
						return sql.ErrNoRows
					}
					return rows.Scan(&cutoffID)
				})
			switch {
			case errors.Is(err, sql.ErrNoRows): // under the cap: no surplus, no work
			case err != nil:
				e.maintenanceFailure(ctx, "retention_count_cutoff", err)
			}
			if err == nil && cutoffID.Valid {
				e.purgeDLQUpToID(ctx, q, cutoffID.Int64, batch, maxBatches, "count")
			}
		}

		// bytes: newest->oldest, the first dead letter whose cumulative body bytes
		// exceed the cap (and everything older) is surplus. Same as count above: an
		// under-cap DLQ has no such row, and that is a no-op, not an error.
		if maxBytes > 0 {
			var cutoffID sql.NullInt64
			err := e.db.queryFresh(ctx,
				`SELECT id FROM (
				     SELECT id, SUM(length(body)) OVER (ORDER BY id DESC) AS cum
				     FROM messages WHERE queue=? AND state='dead_lettered'
				       AND `+liveMessageQueueSQL+`
				 ) WHERE cum > ? ORDER BY id DESC LIMIT 1`, func() []any {
					cutoffID = sql.NullInt64{}
					return []any{q, e.now(), maxBytes}
				}, func(rows *sql.Rows) error {
					if !rows.Next() {
						if err := rows.Err(); err != nil {
							return err
						}
						return sql.ErrNoRows
					}
					return rows.Scan(&cutoffID)
				})
			switch {
			case errors.Is(err, sql.ErrNoRows): // under the cap: no surplus, no work
			case err != nil:
				e.maintenanceFailure(ctx, "retention_bytes_cutoff", err)
			}
			if err == nil && cutoffID.Valid {
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
		epoch := e.observationEpoch()
		res, err := e.db.execFresh(ctx, `DELETE FROM messages WHERE id IN (
			SELECT id FROM messages WHERE queue=? AND state='dead_lettered' AND id <= ?
			  AND `+liveMessageQueueSQL+`
			ORDER BY id LIMIT ?)`, func() []any { return []any{q, cutoffID, e.now(), batch} })
		if err != nil {
			e.maintenanceFailure(ctx, "purge_retention_"+label, err)
			return
		}
		n, _ := res.RowsAffected()
		e.recordMessage(q, "retention_deleted", n, epoch)
		if n < int64(batch) {
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
		e.maintenanceFailure(ctx, "read_freelist", err)
		return
	}
	if free < freePageReclaimMin {
		return
	}
	if err := e.reclaimPagesBounded(ctx, 256); err != nil {
		e.maintenanceFailure(ctx, "reclaim_pages", err)
	}
}

// afterFirstCheckpoint runs once the reclaim sequence is already under way. A test that cancels any
// earlier is stopped by withConn's own pre-start check and never reaches the guards below — which
// is exactly how the first version of the test for this passed while the guards were removed. nil
// in production.
var afterFirstCheckpoint func()

// reclaimPages returns free pages to the OS on a pinned connection so the steps share
// session state (splitting them across pooled exec() calls reclaims almost nothing). The
// sequence: (1) a TRUNCATE checkpoint flushes the WAL's freed pages onto the main-DB
// freelist; (2) incremental_vacuum moves them to the end and drops the page count — it frees
// one page per result step, so the rows MUST be drained to completion (a single Exec reclaims
// exactly one page); (3) a second TRUNCATE checkpoint hands the truncated tail back to the OS
// (in WAL mode the file only shrinks at a checkpoint). Single writer, so neither blocks.
// Shared by the background janitor and Compact(false) (MQLITE-78).
//
// It goes through withConn like everything else: a checkpoint/vacuum interrupted mid-statement
// leaks the local connection and wedges the database exactly as a settle would, and this one runs
// unattended on the janitor's clock (round-6 §4).
func (e *Engine) reclaimPages(ctx context.Context) error {
	return e.reclaimPagesBounded(ctx, 0)
}

// Background reclamation limits page movement; explicit Compact retains its
// documented full incremental reclaim. A zero limit means the whole freelist.
func (e *Engine) reclaimPagesBounded(ctx context.Context, pages int) error {
	// caller is the context that may be cancelled; ec is the protected one the statements run on.
	// The rule is the same one *txn enforces inside a transaction, and it has to be applied by hand
	// here because this is a multi-statement sequence pinned to a connection: the statement already
	// executing finishes, THE NEXT ONE DOES NOT BEGIN. Without the checks, cancelling a Compact
	// while the first checkpoint ran still went on to incremental_vacuum and a second checkpoint —
	// occupying the single writer for an unbounded time on a large freelist, for a caller who had
	// already gone (codex, round-6).
	caller := ctx
	return e.db.withConn(ctx, func(ec context.Context, conn *sql.Conn) error {
		if _, err := conn.ExecContext(ec, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
			return err
		}
		if afterFirstCheckpoint != nil { // test seam: cancel with the sequence already under way
			afterFirstCheckpoint()
		}
		if err := checkCtx(caller); err != nil {
			return err
		}
		query := `PRAGMA incremental_vacuum`
		if pages > 0 {
			query += "(" + strconv.Itoa(pages) + ")"
		}
		rows, err := conn.QueryContext(ec, query)
		if err != nil {
			return err
		}
		for rows.Next() {
			// each step frees one more page; iterating reclaims the whole freelist. A cancelled
			// caller stops stepping: the freelist is simply reclaimed on the next pass.
			if caller.Err() != nil {
				break
			}
		}
		if cerr := rows.Err(); cerr != nil {
			_ = rows.Close()
			return cerr
		}
		_ = rows.Close()
		if err := checkCtx(caller); err != nil {
			return err
		}
		_, err = conn.ExecContext(ec, `PRAGMA wal_checkpoint(TRUNCATE)`)
		return err
	})
}

// RunMaintenanceOnce runs every maintenance pass synchronously. Tests with
// DisableBackground use this to drive time-based transitions deterministically.
func (e *Engine) RunMaintenanceOnce(ctx context.Context) {
	e.runMaintenance(ctx, "subscription_expiry", e.transitionExpiredSubscriptions)
	e.runMaintenance(ctx, "lifecycle", e.maintainLifecycle)
	e.runMaintenance(ctx, "locks", e.reapLocks)
	e.runMaintenance(ctx, "scheduled", e.activateScheduled)
	e.runMaintenance(ctx, "ttl", e.expireTTL)
	e.runMaintenance(ctx, "dedup", e.cleanupDedup)
	e.runMaintenance(ctx, "receipts", e.cleanupExpiredAux)
	e.runMaintenance(ctx, "retention", e.reapDLQ)
	e.runMaintenance(ctx, "reclaim", e.reclaimFreePages)
}
