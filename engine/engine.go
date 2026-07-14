package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Engine is the embeddable queue core: a Store plus Service Bus semantics,
// independent of any network/transport (design §12.3). Both the in-process
// embedded API and the network broker drive this same type.
type Engine struct {
	db   *db
	note *notifier
	now  func() int64 // epoch ms; injectable for tests
	log  *slog.Logger // structured logging for background-loop failures (MQLITE-5)

	bgWG      sync.WaitGroup
	bgCancel  context.CancelFunc
	closed    chan struct{}
	closeOnce sync.Once

	maxMsgBytes int64

	dlqMaxAgeMs int64 // DLQ retention: drop dead letters older than this (0 = off)
	dlqMaxCount int   // DLQ retention: keep at most N dead letters per queue (0 = off)
	dlqMaxBytes int64 // DLQ retention: cap dead-letter body bytes per queue (0 = off)

	qmu    sync.RWMutex
	qcache map[string]queueRow

	filterMu    sync.Mutex              // guards filterCache
	filterCache map[string]*filterEntry // compiled subscription filters, keyed by subscription

	// processed counts lifetime Completed messages per queue — in-process and
	// rough: it resets on restart (no durable store, no schema change). Exposed
	// as the mqlite_messages_completed_total counter; Prometheus rate()/increase()
	// absorb the restart reset. Zero value is ready; values are *atomic.Uint64. (MQLITE-54)
	processed sync.Map
}

// Options configures Open.
type Options struct {
	DB                string       // DB DSN: file:./mq.db | :memory: | libsql://host
	AuthToken         string       // injected from env; never compiled in
	Now               func() int64 // test clock (epoch ms)
	DisableBackground bool         // skip reaper/scheduler loops (tests)
	Synchronous       string       // local SQLite PRAGMA synchronous: NORMAL(default)|FULL|OFF
	MaxMessageBytes   int64        // reject bodies larger than this; 0 -> 1 MiB (§11.4)
	// DLQ retention bounds (MQLITE-21): a background pass drops dead letters
	// oldest-first past these. ONLY state='dead_lettered' is touched; 0 = that
	// dimension unbounded. Defaults are applied by the broker, not the engine.
	DLQMaxAgeMs int64        // dead letters older than this (by enqueued_at) are dropped
	DLQMaxCount int          // keep at most this many dead letters per queue (drop oldest)
	DLQMaxBytes int64        // cap total dead-letter body bytes per queue (drop oldest)
	Logger      *slog.Logger // background-loop failures log here; nil -> slog.Default()
}

// DefaultMaxMessageBytes is the default body-size cap (1 MiB, design §14-Q7).
const DefaultMaxMessageBytes = 1 << 20

type queueRow struct {
	name             string
	kind             string
	lockDurationMs   int64
	maxDeliveryCount int
	defaultTTLMs     int64
	deadLetterOnExp  bool
	dedupWindowMs    int64
	ordering         OrderingMode
	// per-queue DLQ retention overrides (MQLITE-29): 0 = inherit engine default,
	// >0 = this queue's bound, -1 = explicitly unbounded.
	dlqMaxAgeMs int64
	dlqMaxCount int64
	dlqMaxBytes int64
}

// Open opens the store (initializing the schema), performs single-broker crash
// recovery, and starts background maintenance loops.
func Open(ctx context.Context, opts Options) (*Engine, error) {
	d, err := openDB(ctx, opts.DB, opts.AuthToken, opts.Synchronous)
	if err != nil {
		return nil, err
	}
	if err := d.initSchema(ctx); err != nil {
		_ = d.close()
		return nil, err
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = func() int64 { return time.Now().UnixMilli() }
	}
	maxMsg := opts.MaxMessageBytes
	if maxMsg <= 0 {
		maxMsg = DefaultMaxMessageBytes
	}
	lg := opts.Logger
	if lg == nil {
		lg = slog.Default()
	}
	e := &Engine{
		db:          d,
		note:        newNotifier(),
		now:         nowFn,
		log:         lg,
		closed:      make(chan struct{}),
		qcache:      map[string]queueRow{},
		filterCache: map[string]*filterEntry{},
		maxMsgBytes: maxMsg,
		dlqMaxAgeMs: opts.DLQMaxAgeMs,
		dlqMaxCount: opts.DLQMaxCount,
		dlqMaxBytes: opts.DLQMaxBytes,
	}

	// Single-broker crash recovery (§4.4): any 'locked' row is an orphan from a
	// previous process — reclaim it immediately (delivery_count was already
	// incremented at claim time, so this counts as one redelivery). Same rule as
	// the reaper (MQLITE-58): a message that died on its LAST allowed attempt is
	// dead-lettered here, not redelivered an (max+1)th time — max_delivery_count
	// is a bound on deliveries, not on deliveries-without-a-crash.
	if _, err := e.db.exec(ctx, `
		UPDATE messages SET
		    state = CASE WHEN delivery_count >= (SELECT max_delivery_count FROM queues WHERE name=messages.queue)
		                 THEN 'dead_lettered' ELSE 'active' END,
		    locked_until = 0, lock_token = NULL,
		    dead_letter_reason = CASE WHEN delivery_count >= (SELECT max_delivery_count FROM queues WHERE name=messages.queue)
		                              THEN 'MaxDeliveryCountExceeded' ELSE dead_letter_reason END
		 WHERE state='locked'`); err != nil {
		_ = d.close()
		return nil, fmt.Errorf("crash recovery: %w", err)
	}

	if !opts.DisableBackground {
		var bgctx context.Context
		bgctx, e.bgCancel = context.WithCancel(context.Background())
		e.startBackground(bgctx)
	}
	return e, nil
}

func (e *Engine) Close() error {
	var err error
	e.closeOnce.Do(func() {
		if e.bgCancel != nil {
			e.bgCancel()
		}
		e.bgWG.Wait()
		close(e.closed)
		err = e.db.close()
	})
	return err
}

// Remote reports whether the underlying store is a remote Turso/libSQL DB.
func (e *Engine) Remote() bool { return e.db.remote }

// ── queue metadata ──────────────────────────────────────────────────────────

// CreateQueue creates a queue (idempotent on name; updates config if exists).
// validateQueueConfig rejects a request that would otherwise hit a SQLite CHECK or
// NOT NULL constraint and surface as an opaque 500. The enum/name guards are
// centralized here so BOTH CreateQueue and Subscribe fail loud with a typed
// ErrInvalidArgument (→ 400) — agent-facing APIs must fail predictably (MQLITE-86 / D5).
func validateQueueConfig(name string, cfg QueueConfig) error {
	if name == "" {
		return fmt.Errorf("%w: queue name required", ErrInvalidArgument)
	}
	switch cfg.Kind {
	case "", "queue", "subscription":
	default:
		return fmt.Errorf("%w: unknown kind %q (want \"queue\" or \"subscription\")", ErrInvalidArgument, cfg.Kind)
	}
	switch cfg.Ordering {
	case "", OrderStandard, OrderGroupFIFO, OrderStrictFIFO:
	default:
		return fmt.Errorf("%w: unknown ordering_mode %q (want standard, group_fifo or strict_fifo)", ErrInvalidArgument, cfg.Ordering)
	}
	return nil
}

func (e *Engine) CreateQueue(ctx context.Context, name string, cfg QueueConfig) error {
	if err := validateQueueConfig(name, cfg); err != nil {
		return err
	}
	if err := e.inTx(ctx, func(ctx context.Context, tx *txn) error {
		return e.createQueueTx(ctx, tx, name, cfg, true)
	}); err != nil {
		return err
	}
	e.qmu.Lock()
	delete(e.qcache, name)
	e.qmu.Unlock()
	return nil
}

// createQueueTx is CreateQueue's body bound to a transaction so callers
// (CreateQueue, Subscribe) keep the name guards and the insert atomic.
//
// Queue, subscription and topic names share ONE flat namespace and must stay
// disjoint (review F2 / MQLITE-57): resolveTargets resolves a name with
// subscription rows as a topic FIRST, so a queue sharing a live topic's name
// could never be reached by Send. Refuse to create the unreachable queue
// (fail loud at creation) instead of letting sends silently fan out past it.
// The upsert path for an existing queue of the same name stays open ONLY for
// the same kind: a plain CreateQueue over a subscription's backing queue (or a
// kind='subscription' request over a plain queue) is rejected rather than
// silently retuning an entity the caller doesn't think they own. A deliberate
// backing-queue reconfig stays possible by passing Kind:"subscription".
// createQueueTx creates the queue `name`, or — when overwrite is true — reconfigures an
// existing one from cfg (CreateQueue's documented "update config if exists" contract).
// overwrite=false only ensures the row exists and never touches an existing queue's config:
// Subscribe uses it so re-subscribing (a filter update) can't reset the backing queue's
// lock/delivery/TTL/dedup/ordering/DLQ settings (MQLITE-73). The name guards run either way.
func (e *Engine) createQueueTx(ctx context.Context, tx *txn, name string, cfg QueueConfig, overwrite bool) error {
	// Central guard: also runs for Subscribe's backing-queue creation, so a bad name
	// or enum can't reach the INSERT and CHECK-fault into a 500 (MQLITE-86).
	if err := validateQueueConfig(name, cfg); err != nil {
		return err
	}
	kind := cfg.Kind
	if kind == "" {
		kind = "queue"
	}
	var one int
	err := tx.QueryRowContext(ctx,
		`SELECT 1 FROM subscriptions WHERE topic=? LIMIT 1`, name).Scan(&one)
	if err == nil {
		return fmt.Errorf("%w: %q is already a topic", ErrNameConflict, name)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var existingKind string
	err = tx.QueryRowContext(ctx,
		`SELECT kind FROM queues WHERE name=?`, name).Scan(&existingKind)
	if err == nil && existingKind != kind {
		return fmt.Errorf("%w: %q is already a %s", ErrNameConflict, name, existingKind)
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	lock := cfg.LockDurationMs
	if lock <= 0 {
		lock = 30000
	}
	maxdc := cfg.MaxDeliveryCount
	if maxdc <= 0 {
		maxdc = 10
	}
	dle := 1
	if cfg.DeadLetterOnExpire != nil && !*cfg.DeadLetterOnExpire {
		dle = 0
	}
	ordering := cfg.Ordering
	if ordering == "" {
		ordering = OrderStandard
	}
	now := e.now()
	// One statement either way (Hrana wants one per exec). overwrite chooses the conflict
	// action: reconfigure (CreateQueue) vs leave an existing queue untouched (Subscribe).
	conflict := ` ON CONFLICT(name) DO NOTHING`
	if overwrite {
		conflict = ` ON CONFLICT(name) DO UPDATE SET
		    lock_duration_ms=excluded.lock_duration_ms,
		    max_delivery_count=excluded.max_delivery_count,
		    default_ttl_ms=excluded.default_ttl_ms,
		    dead_letter_on_expire=excluded.dead_letter_on_expire,
		    dedup_window_ms=excluded.dedup_window_ms,
		    ordering_mode=excluded.ordering_mode,
		    dlq_max_age_ms=excluded.dlq_max_age_ms,
		    dlq_max_count=excluded.dlq_max_count,
		    dlq_max_bytes=excluded.dlq_max_bytes,
		    updated_at=excluded.updated_at`
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO queues (name,kind,lock_duration_ms,max_delivery_count,default_ttl_ms,
		                    dead_letter_on_expire,dedup_window_ms,ordering_mode,
		                    dlq_max_age_ms,dlq_max_count,dlq_max_bytes,created_at,updated_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`+conflict,
		name, kind, lock, maxdc, cfg.DefaultTTLMs, dle, cfg.DedupWindowMs, string(ordering),
		cfg.DLQMaxAgeMs, cfg.DLQMaxCount, cfg.DLQMaxBytes, now, now)
	return err
}

// Subscribe registers subscription `name` under `topic`, creating the
// subscription's backing queue and the fan-out mapping (§10.1).
//
// Names are guarded in BOTH directions so queue, subscription and topic names
// stay one disjoint namespace (MQLITE-57; ASB works the same way — entities
// share a namespace and same-name creation conflicts):
//   - the subscription name must not belong to a plain queue or to a different
//     topic's subscription — reusing it would silently merge two delivery
//     targets into one backing queue (pub/sub isolation breach, eval report r2
//     §architecture/P0-3);
//   - the TOPIC name must not belong to any existing queue or subscription —
//     resolveTargets resolves a subscribed name as a topic first, so a topic
//     shadowing a live queue would silently reroute every Send for that name
//     to the fan-out and starve the queue (review F2);
//   - topic == name is rejected: it would make the backing queue's own name
//     resolve as the topic.
//
// Per-topic-scoped subscription naming (reusing a name across topics) was
// considered and rejected in favor of this flat disjoint namespace — it moves
// the ambiguity into addressing instead of removing it.
func (e *Engine) Subscribe(ctx context.Context, topic, name string, filter *Filter) error {
	if topic == "" || name == "" {
		return fmt.Errorf("%w: topic and subscription name required", ErrInvalidArgument)
	}
	if topic == name {
		return fmt.Errorf("%w: subscription %q cannot use its own topic's name", ErrNameConflict, name)
	}
	// Validate the filter expression up front: a bad one is rejected here
	// (ErrInvalidFilter -> 400) with the precise compiler error and never stored.
	if filter != nil {
		if _, err := compileFilter(filter.Expr); err != nil {
			return err
		}
	}
	var fj sql.NullString
	if filter != nil {
		b, err := json.Marshal(filter)
		if err != nil {
			return err
		}
		fj = sql.NullString{String: string(b), Valid: true}
	}
	// Guards + backing-queue upsert + mapping upsert run in ONE transaction so a
	// concurrent CreateQueue/Subscribe can't slip between check and insert (the
	// local single connection serializes anyway; the remote pool does not).
	if err := e.inTx(ctx, func(ctx context.Context, tx *txn) error {
		var topicKind string
		err := tx.QueryRowContext(ctx,
			`SELECT kind FROM queues WHERE name=?`, topic).Scan(&topicKind)
		if err == nil {
			return fmt.Errorf("%w: topic %q is already a %s", ErrNameConflict, topic, topicKind)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var existingKind sql.NullString
		if err := tx.QueryRowContext(ctx,
			`SELECT kind FROM queues WHERE name=?`, name).Scan(&existingKind); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if existingKind.Valid {
			if existingKind.String != "subscription" {
				return fmt.Errorf("%w: %q is already a queue", ErrNameConflict, name)
			}
			var otherTopic sql.NullString
			if err := tx.QueryRowContext(ctx,
				`SELECT topic FROM subscriptions WHERE subscription=? AND topic<>? LIMIT 1`,
				name, topic).Scan(&otherTopic); err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if otherTopic.Valid {
				return fmt.Errorf("%w: subscription %q already exists under topic %q",
					ErrNameConflict, name, otherTopic.String)
			}
		}
		// createQueueTx additionally rejects a subscription name that is itself a
		// live topic — the same shadowing hazard from the other side.
		if err := e.createQueueTx(ctx, tx, name, QueueConfig{Kind: "subscription"}, false); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO subscriptions (topic,subscription,filter_json,created_at)
			VALUES (?,?,?,?)
			ON CONFLICT(topic,subscription) DO UPDATE SET filter_json=excluded.filter_json`,
			topic, name, fj, e.now())
		return err
	}); err != nil {
		return err
	}
	e.qmu.Lock()
	delete(e.qcache, name)
	e.qmu.Unlock()
	// A re-subscribe may have changed the filter — drop any cached program so the
	// next publish recompiles from the freshly stored expression.
	e.invalidateFilter(name)
	return nil
}

// SubscriptionInfo describes one topic→subscription mapping and its filter.
type SubscriptionInfo struct {
	Topic string `json:"topic"`
	Name  string `json:"name"`
	Expr  string `json:"expr,omitempty"` // the filter expression; "" = match all
}

// ListSubscriptions returns every subscription with its topic and filter expression,
// ordered by topic then name. (ListQueues shows the backing queues; this exposes the
// topic membership + filter that ListQueues does not.)
func (e *Engine) ListSubscriptions(ctx context.Context) ([]SubscriptionInfo, error) {
	var out []SubscriptionInfo
	err := e.db.queryRows(ctx,
		`SELECT topic, subscription, filter_json FROM subscriptions ORDER BY topic, subscription`,
		func(rows *sql.Rows) error {
			for rows.Next() {
				var topic, sub string
				var fj sql.NullString
				if err := rows.Scan(&topic, &sub, &fj); err != nil {
					return err
				}
				s := SubscriptionInfo{Topic: topic, Name: sub}
				if fj.Valid && fj.String != "" {
					var f Filter
					if json.Unmarshal([]byte(fj.String), &f) == nil {
						s.Expr = f.Expr
					}
				}
				out = append(out, s)
			}
			return rows.Err()
		})
	return out, err
}

func (e *Engine) loadQueue(ctx context.Context, name string) (queueRow, error) {
	e.qmu.RLock()
	q, ok := e.qcache[name]
	e.qmu.RUnlock()
	if ok {
		return q, nil
	}
	var dle int
	var ordering string
	if err := e.db.queryRowScan(ctx,
		[]any{&q.name, &q.kind, &q.lockDurationMs, &q.maxDeliveryCount, &q.defaultTTLMs, &dle, &q.dedupWindowMs, &ordering,
			&q.dlqMaxAgeMs, &q.dlqMaxCount, &q.dlqMaxBytes}, `
		SELECT name,kind,lock_duration_ms,max_delivery_count,default_ttl_ms,
		       dead_letter_on_expire,dedup_window_ms,ordering_mode,
		       dlq_max_age_ms,dlq_max_count,dlq_max_bytes FROM queues WHERE name=?`, name); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return queueRow{}, ErrQueueNotFound
		}
		return queueRow{}, err
	}
	q.deadLetterOnExp = dle != 0
	q.ordering = OrderingMode(ordering)
	e.qmu.Lock()
	e.qcache[name] = q
	e.qmu.Unlock()
	return q, nil
}

// ListQueues lists all queues/subscriptions.
func (e *Engine) ListQueues(ctx context.Context) ([]QueueInfo, error) {
	var out []QueueInfo
	err := e.db.queryRows(ctx, `
		SELECT name,kind,lock_duration_ms,max_delivery_count,default_ttl_ms,dedup_window_ms
		FROM queues ORDER BY name`,
		func(rows *sql.Rows) error {
			for rows.Next() {
				var qi QueueInfo
				if err := rows.Scan(&qi.Name, &qi.Kind, &qi.LockDurationMs, &qi.MaxDeliveryCount,
					&qi.DefaultTTLMs, &qi.DedupWindowMs); err != nil {
					return err
				}
				out = append(out, qi)
			}
			return rows.Err()
		})
	return out, err
}

// ── send / schedule ─────────────────────────────────────────────────────────

// SendOne enqueues one message, returning its seq_number. A dedup conflict
// (same id, different body) surfaces as ErrDedupConflict. Publishing to a topic
// that no subscription filter accepts is a valid no-op: it returns (0, nil).
func (e *Engine) SendOne(ctx context.Context, queue string, m OutMessage) (int64, error) {
	seqs, conflicts, err := e.sendTracked(ctx, queue, []OutMessage{m}, 0, StateActive)
	if err != nil {
		return 0, err
	}
	if seqs[0] == 0 && conflicts[0] { // 0 because of a dedup conflict, not a no-match
		return 0, ErrDedupConflict
	}
	return seqs[0], nil
}

// Send enqueues one or many messages in one transaction (§11.3 Batch).
func (e *Engine) Send(ctx context.Context, queue string, ms ...OutMessage) ([]int64, error) {
	if len(ms) == 0 {
		return nil, nil
	}
	return e.send(ctx, queue, ms, 0, StateActive)
}

// Schedule enqueues a message that becomes visible at `atMs` (epoch ms) (§8.7).
// As with SendOne, a dedup conflict surfaces as ErrDedupConflict while a publish that
// no subscription accepts is a no-op returning (0, nil).
func (e *Engine) Schedule(ctx context.Context, queue string, m OutMessage, atMs int64) (int64, error) {
	if err := validateScheduleTime(atMs, e.now()); err != nil {
		return 0, err
	}
	seqs, conflicts, err := e.sendTracked(ctx, queue, []OutMessage{m}, atMs, StateScheduled)
	if err != nil {
		return 0, err
	}
	if seqs[0] == 0 && conflicts[0] {
		return 0, ErrDedupConflict
	}
	return seqs[0], nil
}

// ScheduleBatch enqueues one or more messages to become visible at atMs, atomically: like
// Send, the whole batch commits in one transaction or none does — a mid-batch failure never
// leaves earlier items scheduled. A conflicting slot (same message_id, different body) comes
// back as seq 0 while the rest commit; a publish that matches no subscription is a valid
// no-op (seq 0). Use Schedule for a single message when you must tell a dedup conflict apart
// from a no-subscriber no-op.
func (e *Engine) ScheduleBatch(ctx context.Context, queue string, ms []OutMessage, atMs int64) ([]int64, error) {
	if len(ms) == 0 {
		return nil, nil
	}
	if err := validateScheduleTime(atMs, e.now()); err != nil {
		return nil, err
	}
	return e.send(ctx, queue, ms, atMs, StateScheduled)
}

// validateScheduleTime enforces future delivery against the ENGINE (broker) clock, so a
// schedule accepted/rejected decision doesn't depend on the caller's clock (a CLI on a
// skewed host would otherwise reject a broker-future time or accept a broker-past one —
// review 2026-07-12 P2-1 / codex). `schedule` is future delivery; use Send for immediate.
func validateScheduleTime(atMs, nowMs int64) error {
	if atMs <= nowMs {
		return fmt.Errorf("%w: scheduled time must be in the future (use Send for immediate delivery)", ErrInvalidArgument)
	}
	return nil
}

// Cancel deletes a not-yet-activated scheduled message by seq.
func (e *Engine) Cancel(ctx context.Context, queue string, seq int64) error {
	res, err := e.db.exec(ctx,
		`DELETE FROM messages WHERE id=? AND queue=? AND state='scheduled'`, seq, queue)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// Compact reclaims free database pages to the OS (MQLITE-31). New local DBs use
// auto_vacuum=INCREMENTAL, so the default drives the incremental reclaim — a WAL checkpoint,
// incremental_vacuum to completion, then a second checkpoint that hands the truncated tail
// back to the OS (the same reclaimPages the background janitor runs) — bounded, no global
// lock, janitor-friendly. full=true runs a full `VACUUM`, which rewrites the whole file and
// holds a global write lock (a maintenance-window operation). Both are local-only — a remote
// Turso/libSQL store manages its own storage.
func (e *Engine) Compact(ctx context.Context, full bool) error {
	if e.Remote() {
		return errors.New("mqlite: compact is not supported on a remote (Turso/libSQL) store")
	}
	if full {
		_, err := e.db.exec(ctx, "VACUUM")
		return err
	}
	// Incremental: run the SAME reclaim the background janitor uses. A single Exec of
	// `PRAGMA incremental_vacuum` frees only one page and never checkpoints, so almost
	// nothing is returned to the OS and the file never shrinks (MQLITE-78).
	return e.reclaimPages(ctx)
}

// send is the shared enqueue path. forced state is 'active' or 'scheduled'.
func (e *Engine) send(ctx context.Context, name string, ms []OutMessage, atMs int64, forced State) ([]int64, error) {
	seqs, _, err := e.sendTracked(ctx, name, ms, atMs, forced)
	return seqs, err
}

// sendTracked is send with per-message dedup-conflict tracking. conflicts[i] is true
// only when message i was rejected by a dedup conflict (same id, different body) —
// distinct from a 0 seq because the message matched no subscription filter, which is
// a valid no-op. The single-message wrappers use this to avoid reporting a spurious
// ErrDedupConflict for a publish that simply had no interested subscriber.
func (e *Engine) sendTracked(ctx context.Context, name string, ms []OutMessage, atMs int64, forced State) ([]int64, []bool, error) {
	for i := range ms {
		if int64(len(ms[i].Body)) > e.maxMsgBytes {
			return nil, nil, fmt.Errorf("%w: %d > %d bytes", ErrMessageTooLarge, len(ms[i].Body), e.maxMsgBytes)
		}
	}
	targets, err := e.resolveTargets(ctx, name)
	if err != nil {
		return nil, nil, err
	}
	now := e.now()
	// visible_at for the filter env: the scheduled time for a delayed send, else now.
	visibleAt := now
	if atMs > 0 {
		visibleAt = atMs
	}
	seqs := make([]int64, len(ms))
	conflicts := make([]bool, len(ms))
	woke := map[string]bool{}

	err = e.inTx(ctx, func(ctx context.Context, tx *txn) error {
		// reset per-attempt accumulators (inTx may retry the whole closure).
		for i := range seqs {
			seqs[i] = 0
			conflicts[i] = false
		}
		woke = map[string]bool{}
		for i, m := range ms {
			// fan-out: identical body to each subscription target (topic) or the one queue.
			var lastSeq int64
			for _, t := range targets {
				if !e.filterAccepts(t, m, now, visibleAt) { // empty/nil filter matches all
					continue
				}
				q, err := e.loadQueueTx(ctx, tx, t.name)
				if err != nil {
					return err
				}
				if q.ordering == OrderGroupFIFO && m.GroupID == "" {
					return ErrGroupRequired
				}
				seq, deduped, err := e.insertOne(ctx, tx, q, m, atMs, forced, now)
				if errors.Is(err, ErrDedupConflict) {
					// Batch-safe: a dedup conflict (same id, different body) skips
					// only the offending message — the rest of the batch still
					// commits. The conflicting slot stays seq=0; single Send /
					// Schedule re-surface it as ErrDedupConflict (vs a 0 that just
					// means no subscription filter accepted the message).
					conflicts[i] = true
					continue
				}
				if err != nil {
					return err
				}
				lastSeq = seq
				if !deduped && forced == StateActive {
					woke[t.name] = true
				}
			}
			seqs[i] = lastSeq
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	for q := range woke {
		e.note.notify(q)
	}
	return seqs, conflicts, nil
}

// target is a concrete deliverable queue name plus an optional compiled filter.
// entry is nil for a plain queue or an empty filter (match all).
type target struct {
	name  string
	entry *filterEntry
}

// targetFor builds a fan-out target from a subscription row, compiling and caching
// its filter expression (empty or absent → entry nil → match all).
func (e *Engine) targetFor(sub string, fj sql.NullString) target {
	t := target{name: sub}
	if fj.Valid && fj.String != "" {
		var f Filter
		if json.Unmarshal([]byte(fj.String), &f) == nil && f.Expr != "" {
			t.entry = e.compiledFilter(sub, f.Expr)
		}
	}
	return t
}

// resolveTargets expands a topic to its subscriptions, else validates the queue.
func (e *Engine) resolveTargets(ctx context.Context, name string) ([]target, error) {
	var subs []target
	if err := e.db.queryRows(ctx,
		`SELECT subscription, filter_json FROM subscriptions WHERE topic=? ORDER BY subscription`,
		func(rows *sql.Rows) error {
			for rows.Next() {
				var s string
				var fj sql.NullString
				if err := rows.Scan(&s, &fj); err != nil {
					return err
				}
				subs = append(subs, e.targetFor(s, fj))
			}
			return rows.Err()
		}, name); err != nil {
		return nil, err
	}
	if len(subs) > 0 {
		return subs, nil
	}
	// not a topic — must be an existing queue.
	if _, err := e.loadQueue(ctx, name); err != nil {
		return nil, err
	}
	return []target{{name: name}}, nil
}

func (e *Engine) loadQueueTx(ctx context.Context, tx *txn, name string) (queueRow, error) {
	e.qmu.RLock()
	q, ok := e.qcache[name]
	e.qmu.RUnlock()
	if ok {
		return q, nil
	}
	row := tx.QueryRowContext(ctx, `
		SELECT name,kind,lock_duration_ms,max_delivery_count,default_ttl_ms,
		       dead_letter_on_expire,dedup_window_ms,ordering_mode FROM queues WHERE name=?`, name)
	var dle int
	var ordering string
	if err := row.Scan(&q.name, &q.kind, &q.lockDurationMs, &q.maxDeliveryCount,
		&q.defaultTTLMs, &dle, &q.dedupWindowMs, &ordering); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return queueRow{}, ErrQueueNotFound
		}
		return queueRow{}, err
	}
	q.deadLetterOnExp = dle != 0
	q.ordering = OrderingMode(ordering)
	return q, nil
}

func propsJSON(p map[string]string) (sql.NullString, error) {
	if len(p) == 0 {
		return sql.NullString{}, nil
	}
	b, err := json.Marshal(p)
	if err != nil {
		return sql.NullString{}, err
	}
	return sql.NullString{String: string(b), Valid: true}, nil
}

func parseProps(s sql.NullString) map[string]string {
	if !s.Valid || s.String == "" {
		return nil
	}
	var m map[string]string
	if json.Unmarshal([]byte(s.String), &m) != nil {
		return nil
	}
	return m
}

func nz(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}
