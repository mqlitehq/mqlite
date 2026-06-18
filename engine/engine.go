package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
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

	bgWG      sync.WaitGroup
	bgCancel  context.CancelFunc
	closed    chan struct{}
	closeOnce sync.Once

	maxMsgBytes int64

	qmu    sync.RWMutex
	qcache map[string]queueRow
}

// Options configures Open.
type Options struct {
	DB                string       // DB DSN: file:./mq.db | :memory: | libsql://host
	AuthToken         string       // injected from env; never compiled in
	Now               func() int64 // test clock (epoch ms)
	DisableBackground bool         // skip reaper/scheduler loops (tests)
	Synchronous       string       // local SQLite PRAGMA synchronous: NORMAL(default)|FULL|OFF
	MaxMessageBytes   int64        // reject bodies larger than this; 0 -> 1 MiB (§11.4)
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
}

// Open opens (and migrates) the store, performs single-broker crash recovery,
// and starts background maintenance loops.
func Open(ctx context.Context, opts Options) (*Engine, error) {
	d, err := openDB(ctx, opts.DB, opts.AuthToken, opts.Synchronous)
	if err != nil {
		return nil, err
	}
	if err := d.migrate(ctx); err != nil {
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
	e := &Engine{
		db:          d,
		note:        newNotifier(),
		now:         nowFn,
		closed:      make(chan struct{}),
		qcache:      map[string]queueRow{},
		maxMsgBytes: maxMsg,
	}

	// Single-broker crash recovery (§4.4): any 'locked' row is an orphan from a
	// previous process — reclaim it to 'active' immediately (delivery_count was
	// already incremented at claim time, so this counts as one redelivery).
	if _, err := e.db.exec(ctx,
		`UPDATE messages SET state='active', locked_until=0, lock_token=NULL WHERE state='locked'`); err != nil {
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
func (e *Engine) CreateQueue(ctx context.Context, name string, cfg QueueConfig) error {
	if name == "" {
		return errors.New("mqlite: queue name required")
	}
	kind := cfg.Kind
	if kind == "" {
		kind = "queue"
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
	_, err := e.db.exec(ctx, `
		INSERT INTO queues (name,kind,lock_duration_ms,max_delivery_count,default_ttl_ms,
		                    dead_letter_on_expire,dedup_window_ms,ordering_mode,created_at,updated_at)
		VALUES (?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(name) DO UPDATE SET
		    lock_duration_ms=excluded.lock_duration_ms,
		    max_delivery_count=excluded.max_delivery_count,
		    default_ttl_ms=excluded.default_ttl_ms,
		    dead_letter_on_expire=excluded.dead_letter_on_expire,
		    dedup_window_ms=excluded.dedup_window_ms,
		    ordering_mode=excluded.ordering_mode,
		    updated_at=excluded.updated_at`,
		name, kind, lock, maxdc, cfg.DefaultTTLMs, dle, cfg.DedupWindowMs, string(ordering), now, now)
	if err != nil {
		return err
	}
	e.qmu.Lock()
	delete(e.qcache, name)
	e.qmu.Unlock()
	return nil
}

// Subscribe registers subscription `name` under `topic`, creating the
// subscription's backing queue and the fan-out mapping (§10.1).
func (e *Engine) Subscribe(ctx context.Context, topic, name string, filter *Filter) error {
	if topic == "" || name == "" {
		return errors.New("mqlite: topic and subscription name required")
	}
	// A subscription's backing queue is addressed by the bare subscription name
	// (messages fan out into queue `name`, and Receive/Metrics/Redrive target it
	// by that name). Reusing a name that already belongs to a plain queue, or to a
	// *different* topic's subscription, would silently merge two delivery targets
	// into one backing queue and leak messages across topics (pub/sub isolation
	// breach, eval report r2 §architecture/P0-3). Fail loud instead of corrupting
	// data. Per-topic-scoped naming (so the same name may be reused across topics)
	// is a public addressing/API change tracked separately (#13).
	var existingKind sql.NullString
	if err := e.db.queryRowScan(ctx, []any{&existingKind},
		`SELECT kind FROM queues WHERE name=?`, name); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if existingKind.Valid {
		if existingKind.String != "subscription" {
			return fmt.Errorf("%w: %q is already a queue", ErrNameConflict, name)
		}
		var otherTopic sql.NullString
		if err := e.db.queryRowScan(ctx, []any{&otherTopic},
			`SELECT topic FROM subscriptions WHERE subscription=? AND topic<>? LIMIT 1`,
			name, topic); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if otherTopic.Valid {
			return fmt.Errorf("%w: subscription %q already exists under topic %q",
				ErrNameConflict, name, otherTopic.String)
		}
	}
	cfg := QueueConfig{Kind: "subscription"}
	if err := e.CreateQueue(ctx, name, cfg); err != nil {
		return err
	}
	var fj sql.NullString
	if filter != nil {
		b, err := json.Marshal(filter)
		if err != nil {
			return err
		}
		fj = sql.NullString{String: string(b), Valid: true}
	}
	_, err := e.db.exec(ctx, `
		INSERT INTO subscriptions (topic,subscription,filter_json,created_at)
		VALUES (?,?,?,?)
		ON CONFLICT(topic,subscription) DO UPDATE SET filter_json=excluded.filter_json`,
		topic, name, fj, e.now())
	return err
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
		[]any{&q.name, &q.kind, &q.lockDurationMs, &q.maxDeliveryCount, &q.defaultTTLMs, &dle, &q.dedupWindowMs, &ordering}, `
		SELECT name,kind,lock_duration_ms,max_delivery_count,default_ttl_ms,
		       dead_letter_on_expire,dedup_window_ms,ordering_mode FROM queues WHERE name=?`, name); err != nil {
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
	rows, err := e.db.query(ctx, `
		SELECT name,kind,lock_duration_ms,max_delivery_count,default_ttl_ms,dedup_window_ms
		FROM queues ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []QueueInfo
	for rows.Next() {
		var qi QueueInfo
		if err := rows.Scan(&qi.Name, &qi.Kind, &qi.LockDurationMs, &qi.MaxDeliveryCount,
			&qi.DefaultTTLMs, &qi.DedupWindowMs); err != nil {
			return nil, err
		}
		out = append(out, qi)
	}
	return out, rows.Err()
}

// ── send / schedule ─────────────────────────────────────────────────────────

// SendOne enqueues one message, returning its seq_number. A dedup conflict
// (same id, different body) surfaces as ErrDedupConflict.
func (e *Engine) SendOne(ctx context.Context, queue string, m OutMessage) (int64, error) {
	seqs, err := e.send(ctx, queue, []OutMessage{m}, 0, StateActive)
	if err != nil {
		return 0, err
	}
	if seqs[0] == 0 { // skipped as a dedup conflict (batch path swallows it)
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
func (e *Engine) Schedule(ctx context.Context, queue string, m OutMessage, atMs int64) (int64, error) {
	seqs, err := e.send(ctx, queue, []OutMessage{m}, atMs, StateScheduled)
	if err != nil {
		return 0, err
	}
	if seqs[0] == 0 { // skipped as a dedup conflict (batch path swallows it)
		return 0, ErrDedupConflict
	}
	return seqs[0], nil
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

// send is the shared enqueue path. forced state is 'active' or 'scheduled'.
func (e *Engine) send(ctx context.Context, name string, ms []OutMessage, atMs int64, forced State) ([]int64, error) {
	for i := range ms {
		if int64(len(ms[i].Body)) > e.maxMsgBytes {
			return nil, fmt.Errorf("%w: %d > %d bytes", ErrMessageTooLarge, len(ms[i].Body), e.maxMsgBytes)
		}
	}
	targets, err := e.resolveTargets(ctx, name)
	if err != nil {
		return nil, err
	}
	now := e.now()
	seqs := make([]int64, len(ms))
	woke := map[string]bool{}

	err = e.inTx(ctx, func(tx *sql.Tx) error {
		// reset per-attempt accumulators (inTx may retry the whole closure).
		for i := range seqs {
			seqs[i] = 0
		}
		woke = map[string]bool{}
		for i, m := range ms {
			// fan-out: identical body to each subscription target (topic) or the one queue.
			var lastSeq int64
			for _, t := range targets {
				if !t.filter.match(m) { // subscription filter; nil filter matches all
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
					// Schedule re-surface it as ErrDedupConflict.
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
		return nil, err
	}
	for q := range woke {
		e.note.notify(q)
	}
	return seqs, nil
}

// target is a concrete deliverable queue name plus an optional subscription filter.
type target struct {
	name   string
	filter *Filter
}

// resolveTargets expands a topic to its subscriptions, else validates the queue.
func (e *Engine) resolveTargets(ctx context.Context, name string) ([]target, error) {
	rows, err := e.db.query(ctx,
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
		t := target{name: s}
		if fj.Valid && fj.String != "" {
			var f Filter
			if json.Unmarshal([]byte(fj.String), &f) == nil {
				t.filter = &f
			}
		}
		subs = append(subs, t)
	}
	if err := rows.Err(); err != nil {
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

func (e *Engine) loadQueueTx(ctx context.Context, tx *sql.Tx, name string) (queueRow, error) {
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
