package engine

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// CollectionStatus describes availability, not a healthy zero on collection failure.
// ErrorCode is a fixed classification; no underlying database error is exposed.
type CollectionStatus struct {
	State           string  `json:"state"`
	LastSuccessAtMs int64   `json:"last_success_at_ms"`
	DurationSeconds float64 `json:"duration_seconds"`
	ErrorCode       string  `json:"error_code"`
}

// QueueObservation is the canonical queue snapshot. Total includes unexpected
// retained states, which are reported separately rather than silently discarded.
type QueueObservation struct {
	Metrics
	Kind       string `json:"kind"`
	Unexpected int64  `json:"unexpected"`
}

type MessageCounter struct {
	Queue string `json:"queue"`
	Event string `json:"event"`
	Count uint64 `json:"count"`
}

// DurationHistogram has cumulative finite buckets; Count is also its +Inf bucket.
// Seconds are a serialization of monotonic elapsed time, independent of the engine clock.
type DurationHistogram struct {
	BoundsSeconds []float64 `json:"bounds_seconds"`
	BucketCounts  []uint64  `json:"bucket_counts"`
	Count         uint64    `json:"count"`
	SumSeconds    float64   `json:"sum_seconds"`
}

type StorageOperation struct {
	Operation string            `json:"operation"`
	Outcome   string            `json:"outcome"`
	ErrorCode string            `json:"error_code"`
	Duration  DurationHistogram `json:"duration"`
	Retries   uint64            `json:"retries"`
}

type PoolObservation struct {
	MaxOpen             int     `json:"max_open"`
	Open                int     `json:"open"`
	InUse               int     `json:"in_use"`
	Idle                int     `json:"idle"`
	WaitCount           int64   `json:"wait_count"`
	WaitDurationSeconds float64 `json:"wait_duration_seconds"`
}

type StorageObservation struct {
	Operations []StorageOperation `json:"operations"`
	Pool       PoolObservation    `json:"pool"`
}

type MaintenanceObservation struct {
	Enabled         bool              `json:"enabled"`
	Task            string            `json:"task"`
	Runs            uint64            `json:"runs"`
	Interrupted     uint64            `json:"interrupted"`
	Failures        uint64            `json:"failures"`
	LastSuccessAtMs int64             `json:"last_success_at_ms"`
	Duration        DurationHistogram `json:"duration"`
}

type FilterCounter struct {
	Stage string `json:"stage"`
	Count uint64 `json:"count"`
}

// ObservabilitySnapshot is one engine-owned contract shared by native and
// Prometheus adapters. Counters are process-local, not a durable business ledger.
// Queues is nil when collection fails; the in-memory event domains remain usable.
type ObservabilitySnapshot struct {
	SampledAtMs       int64                    `json:"sampled_at_ms"`
	StartedAtMs       int64                    `json:"started_at_ms"`
	Backend           string                   `json:"backend"`
	Runtime           RuntimeObservation       `json:"runtime"`
	Collection        CollectionStatus         `json:"collection"`
	QueueCount        int                      `json:"queue_count"`
	SubscriptionCount int                      `json:"subscription_count"`
	Queues            []QueueObservation       `json:"queues"`
	Messages          []MessageCounter         `json:"messages"`
	Storage           StorageObservation       `json:"storage"`
	Maintenance       []MaintenanceObservation `json:"maintenance"`
	Filters           []FilterCounter          `json:"filters"`
}

var durationBounds = [...]float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20}

// MessageEventNames returns the complete bounded event vocabulary. A delivery or
// enqueue includes its redelivered/scheduled subset; never sum events as a ledger.
func MessageEventNames() []string {
	return []string{"enqueued", "scheduled", "deduplicated", "dedup_conflict", "delivered", "redelivered", "completed", "receive_deleted", "abandoned", "deferred", "rejected", "dead_lettered", "ttl_discarded", "retention_deleted", "purged", "canceled", "redriven", "lock_expired", "recovered", "activated"}
}

func MaintenanceTaskNames() []string {
	return []string{"locks", "scheduled", "ttl", "dedup", "receipts", "retention", "reclaim"}
}

type eventKey struct{ queue, event string }
type observations struct {
	known       map[string]bool
	background  bool
	mu          sync.Mutex
	started     int64
	lastSuccess int64
	events      map[eventKey]uint64
	maintenance map[string]MaintenanceObservation
	filters     [2]uint64
}

func (o *observations) add(events map[eventKey]uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.events == nil {
		o.events = make(map[eventKey]uint64)
	}
	if o.known == nil {
		o.known = make(map[string]bool)
	}
	for k, n := range events {
		o.known[k.queue] = true
		o.events[k] += n
	}
}

func (e *Engine) recordMessage(queue, event string, n int64) {
	if n > 0 {
		e.observation.add(map[eventKey]uint64{{queue, event}: uint64(n)})
	}
}

// record accumulates only this transaction attempt. inTx publishes the attempt's
// effects exactly once after a confirmed commit, discarding rollback/retry state.
func (t *txn) record(queue, event string, n int64) {
	if n <= 0 {
		return
	}
	if t.events == nil {
		t.events = make(map[eventKey]uint64)
	}
	t.events[eventKey{queue, event}] += uint64(n)
}

func newDurationHistogram() DurationHistogram {
	return DurationHistogram{BoundsSeconds: append([]float64{}, durationBounds[:]...), BucketCounts: make([]uint64, len(durationBounds))}
}

func (h *DurationHistogram) observe(d time.Duration) {
	if h.BoundsSeconds == nil {
		*h = newDurationHistogram()
	}
	s := d.Seconds()
	for i, bound := range h.BoundsSeconds {
		if s <= bound {
			h.BucketCounts[i]++
		}
	}
	h.Count++
	h.SumSeconds += s
}

func cloneHistogram(h DurationHistogram) DurationHistogram {
	if h.BoundsSeconds == nil {
		return newDurationHistogram()
	}
	h.BoundsSeconds = append([]float64{}, h.BoundsSeconds...)
	h.BucketCounts = append([]uint64{}, h.BucketCounts...)
	return h
}

// queueSnapshot performs one grouped read even for an empty queue inventory.
// Its single engine-clock read supplies all ages and the observation timestamp.
func queueSnapshotQuery(queue *string) (string, []any) {
	// Aggregate messages before joining the inventory: the message indexes are
	// intentionally state-specific, so joining raw rows can rescan a deep backlog
	// once for every empty queue. The grouped relation scans retained rows once.
	query := `SELECT q.name,q.kind,
	COALESCE(m.active,0),COALESCE(m.locked,0),COALESCE(m.deferred,0),
	COALESCE(m.scheduled,0),COALESCE(m.dead_lettered,0),COALESCE(m.total,0),m.oldest
	FROM queues q LEFT JOIN (
	SELECT queue,SUM(state='active') AS active,SUM(state='locked') AS locked,
	SUM(state='deferred') AS deferred,SUM(state='scheduled') AS scheduled,
	SUM(state='dead_lettered') AS dead_lettered,COUNT(*) AS total,
	MIN(CASE WHEN state IN ('active','locked') THEN enqueued_at END) AS oldest
	FROM messages`
	var args []any
	if queue != nil {
		query += ` WHERE queue=?`
		args = append(args, *queue)
	}
	query += ` GROUP BY queue) m ON m.queue=q.name`
	if queue != nil {
		query += ` WHERE q.name=?`
		args = append(args, *queue)
	}
	query += ` ORDER BY q.name`
	return query, args
}

func (e *Engine) queueSnapshot(ctx context.Context, queue *string) ([]QueueObservation, int64, error) {
	query, args := queueSnapshotQuery(queue)
	out := make([]QueueObservation, 0)
	var oldestTimes []sql.NullInt64
	err := e.db.queryRows(ctx, query, func(rows *sql.Rows) error {
		for rows.Next() {
			var q QueueObservation
			var oldest sql.NullInt64
			if err := rows.Scan(&q.Queue, &q.Kind, &q.Active, &q.Locked, &q.Deferred, &q.Scheduled, &q.DeadLettered, &q.Total, &oldest); err != nil {
				return err
			}
			q.Unexpected = q.Total - q.Active - q.Locked - q.Deferred - q.Scheduled - q.DeadLettered
			oldestTimes = append(oldestTimes, oldest)
			out = append(out, q)
		}
		return rows.Err()
	}, args...)
	now := e.now()
	if err != nil {
		return nil, now, err
	}
	for i, oldest := range oldestTimes {
		if oldest.Valid && now > oldest.Int64 {
			out[i].OldestMessageAgeMs = now - oldest.Int64
		}
	}
	return out, now, nil
}

func (e *Engine) Observability(ctx context.Context) ObservabilitySnapshot {
	start := time.Now()
	s := ObservabilitySnapshot{Backend: "memory", Messages: []MessageCounter{}, Maintenance: []MaintenanceObservation{}, Filters: []FilterCounter{}}
	if e.db.remote {
		s.Backend = "remote"
	} else if _, ok := localFilePath(e.db.dsn); ok {
		s.Backend = "local"
	}
	s.Runtime, _ = e.runtimeObservation(ctx)
	var err error
	s.Queues, s.SampledAtMs, err = e.queueSnapshot(ctx, nil)
	s.Collection = CollectionStatus{State: "available", DurationSeconds: time.Since(start).Seconds()}
	if err != nil {
		s.Collection.State = "unavailable"
		s.Collection.ErrorCode = storageErrorCode(err)
	}
	for _, q := range s.Queues {
		if q.Kind == "subscription" {
			s.SubscriptionCount++
		} else {
			s.QueueCount++
		}
	}
	e.observation.mu.Lock()
	s.StartedAtMs = e.observation.started
	if err == nil {
		e.observation.lastSuccess = s.SampledAtMs
	}
	s.Collection.LastSuccessAtMs = e.observation.lastSuccess
	if e.observation.known == nil {
		e.observation.known = make(map[string]bool)
	}
	for _, q := range s.Queues {
		e.observation.known[q.Queue] = true
	}
	for queue := range e.observation.known {
		for _, event := range MessageEventNames() {
			s.Messages = append(s.Messages, MessageCounter{Queue: queue, Event: event, Count: e.observation.events[eventKey{queue, event}]})
		}
	}
	for _, task := range MaintenanceTaskNames() {
		m := e.observation.maintenance[task]
		m.Task = task
		m.Enabled = e.observation.background && (task != "reclaim" || s.Backend == "local")
		m.Duration = cloneHistogram(m.Duration)
		s.Maintenance = append(s.Maintenance, m)
	}
	s.Filters = []FilterCounter{{Stage: "compile", Count: e.observation.filters[0]}, {Stage: "evaluate", Count: e.observation.filters[1]}}
	e.observation.mu.Unlock()
	sort.Slice(s.Messages, func(i, j int) bool {
		if s.Messages[i].Queue != s.Messages[j].Queue {
			return s.Messages[i].Queue < s.Messages[j].Queue
		}
		return s.Messages[i].Event < s.Messages[j].Event
	})
	s.Storage = e.db.storage.snapshot(e.db.sql.Stats())
	return s
}

// RuntimeStatus shares legacy status facts and queue/subscription counting between
// the native SDK and HTTP adapters. Failed inventory retains the legacy zero counts.
func (e *Engine) RuntimeStatus(ctx context.Context) (Status, int, int) {
	s := e.Status(ctx)
	queues, err := e.ListQueues(ctx)
	if err != nil {
		return s, 0, 0
	}
	var q, n int
	for _, row := range queues {
		if row.Kind == "subscription" {
			n++
		} else {
			q++
		}
	}
	return s, q, n
}

type storageKey struct{ operation, outcome, code string }
type storageObservations struct {
	mu         sync.Mutex
	operations map[storageKey]StorageOperation
}

// StorageDimension identifies one bounded storage operation measurement.
type StorageDimension struct {
	Operation string `json:"operation"`
	Outcome   string `json:"outcome"`
	ErrorCode string `json:"error_code"`
}

// StorageDimensions returns a fresh copy of every required storage counter label.
// Future dimensions may be added without changing existing measurement meanings.
func StorageDimensions() []StorageDimension {
	out := make([]StorageDimension, 0, 33)
	for _, operation := range []string{"read", "write", "transaction"} {
		out = append(out, StorageDimension{operation, "ok", ""}, StorageDimension{operation, "rejected", "application"}, StorageDimension{operation, "outcome_unknown", "outcome_unknown"})
		for _, code := range []string{"canceled", "closed", "busy", "connection", "full", "corrupt", "io", "other"} {
			out = append(out, StorageDimension{operation, "error", code})
		}
	}
	return out
}

// Initialize the finite label domain before its first event. A series first sampled
// at one would hide the first isolated failure from rate/increase consumers.
func (o *storageObservations) initialize() {
	if o.operations != nil {
		return
	}
	o.operations = make(map[storageKey]StorageOperation)
	for _, dimension := range StorageDimensions() {
		key := storageKey{dimension.Operation, dimension.Outcome, dimension.ErrorCode}
		o.operations[key] = StorageOperation{Operation: key.operation, Outcome: key.outcome, ErrorCode: key.code, Duration: newDurationHistogram()}
	}
}

type operationContextKey struct{}
type storageAttempt struct {
	operation        string
	start            time.Time
	retries          atomic.Uint64
	callbackRejected atomic.Bool
}

func startStorage(ctx context.Context, operation string) (context.Context, *storageAttempt) {
	a := &storageAttempt{operation: operation, start: time.Now()}
	return context.WithValue(ctx, operationContextKey{}, a), a
}

func storageErrorCode(err error) string {
	switch {
	case err == nil || errors.Is(err, sql.ErrNoRows):
		return ""
	case errors.Is(err, ErrOutcomeUnknown):
		return "outcome_unknown"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "canceled"
	case errors.Is(err, ErrClosed):
		return "closed"
	case isBusyErr(err):
		return "busy"
	case isConnErr(err):
		return "connection"
	}
	for _, expected := range []error{ErrQueueNotFound, ErrLockLost, ErrUnauthenticated, ErrPermissionDenied, ErrKeyConflict, ErrDedupConflict, ErrNotFound, ErrUnsupported, ErrMessageTooLarge, ErrNameConflict, ErrGroupRequired, ErrInvalidFilter, ErrInvalidArgument} {
		if errors.Is(err, expected) {
			return "application"
		}
	}
	var coded interface{ Code() int }
	if errors.As(err, &coded) {
		switch coded.Code() & 255 {
		case 13:
			return "full"
		case 11, 26:
			return "corrupt"
		case 10:
			return "io"
		}
	}
	// Remote providers may wrap SQLite result codes as strings. Only fixed
	// classifications leave this boundary; no error text becomes a dimension.
	u := strings.ToUpper(err.Error())
	for _, p := range []struct{ match, code string }{{"SQLITE_FULL", "full"}, {"SQLITE_CORRUPT", "corrupt"}, {"SQLITE_NOTADB", "corrupt"}, {"SQLITE_IOERR", "io"}} {
		if strings.Contains(u, p.match) {
			return p.code
		}
	}
	return "other"
}

func (d *db) finishStorage(a *storageAttempt, err error) {
	code := storageErrorCode(err)
	if err != nil && a.callbackRejected.Load() && code != "canceled" && code != "closed" {
		code = "application"
	}
	outcome := "ok"
	if code != "" {
		outcome = "error"
	}
	if code == "outcome_unknown" {
		outcome = "outcome_unknown"
	}
	if code == "application" {
		outcome = "rejected"
	}
	k := storageKey{a.operation, outcome, code}
	d.storage.mu.Lock()
	defer d.storage.mu.Unlock()
	d.storage.initialize()
	o := d.storage.operations[k]
	o.Operation = a.operation
	o.Outcome = outcome
	o.ErrorCode = code
	o.Retries += a.retries.Load()
	o.Duration.observe(time.Since(a.start))
	d.storage.operations[k] = o
}

func (o *storageObservations) snapshot(stats sql.DBStats) StorageObservation {
	s := StorageObservation{Operations: []StorageOperation{}, Pool: PoolObservation{MaxOpen: stats.MaxOpenConnections, Open: stats.OpenConnections, InUse: stats.InUse, Idle: stats.Idle, WaitCount: stats.WaitCount, WaitDurationSeconds: stats.WaitDuration.Seconds()}}
	o.mu.Lock()
	o.initialize()
	for _, v := range o.operations {
		v.Duration = cloneHistogram(v.Duration)
		s.Operations = append(s.Operations, v)
	}
	o.mu.Unlock()
	sort.Slice(s.Operations, func(i, j int) bool {
		a, b := s.Operations[i], s.Operations[j]
		if a.Operation != b.Operation {
			return a.Operation < b.Operation
		}
		if a.Outcome != b.Outcome {
			return a.Outcome < b.Outcome
		}
		return a.ErrorCode < b.ErrorCode
	})
	return s
}

// Storage observations count outer operations, including waiting and safe retries.
// Transaction statements are included in the transaction duration, not double-counted.
func (d *db) exec(ctx context.Context, q string, args ...any) (sql.Result, error) {
	ctx, a := startStorage(ctx, "write")
	r, err := d.execUnobserved(ctx, q, args...)
	d.finishStorage(a, err)
	return r, err
}
func (d *db) execFresh(ctx context.Context, q string, args func() []any) (sql.Result, error) {
	ctx, a := startStorage(ctx, "write")
	r, err := d.execFreshUnobserved(ctx, q, args)
	d.finishStorage(a, err)
	return r, err
}
func (d *db) queryFresh(ctx context.Context, q string, args func() []any, scan func(*sql.Rows) error) error {
	ctx, a := startStorage(ctx, storageQueryOperation(q))
	err := d.queryFreshUnobserved(ctx, q, args, scan)
	d.finishStorage(a, err)
	return err
}
func (d *db) queryRows(ctx context.Context, q string, scan func(*sql.Rows) error, args ...any) error {
	ctx, a := startStorage(ctx, "read")
	err := d.queryRowsUnobserved(ctx, q, scan, args...)
	d.finishStorage(a, err)
	return err
}
func (d *db) queryRowScan(ctx context.Context, dest []any, q string, args ...any) error {
	ctx, a := startStorage(ctx, "read")
	err := d.queryRowScanUnobserved(ctx, dest, q, args...)
	d.finishStorage(a, err)
	return err
}
func (e *Engine) inTx(ctx context.Context, fn func(context.Context, *txn) error) error {
	ctx, a := startStorage(ctx, "transaction")
	err := e.inTxUnobserved(ctx, fn)
	e.db.finishStorage(a, err)
	return err
}

func (e *Engine) rememberQueue(name string) {
	e.observation.mu.Lock()
	defer e.observation.mu.Unlock()
	if e.observation.known == nil {
		e.observation.known = make(map[string]bool)
	}
	e.observation.known[name] = true
}

// mutateMessages records the exact returned rows after a confirmed transaction.
// The predicate is unchanged; no preflight read races the actual mutation.
func (e *Engine) mutateMessages(ctx context.Context, event, query string, args ...any) error {
	return e.inTx(ctx, func(ctx context.Context, tx *txn) error {
		rows, err := tx.QueryContext(ctx, query+` RETURNING queue,state`, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var queue, state string
			if err := rows.Scan(&queue, &state); err != nil {
				return err
			}
			tx.record(queue, event, 1)
			if state == "dead_lettered" && event != "dead_lettered" {
				tx.record(queue, "dead_lettered", 1)
			}
		}
		return rows.Err()
	})
}

type maintenanceContextKey struct{}
type maintenanceAttempt struct{ failed, interrupted bool }

func markMaintenanceFailure(ctx context.Context, err error) {
	if a, ok := ctx.Value(maintenanceContextKey{}).(*maintenanceAttempt); ok {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrClosed) {
			a.interrupted = true
		} else {
			a.failed = true
		}
	}
}
func (e *Engine) runMaintenance(ctx context.Context, task string, fn func(context.Context)) {
	if task == "reclaim" {
		if _, ok := localFilePath(e.db.dsn); e.db.remote || !ok {
			return
		}
	}
	start := time.Now()
	a := &maintenanceAttempt{}
	fn(context.WithValue(ctx, maintenanceContextKey{}, a))
	e.observation.mu.Lock()
	defer e.observation.mu.Unlock()
	if e.observation.maintenance == nil {
		e.observation.maintenance = make(map[string]MaintenanceObservation)
	}
	m := e.observation.maintenance[task]
	m.Runs++
	if a.failed {
		m.Failures++
	} else if a.interrupted || ctx.Err() != nil {
		m.Interrupted++
	} else {
		m.LastSuccessAtMs = e.now()
	}
	m.Duration.observe(time.Since(start))
	e.observation.maintenance[task] = m
}

func storageQueryOperation(query string) string {
	fields := strings.Fields(query)
	if len(fields) > 0 {
		switch strings.ToUpper(fields[0]) {
		case "UPDATE", "DELETE", "INSERT", "REPLACE":
			return "write"
		}
	}
	return "read"
}
