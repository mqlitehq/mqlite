package engine

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
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
	LastError *ErrorDetail      `json:"last_error,omitempty"`
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
	Enabled         bool                    `json:"enabled"`
	Task            string                  `json:"task"`
	Runs            uint64                  `json:"runs"`
	Interrupted     uint64                  `json:"interrupted"`
	Failures        uint64                  `json:"failures"`
	LastSuccessAtMs int64                   `json:"last_success_at_ms"`
	Duration        DurationHistogram       `json:"duration"`
	Diagnostics     *MaintenanceDiagnostics `json:"diagnostics,omitempty"`
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
	return []string{"locks", "scheduled", "ttl", "dedup", "receipts", "retention", "reclaim", "subscription_expiry", "lifecycle"}
}

type eventKey struct{ queue, event string }
type observations struct {
	known       map[string]bool
	initialized bool
	epoch       uint64
	background  bool
	mu          sync.Mutex
	started     int64
	lastSuccess int64
	events      map[eventKey]uint64
	maintenance map[string]MaintenanceObservation
	filters     [2]uint64
	queueCache  *queueObservationSnapshot
	queueFlight *queueObservationFlight
}

const queueObservationTTL = 10 * time.Second

// Test seams used to make topology changes at the two otherwise microscopic
// singleflight boundaries deterministic. Production leaves both nil.
var afterQueueSnapshotCollected func()
var afterGlobalQueueFlightJoin func()

// queueObservationSnapshot keeps all freshness metadata beside the rows it
// describes. A flight captures epoch before its read; a published cache entry
// carries the topology fence against which those rows were reconciled. Relabeling
// stale rows with an unrelated current epoch could erase a newly created queue.
type queueObservationSnapshot struct {
	queues          []QueueObservation
	sampledAtMs     int64
	epoch           uint64
	durationSeconds float64
	expiresAt       time.Time
}

// queueObservationFlight shares one global scan, including its failure, with
// callers that arrive while it is running. Failed results are never installed
// in queueCache, so a later call can retry.
type queueObservationFlight struct {
	done   chan struct{}
	result queueObservationSnapshot
	err    error
}

func (o *observations) addLocked(events map[eventKey]uint64, epoch uint64) {
	if o.events == nil {
		o.events = make(map[eventKey]uint64)
	}
	if o.known == nil {
		o.known = make(map[string]bool)
	}
	for k, n := range events {
		if epoch != o.epoch || (o.initialized && !o.known[k.queue]) {
			k.queue = ""
		} else {
			o.known[k.queue] = true
		}
		o.events[k] += n
	}
}

func (e *Engine) observationEpoch() uint64 {
	e.observation.mu.Lock()
	defer e.observation.mu.Unlock()
	return e.observation.epoch
}

// fenceUncertainObservation invalidates cached and in-flight topology after a
// topology commit acknowledgement is lost. It deliberately preserves known
// names and counters: the next successful inventory decides whether the commit
// landed.
func (e *Engine) fenceUncertainObservation() {
	o := &e.observation
	o.mu.Lock()
	o.epoch++
	o.queueCache = nil
	o.mu.Unlock()
}

func (e *Engine) recordMessage(queue, event string, n int64, epochs ...uint64) {
	if n > 0 {
		o := &e.observation
		o.mu.Lock()
		defer o.mu.Unlock()
		epoch := o.epoch
		if len(epochs) != 0 {
			epoch = epochs[0]
		}
		o.addLocked(map[eventKey]uint64{{queue, event}: uint64(n)}, epoch)
	}
}

// commit publishes topology changes and counters after the matching database
// commit. Events from an older topology epoch are retained in one bounded
// retired aggregate instead of recreating a deleted queue dimension.
func (o *observations) commit(t *txn) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for name := range t.retiredQueues {
		o.retireLocked(name, true)
	}
	for name := range t.forgottenQueues {
		o.retireLocked(name, false)
	}
	if len(t.rememberedQueues) > 0 {
		if o.known == nil {
			o.known = make(map[string]bool)
		}
		for name := range t.rememberedQueues {
			o.known[name] = true
		}
		o.epoch++
	}
	o.addLocked(t.events, t.observationEpoch)
}

func (o *observations) forgetLocked(name string) {
	o.retireLocked(name, false)
}

// retireLocked removes one active observation dimension. A durable
// active-to-deleting transition always fences an older scan, even when this
// process had not discovered the active resource. Physical cleanup uses the
// idempotent form as a restart/outcome-unknown reconciliation fallback.
func (o *observations) retireLocked(name string, forceFence bool) {
	_, changed := o.known[name]
	delete(o.known, name)
	for _, event := range MessageEventNames() {
		key := eventKey{name, event}
		if count := o.events[key]; count != 0 {
			changed = true
			o.events[eventKey{"", event}] += count
		}
		delete(o.events, key)
	}
	// Logical retirement is the observable topology change. Physical cleanup
	// repeats forgetQueue as a restart/outcome-unknown reconciliation fallback;
	// once the name and its counters are retired that repeat is a no-op.
	if forceFence || changed {
		o.epoch++
	}
}

// reconcileTopologyLocked applies a successful database inventory only if no
// confirmed topology callback committed after that inventory began. It also
// repairs in-memory topology after an outcome-unknown remote commit, whose
// callback deliberately did not run because the commit acknowledgement was lost.
// The caller holds o.mu.
func (o *observations) reconcileTopologyLocked(live map[string]bool, collectionEpoch uint64) {
	if collectionEpoch != o.epoch {
		return
	}
	if o.known == nil {
		o.known = make(map[string]bool)
	}
	added := false
	for name := range live {
		if !o.known[name] {
			added = true
			break
		}
	}
	removed := false
	for name := range o.known {
		if !live[name] {
			o.forgetLocked(name)
			removed = true
		}
	}
	o.known = live
	// Outcome-unknown remote commits can make an inventory the first observer of
	// a creation. Mark an add-only change too, otherwise an older inventory with
	// the same epoch could later erase the discovered name. A removal already
	// advances the epoch through forgetLocked.
	if added && !removed {
		o.epoch++
	}
}

// initializeObservations runs before startup recovery so existing queues receive
// their process-local recovery counters without later retaining deleted names.
func (e *Engine) initializeObservations(ctx context.Context) error {
	known := make(map[string]bool)
	if err := e.db.queryRows(ctx, `SELECT name FROM queues
		WHERE lifecycle_state='active' AND (expires_at=0 OR expires_at>?)`, func(rows *sql.Rows) error {
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return err
			}
			known[name] = true
		}
		return rows.Err()
	}, e.now()); err != nil {
		return err
	}
	e.observation.mu.Lock()
	e.observation.known = known
	e.observation.initialized = true
	e.observation.mu.Unlock()
	return nil
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

func (t *txn) rememberQueue(name string) {
	if t.rememberedQueues == nil {
		t.rememberedQueues = make(map[string]bool)
	}
	t.rememberedQueues[name] = true
}

func (t *txn) forgetQueue(name string) {
	if t.forgottenQueues == nil {
		t.forgottenQueues = make(map[string]bool)
	}
	t.forgottenQueues[name] = true
}

func (t *txn) retireQueue(name string) {
	if t.retiredQueues == nil {
		t.retiredQueues = make(map[string]bool)
	}
	t.retiredQueues[name] = true
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

func cloneQueueObservations(in []QueueObservation) []QueueObservation {
	if in == nil {
		return nil
	}
	out := make([]QueueObservation, len(in))
	copy(out, in)
	return out
}

// queueSnapshot performs one grouped read even for an empty queue inventory.
// Its single engine-clock read supplies all ages and the observation timestamp.
func queueSnapshotQuery(queue *string, now int64) (string, []any) {
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
	query += ` GROUP BY queue) m ON m.queue=q.name
		WHERE q.lifecycle_state='active' AND (q.expires_at=0 OR q.expires_at>?)`
	args = append(args, now)
	if queue != nil {
		query += ` AND q.name=?`
		args = append(args, *queue)
	}
	query += ` ORDER BY q.name`
	return query, args
}

func (e *Engine) queueSnapshot(ctx context.Context, queue *string) ([]QueueObservation, int64, error) {
	query, _ := queueSnapshotQuery(queue, 0)
	var now int64
	out := make([]QueueObservation, 0)
	var oldestTimes []sql.NullInt64
	err := e.db.queryFreshRead(ctx, query, func() []any {
		// Sample after acquiring the connection so the same fresh instant both
		// fences expired subscriptions and supplies the returned ages. Remote
		// retries rebuild this snapshot after their backoff.
		now = e.now()
		out = out[:0]
		oldestTimes = oldestTimes[:0]
		_, args := queueSnapshotQuery(queue, now)
		return args
	}, func(rows *sql.Rows) error {
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
	})
	if err != nil {
		return nil, now, err
	}
	if afterQueueSnapshotCollected != nil {
		afterQueueSnapshotCollected()
	}
	for i, oldest := range oldestTimes {
		if oldest.Valid && now > oldest.Int64 {
			out[i].OldestMessageAgeMs = now - oldest.Int64
		}
	}
	return out, now, nil
}

// globalQueueSnapshot caches only the expensive all-queue gauge scan. Per-queue
// Stats deliberately bypasses it and remains a fresh indexed read. The cache TTL
// uses monotonic process time; sampledAtMs remains the engine-clock instant that
// produced the gauges and can therefore stay unchanged on a cache hit.
func (e *Engine) globalQueueSnapshot(ctx context.Context) ([]QueueObservation, int64, uint64, float64, error) {
	for {
		if err := checkCtx(ctx); err != nil {
			return nil, 0, 0, 0, err
		}
		if e.db.isClosing() {
			return nil, 0, 0, 0, ErrClosed
		}

		o := &e.observation
		o.mu.Lock()
		now := time.Now()
		if cached := o.queueCache; cached != nil && cached.epoch == o.epoch && now.Before(cached.expiresAt) {
			result := *cached
			o.mu.Unlock()
			result.queues = cloneQueueObservations(result.queues)
			// Close may have started while the observation mutex was held. Cached
			// results use no connection, but they still must not report success once
			// the engine's admission gate is closed.
			if e.db.isClosing() {
				return nil, 0, 0, 0, ErrClosed
			}
			return result.queues, result.sampledAtMs, result.epoch, result.durationSeconds, nil
		}
		// Expiry and topology changes both make the snapshot unusable. Drop the
		// reference before refreshing so a failed refresh cannot look cached and a
		// deep, obsolete inventory can be reclaimed promptly.
		o.queueCache = nil
		if flight := o.queueFlight; flight != nil {
			o.mu.Unlock()
			if afterGlobalQueueFlightJoin != nil {
				afterGlobalQueueFlightJoin()
			}
			select {
			case <-flight.done:
				if err := checkCtx(ctx); err != nil {
					return nil, 0, 0, 0, err
				}
				if e.db.isClosing() {
					return nil, 0, 0, 0, ErrClosed
				}
				// A leader's deadline belongs only to that request. A live waiter
				// competes to refresh again instead of inheriting another caller's
				// cancellation; success and storage failures remain shared.
				if errors.Is(flight.err, context.Canceled) || errors.Is(flight.err, context.DeadlineExceeded) {
					continue
				}
				// A caller that arrived after a topology commit can still find the
				// older flight before its leader publishes the result. Never return that
				// successful pre-change inventory to the newer caller.
				o.mu.Lock()
				current := flight.result.epoch == o.epoch
				o.mu.Unlock()
				if flight.err == nil && !current {
					continue
				}
				return cloneQueueObservations(flight.result.queues), flight.result.sampledAtMs, flight.result.epoch, flight.result.durationSeconds, flight.err
			case <-ctx.Done():
				return nil, 0, 0, 0, ctx.Err()
			case <-e.closed:
				return nil, 0, 0, 0, ErrClosed
			}
		}

		flight := &queueObservationFlight{done: make(chan struct{})}
		flight.result.epoch = o.epoch
		o.queueFlight = flight
		o.mu.Unlock()

		refreshStart := time.Now()
		queues, sampledAtMs, err := e.queueSnapshot(ctx, nil)
		// Neither the flight nor cache may retain a slice exposed to a caller. A
		// waiter can wake and clone concurrently with the leader returning, so sharing
		// queueSnapshot's backing array here would permit caller mutation to race that
		// clone.
		immutableQueues := cloneQueueObservations(queues)
		flight.result.queues = immutableQueues
		flight.result.sampledAtMs = sampledAtMs
		flight.result.durationSeconds = time.Since(refreshStart).Seconds()
		flight.err = err

		var live map[string]bool
		if err == nil {
			live = make(map[string]bool, len(immutableQueues))
			for _, q := range immutableQueues {
				live[q.Queue] = true
			}
		}
		o.mu.Lock()
		if err == nil {
			o.lastSuccess = sampledAtMs
			// A topology callback after the scan began makes this inventory unsafe
			// to reuse. The current caller may consume its one-shot read, but the next
			// request must collect again. When the fence still matches, reconciliation
			// and publication happen under this same lock so rows and epoch stay paired.
			if flight.result.epoch == o.epoch {
				o.reconcileTopologyLocked(live, flight.result.epoch)
				flight.result.epoch = o.epoch
				flight.result.expiresAt = time.Now().Add(queueObservationTTL)
				o.queueCache = &queueObservationSnapshot{
					queues:          immutableQueues,
					sampledAtMs:     sampledAtMs,
					epoch:           flight.result.epoch,
					durationSeconds: flight.result.durationSeconds,
					expiresAt:       flight.result.expiresAt,
				}
			}
		}
		o.queueFlight = nil
		close(flight.done)
		o.mu.Unlock()

		return cloneQueueObservations(immutableQueues), sampledAtMs, flight.result.epoch, flight.result.durationSeconds, err
	}
}

func (e *Engine) Observability(ctx context.Context) ObservabilitySnapshot {
	s := ObservabilitySnapshot{Backend: "memory", Messages: []MessageCounter{}, Maintenance: []MaintenanceObservation{}, Filters: []FilterCounter{}}
	if e.db.remote {
		s.Backend = "remote"
	} else if _, ok := localFilePath(e.db.dsn); ok {
		s.Backend = "local"
	}
	var err error
	var collectionDuration float64
	s.Queues, s.SampledAtMs, _, collectionDuration, err = e.globalQueueSnapshot(ctx)
	// Join or refresh the expensive global scan before probing the same reader.
	// Otherwise every concurrent Observe queues its own SELECT 1 ahead of the
	// refresh and delays both singleflight admission and Close.
	s.Runtime, _ = e.runtimeObservation(ctx)
	s.Collection = CollectionStatus{State: "available", DurationSeconds: collectionDuration}
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
	s.Collection.LastSuccessAtMs = e.observation.lastSuccess
	for queue := range e.observation.known {
		for _, event := range MessageEventNames() {
			s.Messages = append(s.Messages, MessageCounter{Queue: queue, Event: event, Count: e.observation.events[eventKey{queue, event}]})
		}
	}
	hasRetired := false
	for _, event := range MessageEventNames() {
		hasRetired = hasRetired || e.observation.events[eventKey{"", event}] != 0
	}
	if hasRetired {
		for _, event := range MessageEventNames() {
			s.Messages = append(s.Messages, MessageCounter{Queue: "", Event: event, Count: e.observation.events[eventKey{"", event}]})
		}
	}
	for _, task := range MaintenanceTaskNames() {
		m := e.observation.maintenance[task]
		m.Task = task
		m.Enabled = e.observation.background && (task != "reclaim" || s.Backend == "local")
		m.Duration = cloneHistogram(m.Duration)
		if m.Diagnostics == nil {
			m.Diagnostics = &MaintenanceDiagnostics{LastOutcome: "never", RecentFailures: []MaintenanceFailure{}}
		} else {
			copy := *m.Diagnostics
			copy.RecentFailures = append([]MaintenanceFailure{}, copy.RecentFailures...)
			m.Diagnostics = &copy
		}
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
	s.Storage = e.db.storage.snapshot(e.db.poolStats())
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
	for _, expected := range []error{ErrQueueNotFound, ErrSubscriptionNotFound, ErrLockLost, ErrUnauthenticated, ErrPermissionDenied, ErrKeyConflict, ErrDedupConflict, ErrNotFound, ErrUnsupported, ErrMessageTooLarge, ErrNameConflict, ErrQueueNotEmpty, ErrSubscriptionNotEmpty, ErrSubscriptionLimit, ErrGroupRequired, ErrInvalidFilter, ErrInvalidArgument} {
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
	d.storage.initialize()
	o := d.storage.operations[k]
	o.Operation = a.operation
	o.Outcome = outcome
	o.ErrorCode = code
	o.Retries += a.retries.Load()
	o.Duration.observe(time.Since(a.start))
	if outcome == "error" || outcome == "outcome_unknown" {
		detail := describeError(err, time.Now().UnixMilli())
		o.LastError = &detail
	}
	d.storage.operations[k] = o
	d.storage.mu.Unlock()
	if o.LastError != nil && d.log != nil {
		detail := o.LastError
		event, level := "storage.failure", slog.LevelError
		if code == "canceled" || code == "closed" {
			event, level = "storage.interrupted", slog.LevelInfo
		}
		d.log.Log(context.Background(), level, event, "operation", a.operation, "outcome", outcome, "at_ms", detail.AtMs,
			"category", detail.Category, "cause", detail.Message, "fingerprint", detail.Fingerprint,
			"http_status", detail.HTTPStatus, "sqlite_code", detail.SQLiteCode, "retries", a.retries.Load(), "error", privateDiagnosticError(err))
	}
}

func (o *storageObservations) snapshot(stats sql.DBStats) StorageObservation {
	s := StorageObservation{Operations: []StorageOperation{}, Pool: PoolObservation{MaxOpen: stats.MaxOpenConnections, Open: stats.OpenConnections, InUse: stats.InUse, Idle: stats.Idle, WaitCount: stats.WaitCount, WaitDurationSeconds: stats.WaitDuration.Seconds()}}
	o.mu.Lock()
	o.initialize()
	for _, v := range o.operations {
		v.Duration = cloneHistogram(v.Duration)
		if v.LastError != nil {
			copy := *v.LastError
			v.LastError = &copy
		}
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
func (d *db) queryFreshRead(ctx context.Context, q string, args func() []any, scan func(*sql.Rows) error) error {
	ctx, a := startStorage(ctx, storageQueryOperation(q))
	err := d.queryFreshReadUnobserved(ctx, q, args, scan)
	d.finishStorage(a, err)
	return err
}

func (d *db) poolStats() sql.DBStats {
	s := d.sql.Stats()
	if d.reader == nil {
		return s
	}
	r := d.reader.Stats()
	s.MaxOpenConnections += r.MaxOpenConnections
	s.OpenConnections += r.OpenConnections
	s.InUse += r.InUse
	s.Idle += r.Idle
	s.WaitCount += r.WaitCount
	s.WaitDuration += r.WaitDuration
	s.MaxIdleClosed += r.MaxIdleClosed
	s.MaxIdleTimeClosed += r.MaxIdleTimeClosed
	s.MaxLifetimeClosed += r.MaxLifetimeClosed
	return s
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
func (d *db) queryRowScanRead(ctx context.Context, dest []any, q string, args ...any) error {
	ctx, a := startStorage(ctx, "read")
	err := d.queryRowScanReadUnobserved(ctx, dest, q, args...)
	d.finishStorage(a, err)
	return err
}
func (e *Engine) inTx(ctx context.Context, fn func(context.Context, *txn) error) error {
	ctx, a := startStorage(ctx, "transaction")
	err := e.inTxUnobserved(ctx, fn)
	e.db.finishStorage(a, err)
	return err
}

// mutateMessagesFresh rebuilds time-sensitive arguments inside every transaction
// attempt. Remote retries may back off across an expiry boundary, so the fence
// must use the clock observed by the attempt that can actually commit.
func (e *Engine) mutateMessagesFresh(ctx context.Context, event, query string, buildArgs func() []any) error {
	_, err := e.mutateMessagesFreshQueues(ctx, event, query, buildArgs)
	return err
}

// mutateMessagesFreshQueues is mutateMessagesFresh plus the exact set of queues
// changed by the confirmed transaction. Background wakeups use this result so a
// queue that expired between candidate discovery and the mutation is not notified.
func (e *Engine) mutateMessagesFreshQueues(ctx context.Context, event, query string, buildArgs func() []any) ([]string, error) {
	var touched []string
	err := e.inTx(ctx, func(ctx context.Context, tx *txn) error {
		touched = touched[:0]
		seen := make(map[string]struct{})
		rows, err := tx.QueryContext(ctx, query+` RETURNING queue,state`, buildArgs()...)
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
			if _, ok := seen[queue]; !ok {
				seen[queue] = struct{}{}
				touched = append(touched, queue)
			}
		}
		return rows.Err()
	})
	return touched, err
}

type maintenanceContextKey struct{}
type maintenanceAttempt struct {
	failed, interrupted bool
	task                string
	failures            []MaintenanceFailure
}

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
	a := &maintenanceAttempt{task: task}
	fn(context.WithValue(ctx, maintenanceContextKey{}, a))
	e.observation.mu.Lock()
	if e.observation.maintenance == nil {
		e.observation.maintenance = make(map[string]MaintenanceObservation)
	}
	m := e.observation.maintenance[task]
	if m.Diagnostics == nil {
		m.Diagnostics = &MaintenanceDiagnostics{LastOutcome: "never", RecentFailures: []MaintenanceFailure{}}
	}
	diag := m.Diagnostics
	diag.LastRunAtMs = e.now()
	var recoveredFailures uint64
	m.Runs++
	if a.failed {
		m.Failures++
		diag.LastOutcome = "failed"
		diag.ConsecutiveFailures++
		diag.RecoveredAtMs = 0
		diag.RecentFailures = append(diag.RecentFailures, a.failures...)
		if len(diag.RecentFailures) > maintenanceHistoryLimit {
			diag.RecentFailures = append([]MaintenanceFailure{}, diag.RecentFailures[len(diag.RecentFailures)-maintenanceHistoryLimit:]...)
		}
	} else if a.interrupted || ctx.Err() != nil {
		m.Interrupted++
		diag.LastOutcome = "interrupted"
	} else {
		if diag.ConsecutiveFailures > 0 {
			diag.RecoveredAtMs = e.now()
			recoveredFailures = diag.ConsecutiveFailures
		}
		diag.LastOutcome = "ok"
		diag.ConsecutiveFailures = 0
		m.LastSuccessAtMs = e.now()
	}
	m.Duration.observe(time.Since(start))
	e.observation.maintenance[task] = m
	recoveredAt := diag.RecoveredAtMs
	e.observation.mu.Unlock()
	if recoveredFailures > 0 && e.log != nil {
		e.log.Info("maintenance.recovered", "task", task, "at_ms", recoveredAt, "consecutive_failures", recoveredFailures)
	}
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
