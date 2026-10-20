package engine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
)

const (
	lifecycleBatchRows                = 1000
	lifecyclePassRows                 = 4096
	lifecyclePassResources            = 32
	lifecycleBatchBytes         int64 = 8 << 20
	subscriptionRequestWindowMs int64 = 5 * 60 * 1000
)

// DeleteOptions controls explicit destruction of a queue or subscription.
// Force discards all message states, including deliveries already in flight.
type DeleteOptions struct{ Force bool }

// DeleteResult distinguishes logical deletion from completed physical cleanup.
// A deleting entity cannot receive work and keeps its name until cleanup finishes.
type DeleteResult struct {
	Deleted    bool   `json:"deleted"`
	State      string `json:"state"`
	Generation string `json:"generation"`
}

// SubscribeOptions configures one subscription incarnation. TTL is fixed at its
// first creation; updates never extend it. Temporary creation requires a stable
// request ID and absolute retry deadline, at most five minutes in the future.
type SubscribeOptions struct {
	TTLMs              int64
	Generation         string
	RequestID          string
	RequestExpiresAtMs int64
}

// ListSubscriptionsOptions controls a bounded subscription inventory page.
// After is the opaque NextAfter cursor from the previous page. Count and results
// share one database snapshot; subsequent pages may reflect concurrent changes.
type ListSubscriptionsOptions struct {
	Topic           string
	After           string
	Limit           int
	IncludeDeleting bool
}

type SubscriptionPage struct {
	Subscriptions []SubscriptionInfo `json:"subscriptions"`
	Total         int                `json:"total"`
	NextAfter     string             `json:"next_after,omitempty"`
}

const queueSelectColumns = `name,kind,lock_duration_ms,max_delivery_count,default_ttl_ms,
	dead_letter_on_expire,dedup_window_ms,ordering_mode,dlq_max_age_ms,dlq_max_count,dlq_max_bytes,
	COALESCE((SELECT value FROM meta WHERE key='queue-generation:' || queues.name), 'legacy:' || created_at),
	lifecycle_state,expires_at,subscription_ttl_ms`

type queueScanner interface{ Scan(...any) error }

func scanQueue(row queueScanner) (queueRow, error) {
	var q queueRow
	var dle int
	var ordering string
	err := row.Scan(&q.name, &q.kind, &q.lockDurationMs, &q.maxDeliveryCount, &q.defaultTTLMs,
		&dle, &q.dedupWindowMs, &ordering, &q.dlqMaxAgeMs, &q.dlqMaxCount, &q.dlqMaxBytes,
		&q.generation, &q.lifecycleState, &q.expiresAtMs, &q.subscriptionTTLMs)
	if errors.Is(err, sql.ErrNoRows) {
		return queueRow{}, ErrQueueNotFound
	}
	q.deadLetterOnExp = dle != 0
	q.ordering = OrderingMode(ordering)
	return q, err
}

func (e *Engine) readQueueTx(ctx context.Context, tx *txn, name string) (queueRow, error) {
	return scanQueue(tx.QueryRowContext(ctx, `SELECT `+queueSelectColumns+` FROM queues WHERE name=?`, name))
}

func queueLive(q queueRow, generation string, now int64) error {
	if generation != "" && q.generation != generation {
		return ErrQueueChanged
	}
	if q.lifecycleState != "active" || (q.expiresAtMs > 0 && q.expiresAtMs <= now) {
		return ErrQueueNotFound
	}
	return nil
}

// checkQueueLiveTx reads lifecycle metadata inside the operation's transaction.
// Cached configuration must never allow an expired or replaced entity to act.
func (e *Engine) checkQueueLiveTx(ctx context.Context, tx *txn, name, generation string) (queueRow, error) {
	q, err := e.readQueueTx(ctx, tx, name)
	if err != nil {
		return q, err
	}
	return q, queueLive(q, generation, e.now())
}

func (e *Engine) invalidateLifecycle(name string) {
	e.qmu.Lock()
	delete(e.qcache, name)
	e.qepoch++
	e.qmu.Unlock()
	e.invalidateFilter(name)
	e.note.notify(name)
}

// DeleteQueueWithOptions deletes one plain queue incarnation. Empty-only is the
// default. Large cleanup is resumed by maintenance and survives broker restarts.
func (e *Engine) DeleteQueueWithOptions(ctx context.Context, name, generation string, opts DeleteOptions) (DeleteResult, error) {
	return e.deleteEntity(ctx, "", name, generation, "queue", opts)
}

// DeleteSubscription removes both the topic membership and its owned queue.
func (e *Engine) DeleteSubscription(ctx context.Context, topic, name, generation string, opts DeleteOptions) (DeleteResult, error) {
	if topic == "" {
		return DeleteResult{}, fmt.Errorf("%w: topic required", ErrInvalidArgument)
	}
	return e.deleteEntity(ctx, topic, name, generation, "subscription", opts)
}

func (e *Engine) deleteEntity(ctx context.Context, topic, name, generation, kind string, opts DeleteOptions) (DeleteResult, error) {
	if name == "" || generation == "" {
		return DeleteResult{}, fmt.Errorf("%w: name and generation required", ErrInvalidArgument)
	}
	result := DeleteResult{Generation: generation}
	err := e.inTx(ctx, func(ctx context.Context, tx *txn) error {
		result.Deleted = false
		result.State = ""
		q, err := e.readQueueTx(ctx, tx, name)
		if err != nil {
			return err
		}
		if q.generation != generation {
			return ErrQueueChanged
		}
		if q.kind != kind {
			return fmt.Errorf("%w: deletion requires kind %s", ErrInvalidArgument, kind)
		}
		if kind == "subscription" {
			var actualTopic string
			if err := tx.QueryRowContext(ctx, `SELECT topic FROM subscriptions WHERE subscription=?`, name).Scan(&actualTopic); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return ErrQueueNotFound
				}
				return err
			}
			if actualTopic != topic {
				return ErrQueueNotFound
			}
		}
		if q.lifecycleState == "active" && !opts.Force && (q.expiresAtMs <= 0 || q.expiresAtMs > e.now()) {
			var occupied bool
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM messages WHERE queue=?)`, name).Scan(&occupied); err != nil {
				return err
			}
			if occupied {
				return ErrQueueNotEmpty
			}
		}
		if q.lifecycleState == "active" {
			if _, err := tx.ExecContext(ctx, `UPDATE queues SET lifecycle_state='deleting',delete_started_at=? WHERE name=?`, e.now(), name); err != nil {
				return err
			}
		}
		budget := cleanupBudget{rows: lifecycleBatchRows, bytes: lifecycleBatchBytes}
		done, err := e.cleanEntityTx(ctx, tx, name, &budget)
		if err != nil {
			return err
		}
		result.State = "deleting"
		result.Deleted = done
		if done {
			result.State = "deleted"
		}
		return nil
	})
	if err == nil || errors.Is(err, ErrOutcomeUnknown) {
		e.invalidateLifecycle(name)
	}
	return result, err
}

type cleanupBudget struct {
	rows  int
	bytes int64
}

// cleanEntityTx never cascades a message backlog. The reserved queues row is
// removed only after bounded deletion has emptied every owned auxiliary table.
func (e *Engine) cleanEntityTx(ctx context.Context, tx *txn, name string, budget *cleanupBudget) (bool, error) {
	for _, table := range []struct{ name, size string }{
		{"messages", "length(body)+COALESCE(length(properties),0)"},
		{"dedup", "length(message_id)+COALESCE(length(request_hash),0)"},
		{"settlement_receipts", "length(lock_token)+length(operation)+length(args)"},
		{"receive_attempts", "length(response)+length(attempt_id)"},
	} {
		if budget.rows <= 0 || budget.bytes <= 0 {
			return false, nil
		}
		rows, err := tx.QueryContext(ctx, `SELECT rowid,`+table.size+` FROM `+table.name+` WHERE queue=? LIMIT ?`, name, budget.rows)
		if err != nil {
			return false, err
		}
		var ids []any
		var usedBytes int64
		for rows.Next() {
			var id, size int64
			if err := rows.Scan(&id, &size); err != nil {
				_ = rows.Close()
				return false, err
			}
			// One oversized row may exceed the byte budget, ensuring eventual progress.
			if len(ids) > 0 && usedBytes+size > budget.bytes {
				break
			}
			ids = append(ids, id)
			usedBytes += size
			if usedBytes >= budget.bytes {
				break
			}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return false, err
		}
		_ = rows.Close()
		if len(ids) > 0 {
			placeholders := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
			if _, err := tx.ExecContext(ctx, `DELETE FROM `+table.name+` WHERE rowid IN (`+placeholders+`)`, ids...); err != nil {
				return false, err
			}
			budget.rows -= len(ids)
			budget.bytes -= usedBytes
		}
		var remains bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM `+table.name+` WHERE queue=?)`, name).Scan(&remains); err != nil {
			return false, err
		}
		if remains {
			return false, nil
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM meta WHERE key='queue-generation:' || ?`, name); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM queues WHERE name=? AND lifecycle_state='deleting'`, name); err != nil {
		return false, err
	}
	tx.forgetQueue(name)
	return true, nil
}

// maintainLifecycle applies a global row/byte/resource budget, so a burst of
// expiring subscriptions cannot monopolize the single writer in one pass.
func (e *Engine) maintainLifecycle(ctx context.Context) {
	var names []string
	err := e.inTx(ctx, func(ctx context.Context, tx *txn) error {
		names = nil
		now := e.now()
		rows, err := tx.QueryContext(ctx, `SELECT name FROM queues WHERE lifecycle_state='deleting'
			UNION ALL SELECT name FROM queues WHERE lifecycle_state='active' AND expires_at>0 AND expires_at<=?
			LIMIT ?`, now, lifecyclePassResources)
		if err != nil {
			return err
		}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				_ = rows.Close()
				return err
			}
			names = append(names, name)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return err
		}
		_ = rows.Close()
		budget := cleanupBudget{rows: lifecyclePassRows, bytes: lifecycleBatchBytes}
		for _, name := range names {
			if _, err := tx.ExecContext(ctx, `UPDATE queues SET lifecycle_state='deleting',delete_started_at=CASE WHEN lifecycle_state='active' THEN ? ELSE delete_started_at END WHERE name=?`, now, name); err != nil {
				return err
			}
			if budget.rows <= 0 || budget.bytes <= 0 {
				continue
			}
			_, err := e.cleanEntityTx(ctx, tx, name, &budget)
			if err != nil {
				return err
			}
		}
		// The creation ledger has its own bounded pruning work each pass.
		_, err = tx.ExecContext(ctx, `DELETE FROM subscription_creations WHERE request_id IN
			(SELECT request_id FROM subscription_creations WHERE expires_at<=? ORDER BY expires_at LIMIT ?)`, now, lifecycleBatchRows)
		return err
	})
	if err != nil {
		e.maintenanceFailure(ctx, "resource_cleanup", err)
	}
	if err == nil || errors.Is(err, ErrOutcomeUnknown) {
		for _, name := range names {
			e.invalidateLifecycle(name)
		}
	}
}

func (e *Engine) SubscribeWithOptions(ctx context.Context, topic, name string, filter *Filter, opts SubscribeOptions) (SubscriptionInfo, error) {
	if topic == "" || name == "" {
		return SubscriptionInfo{}, fmt.Errorf("%w: topic and subscription name required", ErrInvalidArgument)
	}
	if topic == name {
		return SubscriptionInfo{}, fmt.Errorf("%w: subscription cannot use its topic name", ErrNameConflict)
	}
	if opts.TTLMs < 0 {
		return SubscriptionInfo{}, fmt.Errorf("%w: ttl must not be negative", ErrInvalidArgument)
	}
	var expr string
	if filter != nil {
		expr = filter.Expr
	}
	if _, err := compileFilter(expr); err != nil {
		return SubscriptionInfo{}, err
	}
	if opts.RequestID != "" && (len(opts.RequestID) > 128 || len(name) > 1024 || len(topic) > 1024) {
		return SubscriptionInfo{}, fmt.Errorf("%w: creation request id or entity name too long", ErrInvalidArgument)
	}
	if opts.RequestID == "" && opts.RequestExpiresAtMs != 0 {
		return SubscriptionInfo{}, fmt.Errorf("%w: request deadline requires request id", ErrInvalidArgument)
	}
	fingerprintBytes, _ := json.Marshal(struct {
		Topic, Name, Expr, Generation string
		TTL, Deadline                 int64
	}{topic, name, expr, opts.Generation, opts.TTLMs, opts.RequestExpiresAtMs})
	digest := sha256.Sum256(fingerprintBytes)
	fingerprint := hex.EncodeToString(digest[:])
	var result SubscriptionInfo
	changed := false
	err := e.inTx(ctx, func(ctx context.Context, tx *txn) error {
		changed = false
		now := e.now()
		if opts.RequestID != "" {
			if opts.RequestExpiresAtMs <= now || opts.RequestExpiresAtMs-now > subscriptionRequestWindowMs {
				return fmt.Errorf("%w: creation request deadline must be within the next five minutes", ErrInvalidArgument)
			}
			var storedFingerprint, generation, response string
			err := tx.QueryRowContext(ctx, `SELECT fingerprint,generation,response FROM subscription_creations WHERE request_id=?`, opts.RequestID).Scan(&storedFingerprint, &generation, &response)
			if err == nil {
				if fingerprint != storedFingerprint {
					return fmt.Errorf("%w: creation request id reused with different arguments", ErrInvalidArgument)
				}
				q, err := e.readQueueTx(ctx, tx, name)
				if errors.Is(err, ErrQueueNotFound) {
					return ErrQueueChanged
				}
				if err != nil {
					return err
				}
				if q.generation != generation || queueLive(q, generation, now) != nil {
					return ErrQueueChanged
				}
				return json.Unmarshal([]byte(response), &result)
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}
		var otherKind string
		err := tx.QueryRowContext(ctx, `SELECT kind FROM queues WHERE name=?`, topic).Scan(&otherKind)
		if err == nil {
			return fmt.Errorf("%w: topic %q is already a %s", ErrNameConflict, topic, otherKind)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		q, err := e.readQueueTx(ctx, tx, name)
		if err != nil && !errors.Is(err, ErrQueueNotFound) {
			return err
		}
		newEntity := errors.Is(err, ErrQueueNotFound)
		if !newEntity {
			if q.kind != "subscription" {
				return fmt.Errorf("%w: name already belongs to a queue", ErrNameConflict)
			}
			if opts.Generation != "" && opts.Generation != q.generation {
				return ErrQueueChanged
			}
			if queueLive(q, "", now) != nil {
				return fmt.Errorf("%w: subscription name is reserved until cleanup completes", ErrNameConflict)
			}
			if q.subscriptionTTLMs > 0 && opts.Generation == "" {
				return fmt.Errorf("%w: generation required to update temporary subscription", ErrInvalidArgument)
			}
			if opts.TTLMs != 0 && opts.TTLMs != q.subscriptionTTLMs {
				return fmt.Errorf("%w: subscription ttl is immutable", ErrInvalidArgument)
			}
			if opts.RequestID != "" {
				return fmt.Errorf("%w: new creation request cannot update an existing subscription", ErrNameConflict)
			}
			var otherTopic string
			err := tx.QueryRowContext(ctx, `SELECT topic FROM subscriptions WHERE subscription=?`, name).Scan(&otherTopic)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if err == nil && otherTopic != topic {
				return fmt.Errorf("%w: subscription belongs to another topic", ErrNameConflict)
			}
		} else {
			if opts.Generation != "" {
				return ErrQueueChanged
			}
			if opts.TTLMs > 0 && opts.RequestID == "" {
				return fmt.Errorf("%w: temporary subscription requires creation request id and deadline", ErrInvalidArgument)
			}
			if opts.TTLMs > math.MaxInt64-now {
				return fmt.Errorf("%w: ttl overflows expiration time", ErrInvalidArgument)
			}
			var count int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM queues WHERE kind='subscription'`).Scan(&count); err != nil {
				return err
			}
			if count >= e.maxSubscriptions {
				return fmt.Errorf("%w: subscription capacity %d", ErrResourceLimit, e.maxSubscriptions)
			}
			if opts.RequestID != "" {
				if _, err := tx.ExecContext(ctx, `DELETE FROM subscription_creations WHERE request_id IN (SELECT request_id FROM subscription_creations WHERE expires_at<=? ORDER BY expires_at LIMIT ?)`, now, lifecycleBatchRows); err != nil {
					return err
				}
				if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM subscription_creations`).Scan(&count); err != nil {
					return err
				}
				if count >= e.maxSubscriptionRequests {
					return fmt.Errorf("%w: creation request capacity %d", ErrResourceLimit, e.maxSubscriptionRequests)
				}
			}
			if err := e.createQueueTx(ctx, tx, name, QueueConfig{Kind: "subscription"}, false); err != nil {
				return err
			}
			if opts.TTLMs > 0 {
				if _, err := tx.ExecContext(ctx, `UPDATE queues SET subscription_ttl_ms=?,expires_at=created_at+? WHERE name=?`, opts.TTLMs, opts.TTLMs, name); err != nil {
					return err
				}
			}
			q, err = e.readQueueTx(ctx, tx, name)
			if err != nil {
				return err
			}
		}
		var fj sql.NullString
		if filter != nil {
			b, _ := json.Marshal(filter)
			fj = sql.NullString{String: string(b), Valid: true}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO subscriptions(topic,subscription,filter_json,created_at) VALUES(?,?,?,?)
			ON CONFLICT(topic,subscription) DO UPDATE SET filter_json=excluded.filter_json`, topic, name, fj, now); err != nil {
			return err
		}
		result = SubscriptionInfo{Topic: topic, Name: name, Expr: expr, Generation: q.generation, TTLMs: q.subscriptionTTLMs, ExpiresAtMs: q.expiresAtMs, State: "active"}
		if newEntity && opts.RequestID != "" {
			response, _ := json.Marshal(result)
			if _, err := tx.ExecContext(ctx, `INSERT INTO subscription_creations(request_id,fingerprint,subscription,generation,response,expires_at) VALUES(?,?,?,?,?,?)`, opts.RequestID, fingerprint, name, q.generation, string(response), opts.RequestExpiresAtMs); err != nil {
				return err
			}
		}
		changed = true
		return nil
	})
	if changed && (err == nil || errors.Is(err, ErrOutcomeUnknown)) {
		e.invalidateLifecycle(name)
	}
	return result, err
}

type subscriptionCursor struct {
	Topic, Name, Scope string
	IncludeDeleting    bool
}

func (e *Engine) ListSubscriptionsPage(ctx context.Context, opts ListSubscriptionsOptions) (SubscriptionPage, error) {
	if opts.Limit < 0 || opts.Limit > 1000 {
		return SubscriptionPage{}, fmt.Errorf("%w: limit must be between 1 and 1000", ErrInvalidArgument)
	}
	if opts.Limit == 0 {
		opts.Limit = 100
	}
	var cursor subscriptionCursor
	if opts.After != "" {
		data, err := base64.RawURLEncoding.DecodeString(opts.After)
		if err != nil || json.Unmarshal(data, &cursor) != nil || cursor.Name == "" || cursor.Scope != opts.Topic || cursor.IncludeDeleting != opts.IncludeDeleting {
			return SubscriptionPage{}, fmt.Errorf("%w: invalid subscription cursor", ErrInvalidArgument)
		}
	}
	page := SubscriptionPage{Subscriptions: []SubscriptionInfo{}}
	err := e.inTx(ctx, func(ctx context.Context, tx *txn) error {
		page.Subscriptions = []SubscriptionInfo{}
		page.NextAfter = ""
		now := e.now()
		where := ` WHERE 1=1`
		var args []any
		if opts.Topic != "" {
			where += ` AND s.topic=?`
			args = append(args, opts.Topic)
		}
		if !opts.IncludeDeleting {
			where += ` AND q.lifecycle_state='active' AND (q.expires_at=0 OR q.expires_at>?)`
			args = append(args, now)
		}
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM subscriptions s JOIN queues q ON q.name=s.subscription`+where, args...).Scan(&page.Total); err != nil {
			return err
		}
		if opts.After != "" {
			if opts.Topic != "" {
				where += ` AND s.subscription>?`
				args = append(args, cursor.Name)
			} else {
				where += ` AND (s.topic,s.subscription)>(?,?)`
				args = append(args, cursor.Topic, cursor.Name)
			}
		}
		args = append(args, opts.Limit+1)
		rows, err := tx.QueryContext(ctx, `SELECT s.topic,s.subscription,s.filter_json,
			COALESCE((SELECT value FROM meta WHERE key='queue-generation:' || q.name),'legacy:' || q.created_at),
			q.subscription_ttl_ms,q.expires_at,q.lifecycle_state FROM subscriptions s JOIN queues q ON q.name=s.subscription`+where+` ORDER BY s.topic,s.subscription LIMIT ?`, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var info SubscriptionInfo
			var fj sql.NullString
			if err := rows.Scan(&info.Topic, &info.Name, &fj, &info.Generation, &info.TTLMs, &info.ExpiresAtMs, &info.State); err != nil {
				return err
			}
			if info.State == "active" && info.ExpiresAtMs > 0 && info.ExpiresAtMs <= now {
				info.State = "deleting"
			}
			if fj.Valid {
				var f Filter
				if json.Unmarshal([]byte(fj.String), &f) == nil {
					info.Expr = f.Expr
				}
			}
			page.Subscriptions = append(page.Subscriptions, info)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(page.Subscriptions) > opts.Limit {
			page.Subscriptions = page.Subscriptions[:opts.Limit]
			last := page.Subscriptions[len(page.Subscriptions)-1]
			b, _ := json.Marshal(subscriptionCursor{Topic: last.Topic, Name: last.Name, Scope: opts.Topic, IncludeDeleting: opts.IncludeDeleting})
			page.NextAfter = base64.RawURLEncoding.EncodeToString(b)
		}
		return nil
	})
	return page, err
}
