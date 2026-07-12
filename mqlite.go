// Package mqlite is the native Go SDK for mqlite (design §17.1). The same method
// set drives two modes:
//
//	Open         — a remote client talking to an mqlite broker over HTTP.
//	OpenEmbedded — the queue engine in-process (like goqite), with same-DB
//	               transactional enqueue and a one-line upgrade to a broker.
//
// Settlement methods hang off *Message so the lock token never leaks into user
// code, and receive comes in two tiers: low-level Receive (you settle) and
// high-level Receiver.Run (handler returns nil -> Complete, error -> Abandon).
package mqlite

import (
	"context"
	"time"

	"github.com/mqlitehq/mqlite/engine"
)

// Re-exported sentinel errors so callers can use errors.Is on either mode.
var (
	ErrLockLost        = engine.ErrLockLost
	ErrUnauthenticated = engine.ErrUnauthenticated
	ErrOutcomeUnknown  = engine.ErrOutcomeUnknown
	ErrInvalidArgument = engine.ErrInvalidArgument
	ErrNotFound        = engine.ErrNotFound
	ErrQueueNotFound   = engine.ErrQueueNotFound
	ErrDedupConflict   = engine.ErrDedupConflict
	ErrMessageTooLarge = engine.ErrMessageTooLarge
	ErrNameConflict    = engine.ErrNameConflict
	ErrGroupRequired   = engine.ErrGroupRequired
	// ErrDBLocked is returned by OpenEmbedded when the local file DB is already open
	// in another process: embedded mode is single-process, single-writer (MQLITE-6).
	ErrDBLocked = engine.ErrDBLocked
)

// OrderingMode mirrors engine.OrderingMode for queue-level delivery ordering.
type OrderingMode = engine.OrderingMode

const (
	OrderStandard   = engine.OrderStandard
	OrderGroupFIFO  = engine.OrderGroupFIFO
	OrderStrictFIFO = engine.OrderStrictFIFO
)

// State mirrors engine.State for Peek filtering.
type State = engine.State

const (
	Active       = engine.StateActive
	Locked       = engine.StateLocked
	Deferred     = engine.StateDeferred
	Scheduled    = engine.StateScheduled
	DeadLettered = engine.StateDeadLettered
)

// Filter is a subscription filter (equality-AND + subject prefix).
type Filter = engine.Filter

// OutMessage is a message to send. Body is opaque; broker never parses it.
type OutMessage struct {
	Body      []byte
	MessageID string // dedup/idempotency key; empty -> body SHA-256 when dedup on
	// GroupID is an ordering/partition key (= SQS MessageGroupId, ASB SessionId):
	// same GroupID = strict in-order (FIFO per group); empty = own group (max
	// parallelism). NOT a consumer group — competing consumers just Receive the
	// same queue; peek-lock gives each message to exactly one.
	GroupID       string
	CorrelationID string
	ReplyTo       string // = ASB ReplyTo; opaque address the consumer should reply to
	Subject       string // = ASB Label
	ContentType   string
	Properties    map[string]string // custom KV (headers)
	TTL           time.Duration     // 0 -> queue default
}

func (m OutMessage) toEngine() engine.OutMessage {
	return engine.OutMessage{
		Body:          m.Body,
		MessageID:     m.MessageID,
		GroupID:       m.GroupID,
		CorrelationID: m.CorrelationID,
		ReplyTo:       m.ReplyTo,
		Subject:       m.Subject,
		ContentType:   m.ContentType,
		Properties:    m.Properties,
		TTLMs:         m.TTL.Milliseconds(),
	}
}

// QueueConfig configures a queue (durations as time.Duration).
type QueueConfig struct {
	LockDuration       time.Duration
	MaxDeliveryCount   int
	DefaultTTL         time.Duration
	DeadLetterOnExpire *bool
	DedupWindow        time.Duration
	Ordering           OrderingMode // "" -> standard
	// Per-queue DLQ retention overrides (MQLITE-29). For each: 0 -> inherit the
	// broker default; >0 -> this queue's drop-oldest bound; a negative value ->
	// explicitly unbounded (opt out of the default).
	DLQMaxAge   time.Duration
	DLQMaxCount int
	DLQMaxBytes int64
}

func (c QueueConfig) toEngine() engine.QueueConfig {
	return engine.QueueConfig{
		LockDurationMs:     c.LockDuration.Milliseconds(),
		MaxDeliveryCount:   c.MaxDeliveryCount,
		DefaultTTLMs:       c.DefaultTTL.Milliseconds(),
		DeadLetterOnExpire: c.DeadLetterOnExpire,
		DedupWindowMs:      c.DedupWindow.Milliseconds(),
		Ordering:           c.Ordering,
		DLQMaxAgeMs:        c.DLQMaxAge.Milliseconds(),
		DLQMaxCount:        c.DLQMaxCount,
		DLQMaxBytes:        c.DLQMaxBytes,
	}
}

// Metrics mirrors engine.Metrics with the same fields.
type Metrics = engine.Metrics

// QueueInfo mirrors engine.QueueInfo.
type QueueInfo = engine.QueueInfo

// SubscriptionInfo mirrors engine.SubscriptionInfo (topic, name, filter expression).
type SubscriptionInfo = engine.SubscriptionInfo

// FilterTestResult mirrors engine.FilterTestResult — the dry-run of a filter expression.
type FilterTestResult = engine.FilterTestResult

// StatusInfo is a desensitized runtime snapshot of the backend (Client.Status /
// Embedded.Status): never a connection string or token — Location is a local path or a
// masked remote host. Version/Queues/Subscriptions come from the broker on the remote
// path; embedded fills what it can locally (Version is empty embedded).
type StatusInfo struct {
	Version       string `json:"version,omitempty"`
	Backend       string `json:"backend"`
	Remote        bool   `json:"remote"`
	Location      string `json:"location"`
	SchemaVersion string `json:"schema_version"`
	PingMs        int64  `json:"ping_ms"`
	SizeBytes     int64  `json:"size_bytes"`
	Queues        int    `json:"queues"`
	Subscriptions int    `json:"subscriptions"`
}

// ── data-plane options ───────────────────────────────────────────────────────
//
// These are plain option structs passed as a trailing variadic argument: callers
// pass nothing for defaults, or a single literal to set fields. Construction and
// receiver/loop config (Open/OpenEmbedded/Receiver) keep their functional With…
// options — those configure the transport, not a single call.

// RecvOpts configures a Receive call.
type RecvOpts struct {
	Max        int           // most messages to claim (0 -> 1)
	Wait       time.Duration // long-poll wait (0 -> don't wait); capped at 20s
	AtMostOnce bool          // receive-and-delete (no lock, no settle)
	Attempt    string        // idempotent-receive key; a retry replays the same batch
	Pick       []int64       // fetch these deferred seqs by seq instead of claiming
}

func (o RecvOpts) mode() engine.ReceiveMode {
	if o.AtMostOnce {
		return engine.ReceiveAndDelete
	}
	return engine.PeekLock
}

func (o RecvOpts) toEngine() engine.ReceiveOptions {
	max := o.Max
	if max <= 0 {
		max = 1
	}
	return engine.ReceiveOptions{MaxMessages: max, WaitMs: o.Wait.Milliseconds(), Mode: o.mode(), AttemptID: o.Attempt}
}

// SendOpts configures a SendOne call.
type SendOpts struct {
	At time.Time // schedule delivery for t (zero -> immediate)
}

// AbandonOpts configures a Message.Abandon call.
type AbandonOpts struct {
	Delay time.Duration // re-hide for this long before redelivery (backoff)
}

// RejectOpts configures a Message.Reject call.
type RejectOpts struct {
	Reason string // dead-letter reason
	Detail string // dead-letter description
}

// PeekOpts configures a Peek call.
type PeekOpts struct {
	From  int64 // start browsing at this seq
	State State // filter by state
	Max   int   // cap results
}

func (o PeekOpts) toEngine() engine.PeekOptions {
	return engine.PeekOptions{FromSeq: o.From, State: o.State, Max: o.Max}
}

// RedriveOpts configures a Redrive call.
type RedriveOpts struct {
	To        string        // target queue (empty -> back to source)
	Max       int           // cap how many messages move
	OlderThan time.Duration // only move messages older than this
	Rate      int           // throughput limit per second
}

// olderThanMs converts a duration to ms while PRESERVING the sign of a negative value: a
// sub-millisecond negative (e.g. -1ns) would otherwise truncate to 0 and slip past the
// engine's non-negative validation, letting a bounded purge/redrive delete everything
// (review 2026-07-12). A real negative maps to -1 so validation rejects it.
func olderThanMs(d time.Duration) int64 {
	if d < 0 {
		return -1
	}
	return d.Milliseconds()
}

func (o RedriveOpts) toEngine() engine.RedriveOptions {
	return engine.RedriveOptions{Target: o.To, Max: o.Max, OlderThanMs: olderThanMs(o.OlderThan), RatePerSec: o.Rate}
}

// PurgeOpts configures a Purge call.
type PurgeOpts struct {
	Max       int           // cap how many messages are deleted
	OlderThan time.Duration // only delete messages older than this
}

func (o PurgeOpts) toEngine() engine.RedriveOptions {
	return engine.RedriveOptions{Max: o.Max, OlderThanMs: olderThanMs(o.OlderThan)}
}

// firstOpt returns opts[0] or the zero value, the shared "trailing variadic"
// idiom for the option structs above.
func firstOpt[T any](opts []T) T {
	if len(opts) > 0 {
		return opts[0]
	}
	var zero T
	return zero
}

// settler is implemented by both the remote Client and the Embedded engine, so
// a *Message settles the same way regardless of transport.
type settler interface {
	complete(ctx context.Context, queue string, seq int64, token string) error
	abandon(ctx context.Context, queue string, seq int64, token string, delayMs int64) error
	reject(ctx context.Context, queue string, seq int64, token, reason, desc string) error
	deferMsg(ctx context.Context, queue string, seq int64, token string) error
	renew(ctx context.Context, queue string, seq int64, token string) error
}

// receiveSource is implemented by Client and Embedded so Receiver works on both.
type receiveSource interface {
	settler
	receiveOne(ctx context.Context, queue string, max int, waitMs int64, mode engine.ReceiveMode, attemptID string) ([]*Message, error)
}
