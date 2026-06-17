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
	ErrNotFound        = engine.ErrNotFound
	ErrQueueNotFound   = engine.ErrQueueNotFound
	ErrDedupConflict   = engine.ErrDedupConflict
	ErrMessageTooLarge = engine.ErrMessageTooLarge
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
	Body          []byte
	MessageID     string // dedup/idempotency key; empty -> body SHA-256 when dedup on
	SessionID     string // = MessageGroupId; empty -> own group (max parallelism)
	CorrelationID string
	Subject       string // = ASB Label
	ContentType   string
	Properties    map[string]string // custom KV (headers)
	TTL           time.Duration     // 0 -> queue default
}

func (m OutMessage) toEngine() engine.OutMessage {
	return engine.OutMessage{
		Body:          m.Body,
		MessageID:     m.MessageID,
		SessionID:     m.SessionID,
		CorrelationID: m.CorrelationID,
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
}

func (c QueueConfig) toEngine() engine.QueueConfig {
	return engine.QueueConfig{
		LockDurationMs:     c.LockDuration.Milliseconds(),
		MaxDeliveryCount:   c.MaxDeliveryCount,
		DefaultTTLMs:       c.DefaultTTL.Milliseconds(),
		DeadLetterOnExpire: c.DeadLetterOnExpire,
		DedupWindowMs:      c.DedupWindow.Milliseconds(),
	}
}

// Metrics mirrors engine.Metrics with the same fields.
type Metrics = engine.Metrics

// QueueInfo mirrors engine.QueueInfo.
type QueueInfo = engine.QueueInfo

// ── receive options ─────────────────────────────────────────────────────────

type receiveConfig struct {
	max  int
	wait time.Duration
	mode engine.ReceiveMode
}

// ReceiveOption configures a Receive call.
type ReceiveOption func(*receiveConfig)

// WithMaxMessages caps how many messages a single Receive returns.
func WithMaxMessages(n int) ReceiveOption { return func(c *receiveConfig) { c.max = n } }

// WithWait enables long-polling up to d (capped at 20s).
func WithWait(d time.Duration) ReceiveOption { return func(c *receiveConfig) { c.wait = d } }

// WithReceiveAndDelete switches to at-most-once fast-path delivery.
func WithReceiveAndDelete() ReceiveOption {
	return func(c *receiveConfig) { c.mode = engine.ReceiveAndDelete }
}

func buildReceive(opts []ReceiveOption) engine.ReceiveOptions {
	c := receiveConfig{max: 1}
	for _, o := range opts {
		o(&c)
	}
	return engine.ReceiveOptions{MaxMessages: c.max, WaitMs: c.wait.Milliseconds(), Mode: c.mode}
}

// ── peek options ────────────────────────────────────────────────────────────

type peekConfig struct {
	from  int64
	state State
	max   int
}

// PeekOption configures a Peek call.
type PeekOption func(*peekConfig)

// PeekFrom starts browsing at seq.
func PeekFrom(seq int64) PeekOption { return func(c *peekConfig) { c.from = seq } }

// PeekState filters by state.
func PeekState(s State) PeekOption { return func(c *peekConfig) { c.state = s } }

// PeekMax caps results.
func PeekMax(n int) PeekOption { return func(c *peekConfig) { c.max = n } }

func buildPeek(opts []PeekOption) engine.PeekOptions {
	c := peekConfig{}
	for _, o := range opts {
		o(&c)
	}
	return engine.PeekOptions{FromSeq: c.from, State: c.state, Max: c.max}
}

// ── redrive options ─────────────────────────────────────────────────────────

type redriveConfig struct {
	target    string
	max       int
	olderThan time.Duration
	rate      int
}

// RedriveOption configures a Redrive call.
type RedriveOption func(*redriveConfig)

// RedriveTo redrives cross-queue (engine re-INSERTs with a new rowid).
func RedriveTo(target string) RedriveOption { return func(c *redriveConfig) { c.target = target } }

// RedriveMax caps how many messages are moved.
func RedriveMax(n int) RedriveOption { return func(c *redriveConfig) { c.max = n } }

// RedriveOlderThan only redrives messages older than d.
func RedriveOlderThan(d time.Duration) RedriveOption {
	return func(c *redriveConfig) { c.olderThan = d }
}

// RedriveRate limits redrive throughput (per second).
func RedriveRate(perSec int) RedriveOption { return func(c *redriveConfig) { c.rate = perSec } }

func buildRedrive(opts []RedriveOption) engine.RedriveOptions {
	c := redriveConfig{}
	for _, o := range opts {
		o(&c)
	}
	return engine.RedriveOptions{Target: c.target, Max: c.max, OlderThanMs: c.olderThan.Milliseconds(), RatePerSec: c.rate}
}

// settler is implemented by both the remote Client and the Embedded engine, so
// a *Message settles the same way regardless of transport.
type settler interface {
	complete(ctx context.Context, queue string, seq int64, token string) error
	abandon(ctx context.Context, queue string, seq int64, token string, delayMs int64) error
	deadLetter(ctx context.Context, queue string, seq int64, token, reason, desc string) error
	deferMsg(ctx context.Context, queue string, seq int64, token string) error
	renewLock(ctx context.Context, queue string, seq int64, token string) error
}

// receiveSource is implemented by Client and Embedded so Receiver works on both.
type receiveSource interface {
	settler
	receiveOne(ctx context.Context, queue string, max int, waitMs int64, mode engine.ReceiveMode) ([]*Message, error)
}
