package engine

import "errors"

// State is the lifecycle state of a message (design §6).
type State string

const (
	StateActive       State = "active"
	StateLocked       State = "locked"
	StateDeferred     State = "deferred"
	StateScheduled    State = "scheduled"
	StateCompleted    State = "completed"
	StateDeadLettered State = "dead_lettered"
)

// Dead-letter reasons.
const (
	ReasonMaxDeliveryCount = "MaxDeliveryCountExceeded"
	ReasonTTLExpired       = "TTLExpired"
	ReasonAppRequested     = "AppRequested"
)

// Sentinel errors. The server maps these onto Connect/HTTP error codes.
var (
	ErrQueueNotFound   = errors.New("mqlite: queue not found")
	ErrLockLost        = errors.New("mqlite: lock lost or already settled")
	ErrDedupConflict   = errors.New("mqlite: dedup conflict (same id, different body)")
	ErrNotFound        = errors.New("mqlite: not found")
	ErrClosed          = errors.New("mqlite: engine closed")
	ErrMessageTooLarge = errors.New("mqlite: message body exceeds max size")
)

// QueueConfig configures a queue or subscription (entity-level defaults).
// Zero values mean "use the documented default".
type QueueConfig struct {
	Kind               string // "queue" (default) or "subscription"
	LockDurationMs     int64  // Peek-Lock default lock duration; 0 -> 30000
	MaxDeliveryCount   int    // 0 -> 10
	DefaultTTLMs       int64  // 0 -> unlimited
	DeadLetterOnExpire *bool  // nil -> true
	DedupWindowMs      int64  // 0 -> dedup disabled
}

// OutMessage is a message to enqueue. Body is opaque; the broker never parses it.
type OutMessage struct {
	Body          []byte
	MessageID     string // dedup / idempotency key; empty -> body SHA-256 used when dedup on
	SessionID     string // = MessageGroupId; empty -> message is its own group (max parallelism)
	CorrelationID string
	Subject       string // = ASB Label
	ContentType   string
	Properties    map[string]string // custom KV (headers), JSON-encoded; broker does not interpret
	TTLMs         int64             // 0 -> use queue default
}

// ReceiveMode selects Peek-Lock (default) or Receive-and-Delete (at-most-once fast path).
type ReceiveMode int

const (
	PeekLock ReceiveMode = iota
	ReceiveAndDelete
)

// Message is a delivered message carrying the lock token (Peek-Lock).
type Message struct {
	SeqNumber     int64
	Body          []byte
	MessageID     string
	SessionID     string
	CorrelationID string
	Subject       string
	ContentType   string
	Properties    map[string]string
	DeliveryCount int
	EnqueuedAtMs  int64
	LockedUntilMs int64
	LockToken     string // fencing token; echoed back on settle
}

// PeekedMessage is a read-only browse result (no lock, cannot be settled).
type PeekedMessage struct {
	SeqNumber             int64
	State                 State
	Body                  []byte
	MessageID             string
	SessionID             string
	CorrelationID         string
	Subject               string
	ContentType           string
	Properties            map[string]string
	DeliveryCount         int
	EnqueuedAtMs          int64
	VisibleAtMs           int64
	LockedUntilMs         int64
	DeadLetterReason      string
	DeadLetterDescription string
}

// Metrics mirrors pgmq-style queue counters (§7.3 GetQueueMetrics).
type Metrics struct {
	Queue              string
	Active             int64
	Locked             int64
	Deferred           int64
	Scheduled          int64
	DeadLettered       int64
	Total              int64
	OldestMessageAgeMs int64
}

// QueueInfo describes a queue for listing.
type QueueInfo struct {
	Name             string
	Kind             string
	LockDurationMs   int64
	MaxDeliveryCount int
	DefaultTTLMs     int64
	DedupWindowMs    int64
}

// ReceiveOptions controls a Receive call.
type ReceiveOptions struct {
	MaxMessages int
	WaitMs      int64 // long-poll up to 20000
	Mode        ReceiveMode
}

// PeekOptions controls a Peek call.
type PeekOptions struct {
	FromSeq int64
	State   State // empty -> any state
	Max     int
}

// RedriveOptions controls a Redrive call (§11.2).
type RedriveOptions struct {
	Target      string // empty -> back to source queue (in-place); else cross-queue re-INSERT
	Max         int
	OlderThanMs int64
	RatePerSec  int
}
