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

// OrderingMode is a queue-level delivery-ordering policy.
//
//	OrderStandard   — best-effort FIFO per group; ungrouped messages are each
//	                  their own group (max parallelism). The default.
//	OrderGroupFIFO  — strict in-order delivery within a group; GroupID is
//	                  required on every message (claim behaves like standard).
//	OrderStrictFIFO — strict single-flight global FIFO: the queue head blocks
//	                  the whole queue until it is settled (no grouping).
type OrderingMode string

const (
	OrderStandard   OrderingMode = "standard"
	OrderGroupFIFO  OrderingMode = "group_fifo"
	OrderStrictFIFO OrderingMode = "strict_fifo"
)

// Dead-letter reasons.
const (
	ReasonMaxDeliveryCount = "MaxDeliveryCountExceeded"
	ReasonTTLExpired       = "TTLExpired"
	ReasonAppRequested     = "AppRequested"
)

// Sentinel errors. The server maps these onto Connect/HTTP error codes.
var (
	ErrQueueNotFound         = errors.New("mqlite: queue not found")
	ErrLockLost              = errors.New("mqlite: lock lost or already settled")
	ErrUnauthenticated       = errors.New("mqlite: unauthenticated (bad or missing token)")
	ErrOutcomeUnknown        = errors.New("mqlite: operation outcome unknown (remote commit lost its acknowledgement — it may or may not have applied; check by message_id/dedup before retrying)")
	ErrDedupConflict         = errors.New("mqlite: dedup conflict (same id, different body)")
	ErrNotFound              = errors.New("mqlite: not found")
	ErrClosed                = errors.New("mqlite: engine closed")
	ErrMessageTooLarge       = errors.New("mqlite: message body exceeds max size")
	ErrNameConflict          = errors.New("mqlite: name already in use by another queue or topic")
	ErrGroupRequired         = errors.New("mqlite: group id required for group_fifo queue")
	ErrDBLocked              = errors.New("mqlite: database file is already open by another process")
	ErrSchemaVersionMismatch = errors.New("mqlite: database schema version is incompatible with this build")
	ErrInvalidFilter         = errors.New("mqlite: invalid subscription filter expression")
	// ErrInvalidArgument is a caller-side request/config error (empty name, unknown
	// enum, malformed body) — the server maps it to 400 invalid_argument rather than
	// letting it leak out as an opaque 500 (MQLITE-86).
	ErrInvalidArgument = errors.New("mqlite: invalid argument")
)

// QueueConfig configures a queue or subscription (entity-level defaults).
// Zero values mean "use the documented default".
type QueueConfig struct {
	Kind               string       // "queue" (default) or "subscription"
	LockDurationMs     int64        // Peek-Lock default lock duration; 0 -> 30000
	MaxDeliveryCount   int          // 0 -> 10
	DefaultTTLMs       int64        // 0 -> unlimited
	DeadLetterOnExpire *bool        // nil -> true
	DedupWindowMs      int64        // 0 -> dedup disabled
	Ordering           OrderingMode // "" -> standard
	// Per-queue DLQ retention overrides (MQLITE-29). For each: 0 -> inherit the
	// broker/engine default; >0 -> this queue's own drop-oldest bound; -1 ->
	// explicitly unbounded (opt out of the default).
	DLQMaxAgeMs int64 // dead letters older than this (by enqueued_at) are dropped
	DLQMaxCount int   // keep at most this many dead letters in this queue
	DLQMaxBytes int64 // cap total dead-letter body bytes in this queue
}

// OutMessage is a message to enqueue. Body is opaque; the broker never parses it.
type OutMessage struct {
	Body      []byte
	MessageID string // dedup / idempotency key; empty -> body SHA-256 used when dedup on
	// GroupID is an ordering / partition key (= SQS MessageGroupId, ASB SessionId):
	// messages sharing a GroupID are delivered strictly in-order (FIFO per group);
	// empty -> the message is its own group (max parallelism). It is NOT a consumer
	// group — competing consumers just Receive the same queue and peek-lock hands
	// each message to exactly one of them.
	GroupID       string
	CorrelationID string
	ReplyTo       string // = ASB ReplyTo; opaque address the consumer should reply to
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
	GroupID       string
	CorrelationID string
	ReplyTo       string
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
	GroupID               string
	CorrelationID         string
	ReplyTo               string
	Subject               string
	ContentType           string
	Properties            map[string]string
	DeliveryCount         int
	EnqueuedAtMs          int64
	VisibleAtMs           int64
	ExpiresAtMs           int64 // 0 = no TTL
	LockedUntilMs         int64
	DeadLetterReason      string
	DeadLetterDescription string
}

// Metrics mirrors pgmq-style queue counters (§7.3 Stats).
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
	// AttemptID, when set, makes Receive idempotent under client retries: a retry
	// with the same id replays the same batch (same lock tokens) instead of
	// claiming new messages / burning delivery_count (SQS ReceiveRequestAttemptId).
	AttemptID string
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
