package engine

import "errors"

// queueNotFoundError is both the queue-specific category and a subtype of the
// general not-found category. This keeps embedded and remote clients portable:
// callers that only care about absence can check ErrNotFound, while queue-aware
// callers retain the more precise ErrQueueNotFound match.
type queueNotFoundError struct{}

func (queueNotFoundError) Error() string { return "mqlite: queue not found" }

func (queueNotFoundError) Is(target error) bool { return target == ErrNotFound }

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
	ErrNotFound         = errors.New("mqlite: not found")
	ErrQueueNotFound    = error(queueNotFoundError{})
	ErrLockLost         = errors.New("mqlite: lock lost or already settled")
	ErrUnauthenticated  = errors.New("mqlite: unauthenticated (bad or missing token)")
	ErrPermissionDenied = errors.New("mqlite: permission denied")
	ErrKeyConflict      = errors.New("mqlite: access key id already exists")
	ErrOutcomeUnknown   = errors.New("mqlite: operation outcome unknown (the acknowledgement was lost or invalid; reconcile the operation's state before retrying)")
	ErrDedupConflict    = errors.New("mqlite: dedup conflict (same id, different body)")
	// ErrUnsupported: the BROKER does not serve this operation — an older broker that predates
	// it. Distinct from ErrNotFound ("the queue/message does not exist"), which the same broker
	// answers with a 404 too. A client can use it to fall back to an older equivalent.
	ErrUnsupported           = errors.New("mqlite: operation not supported by this broker")
	ErrClosed                = errors.New("mqlite: engine closed")
	ErrMessageTooLarge       = errors.New("mqlite: message body exceeds max size")
	ErrQueueNotEmpty         = errors.New("mqlite: queue still contains messages; drain all states before deleting")
	ErrSubscriptionNotFound  = errors.New("mqlite: subscription not found")
	ErrSubscriptionNotEmpty  = errors.New("mqlite: subscription still contains messages; drain all states before deleting")
	ErrSubscriptionLimit     = errors.New("mqlite: subscription limit reached")
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
	LockDurationMs     int64        // Peek-Lock duration; 0 -> 30000, otherwise >=1000
	MaxDeliveryCount   int          // ordinary claim budget; 0 -> 10, and the final claim remains usable
	DefaultTTLMs       int64        // default and ceiling for message TTL; 0 -> unlimited
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
	TTLMs         int64             // 0 -> queue default; negative is invalid; a positive queue default caps longer values
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
	Queue              string `json:"queue"`
	Active             int64  `json:"active"`
	Locked             int64  `json:"locked"`
	Deferred           int64  `json:"deferred"`
	Scheduled          int64  `json:"scheduled"`
	DeadLettered       int64  `json:"dead_lettered"`
	Total              int64  `json:"total"`
	OldestMessageAgeMs int64  `json:"oldest_message_age_ms"`
}

// QueueInfo describes a queue for listing.
type QueueInfo struct {
	State            string `json:"state"`
	Name             string `json:"name"`
	Kind             string `json:"kind"`
	LockDurationMs   int64  `json:"lock_duration_ms"`
	MaxDeliveryCount int    `json:"max_delivery_count"`
	DefaultTTLMs     int64  `json:"default_ttl_ms"`
	DedupWindowMs    int64  `json:"dedup_window_ms"`
}

// ReceiveOptions controls a Receive call.
type ReceiveOptions struct {
	MaxMessages int
	WaitMs      int64 // long-poll up to 20000
	Mode        ReceiveMode
	// AttemptID, when set, makes Receive idempotent under client retries: a retry
	// with the same id replays the same batch while every Peek-Lock lease remains
	// valid. If any lease is no longer owned, the stale response is invalidated and
	// the retry safely performs a new claim. ReceiveAndDelete responses remain
	// replayable for the attempt lifetime. While a non-empty saved response is
	// live, every use of its ID must keep the original Mode (SQS ReceiveRequestAttemptId).
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
