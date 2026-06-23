// Package wire defines the JSON-over-HTTP contract shared by the mqlite broker
// (server) and the Go SDK client. It mirrors the proto sketch in design §7.2.
// One source of truth so the two sides can never drift.
//
// JSON conventions: `body` is base64 (Go marshals []byte as base64, matching the
// curl examples in §7.4); timestamps are epoch-ms integers; seq numbers are
// integers. Unary RPC = HTTP POST to /mqlite.v1.<Service>/<Method>.
package wire

import "github.com/mqlitehq/mqlite/engine"

// Route paths (Connect-style: /package.Service/Method).
const (
	PathSend            = "/mqlite.v1.QueueService/Send"
	PathReceive         = "/mqlite.v1.QueueService/Receive"
	PathComplete        = "/mqlite.v1.QueueService/Complete"
	PathCompleteBatch   = "/mqlite.v1.QueueService/CompleteBatch"
	PathAbandon         = "/mqlite.v1.QueueService/Abandon"
	PathReject          = "/mqlite.v1.QueueService/Reject"
	PathDefer           = "/mqlite.v1.QueueService/Defer"
	PathReceiveDeferred = "/mqlite.v1.QueueService/ReceiveDeferred"
	PathRenew           = "/mqlite.v1.QueueService/Renew"
	PathSchedule        = "/mqlite.v1.QueueService/Schedule"
	PathCancel          = "/mqlite.v1.QueueService/Cancel"
	PathPeek            = "/mqlite.v1.QueueService/Peek"
	PathStats           = "/mqlite.v1.QueueService/Stats"

	PathCreateQueue       = "/mqlite.v1.AdminService/CreateQueue"
	PathSubscribe         = "/mqlite.v1.AdminService/Subscribe"
	PathListQueues        = "/mqlite.v1.AdminService/ListQueues"
	PathListSubscriptions = "/mqlite.v1.AdminService/ListSubscriptions"
	PathTestFilter        = "/mqlite.v1.AdminService/TestFilter"
	PathRedrive           = "/mqlite.v1.AdminService/Redrive"
	PathPurge             = "/mqlite.v1.AdminService/Purge"
	PathStatus            = "/mqlite.v1.AdminService/Status"
)

// Message is the wire form of a message (both send input and receive output).
type Message struct {
	SeqNumber             int64             `json:"seq_number,omitempty"`
	EnqueuedAtMs          int64             `json:"enqueued_at_ms,omitempty"`
	ExpiresAtMs           int64             `json:"expires_at_ms,omitempty"`
	VisibleAtMs           int64             `json:"visible_at_ms,omitempty"`
	LockedUntilMs         int64             `json:"locked_until_ms,omitempty"`
	DeliveryCount         int               `json:"delivery_count,omitempty"`
	LockToken             string            `json:"lock_token,omitempty"`
	State                 string            `json:"state,omitempty"`
	DeadLetterReason      string            `json:"dead_letter_reason,omitempty"`
	DeadLetterDescription string            `json:"dead_letter_description,omitempty"`
	MessageID             string            `json:"message_id,omitempty"`
	CorrelationID         string            `json:"correlation_id,omitempty"`
	ReplyTo               string            `json:"reply_to,omitempty"`
	GroupID               string            `json:"group_id,omitempty"`
	ContentType           string            `json:"content_type,omitempty"`
	Subject               string            `json:"subject,omitempty"`
	Properties            map[string]string `json:"properties,omitempty"`
	Body                  []byte            `json:"body,omitempty"` // base64 in JSON
}

type SendRequest struct {
	Queue                  string    `json:"queue"`
	Messages               []Message `json:"messages"`
	ScheduledEnqueueTimeMs int64     `json:"scheduled_enqueue_time_ms,omitempty"`
	TTLMs                  int64     `json:"ttl_ms,omitempty"`
}

// SendResponse returns one sequence number per input message, positionally.
// In a multi-message batch a slot is 0 when that message hit a dedup conflict
// (same message_id, different body): the offending message is skipped so the rest
// of the batch still commits, and seq 0 marks the slot that was NOT enqueued. A
// single-message Send instead surfaces that conflict as a 409 (already_exists)
// rather than a 200 with a 0, so a non-batch client never has to special-case it.
type SendResponse struct {
	SeqNumbers []int64 `json:"seq_numbers"`
}

type ReceiveRequest struct {
	Queue       string `json:"queue"`
	MaxMessages int    `json:"max_messages,omitempty"`
	WaitTimeMs  int64  `json:"wait_time_ms,omitempty"`
	ReceiveMode int    `json:"receive_mode,omitempty"`       // 0=peek-lock, 1=receive-and-delete
	AttemptID   string `json:"receive_attempt_id,omitempty"` // idempotency key for retried receives
}
type ReceiveResponse struct {
	Messages []Message `json:"messages"`
}

type ReceiveDeferredRequest struct {
	Queue      string  `json:"queue"`
	SeqNumbers []int64 `json:"seq_numbers"`
}

type SettleRequest struct {
	Queue                 string `json:"queue"`
	SeqNumber             int64  `json:"seq_number"`
	LockToken             string `json:"lock_token"`
	DeadLetterReason      string `json:"dead_letter_reason,omitempty"`
	DeadLetterDescription string `json:"dead_letter_description,omitempty"`
	DelayMs               int64  `json:"delay_ms,omitempty"` // Abandon backoff
}
type SettleResponse struct {
	Ok bool `json:"ok"`
}

// CompleteBatch settles many messages in one round-trip (fixes the drain N+1).
type SettleItem struct {
	SeqNumber int64  `json:"seq_number"`
	LockToken string `json:"lock_token"`
}
type CompleteBatchRequest struct {
	Queue    string       `json:"queue"`
	Messages []SettleItem `json:"messages"`
}
type SettleItemResult struct {
	SeqNumber int64 `json:"seq_number"`
	Ok        bool  `json:"ok"`
}
type CompleteBatchResponse struct {
	Results []SettleItemResult `json:"results"`
}

type CancelRequest struct {
	Queue     string `json:"queue"`
	SeqNumber int64  `json:"seq_number"`
}

type PeekRequest struct {
	Queue   string `json:"queue"`
	FromSeq int64  `json:"from_seq,omitempty"`
	State   string `json:"state,omitempty"`
	Max     int    `json:"max,omitempty"`
}
type PeekResponse struct {
	Messages []Message `json:"messages"`
}

type MetricsRequest struct {
	Queue string `json:"queue"`
}
type MetricsResponse struct {
	Queue              string `json:"queue"`
	Active             int64  `json:"active"`
	Locked             int64  `json:"locked"`
	Deferred           int64  `json:"deferred"`
	Scheduled          int64  `json:"scheduled"`
	DeadLettered       int64  `json:"dead_lettered"`
	Total              int64  `json:"total"`
	OldestMessageAgeMs int64  `json:"oldest_message_age_ms"`
}

type QueueConfigJSON struct {
	Kind               string `json:"kind,omitempty"`
	LockDurationMs     int64  `json:"lock_duration_ms,omitempty"`
	MaxDeliveryCount   int    `json:"max_delivery_count,omitempty"`
	DefaultTTLMs       int64  `json:"default_ttl_ms,omitempty"`
	DeadLetterOnExpire *bool  `json:"dead_letter_on_expire,omitempty"`
	DedupWindowMs      int64  `json:"dedup_window_ms,omitempty"`
	OrderingMode       string `json:"ordering_mode,omitempty"`
	// Per-queue DLQ retention overrides (MQLITE-29): 0 inherits the broker default,
	// >0 sets this queue's bound, -1 is explicitly unbounded.
	DLQMaxAgeMs int64 `json:"dlq_max_age_ms,omitempty"`
	DLQMaxCount int   `json:"dlq_max_count,omitempty"`
	DLQMaxBytes int64 `json:"dlq_max_bytes,omitempty"`
}
type CreateQueueRequest struct {
	Name   string          `json:"name"`
	Config QueueConfigJSON `json:"config"`
}
type SubscribeRequest struct {
	Topic  string         `json:"topic"`
	Name   string         `json:"name"`
	Filter *engine.Filter `json:"filter,omitempty"`
}
type ListQueuesResponse struct {
	Queues []QueueInfoJSON `json:"queues"`
}
type QueueInfoJSON struct {
	Name             string `json:"name"`
	Kind             string `json:"kind"`
	LockDurationMs   int64  `json:"lock_duration_ms"`
	MaxDeliveryCount int    `json:"max_delivery_count"`
	DefaultTTLMs     int64  `json:"default_ttl_ms"`
	DedupWindowMs    int64  `json:"dedup_window_ms"`
}

type RedriveRequest struct {
	Queue       string `json:"queue"`
	Target      string `json:"target,omitempty"`
	Max         int    `json:"max,omitempty"`
	OlderThanMs int64  `json:"older_than_ms,omitempty"`
	RatePerSec  int    `json:"rate_per_sec,omitempty"`
}
type RedriveResponse struct {
	Moved int `json:"moved"`
}

type PurgeRequest struct {
	Queue       string `json:"queue"`
	Max         int    `json:"max,omitempty"`
	OlderThanMs int64  `json:"older_than_ms,omitempty"`
}
type PurgeResponse struct {
	Purged int `json:"purged"`
}

// SubscriptionJSON is one topic→subscription mapping + its filter expression.
type SubscriptionJSON struct {
	Topic string `json:"topic"`
	Name  string `json:"name"`
	Expr  string `json:"expr,omitempty"`
}
type ListSubscriptionsResponse struct {
	Subscriptions []SubscriptionJSON `json:"subscriptions"`
}

// TestFilter dry-runs a filter expression: it compiles `expr` and, if `message` is
// given, evaluates it (no enqueue). Message carries the sample (body base64, subject,
// properties, and optional enqueued_at_ms/visible_at_ms for time fields).
type TestFilterRequest struct {
	Expr    string   `json:"expr"`
	Message *Message `json:"message,omitempty"`
}
type TestFilterResponse struct {
	Valid   bool   `json:"valid"`
	Error   string `json:"error,omitempty"`
	Ran     bool   `json:"ran"`
	Matched bool   `json:"matched"`
}

// StatusResponse is a desensitized runtime snapshot (AdminService/Status). It never
// includes a connection string or auth token: `location` is a local path or a masked
// remote host.
type StatusResponse struct {
	Version       string `json:"version"`
	Backend       string `json:"backend"` // memory | local file | remote libSQL/Turso
	Remote        bool   `json:"remote"`
	Location      string `json:"location"`
	SchemaVersion string `json:"schema_version"`
	PingMs        int64  `json:"ping_ms"`       // SELECT 1 read round-trip; -1 if it failed
	DBSizeBytes   int64  `json:"db_size_bytes"` // local on-disk footprint; 0 for memory/remote
	Queues        int    `json:"queues"`
	Subscriptions int    `json:"subscriptions"`
	UptimeMs      int64  `json:"uptime_ms"`
	Auth          bool   `json:"auth"`
}

type Empty struct{}

// ErrorBody is the Connect-style JSON error envelope.
type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ── conversions between wire and engine ─────────────────────────────────────

func (m Message) ToOut() engine.OutMessage {
	return engine.OutMessage{
		Body:          m.Body,
		MessageID:     m.MessageID,
		GroupID:       m.GroupID,
		CorrelationID: m.CorrelationID,
		ReplyTo:       m.ReplyTo,
		Subject:       m.Subject,
		ContentType:   m.ContentType,
		Properties:    m.Properties,
	}
}

func FromEngineMessage(m *engine.Message) Message {
	return Message{
		SeqNumber:     m.SeqNumber,
		Body:          m.Body,
		MessageID:     m.MessageID,
		GroupID:       m.GroupID,
		CorrelationID: m.CorrelationID,
		ReplyTo:       m.ReplyTo,
		Subject:       m.Subject,
		ContentType:   m.ContentType,
		Properties:    m.Properties,
		DeliveryCount: m.DeliveryCount,
		EnqueuedAtMs:  m.EnqueuedAtMs,
		LockedUntilMs: m.LockedUntilMs,
		LockToken:     m.LockToken,
	}
}

func FromPeeked(p *engine.PeekedMessage) Message {
	return Message{
		SeqNumber:             p.SeqNumber,
		State:                 string(p.State),
		Body:                  p.Body,
		MessageID:             p.MessageID,
		GroupID:               p.GroupID,
		CorrelationID:         p.CorrelationID,
		ReplyTo:               p.ReplyTo,
		Subject:               p.Subject,
		ContentType:           p.ContentType,
		Properties:            p.Properties,
		DeliveryCount:         p.DeliveryCount,
		EnqueuedAtMs:          p.EnqueuedAtMs,
		VisibleAtMs:           p.VisibleAtMs,
		ExpiresAtMs:           p.ExpiresAtMs,
		LockedUntilMs:         p.LockedUntilMs,
		DeadLetterReason:      p.DeadLetterReason,
		DeadLetterDescription: p.DeadLetterDescription,
	}
}

func (c QueueConfigJSON) ToConfig() engine.QueueConfig {
	return engine.QueueConfig{
		Kind:               c.Kind,
		LockDurationMs:     c.LockDurationMs,
		MaxDeliveryCount:   c.MaxDeliveryCount,
		DefaultTTLMs:       c.DefaultTTLMs,
		DeadLetterOnExpire: c.DeadLetterOnExpire,
		DedupWindowMs:      c.DedupWindowMs,
		Ordering:           engine.OrderingMode(c.OrderingMode),
		DLQMaxAgeMs:        c.DLQMaxAgeMs,
		DLQMaxCount:        c.DLQMaxCount,
		DLQMaxBytes:        c.DLQMaxBytes,
	}
}
