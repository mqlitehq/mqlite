package mqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/wire"
)

// Client is a remote mqlite client (Connect-style JSON over HTTP).
type Client struct {
	endpoint string
	token    string
	http     *http.Client
}

type config struct {
	token      string
	httpClient *http.Client
}

// Option configures Open.
type Option func(*config)

// WithToken overrides the token from the connection string.
func WithToken(tok string) Option { return func(c *config) { c.token = tok } }

// WithHTTPClient supplies a custom *http.Client (e.g. for TLS config).
func WithHTTPClient(h *http.Client) Option { return func(c *config) { c.httpClient = h } }

// Open builds a client from a connection string: mqlite://<token>@host:port?tls=true
func Open(ctx context.Context, dsn string, opts ...Option) (*Client, error) {
	endpoint, token, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	var cfg config
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.token != "" {
		token = cfg.token
	}
	hc := cfg.httpClient
	if hc == nil {
		// No global timeout: long-poll Receive relies on the request context.
		hc = &http.Client{}
	}
	return &Client{endpoint: endpoint, token: token, http: hc}, nil
}

// Close releases client resources (no-op for HTTP).
func (c *Client) Close() error { return nil }

func (c *Client) post(ctx context.Context, path string, reqBody, respOut any) error {
	b, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		var eb wire.ErrorBody
		_ = json.NewDecoder(resp.Body).Decode(&eb)
		return mapErr(eb)
	}
	if respOut != nil {
		return json.NewDecoder(resp.Body).Decode(respOut)
	}
	return nil
}

func mapErr(eb wire.ErrorBody) error {
	switch eb.Code {
	case "not_found":
		return fmt.Errorf("%w: %s", ErrNotFound, eb.Message)
	case "already_exists":
		return fmt.Errorf("%w: %s", ErrDedupConflict, eb.Message)
	case "name_conflict":
		return fmt.Errorf("%w: %s", ErrNameConflict, eb.Message)
	case "group_required":
		return fmt.Errorf("%w: %s", ErrGroupRequired, eb.Message)
	case "message_too_large":
		return fmt.Errorf("%w: %s", ErrMessageTooLarge, eb.Message)
	case "lock_lost":
		return fmt.Errorf("%w: %s", ErrLockLost, eb.Message)
	case "unauthenticated":
		return fmt.Errorf("mqlite: unauthenticated: %s", eb.Message)
	default:
		if eb.Code == "" {
			return fmt.Errorf("mqlite: request failed")
		}
		return fmt.Errorf("mqlite: %s: %s", eb.Code, eb.Message)
	}
}

func outToWire(m OutMessage) wire.Message {
	return wire.Message{
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

func (c *Client) wireToMessage(queue string, wm wire.Message) *Message {
	return &Message{
		SequenceNumber: wm.SeqNumber,
		Body:           wm.Body,
		MessageID:      wm.MessageID,
		GroupID:        wm.GroupID,
		CorrelationID:  wm.CorrelationID,
		ReplyTo:        wm.ReplyTo,
		Subject:        wm.Subject,
		ContentType:    wm.ContentType,
		Properties:     wm.Properties,
		DeliveryCount:  wm.DeliveryCount,
		EnqueuedAt:     msToTime(wm.EnqueuedAtMs),
		LockedUntil:    msToTime(wm.LockedUntilMs),
		queue:          queue,
		lockToken:      wm.LockToken,
		s:              c,
	}
}

func msToTime(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}

// ── send ────────────────────────────────────────────────────────────────────

// SendOne enqueues one message and returns its seq. A dedup conflict (same id,
// different body) surfaces as ErrDedupConflict. SendOpts.At schedules delayed delivery.
func (c *Client) SendOne(ctx context.Context, queue string, m OutMessage, opts ...SendOpts) (int64, error) {
	o := firstOpt(opts)
	path := wire.PathSend
	req := wire.SendRequest{Queue: queue, Messages: []wire.Message{outToWire(m)}, TTLMs: m.TTL.Milliseconds()}
	if !o.At.IsZero() {
		path = wire.PathSchedule
		req.ScheduledEnqueueTimeMs = o.At.UnixMilli()
	}
	var resp wire.SendResponse
	if err := c.post(ctx, path, req, &resp); err != nil {
		return 0, err
	}
	if len(resp.SeqNumbers) == 0 || resp.SeqNumbers[0] == 0 {
		return 0, ErrDedupConflict
	}
	return resp.SeqNumbers[0], nil
}

// Send enqueues one or many messages in one request/transaction (use SendOne
// for a single seq return or At() scheduling).
func (c *Client) Send(ctx context.Context, queue string, msgs ...OutMessage) ([]int64, error) {
	if len(msgs) == 0 {
		return nil, nil
	}
	wm := make([]wire.Message, len(msgs))
	var ttl int64
	for i, m := range msgs {
		wm[i] = outToWire(m)
		if ttl == 0 {
			ttl = m.TTL.Milliseconds()
		}
	}
	var resp wire.SendResponse
	if err := c.post(ctx, wire.PathSend, wire.SendRequest{Queue: queue, Messages: wm, TTLMs: ttl}, &resp); err != nil {
		return nil, err
	}
	return resp.SeqNumbers, nil
}

// Cancel deletes a not-yet-activated scheduled message.
func (c *Client) Cancel(ctx context.Context, queue string, seq int64) error {
	return c.post(ctx, wire.PathCancel, wire.CancelRequest{Queue: queue, SeqNumber: seq}, &wire.SettleResponse{})
}

// ── receive ─────────────────────────────────────────────────────────────────

// Receive claims up to N messages (Peek-Lock by default), with optional long-poll.
// RecvOpts.Pick fetches previously-deferred messages by seq instead of claiming.
func (c *Client) Receive(ctx context.Context, queue string, opts ...RecvOpts) ([]*Message, error) {
	o := firstOpt(opts)
	var resp wire.ReceiveResponse
	if len(o.Pick) > 0 {
		if err := c.post(ctx, wire.PathReceiveDeferred, wire.ReceiveDeferredRequest{Queue: queue, SeqNumbers: o.Pick}, &resp); err != nil {
			return nil, err
		}
	} else {
		eo := o.toEngine()
		if err := c.post(ctx, wire.PathReceive, wire.ReceiveRequest{
			Queue: queue, MaxMessages: eo.MaxMessages, WaitTimeMs: eo.WaitMs,
			ReceiveMode: int(eo.Mode), AttemptID: eo.AttemptID, // idempotent receive
		}, &resp); err != nil {
			return nil, err
		}
	}
	out := make([]*Message, len(resp.Messages))
	for i, wm := range resp.Messages {
		out[i] = c.wireToMessage(queue, wm)
	}
	return out, nil
}

func (c *Client) receiveOne(ctx context.Context, queue string, max int, waitMs int64, mode engine.ReceiveMode) ([]*Message, error) {
	var resp wire.ReceiveResponse
	if err := c.post(ctx, wire.PathReceive, wire.ReceiveRequest{
		Queue: queue, MaxMessages: max, WaitTimeMs: waitMs, ReceiveMode: int(mode),
	}, &resp); err != nil {
		return nil, err
	}
	out := make([]*Message, len(resp.Messages))
	for i, wm := range resp.Messages {
		out[i] = c.wireToMessage(queue, wm)
	}
	return out, nil
}

// Peek browses without locking.
func (c *Client) Peek(ctx context.Context, queue string, opts ...PeekOpts) ([]*PeekedMessage, error) {
	p := firstOpt(opts).toEngine()
	var resp wire.PeekResponse
	if err := c.post(ctx, wire.PathPeek, wire.PeekRequest{
		Queue: queue, FromSeq: p.FromSeq, State: string(p.State), Max: p.Max,
	}, &resp); err != nil {
		return nil, err
	}
	out := make([]*PeekedMessage, len(resp.Messages))
	for i, wm := range resp.Messages {
		out[i] = wireToPeeked(wm)
	}
	return out, nil
}

// ── admin ───────────────────────────────────────────────────────────────────

// CreateQueue creates or updates a queue.
func (c *Client) CreateQueue(ctx context.Context, name string, cfg QueueConfig) error {
	ec := cfg.toEngine()
	return c.post(ctx, wire.PathCreateQueue, wire.CreateQueueRequest{
		Name: name, Config: wire.QueueConfigJSON{
			LockDurationMs: ec.LockDurationMs, MaxDeliveryCount: ec.MaxDeliveryCount,
			DefaultTTLMs: ec.DefaultTTLMs, DeadLetterOnExpire: ec.DeadLetterOnExpire, DedupWindowMs: ec.DedupWindowMs,
			OrderingMode: string(ec.Ordering),
		},
	}, &wire.Empty{})
}

// Subscribe registers a subscription under a topic with an optional filter.
func (c *Client) Subscribe(ctx context.Context, topic, name string, f *Filter) error {
	return c.post(ctx, wire.PathSubscribe, wire.SubscribeRequest{Topic: topic, Name: name, Filter: f}, &wire.Empty{})
}

// ListQueues lists queues/subscriptions.
func (c *Client) ListQueues(ctx context.Context) ([]QueueInfo, error) {
	var resp wire.ListQueuesResponse
	if err := c.post(ctx, wire.PathListQueues, wire.Empty{}, &resp); err != nil {
		return nil, err
	}
	out := make([]QueueInfo, len(resp.Queues))
	for i, q := range resp.Queues {
		out[i] = QueueInfo{Name: q.Name, Kind: q.Kind, LockDurationMs: q.LockDurationMs,
			MaxDeliveryCount: q.MaxDeliveryCount, DefaultTTLMs: q.DefaultTTLMs, DedupWindowMs: q.DedupWindowMs}
	}
	return out, nil
}

// Stats returns counters for a queue.
func (c *Client) Stats(ctx context.Context, queue string) (Metrics, error) {
	var resp wire.MetricsResponse
	if err := c.post(ctx, wire.PathStats, wire.MetricsRequest{Queue: queue}, &resp); err != nil {
		return Metrics{}, err
	}
	return Metrics{Queue: resp.Queue, Active: resp.Active, Locked: resp.Locked, Deferred: resp.Deferred,
		Scheduled: resp.Scheduled, DeadLettered: resp.DeadLettered, Total: resp.Total,
		OldestMessageAgeMs: resp.OldestMessageAgeMs}, nil
}

// Redrive moves dead-lettered messages back to active.
func (c *Client) Redrive(ctx context.Context, dlqQueue string, opts ...RedriveOpts) (int, error) {
	r := firstOpt(opts).toEngine()
	var resp wire.RedriveResponse
	if err := c.post(ctx, wire.PathRedrive, wire.RedriveRequest{
		Queue: dlqQueue, Target: r.Target, Max: r.Max, OlderThanMs: r.OlderThanMs, RatePerSec: r.RatePerSec,
	}, &resp); err != nil {
		return 0, err
	}
	return resp.Moved, nil
}

// Purge permanently deletes dead-lettered messages (PurgeOpts scopes it; no opts
// purges the whole DLQ). Returns count deleted.
func (c *Client) Purge(ctx context.Context, queue string, opts ...PurgeOpts) (int, error) {
	p := firstOpt(opts).toEngine()
	var resp wire.PurgeResponse
	if err := c.post(ctx, wire.PathPurge, wire.PurgeRequest{
		Queue: queue, Max: p.Max, OlderThanMs: p.OlderThanMs,
	}, &resp); err != nil {
		return 0, err
	}
	return resp.Purged, nil
}

// Receiver returns a stateful receive loop bound to this client.
func (c *Client) Receiver(queue string, opts ...ReceiverOption) *Receiver {
	return newReceiver(c, queue, opts)
}

// ── settler ─────────────────────────────────────────────────────────────────

func (c *Client) settle(ctx context.Context, path string, req wire.SettleRequest) error {
	var r wire.SettleResponse
	if err := c.post(ctx, path, req, &r); err != nil {
		return err
	}
	if !r.Ok {
		return ErrLockLost
	}
	return nil
}

func (c *Client) complete(ctx context.Context, q string, seq int64, tok string) error {
	return c.settle(ctx, wire.PathComplete, wire.SettleRequest{Queue: q, SeqNumber: seq, LockToken: tok})
}
func (c *Client) abandon(ctx context.Context, q string, seq int64, tok string, delayMs int64) error {
	return c.settle(ctx, wire.PathAbandon, wire.SettleRequest{Queue: q, SeqNumber: seq, LockToken: tok, DelayMs: delayMs})
}
func (c *Client) reject(ctx context.Context, q string, seq int64, tok, reason, desc string) error {
	return c.settle(ctx, wire.PathReject, wire.SettleRequest{Queue: q, SeqNumber: seq, LockToken: tok, DeadLetterReason: reason, DeadLetterDescription: desc})
}
func (c *Client) deferMsg(ctx context.Context, q string, seq int64, tok string) error {
	return c.settle(ctx, wire.PathDefer, wire.SettleRequest{Queue: q, SeqNumber: seq, LockToken: tok})
}
func (c *Client) renew(ctx context.Context, q string, seq int64, tok string) error {
	return c.settle(ctx, wire.PathRenew, wire.SettleRequest{Queue: q, SeqNumber: seq, LockToken: tok})
}

// PeekedMessage is a read-only browse result.
type PeekedMessage struct {
	SequenceNumber int64
	State          State
	Body           []byte
	MessageID      string
	GroupID        string
	CorrelationID  string
	ReplyTo        string
	Subject        string
	ContentType    string
	Properties     map[string]string
	DeliveryCount  int
	EnqueuedAt     time.Time
	VisibleAt      time.Time
	LockedUntil    time.Time
}

func wireToPeeked(wm wire.Message) *PeekedMessage {
	return &PeekedMessage{
		SequenceNumber: wm.SeqNumber,
		State:          State(wm.State),
		Body:           wm.Body,
		MessageID:      wm.MessageID,
		GroupID:        wm.GroupID,
		CorrelationID:  wm.CorrelationID,
		ReplyTo:        wm.ReplyTo,
		Subject:        wm.Subject,
		ContentType:    wm.ContentType,
		Properties:     wm.Properties,
		DeliveryCount:  wm.DeliveryCount,
		EnqueuedAt:     msToTime(wm.EnqueuedAtMs),
		VisibleAt:      msToTime(wm.VisibleAtMs),
		LockedUntil:    msToTime(wm.LockedUntilMs),
	}
}
