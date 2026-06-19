package mqlite

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/server"
)

// Embedded runs the queue engine in-process (like goqite), exposing the same
// method set as the remote Client plus same-DB transactional enqueue (Tx) and a
// one-line upgrade to a network broker (Serve). See design §4.5.
type Embedded struct {
	eng *engine.Engine
}

type embeddedConfig struct {
	authToken         string
	disableBackground bool
	now               func() int64
	maxMessageBytes   int64
	logger            *slog.Logger
}

// EmbeddedOption configures OpenEmbedded.
type EmbeddedOption func(*embeddedConfig)

// WithDBAuthToken supplies the auth token for a remote libSQL/Turso DSN.
// The token is passed in by the caller (typically from an env var) — never compiled in.
func WithDBAuthToken(tok string) EmbeddedOption {
	return func(c *embeddedConfig) { c.authToken = tok }
}

// WithoutBackground disables the maintenance loops (tests drive them manually).
func WithoutBackground() EmbeddedOption {
	return func(c *embeddedConfig) { c.disableBackground = true }
}

// WithClock injects a test clock returning epoch ms.
func WithClock(now func() int64) EmbeddedOption {
	return func(c *embeddedConfig) { c.now = now }
}

// WithMaxMessageBytes caps message body size (0 -> 1 MiB default).
func WithMaxMessageBytes(n int64) EmbeddedOption {
	return func(c *embeddedConfig) { c.maxMessageBytes = n }
}

// WithLogger routes background-loop failures to lg (nil -> slog.Default()).
func WithLogger(lg *slog.Logger) EmbeddedOption {
	return func(c *embeddedConfig) { c.logger = lg }
}

// OpenEmbedded opens the engine on a DB DSN: file:./mq.db | :memory: | libsql://host
func OpenEmbedded(ctx context.Context, dbDSN string, opts ...EmbeddedOption) (*Embedded, error) {
	var cfg embeddedConfig
	for _, o := range opts {
		o(&cfg)
	}
	eng, err := engine.Open(ctx, engine.Options{
		DB:                dbDSN,
		AuthToken:         cfg.authToken,
		Now:               cfg.now,
		DisableBackground: cfg.disableBackground,
		MaxMessageBytes:   cfg.maxMessageBytes,
		Logger:            cfg.logger,
	})
	if err != nil {
		return nil, err
	}
	return &Embedded{eng: eng}, nil
}

// Engine exposes the underlying engine (advanced/embedded use).
func (e *Embedded) Engine() *engine.Engine { return e.eng }

// Close stops background loops and closes the DB.
func (e *Embedded) Close() error { return e.eng.Close() }

func (e *Embedded) engineToMessage(queue string, m *engine.Message) *Message {
	return &Message{
		SequenceNumber: m.SeqNumber,
		Body:           m.Body,
		MessageID:      m.MessageID,
		GroupID:        m.GroupID,
		CorrelationID:  m.CorrelationID,
		ReplyTo:        m.ReplyTo,
		Subject:        m.Subject,
		ContentType:    m.ContentType,
		Properties:     m.Properties,
		DeliveryCount:  m.DeliveryCount,
		EnqueuedAt:     msToTime(m.EnqueuedAtMs),
		LockedUntil:    msToTime(m.LockedUntilMs),
		queue:          queue,
		lockToken:      m.LockToken,
		s:              e,
	}
}

// ── send ──────────────────────────────────────────────────────────────────

// SendOne enqueues one message. A dedup conflict surfaces as ErrDedupConflict.
// SendOpts.At schedules delayed delivery.
func (e *Embedded) SendOne(ctx context.Context, queue string, m OutMessage, opts ...SendOpts) (int64, error) {
	o := firstOpt(opts)
	if !o.At.IsZero() {
		return e.eng.Schedule(ctx, queue, m.toEngine(), o.At.UnixMilli())
	}
	return e.eng.SendOne(ctx, queue, m.toEngine())
}

// Send enqueues one or many messages in one transaction.
func (e *Embedded) Send(ctx context.Context, queue string, msgs ...OutMessage) ([]int64, error) {
	outs := make([]engine.OutMessage, len(msgs))
	for i, m := range msgs {
		outs[i] = m.toEngine()
	}
	return e.eng.Send(ctx, queue, outs...)
}

// Cancel deletes a not-yet-activated scheduled message.
func (e *Embedded) Cancel(ctx context.Context, queue string, seq int64) error {
	return e.eng.Cancel(ctx, queue, seq)
}

// Tx runs business writes and enqueues in one transaction (§4.5, embedded-only).
func (e *Embedded) Tx(ctx context.Context, fn func(*engine.EngineTx) error) error {
	return e.eng.Tx(ctx, fn)
}

// ── receive ─────────────────────────────────────────────────────────────────

func (e *Embedded) Receive(ctx context.Context, queue string, opts ...RecvOpts) ([]*Message, error) {
	o := firstOpt(opts)
	var ms []*engine.Message
	var err error
	if len(o.Pick) > 0 {
		ms, err = e.eng.ReceiveDeferred(ctx, queue, o.Pick...)
	} else {
		ms, err = e.eng.Receive(ctx, queue, o.toEngine()) // carries AttemptID for idempotent receive
	}
	if err != nil {
		return nil, err
	}
	out := make([]*Message, len(ms))
	for i, m := range ms {
		out[i] = e.engineToMessage(queue, m)
	}
	return out, nil
}

func (e *Embedded) receiveOne(ctx context.Context, queue string, max int, waitMs int64, mode engine.ReceiveMode, attemptID string) ([]*Message, error) {
	ms, err := e.eng.Receive(ctx, queue, engine.ReceiveOptions{MaxMessages: max, WaitMs: waitMs, Mode: mode, AttemptID: attemptID})
	if err != nil {
		return nil, err
	}
	out := make([]*Message, len(ms))
	for i, m := range ms {
		out[i] = e.engineToMessage(queue, m)
	}
	return out, nil
}

func (e *Embedded) Peek(ctx context.Context, queue string, opts ...PeekOpts) ([]*PeekedMessage, error) {
	ms, err := e.eng.Peek(ctx, queue, firstOpt(opts).toEngine())
	if err != nil {
		return nil, err
	}
	out := make([]*PeekedMessage, len(ms))
	for i, p := range ms {
		out[i] = &PeekedMessage{
			SequenceNumber: p.SeqNumber, State: p.State, Body: p.Body, MessageID: p.MessageID,
			GroupID: p.GroupID, CorrelationID: p.CorrelationID, ReplyTo: p.ReplyTo, Subject: p.Subject, ContentType: p.ContentType,
			Properties: p.Properties, DeliveryCount: p.DeliveryCount,
			EnqueuedAt: msToTime(p.EnqueuedAtMs), VisibleAt: msToTime(p.VisibleAtMs), LockedUntil: msToTime(p.LockedUntilMs),
		}
	}
	return out, nil
}

// ── admin ───────────────────────────────────────────────────────────────────

func (e *Embedded) CreateQueue(ctx context.Context, name string, cfg QueueConfig) error {
	return e.eng.CreateQueue(ctx, name, cfg.toEngine())
}

func (e *Embedded) Subscribe(ctx context.Context, topic, name string, f *Filter) error {
	return e.eng.Subscribe(ctx, topic, name, f)
}

func (e *Embedded) ListQueues(ctx context.Context) ([]QueueInfo, error) {
	return e.eng.ListQueues(ctx)
}

func (e *Embedded) Stats(ctx context.Context, queue string) (Metrics, error) {
	return e.eng.Stats(ctx, queue)
}

func (e *Embedded) Redrive(ctx context.Context, dlqQueue string, opts ...RedriveOpts) (int, error) {
	return e.eng.Redrive(ctx, dlqQueue, firstOpt(opts).toEngine())
}

// Purge permanently deletes dead-lettered messages (PurgeOpts scopes it; no opts
// purges the whole DLQ). Returns count deleted.
func (e *Embedded) Purge(ctx context.Context, queue string, opts ...PurgeOpts) (int, error) {
	return e.eng.Purge(ctx, queue, firstOpt(opts).toEngine())
}

// Receiver returns a stateful receive loop bound to the embedded engine.
func (e *Embedded) Receiver(queue string, opts ...ReceiverOption) *Receiver {
	return newReceiver(e, queue, opts)
}

// ── serve: upgrade in-process engine to a network broker ─────────────────────

type serveConfig struct{ tokens []string }

// ServeOption configures Serve.
type ServeOption func(*serveConfig)

// WithTokens sets the accepted Bearer tokens for the broker.
func WithTokens(tokens ...string) ServeOption {
	return func(c *serveConfig) { c.tokens = append(c.tokens, tokens...) }
}

// WithTokenCSV sets accepted Bearer tokens from a comma-separated string (env-friendly).
func WithTokenCSV(csv string) ServeOption {
	return func(c *serveConfig) {
		for _, t := range strings.Split(csv, ",") {
			if t = strings.TrimSpace(t); t != "" {
				c.tokens = append(c.tokens, t)
			}
		}
	}
}

// Serve exposes this engine as an HTTP broker until ctx is canceled.
func (e *Embedded) Serve(ctx context.Context, addr string, opts ...ServeOption) error {
	var sc serveConfig
	for _, o := range opts {
		o(&sc)
	}
	srv := server.New(e.eng, sc.tokens)
	hs := &http.Server{Addr: addr, Handler: srv.Handler()}
	errCh := make(chan error, 1)
	go func() { errCh <- hs.ListenAndServe() }()
	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = hs.Shutdown(shutCtx)
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// ── settler ─────────────────────────────────────────────────────────────────

func (e *Embedded) complete(ctx context.Context, q string, seq int64, tok string) error {
	return e.eng.Complete(ctx, q, seq, tok)
}
func (e *Embedded) abandon(ctx context.Context, q string, seq int64, tok string, delayMs int64) error {
	return e.eng.Abandon(ctx, q, seq, tok, delayMs)
}
func (e *Embedded) reject(ctx context.Context, q string, seq int64, tok, reason, desc string) error {
	return e.eng.Reject(ctx, q, seq, tok, reason, desc)
}
func (e *Embedded) deferMsg(ctx context.Context, q string, seq int64, tok string) error {
	return e.eng.Defer(ctx, q, seq, tok)
}
func (e *Embedded) renew(ctx context.Context, q string, seq int64, tok string) error {
	return e.eng.Renew(ctx, q, seq, tok)
}
