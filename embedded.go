package mqlite

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
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
	synchronous       string
	dlqMaxAgeMs       int64
	dlqMaxCount       int
	dlqMaxBytes       int64
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

// WithSynchronous sets the local SQLite PRAGMA synchronous level — the durability
// vs throughput knob (MQLITE-7). "NORMAL" (default) is fast and durable across a
// process crash, but a sudden OS/power loss can drop the last few not-yet-
// checkpointed commits; "FULL" fsyncs every commit (safer, slower). Database-wide
// (SQLite has no per-queue sync); ignored for remote Turso DSNs.
func WithSynchronous(mode string) EmbeddedOption {
	return func(c *embeddedConfig) { c.synchronous = mode }
}

// WithDLQRetention bounds the dead-letter queue so a long-running broker can't grow
// without limit (MQLITE-21). A background pass drops dead letters oldest-first when
// they are older than maxAge, beyond maxCount per queue, or over maxBytes of body
// per queue; pass 0 to leave a dimension unbounded. ONLY the DLQ is affected —
// undelivered and in-flight messages are never deleted. Off by default for the
// embedded library; the `mqlite serve` broker enables a sane default (see cmd/mqlite).
func WithDLQRetention(maxAge time.Duration, maxCount int, maxBytes int64) EmbeddedOption {
	return func(c *embeddedConfig) {
		if maxAge > 0 {
			c.dlqMaxAgeMs = maxAge.Milliseconds()
		}
		c.dlqMaxCount = maxCount
		c.dlqMaxBytes = maxBytes
	}
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
		Synchronous:       cfg.synchronous,
		DLQMaxAgeMs:       cfg.dlqMaxAgeMs,
		DLQMaxCount:       cfg.dlqMaxCount,
		DLQMaxBytes:       cfg.dlqMaxBytes,
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

// Compact reclaims free DB pages to the OS: `PRAGMA incremental_vacuum` (no global
// lock; new local DBs default to auto_vacuum=INCREMENTAL), or a full `VACUUM` when
// full=true (rewrites the file, global lock — maintenance only). Local-only.
func (e *Embedded) Compact(ctx context.Context, full bool) error { return e.eng.Compact(ctx, full) }

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

// CompleteBatch completes many received messages in one transaction (mirrors the
// remote Client method; for the in-process engine there is no round-trip to save,
// but it keeps the API symmetric and settles atomically).
// Message rehydrates a settleable handle for a message you already received but did not
// settle (the embedded twin of Client.Message). Pass the queue, its SequenceNumber, and
// the LockToken() from that receive; the returned *Message settles through this engine.
func (e *Embedded) Message(queue string, seq int64, lockToken string) *Message {
	return &Message{queue: queue, SequenceNumber: seq, lockToken: lockToken, s: e}
}

// CompleteBatch completes many messages in one transaction, returning a per-message result.
func (e *Embedded) CompleteBatch(ctx context.Context, queue string, msgs ...*Message) ([]SettleResult, error) {
	items := make([]engine.SettleItem, len(msgs))
	for i, m := range msgs {
		items[i] = engine.SettleItem{SeqNumber: m.SequenceNumber, LockToken: m.lockToken}
	}
	res, err := e.eng.CompleteBatch(ctx, queue, items)
	if err != nil {
		return nil, err
	}
	out := make([]SettleResult, len(res))
	for i, r := range res {
		out[i] = SettleResult{SequenceNumber: r.SeqNumber, Ok: r.Ok}
	}
	return out, nil
}

// RenewBatch extends the lock lease of many messages in one transaction (the embedded twin of
// Client.RenewBatch), returning a per-message result.
func (e *Embedded) RenewBatch(ctx context.Context, queue string, msgs ...*Message) ([]SettleResult, error) {
	items := make([]engine.SettleItem, len(msgs))
	for i, m := range msgs {
		items[i] = engine.SettleItem{SeqNumber: m.SequenceNumber, LockToken: m.lockToken}
	}
	res, err := e.eng.RenewBatch(ctx, queue, items)
	if err != nil {
		return nil, err
	}
	out := make([]SettleResult, len(res))
	for i, r := range res {
		out[i] = SettleResult{SequenceNumber: r.SeqNumber, Ok: r.Ok}
	}
	return out, nil
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
			EnqueuedAt: msToTime(p.EnqueuedAtMs), VisibleAt: msToTime(p.VisibleAtMs),
			ExpiresAt: msToTime(p.ExpiresAtMs), LockedUntil: msToTime(p.LockedUntilMs),
			DeadLetterReason: p.DeadLetterReason, DeadLetterDescription: p.DeadLetterDescription,
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

// Status returns a desensitized snapshot of the local backend (the embedded twin of
// Client.Status). Version is empty and UptimeMs/Auth are zero-valued — an in-process engine
// has no broker build stamp, no process uptime and no auth; queue/subscription counts are live.
func (e *Embedded) Status(ctx context.Context) (StatusInfo, error) {
	s := e.eng.Status(ctx)
	info := StatusInfo{
		Backend: s.Backend, Remote: s.Remote, Location: s.Location,
		SchemaVersion: s.SchemaVersion, PingMs: s.PingMs, DBSizeBytes: s.SizeBytes,
	}
	if qs, err := e.eng.ListQueues(ctx); err == nil {
		// Exclude subscription backing queues from the queue count so embedded status
		// matches what the broker's /status reports (queues vs subscriptions are disjoint).
		for _, q := range qs {
			if q.Kind != "subscription" {
				info.Queues++
			}
		}
	}
	if ss, err := e.eng.ListSubscriptions(ctx); err == nil {
		info.Subscriptions = len(ss)
	}
	return info, nil
}

// ListSubscriptions returns every subscription with its topic and filter expression.
func (e *Embedded) ListSubscriptions(ctx context.Context) ([]SubscriptionInfo, error) {
	return e.eng.ListSubscriptions(ctx)
}

// TestFilter dry-runs a subscription filter expression against an optional sample message
// (nothing is enqueued) — the embedded twin of Client.TestFilter.
func (e *Embedded) TestFilter(ctx context.Context, expr string, sample *OutMessage, enqueuedAtMs, visibleAtMs int64) (FilterTestResult, error) {
	var es *engine.OutMessage
	if sample != nil {
		s := sample.toEngine()
		es = &s
	}
	// Match the broker's /TestFilter defaulting so time-based expressions agree in both
	// modes: a zero enqueued_at means "now"; a zero visible_at means "= enqueued_at".
	now := time.Now().UnixMilli()
	if enqueuedAtMs == 0 {
		enqueuedAtMs = now
	}
	if visibleAtMs == 0 {
		visibleAtMs = enqueuedAtMs
	}
	return engine.TestFilter(expr, es, enqueuedAtMs, visibleAtMs), nil
}

// Receiver returns a stateful receive loop bound to the embedded engine.
func (e *Embedded) Receiver(queue string, opts ...ReceiverOption) *Receiver {
	return newReceiver(e, queue, opts)
}

// ── serve: upgrade in-process engine to a network broker ─────────────────────

type serveConfig struct {
	tokens    []string
	version   string
	cors      string
	reqLogger *slog.Logger
	ui        bool
	ready     func()
}

// ServeOption configures Serve.
type ServeOption func(*serveConfig)

// WithVersion sets the version string the broker reports on its open "/" discovery
// endpoint (typically the CLI/build version).
func WithVersion(v string) ServeOption {
	return func(c *serveConfig) { c.version = v }
}

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

// WithCORS sets the Access-Control-Allow-Origin the broker sends, enabling browser apps
// served from another origin (the standalone console) to call it. "" (the default) leaves
// CORS off; "*" allows any origin — safe here because every RPC needs a Bearer token and
// the API uses no cookies, so a cross-origin page gains nothing without the token.
func WithCORS(origin string) ServeOption {
	return func(c *serveConfig) { c.cors = origin }
}

// WithRequestLog enables a per-request access log on the broker, emitted through lg (one
// line per RPC: method, status, latency, at a level chosen by status). nil leaves request
// logging off. Separate from WithLogger (which routes the engine's background-loop
// failures) so a caller can colour the broker's live traffic without touching engine logs.
func WithRequestLog(lg *slog.Logger) ServeOption {
	return func(c *serveConfig) { c.reqLogger = lg }
}

// WithUI serves the embedded admin console (the built mqlite-web SPA) at /ui. Off by
// default for the embedded library; the `mqlite serve` binary turns it on (MQLITE_UI).
func WithUI(on bool) ServeOption {
	return func(c *serveConfig) { c.ui = on }
}

// WithReady registers a callback Serve invokes once the listener is bound, before it
// starts accepting — so a caller announces "ready" only after the bind actually
// succeeded, never before a bind that then fails (MQLITE-88).
func WithReady(fn func()) ServeOption {
	return func(c *serveConfig) { c.ready = fn }
}

// Serve exposes this engine as an HTTP broker until ctx is canceled.
func (e *Embedded) Serve(ctx context.Context, addr string, opts ...ServeOption) error {
	var sc serveConfig
	for _, o := range opts {
		o(&sc)
	}
	srv := server.New(e.eng, sc.tokens)
	srv.Version = sc.version
	srv.CORS = sc.cors
	srv.Logger = sc.reqLogger
	srv.UI = sc.ui
	hs := newHTTPServer(addr, srv.Handler())

	// Bind synchronously so a bind failure (port in use, bad addr) surfaces here —
	// before any "ready" is announced — instead of after the caller already logged it.
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	if sc.ready != nil {
		sc.ready()
	}
	errCh := make(chan error, 1)
	go func() { errCh <- hs.Serve(ln) }()
	select {
	case <-ctx.Done():
		// Grace must exceed the longest in-flight request — a Receive long-poll runs up
		// to 20s — so a clean Ctrl-C drains it instead of cutting it at 5s (MQLITE-88).
		shutCtx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()
		// Return the shutdown result: a DeadlineExceeded means connections didn't drain
		// in time — a real signal, not something to swallow.
		return hs.Shutdown(shutCtx)
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

// newHTTPServer builds the broker's http.Server with hardening defaults (review F9,
// MQLITE-88): ReadHeaderTimeout bounds header dribble (the Slowloris vector) and
// ReadTimeout bounds the whole request read (a slow-drip body), while IdleTimeout
// reclaims dead keep-alives. ReadTimeout only covers reading the request — the long-poll
// wait and the response happen after — so 60s bounds slow uploads (a legal body is
// <= MaxBodyBytes, 32 MiB) without cutting a Receive long-poll. WriteTimeout stays zero
// ON PURPOSE: it would cap the response and break the 20s long-poll.
func newHTTPServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
}
