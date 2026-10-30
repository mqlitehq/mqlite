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
	"github.com/mqlitehq/mqlite/internal/receivegate"
	"github.com/mqlitehq/mqlite/internal/version"
	"github.com/mqlitehq/mqlite/server"
	"github.com/mqlitehq/mqlite/wire"
)

// Embedded runs the queue engine in-process (like goqite), exposing the same
// method set as the remote Client plus same-DB transactional enqueue (Tx). When
// opened with WithBrokerMode, it can expose the same engine as a network broker
// through Serve. See design §4.5.
type Embedded struct {
	eng        *engine.Engine
	brokerMode bool
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
	maxSubscriptions  int
	brokerMode        bool
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

// WithMaxSubscriptions caps the total active and deleting subscriptions.
// Zero uses the engine default.
func WithMaxSubscriptions(n int) EmbeddedOption {
	return func(c *embeddedConfig) { c.maxSubscriptions = n }
}

// WithBrokerMode configures OpenEmbedded for a network broker whose remote
// consumers can outlive this process. On reopen, unexpired Peek-Lock leases on
// active resources and their fencing tokens remain valid; only expired locks are
// recovered. Resource lifecycle and message TTL still apply. Pass this option
// whenever the returned engine will be served with Serve. Plain embedded mode
// intentionally keeps immediate orphan recovery because its handlers die with
// the engine.
func WithBrokerMode() EmbeddedOption {
	return func(c *embeddedConfig) { c.brokerMode = true }
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
		DB:                      dbDSN,
		AuthToken:               cfg.authToken,
		Now:                     cfg.now,
		DisableBackground:       cfg.disableBackground,
		PreserveUnexpiredLeases: cfg.brokerMode,
		MaxMessageBytes:         cfg.maxMessageBytes,
		MaxSubscriptions:        cfg.maxSubscriptions,
		Logger:                  cfg.logger,
		Synchronous:             cfg.synchronous,
		DLQMaxAgeMs:             cfg.dlqMaxAgeMs,
		DLQMaxCount:             cfg.dlqMaxCount,
		DLQMaxBytes:             cfg.dlqMaxBytes,
	})
	if err != nil {
		return nil, err
	}
	return &Embedded{eng: eng, brokerMode: cfg.brokerMode}, nil
}

// Engine exposes the underlying engine (advanced/embedded use).
func (e *Embedded) Engine() *engine.Engine { return e.eng }

// Close shuts the engine down safely: it stops the background loops, closes the admission
// gate (any DB operation that starts after shutdown begins fails fast with ErrClosed), wakes
// empty long-poll Receives with ErrClosed, waits for already-admitted writes and
// transactions to finish, and only then closes the DB and releases the file lock. Returns
// the teardown error, if any.
//
// Because it waits for in-flight work, Close MUST NOT be called from inside an operation it
// would wait for — in particular, not from within a Tx callback (see Tx). Doing so deadlocks.
func (e *Embedded) Close() error { return e.eng.Close() }

// Compact reclaims free DB pages to the OS: `PRAGMA incremental_vacuum` (no global
// lock; new local DBs default to auto_vacuum=INCREMENTAL), or a full `VACUUM` when
// full=true (rewrites the file, global lock — maintenance only). Local-only.
func (e *Embedded) Compact(ctx context.Context, full bool) error { return e.eng.Compact(ctx, full) }

func embeddedLeaseDeadline(deadlineMs, serverNowMs int64) time.Time {
	if deadlineMs <= 0 {
		return time.Time{}
	}
	// Engine deadlines live in the injectable engine clock domain. Translate the
	// remaining duration onto time.Now so Receiver scheduling keeps a monotonic
	// horizon even when WithClock uses a test epoch far from the host wall clock.
	remaining := time.UnixMilli(deadlineMs).Sub(time.UnixMilli(serverNowMs))
	return time.Now().Add(remaining)
}

func (e *Embedded) engineToMessage(queue string, m *engine.Message, serverNowMs int64) *Message {
	return &Message{
		SequenceNumber:   m.SeqNumber,
		Body:             m.Body,
		MessageID:        m.MessageID,
		GroupID:          m.GroupID,
		CorrelationID:    m.CorrelationID,
		ReplyTo:          m.ReplyTo,
		Subject:          m.Subject,
		ContentType:      m.ContentType,
		Properties:       m.Properties,
		DeliveryCount:    m.DeliveryCount,
		EnqueuedAt:       msToTime(m.EnqueuedAtMs),
		LockedUntil:      msToTime(m.LockedUntilMs),
		localLockedUntil: embeddedLeaseDeadline(m.LockedUntilMs, serverNowMs),
		queue:            queue,
		lockToken:        m.LockToken,
		s:                e,
	}
}

// ── send ──────────────────────────────────────────────────────────────────

// SendOne enqueues one message. A dedup conflict surfaces as ErrDedupConflict.
// SendOpts.At schedules delayed delivery.
func (e *Embedded) SendOne(ctx context.Context, queue string, m OutMessage, opts ...SendOpts) (int64, error) {
	if err := validateOutMessageTTL(m); err != nil {
		return 0, err
	}
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
		if err := validateOutMessageTTL(m); err != nil {
			return nil, fmt.Errorf("message %d: %w", i, err)
		}
		outs[i] = m.toEngine()
	}
	return e.eng.Send(ctx, queue, outs...)
}

// Cancel deletes a never-delivered scheduled Send. It cannot cancel a retry
// parked in scheduled by delayed Abandon.
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

// RenewBatch extends the lock lease of many messages in ONE STATEMENT — deliberately not a
// transaction (the embedded twin of Client.RenewBatch), returning a per-message result.
//
// SettleResult.LockedUntil is the absolute deadline actually committed. Ok says the lease was live
// when the STATEMENT finished, which is already in the past by the time you read it: nothing is
// encoded or sent in embedded mode, but your own handling still takes time and a very short lease
// can lapse inside it. Comparing LockedUntil with another clock assumes those clocks are
// synchronized; the managed Receiver has a conservative fallback for an unusable comparison.
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
		out[i] = SettleResult{SequenceNumber: r.SeqNumber, Ok: r.Ok, LockedUntil: msFromEpoch(r.LockedUntilMs)}
	}
	return out, nil
}

// Tx runs business writes and enqueues in one transaction (§4.5, embedded-only): business success
// ⇔ message enqueued, with no dual-write window.
//
// On a local store, use tx.Context() for raw tx.SQL() statements and check the original ctx between
// them. Raw SQL bypasses mqlite's per-statement cancellation guards and can still execute after ctx
// is cancelled. It is supported for caller-owned tables, not direct changes to mqlite's internal
// tables. The callback must return before the writer is released; cancellation observed before commit
// rolls back the whole transaction, even if the callback returns nil. An already-cancelled caller
// never enters the callback. See engine.EngineTx.SQL and engine.EngineTx.Context.
//
// ON A REMOTE (Turso/libSQL) STORE, fn MAY RUN MORE THAN ONCE. A transaction that fails on a
// retryable connection or busy error is replayed from the start — the database work rolls back, so
// the DATA stays correct, but anything else fn did will have happened twice. Keep fn to
// transaction-bound work: SQL through the provided handle, and nothing else. A charge, an email, a
// counter in memory, an HTTP call — none of those roll back with the transaction, and a replay will
// repeat them (round-4 §5.2). Local file and :memory: stores never retry, so fn runs at most once
// there.
//
// fn MUST NOT call Close: Close waits for the open transaction to finish before tearing the engine
// down, and fn cannot finish until it returns, so it would deadlock. Close from outside the callback.
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
	if len(ms) == 0 {
		return out, nil
	}
	serverNowMs := e.eng.ServerTimeMs()
	for i, m := range ms {
		out[i] = e.engineToMessage(queue, m, serverNowMs)
	}
	return out, nil
}

func (e *Embedded) receiveOne(ctx context.Context, queue string, max int, waitMs int64, mode engine.ReceiveMode, attemptID string) ([]*Message, error) {
	ms, err := e.eng.Receive(ctx, queue, engine.ReceiveOptions{MaxMessages: max, WaitMs: waitMs, Mode: mode, AttemptID: attemptID})
	if err != nil {
		return nil, err
	}
	out := make([]*Message, len(ms))
	if len(ms) == 0 {
		return out, nil
	}
	serverNowMs := e.eng.ServerTimeMs()
	for i, m := range ms {
		out[i] = e.engineToMessage(queue, m, serverNowMs)
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

// DeleteQueue removes an empty plain queue. An accepted deletion returns nil
// whether bounded physical cleanup completed in this call or continues in the
// background. Use DeleteQueueWithOptions to force-discard retained messages or
// inspect the accepted deletion state.
func (e *Embedded) DeleteQueue(ctx context.Context, name string) error {
	_, err := e.DeleteQueueWithOptions(ctx, name)
	return err
}

// DeleteQueueWithOptions reports whether physical cleanup is complete.
func (e *Embedded) DeleteQueueWithOptions(ctx context.Context, name string, opts ...DeleteQueueOpts) (DeleteResult, error) {
	o := firstOpt(opts)
	return e.eng.DeleteQueueWithOptions(ctx, name, engine.DeleteOptions{Force: o.Force})
}

// DeleteSubscription removes an empty subscription. An accepted deletion returns
// nil whether bounded physical cleanup completed in this call or continues in the
// background. Use DeleteSubscriptionWithOptions to force-discard retained messages
// or inspect the accepted deletion state.
func (e *Embedded) DeleteSubscription(ctx context.Context, topic, name string) error {
	_, err := e.DeleteSubscriptionWithOptions(ctx, topic, name)
	return err
}

// DeleteSubscriptionWithOptions reports whether physical cleanup is complete.
func (e *Embedded) DeleteSubscriptionWithOptions(ctx context.Context, topic, name string, opts ...DeleteSubscriptionOpts) (DeleteResult, error) {
	o := firstOpt(opts)
	return e.eng.DeleteSubscriptionWithOptions(ctx, topic, name, engine.DeleteOptions{Force: o.Force})
}

func (e *Embedded) CreateQueue(ctx context.Context, name string, cfg QueueConfig) error {
	ec, err := cfg.toEngine()
	if err != nil {
		return err
	}
	return e.eng.CreateQueue(ctx, name, ec)
}

func (e *Embedded) Subscribe(ctx context.Context, topic, name string, f *Filter) error {
	return e.eng.Subscribe(ctx, topic, name, f)
}

// SubscribeWithOptions creates or updates a subscription and optionally assigns
// a fixed lifetime on first creation. Supplying zero TTL preserves the lifetime
// of an existing subscription.
func (e *Embedded) SubscribeWithOptions(ctx context.Context, topic, name string, f *Filter, opts SubscribeOptions) error {
	o, err := opts.toEngine()
	if err != nil {
		return err
	}
	return e.eng.SubscribeWithOptions(ctx, topic, name, f, o)
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
	s, queues, subscriptions := e.eng.RuntimeStatus(ctx)
	info := StatusInfo{
		Backend: s.Backend, Remote: s.Remote, Location: s.Location,
		SchemaVersion: s.SchemaVersion, PingMs: s.PingMs, DBSizeBytes: s.SizeBytes,
		Queues: queues, Subscriptions: subscriptions, ServerTimeMs: s.ServerTimeMs,
	}

	return info, nil
}

// Observe samples the same canonical observation as the broker. HTTP measurements
// are explicitly not applicable to an in-process engine.
func (e *Embedded) Observe(ctx context.Context) (Observation, error) {
	return Observation{
		ObservabilitySnapshot: e.eng.Observability(ctx), Access: "embedded", Version: version.Version,
		HTTP: wire.HTTPObservation{State: "not_applicable", Requests: []wire.RequestObservation{}, Authentication: []wire.AuthenticationObservation{}, HandlerLatency: []wire.RPCLatencyObservation{}},
	}, nil
}

// ListSubscriptions returns every active, unexpired subscription with its topic and
// filter expression.
//
// Compatibility note: use ListSubscriptionsWithOptions for bounded inventory and
// lifecycle reconciliation, including deleting name reservations.
func (e *Embedded) ListSubscriptions(ctx context.Context) ([]SubscriptionInfo, error) {
	return e.eng.ListSubscriptions(ctx)
}

// ListSubscriptionsWithOptions returns one internally consistent subscription
// page. Separate page requests may observe concurrent roster changes.
func (e *Embedded) ListSubscriptionsWithOptions(ctx context.Context, opts ListSubscriptionsOptions) (SubscriptionPage, error) {
	return e.eng.ListSubscriptionsWithOptions(ctx, opts)
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
	tokens        []string
	monitorTokens []string
	metrics       bool
	version       string
	cors          string
	reqLogger     *slog.Logger
	ui            bool
	ready         func()
	// receiveGate is injected only by white-box shutdown tests. Production Serve
	// creates a fresh gate for each invocation.
	receiveGate *receivegate.Gate
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

// WithMonitorTokens adds configured read-only credentials for Observe and /metrics.
// Serve rejects blank credentials, overlap with administrators, or disabled auth.
// These credentials can authenticate during a storage outage without a DB lookup.
func WithMonitorTokens(tokens ...string) ServeOption {
	return func(c *serveConfig) { c.monitorTokens = append(c.monitorTokens, tokens...) }
}

// WithMetrics enables authenticated GET /metrics on the broker's API listener.
// Off by default. Enabling it requires administrator authentication and adds
// the /metrics path to discovery. Scrapers can use a read-only monitor token.
func WithMetrics(on bool) ServeOption {
	return func(c *serveConfig) { c.metrics = on }
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
// line per RPC: method, status, request ID, safe actor identity, latency, and operation
// details at a level chosen by status). nil leaves request logging off. Callers that need
// durable audit evidence must retain this logger externally. Separate from WithLogger
// (which routes the engine's background-loop failures) so a caller can colour the broker's
// live traffic without touching engine logs.
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

// Serve exposes this engine as an HTTP broker until ctx is canceled. The engine
// must have been opened with WithBrokerMode so a restart cannot revoke leases
// still owned by remote consumers.
func (e *Embedded) Serve(ctx context.Context, addr string, opts ...ServeOption) error {
	if !e.brokerMode {
		return errors.New("mqlite: Serve requires OpenEmbedded with WithBrokerMode")
	}
	var sc serveConfig
	for _, o := range opts {
		o(&sc)
	}
	if err := server.ValidateMonitorTokens(sc.tokens, sc.monitorTokens); err != nil {
		return err
	}
	if sc.metrics {
		hasAdmin := false
		for _, token := range sc.tokens {
			hasAdmin = hasAdmin || strings.TrimSpace(token) != ""
		}
		if !hasAdmin {
			return errors.New("metrics require administrator authentication")
		}
	}

	srv := server.New(e.eng, sc.tokens)
	srv.MonitorTokens = append([]string(nil), sc.monitorTokens...)
	srv.Version = sc.version
	srv.CORS = sc.cors
	srv.Logger = sc.reqLogger
	srv.UI = sc.ui
	srv.Metrics = sc.metrics
	// http.Server.Shutdown waits for active handlers but deliberately does not wake
	// long polls. The gate interrupts only the wait between claim rounds. Stop and
	// TryEnterClaim share an atomic boundary, so a claim admitted before shutdown
	// may return its real result while no claim can start after the barrier.
	receiveGate := sc.receiveGate
	if receiveGate == nil {
		receiveGate = receivegate.New()
	}
	hs := newHTTPServer(addr, receiveDrainHandler(srv.Handler(), receiveGate))

	// Bind synchronously so a bind failure (port in use, bad addr) surfaces here —
	// before any "ready" is announced — instead of after the caller already logged it.
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	defer ln.Close()
	defer hs.Close()
	if sc.ready != nil {
		sc.ready()
	}
	errCh := make(chan error, 1)
	go func() { errCh <- hs.Serve(ln) }()
	select {
	case <-ctx.Done():
		// Grace exceeds the longest Receive long poll (20s).
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	// Wake parked Receive handlers before waiting for the remaining admitted HTTP
	// work. An atomically admitted claim keeps the caller's request context and may
	// finish; unrelated handlers retain their contexts.
	receiveGate.Stop()
	shutCtx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	return hs.Shutdown(shutCtx)
}

// receiveDrainHandler attaches the internal claim gate only to Receive. It never
// cancels the HTTP request context, so an active claim/commit keeps its real
// outcome and every non-Receive RPC is untouched.
func receiveDrainHandler(next http.Handler, gate *receivegate.Gate) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == wire.PathReceive {
			r = r.WithContext(receivegate.WithContext(r.Context(), gate))
		}
		next.ServeHTTP(w, r)
	})
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

// renewDeadline is the managed Receiver's deadline-bearing renewal path. Keep
// Message.Renew's public error-only signature stable while pacing later
// heartbeats from the deadline the engine actually committed.
func (e *Embedded) renewDeadline(ctx context.Context, q string, seq int64, tok string) (renewalLease, error) {
	results, err := e.eng.RenewBatch(ctx, q, []engine.SettleItem{{SeqNumber: seq, LockToken: tok}})
	if err != nil {
		return renewalLease{}, err
	}
	if len(results) != 1 || results[0].SeqNumber != seq {
		return renewalLease{}, fmt.Errorf("mqlite: invalid embedded RenewBatch result")
	}
	result := results[0]
	if !result.Ok {
		if result.LockedUntilMs != 0 {
			return renewalLease{}, fmt.Errorf("mqlite: invalid embedded RenewBatch lost-lock deadline")
		}
		return renewalLease{}, ErrLockLost
	}
	if result.LockedUntilMs <= 0 {
		return renewalLease{}, fmt.Errorf("mqlite: invalid embedded RenewBatch deadline")
	}
	return renewalLease{deadline: embeddedLeaseDeadline(result.LockedUntilMs, e.eng.ServerTimeMs())}, nil
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
