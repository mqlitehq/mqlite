package mqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/wire"
)

const (
	defaultMaxIdleConnsPerHost = 64
	maxClientResponseBytes     = 64 << 20
	maxClientErrorBytes        = 64 << 10
)

type invalidSuccessResponseError struct{ cause error }

func (e *invalidSuccessResponseError) Error() string {
	return "mqlite: invalid successful response: " + e.cause.Error()
}

func (e *invalidSuccessResponseError) Unwrap() error { return e.cause }

type ambiguousAcknowledgementError struct {
	operation string
	cause     error
}

func (e *ambiguousAcknowledgementError) Error() string {
	return fmt.Sprintf("%s: invalid %s acknowledgement: %v", ErrOutcomeUnknown, e.operation, e.cause)
}

func (e *ambiguousAcknowledgementError) Unwrap() error { return e.cause }

func (e *ambiguousAcknowledgementError) Is(target error) bool { return target == ErrOutcomeUnknown }

// HTTPError reports the HTTP status returned by a broker or intermediary.
// Structured broker errors remain available through errors.Is on the same value.
type HTTPError struct {
	StatusCode int
	cause      error
}

func (e *HTTPError) Error() string {
	if e == nil {
		return "mqlite: HTTP request failed"
	}
	if e.cause != nil {
		return fmt.Sprintf("%v (HTTP %d)", e.cause, e.StatusCode)
	}
	return fmt.Sprintf("mqlite: request failed: HTTP %d", e.StatusCode)
}

func (e *HTTPError) Unwrap() error { return e.cause }

// HTTPStatus returns the response status code.
func (e *HTTPError) HTTPStatus() int { return e.StatusCode }

// Client is a remote mqlite client (Connect-style JSON over HTTP).
type Client struct {
	endpoint  string
	token     string
	http      *http.Client
	ownedHTTP bool
}

type serverTimeObservation struct {
	localAt     time.Time
	serverUpper time.Time
}

var _ deadlineRenewer = (*Client)(nil)

type config struct {
	token      string
	httpClient *http.Client
}

// Option configures Open.
type Option func(*config)

// WithToken overrides the token from the connection string.
func WithToken(tok string) Option { return func(c *config) { c.token = tok } }

// WithHTTPClient supplies a caller-owned *http.Client (e.g. for TLS config).
// MQLite preserves its transport, timeout, and jar but disables redirects for
// every RPC. Client.Close does not close a caller-owned client's idle connections.
// Size a custom Transport's MaxIdleConnsPerHost to at least Receiver concurrency
// plus effective prefetch so overlapped settlement does not churn connections.
func WithHTTPClient(h *http.Client) Option { return func(c *config) { c.httpClient = h } }

// Open builds a client from a connection string: mqlite://<token>@host:port?tls=true.
// When http.DefaultTransport is a custom RoundTripper, Open preserves it for
// compatibility with process-wide instrumentation and mocks. That transport
// and its connection pool remain caller-owned; use WithHTTPClient when explicit
// per-client transport ownership or pool sizing is required.
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
	base := cfg.httpClient
	ownedHTTP := false
	if base == nil {
		defaultTransport, ok := http.DefaultTransport.(*http.Transport)
		if ok {
			transport := defaultTransport.Clone()
			transport.MaxIdleConnsPerHost = defaultMaxIdleConnsPerHost
			if transport.MaxIdleConns < defaultMaxIdleConnsPerHost {
				transport.MaxIdleConns = defaultMaxIdleConnsPerHost
			}
			// No global timeout: long-poll Receive relies on the request context.
			base = &http.Client{Transport: transport}
			ownedHTTP = true
		} else {
			// Preserve the pre-existing net/http behavior for applications that
			// replace the global transport (for example, telemetry or HTTP mocks).
			// A generic RoundTripper cannot be cloned or have its pool tuned safely.
			base = &http.Client{Transport: http.DefaultTransport}
		}
	}
	// Do not mutate a caller-owned client. The shallow copy preserves its
	// transport, timeout and jar while enforcing the SDK's redirect boundary.
	hc := *base
	hc.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &Client{endpoint: endpoint, token: token, http: &hc, ownedHTTP: ownedHTTP}, nil
}

// Close releases idle connections owned by this client. A client supplied with
// WithHTTPClient remains caller-owned and is not closed.
func (c *Client) Close() error {
	if c != nil && c.ownedHTTP && c.http != nil {
		c.http.CloseIdleConnections()
	}
	return nil
}

func isLifecycleDeletePath(path string) bool {
	return path == wire.PathDeleteQueue || path == wire.PathDeleteSubscription
}

func lifecycleReconcileHint(path string) string {
	if path == wire.PathDeleteSubscription {
		return "ListSubscriptionsWithOptions with IncludeDeleting: true"
	}
	return "ListQueues"
}

func readClientResponse(r io.Reader, contentLength, limit int64) ([]byte, error) {
	if contentLength > limit {
		return nil, fmt.Errorf("mqlite: response exceeds %d bytes", limit)
	}
	data, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("mqlite: response exceeds %d bytes", limit)
	}
	return data, nil
}

func observeServerTime(header http.Header, started, received time.Time) serverTimeObservation {
	roundTrip := received.Sub(started)
	if roundTrip < 0 {
		roundTrip = 0
	}
	var serverLower, serverUpper time.Time
	if raw := header.Get(wire.ServerTimeHeader); raw != "" {
		if ms, err := strconv.ParseInt(raw, 10, 64); err == nil && ms > 0 {
			serverLower = time.UnixMilli(ms)
			serverUpper = serverLower.Add(time.Millisecond + roundTrip)
		}
	}
	if serverLower.IsZero() {
		if parsed, err := http.ParseTime(header.Get("Date")); err == nil {
			serverLower = parsed
			serverUpper = parsed.Add(time.Second + roundTrip)
		}
	}
	if serverLower.IsZero() {
		return serverTimeObservation{}
	}
	return serverTimeObservation{localAt: received, serverUpper: serverUpper}
}

// localLeaseDeadline maps a broker-wall deadline onto the local monotonic
// timeline. The upper bound deliberately charges the complete observed RTT, not
// an assumed symmetric half: a slow response may have spent almost all its time
// after the broker committed the lease. It never substitutes the local wall
// clock merely because it falls inside the uncertainty interval. In particular,
// doing that after a long poll could extend a lease when the client clock is slow.
func (o serverTimeObservation) localLeaseDeadline(deadline time.Time) (time.Time, bool) {
	if deadline.IsZero() || o.localAt.IsZero() {
		return time.Time{}, false
	}
	now := time.Now()
	elapsed := now.Sub(o.localAt)
	if elapsed < 0 {
		elapsed = 0
	}
	serverUpper := o.serverUpper.Add(elapsed)
	return now.Add(deadline.Sub(serverUpper)), true
}

func (c *Client) post(ctx context.Context, path string, reqBody, respOut any) error {
	_, err := c.postObserved(ctx, path, reqBody, respOut)
	return err
}

// postObserved returns the clock observation from this exact successful response.
// Lease-bearing data must use its own response rather than a process-wide sample:
// a long-poll RTT is a safe budget for that batch, but must not shorten unrelated
// leases returned by later requests.
func (c *Client) postObserved(ctx context.Context, path string, reqBody, respOut any) (serverTimeObservation, error) {
	b, err := json.Marshal(reqBody)
	if err != nil {
		return serverTimeObservation{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+path, bytes.NewReader(b))
	if err != nil {
		return serverTimeObservation{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	started := time.Now()
	resp, err := c.http.Do(req)
	received := time.Now()
	if err != nil {
		if isLifecycleDeletePath(path) {
			// Once RoundTrip starts, a transport error cannot prove whether the
			// broker committed the name-addressed deletion. Do not invite an
			// automatic replay that could hit a later same-name resource.
			return serverTimeObservation{}, fmt.Errorf("%w: deletion response unavailable; inspect %s to reconcile: %s", ErrOutcomeUnknown, lifecycleReconcileHint(path), err.Error())
		}
		return serverTimeObservation{}, err
	}
	defer resp.Body.Close()
	// A proxy or gateway may synthesize redirects and failures with its own Date
	// header. Only a successful broker response is evidence about the clock that
	// issued the lease carried by that response.
	var observation serverTimeObservation
	if resp.StatusCode/100 == 2 {
		observation = observeServerTime(resp.Header, started, received)
	}
	if resp.StatusCode/100 == 3 {
		if isLifecycleDeletePath(path) {
			return serverTimeObservation{}, fmt.Errorf("%w: deletion received HTTP redirect %d; inspect %s to reconcile", ErrOutcomeUnknown, resp.StatusCode, lifecycleReconcileHint(path))
		}
		// Location is controlled by an intermediary and may contain credentials.
		// Report only the status; RPC redirects are never part of the protocol.
		return serverTimeObservation{}, &HTTPError{StatusCode: resp.StatusCode, cause: errors.New("mqlite: unexpected HTTP redirect")}
	}
	if isLifecycleDeletePath(path) && resp.StatusCode/100 == 5 {
		// An intermediary can synthesize a gateway error after the broker has
		// accepted the request but before its acknowledgement reaches us. The
		// status and body therefore cannot safely authorize a name-only replay.
		return serverTimeObservation{}, fmt.Errorf("%w: deletion received HTTP %d; inspect %s to reconcile", ErrOutcomeUnknown, resp.StatusCode, lifecycleReconcileHint(path))
	}
	keyResponse := path == wire.PathCreateKey || path == wire.PathListKeys || path == wire.PathRevokeKey
	if resp.StatusCode/100 != 2 {
		var eb wire.ErrorBody
		data, readErr := readClientResponse(resp.Body, resp.ContentLength, maxClientErrorBytes)
		if readErr == nil {
			if err := json.Unmarshal(data, &eb); err != nil {
				eb = wire.ErrorBody{}
			}
		}
		// Did the broker say "I have no such ROUTE" — i.e. it is too old to serve this operation —
		// rather than "that queue/message does not exist"? Both are 404s, and the difference
		// decides whether a client may fall back to an older equivalent.
		//
		// The broker's catch-all answers an unknown path with a STRUCTURED body: code
		// "not_found", message "no such path: <path>". (An earlier version of this check only
		// recognized a bare, code-less 404 — which is what a dumb proxy emits, but NOT what the
		// released broker does. It would therefore never have fired against the very brokers it
		// exists for.) Accept both shapes: the structured no-such-path answer, and a code-less
		// 404 from something in front of the broker.
		if resp.StatusCode == http.StatusNotFound &&
			(eb.Code == "" || strings.HasPrefix(eb.Message, "no such path")) {
			return serverTimeObservation{}, &HTTPError{StatusCode: resp.StatusCode, cause: fmt.Errorf("%w: %s", ErrUnsupported, path)}
		}
		if keyResponse {
			// Intermediary error bodies are untrusted too. Keep recognized codes
			// without echoing arbitrary text that could contain a partial secret.
			eb.Message = "access key request failed"
			switch eb.Code {
			case "unauthenticated", "permission_denied", "key_conflict", "not_found", "invalid_argument", "message_too_large", "outcome_unknown", "internal", "canceled", "unimplemented":
			default:
				eb.Code = ""
			}
		}
		// Proxies may omit or rename the JSON error code. HTTP authentication
		// failures are still permanent, so a Receiver must never retry them.
		switch resp.StatusCode {
		case http.StatusUnauthorized:
			eb.Code = "unauthenticated"
		case http.StatusForbidden:
			eb.Code = "permission_denied"
		}
		if path == wire.PathDeleteSubscription && eb.Code == "not_found" {
			return serverTimeObservation{}, &HTTPError{StatusCode: resp.StatusCode, cause: fmt.Errorf("%w: %s", ErrSubscriptionNotFound, eb.Message)}
		}
		if path == wire.PathSubscribe && eb.Code == "resource_exhausted" {
			return serverTimeObservation{}, &HTTPError{StatusCode: resp.StatusCode, cause: fmt.Errorf("%w: %s", ErrSubscriptionLimit, eb.Message)}
		}
		if eb.Code == "" {
			return serverTimeObservation{}, &HTTPError{StatusCode: resp.StatusCode}
		}
		return serverTimeObservation{}, &HTTPError{StatusCode: resp.StatusCode, cause: mapErr(eb)}
	}
	if respOut == nil {
		return observation, nil
	}

	var data []byte
	switch {
	case keyResponse:
		data, err = wire.ReadKeyResponse(resp.Body)
	case path == wire.PathObserve:
		data, err = wire.ReadObserveResponse(resp.Body)
	case path == wire.PathListSubscriptionsPage:
		data, err = wire.ReadListSubscriptionsPageResponse(resp.Body)
	case isLifecycleDeletePath(path):
		data, err = wire.ReadDeleteResponse(resp.Body)
	default:
		data, err = readClientResponse(resp.Body, resp.ContentLength, maxClientResponseBytes)
	}
	if err != nil {
		switch {
		case isLifecycleDeletePath(path):
			return observation, fmt.Errorf("%w: invalid deletion acknowledgement; inspect %s to reconcile", ErrOutcomeUnknown, lifecycleReconcileHint(path))
		case keyResponse && (path == wire.PathCreateKey || path == wire.PathRevokeKey):
			return observation, fmt.Errorf("%w: incomplete or oversized access key response", ErrOutcomeUnknown)
		default:
			return observation, &invalidSuccessResponseError{cause: err}
		}
	}
	if raw, ok := respOut.(*json.RawMessage); ok {
		*raw = data
		return observation, nil
	}
	if err := json.Unmarshal(data, respOut); err != nil {
		return observation, &invalidSuccessResponseError{cause: err}
	}
	return observation, nil
}

func mapErr(eb wire.ErrorBody) error {
	switch eb.Code {
	case "not_found":
		if eb.Resource == "queue" {
			return fmt.Errorf("%w: %s", ErrQueueNotFound, eb.Message)
		}
		if eb.Resource == "subscription" {
			return fmt.Errorf("%w: %s", ErrSubscriptionNotFound, eb.Message)
		}
		return fmt.Errorf("%w: %s", ErrNotFound, eb.Message)
	case "already_exists":
		return fmt.Errorf("%w: %s", ErrDedupConflict, eb.Message)
	case "queue_not_empty":
		return fmt.Errorf("%w: %s", ErrQueueNotEmpty, eb.Message)
	case "subscription_not_empty":
		return fmt.Errorf("%w: %s", ErrSubscriptionNotEmpty, eb.Message)
	case "name_conflict":
		return fmt.Errorf("%w: %s", ErrNameConflict, eb.Message)
	case "group_required":
		return fmt.Errorf("%w: %s", ErrGroupRequired, eb.Message)
	case "invalid_argument":
		return fmt.Errorf("%w: %s", ErrInvalidArgument, eb.Message)
	case "message_too_large":
		return fmt.Errorf("%w: %s", ErrMessageTooLarge, eb.Message)
	case "lock_lost":
		return fmt.Errorf("%w: %s", ErrLockLost, eb.Message)
	case "unauthenticated":
		return fmt.Errorf("%w: %s", ErrUnauthenticated, eb.Message)
	case "permission_denied":
		return fmt.Errorf("%w: %s", ErrPermissionDenied, eb.Message)
	case "key_conflict":
		return fmt.Errorf("%w: %s", ErrKeyConflict, eb.Message)
	case "outcome_unknown":
		return fmt.Errorf("%w: %s", ErrOutcomeUnknown, eb.Message)
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

func (c *Client) wireToMessage(queue string, wm wire.Message, observation serverTimeObservation) *Message {
	lockedUntil := msToTime(wm.LockedUntilMs)
	localLockedUntil, _ := observation.localLeaseDeadline(lockedUntil)
	return &Message{
		SequenceNumber:   wm.SeqNumber,
		Body:             wm.Body,
		MessageID:        wm.MessageID,
		GroupID:          wm.GroupID,
		CorrelationID:    wm.CorrelationID,
		ReplyTo:          wm.ReplyTo,
		Subject:          wm.Subject,
		ContentType:      wm.ContentType,
		Properties:       wm.Properties,
		DeliveryCount:    wm.DeliveryCount,
		EnqueuedAt:       msToTime(wm.EnqueuedAtMs),
		LockedUntil:      lockedUntil,
		localLockedUntil: localLockedUntil,
		queue:            queue,
		lockToken:        wm.LockToken,
		s:                c,
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
// An invalid successful acknowledgement returns ErrOutcomeUnknown because the
// message may already have committed; reconcile before retrying.
func (c *Client) SendOne(ctx context.Context, queue string, m OutMessage, opts ...SendOpts) (int64, error) {
	if err := validateOutMessageTTL(m); err != nil {
		return 0, err
	}
	o := firstOpt(opts)
	path := wire.PathSend
	req := wire.SendRequest{Queue: queue, Messages: []wire.Message{outToWire(m)}, TTLMs: m.TTL.Milliseconds()}
	if !o.At.IsZero() {
		path = wire.PathSchedule
		req.ScheduledEnqueueTimeMs = o.At.UnixMilli()
	}
	var resp wire.SendResponse
	if err := c.post(ctx, path, req, &resp); err != nil {
		var invalid *invalidSuccessResponseError
		if errors.As(err, &invalid) {
			return 0, &ambiguousAcknowledgementError{operation: "Send", cause: err}
		}
		return 0, err
	}
	if len(resp.SeqNumbers) != 1 {
		return 0, &ambiguousAcknowledgementError{
			operation: "Send",
			cause:     fmt.Errorf("received %d sequence numbers for 1 message", len(resp.SeqNumbers)),
		}
	}
	// seq 0 on a 200 response is a topic publish that matched no subscription — a valid no-op
	// returning (0, nil), matching the embedded SDK. A real single-message dedup conflict
	// arrives as HTTP 409 already_exists (mapped to ErrDedupConflict by c.post above), so it
	// never reaches here as a 0 seq (MQLITE-75).
	return resp.SeqNumbers[0], nil
}

// Send enqueues one or many messages in one request/transaction (use SendOne
// for a single seq return or At() scheduling). Batches whose messages use
// different TTL values require a broker that supports per-message ttl_ms;
// older strict brokers reject that additive field rather than applying it wrongly.
// An invalid successful acknowledgement returns ErrOutcomeUnknown because the
// batch may already have committed; reconcile before retrying.
func (c *Client) Send(ctx context.Context, queue string, msgs ...OutMessage) ([]int64, error) {
	if len(msgs) == 0 {
		return nil, nil
	}
	for i, m := range msgs {
		if err := validateOutMessageTTL(m); err != nil {
			return nil, fmt.Errorf("message %d: %w", i, err)
		}
	}
	ttl := msgs[0].TTL.Milliseconds()
	uniformTTL := true
	for _, m := range msgs[1:] {
		if m.TTL.Milliseconds() != ttl {
			uniformTTL = false
			break
		}
	}
	// Preserve the legacy request-level field for uniform batches so new clients
	// remain compatible with old strict brokers. Only a mixed batch needs the
	// additive per-message field; an old broker rejects it instead of silently
	// applying one message's TTL to every row.
	var request any
	if uniformTTL {
		messages := make([]wire.Message, len(msgs))
		for i, m := range msgs {
			messages[i] = outToWire(m)
		}
		request = wire.SendRequest{Queue: queue, Messages: messages, TTLMs: ttl}
	} else {
		messages := make([]wire.SendMessage, len(msgs))
		for i, m := range msgs {
			messages[i] = wire.SendMessage{Message: outToWire(m), TTLMs: m.TTL.Milliseconds()}
		}
		request = wire.SendWithMessageTTLsRequest{Queue: queue, Messages: messages}
	}
	var resp wire.SendResponse
	if err := c.post(ctx, wire.PathSend, request, &resp); err != nil {
		var invalid *invalidSuccessResponseError
		if errors.As(err, &invalid) {
			return nil, &ambiguousAcknowledgementError{operation: "Send", cause: err}
		}
		return nil, err
	}
	if len(resp.SeqNumbers) != len(msgs) {
		return nil, &ambiguousAcknowledgementError{
			operation: "Send",
			cause:     fmt.Errorf("received %d sequence numbers for %d messages", len(resp.SeqNumbers), len(msgs)),
		}
	}
	return resp.SeqNumbers, nil
}

// Cancel deletes a never-delivered scheduled Send. It cannot cancel a retry
// parked in scheduled by delayed Abandon.
func (c *Client) Cancel(ctx context.Context, queue string, seq int64) error {
	var response wire.SettleResponse
	if err := c.post(ctx, wire.PathCancel, wire.CancelRequest{Queue: queue, SeqNumber: seq}, &response); err != nil {
		var invalid *invalidSuccessResponseError
		if errors.As(err, &invalid) {
			return &ambiguousAcknowledgementError{operation: "Cancel", cause: err}
		}
		return err
	}
	if !response.Ok {
		return &ambiguousAcknowledgementError{
			operation: "Cancel",
			cause:     errors.New("successful response contained ok=false"),
		}
	}
	return nil
}

// SettleResult is the per-message outcome of a batch settle (Ok=false = the lock was
// lost/expired for that message — not a fatal error for the batch).
type SettleResult struct {
	SequenceNumber int64
	Ok             bool
	// LockedUntil is the lease deadline a RenewBatch actually committed (zero for other
	// operations). A renewing caller needs it to know when to renew next — without it, it can
	// only guess, and would either hammer the broker or let the lock it just extended lapse.
	//
	// It is REPORTED, never written back onto the *Message: the caller may be reading those
	// messages concurrently (the CLI renders them while a background renewer runs), and mutating
	// a shared time.Time from another goroutine is a data race.
	LockedUntil time.Time
}

func toSettleResults(rs []wire.SettleItemResult) []SettleResult {
	out := make([]SettleResult, len(rs))
	for i, r := range rs {
		out[i] = SettleResult{SequenceNumber: r.SeqNumber, Ok: r.Ok, LockedUntil: msFromEpoch(r.LockedUntilMs)}
	}
	return out
}

func settleItems(msgs []*Message) ([]wire.SettleItem, error) {
	items := make([]wire.SettleItem, len(msgs))
	for i, m := range msgs {
		if m == nil {
			return nil, fmt.Errorf("%w: message %d is nil", ErrInvalidArgument, i)
		}
		items[i] = wire.SettleItem{SeqNumber: m.SequenceNumber, LockToken: m.lockToken}
	}
	return items, nil
}

func validateBatchResults(msgs []*Message, results []wire.SettleItemResult, renew bool) error {
	if len(results) != len(msgs) {
		return fmt.Errorf("mqlite: batch response has %d results for %d messages", len(results), len(msgs))
	}
	for i, result := range results {
		if result.SeqNumber != msgs[i].SequenceNumber || result.LockedUntilMs < 0 ||
			(!renew && result.LockedUntilMs != 0) ||
			(renew && ((result.Ok && result.LockedUntilMs == 0) || (!result.Ok && result.LockedUntilMs != 0))) {
			return fmt.Errorf("mqlite: invalid batch result at index %d", i)
		}
	}
	return nil
}

// msFromEpoch turns an epoch-ms deadline into a time, keeping the zero time for "not set".
func msFromEpoch(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}

// CompleteBatch completes many received messages in one round-trip — the fix for the
// drain N+1 (settling a received batch one HTTP call at a time). Messages should
// share one queue; a stale lock comes back as Ok=false rather than failing the batch.
// Message rehydrates a settleable handle for a message you already received but did not
// settle (e.g. `mqlite receive --no-ack`, then settle in a later invocation). Pass the
// queue, its SequenceNumber, and the LockToken() from that receive; the returned *Message
// settles through this Client (Complete/Abandon/Reject/Defer/Renew).
func (c *Client) Message(queue string, seq int64, lockToken string) *Message {
	return &Message{queue: queue, SequenceNumber: seq, lockToken: lockToken, s: c}
}

// CompleteBatch completes many messages in one request (one settlement RPC instead of N),
// returning a per-message result so a caller can see which sequence numbers settled.
func (c *Client) CompleteBatch(ctx context.Context, queue string, msgs ...*Message) ([]SettleResult, error) {
	items, err := settleItems(msgs)
	if err != nil {
		return nil, err
	}
	var resp wire.CompleteBatchResponse
	if err := c.post(ctx, wire.PathCompleteBatch, wire.CompleteBatchRequest{Queue: queue, Messages: items}, &resp); err != nil {
		var invalid *invalidSuccessResponseError
		if errors.As(err, &invalid) {
			return nil, &ambiguousAcknowledgementError{operation: "CompleteBatch", cause: err}
		}
		return nil, err
	}
	if err := validateBatchResults(msgs, resp.Results, false); err != nil {
		return nil, &ambiguousAcknowledgementError{operation: "CompleteBatch", cause: err}
	}
	return toSettleResults(resp.Results), nil
}

// RenewBatch extends the lock lease of many messages in ONE request, returning a per-message
// result. Renewing a batch with N separate Renew calls costs N round trips — on a slow link
// that outlasts the very lease it is trying to save (a 64-message batch on a 50ms link needs
// 3.2s of renewals against a 2s lease), so the locks expire mid-renewal.
//
// SettleResult.LockedUntil is the absolute broker deadline that was committed. Ok says the lease
// was live when the broker's STATEMENT finished; the response still has to travel back to you, so
// on a very short lease it can lapse on the way. Raw callers comparing LockedUntil with a client
// wall clock still require synchronized clocks. The managed Receiver binds the broker-time header
// from this exact response to that deadline, maps it onto local monotonic time, and uses successful
// renewal as a dispatch fence when no usable clock observation is available.
func (c *Client) RenewBatch(ctx context.Context, queue string, msgs ...*Message) ([]SettleResult, error) {
	results, _, err := c.renewBatchObserved(ctx, queue, msgs...)
	return results, err
}

func (c *Client) renewBatchObserved(ctx context.Context, queue string, msgs ...*Message) ([]SettleResult, serverTimeObservation, error) {
	if len(msgs) > MaxRenewBatch {
		return nil, serverTimeObservation{}, fmt.Errorf("%w: renew batch exceeds %d messages", ErrInvalidArgument, MaxRenewBatch)
	}
	items, err := settleItems(msgs)
	if err != nil {
		return nil, serverTimeObservation{}, err
	}
	var resp wire.RenewBatchResponse
	observation, err := c.postObserved(ctx, wire.PathRenewBatch, wire.RenewBatchRequest{Queue: queue, Messages: items}, &resp)
	if err != nil {
		var invalid *invalidSuccessResponseError
		if errors.As(err, &invalid) {
			return nil, serverTimeObservation{}, &ambiguousAcknowledgementError{operation: "RenewBatch", cause: err}
		}
		return nil, serverTimeObservation{}, err
	}
	if err := validateBatchResults(msgs, resp.Results, true); err != nil {
		return nil, serverTimeObservation{}, &ambiguousAcknowledgementError{operation: "RenewBatch", cause: err}
	}
	return toSettleResults(resp.Results), observation, nil
}

// ── receive ─────────────────────────────────────────────────────────────────

// Receive claims up to N messages (Peek-Lock by default), with optional long-poll.
// RecvOpts.Pick fetches previously-deferred messages by seq instead of claiming.
func (c *Client) Receive(ctx context.Context, queue string, opts ...RecvOpts) ([]*Message, error) {
	o := firstOpt(opts)
	var resp wire.ReceiveResponse
	var observation serverTimeObservation
	if len(o.Pick) > 0 {
		var err error
		observation, err = c.postObserved(ctx, wire.PathReceiveDeferred, wire.ReceiveDeferredRequest{Queue: queue, SeqNumbers: o.Pick}, &resp)
		if err != nil {
			return nil, err
		}
	} else {
		eo := o.toEngine()
		var err error
		observation, err = c.postObserved(ctx, wire.PathReceive, wire.ReceiveRequest{
			Queue: queue, MaxMessages: eo.MaxMessages, WaitTimeMs: eo.WaitMs,
			ReceiveMode: int(eo.Mode), AttemptID: eo.AttemptID, // idempotent receive
		}, &resp)
		if err != nil {
			var invalid *invalidSuccessResponseError
			if eo.Mode == engine.ReceiveAndDelete && errors.As(err, &invalid) {
				return nil, &ambiguousAcknowledgementError{operation: "AtMostOnce Receive", cause: err}
			}
			return nil, err
		}
	}
	out := make([]*Message, len(resp.Messages))
	for i, wm := range resp.Messages {
		out[i] = c.wireToMessage(queue, wm, observation)
	}
	return out, nil
}

func (c *Client) receiveOne(ctx context.Context, queue string, max int, waitMs int64, mode engine.ReceiveMode, attemptID string) ([]*Message, error) {
	var resp wire.ReceiveResponse
	observation, err := c.postObserved(ctx, wire.PathReceive, wire.ReceiveRequest{
		Queue: queue, MaxMessages: max, WaitTimeMs: waitMs, ReceiveMode: int(mode), AttemptID: attemptID,
	}, &resp)
	if err != nil {
		return nil, err
	}
	out := make([]*Message, len(resp.Messages))
	for i, wm := range resp.Messages {
		out[i] = c.wireToMessage(queue, wm, observation)
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
			DLQMaxAgeMs:  ec.DLQMaxAgeMs, DLQMaxCount: ec.DLQMaxCount, DLQMaxBytes: ec.DLQMaxBytes,
		},
	}, &wire.Empty{})
}

// DeleteQueue removes an empty plain queue. An accepted deletion returns nil
// whether bounded physical cleanup completed in this call or continues in the
// background. Use DeleteQueueWithOptions to force-discard retained messages or
// inspect the accepted deletion state.
func (c *Client) DeleteQueue(ctx context.Context, name string) error {
	_, err := c.DeleteQueueWithOptions(ctx, name)
	return err
}

// DeleteQueueWithOptions returns the accepted deletion state.
func (c *Client) DeleteQueueWithOptions(ctx context.Context, name string, opts ...DeleteQueueOpts) (DeleteResult, error) {
	o := firstOpt(opts)
	var data json.RawMessage
	if err := c.post(ctx, wire.PathDeleteQueue, wire.DeleteQueueRequest{Name: name, Force: o.Force}, &data); err != nil {
		return DeleteResult{}, err
	}
	resp, err := wire.DecodeDeleteResponse(data)
	if err != nil {
		return DeleteResult{}, fmt.Errorf("%w: invalid deletion acknowledgement; inspect ListQueues to reconcile", ErrOutcomeUnknown)
	}
	return DeleteResult{Deleted: resp.Deleted, State: resp.State}, nil
}

// DeleteSubscription removes an empty subscription. An accepted deletion
// returns nil whether bounded physical cleanup completed in this call or
// continues in the background. Use DeleteSubscriptionWithOptions to
// force-discard retained messages or inspect the accepted deletion state.
func (c *Client) DeleteSubscription(ctx context.Context, topic, name string) error {
	_, err := c.DeleteSubscriptionWithOptions(ctx, topic, name)
	return err
}

// DeleteSubscriptionWithOptions returns the accepted deletion state.
func (c *Client) DeleteSubscriptionWithOptions(ctx context.Context, topic, name string, opts ...DeleteSubscriptionOpts) (DeleteResult, error) {
	o := firstOpt(opts)
	var data json.RawMessage
	if err := c.post(ctx, wire.PathDeleteSubscription, wire.DeleteSubscriptionRequest{Topic: topic, Name: name, Force: o.Force}, &data); err != nil {
		return DeleteResult{}, err
	}
	resp, err := wire.DecodeDeleteResponse(data)
	if err != nil {
		return DeleteResult{}, fmt.Errorf("%w: invalid deletion acknowledgement; inspect ListSubscriptionsWithOptions with IncludeDeleting: true to reconcile", ErrOutcomeUnknown)
	}
	return DeleteResult{Deleted: resp.Deleted, State: resp.State}, nil
}

// Subscribe registers a subscription under a topic with an optional filter.
func (c *Client) Subscribe(ctx context.Context, topic, name string, f *Filter) error {
	return c.post(ctx, wire.PathSubscribe, wire.SubscribeRequest{Topic: topic, Name: name, Filter: f}, &wire.Empty{})
}

// SubscribeWithOptions creates or updates a subscription and optionally assigns
// a fixed lifetime on first creation. Supplying zero TTL preserves the lifetime
// of an existing subscription.
func (c *Client) SubscribeWithOptions(ctx context.Context, topic, name string, f *Filter, opts SubscribeOptions) error {
	o, err := opts.toEngine()
	if err != nil {
		return err
	}
	return c.post(ctx, wire.PathSubscribe, wire.SubscribeWithOptionsRequest{
		Topic: topic, Name: name, Filter: f, TTLMs: o.TTLMs,
	}, &wire.Empty{})
}

// ListQueues lists queues/subscriptions.
func (c *Client) ListQueues(ctx context.Context) ([]QueueInfo, error) {
	var resp wire.ListQueuesResponse
	if err := c.post(ctx, wire.PathListQueues, wire.Empty{}, &resp); err != nil {
		return nil, err
	}
	out := make([]QueueInfo, len(resp.Queues))
	for i, q := range resp.Queues {
		state := q.State
		if state == "" {
			// Brokers released before queue lifecycle support omitted state; every
			// queue they could list was active.
			state = "active"
		}
		out[i] = QueueInfo{Name: q.Name, Kind: q.Kind, State: state, LockDurationMs: q.LockDurationMs,
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

// Observe returns canonical queue, storage, maintenance and HTTP measurements.
func (c *Client) Observe(ctx context.Context) (Observation, error) {
	var data json.RawMessage
	if err := c.post(ctx, wire.PathObserve, wire.ObserveRequest{}, &data); err != nil {
		return Observation{}, err
	}
	return wire.DecodeObserveResponse(data)
}

// Status returns a desensitized snapshot of the broker's backend (never a token or DSN).
func (c *Client) Status(ctx context.Context) (StatusInfo, error) {
	var resp wire.StatusResponse
	if err := c.post(ctx, wire.PathStatus, wire.Empty{}, &resp); err != nil {
		return StatusInfo{}, err
	}
	return StatusInfo{
		Version: resp.Version, Backend: resp.Backend, Remote: resp.Remote, Location: resp.Location,
		SchemaVersion: resp.SchemaVersion, PingMs: resp.PingMs, DBSizeBytes: resp.DBSizeBytes,
		Queues: resp.Queues, Subscriptions: resp.Subscriptions,
		ServerTimeMs: resp.ServerTimeMs, UptimeMs: resp.UptimeMs, Auth: resp.Auth,
	}, nil
}

// ListSubscriptions returns every active, unexpired subscription with its topic and
// filter expression.
//
// Compatibility note: use ListSubscriptionsWithOptions for bounded inventory and
// lifecycle reconciliation, including deleting name reservations.
func (c *Client) ListSubscriptions(ctx context.Context) ([]SubscriptionInfo, error) {
	var resp wire.ListSubscriptionsResponse
	if err := c.post(ctx, wire.PathListSubscriptions, wire.Empty{}, &resp); err != nil {
		return nil, err
	}
	out := make([]SubscriptionInfo, len(resp.Subscriptions))
	for i, s := range resp.Subscriptions {
		out[i] = SubscriptionInfo{Topic: s.Topic, Name: s.Name, Expr: s.Expr}
	}
	return out, nil
}

// ListSubscriptionsWithOptions returns one internally consistent subscription
// page. Separate page requests may observe concurrent roster changes.
func (c *Client) ListSubscriptionsWithOptions(ctx context.Context, opts ListSubscriptionsOptions) (SubscriptionPage, error) {
	request := wire.ListSubscriptionsPageRequest{
		Topic: opts.Topic, After: opts.After, Limit: opts.Limit, IncludeDeleting: opts.IncludeDeleting,
	}
	var data json.RawMessage
	if err := c.post(ctx, wire.PathListSubscriptionsPage, request, &data); err != nil {
		return SubscriptionPage{}, err
	}
	resp, err := wire.DecodeListSubscriptionsPageResponse(data, request)
	if err != nil {
		return SubscriptionPage{}, err
	}
	out := SubscriptionPage{
		Subscriptions: make([]SubscriptionPageItem, len(resp.Subscriptions)),
		Total:         resp.Total,
		NextAfter:     resp.NextAfter,
	}
	for i, s := range resp.Subscriptions {
		out.Subscriptions[i] = SubscriptionPageItem{
			Topic: s.Topic, Name: s.Name, Expr: s.Expr, State: s.State,
			CreatedAtMs: s.CreatedAtMs, ExpiresAtMs: s.ExpiresAtMs,
		}
	}
	return out, nil
}

// TestFilter dry-runs a subscription filter expression: it compiles expr and, when sample
// is non-nil, evaluates it against that message (nothing is enqueued). Read-only.
func (c *Client) TestFilter(ctx context.Context, expr string, sample *OutMessage, enqueuedAtMs, visibleAtMs int64) (FilterTestResult, error) {
	req := wire.TestFilterRequest{Expr: expr}
	if sample != nil {
		wm := outToWire(*sample)
		wm.EnqueuedAtMs = enqueuedAtMs
		wm.VisibleAtMs = visibleAtMs
		req.Message = &wm
	}
	var resp wire.TestFilterResponse
	if err := c.post(ctx, wire.PathTestFilter, req, &resp); err != nil {
		return FilterTestResult{}, err
	}
	return FilterTestResult{Valid: resp.Valid, Error: resp.Error, Ran: resp.Ran, Matched: resp.Matched}, nil
}

// Receiver returns a stateful receive loop bound to this client.
func (c *Client) Receiver(queue string, opts ...ReceiverOption) *Receiver {
	return newReceiver(c, queue, opts)
}

// ── settler ─────────────────────────────────────────────────────────────────

func (c *Client) settle(ctx context.Context, path string, req wire.SettleRequest) error {
	var r wire.SettleResponse
	if err := c.post(ctx, path, req, &r); err != nil {
		var invalid *invalidSuccessResponseError
		if errors.As(err, &invalid) {
			operation := path
			if slash := strings.LastIndexByte(operation, '/'); slash >= 0 {
				operation = operation[slash+1:]
			}
			return &ambiguousAcknowledgementError{operation: operation, cause: err}
		}
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

func (c *Client) renewDeadline(ctx context.Context, q string, seq int64, tok string) (time.Time, error) {
	results, observation, err := c.renewBatchObserved(ctx, q, c.Message(q, seq, tok))
	if err != nil {
		return time.Time{}, err
	}
	if len(results) != 1 || !results[0].Ok {
		return time.Time{}, ErrLockLost
	}
	if local, ok := observation.localLeaseDeadline(results[0].LockedUntil); ok {
		return local, nil
	}
	return results[0].LockedUntil, nil
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
	ExpiresAt      time.Time // TTL expiry (zero = no TTL)
	LockedUntil    time.Time
	// Dead-letter metadata, set for messages in the dead_lettered state (triage).
	DeadLetterReason      string
	DeadLetterDescription string
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
		ExpiresAt:      msToTime(wm.ExpiresAtMs),
		LockedUntil:    msToTime(wm.LockedUntilMs),

		DeadLetterReason:      wm.DeadLetterReason,
		DeadLetterDescription: wm.DeadLetterDescription,
	}
}
