package mqlite

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mqlitehq/mqlite/engine"
)

// newAttemptID returns a random idempotency key for one receive round (§17.1).
func newAttemptID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

const (
	receiveBackoffBase = 500 * time.Millisecond
	receiveBackoffMax  = 10 * time.Second
	renewRetryBase     = 50 * time.Millisecond
	renewRetryMax      = 500 * time.Millisecond
	settleRetryBase    = 50 * time.Millisecond
	settleRetryMax     = 500 * time.Millisecond
	settleTimeout      = 5 * time.Second
	renewFallbackLease = 20 * time.Second
	replayLeaseSafety  = 10 * time.Millisecond
)

// deadlineRenewer is an optional managed-Receiver extension. A source that can
// return the deadline actually committed by Renew lets the next heartbeat use
// fresh lease state without racing a write to Message.LockedUntil. Sources that
// only implement the legacy settler continue to work with a conservative local
// lease estimate.
type deadlineRenewer interface {
	renewDeadline(context.Context, string, int64, string) (time.Time, error)
}

func exponentialBackoff(base, max time.Duration, step int) time.Duration {
	d := base
	for i := 0; i < step && d < max; i++ {
		if d > max/2 {
			return max
		}
		d *= 2
	}
	if d > max {
		return max
	}
	return d
}

// jitterDuration spreads retries across an 80-120% window. crypto/rand avoids
// giving every process the same sequence after a simultaneous broker restart.
func jitterDuration(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	var b [1]byte
	if _, err := rand.Read(b[:]); err != nil {
		return d
	}
	percent := int64(80) + int64(b[0])*40/255
	return time.Duration(int64(d) * percent / 100)
}

func waitContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

type receiverConfig struct {
	autoRenew   bool
	concurrency int
	prefetch    int
	onError     func(error)
}

// ReceiverOption configures a Receiver.
type ReceiverOption func(*receiverConfig)

// WithAutoRenew keeps locks alive with a background heartbeat for long handlers.
// Transient renewal failures are retried while the known lease remains usable. If
// the lock is lost or renewal cannot be confirmed before its deadline, the
// per-message handler context is canceled and that delivery is not settled.
func WithAutoRenew() ReceiverOption { return func(c *receiverConfig) { c.autoRenew = true } }

// WithConcurrency sets the maximum number of handlers executing in parallel.
// Renewal shutdown and settlement may overlap later handlers; the Receiver bounds
// the total claimed deliveries to concurrency plus the effective prefetch.
func WithConcurrency(n int) ReceiverOption { return func(c *receiverConfig) { c.concurrency = n } }

// WithPrefetch caps how many messages are claimed per receive. It is clamped to
// the concurrency: the Receiver never claims more than it has idle handler slots
// for, so a claimed message starts immediately instead of burning its lock behind
// a busy handler (MQLITE-76). Settlement may overlap the next claim; combined
// handler-plus-settlement work is bounded to concurrency plus effective prefetch
// (at most twice the concurrency). Values above concurrency have no extra effect.
func WithPrefetch(n int) ReceiverOption { return func(c *receiverConfig) { c.prefetch = n } }

// WithErrorHandler registers a callback for errors the receive loop would otherwise swallow:
// a transient receive error being retried, or a per-message settle/renew failure (e.g. a lost
// lock). It is advisory — for logging or metrics — is called serially (the Receiver guards
// it, so a plain slice-append or counter is safe), and must not block. A permanent error
// (bad token, denied permission, missing queue) is passed here first and then also
// returned from Run (MQLITE-77).
func WithErrorHandler(fn func(error)) ReceiverOption {
	return func(c *receiverConfig) { c.onError = fn }
}

// Receiver is a stateful receive loop. handler returning nil auto-Completes;
// returning error auto-Abandons (§17.1). Handlers must honor their context:
// cancellation can mean Run is stopping or auto-renew could no longer retain
// that delivery's lock.
type Receiver struct {
	src   receiveSource
	queue string
	cfg   receiverConfig
	errMu sync.Mutex // serializes onError across the loop, worker, and renew goroutines
	// deadlineRenewUnsupported is learned once from an older broker. Protect only
	// this capability bit; never hold the mutex across a network request.
	renewCapabilityMu        sync.Mutex
	deadlineRenewUnsupported bool
}

func newReceiver(src receiveSource, queue string, opts []ReceiverOption) *Receiver {
	cfg := receiverConfig{concurrency: 1}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.concurrency < 1 {
		cfg.concurrency = 1
	}
	return &Receiver{src: src, queue: queue, cfg: cfg}
}

func (r *Receiver) notify(err error) {
	if r.cfg.onError == nil || err == nil {
		return
	}
	r.errMu.Lock()
	defer r.errMu.Unlock()
	r.cfg.onError(err)
}

// isPermanent reports whether an error will not be fixed by retrying and means the consumer
// is misconfigured — a bad token, denied permission, or a missing queue: the loop
// stops and Run returns it, instead of spinning forever. Transient errors (network, 5xx, timeouts) and an expected
// ErrLockLost are not permanent.
func isPermanent(err error) bool {
	return errors.Is(err, ErrUnauthenticated) ||
		errors.Is(err, ErrPermissionDenied) ||
		errors.Is(err, ErrNotFound) ||
		errors.Is(err, ErrQueueNotFound) ||
		// The broker does not serve this operation at all — an older broker, or a proxy that
		// routes only part of the API. Retrying cannot make the route appear, so a receiver that
		// treated this as transient would spin forever instead of reporting the incompatibility
		// (MQLITE-97; this sentinel used to arrive as ErrNotFound, which WAS classified here).
		errors.Is(err, ErrUnsupported) ||
		// The embedded engine has been closed (another goroutine called Embedded.Close). No amount
		// of retrying brings it back, so Run must return instead of hammering the error handler
		// every 500ms forever (round-8).
		errors.Is(err, ErrClosed)
}

// reservePaired blocks until both a handler slot and a total-inflight slot are
// available, then greedily takes up to max complete pairs without blocking.
// Acquiring the pair before Receive keeps claims within immediately runnable
// handler capacity while the separate total limit bounds settlement overlap.
func reservePaired(ctx context.Context, handlers, total chan struct{}, max int) int {
	select {
	case handlers <- struct{}{}:
	case <-ctx.Done():
		return 0
	}
	select {
	case total <- struct{}{}:
	case <-ctx.Done():
		<-handlers
		return 0
	}
	n := 1
	for n < max {
		select {
		case handlers <- struct{}{}:
		case <-ctx.Done():
			return n
		default:
			return n
		}
		select {
		case total <- struct{}{}:
			n++
		case <-ctx.Done():
			<-handlers
			return n
		default:
			// A partial pair cannot authorize a claim. Roll the handler token
			// back when the tighter total-inflight budget is already full.
			<-handlers
			return n
		}
	}
	return n
}

func releasePaired(handlers, total chan struct{}, n int) {
	for i := 0; i < n; i++ {
		<-handlers
		<-total
	}
}

// Run pulls and processes messages until ctx is canceled or a permanent error occurs.
// It returns ctx.Err() on cancellation, or the first permanent receive/settle/renew error.
func (r *Receiver) Run(ctx context.Context, handler func(context.Context, *Message) error) error {
	batch := r.cfg.prefetch
	if batch <= 0 || batch > r.cfg.concurrency {
		batch = r.cfg.concurrency
	}
	handlerSlots := make(chan struct{}, r.cfg.concurrency)
	// Let handlers overlap one additional claim wave with renew shutdown and
	// settlement. The default prefetch equals concurrency, giving a 2C bound;
	// smaller prefetch values deliberately tighten that bound.
	totalSlots := make(chan struct{}, r.cfg.concurrency+batch)
	// fatalRec carries the first permanent error PLUS the shutdown ordering at the moment it was
	// captured. The ordering must be recorded here, at capture, not read later in done(): by the
	// time done() runs, the caller may have cancelled AFTER a genuine engine-close failure was
	// recorded, and a late ctx check would misfile that failure as "just cancellation" (codex).
	type fatalRec struct {
		err         error
		afterCancel bool // the caller had ALREADY cancelled when this error was captured
	}
	fatal := make(chan fatalRec, 1)
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup

	// fail records the first permanent error and stops the loop + all workers. afterCancel is
	// SAMPLED BY THE CALLER at the moment the error was first observed — not here. Between
	// observation and this call sit the transient retry and r.notify (whose user callback may
	// itself cancel the context), so sampling here would let a later cancellation rewrite the
	// ordering of an earlier ErrClosed (codex).
	fail := func(err error, afterCancel bool) {
		select {
		case fatal <- fatalRec{err: err, afterCancel: afterCancel}:
		default:
		}
		cancel()
	}
	// cancelled samples the CALLER's context — the ordering fact workers record with a fatal.
	cancelled := func() bool { return ctx.Err() != nil }
	// done drains in-flight workers and returns the fatal error if one was recorded, else
	// the context error (normal cancellation).
	done := func() error {
		wg.Wait()
		select {
		case rec := <-fatal:
			// Graceful-shutdown ordering (round-8, codex): when the CALLER cancelled Run first and
			// the engine was closed afterwards, an in-flight worker's cleanup settle hits the closed
			// engine and records ErrClosed — an artifact of the receiver's own shutdown, not
			// information, so Run reports the caller's own context.Canceled. But an ErrClosed
			// captured while the context was LIVE (the engine was closed under a running receiver)
			// keeps precedence even if the caller cancels later, which is why the ordering is part
			// of the record, not a late ctx check.
			if rec.afterCancel && errors.Is(rec.err, ErrClosed) {
				return ctx.Err()
			}
			return rec.err
		default:
			return ctx.Err()
		}
	}

	for {
		if rctx.Err() != nil {
			return done()
		}
		// Reserve handler and total-inflight capacity BEFORE claiming, and claim no
		// more than reserved: every claimed message immediately gets a handler (and
		// auto-renew), while settlement overlap stays bounded (MQLITE-76).
		n := reservePaired(rctx, handlerSlots, totalSlots, batch)
		if n == 0 {
			return done()
		}
		// Cancellation can race a total-slot release, leaving both select cases
		// ready inside reservePaired. Do not issue a post-shutdown Receive merely
		// because capacity won that select; roll the complete reservations back.
		if rctx.Err() != nil {
			releasePaired(handlerSlots, totalSlots, n)
			return done()
		}
		// Keep both the reservation and attempt id across every transient retry. If the broker
		// already claimed and recorded this batch (only the responses were lost), each retry
		// replays that exact batch instead of claiming more messages. Holding the reservation
		// also keeps the request's MaxMessages stable across the replay window.
		attemptID := newAttemptID()
		var msgs []*Message
		var err error
		var errAfterCancel bool
		hadTransientFailure := false
		transientFailures := 0
		for {
			msgs, err = r.src.receiveOne(rctx, r.queue, n, 20000, engine.PeekLock, attemptID) // 20s long-poll
			// Sample the shutdown ordering the moment the error is observed — before a retry or
			// notify callback can span a caller cancellation (codex).
			errAfterCancel = err != nil && cancelled()
			if err == nil || !retryableManagedError(err) || rctx.Err() != nil {
				break
			}
			hadTransientFailure = true
			transientFailures++
			if transientFailures == 1 {
				// Preserve the established fast replay for a single lost response.
				continue
			}
			r.notify(err)
			delay := jitterDuration(exponentialBackoff(receiveBackoffBase, receiveBackoffMax, transientFailures-2))
			if !waitContext(rctx, delay) {
				err = rctx.Err()
				errAfterCancel = cancelled()
				break
			}
		}
		if err != nil {
			// Release ALL reserved slots and drop any returned messages (their locks expire
			// and redeliver — at-least-once safe): never leak capacity, even if a source ever
			// returned messages together with an error.
			releasePaired(handlerSlots, totalSlots, n)
			if rctx.Err() != nil && !isPermanent(err) {
				return done()
			}
			if !retryableManagedError(err) {
				r.notify(err)
				fail(err, errAfterCancel)
				return done()
			}
			// Retryable failures are retained inside the same-attempt loop above. This
			// fallback is reachable only if a source couples an error with cancellation.
			return done()
		}
		// A request can race cancellation and still return a decoded successful
		// response. Once shutdown or a fatal worker error has canceled rctx, do
		// not dispatch newly returned messages with an already-canceled context.
		if rctx.Err() != nil {
			releasePaired(handlerSlots, totalSlots, n)
			return done()
		}
		if len(msgs) > n {
			releasePaired(handlerSlots, totalSlots, n)
			err := fmt.Errorf("mqlite: broker returned %d messages for a receive capped at %d", len(msgs), n)
			afterCancel := cancelled()
			r.notify(err)
			fail(err, afterCancel)
			return done()
		}
		// A Peek-Lock response without a message or lease deadline is invalid on
		// every receive, including the first successful response. The local-clock
		// stale cutoff is intentionally narrower: only an attempt replay may have
		// spent its original lease waiting for lost responses. Applying it to a
		// first response would reject a legitimate near-expiry delivery.
		cutoff := time.Now().Add(replayLeaseSafety)
		live := msgs[:0]
		invalid := 0
		stale := 0
		for _, m := range msgs {
			if m == nil || m.LockedUntil.IsZero() {
				invalid++
				continue
			}
			if hadTransientFailure && !m.LockedUntil.After(cutoff) {
				stale++
				continue
			}
			live = append(live, m)
		}
		msgs = live
		if invalid > 0 {
			r.notify(fmt.Errorf("%w: discarded %d invalid peek-lock message(s) without a lease deadline", ErrLockLost, invalid))
		}
		if stale > 0 {
			r.notify(fmt.Errorf("%w: discarded %d stale message(s) from receive replay", ErrLockLost, stale))
		}
		// The observer runs synchronously and may cancel Run in response to a
		// filtered delivery. Re-check after the final callback so no surviving
		// message is dispatched with an already-canceled handler context.
		if rctx.Err() != nil {
			releasePaired(handlerSlots, totalSlots, n)
			return done()
		}
		// Release the slots we reserved but did not fill, then run a worker per message.
		releasePaired(handlerSlots, totalSlots, n-len(msgs))
		for _, m := range msgs {
			wg.Add(1)
			go func(m *Message) {
				defer wg.Done()
				defer func() { <-totalSlots }()
				var releaseHandlerOnce sync.Once
				releaseHandler := func() {
					releaseHandlerOnce.Do(func() { <-handlerSlots })
				}
				// A panic or future early return must not strand handler capacity.
				// On the normal path process calls this immediately after handler.
				defer releaseHandler()
				r.process(rctx, m, handler, releaseHandler, cancelled, fail)
			}(m)
		}
	}
}

func (r *Receiver) process(ctx context.Context, m *Message, handler func(context.Context, *Message) error, releaseHandler func(), cancelled func() bool, fail func(error, bool)) {
	handlerCtx, cancelHandler := context.WithCancel(ctx)
	defer cancelHandler()

	var cancelRenew context.CancelFunc
	var renewWG sync.WaitGroup
	var renewal renewalOutcome
	if r.cfg.autoRenew {
		renewCtx, cancel := context.WithCancel(ctx)
		cancelRenew = cancel
		state := newRenewalState(m.LockedUntil)
		if needsInitialRenewalGate(m.LockedUntil) {
			if renewCtx.Err() != nil {
				cancel()
				return
			}
			var confirmed bool
			state, confirmed = r.confirmInitialRenewal(renewCtx, m, state, cancelHandler, cancelled, fail)
			if !confirmed {
				cancel()
				return
			}
			// A custom transport can return a successful response after its request
			// context was canceled. The lease may be confirmed, but Run has already
			// stopped, so do not turn that late acknowledgement into a new dispatch.
			if renewCtx.Err() != nil {
				cancel()
				return
			}
		}
		renewWG.Add(1)
		go func() {
			defer renewWG.Done()
			renewal = r.renewLoop(renewCtx, m, state, cancelHandler, cancelled, fail)
		}()
	}
	var stopRenewOnce sync.Once
	stopRenew := func() {
		stopRenewOnce.Do(func() {
			if cancelRenew != nil {
				cancelRenew()
				renewWG.Wait()
			}
		})
	}
	// runtime.Goexit and panics do not return from handler to the statements
	// below, but they do run defers. Keep the total-inflight token held until a
	// renewal has been canceled and joined on every exit path.
	defer stopRenew()

	herr := handler(handlerCtx, m)
	// Handler concurrency is independent from renewal shutdown and settlement.
	// Releasing here lets the next claimed message start while this delivery's
	// terminal work continues under the separate total-inflight bound.
	releaseHandler()
	// A terminal settlement must never race the heartbeat for the same token.
	// Cancel and join it first; Wait also publishes renewal's outcome safely.
	stopRenew()
	if renewal.skipSettlement {
		return
	}

	// Settle on a fresh short-lived context so cleanup still happens during shutdown.
	// Exact request replays are safe: the broker's settlement receipt distinguishes
	// Complete from Abandon and includes Abandon's delay in the request identity.
	sctx, cancelSettle := context.WithTimeout(context.Background(), settleTimeout)
	defer cancelSettle()
	if herr != nil {
		err := r.retrySettlement(sctx, func(callCtx context.Context) error { return m.Abandon(callCtx) })
		r.settleErr(err, cancelled, fail)
		return
	}
	err := r.retrySettlement(sctx, func(callCtx context.Context) error { return m.Complete(callCtx) })
	r.settleErr(err, cancelled, fail)
}

func retryableManagedError(err error) bool {
	if err == nil ||
		errors.Is(err, context.Canceled) {
		return false
	}
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		if httpErr == nil {
			return false
		}
		status := httpErr.StatusCode
		return status == 408 || status == 429 || status >= 500
	}
	if errors.Is(err, ErrLockLost) ||
		errors.Is(err, ErrInvalidArgument) ||
		isPermanent(err) {
		return false
	}
	// Transport failures do not carry an HTTP response and are safe to replay for
	// fenced Renew and exact settlement requests. This includes DeadlineExceeded
	// from a custom http.Client.Timeout while the operation's own context is still
	// live; each retry loop separately checks that controlling context.
	return true
}

func (r *Receiver) retrySettlement(ctx context.Context, settle func(context.Context) error) error {
	var lastErr error
	for retry := 0; ; retry++ {
		lastErr = settle(ctx)
		if lastErr == nil || !retryableManagedError(lastErr) || ctx.Err() != nil {
			return lastErr
		}
		delay := jitterDuration(exponentialBackoff(settleRetryBase, settleRetryMax, retry))
		if !waitContext(ctx, delay) {
			return lastErr
		}
	}
}

// settleErr surfaces a final settle failure. ErrLockLost and exhausted transient
// failures are advisory; a permanent error also stops Run.
func (r *Receiver) settleErr(err error, cancelled func() bool, fail func(error, bool)) {
	if err == nil {
		return
	}
	// Ordering is sampled at OBSERVATION, before notify: the user's error handler may cancel the
	// context, and that later cancellation must not rewrite whether this error preceded it (codex).
	afterCancel := cancelled()
	r.notify(err)
	// Preserve the HTTP-status-first classification after retries are exhausted.
	// An intermediary's 503 body may claim permission_denied/not_found; that
	// untrusted sentinel must not turn a transient status into a fatal Run error.
	if isPermanent(err) && !retryableManagedError(err) {
		fail(err, afterCancel)
	}
}

type renewalOutcome struct {
	skipSettlement bool
}

type renewalState struct {
	deadline      time.Time
	leaseEstimate time.Duration
}

func newRenewalState(deadline time.Time) renewalState {
	now := time.Now()
	leaseEstimate := deadline.Sub(now)
	if deadline.IsZero() {
		leaseEstimate = renewFallbackLease
		deadline = now.Add(leaseEstimate)
	} else if leaseEstimate <= 0 {
		// A client clock ahead of the broker still gets one authoritative renewal
		// attempt. If only the legacy route is available, this is the conservative
		// estimate used to schedule the next heartbeat after that attempt succeeds.
		leaseEstimate = renewFallbackLease
	}
	return renewalState{deadline: deadline, leaseEstimate: leaseEstimate}
}

func needsInitialRenewalGate(deadline time.Time) bool {
	return deadline.IsZero() || !deadline.After(time.Now().Add(replayLeaseSafety))
}

func (r *Receiver) loseRenewal(cancelHandler context.CancelFunc, cause error) renewalOutcome {
	cancelHandler()
	err := cause
	if !errors.Is(err, ErrLockLost) {
		// Keep ErrLockLost as the sole semantic category. An intermediary can
		// return HTTP 503 with a misleading permission/not-found body; unwrapping
		// that body here would make an exhausted transient renewal look permanent.
		err = fmt.Errorf("%w: auto-renew could not be confirmed before the lease deadline: %s", ErrLockLost, cause.Error())
	}
	r.notify(err)
	return renewalOutcome{skipSettlement: true}
}

func (r *Receiver) stopRenewal(cancelHandler context.CancelFunc, err error, cancelled func() bool, fail func(error, bool)) renewalOutcome {
	cancelHandler()
	afterCancel := cancelled()
	r.notify(err)
	if isPermanent(err) {
		fail(err, afterCancel)
	}
	return renewalOutcome{skipSettlement: true}
}

// confirmInitialRenewal fences handler dispatch when the lease in a fresh
// Receive response is already inside the local safety window. Starting the
// handler and an immediate renew goroutine concurrently would let a fast handler
// win the race, perform side effects, and cancel the renewal before it was sent.
func (r *Receiver) confirmInitialRenewal(ctx context.Context, m *Message, state renewalState, cancelHandler context.CancelFunc, cancelled func() bool, fail func(error, bool)) (renewalState, bool) {
	newDeadline, err := r.renewMessage(ctx, m)
	if err != nil {
		if ctx.Err() != nil {
			return state, false
		}
		if !retryableManagedError(err) {
			if errors.Is(err, ErrLockLost) {
				r.loseRenewal(cancelHandler, err)
			} else {
				r.stopRenewal(cancelHandler, err, cancelled, fail)
			}
			return state, false
		}
		// There is no safe lease time left for backoff before dispatch. A retryable
		// failure is therefore an unconfirmed lock, just as it is when the normal
		// renewal loop exhausts its deadline.
		r.loseRenewal(cancelHandler, err)
		return state, false
	}

	now := time.Now()
	if !newDeadline.IsZero() {
		if !newDeadline.After(now.Add(replayLeaseSafety)) {
			r.loseRenewal(cancelHandler, fmt.Errorf("%w: renewed lease deadline was already unusable when received", ErrLockLost))
			return state, false
		}
		state.deadline = newDeadline
		state.leaseEstimate = newDeadline.Sub(now)
		return state, true
	}

	// An older broker's single-message Renew has no response deadline. Its
	// successful acknowledgement still gates dispatch; retain the existing
	// conservative lease estimate for subsequent scheduling.
	state.deadline = now.Add(state.leaseEstimate)
	return state, true
}

func autoRenewDelay(deadline time.Time) time.Duration {
	if deadline.IsZero() {
		return renewFallbackLease / 2
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return 0
	}
	return remaining / 2
}

func (r *Receiver) renewMessage(ctx context.Context, m *Message) (time.Time, error) {
	r.renewCapabilityMu.Lock()
	unsupported := r.deadlineRenewUnsupported
	r.renewCapabilityMu.Unlock()
	if source, ok := r.src.(deadlineRenewer); ok && !unsupported {
		deadline, err := source.renewDeadline(ctx, m.queue, m.SequenceNumber, m.lockToken)
		if !errors.Is(err, ErrUnsupported) {
			return deadline, err
		}
		// An older broker may not implement the deadline-bearing route. Fall back
		// to stable single-message Renew from now on, not once per heartbeat.
		r.renewCapabilityMu.Lock()
		r.deadlineRenewUnsupported = true
		r.renewCapabilityMu.Unlock()
	}
	return time.Time{}, m.Renew(ctx)
}

func (r *Receiver) renewLoop(ctx context.Context, m *Message, state renewalState, cancelHandler context.CancelFunc, cancelled func() bool, fail func(error, bool)) renewalOutcome {
	for {
		if !waitContext(ctx, autoRenewDelay(state.deadline)) {
			return renewalOutcome{}
		}
		for retry := 0; ; retry++ {
			newDeadline, err := r.renewMessage(ctx, m)
			if err == nil {
				now := time.Now()
				if !newDeadline.IsZero() && !newDeadline.After(now.Add(replayLeaseSafety)) {
					return r.loseRenewal(cancelHandler, fmt.Errorf("%w: renewed lease deadline was already unusable when received", ErrLockLost))
				}
				if !newDeadline.IsZero() {
					state.deadline = newDeadline
					state.leaseEstimate = newDeadline.Sub(now)
				} else {
					// Legacy Renew has no response deadline. A successful call starts a
					// fresh broker lease, so schedule conservatively from receipt time.
					state.deadline = now.Add(state.leaseEstimate)
				}
				break
			}
			if ctx.Err() != nil {
				return renewalOutcome{}
			}
			if !retryableManagedError(err) {
				if errors.Is(err, ErrLockLost) {
					return r.loseRenewal(cancelHandler, err)
				}
				return r.stopRenewal(cancelHandler, err, cancelled, fail)
			}

			remaining := time.Until(state.deadline) - replayLeaseSafety
			if remaining <= 0 {
				return r.loseRenewal(cancelHandler, err)
			}
			delay := jitterDuration(exponentialBackoff(renewRetryBase, renewRetryMax, retry))
			if delay >= remaining {
				// Leave another half-window for the request itself; sleeping to the
				// safety boundary creates a predictably late, usually futile retry.
				delay = remaining / 2
			}
			if delay <= 0 {
				return r.loseRenewal(cancelHandler, err)
			}
			if !waitContext(ctx, delay) {
				return renewalOutcome{}
			}
		}
	}
}

// Close releases receiver resources (no-op).
func (r *Receiver) Close() error { return nil }
