package mqlite

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
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

type receiverConfig struct {
	autoRenew   bool
	concurrency int
	prefetch    int
	onError     func(error)
}

// ReceiverOption configures a Receiver.
type ReceiverOption func(*receiverConfig)

// WithAutoRenew keeps locks alive with a background heartbeat for long handlers.
func WithAutoRenew() ReceiverOption { return func(c *receiverConfig) { c.autoRenew = true } }

// WithConcurrency sets how many messages are processed in parallel.
func WithConcurrency(n int) ReceiverOption { return func(c *receiverConfig) { c.concurrency = n } }

// WithPrefetch caps how many messages are claimed per receive. It is clamped to the
// concurrency: the Receiver never claims more than it has idle workers for, so a claimed
// message always has a worker (and, with WithAutoRenew, a renew heartbeat) ready — it can't
// sit locked behind a busy worker and let its lock expire (MQLITE-76). Values above the
// concurrency therefore have no extra effect; use it only to claim in smaller chunks.
func WithPrefetch(n int) ReceiverOption { return func(c *receiverConfig) { c.prefetch = n } }

// WithErrorHandler registers a callback for errors the receive loop would otherwise swallow:
// a transient receive error being retried, or a per-message settle/renew failure (e.g. a
// lost lock). It is advisory — for logging or metrics — and must not block. Errors that mean
// the consumer is misconfigured or the source is gone (bad token, missing queue, engine
// closed) are instead returned from Run (MQLITE-77); those are also passed here first.
func WithErrorHandler(fn func(error)) ReceiverOption {
	return func(c *receiverConfig) { c.onError = fn }
}

// Receiver is a stateful receive loop. handler returning nil auto-Completes;
// returning error auto-Abandons (§17.1).
type Receiver struct {
	src   receiveSource
	queue string
	cfg   receiverConfig
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
	if r.cfg.onError != nil && err != nil {
		r.cfg.onError(err)
	}
}

// isPermanent reports whether an error will not be fixed by retrying and means the consumer
// is misconfigured (bad token, missing queue) or the source is gone (engine closed): the
// receive loop stops and Run returns it, instead of spinning forever. Transient errors
// (network, 5xx, timeouts) and an expected ErrLockLost are not permanent.
func isPermanent(err error) bool {
	return errors.Is(err, ErrUnauthenticated) ||
		errors.Is(err, ErrNotFound) ||
		errors.Is(err, ErrQueueNotFound) ||
		errors.Is(err, engine.ErrClosed)
}

// reserve blocks until at least one worker slot is free (ctx-aware), then greedily takes up
// to max free slots without blocking. It returns how many it took (0 only if ctx is
// cancelled first). Reserving before claiming is what bounds claims to idle capacity.
func reserve(ctx context.Context, sem chan struct{}, max int) int {
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return 0
	}
	n := 1
	for n < max {
		select {
		case sem <- struct{}{}:
			n++
		default:
			return n
		}
	}
	return n
}

// Run pulls and processes messages until ctx is canceled or a permanent error occurs.
// It returns ctx.Err() on cancellation, or the first permanent receive/settle/renew error.
func (r *Receiver) Run(ctx context.Context, handler func(context.Context, *Message) error) error {
	batch := r.cfg.prefetch
	if batch <= 0 || batch > r.cfg.concurrency {
		batch = r.cfg.concurrency
	}
	sem := make(chan struct{}, r.cfg.concurrency)
	fatal := make(chan error, 1)
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup

	// fail records the first permanent error and stops the loop + all workers.
	fail := func(err error) {
		select {
		case fatal <- err:
		default:
		}
		cancel()
	}
	// done drains in-flight workers and returns the fatal error if one was recorded, else
	// the context error (normal cancellation).
	done := func() error {
		wg.Wait()
		select {
		case err := <-fatal:
			return err
		default:
			return ctx.Err()
		}
	}

	for {
		if rctx.Err() != nil {
			return done()
		}
		// Reserve worker capacity BEFORE claiming, and claim no more than reserved: every
		// claimed message immediately gets a worker (and auto-renew), so it can never burn
		// its lock queued behind a busy worker (MQLITE-76).
		n := reserve(rctx, sem, batch)
		if n == 0 {
			return done()
		}
		// Same attempt id across the one transient retry: if the broker already claimed and
		// recorded this batch (only the response was lost), the retry replays it instead of
		// claiming new messages.
		attemptID := newAttemptID()
		msgs, err := r.src.receiveOne(rctx, r.queue, n, 20000, engine.PeekLock, attemptID) // 20s long-poll
		if err != nil && rctx.Err() == nil {
			msgs, err = r.src.receiveOne(rctx, r.queue, n, 20000, engine.PeekLock, attemptID)
		}
		// Release the slots we reserved but did not fill.
		for i := len(msgs); i < n; i++ {
			<-sem
		}
		if err != nil {
			if rctx.Err() != nil {
				return done()
			}
			r.notify(err)
			if isPermanent(err) {
				fail(err)
				return done()
			}
			select {
			case <-rctx.Done():
				return done()
			case <-time.After(500 * time.Millisecond): // transient backoff
			}
			continue
		}
		for _, m := range msgs {
			wg.Add(1)
			go func(m *Message) {
				defer wg.Done()
				defer func() { <-sem }()
				r.process(rctx, m, handler, fail)
			}(m)
		}
	}
}

func (r *Receiver) process(ctx context.Context, m *Message, handler func(context.Context, *Message) error, fail func(error)) {
	if r.cfg.autoRenew {
		rctx, cancel := context.WithCancel(ctx)
		defer cancel()
		go r.renewLoop(rctx, m, fail)
	}
	herr := handler(ctx, m)

	// Settle on a fresh short-lived context so cleanup still happens during shutdown.
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if herr != nil {
		r.settleErr(m.Abandon(sctx), fail)
		return
	}
	r.settleErr(m.Complete(sctx), fail)
}

// settleErr surfaces a settle/renew failure: an expected ErrLockLost (the message was
// redelivered) or a transient error goes to the observer only; a permanent one (bad token,
// missing queue) is also fatal and stops Run.
func (r *Receiver) settleErr(err error, fail func(error)) {
	if err == nil {
		return
	}
	r.notify(err)
	if isPermanent(err) {
		fail(err)
	}
}

func (r *Receiver) renewLoop(ctx context.Context, m *Message, fail func(error)) {
	interval := 10 * time.Second
	if d := time.Until(m.LockedUntil); d > 0 {
		interval = d / 2
	}
	if interval < time.Second {
		interval = time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := m.Renew(ctx); err != nil {
				if ctx.Err() == nil { // ignore shutdown; surface a real renew failure
					r.settleErr(err, fail)
				}
				return
			}
		}
	}
}

// Close releases receiver resources (no-op).
func (r *Receiver) Close() error { return nil }
