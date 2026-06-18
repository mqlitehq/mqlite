package mqlite

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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
}

// ReceiverOption configures a Receiver.
type ReceiverOption func(*receiverConfig)

// WithAutoRenew keeps locks alive with a background heartbeat for long handlers.
func WithAutoRenew() ReceiverOption { return func(c *receiverConfig) { c.autoRenew = true } }

// WithConcurrency sets how many messages are processed in parallel.
func WithConcurrency(n int) ReceiverOption { return func(c *receiverConfig) { c.concurrency = n } }

// WithPrefetch sets how many messages to claim per receive (defaults to concurrency).
func WithPrefetch(n int) ReceiverOption { return func(c *receiverConfig) { c.prefetch = n } }

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

// Run pulls and processes messages until ctx is canceled.
func (r *Receiver) Run(ctx context.Context, handler func(context.Context, *Message) error) error {
	batch := r.cfg.prefetch
	if batch <= 0 {
		batch = r.cfg.concurrency
	}
	sem := make(chan struct{}, r.cfg.concurrency)
	var wg sync.WaitGroup
	for {
		if ctx.Err() != nil {
			wg.Wait()
			return ctx.Err()
		}
		// Fresh attempt id per round drives idempotent receive: on a transient error
		// we retry ONCE with the SAME id, so if the broker already claimed+recorded
		// this batch (only the response was lost) the retry replays it instead of
		// claiming new messages — no double-delivery / burned delivery_count. Retrying
		// on any error is safe: the attempt id makes it a no-op replay when the first
		// claim already landed, and a fresh claim when it never did.
		attemptID := newAttemptID()
		msgs, err := r.src.receiveOne(ctx, r.queue, batch, 20000, engine.PeekLock, attemptID) // 20s long-poll
		if err != nil && ctx.Err() == nil {
			msgs, err = r.src.receiveOne(ctx, r.queue, batch, 20000, engine.PeekLock, attemptID)
		}
		if err != nil {
			if ctx.Err() != nil {
				wg.Wait()
				return ctx.Err()
			}
			select {
			case <-ctx.Done():
				wg.Wait()
				return ctx.Err()
			case <-time.After(500 * time.Millisecond): // transient backoff
			}
			continue
		}
		for _, m := range msgs {
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				wg.Wait()
				return ctx.Err()
			}
			wg.Add(1)
			go func(m *Message) {
				defer wg.Done()
				defer func() { <-sem }()
				r.process(ctx, m, handler)
			}(m)
		}
	}
}

func (r *Receiver) process(ctx context.Context, m *Message, handler func(context.Context, *Message) error) {
	if r.cfg.autoRenew {
		rctx, cancel := context.WithCancel(ctx)
		defer cancel()
		go r.renewLoop(rctx, m)
	}
	herr := handler(ctx, m)

	// Settle on a fresh short-lived context so cleanup still happens during shutdown.
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if herr != nil {
		_ = m.Abandon(sctx)
		return
	}
	_ = m.Complete(sctx)
}

func (r *Receiver) renewLoop(ctx context.Context, m *Message) {
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
				return
			}
		}
	}
}

// Close releases receiver resources (no-op).
func (r *Receiver) Close() error { return nil }
