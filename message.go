package mqlite

import (
	"context"
	"time"
)

// Message is a delivered message handle. The lock token is held internally and
// never exposed, so settlement can't be misrouted (§17.1).
type Message struct {
	SequenceNumber int64
	Body           []byte
	MessageID      string
	SessionID      string
	CorrelationID  string
	Subject        string
	ContentType    string
	Properties     map[string]string
	DeliveryCount  int
	EnqueuedAt     time.Time
	LockedUntil    time.Time

	queue     string
	lockToken string
	s         settler
}

// Complete removes a successfully-processed message.
func (m *Message) Complete(ctx context.Context) error {
	return m.s.complete(ctx, m.queue, m.SequenceNumber, m.lockToken)
}

// Abandon releases the lock for immediate redelivery (optionally after a delay).
func (m *Message) Abandon(ctx context.Context, opts ...AbandonOption) error {
	c := abandonConfig{}
	for _, o := range opts {
		o(&c)
	}
	return m.s.abandon(ctx, m.queue, m.SequenceNumber, m.lockToken, c.delay.Milliseconds())
}

// DeadLetter moves the message to the dead-letter state with a reason.
func (m *Message) DeadLetter(ctx context.Context, reason, desc string) error {
	return m.s.deadLetter(ctx, m.queue, m.SequenceNumber, m.lockToken, reason, desc)
}

// Defer sets the message aside, to be retrieved later by SequenceNumber.
func (m *Message) Defer(ctx context.Context) error {
	return m.s.deferMsg(ctx, m.queue, m.SequenceNumber, m.lockToken)
}

// RenewLock extends the lock lease (for long-running handlers).
func (m *Message) RenewLock(ctx context.Context) error {
	return m.s.renewLock(ctx, m.queue, m.SequenceNumber, m.lockToken)
}

// LockToken exposes the fencing token (rarely needed; settlement is preferred).
func (m *Message) LockToken() string { return m.lockToken }

type abandonConfig struct{ delay time.Duration }

// AbandonOption configures Abandon.
type AbandonOption func(*abandonConfig)

// WithDelay re-hides an abandoned message for d before redelivery (backoff).
func WithDelay(d time.Duration) AbandonOption { return func(c *abandonConfig) { c.delay = d } }
