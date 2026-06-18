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
	GroupID        string
	CorrelationID  string
	ReplyTo        string
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
func (m *Message) Abandon(ctx context.Context, opts ...AbandonOpts) error {
	o := firstOpt(opts)
	return m.s.abandon(ctx, m.queue, m.SequenceNumber, m.lockToken, o.Delay.Milliseconds())
}

// Reject moves the message to the dead-letter state (optionally with a reason).
func (m *Message) Reject(ctx context.Context, opts ...RejectOpts) error {
	o := firstOpt(opts)
	return m.s.reject(ctx, m.queue, m.SequenceNumber, m.lockToken, o.Reason, o.Detail)
}

// Defer sets the message aside, to be retrieved later by SequenceNumber.
func (m *Message) Defer(ctx context.Context) error {
	return m.s.deferMsg(ctx, m.queue, m.SequenceNumber, m.lockToken)
}

// Renew extends the lock lease (for long-running handlers).
func (m *Message) Renew(ctx context.Context) error {
	return m.s.renew(ctx, m.queue, m.SequenceNumber, m.lockToken)
}
