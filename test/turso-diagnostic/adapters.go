//go:build mqlite_perf_diag

package main

import (
	"context"
	"net/http"
	"time"

	mq "github.com/mqlitehq/mqlite"
	low "github.com/mqlitehq/mqlite-go"
)

// Low-level profiles retain explicit settlement. Mixed mode uses the real
// managed Receiver; the harness never manually settles its deliveries.
type adapter struct {
	name   string
	native *mq.Client
	low    *low.Client
}

type delivery struct {
	Sequence                                                   int64
	Body                                                       []byte
	ID, Group, Correlation, Reply, Subject, ContentType, Token string
	Properties                                                 map[string]string
	Count                                                      int
	Enqueued, LockedUntil                                      time.Time
}

func openAdapter(ctx context.Context, name, endpoint string, client *http.Client) (*adapter, error) {
	a := &adapter{name: name}
	var err error
	if name == "native" {
		a.native, err = mq.Open(ctx, endpoint, mq.WithHTTPClient(client))
	} else {
		a.low, err = low.Open(ctx, endpoint, low.WithHTTPClient(client))
	}
	return a, err
}
func (a *adapter) close() {
	if a.native != nil {
		_ = a.native.Close()
	}
	if a.low != nil {
		_ = a.low.Close()
	}
}
func (a *adapter) create(ctx context.Context, q string, dedup time.Duration) error {
	return a.createWithLock(ctx, q, dedup, 10*time.Minute)
}
func (a *adapter) createWithLock(ctx context.Context, q string, dedup, lock time.Duration) error {
	if a.native != nil {
		return a.native.CreateQueue(ctx, q, mq.QueueConfig{LockDuration: lock, MaxDeliveryCount: 10, DedupWindow: dedup})
	}
	return a.low.CreateQueue(ctx, q, low.QueueConfig{LockDuration: lock, MaxDeliveryCount: 10, DedupWindow: dedup})
}
func (a *adapter) send(ctx context.Context, q string, ms []mq.OutMessage) ([]int64, error) {
	if a.native != nil {
		return a.native.Send(ctx, q, ms...)
	}
	ls := make([]low.OutMessage, len(ms))
	for i, m := range ms {
		ls[i] = low.OutMessage{Body: m.Body, MessageID: m.MessageID, GroupID: m.GroupID, CorrelationID: m.CorrelationID, ReplyTo: m.ReplyTo, Subject: m.Subject, ContentType: m.ContentType, Properties: m.Properties, TTL: m.TTL}
	}
	return a.low.Send(ctx, q, ls...)
}
func (a *adapter) receive(ctx context.Context, q string, max int, attempt string) ([]*delivery, error) {
	var out []*delivery
	if a.native != nil {
		ms, err := a.native.Receive(ctx, q, mq.RecvOpts{Max: max, Attempt: attempt})
		for _, m := range ms {
			out = append(out, &delivery{m.SequenceNumber, m.Body, m.MessageID, m.GroupID, m.CorrelationID, m.ReplyTo, m.Subject, m.ContentType, m.LockToken(), m.Properties, m.DeliveryCount, m.EnqueuedAt, m.LockedUntil})
		}
		return out, err
	}
	ms, err := a.low.Receive(ctx, q, low.RecvOpts{Max: max, Attempt: attempt})
	for _, m := range ms {
		out = append(out, &delivery{m.SequenceNumber, m.Body, m.MessageID, m.GroupID, m.CorrelationID, m.ReplyTo, m.Subject, m.ContentType, m.LockToken(), m.Properties, m.DeliveryCount, m.EnqueuedAt, m.LockedUntil})
	}
	return out, err
}
func (a *adapter) complete(ctx context.Context, q string, ms []*delivery) ([]bool, error) {
	mask := make([]bool, len(ms))
	if a.native != nil {
		items := make([]*mq.Message, len(ms))
		for i, m := range ms {
			items[i] = a.native.Message(q, m.Sequence, m.Token)
		}
		rs, err := a.native.CompleteBatch(ctx, q, items...)
		if err != nil {
			return mask, err
		}
		if len(rs) != len(ms) {
			return mask, oracle("complete_count")
		}
		for i, r := range rs {
			if r.SequenceNumber != ms[i].Sequence {
				return mask, oracle("complete_sequence")
			}
			mask[i] = r.Ok
		}
		return mask, nil
	}
	items := make([]*low.Message, len(ms))
	for i, m := range ms {
		items[i] = a.low.Message(q, m.Sequence, m.Token)
	}
	rs, err := a.low.CompleteBatch(ctx, q, items...)
	if err != nil {
		return mask, err
	}
	if len(rs) != len(ms) {
		return mask, oracle("complete_count")
	}
	for i, r := range rs {
		if r.SequenceNumber != ms[i].Sequence {
			return mask, oracle("complete_sequence")
		}
		mask[i] = r.Ok
	}
	return mask, nil
}
func (a *adapter) stats(ctx context.Context, q string) (mq.Metrics, error) {
	if a.native != nil {
		return a.native.Stats(ctx, q)
	}
	s, e := a.low.Stats(ctx, q)
	return mq.Metrics{Queue: s.Queue, Active: s.Active, Locked: s.Locked, Deferred: s.Deferred, Scheduled: s.Scheduled, DeadLettered: s.DeadLettered, Total: s.Total}, e
}
func (a *adapter) empty(ctx context.Context, q string) error {
	if a.native != nil {
		rs, e := a.native.Peek(ctx, q, mq.PeekOpts{Max: 1})
		if e != nil {
			return e
		}
		if len(rs) != 0 {
			return oracle("retained_messages")
		}
	} else {
		rs, e := a.low.Peek(ctx, q, low.PeekOpts{Max: 1})
		if e != nil {
			return e
		}
		if len(rs) != 0 {
			return oracle("retained_messages")
		}
	}
	s, e := a.stats(ctx, q)
	if e != nil {
		return e
	}
	if s.Total != 0 || s.Active != 0 || s.Locked != 0 || s.Scheduled != 0 || s.Deferred != 0 || s.DeadLettered != 0 {
		return oracle("nonempty_stats")
	}
	return nil
}

func (a *adapter) runReceiver(ctx context.Context, q string, handler func(context.Context, *delivery) error, onError func(error)) error {
	if a.native != nil {
		return a.native.Receiver(q, mq.WithAutoRenew(), mq.WithConcurrency(4), mq.WithErrorHandler(onError)).Run(ctx, func(call context.Context, m *mq.Message) error {
			return handler(call, &delivery{m.SequenceNumber, m.Body, m.MessageID, m.GroupID, m.CorrelationID, m.ReplyTo, m.Subject, m.ContentType, m.LockToken(), m.Properties, m.DeliveryCount, m.EnqueuedAt, m.LockedUntil})
		})
	}
	return a.low.Receiver(q, low.WithAutoRenew(), low.WithConcurrency(4), low.WithErrorHandler(onError)).Run(ctx, func(call context.Context, m *low.Message) error {
		return handler(call, &delivery{m.SequenceNumber, m.Body, m.MessageID, m.GroupID, m.CorrelationID, m.ReplyTo, m.Subject, m.ContentType, m.LockToken(), m.Properties, m.DeliveryCount, m.EnqueuedAt, m.LockedUntil})
	})
}
