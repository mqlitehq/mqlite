package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	mq "github.com/mqlitehq/mqlite"
	"github.com/mqlitehq/mqlite/engine"
)

type queueAPI interface {
	SendOne(context.Context, string, mq.OutMessage, ...mq.SendOpts) (int64, error)
	Receive(context.Context, string, ...mq.RecvOpts) ([]*mq.Message, error)
	Peek(context.Context, string, ...mq.PeekOpts) ([]*mq.PeekedMessage, error)
	Stats(context.Context, string) (mq.Metrics, error)
	ListQueues(context.Context) ([]mq.QueueInfo, error)
	CompleteBatch(context.Context, string, ...*mq.Message) ([]mq.SettleResult, error)
	RenewBatch(context.Context, string, ...*mq.Message) ([]mq.SettleResult, error)
	Redrive(context.Context, string, ...mq.RedriveOpts) (int, error)
	Purge(context.Context, string, ...mq.PurgeOpts) (int, error)
}

type ownership struct {
	sequence           int64
	count              int
	token              string
	enqueued, deadline time.Time
}

type batch struct {
	plan                      plan
	ledger                    *batchLedger
	api                       queueAPI
	embedded                  *mq.Embedded
	acked                     map[string]bool
	owners                    map[string]ownership
	sequences                 map[string]string
	reset                     map[string]bool
	done                      map[string]bool
	hits                      map[string]bool
	sent, deliveries, replays uint64
}

func (r *runner) runLane(ctx context.Context, lane int) error {
	for n := uint64(0); time.Now().Before(r.stopAt); n++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		started := time.Now()
		p := makePlan(r.seed, lane, n, started.UnixMilli())
		ledger, err := beginBatch(r.cfg.Evidence, p)
		if err != nil {
			return err
		}
		b := &batch{plan: p, ledger: ledger, api: r.client, embedded: r.outbox,
			acked: map[string]bool{}, owners: map[string]ownership{}, sequences: map[string]string{},
			reset: map[string]bool{}, done: map[string]bool{}, hits: map[string]bool{}}
		if lane == 7 {
			b.api = r.outbox
		}
		err = func() (err error) {
			defer func() { err = join(err, ledger.close()) }()
			batchCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
			defer cancel()
			if err := b.perform(batchCtx); err != nil {
				return err
			}
			return b.reconcile(batchCtx)
		}()
		if err != nil {
			return err
		}
		if err := r.verified(b, started); err != nil {
			return err
		}
		if err := ledger.remove(); err != nil {
			return err
		}
		next := started.Add(r.cfg.Interval)
		if next.After(r.stopAt) {
			next = r.stopAt
		}
		if err := waitUntil(ctx, next); err != nil {
			return err
		}
	}
	return nil
}

func waitUntil(ctx context.Context, until time.Time) error {
	if remaining := time.Until(until); remaining > 0 {
		timer := time.NewTimer(remaining)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	return ctx.Err()
}

func key(q, id string) string { return q + "\x00" + id }

func (b *batch) note(op, outcome, q, id string, seq int64) error {
	return b.ledger.ack.add(map[string]any{"operation": op, "outcome": outcome, "queue": q,
		"id": id, "sequence": seq, "at_ms": time.Now().UnixMilli()})
}

func (b *batch) send(ctx context.Context, i int, replay bool) error {
	m := b.plan.Inputs[i]
	q := m.Targets[0]
	if b.plan.Lane == 6 {
		q = queue(b.plan.Seed, "events")
	}
	op := "send"
	if replay {
		op = "send-replay"
	}
	if err := b.note(op, "requested", q, m.ID, 0); err != nil {
		return err
	}
	var opts mq.SendOpts
	if m.ScheduledAt != 0 {
		opts.At = time.UnixMilli(m.ScheduledAt)
	}
	seq, err := b.api.SendOne(ctx, q, m.out(), opts)
	if err != nil {
		return join(err, b.note(op, "error", q, m.ID, 0))
	}
	if seq <= 0 {
		return fmt.Errorf("send accepted invalid sequence for %s: %d", m.ID, seq)
	}
	if err := b.note(op, "accepted", q, m.ID, seq); err != nil {
		return err
	}
	if b.plan.Lane != 6 {
		k := key(q, m.ID)
		if old, ok := b.owners[k]; ok && old.sequence != seq {
			return errors.New("dedup send replay changed sequence")
		}
		if !replay {
			b.owners[k] = ownership{sequence: seq}
		}
	}
	if !replay {
		if b.acked[m.ID] {
			return errors.New("unplanned duplicate send acknowledgement")
		}
		b.acked[m.ID] = true
		b.sent++
	}
	return nil
}

func (b *batch) sendAll(ctx context.Context) error {
	for i := range b.plan.Inputs {
		if err := b.send(ctx, i, false); err != nil {
			return err
		}
	}
	return nil
}

func (b *batch) observe(q, kind string, m *mq.Message) error {
	return b.ledger.observed.add(map[string]any{"kind": kind, "queue": q, "id": m.MessageID,
		"sequence": m.SequenceNumber, "body_sha256": sumBytes(m.Body), "body_bytes": len(m.Body),
		"group": m.GroupID, "subject": m.Subject, "correlation": m.CorrelationID, "reply": m.ReplyTo,
		"content_type": m.ContentType, "properties": m.Properties, "delivery_count": m.DeliveryCount,
		"token": m.LockToken(), "enqueued_at": m.EnqueuedAt, "locked_until": m.LockedUntil, "at_ms": time.Now().UnixMilli()})
}

func (b *batch) receive(ctx context.Context, q string, indices []int, count int, opts mq.RecvOpts, replay bool) ([]*mq.Message, error) {
	opts.Max = len(indices)
	expected := map[string]int{}
	for _, i := range indices {
		expected[b.plan.Inputs[i].ID] = i
	}
	got := map[string]*mq.Message{}
	for len(got) < len(indices) {
		rows, err := b.api.Receive(ctx, q, opts)
		if err != nil {
			return nil, err
		}
		for _, m := range rows {
			kind := "fresh"
			if replay {
				kind = "attempt-replay"
			} else if opts.AtMostOnce {
				kind = "receive-delete"
			}
			if err := b.observe(q, kind, m); err != nil {
				return nil, err
			}
			i, ok := expected[m.MessageID]
			if !ok {
				return nil, fmt.Errorf("unexpected/late identity in %s: %s", q, m.MessageID)
			}
			if got[m.MessageID] != nil {
				return nil, fmt.Errorf("duplicate identity in one receive stage: %s", m.MessageID)
			}
			if err := checkContent(m, b.plan.Inputs[i]); err != nil {
				return nil, err
			}
			if m.SequenceNumber <= 0 || m.DeliveryCount != count {
				return nil, fmt.Errorf("wrong sequence/delivery count for %s: %d/%d want count %d", m.MessageID, m.SequenceNumber, m.DeliveryCount, count)
			}
			if m.EnqueuedAt.IsZero() || m.EnqueuedAt.UnixMilli() < b.plan.CreatedAt || m.EnqueuedAt.After(time.Now()) {
				return nil, errors.New("delivery enqueue timestamp is outside its send/observation interval")
			}
			if opts.AtMostOnce != (m.LockToken() == "") {
				return nil, errors.New("receive mode has wrong token presence")
			}
			if opts.AtMostOnce && !m.LockedUntil.IsZero() {
				return nil, errors.New("receive-delete returned a lease")
			}
			if !opts.AtMostOnce && !replay && !m.LockedUntil.After(time.Now()) {
				return nil, errors.New("fresh delivery returned an expired lease")
			}
			k := key(q, m.MessageID)
			old := b.owners[k]
			if old.sequence != 0 && old.sequence != m.SequenceNumber {
				return nil, errors.New("message sequence changed")
			}
			seqKey := fmt.Sprintf("%s/%d", q, m.SequenceNumber)
			if id, exists := b.sequences[seqKey]; exists && id != m.MessageID {
				return nil, errors.New("one sequence identifies multiple logical messages")
			}
			b.sequences[seqKey] = m.MessageID
			if replay {
				if old.count == 0 || old.count != count || old.token != m.LockToken() || !old.deadline.Equal(m.LockedUntil) || !old.enqueued.Equal(m.EnqueuedAt) {
					return nil, errors.New("attempt replay changed ownership/content timing")
				}
				b.replays++
			} else {
				if old.count != 0 && (old.token == m.LockToken() || (!b.reset[k] && count != old.count+1)) {
					return nil, errors.New("fresh redelivery did not increment/fence ownership")
				}
				if old.count != 0 && !old.enqueued.Equal(m.EnqueuedAt) {
					return nil, errors.New("redelivery changed enqueue timestamp")
				}
				b.reset[k] = false
				b.deliveries++
				b.owners[k] = ownership{m.SequenceNumber, count, m.LockToken(), m.EnqueuedAt, m.LockedUntil}
				if opts.AtMostOnce {
					if err := b.terminal(q, m.MessageID); err != nil {
						return nil, err
					}
				}
			}
			got[m.MessageID] = m
		}
		if len(got) < len(indices) {
			// An attempt replays its original result forever; a partial result is
			// already a failed contract, not a reason to collect duplicates.
			if opts.Attempt != "" && len(rows) > 0 {
				return nil, errors.New("receive attempt returned an incomplete expected batch")
			}
			if err := waitUntil(ctx, time.Now().Add(100*time.Millisecond)); err != nil {
				return nil, err
			}
		}
	}
	ordered := make([]*mq.Message, len(indices))
	for pos, i := range indices {
		ordered[pos] = got[b.plan.Inputs[i].ID]
	}
	return ordered, nil
}

func (b *batch) one(ctx context.Context, q string, i, count int, opts mq.RecvOpts) (*mq.Message, error) {
	rows, err := b.receive(ctx, q, []int{i}, count, opts, false)
	if err != nil {
		return nil, err
	}
	return rows[0], nil
}

func (b *batch) terminal(q, id string) error {
	k := key(q, id)
	if b.done[k] {
		return fmt.Errorf("duplicate terminal outcome for %s", id)
	}
	b.done[k] = true
	return nil
}

func (b *batch) settle(ctx context.Context, q string, m *mq.Message, op string, action func() error, terminal bool) error {
	if err := b.note(op, "requested", q, m.MessageID, m.SequenceNumber); err != nil {
		return err
	}
	if err := action(); err != nil {
		return join(err, b.note(op, "error", q, m.MessageID, m.SequenceNumber))
	}
	if err := b.note(op, "accepted", q, m.MessageID, m.SequenceNumber); err != nil {
		return err
	}
	if terminal {
		return b.terminal(q, m.MessageID)
	}
	return ctx.Err()
}

func (b *batch) complete(ctx context.Context, q string, m *mq.Message) error {
	return b.settle(ctx, q, m, "complete", func() error { return m.Complete(ctx) }, true)
}

func (b *batch) expectError(op, q string, m *mq.Message, actual, expected error) error {
	if actual == nil {
		return join(fmt.Errorf("%s unexpectedly succeeded", op), b.note(op, "unexpected-success", q, m.MessageID, m.SequenceNumber))
	}
	if !errors.Is(actual, expected) {
		return join(fmt.Errorf("%s unexpected error: %w", op, actual), b.note(op, "unexpected-error", q, m.MessageID, m.SequenceNumber))
	}
	return b.note(op, "expected-rejection", q, m.MessageID, m.SequenceNumber)
}

func (b *batch) excluded(ctx context.Context, q string, opts mq.RecvOpts) error {
	rows, err := b.api.Receive(ctx, q, opts)
	if err != nil {
		return err
	}
	for _, m := range rows {
		if err := b.observe(q, "unexpected-excluded-delivery", m); err != nil {
			return err
		}
	}
	if len(rows) != 0 {
		return fmt.Errorf("%s delivered excluded/head-blocked messages", q)
	}
	return b.ledger.observed.add(map[string]any{"kind": "excluded", "queue": q, "at_ms": time.Now().UnixMilli()})
}

func emptyStore(ctx context.Context, api queueAPI, q string) error {
	rows, err := api.Peek(ctx, q, mq.PeekOpts{Max: 100})
	if err != nil {
		return err
	}
	if len(rows) != 0 {
		return fmt.Errorf("%s has extra/pending identity %s body_sha256=%s state=%s", q, rows[0].MessageID, sumBytes(rows[0].Body), rows[0].State)
	}
	s, err := api.Stats(ctx, q)
	if err != nil {
		return err
	}
	if s.Total != 0 || s.Active != 0 || s.Locked != 0 || s.Scheduled != 0 || s.Deferred != 0 || s.DeadLettered != 0 {
		return fmt.Errorf("%s has nonzero final Stats: %+v", q, s)
	}
	return nil
}

func (b *batch) reconcile(ctx context.Context) error {
	targets := map[string]bool{}
	expectedTerminals, expectedAcks := 0, 0
	for _, m := range b.plan.Inputs {
		if m.Outcome != "rollback" {
			expectedAcks++
		}
		if m.Outcome != "rollback" && !b.acked[m.ID] {
			return fmt.Errorf("missing send acknowledgement: %s", m.ID)
		}
		if m.Outcome == "rollback" && b.acked[m.ID] {
			return errors.New("rolled-back send was acknowledged")
		}
		for _, q := range m.Targets {
			expectedTerminals++
			targets[q] = true
			if !b.done[key(q, m.ID)] {
				return fmt.Errorf("missing verified terminal identity: %s/%s", q, m.ID)
			}
		}
	}
	if len(b.acked) != expectedAcks {
		return errors.New("extra send acknowledgement identities")
	}
	if len(b.done) != expectedTerminals {
		return errors.New("extra terminal identities")
	}
	for _, q := range sortedKeys(targets) {
		if err := emptyStore(ctx, b.api, q); err != nil {
			return err
		}
	}
	return nil
}

func (b *batch) perform(ctx context.Context) error {
	if b.plan.Lane == 7 {
		return b.outboxLane(ctx)
	}
	if err := b.sendAll(ctx); err != nil {
		return err
	}
	q := queue(b.plan.Seed, laneNames[b.plan.Lane])
	switch b.plan.Lane {
	case 0:
		return b.ordinary(ctx, q)
	case 1:
		return b.group(ctx, q)
	case 2:
		return b.strict(ctx, q)
	case 3:
		return b.retry(ctx, q)
	case 4:
		return b.scheduled(ctx, q)
	case 5:
		return b.deferred(ctx, q)
	case 6:
		return b.topics(ctx)
	}
	return errors.New("unknown lane")
}

func (b *batch) ordinary(ctx context.Context, q string) error {
	if err := b.send(ctx, 0, true); err != nil {
		return err
	}
	b.hits["dedup-replay"] = true
	opts := mq.RecvOpts{Attempt: fmt.Sprintf("%s/ordinary/%d", b.plan.Seed, b.plan.Batch), AtMostOnce: b.plan.Batch%3 == 2}
	indices := []int{0, 1, 2, 3}
	msgs, err := b.receive(ctx, q, indices, 1, opts, false)
	if err != nil {
		return err
	}
	if _, err := b.receive(ctx, q, indices, 1, opts, true); err != nil {
		return err
	}
	b.hits["attempt-replay"] = true
	if opts.AtMostOnce {
		b.hits["receive-delete"] = true
		return nil
	}
	for _, m := range msgs {
		if err := b.settle(ctx, q, m, "renew", func() error { return m.Renew(ctx) }, false); err != nil {
			return err
		}
	}
	// Inspect actual stored deadlines: single Renew does not mutate the SDK handle.
	before, err := b.api.Peek(ctx, q, mq.PeekOpts{State: mq.Locked, Max: len(msgs)})
	if err != nil || len(before) != len(msgs) {
		return fmt.Errorf("renewed batch missing from Peek: %w", err)
	}
	deadlines := map[int64]time.Time{}
	for _, m := range before {
		deadlines[m.SequenceNumber] = m.LockedUntil
	}
	for _, m := range msgs {
		deadline := deadlines[m.SequenceNumber]
		if deadline.Before(m.LockedUntil) || !deadline.After(time.Now()) {
			return errors.New("single Renew shortened or failed to retain a live lease")
		}
	}
	b.hits["renew"] = true
	if err := b.note("renew-batch", "requested", q, "", 0); err != nil {
		return err
	}
	renewed, err := b.api.RenewBatch(ctx, q, msgs...)
	if err != nil || len(renewed) != len(msgs) {
		return fmt.Errorf("RenewBatch result missing: %w", err)
	}
	for i, result := range renewed {
		if !result.Ok || result.SequenceNumber != msgs[i].SequenceNumber ||
			result.LockedUntil.Before(deadlines[result.SequenceNumber]) || !result.LockedUntil.After(time.Now()) {
			return errors.New("RenewBatch changed identity, shortened or returned an expired lease")
		}
		if err := b.note("renew-batch", "accepted", q, msgs[i].MessageID, result.SequenceNumber); err != nil {
			return err
		}
	}
	b.hits["renew-batch"] = true
	if b.plan.Batch%3 == 0 {
		for replay := 0; replay < 2; replay++ {
			if err := b.note("complete-batch", "requested", q, "", 0); err != nil {
				return err
			}
			results, err := b.api.CompleteBatch(ctx, q, msgs...)
			if err != nil {
				return err
			}
			if len(results) != len(msgs) {
				return errors.New("CompleteBatch result length mismatch")
			}
			for i, result := range results {
				if !result.Ok || result.SequenceNumber != msgs[i].SequenceNumber {
					return errors.New("CompleteBatch result identity/success mismatch")
				}
				if err := b.note("complete-batch", "accepted", q, msgs[i].MessageID, result.SequenceNumber); err != nil {
					return err
				}
				if replay == 0 {
					if err := b.terminal(q, msgs[i].MessageID); err != nil {
						return err
					}
				}
			}
		}
		b.hits["complete-batch"], b.hits["complete-replay"] = true, true
		return nil
	}
	for _, m := range msgs {
		if err := b.complete(ctx, q, m); err != nil {
			return err
		}
		if err := b.settle(ctx, q, m, "complete-replay", func() error { return m.Complete(ctx) }, false); err != nil {
			return err
		}
		if err := b.expectError("wrong-settlement-verb", q, m, m.Abandon(ctx), mq.ErrLockLost); err != nil {
			return err
		}
	}
	b.hits["complete-replay"] = true
	return nil
}

func (b *batch) group(ctx context.Context, q string) error {
	head, err := b.one(ctx, q, 0, 1, mq.RecvOpts{})
	if err != nil {
		return err
	}
	until := time.Now().Add(2 * time.Second)
	if err := b.settle(ctx, q, head, "abandon-backoff", func() error { return head.Abandon(ctx, mq.AbandonOpts{Delay: 2 * time.Second}) }, false); err != nil {
		return err
	}
	// Input order is A0,A1,B0,B1. B progresses while A0 holds A1 back.
	for _, i := range []int{2, 3} {
		m, err := b.one(ctx, q, i, 1, mq.RecvOpts{})
		if err != nil {
			return err
		}
		if err := b.complete(ctx, q, m); err != nil {
			return err
		}
	}
	if !time.Now().Before(until) {
		return errors.New("group backoff assertion window elapsed before probe")
	}
	if err := b.excluded(ctx, q, mq.RecvOpts{}); err != nil {
		return err
	}
	if err := waitUntil(ctx, until); err != nil {
		return err
	}
	for _, i := range []int{0, 1} {
		count := 1
		if i == 0 {
			count = 2
		}
		m, err := b.one(ctx, q, i, count, mq.RecvOpts{})
		if err != nil {
			return err
		}
		if err := b.complete(ctx, q, m); err != nil {
			return err
		}
	}
	b.hits["two-groups"], b.hits["backoff-hol"], b.hits["group-order"] = true, true, true
	return nil
}

func (b *batch) strict(ctx context.Context, q string) error {
	head, err := b.one(ctx, q, 0, 1, mq.RecvOpts{})
	if err != nil {
		return err
	}
	if err := b.excluded(ctx, q, mq.RecvOpts{}); err != nil {
		return err
	}
	if err := b.settle(ctx, q, head, "defer", func() error { return head.Defer(ctx) }, false); err != nil {
		return err
	}
	if err := b.excluded(ctx, q, mq.RecvOpts{}); err != nil {
		return err
	}
	head, err = b.one(ctx, q, 0, 2, mq.RecvOpts{Pick: []int64{head.SequenceNumber}})
	if err != nil {
		return err
	}
	until := time.Now().Add(2 * time.Second)
	if err := b.settle(ctx, q, head, "abandon-backoff", func() error { return head.Abandon(ctx, mq.AbandonOpts{Delay: 2 * time.Second}) }, false); err != nil {
		return err
	}
	if err := b.excluded(ctx, q, mq.RecvOpts{}); err != nil {
		return err
	}
	if err := waitUntil(ctx, until); err != nil {
		return err
	}
	head, err = b.one(ctx, q, 0, 3, mq.RecvOpts{})
	if err != nil {
		return err
	}
	if err := b.complete(ctx, q, head); err != nil {
		return err
	}
	tail, err := b.one(ctx, q, 1, 1, mq.RecvOpts{})
	if err != nil {
		return err
	}
	if err := b.complete(ctx, q, tail); err != nil {
		return err
	}
	for _, name := range requiredRecipes[2] {
		b.hits[name] = true
	}
	return nil
}

// Retained rows use the same full-content oracle as deliveries. The expected
// lease state, TTL, original enqueue time and dead-letter details are checked too.
func (b *batch) checkRetained(q string, p *mq.PeekedMessage, i, count int, state mq.State, reason, description string) error {
	m := &mq.Message{SequenceNumber: p.SequenceNumber, MessageID: p.MessageID, Body: p.Body, GroupID: p.GroupID,
		Subject: p.Subject, CorrelationID: p.CorrelationID, ReplyTo: p.ReplyTo, ContentType: p.ContentType, Properties: p.Properties,
		DeliveryCount: p.DeliveryCount, EnqueuedAt: p.EnqueuedAt, LockedUntil: p.LockedUntil}
	if err := b.observe(q, "peek-"+string(p.State), m); err != nil {
		return err
	}
	if err := b.ledger.observed.add(map[string]any{"kind": "retained-detail", "id": p.MessageID,
		"state": p.State, "visible_at": p.VisibleAt, "expires_at": p.ExpiresAt,
		"reason": p.DeadLetterReason, "description": p.DeadLetterDescription}); err != nil {
		return err
	}
	expected := b.plan.Inputs[i]
	if err := checkContent(m, expected); err != nil {
		return err
	}
	old := b.owners[key(q, expected.ID)]
	expires := time.Time{}
	if expected.TTLMillis != 0 {
		expires = old.enqueued.Add(time.Duration(expected.TTLMillis) * time.Millisecond)
	}
	if p.State != state || p.DeliveryCount != count || p.DeadLetterReason != reason || p.DeadLetterDescription != description ||
		p.SequenceNumber != old.sequence || !p.EnqueuedAt.Equal(old.enqueued) || !p.ExpiresAt.Equal(expires) ||
		!p.LockedUntil.IsZero() {
		return errors.New("retained state/reason/description/count/sequence/timestamp mismatch")
	}
	return nil
}

func (b *batch) peekOne(ctx context.Context, q string, i, count int, state mq.State, reason, description string) error {
	for {
		rows, err := b.api.Peek(ctx, q, mq.PeekOpts{State: state, Max: 2})
		if err != nil {
			return err
		}
		if len(rows) > 0 {
			if len(rows) != 1 {
				return errors.New("unexpected multiplicity in retained state")
			}
			if err := b.checkRetained(q, rows[0], i, count, state, reason, description); err != nil {
				return err
			}
			return nil
		}
		if err := waitUntil(ctx, time.Now().Add(100*time.Millisecond)); err != nil {
			return err
		}
	}
}

func (b *batch) redrive(ctx context.Context, q string, m *mq.Message) error {
	if err := b.note("redrive", "requested", q, m.MessageID, m.SequenceNumber); err != nil {
		return err
	}
	n, err := b.api.Redrive(ctx, q)
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("redrive got %d messages, want 1", n)
	}
	b.reset[key(q, m.MessageID)] = true
	return b.note("redrive", "accepted", q, m.MessageID, m.SequenceNumber)
}

func (b *batch) retry(ctx context.Context, q string) error {
	m, err := b.one(ctx, q, 0, 1, mq.RecvOpts{})
	if err != nil {
		return err
	}
	reason, count := "MaxDeliveryCountExceeded", 3
	description := ""
	if b.plan.Batch%3 == 1 {
		reason, count = "SoakExplicitReject", 1
		description = "verified full-content retry fixture"
		if err := b.settle(ctx, q, m, "reject", func() error {
			return m.Reject(ctx, mq.RejectOpts{Reason: reason, Detail: description})
		}, false); err != nil {
			return err
		}
		b.hits["explicit-reject"] = true
	} else {
		first := 1
		if b.plan.Batch%3 == 2 {
			old := m
			if err := waitUntil(ctx, old.LockedUntil.Add(50*time.Millisecond)); err != nil {
				return err
			}
			if err := b.expectError("expired-token", q, old, old.Complete(ctx), mq.ErrLockLost); err != nil {
				return err
			}
			m, err = b.one(ctx, q, 0, 2, mq.RecvOpts{})
			if err != nil {
				return err
			}
			if err := b.expectError("stale-token-after-reclaim", q, old, old.Complete(ctx), mq.ErrLockLost); err != nil {
				return err
			}
			first = 2
			b.hits["expired-token-fenced"] = true
		}
		for delivery := first; delivery <= 3; delivery++ {
			if err := b.settle(ctx, q, m, "abandon", func() error { return m.Abandon(ctx) }, false); err != nil {
				return err
			}
			if delivery < 3 {
				m, err = b.one(ctx, q, 0, delivery+1, mq.RecvOpts{})
				if err != nil {
					return err
				}
			}
		}
		b.hits["max-delivery-dlq"] = true
	}
	if err := b.excluded(ctx, q, mq.RecvOpts{}); err != nil {
		return err
	}
	if err := b.peekOne(ctx, q, 0, count, mq.DeadLettered, reason, description); err != nil {
		return err
	}
	if err := b.redrive(ctx, q, m); err != nil {
		return err
	}
	m, err = b.one(ctx, q, 0, 1, mq.RecvOpts{})
	if err != nil {
		return err
	}
	if err := b.complete(ctx, q, m); err != nil {
		return err
	}
	b.hits["redrive"] = true
	return nil
}

func (b *batch) scheduled(ctx context.Context, q string) error {
	at := time.UnixMilli(b.plan.Inputs[0].ScheduledAt)
	if !time.Now().Before(at) {
		return errors.New("schedule not-before assertion window elapsed")
	}
	if err := b.excluded(ctx, q, mq.RecvOpts{}); err != nil {
		return err
	}
	if err := waitUntil(ctx, at); err != nil {
		return err
	}
	msgs, err := b.receive(ctx, q, []int{0, 1}, 1, mq.RecvOpts{}, false)
	if err != nil {
		return err
	}
	if time.Now().Before(at) {
		return errors.New("scheduled delivery occurred before requested time")
	}
	for _, m := range msgs {
		if err := b.complete(ctx, q, m); err != nil {
			return err
		}
	}
	b.hits["not-before"], b.hits["eventual-delivery"] = true, true
	return nil
}

func (b *batch) deferred(ctx context.Context, q string) error {
	keep, err := b.one(ctx, q, 0, 1, mq.RecvOpts{})
	if err != nil {
		return err
	}
	if err := b.settle(ctx, q, keep, "defer", func() error { return keep.Defer(ctx) }, false); err != nil {
		return err
	}
	expireQ := b.plan.Inputs[1].Targets[0]
	expire, err := b.one(ctx, expireQ, 1, 1, mq.RecvOpts{})
	if err != nil {
		return err
	}
	if err := b.settle(ctx, expireQ, expire, "defer", func() error { return expire.Defer(ctx) }, false); err != nil {
		return err
	}
	if err := b.excluded(ctx, q, mq.RecvOpts{}); err != nil {
		return err
	}
	keep, err = b.one(ctx, q, 0, 2, mq.RecvOpts{Pick: []int64{keep.SequenceNumber}})
	if err != nil {
		return err
	}
	if err := b.complete(ctx, q, keep); err != nil {
		return err
	}
	expires := expire.EnqueuedAt.Add(time.Duration(b.plan.Inputs[1].TTLMillis) * time.Millisecond)
	if err := waitUntil(ctx, expires.Add(50*time.Millisecond)); err != nil {
		return err
	}
	if err := b.excluded(ctx, expireQ, mq.RecvOpts{Pick: []int64{expire.SequenceNumber}}); err != nil {
		return err
	}
	if b.plan.Inputs[1].Outcome == "ttl-dlq" {
		if err := b.peekOne(ctx, expireQ, 1, 1, mq.DeadLettered, "TTLExpired", ""); err != nil {
			return err
		}
		n, err := b.api.Purge(ctx, expireQ)
		if err != nil {
			return err
		}
		if n != 1 {
			return fmt.Errorf("verified TTL DLQ purge count=%d want=1", n)
		}
		if err := b.note("purge-verified-ttl", "accepted", expireQ, expire.MessageID, expire.SequenceNumber); err != nil {
			return err
		}
		b.hits["ttl-dlq"] = true
	} else {
		for {
			rows, err := b.api.Peek(ctx, expireQ, mq.PeekOpts{Max: 2})
			if err != nil {
				return err
			}
			if len(rows) == 0 {
				break
			}
			if len(rows) != 1 {
				return errors.New("TTL discard retained unexpected multiplicity")
			}
			if err := b.checkRetained(expireQ, rows[0], 1, 1, mq.Deferred, "", ""); err != nil {
				return err
			}
			if err := waitUntil(ctx, time.Now().Add(100*time.Millisecond)); err != nil {
				return err
			}
		}
		if err := b.note("ttl-discard", "observed-absent-after-expiry", expireQ, expire.MessageID, expire.SequenceNumber); err != nil {
			return err
		}
		b.hits["ttl-discard"] = true
	}
	if err := b.terminal(expireQ, expire.MessageID); err != nil {
		return err
	}
	b.hits["deferred-excluded"], b.hits["pick"], b.hits["expired-deferred-excluded"] = true, true, true
	return nil
}

func (b *batch) topics(ctx context.Context) error {
	for _, suffix := range []string{"all-events", "eu-events"} {
		q := queue(b.plan.Seed, suffix)
		indices := []int{0, 1}
		if suffix == "eu-events" {
			indices = indices[:1]
		}
		msgs, err := b.receive(ctx, q, indices, 1, mq.RecvOpts{}, false)
		if err != nil {
			return err
		}
		for _, m := range msgs {
			if err := b.complete(ctx, q, m); err != nil {
				return err
			}
		}
		if err := b.excluded(ctx, q, mq.RecvOpts{}); err != nil {
			return err
		}
	}
	b.hits["include"], b.hits["exclude"], b.hits["fanout"] = true, true, true
	return nil
}

func (b *batch) outboxLane(ctx context.Context) error {
	q := queue(b.plan.Seed, "outbox")
	rollback := errors.New("intentional soak rollback")
	for i, m := range b.plan.Inputs {
		var seq int64
		if err := b.note("outbox-tx", "requested", q, m.ID, 0); err != nil {
			return err
		}
		err := b.embedded.Tx(ctx, func(tx *engine.EngineTx) error {
			if _, err := tx.SQL().ExecContext(tx.Context(), `INSERT INTO soak_business VALUES (?,?,?)`, m.ID, i+100, m.Body); err != nil {
				return err
			}
			var err error
			out := m.out()
			seq, err = tx.SendOne(q, engine.OutMessage{MessageID: out.MessageID, Body: out.Body, GroupID: out.GroupID,
				Subject: out.Subject, CorrelationID: out.CorrelationID, ReplyTo: out.ReplyTo, ContentType: out.ContentType, Properties: out.Properties})
			if err != nil {
				return err
			}
			if m.Outcome == "rollback" {
				return rollback
			}
			return nil
		})
		if m.Outcome == "rollback" {
			if !errors.Is(err, rollback) {
				if err == nil {
					return errors.New("outbox rollback unexpectedly committed")
				}
				return err
			}
			if err := b.note("outbox-tx", "expected-rollback", q, m.ID, 0); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		if seq <= 0 {
			return errors.New("outbox committed without a sequence")
		}
		if err := b.note("outbox-tx", "accepted", q, m.ID, seq); err != nil {
			return err
		}
		b.acked[m.ID], b.owners[key(q, m.ID)] = true, ownership{sequence: seq}
		b.sent++
	}
	if err := b.embedded.Tx(ctx, func(tx *engine.EngineTx) error {
		rows, err := tx.SQL().QueryContext(tx.Context(), `SELECT id,amount,body FROM soak_business ORDER BY id`)
		if err != nil {
			return err
		}
		defer rows.Close()
		seen := map[string]bool{}
		for rows.Next() {
			var id string
			var amount int
			var body []byte
			if err := rows.Scan(&id, &amount, &body); err != nil {
				return err
			}
			found := false
			for i, m := range b.plan.Inputs[:2] {
				if id == m.ID && amount == i+100 && bytes.Equal(body, m.Body) && !seen[id] {
					found = true
					seen[id] = true
				}
			}
			if !found {
				return errors.New("business rows differ from committed identities/amount/full bodies")
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(seen) != 2 {
			return errors.New("missing committed business rows")
		}
		return nil
	}); err != nil {
		return err
	}
	msgs, err := b.receive(ctx, q, []int{0, 1}, 1, mq.RecvOpts{Attempt: fmt.Sprintf("%s/outbox/%d", b.plan.Seed, b.plan.Batch)}, false)
	if err != nil {
		return err
	}
	for _, m := range msgs {
		if err := b.complete(ctx, q, m); err != nil {
			return err
		}
	}
	if err := b.embedded.Tx(ctx, func(tx *engine.EngineTx) error {
		result, err := tx.SQL().ExecContext(tx.Context(), `DELETE FROM soak_business WHERE id IN (?,?)`, b.plan.Inputs[0].ID, b.plan.Inputs[1].ID)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n != 2 {
			return errors.New("verified business cleanup count mismatch")
		}
		return nil
	}); err != nil {
		return err
	}
	for _, name := range requiredRecipes[7] {
		b.hits[name] = true
	}
	return nil
}

// Stable ordering is useful for external ledger readers; no goroutine scheduling
// order defines FIFO expectations anywhere in this runner.
func sortedKeys(values map[string]bool) []string {
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
