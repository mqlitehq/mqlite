package mqlite_test

import (
	"context"
	"testing"
	"time"

	"github.com/mqlitehq/mqlite"
)

// TestBrokerExercise drives most Client RPCs (and therefore most server handlers
// and wire round-trips) through one in-memory broker, to lock the remote contract
// and lift SDK/server coverage (MQLITE-26).
func TestBrokerExercise(t *testing.T) {
	ctx := context.Background()
	cli, _ := newBroker(t, "") // no auth

	if err := cli.CreateQueue(ctx, "q", mqlite.QueueConfig{MaxDeliveryCount: 2}); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if _, err := cli.Send(ctx, "q",
		mqlite.OutMessage{Body: []byte("a"), MessageID: "a"},
		mqlite.OutMessage{Body: []byte("b")},
	); err != nil {
		t.Fatalf("send: %v", err)
	}

	if m, err := cli.Stats(ctx, "q"); err != nil || m.Active != 2 || m.Total != 2 {
		t.Fatalf("stats: %+v err=%v (want active=2,total=2)", m, err)
	}
	if qs, err := cli.ListQueues(ctx); err != nil || len(qs) == 0 {
		t.Fatalf("list queues: %v n=%d", err, len(qs))
	}

	// Complete one.
	recv := func() *mqlite.Message {
		t.Helper()
		ms, err := cli.Receive(ctx, "q", mqlite.RecvOpts{Max: 1, Wait: 2 * time.Second})
		if err != nil || len(ms) != 1 {
			t.Fatalf("receive: %v n=%d", err, len(ms))
		}
		return ms[0]
	}
	if err := recv().Complete(ctx); err != nil {
		t.Fatalf("complete: %v", err)
	}

	// Abandon the other -> back to active -> receive again -> Reject -> DLQ.
	if err := recv().Abandon(ctx, mqlite.AbandonOpts{Delay: 0}); err != nil {
		t.Fatalf("abandon: %v", err)
	}
	m := recv()
	if err := m.Renew(ctx); err != nil { // extend the lease while we hold it
		t.Fatalf("renew: %v", err)
	}
	if err := m.Reject(ctx, mqlite.RejectOpts{Reason: "bad", Detail: "nope"}); err != nil {
		t.Fatalf("reject: %v", err)
	}

	// It is now dead-lettered: peek the DLQ, redrive it back, then purge.
	dlq, err := cli.Peek(ctx, "q", mqlite.PeekOpts{State: mqlite.DeadLettered})
	if err != nil || len(dlq) != 1 {
		t.Fatalf("peek dlq: %v n=%d", err, len(dlq))
	}
	if moved, err := cli.Redrive(ctx, "q"); err != nil || moved != 1 {
		t.Fatalf("redrive: %v moved=%d", err, moved)
	}
	// Drain it to the DLQ again (maxDelivery=2: this is delivery 2) and purge.
	mm := recv()
	_ = mm.Reject(ctx, mqlite.RejectOpts{Reason: "again"})
	if purged, err := cli.Purge(ctx, "q"); err != nil || purged != 1 {
		t.Fatalf("purge: %v purged=%d", err, purged)
	}

	// Defer + retrieve-by-seq.
	seq, err := cli.SendOne(ctx, "q", mqlite.OutMessage{Body: []byte("deferred")})
	if err != nil {
		t.Fatalf("send for defer: %v", err)
	}
	if err := recv().Defer(ctx); err != nil {
		t.Fatalf("defer: %v", err)
	}
	picked, err := cli.Receive(ctx, "q", mqlite.RecvOpts{Pick: []int64{seq}})
	if err != nil || len(picked) != 1 || string(picked[0].Body) != "deferred" {
		t.Fatalf("receive deferred by seq: %v n=%d", err, len(picked))
	}
	_ = picked[0].Complete(ctx)

	// Topic + subscription fan-out (Subscribe creates the subscription queue).
	if err := cli.Subscribe(ctx, "events", "subA", nil); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if _, err := cli.SendOne(ctx, "events", mqlite.OutMessage{Body: []byte("evt")}); err != nil {
		t.Fatalf("publish to topic: %v", err)
	}
	got, err := cli.Receive(ctx, "subA", mqlite.RecvOpts{Wait: 2 * time.Second})
	if err != nil || len(got) != 1 || string(got[0].Body) != "evt" {
		t.Fatalf("receive from subscription: %v n=%d", err, len(got))
	}
	_ = got[0].Complete(ctx)
}
