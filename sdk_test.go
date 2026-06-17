package mqlite_test

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mqlitehq/mqlite"
	"github.com/mqlitehq/mqlite/server"
)

// newBroker starts an in-memory broker over httptest and returns a connected client.
func newBroker(t *testing.T, token string) (*mqlite.Client, *mqlite.Embedded) {
	t.Helper()
	ctx := context.Background()
	eng, err := mqlite.OpenEmbedded(ctx, "file:"+filepath.Join(t.TempDir(), "mq.db"))
	if err != nil {
		t.Fatalf("open embedded: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })

	var tokens []string
	if token != "" {
		tokens = []string{token}
	}
	ts := httptest.NewServer(server.New(eng.Engine(), tokens).Handler())
	t.Cleanup(ts.Close)

	cli, err := mqlite.Open(ctx, ts.URL, mqlite.WithToken(token))
	if err != nil {
		t.Fatalf("open client: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli, eng
}

func TestRemoteRoundTrip(t *testing.T) {
	ctx := context.Background()
	cli, _ := newBroker(t, "mqk_test")

	if err := cli.CreateQueue(ctx, "orders", mqlite.QueueConfig{}); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	seq, err := cli.Send(ctx, "orders", mqlite.OutMessage{
		Body: []byte("hello"), MessageID: "m1", Subject: "order.created",
		Properties: map[string]string{"tenant": "acme"},
	})
	if err != nil || seq != 1 {
		t.Fatalf("send: seq=%d err=%v", seq, err)
	}

	msgs, err := cli.Receive(ctx, "orders", mqlite.WithWait(2*time.Second))
	if err != nil || len(msgs) != 1 {
		t.Fatalf("receive: %v n=%d", err, len(msgs))
	}
	m := msgs[0]
	if string(m.Body) != "hello" || m.MessageID != "m1" || m.Properties["tenant"] != "acme" {
		t.Fatalf("roundtrip mismatch: %+v", m)
	}
	if err := m.Complete(ctx); err != nil {
		t.Fatalf("complete: %v", err)
	}
	mt, _ := cli.QueueMetrics(ctx, "orders")
	if mt.Total != 0 {
		t.Fatalf("queue should be empty, got %+v", mt)
	}
}

func TestRemoteAuth(t *testing.T) {
	ctx := context.Background()
	eng, err := mqlite.OpenEmbedded(ctx, "file:"+filepath.Join(t.TempDir(), "mq.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	ts := httptest.NewServer(server.New(eng.Engine(), []string{"right"}).Handler())
	t.Cleanup(ts.Close)

	bad, _ := mqlite.Open(ctx, ts.URL, mqlite.WithToken("wrong"))
	if err := bad.CreateQueue(ctx, "q", mqlite.QueueConfig{}); err == nil {
		t.Fatal("expected auth failure with wrong token")
	}
	good, _ := mqlite.Open(ctx, ts.URL, mqlite.WithToken("right"))
	if err := good.CreateQueue(ctx, "q", mqlite.QueueConfig{}); err != nil {
		t.Fatalf("good token should succeed: %v", err)
	}
}

func TestReceiverRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cli, _ := newBroker(t, "")

	if err := cli.CreateQueue(ctx, "jobs", mqlite.QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	const n = 5
	for i := 0; i < n; i++ {
		if _, err := cli.Send(ctx, "jobs", mqlite.OutMessage{Body: []byte("job")}); err != nil {
			t.Fatal(err)
		}
	}

	var processed int64
	runCtx, stop := context.WithCancel(ctx)
	go func() {
		_ = cli.Receiver("jobs", mqlite.WithConcurrency(3)).Run(runCtx, func(c context.Context, m *mqlite.Message) error {
			if atomic.AddInt64(&processed, 1) >= n {
				stop()
			}
			return nil // -> auto Complete
		})
	}()

	deadline := time.After(8 * time.Second)
	for atomic.LoadInt64(&processed) < n {
		select {
		case <-deadline:
			t.Fatalf("processed only %d/%d", atomic.LoadInt64(&processed), n)
		case <-time.After(20 * time.Millisecond):
		}
	}
	stop()
}
