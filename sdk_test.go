package mqlite_test

// Blackbox SDK tests (package mqlite_test) — exercise the public API the way a
// caller would. Organized into sections:
//
//   - Harness         newBroker: an in-memory broker + connected client
//   - Remote / SDK    Client RPCs over HTTP (round-trip, auth, batch, receiver loop,
//                     broad handler exercise)
//   - Embedded        the in-process engine (runnable example / single-writer)
//   - Build guards    go.mod floor / dependency-freeze intent
//
// White-box internals live in receiver_internal_test.go (package mqlite).

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mqlitehq/mqlite"
	"github.com/mqlitehq/mqlite/server"
)

// ─── Harness ────────────────────────────────────────────────────────────────

// newBroker starts an in-memory broker over httptest and returns a connected client.
func newBroker(t *testing.T, token string) (*mqlite.Client, *mqlite.Embedded) {
	t.Helper()
	ctx := context.Background()
	// Not t.TempDir(): on Windows a pure-Go SQLite (modernc) file handle can linger
	// briefly after Close, and t.TempDir()'s auto-RemoveAll fails the test when it
	// can't delete the still-open file. Use a manual dir with a best-effort cleanup
	// (the CI runner reaps temp dirs anyway) so this teardown race can't flake.
	dir, err := os.MkdirTemp("", "mqlite-test-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	eng, err := mqlite.OpenEmbedded(ctx, "file:"+filepath.Join(dir, "mq.db"))
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

// ─── Remote / SDK over HTTP ─────────────────────────────────────────────────

// CompleteBatch over HTTP: receive a batch, then settle it in one round-trip.
func TestRemoteCompleteBatch(t *testing.T) {
	ctx := context.Background()
	cli, _ := newBroker(t, "mqk_test")
	if err := cli.CreateQueue(ctx, "orders", mqlite.QueueConfig{}); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := cli.SendOne(ctx, "orders", mqlite.OutMessage{Body: []byte("x")}); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	msgs, err := cli.Receive(ctx, "orders", mqlite.RecvOpts{Max: 3})
	if err != nil || len(msgs) != 3 {
		t.Fatalf("receive: got %d (err %v)", len(msgs), err)
	}
	res, err := cli.CompleteBatch(ctx, "orders", msgs...)
	if err != nil {
		t.Fatalf("CompleteBatch: %v", err)
	}
	if len(res) != 3 {
		t.Fatalf("want 3 results, got %d", len(res))
	}
	for _, r := range res {
		if !r.Ok {
			t.Fatalf("want all Ok=true, got %+v", res)
		}
	}
	if m, _ := cli.Stats(ctx, "orders"); m.Total != 0 {
		t.Fatalf("queue should be empty after batch complete, total=%d", m.Total)
	}
}

func TestRemoteRoundTrip(t *testing.T) {
	ctx := context.Background()
	cli, _ := newBroker(t, "mqk_test")

	if err := cli.CreateQueue(ctx, "orders", mqlite.QueueConfig{}); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	seq, err := cli.SendOne(ctx, "orders", mqlite.OutMessage{
		Body: []byte("hello"), MessageID: "m1", Subject: "order.created",
		Properties: map[string]string{"tenant": "acme"},
	})
	if err != nil || seq != 1 {
		t.Fatalf("send: seq=%d err=%v", seq, err)
	}

	msgs, err := cli.Receive(ctx, "orders", mqlite.RecvOpts{Wait: 2 * time.Second})
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
	mt, _ := cli.Stats(ctx, "orders")
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
		if _, err := cli.SendOne(ctx, "jobs", mqlite.OutMessage{Body: []byte("job")}); err != nil {
			t.Fatal(err)
		}
	}

	var processed int64
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
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
	<-done // wait for Run (and its in-flight settles) to fully stop before cleanup
	//        closes the engine — else Windows can't delete the still-open DB file.
}

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

// ─── Embedded (in-process) ──────────────────────────────────────────────────

// Example_embedded runs the whole queue in-process — no broker, no HTTP — against a
// local SQLite file, and shows the single-process / single-writer guarantee: a
// second opener of the same DB is rejected with ErrDBLocked (MQLITE-6 / MQLITE-15).
func Example_embedded() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "mqlite-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	dsn := "file:" + filepath.Join(dir, "mq.db")

	eng, err := mqlite.OpenEmbedded(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = eng.Close() }()

	if err := eng.CreateQueue(ctx, "orders", mqlite.QueueConfig{}); err != nil {
		log.Fatal(err)
	}
	if _, err := eng.SendOne(ctx, "orders", mqlite.OutMessage{Body: []byte("order-42")}); err != nil {
		log.Fatal(err)
	}

	msgs, err := eng.Receive(ctx, "orders", mqlite.RecvOpts{})
	if err != nil {
		log.Fatal(err)
	}
	for _, m := range msgs {
		fmt.Printf("received: %s\n", m.Body)
		_ = m.Complete(ctx) // at-least-once; the handler must be idempotent
	}

	// Embedded mode is single-writer: a second process — or a second OpenEmbedded on
	// the same file — is rejected rather than racing the first.
	if _, err := mqlite.OpenEmbedded(ctx, dsn); errors.Is(err, mqlite.ErrDBLocked) {
		fmt.Println("second open rejected: single writer")
	}

	// Output:
	// received: order-42
	// second open rejected: single writer
}

// ─── Build / dependency guards ──────────────────────────────────────────────

// TestGoModFloorStaysAt121 guards the go.mod floor. MQLite pins it at go 1.21 so
// the SDK stays embeddable in older projects (MQLITE-1). That floor also freezes
// modernc.org/sqlite at v1.36.1 and golang.org/x/sys at v0.30.0 — every later
// release of either requires go >= 1.23 (see docs/dependencies.md). The go 1.21.x
// CI matrix enforces that the code still *builds*; this test enforces *intent*, so
// a floor bump fails with a clear message instead of a cryptic toolchain error and
// the dependency freeze can't be lifted by accident.
func TestGoModFloorStaysAt121(t *testing.T) {
	b, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	const want = "go 1.21"
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "go 1.") {
			if line != want {
				t.Fatalf("go.mod floor = %q, want %q — raising it drops embedding "+
					"compatibility and unfreezes sqlite/x/sys. If intended, update "+
					"docs/dependencies.md and the Dependabot ignore rules deliberately.", line, want)
			}
			return
		}
	}
	t.Fatal("no `go 1.x` directive found in go.mod")
}
