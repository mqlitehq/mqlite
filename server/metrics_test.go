package server_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/server"
)

// MQLITE-5: /metrics serves per-queue counters in Prometheus text format.
func TestMetricsEndpoint(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer eng.Close()
	if err := eng.CreateQueue(ctx, "orders", engine.QueueConfig{}); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if _, err := eng.SendOne(ctx, "orders", engine.OutMessage{Body: []byte("x")}); err != nil {
		t.Fatalf("send: %v", err)
	}

	ts := httptest.NewServer(server.New(eng, nil).Handler()) // nil tokens -> auth off
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	out := string(body)
	for _, want := range []string{
		`mqlite_queue_messages{queue="orders",state="active"} 1`,
		`mqlite_queue_messages{queue="orders",state="locked"} 0`,
		`mqlite_queue_total{queue="orders"} 1`,
		"# TYPE mqlite_queue_messages gauge",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("/metrics output missing %q:\n%s", want, out)
		}
	}
}

// /metrics exposes a lifetime completed-message counter that persists past the row
// being deleted on Complete — so "how many were processed" is readable even on an
// empty queue (MQLITE-54).
func TestMetricsCompletedCounter(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer eng.Close()
	if err := eng.CreateQueue(ctx, "orders", engine.QueueConfig{LockDurationMs: 600_000, MaxDeliveryCount: 10}); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := eng.SendOne(ctx, "orders", engine.OutMessage{Body: []byte("x")}); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	msgs, err := eng.Receive(ctx, "orders", engine.ReceiveOptions{MaxMessages: 3})
	if err != nil || len(msgs) != 3 {
		t.Fatalf("receive: got %d (err %v)", len(msgs), err)
	}
	for _, m := range msgs {
		if err := eng.Complete(ctx, "orders", m.SeqNumber, m.LockToken); err != nil {
			t.Fatalf("complete: %v", err)
		}
	}

	ts := httptest.NewServer(server.New(eng, nil).Handler()) // auth off
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	out := string(body)
	for _, want := range []string{
		"# TYPE mqlite_messages_completed_total counter",
		`mqlite_messages_completed_total{queue="orders"} 3`, // the queue is empty, yet the count survives
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("/metrics missing %q:\n%s", want, out)
		}
	}
}

// /metrics also exposes a per-RPC latency histogram, fed by every RPC call — so a slow
// dequeue is visible in monitoring, not just in tests.
func TestMetricsRPCLatencyHistogram(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer eng.Close()
	ts := httptest.NewServer(server.New(eng, nil).Handler()) // auth off
	defer ts.Close()

	const n = 5
	for i := 0; i < n; i++ {
		res, err := http.Post(ts.URL+"/mqlite.v1.AdminService/ListQueues", "application/json", strings.NewReader("{}"))
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
	}

	// observe() runs just after the handler returns, so a read can race the last call —
	// poll until the histogram has counted all n (it settles in microseconds).
	want := `mqlite_rpc_duration_seconds_count{rpc="AdminService/ListQueues"} 5`
	var out string
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		res, err := http.Get(ts.URL + "/metrics")
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if out = string(body); strings.Contains(out, want) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	for _, w := range []string{
		want,
		`mqlite_rpc_duration_seconds_bucket{rpc="AdminService/ListQueues",le="+Inf"} 5`,
		`mqlite_rpc_duration_seconds_sum{rpc="AdminService/ListQueues"}`,
		"# TYPE mqlite_rpc_duration_seconds histogram",
	} {
		if !strings.Contains(out, w) {
			t.Errorf("histogram missing %q:\n%s", w, out)
		}
	}
	// the observer covers only RPCs — not /metrics itself, /healthz, or static paths.
	if strings.Contains(out, `rpc="metrics"`) || strings.Contains(out, `rpc="healthz"`) {
		t.Errorf("histogram should not include non-RPC paths:\n%s", out)
	}
}

// MQLITE-62: only REGISTERED routes are ever labeled. An unregistered
// /mqlite.v1.* path (the 404 catch-all) must not create histogram series —
// otherwise any client can grow the label map without bound (one counter set
// per invented path, never evicted): a memory / scrape-size DoS.
func TestMetricsRPCLabelsOnlyForRegisteredRoutes(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer eng.Close()
	ts := httptest.NewServer(server.New(eng, nil).Handler()) // auth off
	defer ts.Close()

	// A burst of distinct invented RPC names — none may become a label.
	for i := 0; i < 20; i++ {
		res, err := http.Post(ts.URL+fmt.Sprintf("/mqlite.v1.QueueService/Nope%d", i),
			"application/json", strings.NewReader("{}"))
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound {
			t.Fatalf("invented RPC path should 404, got %d", res.StatusCode)
		}
	}
	// One real RPC so the histogram section exists at all.
	res, err := http.Post(ts.URL+"/mqlite.v1.AdminService/ListQueues", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()

	var out string
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		r, err := http.Get(ts.URL + "/metrics")
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(r.Body)
		r.Body.Close()
		if out = string(body); strings.Contains(out, `rpc="AdminService/ListQueues"`) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !strings.Contains(out, `rpc="AdminService/ListQueues"`) {
		t.Fatalf("registered route must be labeled:\n%s", out)
	}
	if strings.Contains(out, "Nope") {
		t.Fatalf("unregistered RPC paths must never become labels (cardinality DoS):\n%s", out)
	}
}
