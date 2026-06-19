package server_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
