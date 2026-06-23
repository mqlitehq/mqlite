package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/server"
	"github.com/mqlitehq/mqlite/wire"
)

func TestStatusEndpoint(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer eng.Close()
	if err := eng.CreateQueue(ctx, "orders", engine.QueueConfig{}); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if err := eng.Subscribe(ctx, "events", "all", nil); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	s := server.New(eng, []string{"secret"})
	s.Version = "9.9.9"
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+wire.PathStatus, bytes.NewReader([]byte("{}")))
	req.Header.Set("Authorization", "Bearer secret")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status %d", res.StatusCode)
	}
	var st wire.StatusResponse
	if err := json.NewDecoder(res.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	if st.Backend != "memory" || st.Version != "9.9.9" || !st.Auth {
		t.Fatalf("status = %+v", st)
	}
	if st.Queues != 1 { // the subscription's backing queue must not be counted as a queue
		t.Errorf("queues=%d, want 1", st.Queues)
	}
	if st.Subscriptions != 1 {
		t.Errorf("subscriptions=%d, want 1", st.Subscriptions)
	}
	if st.SchemaVersion == "" {
		t.Error("missing schema version")
	}

	// Behind auth: no token → 401.
	res2, err := http.Post(ts.URL+wire.PathStatus, "application/json", bytes.NewReader([]byte("{}")))
	if err != nil {
		t.Fatal(err)
	}
	res2.Body.Close()
	if res2.StatusCode != http.StatusUnauthorized {
		t.Errorf("no-auth status = %d, want 401", res2.StatusCode)
	}
}
