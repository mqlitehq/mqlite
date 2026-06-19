package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/server"
	"github.com/mqlitehq/mqlite/wire"
)

// TestServerSendDedupConflict pins the fix for MQLITE-33: a single HTTP Send that
// hits a dedup conflict (same message_id, different body) must be 409, not 200 with
// a bogus seq 0 — otherwise an HTTP/curl client is told its never-enqueued message
// succeeded. (The batch path swallows the conflict as seq 0; the handler re-surfaces
// it for a single Send, matching engine.SendOne.)
func TestServerSendDedupConflict(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer eng.Close()
	if err := eng.CreateQueue(ctx, "q", engine.QueueConfig{DedupWindowMs: 600000}); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	ts := httptest.NewServer(server.New(eng, nil).Handler())
	defer ts.Close()

	send := func(id, body string) (int, []byte) {
		t.Helper()
		jb, _ := json.Marshal(wire.SendRequest{
			Queue: "q", Messages: []wire.Message{{MessageID: id, Body: []byte(body)}},
		})
		resp, err := http.Post(ts.URL+wire.PathSend, "application/json", bytes.NewReader(jb))
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		defer resp.Body.Close()
		rb, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, rb
	}

	// First send: 200 with a real seq.
	st, rb := send("k", "A")
	var sr wire.SendResponse
	_ = json.Unmarshal(rb, &sr)
	if st != http.StatusOK || len(sr.SeqNumbers) != 1 || sr.SeqNumbers[0] == 0 {
		t.Fatalf("first send: status=%d body=%s, want 200 + non-zero seq", st, rb)
	}
	first := sr.SeqNumbers[0]

	// Same id + same body: idempotent dedup → 200, same seq.
	st, rb = send("k", "A")
	sr = wire.SendResponse{}
	_ = json.Unmarshal(rb, &sr)
	if st != http.StatusOK || len(sr.SeqNumbers) != 1 || sr.SeqNumbers[0] != first {
		t.Fatalf("duplicate send: status=%d body=%s, want 200 + seq %d", st, rb, first)
	}

	// Same id + DIFFERENT body: dedup conflict → 409, not 200 {seq:[0]}.
	st, rb = send("k", "B")
	var e wire.ErrorBody
	_ = json.Unmarshal(rb, &e)
	if st != http.StatusConflict || e.Code != "already_exists" {
		t.Fatalf("dedup conflict: status=%d body=%s, want 409 already_exists (MQLITE-33)", st, rb)
	}
}
