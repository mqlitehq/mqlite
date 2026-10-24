package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
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

// TestServerPublishNoSubscriberIsNoOp pins the fix: a single publish to a topic that
// matches NO subscription filter is a valid no-op (200, seq 0) — NOT a 409 "dedup
// conflict". handleSend used to map every single-message seq-0 to ErrDedupConflict,
// conflating a no-subscriber publish with a real conflict.
func TestServerPublishNoSubscriberIsNoOp(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer eng.Close()
	// topic "t" with ONE filtered subscription (no match-all), so a non-matching publish
	// reaches zero targets.
	if err := eng.Subscribe(ctx, "t", "gold", &engine.Filter{Expr: `properties["tier"]=="gold"`}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	ts := httptest.NewServer(server.New(eng, nil).Handler())
	defer ts.Close()

	pub := func(tier string) wire.SendResponse {
		t.Helper()
		jb, _ := json.Marshal(wire.SendRequest{Queue: "t", Messages: []wire.Message{{
			Body: []byte("{}"), Properties: map[string]string{"tier": tier},
		}}})
		resp, err := http.Post(ts.URL+wire.PathSend, "application/json", bytes.NewReader(jb))
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		defer resp.Body.Close()
		rb, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("tier=%s: status=%d body=%s, want 200 (a no-subscriber publish is a no-op)", tier, resp.StatusCode, rb)
		}
		var sr wire.SendResponse
		_ = json.Unmarshal(rb, &sr)
		return sr
	}

	if sr := pub("gold"); len(sr.SeqNumbers) != 1 || sr.SeqNumbers[0] == 0 {
		t.Fatalf("matching publish: seqs=%v, want one non-zero", sr.SeqNumbers)
	}
	if sr := pub("silver"); len(sr.SeqNumbers) != 1 || sr.SeqNumbers[0] != 0 {
		t.Fatalf("no-subscriber publish: seqs=%v, want [0] (200 no-op, not 409)", sr.SeqNumbers)
	}
}

// TestServerSendPerMessageTTL pins the additive batch contract: legacy callers may
// keep using the request-level ttl_ms, while a mixed batch carries ttl_ms on each
// message. Ambiguous or negative inputs fail before any row is written.
func TestServerSendPerMessageTTL(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer eng.Close()
	const queueTTL = int64(300_000)
	if err := eng.CreateQueue(ctx, "q", engine.QueueConfig{DefaultTTLMs: queueTTL}); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	ts := httptest.NewServer(server.New(eng, nil).Handler())
	defer ts.Close()
	post := func(req any) (int, []byte) {
		t.Helper()
		jb, err := json.Marshal(req)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		resp, err := http.Post(ts.URL+wire.PathSend, "application/json", bytes.NewReader(jb))
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		defer resp.Body.Close()
		rb, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		return resp.StatusCode, rb
	}
	assertInvalid := func(name string, req any) {
		t.Helper()
		status, body := post(req)
		var problem wire.ErrorBody
		_ = json.Unmarshal(body, &problem)
		if status != http.StatusBadRequest || problem.Code != "invalid_argument" {
			t.Fatalf("%s: status=%d body=%s, want 400 invalid_argument", name, status, body)
		}
	}

	status, body := post(wire.SendWithMessageTTLsRequest{Queue: "q", Messages: []wire.SendMessage{
		{Message: wire.Message{Body: []byte("default")}},
		{Message: wire.Message{Body: []byte("short")}, TTLMs: 60_000},
		{Message: wire.Message{Body: []byte("capped")}, TTLMs: 600_000},
		{Message: wire.Message{Body: []byte("huge-capped")}, TTLMs: math.MaxInt64},
	}})
	if status != http.StatusOK {
		t.Fatalf("mixed TTL: status=%d body=%s", status, body)
	}
	peeked, err := eng.Peek(ctx, "q", engine.PeekOptions{Max: 10})
	if err != nil {
		t.Fatalf("peek: %v", err)
	}
	wantTTL := map[string]int64{
		"default": queueTTL, "short": 60_000, "capped": queueTTL, "huge-capped": queueTTL,
	}
	if len(peeked) != len(wantTTL) {
		t.Fatalf("peek count=%d, want %d", len(peeked), len(wantTTL))
	}
	for _, msg := range peeked {
		if got, want := msg.ExpiresAtMs-msg.VisibleAtMs, wantTTL[string(msg.Body)]; got != want {
			t.Errorf("%s effective TTL=%d, want %d", msg.Body, got, want)
		}
	}

	assertInvalid("request and message TTL conflict", wire.SendWithMessageTTLsRequest{
		Queue: "q", TTLMs: 1_000,
		Messages: []wire.SendMessage{{Message: wire.Message{Body: []byte("x")}, TTLMs: 500}},
	})
	assertInvalid("negative request TTL", wire.SendRequest{
		Queue: "q", TTLMs: -1, Messages: []wire.Message{{Body: []byte("x")}},
	})
	assertInvalid("negative message TTL", wire.SendWithMessageTTLsRequest{
		Queue: "q", Messages: []wire.SendMessage{{Message: wire.Message{Body: []byte("x")}, TTLMs: -1}},
	})
	stats, err := eng.Stats(ctx, "q")
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Total != int64(len(wantTTL)) {
		t.Fatalf("invalid requests wrote rows: total=%d, want %d", stats.Total, len(wantTTL))
	}

	if err := eng.CreateQueue(ctx, "unlimited", engine.QueueConfig{}); err != nil {
		t.Fatalf("create unlimited queue: %v", err)
	}
	assertInvalid("TTL expiry overflow", wire.SendRequest{
		Queue: "unlimited", TTLMs: math.MaxInt64,
		Messages: []wire.Message{{Body: []byte("overflow")}},
	})
	if stats, err := eng.Stats(ctx, "unlimited"); err != nil || stats.Total != 0 {
		t.Fatalf("overflow request wrote rows: stats=%+v err=%v", stats, err)
	}
}
