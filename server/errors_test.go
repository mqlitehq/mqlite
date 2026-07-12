package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/server"
	"github.com/mqlitehq/mqlite/wire"
)

// TestServerAuthAndErrors locks the broker's auth + error-envelope contract: the
// status codes and {code,message} bodies clients branch on. The happy-path broker
// test runs with auth off, so these middleware/error paths live here (MQLITE-26).
func TestServerAuthAndErrors(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer eng.Close()
	if err := eng.CreateQueue(ctx, "q", engine.QueueConfig{}); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	ts := httptest.NewServer(server.New(eng, []string{"secret"}).Handler())
	defer ts.Close()

	// do issues one request and returns (status, error-code), draining and closing
	// the body so callers never hold it.
	do := func(method, path, tok string, body []byte) (int, string) {
		t.Helper()
		var rdr io.Reader
		if body != nil {
			rdr = bytes.NewReader(body)
		}
		req, _ := http.NewRequest(method, ts.URL+path, rdr)
		if tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		defer resp.Body.Close()
		var e wire.ErrorBody
		_ = json.NewDecoder(resp.Body).Decode(&e) // empty body on 200 -> Code stays ""
		return resp.StatusCode, e.Code
	}
	jsonOf := func(v any) []byte { b, _ := json.Marshal(v); return b }
	send := func(q string) []byte {
		return jsonOf(wire.SendRequest{Queue: q, Messages: []wire.Message{{Body: []byte("x")}}})
	}
	check := func(name, method, path, tok string, body []byte, wantStatus int, wantCode string) {
		t.Helper()
		st, code := do(method, path, tok, body)
		if st != wantStatus || code != wantCode {
			t.Errorf("%s: got status=%d code=%q, want %d/%q", name, st, code, wantStatus, wantCode)
		}
	}

	check("missing token", http.MethodPost, wire.PathSend, "", send("q"), http.StatusUnauthorized, "unauthenticated")
	check("wrong token", http.MethodPost, wire.PathSend, "nope", send("q"), http.StatusUnauthorized, "unauthenticated")
	check("GET on POST route", http.MethodGet, wire.PathSend, "secret", nil, http.StatusMethodNotAllowed, "unimplemented")
	check("malformed JSON", http.MethodPost, wire.PathSend, "secret", []byte("{not json"), http.StatusBadRequest, "invalid_argument")
	check("unknown queue", http.MethodPost, wire.PathSend, "secret", send("ghost"), http.StatusNotFound, "not_found")
	// /healthz must stay open for liveness probes even when auth is on.
	check("healthz open", http.MethodGet, "/healthz", "", nil, http.StatusOK, "")
	// A valid token passes through to a successful send.
	check("authed send", http.MethodPost, wire.PathSend, "secret", send("q"), http.StatusOK, "")

	// Subscription filter contract: a malformed expr is rejected at Subscribe with
	// 400 invalid_argument (never stored); a valid one succeeds.
	sub := func(name, expr string) []byte {
		return jsonOf(wire.SubscribeRequest{Topic: "ev", Name: name, Filter: &engine.Filter{Expr: expr}})
	}
	check("bad filter expr", http.MethodPost, wire.PathSubscribe, "secret", sub("bad", "subject =="), http.StatusBadRequest, "invalid_argument")
	check("unknown filter field", http.MethodPost, wire.PathSubscribe, "secret", sub("bad2", `nope == "x"`), http.StatusBadRequest, "invalid_argument")
	check("valid filter", http.MethodPost, wire.PathSubscribe, "secret", sub("ok", `subject_parts[0] == "a"`), http.StatusOK, "")

	// Strict request validation (MQLITE-86): an unknown field or data after the JSON
	// body is a typed 400, not a silently-dropped typo; empty name / unknown enum map
	// to 400 invalid_argument instead of faulting a SQLite CHECK into a 500.
	check("unknown field", http.MethodPost, wire.PathSend, "secret",
		[]byte(`{"queue":"q","messsages":[]}`), http.StatusBadRequest, "invalid_argument")
	check("trailing data after body", http.MethodPost, wire.PathSend, "secret",
		[]byte(`{"queue":"q","messages":[]}{"queue":"q"}`), http.StatusBadRequest, "invalid_argument")
	check("empty queue name", http.MethodPost, wire.PathCreateQueue, "secret",
		jsonOf(wire.CreateQueueRequest{Name: ""}), http.StatusBadRequest, "invalid_argument")
	check("unknown ordering_mode", http.MethodPost, wire.PathCreateQueue, "secret",
		jsonOf(wire.CreateQueueRequest{Name: "vq", Config: wire.QueueConfigJSON{OrderingMode: "fifo"}}),
		http.StatusBadRequest, "invalid_argument")
	check("valid create queue", http.MethodPost, wire.PathCreateQueue, "secret",
		jsonOf(wire.CreateQueueRequest{Name: "vq", Config: wire.QueueConfigJSON{OrderingMode: "group_fifo"}}),
		http.StatusOK, "")
}

// A request body over Server.MaxBodyBytes is rejected as 413 message_too_large
// BEFORE JSON decoding (review F8 / MQLITE-64) — without the cap a multi-GB
// body OOMs the broker before the per-message MaxMessageBytes check runs.
func TestRequestBodyCap(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer eng.Close()
	if err := eng.CreateQueue(ctx, "q", engine.QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	srv := server.New(eng, nil) // auth off
	srv.MaxBodyBytes = 1024     // tiny cap for the test; default is 32 MiB
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	big := append([]byte(`{"queue":"q","messages":[{"body":"`),
		append(bytes.Repeat([]byte("A"), 4096), []byte(`"}]}`)...)...)
	res, err := http.Post(ts.URL+wire.PathSend, "application/json", bytes.NewReader(big))
	if err != nil {
		t.Fatal(err)
	}
	var e struct{ Code string }
	_ = json.NewDecoder(res.Body).Decode(&e)
	res.Body.Close()
	if res.StatusCode != http.StatusRequestEntityTooLarge || e.Code != "message_too_large" {
		t.Fatalf("oversized body = %d %q, want 413 message_too_large", res.StatusCode, e.Code)
	}

	// Under the cap still works.
	ok, err := http.Post(ts.URL+wire.PathSend, "application/json",
		strings.NewReader(`{"queue":"q","messages":[{"body":"aGk="}]}`))
	if err != nil {
		t.Fatal(err)
	}
	ok.Body.Close()
	if ok.StatusCode != http.StatusOK {
		t.Fatalf("normal send under the cap = %d, want 200", ok.StatusCode)
	}
}
