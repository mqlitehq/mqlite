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
}
