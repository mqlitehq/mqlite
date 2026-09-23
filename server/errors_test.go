package server_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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

	// A negative destructive limit must be rejected at the HTTP boundary too (not just the
	// CLI) — otherwise a raw request degrades a bounded purge/redrive into "delete all"
	// (review 2026-07-12 P1-2).
	check("negative purge max", http.MethodPost, wire.PathPurge, "secret",
		jsonOf(wire.PurgeRequest{Queue: "q", Max: -1}), http.StatusBadRequest, "invalid_argument")
	check("negative redrive older", http.MethodPost, wire.PathRedrive, "secret",
		jsonOf(wire.RedriveRequest{Queue: "q", OlderThanMs: -1}), http.StatusBadRequest, "invalid_argument")
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

// ── Managed access keys and complete route authorization (MQLITE-121) ───────

func keyTestEngine(t *testing.T, now func() int64) *engine.Engine {
	t.Helper()
	eng, err := engine.Open(context.Background(), engine.Options{DB: ":memory:", DisableBackground: true, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	return eng
}

func keyTestCreate(t *testing.T, eng *engine.Engine, id int, permission engine.KeyPermissions, expiry int64) (engine.AccessKey, string) {
	t.Helper()
	key, token, err := eng.CreateAccessKey(context.Background(), engine.CreateAccessKeyOptions{
		ID: fmt.Sprintf("%032x", id), Name: "test", Permissions: permission, ExpiresAtMs: expiry,
	})
	if err != nil {
		t.Fatal(err)
	}
	return key, token
}

func keyTestRequest(h http.Handler, method, path, token string, body any) *httptest.ResponseRecorder {
	b, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(b))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func keyTestStatus(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d: %s", rec.Code, status, rec.Body)
	}
	if code != "" {
		var body wire.ErrorBody
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body.Code != code {
			t.Fatalf("error = %s, want code %s: %v", rec.Body, code, err)
		}
	}
}

// The literal inventory pins every method and permission independently of the
// implementation. Compare the entire live discovery catalog as well: a new route
// cannot escape the matrix just because this test forgot to mention it.
func TestAccessKeyCompletePermissionMatrix(t *testing.T) {
	routes := []struct {
		path       string
		permission engine.KeyPermissions
	}{
		{"/mqlite.v1.QueueService/Send", engine.KeySend},
		{"/mqlite.v1.QueueService/Receive", engine.KeyListen},
		{"/mqlite.v1.QueueService/Complete", engine.KeyListen},
		{"/mqlite.v1.QueueService/CompleteBatch", engine.KeyListen},
		{"/mqlite.v1.QueueService/RenewBatch", engine.KeyListen},
		{"/mqlite.v1.QueueService/Abandon", engine.KeyListen},
		{"/mqlite.v1.QueueService/Reject", engine.KeyListen},
		{"/mqlite.v1.QueueService/Defer", engine.KeyListen},
		{"/mqlite.v1.QueueService/ReceiveDeferred", engine.KeyListen},
		{"/mqlite.v1.QueueService/Renew", engine.KeyListen},
		{"/mqlite.v1.QueueService/Schedule", engine.KeySend},
		{"/mqlite.v1.QueueService/Cancel", engine.KeySend},
		{"/mqlite.v1.QueueService/Peek", engine.KeyListen},
		{"/mqlite.v1.QueueService/Stats", engine.KeyListen},
		{"/mqlite.v1.AdminService/CreateQueue", engine.KeyManage},
		{"/mqlite.v1.AdminService/Subscribe", engine.KeyManage},
		{"/mqlite.v1.AdminService/ListQueues", engine.KeyManage},
		{"/mqlite.v1.AdminService/ListSubscriptions", engine.KeyManage},
		{"/mqlite.v1.AdminService/TestFilter", engine.KeyManage},
		{"/mqlite.v1.AdminService/Redrive", engine.KeyManage},
		{"/mqlite.v1.AdminService/Purge", engine.KeyManage},
		{"/mqlite.v1.AdminService/Status", engine.KeyManage},
		{"/mqlite.v1.AdminService/Observe", engine.KeyManage},
		{"/mqlite.v1.AuthService/CreateKey", engine.KeyManage},
		{"/mqlite.v1.AuthService/ListKeys", engine.KeyManage},
		{"/mqlite.v1.AuthService/RevokeKey", engine.KeyManage},
	}
	inventory := keyTestRequest(server.New(keyTestEngine(t, nil), []string{"admin"}).Handler(), http.MethodGet, "/", "", nil)
	var card wire.DiscoveryCard
	if err := json.Unmarshal(inventory.Body.Bytes(), &card); err != nil {
		t.Fatal(err)
	}
	paths := make([]string, len(routes))
	for i := range paths {
		paths[i] = routes[i].path
	}
	if !reflect.DeepEqual(card.Endpoints, paths) {
		t.Fatalf("complete route inventory drift:\n got %v\nwant %v", card.Endpoints, paths)
	}
	for _, identity := range []string{"anonymous", "invalid", "send", "listen", "send+listen", "manage", "static", "monitor", "expired", "revoked", "auth-off"} {
		for _, route := range routes {
			t.Run(identity+"/"+route.path, func(t *testing.T) {
				ctx := context.Background()
				now := int64(1000000)
				eng := keyTestEngine(t, func() int64 { return now })
				permission := engine.KeyManage
				token := "admin"
				switch identity {
				case "anonymous", "auth-off":
					token = ""
				case "invalid":
					token = "mqk_" + strings.Repeat("0", 64)
				case "send":
					permission = engine.KeySend
				case "listen":
					permission = engine.KeyListen
				case "send+listen":
					permission = engine.KeySend | engine.KeyListen
				}
				if identity != "anonymous" && identity != "invalid" && identity != "static" && identity != "auth-off" {
					expiry := int64(0)
					if identity == "expired" {
						expiry = now + 1
					}
					key, secret := keyTestCreate(t, eng, 1, permission, expiry)
					token = secret
					if identity == "expired" {
						now++
					}
					if identity == "revoked" {
						if err := eng.RevokeAccessKey(ctx, key.ID); err != nil {
							t.Fatal(err)
						}
					}
				}
				victim, _ := keyTestCreate(t, eng, 2, engine.KeySend, 0)
				for _, queue := range []string{"q", "target"} {
					if err := eng.CreateQueue(ctx, queue, engine.QueueConfig{}); err != nil {
						t.Fatal(err)
					}
				}
				_, err := eng.Send(ctx, "q", engine.OutMessage{Body: []byte("locked")}, engine.OutMessage{Body: []byte("deferred")}, engine.OutMessage{Body: []byte("dead")}, engine.OutMessage{Body: []byte("active")})
				if err != nil {
					t.Fatal(err)
				}
				locked, err := eng.Receive(ctx, "q", engine.ReceiveOptions{MaxMessages: 3})
				if err != nil || len(locked) != 3 {
					t.Fatalf("receive fixture: %v %v", locked, err)
				}
				if err := eng.Defer(ctx, "q", locked[1].SeqNumber, locked[1].LockToken); err != nil {
					t.Fatal(err)
				}
				if err := eng.Reject(ctx, "q", locked[2].SeqNumber, locked[2].LockToken, "test", ""); err != nil {
					t.Fatal(err)
				}
				scheduled, err := eng.Schedule(ctx, "q", engine.OutMessage{Body: []byte("scheduled")}, now+60000)
				if err != nil {
					t.Fatal(err)
				}
				settle := wire.SettleRequest{Queue: "q", SeqNumber: locked[0].SeqNumber, LockToken: locked[0].LockToken}
				batch := []wire.SettleItem{{SeqNumber: locked[0].SeqNumber, LockToken: locked[0].LockToken}}
				var body any = wire.Empty{}
				switch route.path {
				case wire.PathSend:
					body = wire.SendRequest{Queue: "q", Messages: []wire.Message{{Body: []byte("a")}, {Body: []byte("b")}}}
				case wire.PathSchedule:
					body = wire.SendRequest{Queue: "q", Messages: []wire.Message{{Body: []byte("a")}, {Body: []byte("b")}}, ScheduledEnqueueTimeMs: now + 60000}
				case wire.PathReceive:
					body = wire.ReceiveRequest{Queue: "q", MaxMessages: 2, AttemptID: "attempt"}
				case wire.PathReceiveDeferred:
					body = wire.ReceiveDeferredRequest{Queue: "q", SeqNumbers: []int64{locked[1].SeqNumber}}
				case wire.PathComplete, wire.PathAbandon, wire.PathReject, wire.PathDefer, wire.PathRenew:
					body = settle
				case wire.PathCompleteBatch:
					body = wire.CompleteBatchRequest{Queue: "q", Messages: batch}
				case wire.PathRenewBatch:
					body = wire.RenewBatchRequest{Queue: "q", Messages: batch}
				case wire.PathCancel:
					body = wire.CancelRequest{Queue: "q", SeqNumber: scheduled}
				case wire.PathPeek:
					body = wire.PeekRequest{Queue: "q"}
				case wire.PathStats:
					body = wire.MetricsRequest{Queue: "q"}
				case wire.PathCreateQueue:
					body = wire.CreateQueueRequest{Name: "new"}
				case wire.PathSubscribe:
					body = wire.SubscribeRequest{Topic: "topic", Name: "sub"}
				case wire.PathTestFilter:
					body = wire.TestFilterRequest{Expr: "true"}
				case wire.PathRedrive:
					body = wire.RedriveRequest{Queue: "q", Target: "target", Max: 1}
				case wire.PathPurge:
					body = wire.PurgeRequest{Queue: "q", Max: 1}
				case wire.PathCreateKey:
					body = wire.CreateKeyRequest{ID: fmt.Sprintf("%032x", 3), Name: "new", Permissions: []string{"send", "listen", "manage"}}
				case wire.PathListKeys:
					body = wire.ListKeysRequest{}
				case wire.PathRevokeKey:
					body = wire.RevokeKeyRequest{ID: victim.ID}
				}
				snapshot := func() []byte {
					queues, err := eng.ListQueues(ctx)
					if err != nil {
						t.Fatal(err)
					}
					subs, err := eng.ListSubscriptions(ctx)
					if err != nil {
						t.Fatal(err)
					}
					keys, err := eng.ListAccessKeys(ctx, "", 100)
					if err != nil {
						t.Fatal(err)
					}
					messages := map[string][]*engine.PeekedMessage{}
					for _, queue := range queues {
						messages[queue.Name], err = eng.Peek(ctx, queue.Name, engine.PeekOptions{Max: 1000})
						if err != nil {
							t.Fatal(err)
						}
					}
					b, err := json.Marshal([]any{queues, subs, keys, messages, eng.CompletedCounts()})
					if err != nil {
						t.Fatal(err)
					}
					return b
				}
				before := snapshot()
				tokens := []string{"admin"}
				if identity == "auth-off" {
					tokens = nil
				}
				srv := server.New(eng, tokens)
				if identity == "monitor" {
					srv.MonitorTokens = []string{"monitor"}
					token = "monitor"
				}
				srv.CORS = "*"
				rec := keyTestRequest(srv.Handler(), http.MethodPost, route.path, token, body)
				want, code := http.StatusOK, ""
				switch identity {
				case "anonymous", "invalid", "expired", "revoked":
					want, code = http.StatusUnauthorized, "unauthenticated"
				case "auth-off":
					if strings.HasPrefix(route.path, "/mqlite.v1.AuthService/") || route.path == wire.PathObserve {
						want, code = http.StatusForbidden, "permission_denied"
					}
				case "monitor":
					if route.path != wire.PathObserve {
						want, code = http.StatusForbidden, "permission_denied"
					}
				default:
					if !permission.Allows(route.permission) {
						want, code = http.StatusForbidden, "permission_denied"
					}
				}
				keyTestStatus(t, rec, want, code)
				if rec.Header().Get("Access-Control-Allow-Origin") != "*" {
					t.Fatal("CORS must survive auth failures")
				}
				if want != http.StatusOK && !bytes.Equal(before, snapshot()) {
					t.Fatal("denied request changed persisted message/key state")
				}
			})
		}
	}
}

func TestAccessKeyStrictBearerAndOpenPaths(t *testing.T) {
	eng := keyTestEngine(t, nil)
	_, token := keyTestCreate(t, eng, 1, engine.KeySend, 0)
	srv := server.New(eng, []string{"Legacy-Admin"})
	srv.CORS = "*"
	srv.UI = true
	srv.Metrics = true
	h := srv.Handler()
	for _, tt := range []struct {
		name    string
		headers []string
		status  int
	}{
		{"missing", nil, 401}, {"empty", []string{""}, 401},
		{"bare static", []string{"Legacy-Admin"}, 401}, {"bare dynamic", []string{token}, 401},
		{"basic", []string{"Basic Legacy-Admin"}, 401}, {"empty bearer", []string{"Bearer "}, 401},
		{"leading space", []string{" Bearer Legacy-Admin"}, 401}, {"tab separator", []string{"Bearer\tLegacy-Admin"}, 401},
		{"trailing space", []string{"Bearer Legacy-Admin "}, 401},
		{"duplicate valid", []string{"Bearer Legacy-Admin", "Bearer Legacy-Admin"}, 401},
		{"duplicate mixed", []string{"Bearer Legacy-Admin", "Bearer wrong"}, 401},
		{"duplicate empty", []string{"Bearer Legacy-Admin", ""}, 401},
		{"combined", []string{"Bearer Legacy-Admin, Bearer Legacy-Admin"}, 401},
		{"wrong case token", []string{"Bearer legacy-admin"}, 401},
		{"legacy static", []string{"Bearer Legacy-Admin"}, 200},
		{"case insensitive scheme", []string{"bEaReR Legacy-Admin"}, 200},
		{"multiple scheme spaces", []string{"Bearer   Legacy-Admin"}, 200},
		{"restricted dynamic", []string{"Bearer " + token}, 403},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, route := range []struct{ method, path string }{{http.MethodPost, wire.PathListKeys}, {http.MethodGet, "/metrics"}} {
				req := httptest.NewRequest(route.method, route.path, strings.NewReader(`{}`))
				for _, header := range tt.headers {
					req.Header.Add("Authorization", header)
				}
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				keyTestStatus(t, rec, tt.status, "")
				if strings.Contains(rec.Body.String(), token) || strings.Contains(rec.Body.String(), "Legacy-Admin") {
					t.Fatal("credential exposed in response")
				}
			}
		})
	}
	for _, tok := range []string{"", "wrong", token, "Legacy-Admin"} {
		for _, tt := range []struct {
			path   string
			status int
		}{{"/", 200}, {"/healthz", 200}, {"/ui", 301}, {"/ui/", 200}} {
			keyTestStatus(t, keyTestRequest(h, http.MethodGet, tt.path, tok, nil), tt.status, "")
		}
		for _, path := range []string{"/uixyz", "/healthz-extra", "/unknown", "/mqlite.v1.AuthService/Unknown"} {
			want := 404
			if tok == "" || tok == "wrong" {
				want = 401
			}
			keyTestStatus(t, keyTestRequest(h, http.MethodPost, path, tok, nil), want, "")
		}
		// All authenticated callers get method errors only after route authorization.
		want := 401
		if tok == token {
			want = 403
		}
		if tok == "Legacy-Admin" {
			want = 405
		}
		keyTestStatus(t, keyTestRequest(h, http.MethodGet, wire.PathListKeys, tok, nil), want, "")
		for _, path := range append(append([]string{}, wantRPCRoutes...), "/uixyz", "/metrics") {
			req := httptest.NewRequest(http.MethodOptions, path, nil)
			req.Header.Set("Origin", "https://example.test")
			req.Header.Set("Access-Control-Request-Method", "POST")
			req.Header.Set("Authorization", tok)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			keyTestStatus(t, rec, 204, "")
		}
	}
}

func TestAccessKeyManagementLifecycleAndSecrecy(t *testing.T) {
	ctx := context.Background()
	eng := keyTestEngine(t, func() int64 { return 1000000 })
	var logs bytes.Buffer
	srv := server.New(eng, []string{"admin"})
	srv.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	h := srv.Handler()
	keyTestStatus(t, keyTestRequest(h, http.MethodPost, wire.PathListKeys, "admin", wire.ListKeysRequest{}), 200, "")
	empty := keyTestRequest(h, http.MethodPost, wire.PathListKeys, "admin", wire.ListKeysRequest{})
	if strings.TrimSpace(empty.Body.String()) != `{"keys":[]}` {
		t.Fatalf("empty list = %s", empty.Body)
	}
	parentRequest := wire.CreateKeyRequest{ID: fmt.Sprintf("%032x", 1), Name: "ops", Permissions: []string{"send", "listen", "manage"}}
	rec := keyTestRequest(h, http.MethodPost, wire.PathCreateKey, "admin", parentRequest)
	keyTestStatus(t, rec, 200, "")
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("credential response must not be cached")
	}
	var parent wire.CreateKeyResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &parent); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(parent.Key.Permissions, []string{"manage"}) || len(parent.Token) != 68 {
		t.Fatalf("create result: %+v", parent.Key)
	}
	childRequest := wire.CreateKeyRequest{ID: fmt.Sprintf("%032x", 2), Name: "ops", Permissions: []string{"manage"}}
	rec = keyTestRequest(h, http.MethodPost, wire.PathCreateKey, parent.Token, childRequest)
	keyTestStatus(t, rec, 200, "")
	var child wire.CreateKeyResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &child); err != nil {
		t.Fatal(err)
	}
	keyTestStatus(t, keyTestRequest(h, http.MethodPost, wire.PathCreateKey, child.Token, childRequest), 409, "key_conflict")
	// A child administrator may revoke its issuer, itself, and any other database
	// administrator. Revocation is not inherited by keys that issuer created.
	keyTestStatus(t, keyTestRequest(h, http.MethodPost, wire.PathRevokeKey, child.Token, wire.RevokeKeyRequest{ID: parent.Key.ID}), 200, "")
	keyTestStatus(t, keyTestRequest(h, http.MethodPost, wire.PathListKeys, parent.Token, wire.ListKeysRequest{}), 401, "unauthenticated")
	page := keyTestRequest(h, http.MethodPost, wire.PathListKeys, child.Token, wire.ListKeysRequest{Limit: 1})
	keyTestStatus(t, page, 200, "")
	var first wire.ListKeysResponse
	if err := json.Unmarshal(page.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	if len(first.Keys) != 1 || first.Keys[0].ID != parent.Key.ID || first.NextAfterID != parent.Key.ID {
		t.Fatalf("first page: %+v", first)
	}
	page2 := keyTestRequest(h, http.MethodPost, wire.PathListKeys, child.Token, wire.ListKeysRequest{AfterID: first.NextAfterID, Limit: 1})
	keyTestStatus(t, page2, 200, "")
	var second wire.ListKeysResponse
	if err := json.Unmarshal(page2.Body.Bytes(), &second); err != nil {
		t.Fatal(err)
	}
	if len(second.Keys) != 1 || second.Keys[0].ID != child.Key.ID || second.NextAfterID != "" {
		t.Fatalf("second page: %+v", second)
	}
	for _, path := range []string{wire.PathListKeys, wire.PathStatus, "/"} {
		result := keyTestRequest(h, http.MethodPost, path, child.Token, wire.ListKeysRequest{})
		for _, secret := range []string{parent.Token, child.Token, "token_hash"} {
			if strings.Contains(result.Body.String(), secret) {
				t.Fatalf("%s exposed secret material", path)
			}
		}
	}
	for _, path := range []string{wire.PathCreateKey, wire.PathListKeys, wire.PathRevokeKey} {
		for _, bad := range []string{`{`, `{"unknown":true}`, `{} {}`} {
			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(bad))
			req.Header.Set("Authorization", "Bearer "+child.Token)
			result := httptest.NewRecorder()
			h.ServeHTTP(result, req)
			keyTestStatus(t, result, 400, "invalid_argument")
		}
	}
	for _, tt := range []struct {
		path string
		body any
	}{
		{wire.PathCreateKey, wire.CreateKeyRequest{ID: "bad", Name: "test", Permissions: []string{"send"}}},
		{wire.PathCreateKey, wire.CreateKeyRequest{ID: fmt.Sprintf("%032x", 3), Name: "test", Permissions: []string{"manage", "unknown"}}},
		{wire.PathListKeys, wire.ListKeysRequest{Limit: -1}},
		{wire.PathListKeys, wire.ListKeysRequest{Limit: 1001}},
		{wire.PathListKeys, wire.ListKeysRequest{AfterID: "bad"}},
		{wire.PathRevokeKey, wire.RevokeKeyRequest{ID: "bad"}},
	} {
		keyTestStatus(t, keyTestRequest(h, http.MethodPost, tt.path, child.Token, tt.body), 400, "invalid_argument")
	}
	keyTestStatus(t, keyTestRequest(h, http.MethodPost, wire.PathRevokeKey, child.Token, wire.RevokeKeyRequest{ID: fmt.Sprintf("%032x", 99)}), 404, "not_found")
	keyTestStatus(t, keyTestRequest(h, http.MethodPost, wire.PathRevokeKey, child.Token, wire.RevokeKeyRequest{ID: child.Key.ID}), 200, "")
	keyTestStatus(t, keyTestRequest(h, http.MethodPost, wire.PathRevokeKey, child.Token, wire.RevokeKeyRequest{ID: child.Key.ID}), 401, "unauthenticated")
	keyTestStatus(t, keyTestRequest(h, http.MethodPost, wire.PathRevokeKey, "admin", wire.RevokeKeyRequest{ID: child.Key.ID}), 200, "")
	for _, secret := range []string{parent.Token, child.Token, "token_hash"} {
		if strings.Contains(logs.String(), secret) {
			t.Fatal("access log exposed secret material")
		}
	}
	// Static configuration is a separate administrator grant, taking precedence
	// over revocation and the permissions recorded in the database.
	_, restricted := keyTestCreate(t, eng, 3, engine.KeySend, 0)
	if err := eng.RevokeAccessKey(ctx, fmt.Sprintf("%032x", 3)); err != nil {
		t.Fatal(err)
	}
	static := server.New(eng, []string{restricted, parent.Token}).Handler()
	for _, token := range []string{restricted, parent.Token} {
		keyTestStatus(t, keyTestRequest(static, http.MethodPost, wire.PathListKeys, token, wire.ListKeysRequest{}), 200, "")
	}
}

func TestAccessKeyStorageFailureDoesNotBecomeAnonymous(t *testing.T) {
	eng := keyTestEngine(t, nil)
	_, dynamic := keyTestCreate(t, eng, 1, engine.KeyManage, 0)
	h := server.New(eng, []string{"admin"}).Handler()
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
	keyTestStatus(t, keyTestRequest(h, http.MethodPost, wire.PathTestFilter, dynamic, wire.TestFilterRequest{Expr: "true"}), 500, "internal")
	// A static administrator can still authenticate without a database lookup.
	keyTestStatus(t, keyTestRequest(h, http.MethodPost, wire.PathTestFilter, "admin", wire.TestFilterRequest{Expr: "true"}), 200, "")
	for _, tt := range []struct {
		path string
		body any
	}{
		{wire.PathCreateKey, wire.CreateKeyRequest{ID: fmt.Sprintf("%032x", 2), Name: "test", Permissions: []string{"send"}}},
		{wire.PathListKeys, wire.ListKeysRequest{}},
		{wire.PathRevokeKey, wire.RevokeKeyRequest{ID: fmt.Sprintf("%032x", 1)}},
	} {
		keyTestStatus(t, keyTestRequest(h, http.MethodPost, tt.path, "admin", tt.body), 500, "internal")
	}
}

// A gated body proves authentication has finished before revocation/expiry,
// without sleeps or observing a scheduler-dependent SQL timing window.
type keyGatedBody struct {
	io.Reader
	started chan struct{}
	proceed chan struct{}
	first   bool
}

func (b *keyGatedBody) Read(p []byte) (int, error) {
	if !b.first {
		b.first = true
		close(b.started)
		<-b.proceed
	}
	return b.Reader.Read(p)
}
func (b *keyGatedBody) Close() error { return nil }

func TestAccessKeyRevocationAndExpiryBoundaries(t *testing.T) {
	for _, boundary := range []string{"revoke", "expire"} {
		t.Run(boundary, func(t *testing.T) {
			ctx := context.Background()
			var now atomic.Int64
			now.Store(1000000)
			eng := keyTestEngine(t, now.Load)
			if err := eng.CreateQueue(ctx, "q", engine.QueueConfig{}); err != nil {
				t.Fatal(err)
			}
			key, token := keyTestCreate(t, eng, 1, engine.KeyListen, now.Load()+100)
			h := server.New(eng, []string{"admin"}).Handler()
			h2 := server.New(eng, []string{"admin"}).Handler()
			if _, err := eng.SendOne(ctx, "q", engine.OutMessage{Body: []byte("done")}); err != nil {
				t.Fatal(err)
			}
			receive := wire.ReceiveRequest{Queue: "q", MaxMessages: 1, AttemptID: "stable-attempt"}
			got := keyTestRequest(h, http.MethodPost, wire.PathReceive, token, receive)
			keyTestStatus(t, got, 200, "")
			var messages wire.ReceiveResponse
			if err := json.Unmarshal(got.Body.Bytes(), &messages); err != nil || len(messages.Messages) != 1 {
				t.Fatalf("receive: %v %s", err, got.Body)
			}
			message := messages.Messages[0]
			settle := wire.SettleRequest{Queue: "q", SeqNumber: message.SeqNumber, LockToken: message.LockToken}
			keyTestStatus(t, keyTestRequest(h, http.MethodPost, wire.PathComplete, token, settle), 200, "")
			body := &keyGatedBody{Reader: strings.NewReader(`{"queue":"q","wait_time_ms":1}`), started: make(chan struct{}), proceed: make(chan struct{})}
			req := httptest.NewRequest(http.MethodPost, wire.PathReceive, nil)
			req.Body = body
			req.Header.Set("Authorization", "Bearer "+token)
			done := make(chan *httptest.ResponseRecorder, 1)
			go func() { rec := httptest.NewRecorder(); h.ServeHTTP(rec, req); done <- rec }()
			select {
			case <-body.started:
			case <-time.After(5 * time.Second):
				t.Fatal("authorized body read never started")
			}
			if boundary == "revoke" {
				if err := eng.RevokeAccessKey(ctx, key.ID); err != nil {
					t.Fatal(err)
				}
			} else {
				now.Add(100)
			}
			close(body.proceed)
			// Delivery wakes the authorized long poll even after its credential changed.
			if _, err := eng.SendOne(ctx, "q", engine.OutMessage{Body: []byte("in-flight")}); err != nil {
				t.Fatal(err)
			}
			select {
			case rec := <-done:
				keyTestStatus(t, rec, 200, "")
			case <-time.After(5 * time.Second):
				t.Fatal("in-flight receive did not finish")
			}
			// Replay caches and batches cannot bypass a fresh authentication check,
			// including another Server wrapping the same Engine.
			for _, handler := range []http.Handler{h, h2} {
				for _, tt := range []struct {
					path string
					body any
				}{
					{wire.PathReceive, receive}, {wire.PathComplete, settle}, {wire.PathRenew, settle},
					{wire.PathCompleteBatch, wire.CompleteBatchRequest{Queue: "q", Messages: []wire.SettleItem{{SeqNumber: message.SeqNumber, LockToken: message.LockToken}}}},
					{wire.PathRenewBatch, wire.RenewBatchRequest{Queue: "q", Messages: []wire.SettleItem{{SeqNumber: message.SeqNumber, LockToken: message.LockToken}}}},
					{wire.PathReceive, wire.ReceiveRequest{Queue: "q", ReceiveMode: int(engine.ReceiveAndDelete)}},
				} {
					keyTestStatus(t, keyTestRequest(handler, http.MethodPost, tt.path, token, tt.body), 401, "unauthenticated")
				}
			}
		})
	}
}

func TestAccessKeyErrorsDoNotEchoSecrets(t *testing.T) {
	eng := keyTestEngine(t, nil)
	const static = "legacy-static-secret-for-redaction-check"
	key, token := keyTestCreate(t, eng, 1, engine.KeySend, 0)
	var logs bytes.Buffer
	srv := server.New(eng, []string{static})
	srv.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	h := srv.Handler()
	secrets := []string{static, token, fmt.Sprintf("%x", sha256.Sum256([]byte(token))), "token_hash"}
	for _, tt := range []struct {
		path, credential string
		body             any
		status           int
	}{
		{wire.PathCreateKey, static, wire.CreateKeyRequest{ID: token, Name: "test", Permissions: []string{"send"}}, 400},
		{wire.PathCreateKey, static, wire.CreateKeyRequest{ID: fmt.Sprintf("%032x", 2), Name: "test", Permissions: []string{token}}, 400},
		{wire.PathCreateKey, static, wire.CreateKeyRequest{ID: key.ID, Name: "test", Permissions: []string{"send"}}, 409},
		{wire.PathListKeys, static, wire.ListKeysRequest{AfterID: token}, 400},
		{wire.PathRevokeKey, static, wire.RevokeKeyRequest{ID: token}, 400},
		{wire.PathRevokeKey, static, wire.RevokeKeyRequest{ID: static}, 400},
		{wire.PathRevokeKey, static, wire.RevokeKeyRequest{ID: fmt.Sprintf("%032x", 99)}, 404},
		{wire.PathCreateKey, token, wire.CreateKeyRequest{}, 403},
		{wire.PathListKeys, token, wire.ListKeysRequest{}, 403},
		{wire.PathRevokeKey, token, wire.RevokeKeyRequest{}, 403},
		{wire.PathListKeys, token + "x", wire.ListKeysRequest{}, 401},
	} {
		rec := keyTestRequest(h, http.MethodPost, tt.path, tt.credential, tt.body)
		keyTestStatus(t, rec, tt.status, "")
		for _, secret := range secrets {
			if strings.Contains(rec.Body.String(), secret) {
				t.Fatalf("%s error echoed credential material", tt.path)
			}
		}
	}
	for _, secret := range secrets {
		if strings.Contains(logs.String(), secret) {
			t.Fatal("access log echoed credential material")
		}
	}
}

func TestAccessKeyCreationOrderHTTP(t *testing.T) {
	ctx := context.Background()
	now := int64(1_000_000)
	eng := keyTestEngine(t, func() int64 { return now })
	var keys []engine.AccessKey
	for i, id := range []int{90, 1, 50, 3, 80, 2, 40} {
		now = 1_000_000 + int64(i/2)
		key, _ := keyTestCreate(t, eng, id, engine.KeySend, 0)
		keys = append(keys, key)
	}
	handler := server.New(eng, []string{"admin"}).Handler()
	// Expected full order is independent of the query and spans four HTTP pages.
	expected := []string{keys[6].ID, keys[4].ID, keys[5].ID, keys[2].ID, keys[3].ID, keys[0].ID, keys[1].ID}
	var listed []string
	for after := ""; ; {
		req := wire.ListKeysRequest{Sort: engine.KeySortCreatedDesc, AfterID: after, Limit: 2}
		response := keyTestRequest(handler, http.MethodPost, wire.PathListKeys, "admin", req)
		keyTestStatus(t, response, 200, "")
		page, err := wire.DecodeListKeysResponse(response.Body.Bytes(), req)
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range page.Keys {
			listed = append(listed, key.ID)
		}
		if page.NextAfterID == "" {
			break
		}
		if err := eng.RevokeAccessKey(ctx, page.NextAfterID); err != nil {
			t.Fatal(err)
		}
		after = page.NextAfterID
	}
	if !reflect.DeepEqual(listed, expected) {
		t.Fatalf("HTTP sort ignored global timestamps/tie order: %v != %v", listed, expected)
	}
	for _, req := range []wire.ListKeysRequest{
		{Sort: "unknown"}, {Sort: "CREATED_DESC"},
		{Sort: engine.KeySortCreatedDesc, AfterID: fmt.Sprintf("%032x", 999)},
	} {
		keyTestStatus(t, keyTestRequest(handler, http.MethodPost, wire.PathListKeys, "admin", req), 400, "invalid_argument")
	}
	_, token := keyTestCreate(t, eng, 999, engine.KeySend, 0)
	keyTestStatus(t, keyTestRequest(handler, http.MethodPost, wire.PathListKeys, token, wire.ListKeysRequest{Sort: engine.KeySortCreatedDesc}), 403, "permission_denied")
	keyTestStatus(t, keyTestRequest(server.New(eng, nil).Handler(), http.MethodPost, wire.PathListKeys, "", wire.ListKeysRequest{Sort: engine.KeySortCreatedDesc}), 403, "permission_denied")
}
