package server_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/server"
	"github.com/mqlitehq/mqlite/wire"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestJSONResponsesCarryServerTime(t *testing.T) {
	const brokerNow = int64(1_800_000_000_123)
	eng, err := engine.Open(context.Background(), engine.Options{
		DB:                ":memory:",
		DisableBackground: true,
		Now:               func() int64 { return brokerNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	h := server.New(eng, nil).Handler()
	for _, tc := range []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{name: "success", method: http.MethodGet, path: "/"},
		{name: "error", method: http.MethodPost, path: wire.PathSend, body: "{"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body)))
			raw := rec.Header().Get(wire.ServerTimeHeader)
			got, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				t.Fatalf("%s = %q: %v", wire.ServerTimeHeader, raw, err)
			}
			if got != brokerNow {
				t.Fatalf("server time = %d, want injected broker clock %d", got, brokerNow)
			}
		})
	}

	misconfigured := server.New(eng, nil)
	misconfigured.MonitorTokens = []string{"monitor"}
	rec := httptest.NewRecorder()
	misconfigured.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if got := rec.Header().Get(wire.ServerTimeHeader); got != strconv.FormatInt(brokerNow, 10) {
		t.Fatalf("misconfiguration response %s = %q, want injected broker clock", wire.ServerTimeHeader, got)
	}
}

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
	check("unknown receive mode", http.MethodPost, wire.PathReceive, "secret",
		jsonOf(wire.ReceiveRequest{Queue: "q", ReceiveMode: 2, AttemptID: "unknown-mode"}),
		http.StatusBadRequest, "invalid_argument")
	if stats, err := eng.Stats(ctx, "q"); err != nil || stats.Active != 1 || stats.Locked != 0 {
		t.Fatalf("unknown receive mode mutated queue: %+v, %v", stats, err)
	}

	// Subscription filter contract: a malformed expr is rejected at Subscribe with
	// 400 invalid_argument (never stored); a valid one succeeds.
	sub := func(name, expr string) []byte {
		return jsonOf(wire.SubscribeRequest{Topic: "ev", Name: name, Filter: &engine.Filter{Expr: expr}})
	}
	check("bad filter expr", http.MethodPost, wire.PathSubscribe, "secret", sub("bad", "subject =="), http.StatusBadRequest, "invalid_argument")
	check("unknown filter field", http.MethodPost, wire.PathSubscribe, "secret", sub("bad2", `nope == "x"`), http.StatusBadRequest, "invalid_argument")
	check("valid filter", http.MethodPost, wire.PathSubscribe, "secret", sub("ok", `subject_parts[0] == "a"`), http.StatusOK, "")
	check("removed subscription generation", http.MethodPost, wire.PathSubscribe, "secret",
		[]byte(`{"topic":"ev","name":"generation","generation":"retired"}`), http.StatusBadRequest, "invalid_argument")
	check("invalid subscription TTL type", http.MethodPost, wire.PathSubscribe, "secret",
		[]byte(`{"topic":"ev","name":"ttl","ttl_ms":"1000"}`), http.StatusBadRequest, "invalid_argument")

	// Strict request validation (MQLITE-86): an unknown field or data after the JSON
	// body is a typed 400, not a silently-dropped typo; empty name / unknown enum map
	// to 400 invalid_argument instead of faulting a SQLite CHECK into a 500.
	check("unknown field", http.MethodPost, wire.PathSend, "secret",
		[]byte(`{"queue":"q","messsages":[]}`), http.StatusBadRequest, "invalid_argument")
	check("removed delete generation", http.MethodPost, wire.PathDeleteQueue, "secret",
		[]byte(`{"name":"q","generation":"retired"}`), http.StatusBadRequest, "invalid_argument")
	check("invalid delete force", http.MethodPost, wire.PathDeleteQueue, "secret",
		[]byte(`{"name":"q","force":"true"}`), http.StatusBadRequest, "invalid_argument")
	check("removed subscription delete generation", http.MethodPost, wire.PathDeleteSubscription, "secret",
		[]byte(`{"topic":"ev","name":"ok","generation":"retired"}`), http.StatusBadRequest, "invalid_argument")
	check("invalid subscription delete force", http.MethodPost, wire.PathDeleteSubscription, "secret",
		[]byte(`{"topic":"ev","name":"ok","force":"true"}`), http.StatusBadRequest, "invalid_argument")
	check("unknown subscription page field", http.MethodPost, wire.PathListSubscriptionsPage, "secret",
		[]byte(`{"topic":"ev","offset":1}`), http.StatusBadRequest, "invalid_argument")
	check("trailing data after body", http.MethodPost, wire.PathSend, "secret",
		[]byte(`{"queue":"q","messages":[]}{"queue":"q"}`), http.StatusBadRequest, "invalid_argument")
	check("empty queue name", http.MethodPost, wire.PathCreateQueue, "secret",
		jsonOf(wire.CreateQueueRequest{Name: ""}), http.StatusBadRequest, "invalid_argument")
	check("unknown ordering_mode", http.MethodPost, wire.PathCreateQueue, "secret",
		jsonOf(wire.CreateQueueRequest{Name: "vq", Config: wire.QueueConfigJSON{OrderingMode: "fifo"}}),
		http.StatusBadRequest, "invalid_argument")
	check("negative lock duration", http.MethodPost, wire.PathCreateQueue, "secret",
		jsonOf(wire.CreateQueueRequest{Name: "negative-lock", Config: wire.QueueConfigJSON{LockDurationMs: -1}}),
		http.StatusBadRequest, "invalid_argument")
	check("subsecond lock duration", http.MethodPost, wire.PathCreateQueue, "secret",
		jsonOf(wire.CreateQueueRequest{Name: "short-lock", Config: wire.QueueConfigJSON{LockDurationMs: 999}}),
		http.StatusBadRequest, "invalid_argument")
	check("minimum lock duration", http.MethodPost, wire.PathCreateQueue, "secret",
		jsonOf(wire.CreateQueueRequest{Name: "minimum-lock", Config: wire.QueueConfigJSON{LockDurationMs: 1000}}),
		http.StatusOK, "")
	check("maximum lock duration", http.MethodPost, wire.PathCreateQueue, "secret",
		jsonOf(wire.CreateQueueRequest{Name: "maximum-lock", Config: wire.QueueConfigJSON{LockDurationMs: 3_600_000}}),
		http.StatusOK, "")
	check("overlong lock duration", http.MethodPost, wire.PathCreateQueue, "secret",
		jsonOf(wire.CreateQueueRequest{Name: "overlong-lock", Config: wire.QueueConfigJSON{LockDurationMs: 3_600_001}}),
		http.StatusBadRequest, "invalid_argument")
	check("maximum integer lock duration", http.MethodPost, wire.PathCreateQueue, "secret",
		jsonOf(wire.CreateQueueRequest{Name: "integer-max-lock", Config: wire.QueueConfigJSON{LockDurationMs: 9_223_372_036_854_775_807}}),
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

// Every registered RPC accepts one request object and rejects unknown fields.
// The live discovery inventory keeps this contract test complete when a route is
// added; TestDiscoveryCardPinned independently pins that inventory.
func TestEveryRPCRejectsUnknownFields(t *testing.T) {
	eng := keyTestEngine(t, nil)
	h := server.New(eng, []string{"admin"}).Handler()
	inventory := keyTestRequest(h, http.MethodGet, "/", "", nil)
	keyTestStatus(t, inventory, http.StatusOK, "")
	var card wire.DiscoveryCard
	if err := json.Unmarshal(inventory.Body.Bytes(), &card); err != nil {
		t.Fatal(err)
	}
	if len(card.Endpoints) == 0 {
		t.Fatal("discovery returned no registered RPCs")
	}
	for _, path := range card.Endpoints {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, path,
				strings.NewReader(`{"__mqlite_unknown_field__":true}`))
			req.Header.Set("Authorization", "Bearer admin")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			keyTestStatus(t, rec, http.StatusBadRequest, "invalid_argument")
			var body wire.ErrorBody
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(body.Message, `unknown field "__mqlite_unknown_field__"`) {
				t.Fatalf("request was not rejected by strict unknown-field decoding: %q", body.Message)
			}
		})
	}
}

func TestEmptyRPCRequestObjectContract(t *testing.T) {
	eng := keyTestEngine(t, nil)
	srv := server.New(eng, []string{"admin"})
	srv.MaxBodyBytes = 32
	h := srv.Handler()
	paths := []string{
		wire.PathListQueues,
		wire.PathListSubscriptions,
		wire.PathStatus,
		wire.PathObserve,
	}
	cases := []struct {
		name   string
		body   string
		status int
		code   string
	}{
		{name: "object", body: `{}`, status: http.StatusOK},
		{name: "empty", body: ``, status: http.StatusBadRequest, code: "invalid_argument"},
		{name: "null", body: `null`, status: http.StatusBadRequest, code: "invalid_argument"},
		{name: "array", body: `[]`, status: http.StatusBadRequest, code: "invalid_argument"},
		{name: "scalar", body: `true`, status: http.StatusBadRequest, code: "invalid_argument"},
		{name: "trailing_object", body: `{} {}`, status: http.StatusBadRequest, code: "invalid_argument"},
		{name: "oversize_object", body: `{` + strings.Repeat(" ", 64) + `}`, status: http.StatusRequestEntityTooLarge, code: "message_too_large"},
	}
	for _, path := range paths {
		for _, tt := range cases {
			t.Run(path+"/"+tt.name, func(t *testing.T) {
				req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(tt.body))
				req.Header.Set("Authorization", "Bearer admin")
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				keyTestStatus(t, rec, tt.status, tt.code)
			})
		}
	}

	// Duplicate fields retain encoding/json's deterministic last-value-wins rule.
	srv.MaxBodyBytes = 1024
	req := httptest.NewRequest(http.MethodPost, wire.PathCreateQueue,
		strings.NewReader(`{"name":"ignored","name":"accepted","config":{}}`))
	req.Header.Set("Authorization", "Bearer admin")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	keyTestStatus(t, rec, http.StatusOK, "")
	queues, err := eng.ListQueues(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(queues) != 1 || queues[0].Name != "accepted" {
		t.Fatalf("duplicate request field did not keep its last value: %+v", queues)
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
		{"/mqlite.v1.AdminService/Subscribe", engine.KeyListen},
		{"/mqlite.v1.AdminService/ListQueues", engine.KeyManage},
		{"/mqlite.v1.AdminService/ListSubscriptions", engine.KeyManage},
		{wire.PathListSubscriptionsPage, engine.KeyManage},
		{"/mqlite.v1.AdminService/TestFilter", engine.KeyListen},
		{"/mqlite.v1.AdminService/Redrive", engine.KeyManage},
		{"/mqlite.v1.AdminService/Purge", engine.KeyManage},
		{"/mqlite.v1.AdminService/Status", engine.KeyManage},
		{"/mqlite.v1.AdminService/Observe", engine.KeyManage},
		{"/mqlite.v1.AuthService/CreateKey", engine.KeyManage},
		{"/mqlite.v1.AuthService/ListKeys", engine.KeyManage},
		{"/mqlite.v1.AuthService/RevokeKey", engine.KeyManage},
		{wire.PathDeleteQueue, engine.KeyManage},
		{wire.PathDeleteSubscription, engine.KeyManage},
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
				if err := eng.Subscribe(ctx, "topic", "sub", nil); err != nil {
					t.Fatal(err)
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
				case wire.PathDeleteQueue:
					body = wire.DeleteQueueRequest{Name: "target", Force: true}
				case wire.PathDeleteSubscription:
					body = wire.DeleteSubscriptionRequest{Topic: "topic", Name: "sub", Force: true}
				case wire.PathCreateQueue:
					body = wire.CreateQueueRequest{Name: "new"}
				case wire.PathSubscribe:
					body = wire.SubscribeRequest{Topic: "topic", Name: "sub"}
				case wire.PathListSubscriptionsPage:
					body = wire.ListSubscriptionsPageRequest{Limit: 1}
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

// TestListenSubscriptionProvisioningBoundary pins the deliberately asymmetric
// application contract: a trusted broker-wide listener may provision and test a
// subscription, while inventory and both deletion modes remain administrative.
func TestListenSubscriptionProvisioningBoundary(t *testing.T) {
	ctx := context.Background()
	now := int64(1_000_000)
	eng := keyTestEngine(t, func() int64 { return now })
	_, listenToken := keyTestCreate(t, eng, 1, engine.KeyListen, 0)
	_, sendToken := keyTestCreate(t, eng, 2, engine.KeySend, 0)
	h := server.New(eng, []string{"admin"}).Handler()

	const (
		topic = "requests"
		name  = "temporary"
		ttl   = int64(60_000)
	)
	subscribe := func(token, expr string) *httptest.ResponseRecorder {
		return keyTestRequest(h, http.MethodPost, wire.PathSubscribe, token, wire.SubscribeWithOptionsRequest{
			Topic: topic, Name: name, Filter: &engine.Filter{Expr: expr}, TTLMs: ttl,
		})
	}

	keyTestStatus(t, subscribe(listenToken, `subject == "first"`), http.StatusOK, "")
	page, err := eng.ListSubscriptionsWithOptions(ctx, engine.ListSubscriptionsOptions{Topic: topic})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Subscriptions) != 1 {
		t.Fatalf("subscriptions = %+v, want one", page.Subscriptions)
	}
	createdAt, expiresAt := page.Subscriptions[0].CreatedAtMs, page.Subscriptions[0].ExpiresAtMs
	if createdAt != now || expiresAt != now+ttl {
		t.Fatalf("lifetime = created %d expires %d, want %d/%d", createdAt, expiresAt, now, now+ttl)
	}

	now += 1_000
	keyTestStatus(t, subscribe(listenToken, `subject == "second"`), http.StatusOK, "")
	page, err = eng.ListSubscriptionsWithOptions(ctx, engine.ListSubscriptionsOptions{Topic: topic})
	if err != nil {
		t.Fatal(err)
	}
	got := page.Subscriptions[0]
	if got.Expr != `subject == "second"` || got.CreatedAtMs != createdAt || got.ExpiresAtMs != expiresAt {
		t.Fatalf("listener upsert changed the wrong fields: %+v", got)
	}

	filter := keyTestRequest(h, http.MethodPost, wire.PathTestFilter, listenToken, wire.TestFilterRequest{
		Expr: `subject == "second"`, Message: &wire.Message{Subject: "second"},
	})
	keyTestStatus(t, filter, http.StatusOK, "")
	var filterResult wire.TestFilterResponse
	if err := json.Unmarshal(filter.Body.Bytes(), &filterResult); err != nil {
		t.Fatal(err)
	}
	if !filterResult.Valid || !filterResult.Ran || !filterResult.Matched {
		t.Fatalf("unexpected filter result: %+v", filterResult)
	}

	for _, tc := range []struct {
		name string
		path string
		body any
	}{
		{"legacy list", wire.PathListSubscriptions, wire.Empty{}},
		{"paged list", wire.PathListSubscriptionsPage, wire.ListSubscriptionsPageRequest{Topic: topic}},
		{"create queue", wire.PathCreateQueue, wire.CreateQueueRequest{Name: "forbidden"}},
		{"delete empty-only", wire.PathDeleteSubscription, wire.DeleteSubscriptionRequest{Topic: topic, Name: name}},
		{"delete force", wire.PathDeleteSubscription, wire.DeleteSubscriptionRequest{Topic: topic, Name: name, Force: true}},
	} {
		t.Run("listen cannot "+tc.name, func(t *testing.T) {
			keyTestStatus(t, keyTestRequest(h, http.MethodPost, tc.path, listenToken, tc.body),
				http.StatusForbidden, "permission_denied")
		})
	}
	for _, tc := range []struct {
		name string
		path string
		body any
	}{
		{"subscribe", wire.PathSubscribe, wire.SubscribeWithOptionsRequest{Topic: "other", Name: "other"}},
		{"test filter", wire.PathTestFilter, wire.TestFilterRequest{Expr: "true"}},
	} {
		t.Run("send cannot "+tc.name, func(t *testing.T) {
			keyTestStatus(t, keyTestRequest(h, http.MethodPost, tc.path, sendToken, tc.body),
				http.StatusForbidden, "permission_denied")
		})
	}

	page, err = eng.ListSubscriptionsWithOptions(ctx, engine.ListSubscriptionsOptions{Topic: topic, IncludeDeleting: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Subscriptions) != 1 || page.Subscriptions[0].State != "active" {
		t.Fatalf("denied administration changed subscription: %+v", page.Subscriptions)
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
	const (
		static  = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		monitor = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	key, token := keyTestCreate(t, eng, 1, engine.KeySend, 0)
	var logs bytes.Buffer
	srv := server.New(eng, []string{static})
	srv.MonitorTokens = []string{"  " + monitor + "  "}
	srv.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	h := srv.Handler()
	secrets := []string{static, monitor, token, fmt.Sprintf("%x", sha256.Sum256([]byte(token))), "token_hash"}
	for _, tt := range []struct {
		path, credential string
		body             any
		status           int
	}{
		{wire.PathCreateKey, static, wire.CreateKeyRequest{ID: token, Name: "test", Permissions: []string{"send"}}, 400},
		{wire.PathCreateKey, static, wire.CreateKeyRequest{ID: static, Name: "test", Permissions: []string{token}}, 400},
		{wire.PathCreateKey, static, wire.CreateKeyRequest{ID: fmt.Sprintf("%032x", 2), Name: "test", Permissions: []string{token}}, 400},
		{wire.PathCreateKey, static, wire.CreateKeyRequest{ID: key.ID, Name: "test", Permissions: []string{"send"}}, 409},
		{wire.PathListKeys, static, wire.ListKeysRequest{AfterID: token}, 400},
		{wire.PathRevokeKey, static, wire.RevokeKeyRequest{ID: token}, 400},
		{wire.PathRevokeKey, static, wire.RevokeKeyRequest{ID: static}, 404},
		{wire.PathRevokeKey, static, wire.RevokeKeyRequest{ID: monitor}, 404},
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

func TestQueueNotFoundKeepsCompatibleCodeAndResource(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	recorder := httptest.NewRecorder()
	body, err := json.Marshal(wire.MetricsRequest{Queue: "missing"})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, wire.PathStats, bytes.NewReader(body))
	server.New(eng, nil).Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", recorder.Code)
	}
	var envelope wire.ErrorBody
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Code != "not_found" || envelope.Resource != "queue" {
		t.Fatalf("error envelope = %+v, want compatible not_found with queue resource", envelope)
	}
}

// wantRPCRoutes is the complete RPC catalog the open "/" discovery card must enumerate,
// in server-registration order. It IS the pinned contract (MQLITE-87): the card's
// endpoints come straight from what server.routes() registered, so adding or removing a
// route changes this list and fails the test until it's updated deliberately — an agent
// iterating endpoints[] can trust it lists every route, with none stale.
var wantRPCRoutes = []string{
	wire.PathSend, wire.PathReceive, wire.PathComplete, wire.PathCompleteBatch,
	wire.PathRenewBatch,
	wire.PathAbandon, wire.PathReject, wire.PathDefer, wire.PathReceiveDeferred,
	wire.PathRenew, wire.PathSchedule, wire.PathCancel, wire.PathPeek, wire.PathStats,
	wire.PathCreateQueue, wire.PathSubscribe, wire.PathListQueues, wire.PathListSubscriptions,
	wire.PathListSubscriptionsPage,
	wire.PathTestFilter, wire.PathRedrive, wire.PathPurge, wire.PathStatus, wire.PathObserve,
	wire.PathCreateKey, wire.PathListKeys, wire.PathRevokeKey, wire.PathDeleteQueue,
	wire.PathDeleteSubscription,
}

func getCard(t *testing.T, url string) (wire.DiscoveryCard, []string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	// Key set of the raw object — catches any field added/removed from the shape.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatalf("decode raw %q: %v", body, err)
	}
	keys := make([]string, 0, len(raw))
	for k := range raw {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var card wire.DiscoveryCard
	if err := json.Unmarshal(body, &card); err != nil {
		t.Fatalf("decode card: %v", err)
	}
	return card, keys
}

// TestDiscoveryCardPinned pins the exact "/" JSON contract agents rely on: a string
// auth ("bearer"/"none"), a complete endpoints[] catalog, and a fixed field set — so
// the server can't silently drift from the documented shape (MQLITE-87 / D4).
func TestDiscoveryCardPinned(t *testing.T) {
	// Pin both optional fields independently. Metrics require authentication;
	// that invalid combination fails closed rather than publishing a card.
	for _, auth := range []bool{false, true} {
		for _, ui := range []bool{false, true} {
			for _, metrics := range []bool{false, true} {
				name := "anonymous"
				if auth {
					name = "bearer"
				}
				if ui {
					name += "/console"
				}
				if metrics {
					name += "/metrics"
				}
				t.Run(name, func(t *testing.T) {
					eng, err := engine.Open(context.Background(), engine.Options{DB: ":memory:", DisableBackground: true})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { eng.Close() })
					var tokens []string
					wantAuth := "none"
					if auth {
						tokens = []string{"secret"}
						wantAuth = "bearer"
					}
					srv := server.New(eng, tokens)
					srv.UI, srv.Metrics = ui, metrics
					h := srv.Handler()
					if metrics && !auth {
						keyTestStatus(t, keyTestRequest(h, http.MethodGet, "/", "", nil), http.StatusInternalServerError, "internal")
						return
					}
					ts := httptest.NewServer(h)
					t.Cleanup(ts.Close)
					card, keys := getCard(t, ts.URL+"/")
					wantKeys := []string{"auth", "description", "docs", "endpoints", "health", "name", "status", "version"}
					wantUI, wantMetrics := "", ""
					if ui {
						wantKeys = append(wantKeys, "ui")
						wantUI = "/ui"
					}
					if metrics {
						wantKeys = append(wantKeys, "metrics")
						wantMetrics = "/metrics"
					}
					sort.Strings(wantKeys)
					if !reflect.DeepEqual(keys, wantKeys) {
						t.Errorf("card field set = %v\n            want %v", keys, wantKeys)
					}
					if card.Name != "mqlite" || card.Status != "ok" || card.Health != "/healthz" || card.Metrics != wantMetrics || card.UI != wantUI || card.Auth != wantAuth {
						t.Errorf("card metadata drift: %+v", card)
					}
					if !reflect.DeepEqual(card.Endpoints, wantRPCRoutes) {
						t.Errorf("endpoints catalog drift:\n got  %v\n want %v", card.Endpoints, wantRPCRoutes)
					}
					// The advertised relative URL is live on this same handler, and
					// the exporter remains independent of the optional console.
					for _, credential := range []string{"", "secret"} {
						want := http.StatusNotFound
						if metrics {
							want = http.StatusUnauthorized
							if credential != "" {
								want = http.StatusOK
							}
						}
						keyTestStatus(t, keyTestRequest(h, http.MethodGet, "/metrics", credential, nil), want, "")
					}
					wantUIStatus := http.StatusNotFound
					if ui {
						wantUIStatus = http.StatusOK
					}
					keyTestStatus(t, keyTestRequest(h, http.MethodGet, "/ui/", "", nil), wantUIStatus, "")
				})
			}
		}
	}
}

// Round-3 §3.3: the docs named `/mqlite.v1.Queue/Receive`, a route that does not exist (it is
// `/mqlite.v1.QueueService/Receive`). Rather than fix the three occurrences and hope, pin the
// whole surface: EVERY RPC path written in the docs must be a route the broker actually
// registers. Invent one and this fails.
//
// The comparison is against the live catalog the server publishes on "/", so there is no second
// list to keep in sync — the routes are their own source of truth.
func TestDocsCiteOnlyRealRPCPaths(t *testing.T) {
	card, _ := getCard(t, consoleServer(t, false, nil).URL+"/")
	paths := card.Endpoints
	real := make(map[string]bool, len(paths))
	for _, p := range paths {
		real[p] = true
	}

	docs, err := filepath.Glob(filepath.Join("..", "docs", "*.md"))
	if err != nil {
		t.Fatal(err)
	}
	docs = append(docs, filepath.Join("..", "README.md"))
	// Also cover the source comments, which is where the bad path was copied from.
	more, _ := filepath.Glob(filepath.Join("..", "cmd", "mqlite", "*.go"))
	docs = append(docs, more...)

	re := regexp.MustCompile(`/mqlite\.v1\.[A-Za-z]+/[A-Za-z]+`)
	checked := 0
	for _, f := range docs {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		for _, cited := range re.FindAllString(string(b), -1) {
			checked++
			if !real[cited] {
				t.Errorf("%s cites %q, which the broker does not serve; the registered routes are %v",
					filepath.Base(f), cited, paths)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no RPC paths found in the docs — the regex or the paths moved; this guard is not guarding")
	}
}

// The reverse guard: every route the broker SERVES must be documented in the HTTP reference.
//
// TestDocsCiteOnlyRealRPCPaths stops the docs inventing a route that does not exist. This stops
// the opposite drift — shipping a route nobody can find. RenewBatch was added to the wire, the
// server, the SDK and the CLI, and the api-reference simply skipped it: raw-HTTP users could only
// discover it by reading Go source (codex). A guard in one direction is not a guard.
func TestEveryRPCPathIsDocumented(t *testing.T) {
	card, _ := getCard(t, consoleServer(t, false, nil).URL+"/")

	b, err := os.ReadFile(filepath.Join("..", "docs", "api-reference.md"))
	if err != nil {
		t.Fatalf("read api-reference.md: %v", err)
	}
	doc := string(b)

	// The reference documents operations under `###` headings, and related verbs may share one
	// (`### Complete / Abandon / Reject / Defer / Renew`), so collect every name any heading covers.
	documented := map[string]bool{}
	for _, line := range strings.Split(doc, "\n") {
		if !strings.HasPrefix(line, "### ") {
			continue
		}
		for _, part := range strings.Split(strings.TrimPrefix(line, "### "), "/") {
			documented[strings.TrimSpace(part)] = true
		}
	}

	for _, p := range card.Endpoints {
		name := p[strings.LastIndexByte(p, '/')+1:] // the operation is the last element of the route
		if !documented[name] {
			t.Errorf("the broker serves %s but docs/api-reference.md documents no %q operation — a raw-HTTP user cannot find it",
				p, name)
		}
	}
}

// The open "/" discovery endpoint returns a JSON identity card with NO auth — even
// when Bearer tokens are configured — so hitting the broker root tells you what it is.
func TestIndexDiscovery(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer eng.Close()

	srv := server.New(eng, []string{"secret"}) // auth ON
	srv.Version = "9.9.9"
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/") // no Authorization header
	if err != nil {
		t.Fatalf("get /: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET / status: want 200 (open), got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type: want application/json, got %q", ct)
	}
	var card struct {
		Name, Version, Status, Auth string
	}
	body, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &card); err != nil {
		t.Fatalf("decode: %v (%s)", err, body)
	}
	// auth is a string ("bearer" when tokens are set) so an agent can match it; the
	// exact card shape + full endpoint catalog are pinned in TestDiscoveryCardPinned.
	if card.Name != "mqlite" || card.Status != "ok" || card.Version != "9.9.9" || card.Auth != "bearer" {
		t.Fatalf("unexpected index card: %+v (%s)", card, body)
	}
}

func TestIndexNegotiatesBrowserConsole(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer eng.Close()

	srv := server.New(eng, nil)
	srv.UI = true
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	client := *ts.Client()
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}

	browserAccept := "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
	tests := []struct {
		name         string
		method       string
		path         string
		accept       string
		wantStatus   int
		wantLocation string
		wantJSON     bool
		wantVary     bool
	}{
		{name: "no accept", method: http.MethodGet, path: "/", wantStatus: http.StatusOK, wantJSON: true, wantVary: true},
		{name: "wildcard", method: http.MethodGet, path: "/", accept: "*/*", wantStatus: http.StatusOK, wantJSON: true, wantVary: true},
		{name: "json", method: http.MethodGet, path: "/", accept: "application/json", wantStatus: http.StatusOK, wantJSON: true, wantVary: true},
		{name: "browser", method: http.MethodGet, path: "/", accept: browserAccept, wantStatus: http.StatusTemporaryRedirect, wantLocation: "/ui/", wantVary: true},
		{name: "browser head", method: http.MethodHead, path: "/", accept: browserAccept, wantStatus: http.StatusTemporaryRedirect, wantLocation: "/ui/", wantVary: true},
		{name: "html excluded", method: http.MethodGet, path: "/", accept: "text/html;q=0,*/*;q=1", wantStatus: http.StatusOK, wantJSON: true, wantVary: true},
		{name: "json preferred", method: http.MethodGet, path: "/", accept: "text/html;q=0.5, application/json;q=0.9", wantStatus: http.StatusOK, wantJSON: true, wantVary: true},
		{name: "html preferred", method: http.MethodGet, path: "/", accept: "application/json;q=0.5, text/html", wantStatus: http.StatusTemporaryRedirect, wantLocation: "/ui/", wantVary: true},
		{name: "exact tie favors json", method: http.MethodGet, path: "/", accept: "text/html, application/json", wantStatus: http.StatusOK, wantJSON: true, wantVary: true},
		{name: "text wildcard is not navigation", method: http.MethodGet, path: "/", accept: "text/*", wantStatus: http.StatusOK, wantJSON: true, wantVary: true},
		{name: "invalid quality", method: http.MethodGet, path: "/", accept: "text/html;q=NaN, */*;q=0.8", wantStatus: http.StatusOK, wantJSON: true, wantVary: true},
		{name: "post stays discovery", method: http.MethodPost, path: "/", accept: browserAccept, wantStatus: http.StatusOK, wantJSON: true, wantVary: true},
		{name: "unknown path stays not found", method: http.MethodGet, path: "/unknown", accept: browserAccept, wantStatus: http.StatusNotFound, wantJSON: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(tt.method, ts.URL+tt.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			if tt.accept != "" {
				req.Header.Set("Accept", tt.accept)
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body=%q", resp.StatusCode, tt.wantStatus, body)
			}
			if got := resp.Header.Get("Location"); got != tt.wantLocation {
				t.Errorf("Location = %q, want %q", got, tt.wantLocation)
			}
			if tt.wantJSON {
				if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
					t.Errorf("Content-Type = %q, want application/json", ct)
				}
				if !json.Valid(body) {
					t.Errorf("body is not JSON: %q", body)
				}
			}
			if got := varyContains(resp.Header, "Accept"); got != tt.wantVary {
				t.Errorf("Vary has Accept = %v, want %v; values=%q", got, tt.wantVary, resp.Header.Values("Vary"))
			}
			if tt.method == http.MethodHead && len(body) != 0 {
				t.Errorf("HEAD body = %q, want empty", body)
			}
		})
	}

	headless := server.New(eng, nil)
	headlessTS := httptest.NewServer(headless.Handler())
	defer headlessTS.Close()
	req, _ := http.NewRequest(http.MethodGet, headlessTS.URL+"/", nil)
	req.Header.Set("Accept", browserAccept)
	resp, err := headlessTS.Client().Do(req)
	if err != nil {
		t.Fatalf("headless request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("headless browser request = %d %q, want 200 application/json", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	if varyContains(resp.Header, "Accept") {
		t.Fatalf("headless response unexpectedly varies on Accept: %q", resp.Header.Values("Vary"))
	}
}

func TestIndexBrowserRedirectPreservesCORSVary(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer eng.Close()

	srv := server.New(eng, nil)
	srv.UI = true
	srv.CORS = "https://console.example"
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	client := *ts.Client()
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/", nil)
	req.Header.Set("Origin", "https://console.example")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,*/*;q=0.8")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("status = %d, want 307", resp.StatusCode)
	}
	for _, name := range []string{"Accept", "Origin"} {
		if !varyContains(resp.Header, name) {
			t.Errorf("Vary is missing %s: %q", name, resp.Header.Values("Vary"))
		}
	}
}

func varyContains(header http.Header, name string) bool {
	for _, value := range header.Values("Vary") {
		for _, field := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(field), name) {
				return true
			}
		}
	}
	return false
}

// "/" is the mux catch-all, so an unknown path returns a JSON 404 (not the card),
// reached here with auth off.
func TestIndexUnknownPath404(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer eng.Close()
	ts := httptest.NewServer(server.New(eng, nil).Handler()) // auth off
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/nope")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /nope: want 404, got %d", resp.StatusCode)
	}
}

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

func TestStatusEndpoint(t *testing.T) {
	ctx := context.Background()
	const now = int64(1_800_000_000_123)
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true, Now: func() int64 { return now }})
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
	if st.ServerTimeMs != now {
		t.Errorf("server_time_ms=%d, want lifecycle clock %d", st.ServerTimeMs, now)
	}
	if got := res.Header.Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control=%q, want no-store", got)
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

// The console's two endpoints over HTTP: ListSubscriptions exposes topic+filter (which
// ListQueues does not), and TestFilter dry-runs an expression.
func TestListSubscriptionsAndTestFilter(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer eng.Close()
	if err := eng.Subscribe(ctx, "events", "gold", &engine.Filter{Expr: `properties["tier"]=="gold"`}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	ts := httptest.NewServer(server.New(eng, nil).Handler()) // no auth
	defer ts.Close()

	post := func(path string, body, out any) []byte {
		t.Helper()
		b, _ := json.Marshal(body)
		res, err := http.Post(ts.URL+path, "application/json", bytes.NewReader(b))
		if err != nil {
			t.Fatalf("POST %s: %v", path, err)
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("POST %s: status %d", path, res.StatusCode)
		}
		response, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatalf("read POST %s: %v", path, err)
		}
		if out != nil {
			if err := json.Unmarshal(response, out); err != nil {
				t.Fatalf("decode POST %s: %v", path, err)
			}
		}
		return response
	}

	var ls wire.ListSubscriptionsResponse
	legacyBody := post(wire.PathListSubscriptions, wire.Empty{}, &ls)
	if len(ls.Subscriptions) != 1 || ls.Subscriptions[0].Topic != "events" ||
		ls.Subscriptions[0].Name != "gold" || ls.Subscriptions[0].Expr != `properties["tier"]=="gold"` {
		t.Fatalf("ListSubscriptions = %+v", ls.Subscriptions)
	}
	var legacyWire struct {
		Subscriptions []map[string]json.RawMessage `json:"subscriptions"`
	}
	if err := json.Unmarshal(legacyBody, &legacyWire); err != nil || len(legacyWire.Subscriptions) != 1 {
		t.Fatalf("legacy wire response = %s: %v", legacyBody, err)
	}
	if _, present := legacyWire.Subscriptions[0]["state"]; present {
		t.Fatalf("legacy wire response added state: %s", legacyBody)
	}
	var page wire.ListSubscriptionsPageResponse
	post(wire.PathListSubscriptionsPage, wire.ListSubscriptionsPageRequest{Topic: "events", Limit: 1}, &page)
	if page.Total != 1 || page.NextAfter != "" || len(page.Subscriptions) != 1 ||
		page.Subscriptions[0].Topic != ls.Subscriptions[0].Topic ||
		page.Subscriptions[0].Name != ls.Subscriptions[0].Name ||
		page.Subscriptions[0].Expr != ls.Subscriptions[0].Expr ||
		page.Subscriptions[0].State != "active" || page.Subscriptions[0].CreatedAtMs <= 0 ||
		page.Subscriptions[0].ExpiresAtMs != 0 {
		t.Fatalf("ListSubscriptionsPage = %+v", page)
	}

	// valid expr + a matching sample.
	var tfMatch wire.TestFilterResponse
	post(wire.PathTestFilter, wire.TestFilterRequest{
		Expr:    `properties["tier"]=="gold"`,
		Message: &wire.Message{Properties: map[string]string{"tier": "gold"}},
	}, &tfMatch)
	if !tfMatch.Valid || !tfMatch.Ran || !tfMatch.Matched || tfMatch.Error != "" {
		t.Fatalf("TestFilter match = %+v", tfMatch)
	}

	// a non-matching sample.
	var tfNo wire.TestFilterResponse
	post(wire.PathTestFilter, wire.TestFilterRequest{
		Expr:    `properties["tier"]=="gold"`,
		Message: &wire.Message{Properties: map[string]string{"tier": "silver"}},
	}, &tfNo)
	if !tfNo.Ran || tfNo.Matched {
		t.Fatalf("TestFilter no-match = %+v", tfNo)
	}

	// an invalid expression compiles to an error (not a 400 — the dry run reports it).
	var tfBad wire.TestFilterResponse
	post(wire.PathTestFilter, wire.TestFilterRequest{Expr: `subject ==`}, &tfBad)
	if tfBad.Valid || tfBad.Error == "" {
		t.Fatalf("TestFilter invalid = %+v", tfBad)
	}
}

func TestSubscriptionLifecycleRoutes(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true, MaxSubscriptions: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	for _, name := range []string{"a", "b"} {
		if err := eng.Subscribe(ctx, "events", name, nil); err != nil {
			t.Fatal(err)
		}
	}
	h := server.New(eng, nil).Handler()
	call := func(path string, body any) *httptest.ResponseRecorder {
		t.Helper()
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, bytes.NewReader(data)))
		return rec
	}
	errorCode := func(rec *httptest.ResponseRecorder) string {
		t.Helper()
		var body wire.ErrorBody
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		return body.Code
	}

	first := call(wire.PathListSubscriptionsPage, wire.ListSubscriptionsPageRequest{Topic: "events", Limit: 1})
	if first.Code != http.StatusOK {
		t.Fatalf("first page status = %d: %s", first.Code, first.Body.String())
	}
	var p1 wire.ListSubscriptionsPageResponse
	if err := json.Unmarshal(first.Body.Bytes(), &p1); err != nil {
		t.Fatal(err)
	}
	if p1.Total != 2 || len(p1.Subscriptions) != 1 || p1.Subscriptions[0].State != "active" || p1.NextAfter == "" {
		t.Fatalf("first page = %+v", p1)
	}
	second := call(wire.PathListSubscriptionsPage, wire.ListSubscriptionsPageRequest{Topic: "events", Limit: 1, After: p1.NextAfter})
	var p2 wire.ListSubscriptionsPageResponse
	if second.Code != http.StatusOK || json.Unmarshal(second.Body.Bytes(), &p2) != nil || p2.Total != 2 || len(p2.Subscriptions) != 1 || p2.NextAfter != "" {
		t.Fatalf("second page = status %d, %+v", second.Code, p2)
	}

	limit := call(wire.PathSubscribe, wire.SubscribeRequest{Topic: "events", Name: "c"})
	if limit.Code != http.StatusTooManyRequests || errorCode(limit) != "resource_exhausted" {
		t.Fatalf("subscription limit = %d %s", limit.Code, limit.Body.String())
	}
	if _, err := eng.Send(ctx, "events", engine.OutMessage{Body: []byte("retained")}); err != nil {
		t.Fatal(err)
	}
	notEmpty := call(wire.PathDeleteSubscription, wire.DeleteSubscriptionRequest{Topic: "events", Name: "a"})
	if notEmpty.Code != http.StatusConflict || errorCode(notEmpty) != "subscription_not_empty" {
		t.Fatalf("nonempty delete = %d %s", notEmpty.Code, notEmpty.Body.String())
	}
	missing := call(wire.PathDeleteSubscription, wire.DeleteSubscriptionRequest{Topic: "wrong", Name: "a", Force: true})
	if missing.Code != http.StatusNotFound || errorCode(missing) != "not_found" {
		t.Fatalf("wrong-topic delete = %d %s", missing.Code, missing.Body.String())
	}
	deleted := call(wire.PathDeleteSubscription, wire.DeleteSubscriptionRequest{Topic: "events", Name: "a", Force: true})
	ack, decodeErr := wire.DecodeDeleteResponse(deleted.Body.Bytes())
	if deleted.Code != http.StatusOK || decodeErr != nil || !ack.Deleted || ack.State != "deleted" {
		t.Fatalf("force delete = %d %s: %v", deleted.Code, deleted.Body.String(), decodeErr)
	}
}

func TestSubscriptionTTLRouteContract(t *testing.T) {
	ctx := context.Background()
	const createdAt = int64(1_700_000_000_000)
	var now atomic.Int64
	now.Store(createdAt)
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true, Now: now.Load})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	h := server.New(eng, nil).Handler()
	call := func(body any) *httptest.ResponseRecorder {
		t.Helper()
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, wire.PathSubscribe, bytes.NewReader(data)))
		return rec
	}
	errorCode := func(rec *httptest.ResponseRecorder) string {
		t.Helper()
		var body wire.ErrorBody
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		return body.Code
	}

	if rec := call(wire.SubscribeRequest{Topic: "events", Name: "durable"}); rec.Code != http.StatusOK {
		t.Fatalf("legacy subscribe = %d: %s", rec.Code, rec.Body.String())
	}
	if rec := call(wire.SubscribeWithOptionsRequest{
		Topic: "events", Name: "temporary", Filter: &engine.Filter{Expr: `subject == "kept"`}, TTLMs: 5000,
	}); rec.Code != http.StatusOK {
		t.Fatalf("TTL subscribe = %d: %s", rec.Code, rec.Body.String())
	}
	conflict := call(wire.SubscribeWithOptionsRequest{
		Topic: "events", Name: "temporary", Filter: &engine.Filter{Expr: `subject == "discarded"`}, TTLMs: 1000,
	})
	if conflict.Code != http.StatusConflict || errorCode(conflict) != "name_conflict" {
		t.Fatalf("changed TTL = %d: %s", conflict.Code, conflict.Body.String())
	}
	page, err := eng.ListSubscriptionsWithOptions(ctx, engine.ListSubscriptionsOptions{Topic: "events", IncludeDeleting: true})
	if err != nil || page.Total != 2 {
		t.Fatalf("inventory = %+v, %v", page, err)
	}
	var temporary engine.SubscriptionPageItem
	for _, item := range page.Subscriptions {
		if item.Name == "temporary" {
			temporary = item
		}
	}
	if temporary.Expr != `subject == "kept"` || temporary.CreatedAtMs != createdAt || temporary.ExpiresAtMs != createdAt+5000 {
		t.Fatalf("temporary = %+v", temporary)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, wire.PathSubscribe, bytes.NewBufferString(
		`{"topic":"events","name":"temporary","filter":{"expr":"subject == \"zero\""},"ttl_ms":0}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("explicit zero TTL update = %d: %s", rec.Code, rec.Body.String())
	}
	page, err = eng.ListSubscriptionsWithOptions(ctx, engine.ListSubscriptionsOptions{Topic: "events", IncludeDeleting: true})
	if err != nil {
		t.Fatal(err)
	}
	temporary = engine.SubscriptionPageItem{}
	for _, item := range page.Subscriptions {
		if item.Name == "temporary" {
			temporary = item
		}
	}
	if temporary.Name == "" || temporary.Expr != `subject == "zero"` ||
		temporary.CreatedAtMs != createdAt || temporary.ExpiresAtMs != createdAt+5000 {
		t.Fatalf("zero TTL moved the fixed deadline: %+v", temporary)
	}

	negative := call(wire.SubscribeWithOptionsRequest{Topic: "events", Name: "negative", TTLMs: -1})
	if negative.Code != http.StatusBadRequest || errorCode(negative) != "invalid_argument" {
		t.Fatalf("negative TTL = %d: %s", negative.Code, negative.Body.String())
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, wire.PathSubscribe,
		bytes.NewBufferString(`{"topic":"events","name":"fractional","ttl_ms":1.5}`)))
	if rec.Code != http.StatusBadRequest || errorCode(rec) != "invalid_argument" {
		t.Fatalf("fractional TTL = %d: %s", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, wire.PathSubscribe,
		bytes.NewBufferString(`{"topic":"events","name":"zero","ttl_ms":0}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("zero TTL = %d: %s", rec.Code, rec.Body.String())
	}
	page, err = eng.ListSubscriptionsWithOptions(ctx, engine.ListSubscriptionsOptions{Topic: "events", IncludeDeleting: true})
	if err != nil {
		t.Fatal(err)
	}
	var zero engine.SubscriptionPageItem
	for _, item := range page.Subscriptions {
		if item.Name == "zero" {
			zero = item
		}
	}
	if zero.Name != "zero" || zero.ExpiresAtMs != 0 {
		t.Fatalf("zero TTL subscription = %+v", zero)
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, wire.PathSubscribe,
		bytes.NewBufferString(`{"topic":"events","name":"null","ttl_ms":null}`)))
	if rec.Code != http.StatusBadRequest || errorCode(rec) != "invalid_argument" {
		t.Fatalf("null TTL = %d: %s", rec.Code, rec.Body.String())
	}
	now.Store(math.MaxInt64 - 100)
	overflow := call(wire.SubscribeWithOptionsRequest{Topic: "events", Name: "overflow", TTLMs: 1000})
	if overflow.Code != http.StatusBadRequest || errorCode(overflow) != "invalid_argument" {
		t.Fatalf("overflow TTL = %d: %s", overflow.Code, overflow.Body.String())
	}
}
