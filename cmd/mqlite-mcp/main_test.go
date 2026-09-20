package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/internal/authkey"
	"github.com/mqlitehq/mqlite/internal/defaults"
	"github.com/mqlitehq/mqlite/server"
	"github.com/mqlitehq/mqlite/wire"
)

// TestResolveEndpoint pins the MCP endpoint fallback to the shared loopback default and the
// trailing-slash trim (MQLITE-84), without any network call.
func TestResolveEndpoint(t *testing.T) {
	if got, err := resolveEndpoint(""); err != nil || got != defaults.BrokerLoopbackEndpoint {
		t.Errorf("empty -> %q err %v, want %q", got, err, defaults.BrokerLoopbackEndpoint)
	}
	if got, err := resolveEndpoint("http://x:1/"); err != nil || got != "http://x:1" {
		t.Errorf("trailing slash -> %q err %v, want http://x:1", got, err)
	}
	if got, err := resolveEndpoint("https://q.example"); err != nil || got != "https://q.example" {
		t.Errorf("passthrough -> %q err %v, want https://q.example", got, err)
	}
	// A malformed endpoint is rejected here (startup), not deferred to a nil-request deref on
	// the first tool call (MQLITE-81).
	for _, bad := range []string{"://bad", "notaurl", "ftp://host", "http://"} {
		if _, err := resolveEndpoint(bad); err == nil {
			t.Errorf("resolveEndpoint(%q) should reject a malformed endpoint", bad)
		}
	}
}

func call(t *testing.T, line string) rpcResponse {
	t.Helper()
	resp, ok := handle([]byte(line))
	if !ok {
		t.Fatalf("expected a response for: %s", line)
	}
	return resp
}

func TestInitialize(t *testing.T) {
	resp := call(t, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05"}}`)
	si := resp.Result.(map[string]any)["serverInfo"].(map[string]any)
	if si["name"] != serverName {
		t.Fatalf("serverInfo.name = %v, want %q", si["name"], serverName)
	}
}

func TestToolsList(t *testing.T) {
	resp := call(t, `{"jsonrpc":"2.0","id":2,"method":"tools/list"}`)
	list := resp.Result.(map[string]any)["tools"].([]map[string]any)
	names := map[string]bool{}
	for _, td := range list {
		names[td["name"].(string)] = true
		if td["inputSchema"] == nil {
			t.Fatalf("tool %v has no inputSchema", td["name"])
		}
	}
	want := []string{"abandon", "complete", "create_key", "create_queue", "defer", "list_keys", "list_queues", "peek", "purge", "receive", "receive_deferred", "redrive", "reject", "renew", "revoke_key", "send", "stats"}
	got := make([]string, 0, len(names))
	for name := range names {
		got = append(got, name)
	}
	sort.Strings(got)
	if !reflect.DeepEqual(got, want) || len(list) != len(want) {
		t.Fatalf("tool inventory = %v, want %v", got, want)
	}
}

func TestNotificationNoResponse(t *testing.T) {
	if _, ok := handle([]byte(`{"jsonrpc":"2.0","method":"notifications/initialized"}`)); ok {
		t.Fatal("a notification (no id) must not get a response")
	}
}

func TestUnknownMethod(t *testing.T) {
	resp := call(t, `{"jsonrpc":"2.0","id":4,"method":"bogus"}`)
	if resp.Error == nil {
		t.Fatal("expected a JSON-RPC error for an unknown method")
	}
}

// tools/call forwards to the broker's HTTP API: right path, base64 body, result text.
func TestToolsCallForwards(t *testing.T) {
	var gotPath, gotBody string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		_, _ = w.Write([]byte(`{"seq_numbers":[1]}`))
	}))
	defer ts.Close()
	broker.endpoint = ts.URL
	broker.token = ""
	broker.http = ts.Client()

	resp := call(t, `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"send","arguments":{"queue":"orders","body":"hi"}}}`)
	m := resp.Result.(map[string]any)
	if m["isError"] == true {
		t.Fatalf("tool reported error: %+v", m)
	}
	if gotPath != "/mqlite.v1.QueueService/Send" {
		t.Fatalf("forwarded to %q, want Send path", gotPath)
	}
	if !strings.Contains(gotBody, `"queue":"orders"`) {
		t.Fatalf("forwarded body missing queue: %s", gotBody)
	}
	if !strings.Contains(gotBody, "aGk=") { // "hi" base64
		t.Fatalf("body not base64-encoded: %s", gotBody)
	}
	text := m["content"].([]map[string]any)[0]["text"].(string)
	if !strings.Contains(text, "seq_numbers") {
		t.Fatalf("result text = %q", text)
	}
}

// ─── schema ↔ forward ↔ wire consistency ────────────────────────────────────
//
// Closes the gap noted in docs/mcp-wire-compat-notes.md: a tool's hand-written
// inputSchema, the string keys its forward reads (str(a,"queue")), and the wire.*Request
// fields have no compile-time link, so a typo or a wire rename silently sends an empty
// value. These tests make that drift a CI failure instead of a runtime surprise.

func fwdBody(tl tool, a map[string]any) string {
	_, body := tl.forward(a)
	b, _ := json.Marshal(body)
	return string(b)
}

// Every schema property must actually change the forwarded request when set — i.e. the
// key forward reads matches the schema property name and lands in a wire.*Request field.
// A mismatch (schema "queue" vs forward str(a,"queu")) makes the property inert → fail.
func TestToolSchemaForwardConsistency(t *testing.T) {
	for _, tl := range tools {
		props, _ := tl.schema["properties"].(map[string]any)
		if req, ok := tl.schema["required"].([]string); ok {
			for _, r := range req {
				if _, present := props[r]; !present {
					t.Errorf("%s: required %q is not in properties", tl.name, r)
				}
			}
		}
		base := fwdBody(tl, map[string]any{})
		for key, spec := range props {
			var sentinel any = "SENTINEL-" + key
			switch typ, _ := spec.(map[string]any)["type"].(string); typ {
			case "integer":
				sentinel = float64(987654) // JSON numbers decode to float64 in args
			case "array":
				sentinel = []any{float64(987654)} // JSON arrays decode to []any
				items, _ := spec.(map[string]any)["items"].(map[string]any)
				if items["type"] == "string" {
					sentinel = []any{"send"}
				}
			}
			if fwdBody(tl, map[string]any{key: sentinel}) == base {
				t.Errorf("%s: schema property %q has no effect on the forwarded request — "+
					"the key forward reads doesn't match it, or it isn't wired into the wire.*Request", tl.name, key)
			}
		}
	}
}

// Every tool's forward path must be a route the broker actually serves. Catches a tool
// pointing at a stale/renamed path (a 404 "no such path"); a legitimate business error
// is fine — it still proves the route exists.
func TestToolForwardsHitRealBrokerRoutes(t *testing.T) {
	eng, err := engine.Open(context.Background(), engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer eng.Close()
	if err := eng.CreateQueue(context.Background(), "q", engine.QueueConfig{}); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	ts := httptest.NewServer(server.New(eng, []string{"administrator"}).Handler())
	defer ts.Close()
	broker.endpoint = ts.URL
	broker.token = "administrator"
	broker.http = ts.Client()

	// One superset of args; each forward reads only the keys it needs.
	args := map[string]any{
		"name": "q", "queue": "q", "body": "hi", "message_id": "m1", "group_id": "g1",
		"seq_number": float64(1), "lock_token": "tok", "max_messages": float64(1),
		"wait_time_ms": float64(0), "state": "active", "max": float64(8),
		"reason": "because", "delay_ms": float64(0),
		"id": strings.Repeat("a", 32), "permissions": []any{"manage"},
	}
	for _, tl := range tools {
		res := callTool(tl.name, args)
		text := res["content"].([]map[string]any)[0]["text"].(string)
		if strings.Contains(text, "no such path") {
			t.Errorf("tool %q forwards to a route the broker does not serve: %s", tl.name, text)
		}
	}
}

// wireShape walks a wire.*Request type and returns the sorted set of json field
// paths reachable from it (one level into nested structs and slices-of-structs, so
// `messages[].body` and `config.dlq_max_age_ms` are included). `[]byte` and maps are
// leaves (not recursed).
func wireShape(t reflect.Type) []string {
	out := []string{}
	var walk func(rt reflect.Type, prefix string)
	walk = func(rt reflect.Type, prefix string) {
		for rt.Kind() == reflect.Pointer {
			rt = rt.Elem()
		}
		if rt.Kind() != reflect.Struct {
			return
		}
		for i := 0; i < rt.NumField(); i++ {
			f := rt.Field(i)
			if !f.IsExported() {
				continue
			}
			tag := strings.Split(f.Tag.Get("json"), ",")[0]
			if tag == "" || tag == "-" {
				continue
			}
			ft := f.Type
			for ft.Kind() == reflect.Pointer {
				ft = ft.Elem()
			}
			switch {
			case ft.Kind() == reflect.Struct:
				walk(ft, prefix+tag+".")
			case (ft.Kind() == reflect.Slice || ft.Kind() == reflect.Array) &&
				ft.Elem().Kind() == reflect.Struct:
				walk(ft.Elem(), prefix+tag+"[].")
			default:
				out = append(out, prefix+tag)
			}
		}
	}
	walk(t, "")
	sort.Strings(out)
	return out
}

// goldenWireShapes pins the exact field shape of every wire.*Request type the MCP
// tools forward into. The reverse guard to TestToolSchemaForwardConsistency: that test
// proves every *schema property* is wired in; this one proves no *wire field* drifts
// in/out unnoticed. Adding/removing/renaming a field on any of these wire types fails
// here — forcing a conscious decision about whether the MCP tool schema should expose
// it (then update this golden). Closes gap 1-reverse in docs/mcp-wire-compat-notes.md.
var goldenWireShapes = map[string][]string{
	"wire.Empty":            {},
	"wire.CreateKeyRequest": {"id", "name", "permissions", "expires_at_ms"},
	"wire.ListKeysRequest":  {"after_id", "limit", "sort"},
	"wire.RevokeKeyRequest": {"id"},
	"wire.CreateQueueRequest": {
		"config.dead_letter_on_expire", "config.default_ttl_ms", "config.dedup_window_ms",
		"config.dlq_max_age_ms", "config.dlq_max_bytes", "config.dlq_max_count", "config.kind",
		"config.lock_duration_ms", "config.max_delivery_count", "config.ordering_mode", "name",
	},
	"wire.SendRequest": {
		"messages[].body", "messages[].content_type", "messages[].correlation_id",
		"messages[].dead_letter_description", "messages[].dead_letter_reason",
		"messages[].delivery_count", "messages[].enqueued_at_ms", "messages[].expires_at_ms",
		"messages[].group_id", "messages[].locked_until_ms", "messages[].lock_token",
		"messages[].message_id", "messages[].properties", "messages[].reply_to",
		"messages[].seq_number", "messages[].state", "messages[].subject",
		"messages[].visible_at_ms", "queue", "scheduled_enqueue_time_ms", "ttl_ms",
	},
	"wire.ReceiveRequest":         {"max_messages", "queue", "receive_attempt_id", "receive_mode", "wait_time_ms"},
	"wire.ReceiveDeferredRequest": {"queue", "seq_numbers"},
	"wire.SettleRequest": {
		"dead_letter_description", "dead_letter_reason", "delay_ms", "lock_token", "queue", "seq_number",
	},
	"wire.PeekRequest":    {"from_seq", "max", "queue", "state"},
	"wire.MetricsRequest": {"queue"},
	"wire.RedriveRequest": {"max", "older_than_ms", "queue", "rate_per_sec", "target"},
	"wire.PurgeRequest":   {"max", "older_than_ms", "queue"},
}

func TestToolWireShapesPinned(t *testing.T) {
	seen := map[string]bool{}
	for _, tl := range tools {
		_, body := tl.forward(map[string]any{})
		name := reflect.TypeOf(body).String()
		seen[name] = true
		want, ok := goldenWireShapes[name]
		if !ok {
			t.Errorf("tool %q forwards %s, which is not pinned in goldenWireShapes — add it "+
				"(and decide whether the tool schema should expose its fields)", tl.name, name)
			continue
		}
		sort.Strings(want) // golden literals need not be hand-sorted
		if got := wireShape(reflect.TypeOf(body)); !reflect.DeepEqual(got, want) {
			t.Errorf("%s field shape changed.\n  got:  %v\n  want: %v\n"+
				"A wire request type the MCP forwards into gained/lost/renamed a field. Review "+
				"whether cmd/mqlite-mcp's tool schema should expose it, then update goldenWireShapes.",
				name, got, want)
		}
	}
	for name := range goldenWireShapes {
		if !seen[name] {
			t.Errorf("goldenWireShapes pins %s, but no tool forwards it anymore — remove the stale entry", name)
		}
	}
}

// A broker error surfaces as an MCP tool error (isError=true), not a transport failure.
func TestToolsCallBrokerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"code":"not_found","message":"no such queue"}`))
	}))
	defer ts.Close()
	broker.endpoint = ts.URL
	broker.http = ts.Client()

	resp := call(t, `{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"stats","arguments":{"queue":"nope"}}}`)
	if resp.Result.(map[string]any)["isError"] != true {
		t.Fatal("broker 404 should surface as isError=true")
	}
}

// Existing MCP tools forward the configured credential and preserve permission
// failures, including calls made with managed credentials.
func TestToolsPermissionDenied(t *testing.T) {
	old := broker
	defer func() { broker = old }()
	const credential = "restricted-credential"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+credential {
			t.Error("configured token was not forwarded")
		}
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"code":"permission_denied","message":"listen permission required"}`))
	}))
	defer ts.Close()
	broker.endpoint, broker.token, broker.http = ts.URL, credential, ts.Client()
	resp := call(t, `{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"stats","arguments":{"queue":"q"}}}`)
	result := resp.Result.(map[string]any)
	if result["isError"] != true {
		t.Fatal("permission denial must remain an MCP tool error")
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), "permission_denied") || strings.Contains(string(encoded), credential) {
		t.Fatal("tool error must preserve permission code without credential")
	}
}

func toolText(t *testing.T, name string, args map[string]any, wantError bool) string {
	t.Helper()
	params, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": map[string]any{"name": name, "arguments": args}})
	if err != nil {
		t.Fatal(err)
	}
	result := call(t, string(params)).Result.(map[string]any)
	text := result["content"].([]map[string]any)[0]["text"].(string)
	if result["isError"] != wantError {
		t.Fatalf("tool %s isError=%v, want %v: %s", name, result["isError"], wantError, text)
	}
	return text
}

func TestKeyToolsAgainstBroker(t *testing.T) {
	old := broker
	defer func() { broker = old }()
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	ts := httptest.NewServer(server.New(eng, []string{"administrator"}).Handler())
	defer ts.Close()
	broker.endpoint, broker.token, broker.http = ts.URL, "administrator", ts.Client()
	if text := toolText(t, "list_keys", nil, false); text != `{"keys":[]}`+"\n" {
		t.Fatalf("empty list = %q", text)
	}
	managerID, senderID := strings.Repeat("1", 32), strings.Repeat("2", 32)
	createArgs := func(id, name, permission string) map[string]any {
		return map[string]any{"id": id, "name": name, "permissions": []any{permission}}
	}
	var manager, sender wire.CreateKeyResponse
	text := toolText(t, "create_key", createArgs(managerID, "manager", "manage"), false)
	if err := json.Unmarshal([]byte(text), &manager); err != nil {
		t.Fatal(err)
	}
	if manager.Key.ID != managerID || !authkey.ValidToken(manager.Token) || strings.Count(text, manager.Token) != 1 {
		t.Fatal("create_key must return one complete token and its public ID")
	}
	broker.token = manager.Token
	args := createArgs(senderID, "producer", "send")
	args["expires_at_ms"] = float64(time.Now().Add(time.Hour).UnixMilli())
	text = toolText(t, "create_key", args, false)
	if err := json.Unmarshal([]byte(text), &sender); err != nil {
		t.Fatal(err)
	}
	if sender.Key.ID != senderID || sender.Key.ExpiresAtMs != int64(args["expires_at_ms"].(float64)) {
		t.Fatal("managed administrator must be able to issue an expiring key")
	}
	for i, after := range []string{"", managerID} {
		text = toolText(t, "list_keys", map[string]any{"after_id": after, "limit": 1}, false)
		var page wire.ListKeysResponse
		if err := json.Unmarshal([]byte(text), &page); err != nil {
			t.Fatal(err)
		}
		wantID, wantNext := managerID, managerID
		if i == 1 {
			wantID, wantNext = senderID, ""
		}
		if len(page.Keys) != 1 || page.Keys[0].ID != wantID || page.NextAfterID != wantNext || strings.Contains(text, "token") || strings.Contains(text, "hash") {
			t.Fatalf("page %d metadata contract differs", i)
		}
	}
	text = toolText(t, "create_key", createArgs(senderID, "repeat", "manage"), true)
	if !strings.Contains(text, "key_conflict") || !strings.Contains(text, senderID) || strings.Contains(text, sender.Token) {
		t.Fatal("conflict must retain public ID without repeating the token")
	}
	broker.token = sender.Token
	toolText(t, "send", map[string]any{"queue": "missing", "body": "hi"}, true) // send passes auth and reports the missing queue
	for _, request := range []struct {
		name string
		args map[string]any
	}{
		{"create_key", createArgs(strings.Repeat("3", 32), "escalation", "manage")},
		{"list_keys", nil}, {"revoke_key", map[string]any{"id": managerID}},
	} {
		text = toolText(t, request.name, request.args, true)
		if !strings.Contains(text, "permission_denied") || strings.Contains(text, sender.Token) {
			t.Fatalf("%s must preserve permission denial without secret", request.name)
		}
	}
	broker.token = manager.Token
	for i := 0; i < 2; i++ {
		toolText(t, "revoke_key", map[string]any{"id": senderID}, false)
	}
	text = toolText(t, "list_keys", nil, false)
	var page wire.ListKeysResponse
	if err := json.Unmarshal([]byte(text), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Keys) != 2 || page.Keys[1].RevokedAtMs == 0 {
		t.Fatal("revoked key must remain listed")
	}
	broker.token = sender.Token
	if text = toolText(t, "stats", map[string]any{"queue": "q"}, true); !strings.Contains(text, "unauthenticated") {
		t.Fatal("revoked key must stop authenticating")
	}
	broker.token = manager.Token
	toolText(t, "revoke_key", map[string]any{"id": managerID}, false)
	if text = toolText(t, "list_keys", nil, true); !strings.Contains(text, "unauthenticated") {
		t.Fatal("self-revocation must take effect")
	}
	broker.token = "administrator"
	toolText(t, "list_keys", nil, false)
}

func TestCreateKeyToolFailureRetainsIDWithoutSecret(t *testing.T) {
	const id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	secret := "mqk_" + strings.Repeat("b", 64)
	for _, tc := range []struct {
		name, body string
		status     int
	}{
		{"truncated", `{"key":{"id":"` + id + `"},"token":"` + secret, http.StatusOK},
		{"missing secret", `{"key":{"id":"` + id + `"}}`, http.StatusOK},
		{"wrong id", `{"key":{"id":"` + strings.Repeat("c", 32) + `"},"token":"` + secret + `"}`, http.StatusOK},
		{"invalid token", `{"key":{"id":"` + id + `"},"token":"partial-secret"}`, http.StatusOK},
		{"server error", `{"code":"internal","message":"database unavailable"}`, http.StatusInternalServerError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := broker
			defer func() { broker = old }()
			var calls atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer ts.Close()
			broker.endpoint, broker.token, broker.http = ts.URL, "administrator", ts.Client()
			text := toolText(t, "create_key", map[string]any{"id": id, "name": "recovery", "permissions": []any{"manage"}}, true)
			if calls.Load() != 1 || !strings.Contains(text, id) || strings.Contains(text, secret) || strings.Contains(text, "partial-secret") {
				t.Fatalf("uncertain create must identify key without retrying or leaking response fragments; calls=%d", calls.Load())
			}
			for _, badID := range []any{nil, "", "BAD", secret, 123} {
				text = toolText(t, "create_key", map[string]any{"id": badID, "name": "invalid", "permissions": []any{"manage"}}, true)
				if calls.Load() != 1 || strings.Contains(text, secret) {
					t.Fatal("invalid id must be rejected without a request or value echo")
				}
			}
		})
	}
}

func TestKeyToolsRejectMalformedSuccess(t *testing.T) {
	id := strings.Repeat("a", 32)
	secret := "mqk_" + strings.Repeat("b", 64)
	key := wire.AccessKey{ID: id, Name: "worker", Permissions: []string{"manage"}, CreatedAtMs: 100}
	metadata, err := json.Marshal(key)
	if err != nil {
		t.Fatal(err)
	}
	created := `{"key":` + string(metadata) + `,"token":"` + secret + `"}`
	listed := `{"keys":[` + string(metadata) + `]}`
	for _, tc := range []struct {
		name, tool, body string
		truncated        bool
	}{
		{"create null", "create_key", "null", false},
		{"create empty", "create_key", "{}", false},
		{"create missing metadata", "create_key", `{"key":{"id":"` + id + `"},"token":"` + secret + `"}`, false},
		{"create mismatched rights", "create_key", strings.Replace(created, `"manage"`, `"send"`, 1), false},
		{"create wrong expiry", "create_key", strings.Replace(created, `"expires_at_ms":0`, `"expires_at_ms":200`, 1), false},
		{"create wrong name", "create_key", strings.Replace(created, `"worker"`, `"other"`, 1), false},
		{"create trailing", "create_key", created + `{}`, false},
		{"create oversized", "create_key", strings.Repeat(" ", wire.MaxKeyResponseBytes) + created, false},
		{"create truncated", "create_key", created, true},
		{"list null", "list_keys", "null", false},
		{"list empty", "list_keys", "{}", false},
		{"list null keys", "list_keys", `{"keys":null}`, false},
		{"list repeated id", "list_keys", `{"keys":[` + string(metadata) + `,` + string(metadata) + `]}`, false},
		{"list backward cursor", "list_keys", `{"keys":[` + string(metadata) + `],"next_after_id":"` + strings.Repeat("0", 32) + `"}`, false},
		{"list partial metadata", "list_keys", `{"keys":[{"id":"` + id + `","token":"` + secret + `"}]}`, false},
		{"list trailing", "list_keys", listed + `{}`, false},
		{"list oversized", "list_keys", strings.Repeat(" ", wire.MaxKeyResponseBytes) + listed, false},
		{"list truncated", "list_keys", listed, true},
		{"revoke null", "revoke_key", "null", false},
		{"revoke empty", "revoke_key", "{}", false},
		{"revoke false", "revoke_key", `{"ok":false}`, false},
		{"revoke trailing", "revoke_key", `{"ok":true}{}`, false},
		{"revoke oversized", "revoke_key", strings.Repeat(" ", wire.MaxKeyResponseBytes) + `{"ok":true}`, false},
		{"revoke truncated", "revoke_key", `{"ok":true}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := broker
			defer func() { broker = old }()
			var calls atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				if tc.truncated {
					w.Header().Set("Content-Length", fmt.Sprint(len(tc.body)+50))
				}
				_, _ = w.Write([]byte(tc.body))
			}))
			defer ts.Close()
			broker.endpoint, broker.token, broker.http = ts.URL, "administrator", ts.Client()
			args := map[string]any{}
			switch tc.tool {
			case "create_key":
				args = map[string]any{"id": id, "name": "worker", "permissions": []any{"manage"}}
			case "revoke_key":
				args["id"] = id
			}
			text := toolText(t, tc.tool, args, true)
			if calls.Load() != 1 || strings.Contains(text, secret) || tc.tool == "create_key" && !strings.Contains(text, id) {
				t.Fatal("invalid success was retried, leaked its token, or lost the public ID")
			}
		})
	}
	for _, operation := range []string{"create_key", "list_keys", "revoke_key"} {
		t.Run(operation+" error and unknown field secrecy", func(t *testing.T) {
			old := broker
			defer func() { broker = old }()
			body := listed
			switch operation {
			case "create_key":
				body = created
			case "revoke_key":
				body = `{"ok":true}`
			}
			body = strings.TrimSuffix(body, "}") + `,"unexpected_secret":"` + secret + `"}`
			var errorPhase atomic.Bool
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if errorPhase.Load() {
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = w.Write([]byte(`{"code":"internal","message":"` + secret + `"}`))
					return
				}
				_, _ = w.Write([]byte(body))
			}))
			defer ts.Close()
			broker.endpoint, broker.token, broker.http = ts.URL, "administrator", ts.Client()
			args := map[string]any{}
			switch operation {
			case "create_key":
				args = map[string]any{"id": id, "name": "worker", "permissions": []any{"manage"}}
			case "revoke_key":
				args["id"] = id
			}
			text := toolText(t, operation, args, false)
			if strings.Contains(text, "unexpected_secret") || operation != "create_key" && strings.Contains(text, secret) || operation == "create_key" && strings.Count(text, secret) != 1 {
				t.Fatal("unreviewed success fields reached the MCP result")
			}
			errorPhase.Store(true)
			text = toolText(t, operation, args, true)
			if !strings.Contains(text, "internal") || strings.Contains(text, secret) {
				t.Fatal("key error did not preserve code or leaked intermediary text")
			}
		})
	}
}

func TestKeyToolsRejectUnknownArguments(t *testing.T) {
	old := broker
	defer func() { broker = old }()
	var requests atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()
	broker.endpoint, broker.token, broker.http = ts.URL, "administrator", ts.Client()
	id := strings.Repeat("a", 32)
	secret := "mqk_" + strings.Repeat("b", 64)
	for _, tc := range []struct {
		tool, typo string
		args       map[string]any
	}{
		{"create_key", "expires_at_m", map[string]any{"id": id, "name": "worker", "permissions": []any{"manage"}}},
		{"list_keys", "afterId", map[string]any{"limit": 1}},
		{"revoke_key", "key_id", map[string]any{"id": id}},
	} {
		for _, unknown := range []string{tc.typo, secret} {
			tc.args[unknown] = secret
			text := toolText(t, tc.tool, tc.args, true)
			delete(tc.args, unknown)
			if requests.Load() != 0 || strings.Contains(text, unknown) || strings.Contains(text, secret) || tc.tool == "create_key" && !strings.Contains(text, id) {
				t.Fatalf("%s unknown argument was sent, leaked, or lost the create ID", tc.tool)
			}
		}
	}
}

func TestKeyToolSchemasPinned(t *testing.T) {
	golden := map[string]string{
		"create_key": `{"type":"object","additionalProperties":false,"properties":{"id":{"type":"string","pattern":"^[0-9a-f]{32}$","description":"unique public key ID; choose before the call"},"name":{"type":"string","description":"key name"},"permissions":{"type":"array","items":{"type":"string","enum":["send","listen","manage"]},"minItems":1,"description":"manage includes send and listen"},"expires_at_ms":{"type":"integer","description":"UTC epoch milliseconds; 0 or omitted means no expiry"}},"required":["id","name","permissions"]}`,
		"list_keys":  `{"type":"object","additionalProperties":false,"properties":{"after_id":{"type":"string","description":"next_after_id from the previous page"},"limit":{"type":"integer","description":"page size, default 100, maximum 1000"},"sort":{"type":"string","enum":["id_asc","created_desc"],"description":"default id_asc; created_desc lists newest first; keep the same sort when paging"}}}`,
		"revoke_key": `{"type":"object","additionalProperties":false,"properties":{"id":{"type":"string","description":"public key ID, not the secret token"}},"required":["id"]}`,
	}
	seen := map[string]bool{}
	for _, tl := range tools {
		want, ok := golden[tl.name]
		if !ok {
			continue
		}
		seen[tl.name] = true
		got, err := json.Marshal(tl.schema)
		if err != nil {
			t.Fatal(err)
		}
		var actual, expected any
		if err := json.Unmarshal(got, &actual); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(want), &expected); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%s schema changed: review and update the full contract", tl.name)
		}
	}
	if len(seen) != len(golden) {
		t.Fatal("managed key tool inventory changed")
	}
	for _, input := range []any{nil, "send", []any{"send", 1}} {
		if got := strArr(map[string]any{"permissions": input}, "permissions"); got != nil {
			t.Fatalf("invalid permission array accepted: %v", input)
		}
	}
}

func TestKeyToolRejectsInvalidNumbers(t *testing.T) {
	old := broker
	defer func() { broker = old }()
	var calls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()
	broker.endpoint, broker.token, broker.http = ts.URL, "administrator", ts.Client()
	for _, bad := range []any{"tomorrow", nil, true, 1.5, 1e30} {
		text := toolText(t, "create_key", map[string]any{"id": strings.Repeat("a", 32), "name": "worker", "permissions": []any{"send"}, "expires_at_ms": bad}, true)
		if !strings.Contains(text, strings.Repeat("a", 32)) || !strings.Contains(text, "expires_at_ms must be an integer") {
			t.Fatal("invalid expiry must retain key ID and explain the input error")
		}
		toolText(t, "list_keys", map[string]any{"limit": bad}, true)
	}
	if calls.Load() != 0 {
		t.Fatal("invalid numbers must fail before issuing requests")
	}
	if !validKeyInteger(map[string]any{"n": int64(123)}, "n") {
		t.Fatal("int64 helper input must be accepted")
	}
}

func TestKeyToolSortedPagination(t *testing.T) {
	old := broker
	defer func() { broker = old }()
	ctx := context.Background()
	var clock atomic.Int64
	clock.Store(100)
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true, Now: clock.Load})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	for _, fixture := range []struct {
		prefix  string
		created int64
	}{{"f", 100}, {"1", 200}, {"a", 200}} {
		clock.Store(fixture.created)
		if _, _, err := eng.CreateAccessKey(ctx, engine.CreateAccessKeyOptions{ID: strings.Repeat(fixture.prefix, 32), Name: "worker-" + fixture.prefix, Permissions: engine.KeySend}); err != nil {
			t.Fatal(err)
		}
	}
	ts := httptest.NewServer(server.New(eng, []string{"administrator"}).Handler())
	defer ts.Close()
	broker.endpoint, broker.token, broker.http = ts.URL, "administrator", ts.Client()
	for _, order := range []string{"", "id_asc", "created_desc"} {
		want := []string{"1", "a", "f"}
		if order == "created_desc" {
			want = []string{"a", "1", "f"}
		}
		after := ""
		for i, prefix := range want {
			args := map[string]any{"after_id": after, "limit": 1}
			if order != "" {
				args["sort"] = order
			}
			text := toolText(t, "list_keys", args, false)
			var page wire.ListKeysResponse
			if err := json.Unmarshal([]byte(text), &page); err != nil {
				t.Fatal(err)
			}
			id := strings.Repeat(prefix, 32)
			wantNext := id
			if i == len(want)-1 {
				wantNext = ""
			}
			if len(page.Keys) != 1 || page.Keys[0].ID != id || page.NextAfterID != wantNext || strings.Contains(text, "token") || strings.Contains(text, "hash") {
				t.Fatalf("sort=%q page=%d: metadata, order or continuation differs", order, i)
			}
			after = page.NextAfterID
		}
	}
	text := toolText(t, "list_keys", map[string]any{"sort": "created_desc", "after_id": strings.Repeat("0", 32)}, true)
	if !strings.Contains(text, "invalid_argument") {
		t.Fatal("unknown creation-order cursor must preserve the broker's invalid_argument error")
	}
}

func TestKeyToolSortValidation(t *testing.T) {
	old := broker
	defer func() { broker = old }()
	var calls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{"keys":[]}`))
	}))
	defer ts.Close()
	broker.endpoint, broker.token, broker.http = ts.URL, "administrator", ts.Client()
	secret := "mqk_" + strings.Repeat("a", 64)
	for _, bad := range []any{nil, false, 1, "", "newest", secret, []any{"created_desc"}, map[string]any{"sort": "created_desc"}} {
		text := toolText(t, "list_keys", map[string]any{"sort": bad}, true)
		if text != "error: sort must be id_asc or created_desc" || strings.Contains(text, secret) {
			t.Fatal("invalid sort must return a fixed safe validation error")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid sort must fail before issuing requests")
	}
}

func TestKeyToolSortedResponseValidation(t *testing.T) {
	old := broker
	defer func() { broker = old }()
	id := func(prefix string) string { return strings.Repeat(prefix, 32) }
	key := func(prefix string, created int64) wire.AccessKey {
		return wire.AccessKey{ID: id(prefix), Name: "worker", Permissions: []string{"send"}, CreatedAtMs: created}
	}
	for _, tc := range []struct {
		name string
		keys []wire.AccessKey
		ok   bool
	}{
		{"descending creation permits ascending IDs", []wire.AccessKey{key("1", 300), key("f", 200)}, true},
		{"ascending creation", []wire.AccessKey{key("f", 200), key("1", 300)}, false},
		{"equal timestamp descending IDs", []wire.AccessKey{key("f", 200), key("1", 200)}, true},
		{"equal timestamp ascending IDs", []wire.AccessKey{key("1", 200), key("f", 200)}, false},
		{"duplicate ID", []wire.AccessKey{key("1", 300), key("1", 200)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request wire.ListKeysRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.Sort != "created_desc" || request.Limit != 2 {
					t.Error("sort and limit must reach the broker unchanged")
				}
				_ = json.NewEncoder(w).Encode(wire.ListKeysResponse{Keys: tc.keys})
			}))
			defer ts.Close()
			broker.endpoint, broker.token, broker.http = ts.URL, "administrator", ts.Client()
			text := toolText(t, "list_keys", map[string]any{"sort": "created_desc", "limit": 2}, !tc.ok)
			if !tc.ok && strings.Contains(text, id("1")) {
				t.Fatal("invalid success must not return partial metadata")
			}
		})
	}
}
