package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/server"
)

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
	for _, want := range []string{"send", "receive", "complete", "create_queue", "stats", "list_queues"} {
		if !names[want] {
			t.Fatalf("missing tool %q", want)
		}
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
			if typ, _ := spec.(map[string]any)["type"].(string); typ == "integer" {
				sentinel = float64(987654) // JSON numbers decode to float64 in args
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
	ts := httptest.NewServer(server.New(eng, nil).Handler()) // no auth
	defer ts.Close()
	broker.endpoint = ts.URL
	broker.token = ""
	broker.http = ts.Client()

	// One superset of args; each forward reads only the keys it needs.
	args := map[string]any{
		"name": "q", "queue": "q", "body": "hi", "message_id": "m1", "group_id": "g1",
		"seq_number": float64(1), "lock_token": "tok", "max_messages": float64(1),
		"wait_time_ms": float64(0), "state": "active", "max": float64(8),
		"reason": "because", "delay_ms": float64(0),
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
		for rt.Kind() == reflect.Ptr {
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
			for ft.Kind() == reflect.Ptr {
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
	"wire.Empty": {},
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
	"wire.ReceiveRequest": {"max_messages", "queue", "receive_attempt_id", "receive_mode", "wait_time_ms"},
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
