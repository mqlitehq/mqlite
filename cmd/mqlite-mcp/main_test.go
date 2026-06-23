package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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
