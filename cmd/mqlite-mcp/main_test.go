package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
