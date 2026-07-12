// Command mqlite-mcp is a Model Context Protocol (MCP) server that exposes the
// mqlite broker as agent tools. It is a thin, dependency-free forwarder: it speaks
// MCP (JSON-RPC 2.0 over stdio) and turns each tool call into one HTTP POST to the
// broker, so an AI agent can drive queues without writing any HTTP. Aligned with
// mqlite's "friendly to AI agents" goal and its dependency-light ethos — stdlib +
// the in-repo wire contract only, no MCP SDK.
//
// Config (env): MQLITE_ENDPOINT (default http://127.0.0.1:6754) + MQLITE_TOKEN.
// Run it as a stdio MCP server from your agent host.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/mqlitehq/mqlite/internal/defaults"
	ver "github.com/mqlitehq/mqlite/internal/version"
	"github.com/mqlitehq/mqlite/wire"
)

const (
	serverName      = "mqlite-mcp"
	serverVersion   = ver.Version
	defaultProtocol = "2024-11-05"
)

var broker struct {
	endpoint string
	token    string
	http     *http.Client
}

// resolveEndpoint normalizes MQLITE_ENDPOINT (trailing slash trimmed) and falls back to
// the shared loopback default when it is empty, so the MCP server and the broker CLI agree
// on the default port without either hardcoding a literal.
func resolveEndpoint(env string) (string, error) {
	ep := strings.TrimRight(strings.TrimSpace(env), "/")
	if ep == "" {
		ep = defaults.BrokerLoopbackEndpoint
	}
	// Validate at startup so a malformed endpoint fails loud here instead of a nil-request
	// deref on the first tool call (MQLITE-81).
	u, err := url.Parse(ep)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("invalid MQLITE_ENDPOINT %q: want an http(s):// broker URL", ep)
	}
	return ep, nil
}

func main() {
	ep, err := resolveEndpoint(os.Getenv("MQLITE_ENDPOINT"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "mqlite-mcp:", err)
		os.Exit(1)
	}
	broker.endpoint = ep
	broker.token = os.Getenv("MQLITE_TOKEN")
	broker.http = &http.Client{Timeout: 30 * time.Second}

	in := bufio.NewReader(os.Stdin)
	out := bufio.NewWriter(os.Stdout)
	for {
		line, err := in.ReadBytes('\n') // MCP stdio: newline-delimited JSON-RPC
		if t := bytes.TrimSpace(line); len(t) > 0 {
			if resp, ok := handle(t); ok {
				b, _ := json.Marshal(resp)
				_, _ = out.Write(b)
				_ = out.WriteByte('\n')
				_ = out.Flush()
			}
		}
		if err != nil {
			return // EOF / closed stdin
		}
	}
}

// ── JSON-RPC 2.0 ─────────────────────────────────────────────────────────────

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"` // absent on notifications
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// handle parses + dispatches one JSON-RPC message. The bool is false for
// notifications (no id) and unparseable lines — nothing is written back.
func handle(line []byte) (rpcResponse, bool) {
	var req rpcRequest
	if err := json.Unmarshal(line, &req); err != nil {
		return rpcResponse{}, false
	}
	return dispatch(req)
}

func dispatch(req rpcRequest) (rpcResponse, bool) {
	notification := len(req.ID) == 0
	reply := func(result any, e *rpcError) (rpcResponse, bool) {
		if notification {
			return rpcResponse{}, false
		}
		return rpcResponse{JSONRPC: "2.0", ID: req.ID, Result: result, Error: e}, true
	}

	switch req.Method {
	case "initialize":
		var p struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		_ = json.Unmarshal(req.Params, &p)
		pv := p.ProtocolVersion
		if pv == "" {
			pv = defaultProtocol
		}
		return reply(map[string]any{
			"protocolVersion": pv,
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]any{"name": serverName, "version": serverVersion},
		}, nil)
	case "notifications/initialized":
		return rpcResponse{}, false
	case "ping":
		return reply(map[string]any{}, nil)
	case "tools/list":
		out := make([]map[string]any, 0, len(tools))
		for _, t := range tools {
			out = append(out, map[string]any{
				"name": t.name, "description": t.desc, "inputSchema": t.schema,
			})
		}
		return reply(map[string]any{"tools": out}, nil)
	case "tools/call":
		var p struct {
			Name      string         `json:"name"`
			Arguments map[string]any `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return reply(nil, &rpcError{Code: -32602, Message: "invalid params"})
		}
		return reply(callTool(p.Name, p.Arguments), nil)
	default:
		return reply(nil, &rpcError{Code: -32601, Message: "method not found: " + req.Method})
	}
}

// ── tools ────────────────────────────────────────────────────────────────────

type tool struct {
	name    string
	desc    string
	schema  map[string]any
	forward func(args map[string]any) (path string, body any)
}

func obj(props map[string]any, required ...string) map[string]any {
	m := map[string]any{"type": "object", "properties": props}
	if len(required) > 0 {
		m["required"] = required
	}
	return m
}
func strProp(desc string) map[string]any {
	return map[string]any{"type": "string", "description": desc}
}
func intProp(desc string) map[string]any {
	return map[string]any{"type": "integer", "description": desc}
}

func str(a map[string]any, k string) string {
	if v, ok := a[k].(string); ok {
		return v
	}
	return ""
}
func num(a map[string]any, k string) int64 {
	switch v := a[k].(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	}
	return 0
}
func intArrProp(desc string) map[string]any {
	return map[string]any{"type": "array", "items": map[string]any{"type": "integer"}, "description": desc}
}
func intArr(a map[string]any, k string) []int64 {
	raw, ok := a[k].([]any)
	if !ok {
		return nil
	}
	out := make([]int64, 0, len(raw))
	for _, v := range raw {
		switch n := v.(type) {
		case float64:
			out = append(out, int64(n))
		case int64:
			out = append(out, n)
		}
	}
	return out
}

var tools = []tool{
	{
		name: "list_queues", desc: "List all queues and subscriptions.",
		schema:  obj(map[string]any{}),
		forward: func(a map[string]any) (string, any) { return wire.PathListQueues, wire.Empty{} },
	},
	{
		name: "create_queue", desc: "Create or update a queue by name.",
		schema: obj(map[string]any{"name": strProp("queue name")}, "name"),
		forward: func(a map[string]any) (string, any) {
			return wire.PathCreateQueue, wire.CreateQueueRequest{Name: str(a, "name")}
		},
	},
	{
		name: "send", desc: "Send a message to a queue.",
		schema: obj(map[string]any{
			"queue":      strProp("queue name"),
			"body":       strProp("message body (text)"),
			"message_id": strProp("optional dedup/idempotency key"),
			"group_id":   strProp("optional ordering/session key"),
		}, "queue", "body"),
		forward: func(a map[string]any) (string, any) {
			return wire.PathSend, wire.SendRequest{Queue: str(a, "queue"), Messages: []wire.Message{{
				Body: []byte(str(a, "body")), MessageID: str(a, "message_id"), GroupID: str(a, "group_id"),
			}}}
		},
	},
	{
		name: "receive", desc: "Receive (peek-lock) messages; returns seq_number + lock_token to settle with.",
		schema: obj(map[string]any{
			"queue":        strProp("queue name"),
			"max_messages": intProp("max messages (default 1)"),
			"wait_time_ms": intProp("long-poll wait in ms (default 0)"),
		}, "queue"),
		forward: func(a map[string]any) (string, any) {
			return wire.PathReceive, wire.ReceiveRequest{
				Queue: str(a, "queue"), MaxMessages: int(num(a, "max_messages")), WaitTimeMs: num(a, "wait_time_ms"),
			}
		},
	},
	{
		name: "complete", desc: "Complete (acknowledge) a received message by seq_number + lock_token.",
		schema: obj(map[string]any{
			"queue": strProp("queue name"), "seq_number": intProp("message seq number"), "lock_token": strProp("lock token from receive"),
		}, "queue", "seq_number", "lock_token"),
		forward: func(a map[string]any) (string, any) {
			return wire.PathComplete, wire.SettleRequest{Queue: str(a, "queue"), SeqNumber: num(a, "seq_number"), LockToken: str(a, "lock_token")}
		},
	},
	{
		name: "abandon", desc: "Abandon a received message so it is redelivered.",
		schema: obj(map[string]any{
			"queue": strProp("queue name"), "seq_number": intProp("message seq number"), "lock_token": strProp("lock token"), "delay_ms": intProp("optional redelivery delay ms"),
		}, "queue", "seq_number", "lock_token"),
		forward: func(a map[string]any) (string, any) {
			return wire.PathAbandon, wire.SettleRequest{Queue: str(a, "queue"), SeqNumber: num(a, "seq_number"), LockToken: str(a, "lock_token"), DelayMs: num(a, "delay_ms")}
		},
	},
	{
		name: "renew", desc: "Renew a received message's lock so it isn't redelivered while long work continues.",
		schema: obj(map[string]any{
			"queue": strProp("queue name"), "seq_number": intProp("message seq number"), "lock_token": strProp("lock token from receive"),
		}, "queue", "seq_number", "lock_token"),
		forward: func(a map[string]any) (string, any) {
			return wire.PathRenew, wire.SettleRequest{Queue: str(a, "queue"), SeqNumber: num(a, "seq_number"), LockToken: str(a, "lock_token")}
		},
	},
	{
		name: "defer", desc: "Defer a received message: set it aside for later retrieval by seq_number (see receive_deferred).",
		schema: obj(map[string]any{
			"queue": strProp("queue name"), "seq_number": intProp("message seq number"), "lock_token": strProp("lock token from receive"),
		}, "queue", "seq_number", "lock_token"),
		forward: func(a map[string]any) (string, any) {
			return wire.PathDefer, wire.SettleRequest{Queue: str(a, "queue"), SeqNumber: num(a, "seq_number"), LockToken: str(a, "lock_token")}
		},
	},
	{
		name: "receive_deferred", desc: "Retrieve previously deferred messages by their seq_numbers (re-locks them for settling).",
		schema: obj(map[string]any{
			"queue": strProp("queue name"), "seq_numbers": intArrProp("seq numbers of deferred messages to retrieve"),
		}, "queue", "seq_numbers"),
		forward: func(a map[string]any) (string, any) {
			return wire.PathReceiveDeferred, wire.ReceiveDeferredRequest{Queue: str(a, "queue"), SeqNumbers: intArr(a, "seq_numbers")}
		},
	},
	{
		name: "reject", desc: "Reject a received message to the dead-letter queue.",
		schema: obj(map[string]any{
			"queue": strProp("queue name"), "seq_number": intProp("message seq number"), "lock_token": strProp("lock token"), "reason": strProp("optional dead-letter reason"),
		}, "queue", "seq_number", "lock_token"),
		forward: func(a map[string]any) (string, any) {
			return wire.PathReject, wire.SettleRequest{Queue: str(a, "queue"), SeqNumber: num(a, "seq_number"), LockToken: str(a, "lock_token"), DeadLetterReason: str(a, "reason")}
		},
	},
	{
		name: "peek", desc: "Browse messages without locking (optionally by state).",
		schema: obj(map[string]any{
			"queue": strProp("queue name"), "state": strProp("optional: active|locked|deferred|scheduled|dead_lettered"), "max": intProp("max messages (default 32)"),
		}, "queue"),
		forward: func(a map[string]any) (string, any) {
			return wire.PathPeek, wire.PeekRequest{Queue: str(a, "queue"), State: str(a, "state"), Max: int(num(a, "max"))}
		},
	},
	{
		name: "stats", desc: "Queue counters by state (active/locked/deferred/scheduled/dead_lettered).",
		schema: obj(map[string]any{"queue": strProp("queue name")}, "queue"),
		forward: func(a map[string]any) (string, any) {
			return wire.PathStats, wire.MetricsRequest{Queue: str(a, "queue")}
		},
	},
	{
		name: "redrive", desc: "Move dead-lettered messages back to active (optionally a max count).",
		schema: obj(map[string]any{"queue": strProp("queue name"), "max": intProp("optional max to move")}, "queue"),
		forward: func(a map[string]any) (string, any) {
			return wire.PathRedrive, wire.RedriveRequest{Queue: str(a, "queue"), Max: int(num(a, "max"))}
		},
	},
	{
		name: "purge", desc: "Permanently delete dead-lettered messages (optionally a max count).",
		schema: obj(map[string]any{"queue": strProp("queue name"), "max": intProp("optional max to delete")}, "queue"),
		forward: func(a map[string]any) (string, any) {
			return wire.PathPurge, wire.PurgeRequest{Queue: str(a, "queue"), Max: int(num(a, "max"))}
		},
	},
}

func callTool(name string, args map[string]any) map[string]any {
	for _, t := range tools {
		if t.name == name {
			path, body := t.forward(args)
			text, err := post(path, body)
			if err != nil {
				return textResult("error: "+err.Error(), true)
			}
			return textResult(text, false)
		}
	}
	return textResult("unknown tool: "+name, true)
}

func textResult(text string, isErr bool) map[string]any {
	return map[string]any{"content": []map[string]any{{"type": "text", "text": text}}, "isError": isErr}
}

func post(path string, body any) (string, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequest(http.MethodPost, broker.endpoint+path, bytes.NewReader(b))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	if broker.token != "" {
		req.Header.Set("Authorization", "Bearer "+broker.token)
	}
	resp, err := broker.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	rb, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("broker %d: %s", resp.StatusCode, strings.TrimSpace(string(rb)))
	}
	return string(rb), nil
}
