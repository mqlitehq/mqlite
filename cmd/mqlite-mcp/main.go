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
	"math"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/mqlitehq/mqlite/internal/authkey"
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

func keyObj(props map[string]any, required ...string) map[string]any {
	m := obj(props, required...)
	m["additionalProperties"] = false
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

// Key expiry and pagination must not silently treat an invalid number as zero.
func validKeyInteger(args map[string]any, name string) bool {
	value, present := args[name]
	if !present {
		return true
	}
	switch n := value.(type) {
	case float64:
		return n >= -0x1p63 && n < 0x1p63 && math.Trunc(n) == n
	case int64:
		return true
	default:
		return false
	}
}

// strArr rejects mixed-type arrays instead of silently discarding an invalid
// permission. The broker remains the authority for allowed permission names.
func strArr(a map[string]any, key string) []string {
	raw, ok := a[key].([]any)
	if !ok {
		return nil
	}
	out := make([]string, len(raw))
	for i, value := range raw {
		s, ok := value.(string)
		if !ok {
			return nil
		}
		out[i] = s
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
	{
		name: "create_key", desc: "Create an access key (manage required). Save the one-time token; retain id for recovery.",
		schema: keyObj(map[string]any{
			"id":            map[string]any{"type": "string", "pattern": "^[0-9a-f]{32}$", "description": "unique public key ID; choose before the call"},
			"name":          strProp("key name"),
			"permissions":   map[string]any{"type": "array", "items": map[string]any{"type": "string", "enum": []string{"send", "listen", "manage"}}, "minItems": 1, "description": "manage includes send and listen"},
			"expires_at_ms": intProp("UTC epoch milliseconds; 0 or omitted means no expiry"),
		}, "id", "name", "permissions"),
		forward: func(a map[string]any) (string, any) {
			return wire.PathCreateKey, wire.CreateKeyRequest{ID: str(a, "id"), Name: str(a, "name"), Permissions: strArr(a, "permissions"), ExpiresAtMs: num(a, "expires_at_ms")}
		},
	},
	{
		name: "list_keys", desc: "List access key metadata without secrets (manage required). Includes revoked and expired keys.",
		schema: keyObj(map[string]any{
			"after_id": strProp("next_after_id from the previous page"),
			"limit":    intProp("page size, default 100, maximum 1000"),
		}),
		forward: func(a map[string]any) (string, any) {
			return wire.PathListKeys, wire.ListKeysRequest{AfterID: str(a, "after_id"), Limit: int(num(a, "limit"))}
		},
	},
	{
		name: "revoke_key", desc: "Revoke a managed access key by public id (manage required). Repeating revocation is safe.",
		schema: keyObj(map[string]any{"id": strProp("public key ID, not the secret token")}, "id"),
		forward: func(a map[string]any) (string, any) {
			return wire.PathRevokeKey, wire.RevokeKeyRequest{ID: str(a, "id")}
		},
	},
}

func callTool(name string, args map[string]any) map[string]any {
	for _, t := range tools {
		if t.name == name {
			path, body := t.forward(args)
			create, creating := body.(wire.CreateKeyRequest)
			if creating && !authkey.ValidID(create.ID) {
				return textResult("error: key id must be 32 lowercase hexadecimal characters", true)
			}
			if creating || path == wire.PathListKeys || path == wire.PathRevokeKey {
				// Validate against the published schema itself. In particular, a
				// misspelled expiry must not silently create a non-expiring key.
				properties := t.schema["properties"].(map[string]any)
				for name := range args {
					if _, known := properties[name]; !known {
						message := "unknown access key argument"
						if creating {
							message = "create key ID " + create.ID + ": " + message
						}
						return textResult("error: "+message, true)
					}
				}
			}
			if creating && !validKeyInteger(args, "expires_at_ms") {
				return textResult("error: create key ID "+create.ID+": expires_at_ms must be an integer", true)
			}
			if path == wire.PathListKeys && !validKeyInteger(args, "limit") {
				return textResult("error: limit must be an integer", true)
			}
			text, err := post(path, body)
			if err == nil {
				var validated any
				switch request := body.(type) {
				case wire.CreateKeyRequest:
					validated, err = wire.DecodeCreateKeyResponse([]byte(text), request)
					if err != nil {
						err = fmt.Errorf("invalid create response; list and revoke this ID before issuing a replacement")
					}
				case wire.ListKeysRequest:
					validated, err = wire.DecodeListKeysResponse([]byte(text), request)
				case wire.RevokeKeyRequest:
					validated, err = wire.DecodeRevokeKeyResponse([]byte(text))
					if err != nil {
						err = fmt.Errorf("invalid revocation acknowledgement; verify the key state or repeat revocation")
					}
				}
				if validated != nil && err == nil {
					// Only reviewed fields reach the tool result. Never echo an
					// invalid success body, unknown field or partial token.
					normalized, _ := json.Marshal(validated)
					text = string(normalized) + "\n"
				}
			}
			if creating && err != nil {
				err = fmt.Errorf("create key ID %s: %w", create.ID, err)
			}
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
	var rb []byte
	if path == wire.PathCreateKey || path == wire.PathListKeys || path == wire.PathRevokeKey {
		rb, err = wire.ReadKeyResponse(resp.Body)
	} else {
		rb, err = io.ReadAll(resp.Body)
	}
	if err != nil {
		return "", err
	}
	if resp.StatusCode >= 300 {
		if path == wire.PathCreateKey || path == wire.PathListKeys || path == wire.PathRevokeKey {
			// Keep the recognized error code, never arbitrary intermediary text:
			// an error response must not disclose a partial creation secret.
			var envelope wire.ErrorBody
			_ = json.Unmarshal(rb, &envelope)
			code := "request failed"
			switch envelope.Code {
			case "unauthenticated", "permission_denied", "key_conflict", "not_found", "invalid_argument", "message_too_large", "outcome_unknown", "internal", "canceled", "unimplemented":
				code = envelope.Code
			}
			return "", fmt.Errorf("broker %d: %s", resp.StatusCode, code)
		}
		return "", fmt.Errorf("broker %d: %s", resp.StatusCode, strings.TrimSpace(string(rb)))
	}
	return string(rb), nil
}
