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
	"strconv"
	"strings"
	"time"

	"github.com/mqlitehq/mqlite/engine"
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
		decoder := json.NewDecoder(bytes.NewReader(req.Params))
		decoder.UseNumber()
		if err := decoder.Decode(&p); err != nil {
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
func boolProp(desc string) map[string]any {
	return map[string]any{"type": "boolean", "description": desc}
}
func boolean(a map[string]any, k string) bool {
	v, _ := a[k].(bool)
	return v
}

func str(a map[string]any, k string) string {
	if v, ok := a[k].(string); ok {
		return v
	}
	return ""
}

func integer(value any) (int64, bool) {
	switch n := value.(type) {
	case json.Number:
		value, err := strconv.ParseInt(n.String(), 10, 64)
		return value, err == nil
	case float64:
		if n >= -0x1p63 && n < 0x1p63 && math.Trunc(n) == n {
			return int64(n), true
		}
	case int64:
		return n, true
	}
	return 0, false
}

func num(a map[string]any, k string) int64 {
	v, _ := integer(a[k])
	return v
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
		if n, ok := integer(v); ok {
			out = append(out, n)
		}
	}
	return out
}

func subscriptionFilter(a map[string]any) *engine.Filter {
	expr := str(a, "expr")
	if expr == "" {
		return nil
	}
	return &engine.Filter{Expr: expr}
}

// Integer tool arguments must not silently treat an invalid number as zero.
func validInteger(args map[string]any, name string) bool {
	value, present := args[name]
	if !present {
		return true
	}
	_, ok := integer(value)
	return ok
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
		name: "observe", desc: "Observe broker health and counters (monitor or manage).",
		schema:  keyObj(map[string]any{}),
		forward: func(a map[string]any) (string, any) { return wire.PathObserve, wire.ObserveRequest{} },
	},
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
		name: "subscribe", desc: "Create or update a named subscription (listen); ttl_ms is a fixed lifetime from first creation and is never renewed.",
		schema: keyObj(map[string]any{
			"topic":  strProp("topic name"),
			"name":   strProp("subscription name"),
			"expr":   strProp("optional expr-lang filter; omitted or empty matches all messages"),
			"ttl_ms": map[string]any{"type": "integer", "minimum": 1, "description": "optional fixed lifetime in milliseconds from first creation"},
		}, "topic", "name"),
		forward: func(a map[string]any) (string, any) {
			return wire.PathSubscribe, wire.SubscribeWithOptionsRequest{
				Topic: str(a, "topic"), Name: str(a, "name"), Filter: subscriptionFilter(a), TTLMs: num(a, "ttl_ms"),
			}
		},
	},
	{
		name: "list_subscriptions", desc: "List one bounded page of subscriptions (manage), optionally scoped to a topic.",
		schema: keyObj(map[string]any{
			"topic":            strProp("optional topic scope; keep it unchanged while paging"),
			"after":            strProp("opaque next_after cursor from the previous page"),
			"limit":            intProp("page size, default 100, maximum 1000"),
			"include_deleting": boolProp("include subscriptions whose deletion is pending"),
		}),
		forward: func(a map[string]any) (string, any) {
			return wire.PathListSubscriptionsPage, wire.ListSubscriptionsPageRequest{
				Topic: str(a, "topic"), After: str(a, "after"), Limit: int(num(a, "limit")),
				IncludeDeleting: boolean(a, "include_deleting"),
			}
		},
	},
	{
		name: "delete_queue", desc: "Delete an empty queue; force discards retained messages and may leave bounded cleanup pending.",
		schema: keyObj(map[string]any{
			"name":  strProp("queue name"),
			"force": boolProp("discard retained messages while deleting the queue"),
		}, "name"),
		forward: func(a map[string]any) (string, any) {
			return wire.PathDeleteQueue, wire.DeleteQueueRequest{Name: str(a, "name"), Force: boolean(a, "force")}
		},
	},
	{
		name: "delete_subscription", desc: "Delete an empty subscription (manage); force discards retained messages and may leave bounded cleanup pending.",
		schema: keyObj(map[string]any{
			"topic": strProp("topic name"),
			"name":  strProp("subscription name"),
			"force": boolProp("discard retained messages while deleting the subscription"),
		}, "topic", "name"),
		forward: func(a map[string]any) (string, any) {
			return wire.PathDeleteSubscription, wire.DeleteSubscriptionRequest{
				Topic: str(a, "topic"), Name: str(a, "name"), Force: boolean(a, "force"),
			}
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
		name: "abandon", desc: "End this delivery attempt: retry now or later below the delivery limit, otherwise dead-letter.",
		schema: obj(map[string]any{
			"queue": strProp("queue name"), "seq_number": intProp("message seq number"), "lock_token": strProp("lock token"), "delay_ms": intProp("optional retry delay ms"),
		}, "queue", "seq_number", "lock_token"),
		forward: func(a map[string]any) (string, any) {
			return wire.PathAbandon, wire.SettleRequest{Queue: str(a, "queue"), SeqNumber: num(a, "seq_number"), LockToken: str(a, "lock_token"), DelayMs: num(a, "delay_ms")}
		},
	},
	{
		name: "renew", desc: "Extend the same live delivery lock without increasing delivery_count; an expired lock cannot be revived.",
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
			"sort":     map[string]any{"type": "string", "enum": []string{"id_asc", "created_desc"}, "description": "default id_asc; created_desc lists newest first; keep the same sort when paging"},
		}),
		forward: func(a map[string]any) (string, any) {
			return wire.PathListKeys, wire.ListKeysRequest{AfterID: str(a, "after_id"), Limit: int(num(a, "limit")), Sort: str(a, "sort")}
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
			if err := validateStrictToolArgs(t, args); err != nil {
				return textResult("error: "+err.Error(), true)
			}
			path, body := t.forward(args)
			create, creating := body.(wire.CreateKeyRequest)
			if creating && !authkey.ValidID(create.ID) {
				return textResult("error: key id must be 32 lowercase hexadecimal characters", true)
			}
			if path == wire.PathObserve && len(args) != 0 {
				return textResult("error: observe takes no arguments", true)
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
			if creating && !validInteger(args, "expires_at_ms") {
				return textResult("error: create key ID "+create.ID+": expires_at_ms must be an integer", true)
			}
			if path == wire.PathListKeys && !validInteger(args, "limit") {
				return textResult("error: limit must be an integer", true)
			}
			if path == wire.PathListKeys {
				if value, present := args["sort"]; present && value != "id_asc" && value != "created_desc" {
					return textResult("error: sort must be id_asc or created_desc", true)
				}
			}
			text, err := post(path, body)
			if err == nil {
				var validated any
				switch request := body.(type) {
				case wire.ObserveRequest:
					validated, err = wire.DecodeObserveResponse([]byte(text))
				case wire.DeleteQueueRequest:
					var response wire.DeleteQueueResponse
					response, err = wire.DecodeDeleteResponse([]byte(text))
					if err != nil {
						err = fmt.Errorf("outcome unknown: invalid deletion acknowledgement; inspect list_queues to reconcile")
					} else {
						validated = response
					}
				case wire.DeleteSubscriptionRequest:
					var response wire.DeleteSubscriptionResponse
					response, err = wire.DecodeDeleteResponse([]byte(text))
					if err != nil {
						err = fmt.Errorf("outcome unknown: invalid deletion acknowledgement; inspect list_subscriptions(include_deleting: true) to reconcile")
					} else {
						validated = response
					}
				case wire.ListSubscriptionsPageRequest:
					var response wire.ListSubscriptionsPageResponse
					response, err = wire.DecodeListSubscriptionsPageResponse([]byte(text), request)
					if err == nil {
						validated = response
					}
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

func validateStrictToolArgs(t tool, args map[string]any) error {
	switch t.name {
	case "delete_queue", "subscribe", "list_subscriptions", "delete_subscription":
	default:
		return nil
	}
	properties := t.schema["properties"].(map[string]any)
	for key := range args {
		if _, ok := properties[key]; !ok {
			return fmt.Errorf("unknown %s argument", t.name)
		}
	}
	requireString := func(key string) error {
		if _, ok := args[key].(string); !ok {
			return fmt.Errorf("%s %s must be a string", t.name, key)
		}
		return nil
	}
	optionalString := func(key string) error {
		if value, present := args[key]; present {
			if _, ok := value.(string); !ok {
				return fmt.Errorf("%s %s must be a string", t.name, key)
			}
		}
		return nil
	}
	optionalBool := func(key string) error {
		if value, present := args[key]; present {
			if _, ok := value.(bool); !ok {
				return fmt.Errorf("%s %s must be a boolean", t.name, key)
			}
		}
		return nil
	}
	switch t.name {
	case "delete_queue":
		if err := requireString("name"); err != nil {
			return err
		}
		return optionalBool("force")
	case "subscribe":
		if err := requireString("topic"); err != nil {
			return err
		}
		if err := requireString("name"); err != nil {
			return err
		}
		if err := optionalString("expr"); err != nil {
			return err
		}
		if !validInteger(args, "ttl_ms") {
			return fmt.Errorf("subscribe ttl_ms must be an integer")
		}
		if _, present := args["ttl_ms"]; present && num(args, "ttl_ms") <= 0 {
			return fmt.Errorf("subscribe ttl_ms must be positive")
		}
		return nil
	case "delete_subscription":
		if err := requireString("topic"); err != nil {
			return err
		}
		if err := requireString("name"); err != nil {
			return err
		}
		return optionalBool("force")
	case "list_subscriptions":
		if err := optionalString("topic"); err != nil {
			return err
		}
		if err := optionalString("after"); err != nil {
			return err
		}
		if err := optionalBool("include_deleting"); err != nil {
			return err
		}
		if !validInteger(args, "limit") {
			return fmt.Errorf("list_subscriptions limit must be an integer")
		}
		if _, present := args["limit"]; present {
			limit := num(args, "limit")
			if limit < 1 || limit > 1000 {
				return fmt.Errorf("list_subscriptions limit must be between 1 and 1000")
			}
		}
	}
	return nil
}

func textResult(text string, isErr bool) map[string]any {
	return map[string]any{"content": []map[string]any{{"type": "text", "text": text}}, "isError": isErr}
}

func isLifecycleDeletePath(path string) bool {
	return path == wire.PathDeleteQueue || path == wire.PathDeleteSubscription
}

func deletionReconcileHint(path string) string {
	if path == wire.PathDeleteSubscription {
		return "inspect list_subscriptions(include_deleting: true) to reconcile"
	}
	return "inspect list_queues to reconcile"
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
	httpClient := broker.http
	if isLifecycleDeletePath(path) {
		clone := *broker.http
		clone.CheckRedirect = func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		}
		httpClient = &clone
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		if isLifecycleDeletePath(path) {
			return "", fmt.Errorf("outcome unknown: deletion response unavailable; %s: %s", deletionReconcileHint(path), err.Error())
		}
		return "", err
	}
	defer resp.Body.Close()
	if isLifecycleDeletePath(path) && resp.StatusCode/100 == 3 {
		return "", fmt.Errorf("outcome unknown: deletion received HTTP redirect %d; %s", resp.StatusCode, deletionReconcileHint(path))
	}
	if isLifecycleDeletePath(path) && resp.StatusCode/100 == 5 {
		// A gateway can return a 5xx after the broker committed but before its
		// acknowledgement arrived. Never expose its body as replay guidance.
		return "", fmt.Errorf("outcome unknown: deletion received HTTP %d; %s", resp.StatusCode, deletionReconcileHint(path))
	}
	var rb []byte
	switch path {
	case wire.PathCreateKey, wire.PathListKeys, wire.PathRevokeKey:
		rb, err = wire.ReadKeyResponse(resp.Body)
	case wire.PathObserve:
		rb, err = wire.ReadObserveResponse(resp.Body)
	case wire.PathListSubscriptionsPage:
		rb, err = wire.ReadListSubscriptionsPageResponse(resp.Body)
	case wire.PathDeleteQueue, wire.PathDeleteSubscription:
		rb, err = wire.ReadDeleteResponse(resp.Body)
	default:
		rb, err = io.ReadAll(resp.Body)
	}
	if err != nil {
		if isLifecycleDeletePath(path) {
			return "", fmt.Errorf("outcome unknown: invalid deletion acknowledgement; %s", deletionReconcileHint(path))
		}
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
