# MCP server (`mqlite-mcp`)

`mqlite-mcp` is a [Model Context Protocol](https://modelcontextprotocol.io) server
that exposes the mqlite broker as **agent tools** — so an AI agent (Claude, etc.) can
create queues, send, receive, settle messages, and manage access keys without writing any HTTP. It is a
thin, **dependency-free** forwarder: it speaks MCP (JSON-RPC 2.0 over stdio) and turns
each tool call into one HTTP POST to the broker. Stdlib + the in-repo `wire` contract
only — no MCP SDK, no CGO (the same ethos as the rest of mqlite).

## Build & run

```bash
make build           # → bin/mqlite-mcp  (also: go build ./cmd/mqlite-mcp)
```

It's a stdio server: an MCP host launches it and talks over stdin/stdout. Configure
it with the broker endpoint + token:

| Env | Default | Meaning |
|---|---|---|
| `MQLITE_ENDPOINT` | `http://127.0.0.1:6754` | the broker base URL |
| `MQLITE_TOKEN` | — | configured administrator or managed broker access key; tool calls follow its permissions |

## Connect an agent host

Most MCP hosts take a JSON server entry. For example:

```json
{
  "mcpServers": {
    "mqlite": {
      "command": "/path/to/bin/mqlite-mcp",
      "env": {
        "MQLITE_ENDPOINT": "https://your-mqlite.fly.dev",
        "MQLITE_TOKEN": "mqk_prod_xxx"
      }
    }
  }
}
```

The agent then sees the tools below and can drive the broker directly.

## Tools

Minimal — selected core routes map 1:1 (kept small on purpose — over-specified tools make
models misuse them):

| Tool | Does |
|---|---|
| `list_queues` | list queues/subscriptions |
| `create_queue` | create/update a queue by name |
| `send` | send a message (`queue`, `body`, optional `message_id`/`group_id`) |
| `receive` | peek-lock messages → returns `seq_number` + `lock_token` |
| `complete` | acknowledge a message (`queue`, `seq_number`, `lock_token`) |
| `abandon` | release a message for redelivery |
| `reject` | dead-letter a message |
| `renew` | extend a message's lock so long-running work isn't redelivered |
| `defer` | set a message aside for later retrieval by `seq_number` |
| `receive_deferred` | retrieve deferred messages by their `seq_numbers` |
| `peek` | browse without locking (optionally by `state`) |
| `stats` | queue counters by state |
| `redrive` | move dead letters back to active |
| `purge` | permanently delete dead letters |
| `create_key` | create a managed key (`id`, `name`, `permissions`, optional `expires_at_ms`); returns the token once |
| `list_keys` | list public key metadata, including revoked/expired keys (`after_id`, `limit`, `sort`) |
| `revoke_key` | revoke a managed key by public `id`; repeated revocation is safe |

Settlement is by `lock_token` from `receive` — delivery is at-least-once, so an agent
should treat handlers as idempotent. Full HTTP semantics: [api-reference.md](api-reference.md).

## Access key management

The three key tools require `manage`; both an environment administrator token and
an active managed `manage` key may issue further administrator keys. `send` and
`listen` keys receive `permission_denied` for these tools. The MCP server forwards
`MQLITE_TOKEN` on every call, and broker authorization decides what it may do.

Before calling `create_key`, choose and retain a unique 32-character lowercase
hexadecimal `id`. The `id` is public metadata, distinct from the secret token:

```json
{
  "name": "create_key",
  "arguments": {
    "id": "e734810a9f634d75bd8a029c1f7e5062",
    "name": "order-producer",
    "permissions": ["send"]
  }
}
```

`permissions` accepts `send`, `listen`, their combination, or `manage` (which
includes both). `expires_at_ms` is a future UTC epoch-millisecond timestamp; omit
it or use zero for no expiry. The response has `key` metadata and a one-time
`token` in the `mqk_` + 64 lowercase hex format. Store the token securely; it will
be visible in the successful MCP tool result and may be retained by your MCP
host. List responses never contain secrets or token hashes.

Creation is not retried automatically. An error includes the retained valid
public ID; after an uncertain result, use `list_keys` and `revoke_key` for that ID
before issuing a replacement with a new ID. Reusing an ID returns `key_conflict`
and cannot recover a secret. Pass `next_after_id` from `list_keys` back as
`after_id` until the cursor is absent; the default limit is 100, maximum 1000.
The optional `sort` is `id_asc` (default) or `created_desc` (newest-created first,
then descending public ID for timestamp ties). Keep the same sort on every page.
Creation-order cursors must identify an existing key, including revoked or
expired keys. The default ID order also accepts absent predecessor IDs when
reconciling a lost create response.
Environment tokens are not listed or revocable through these tools. Key tools are
unavailable when broker authentication is explicitly disabled.
