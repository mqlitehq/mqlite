# MCP server (`mqlite-mcp`)

`mqlite-mcp` is a [Model Context Protocol](https://modelcontextprotocol.io) server
that exposes the mqlite broker as **agent tools** — so an AI agent (Claude, etc.) can
create queues, send, receive, and settle messages without writing any HTTP. It is a
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
| `MQLITE_TOKEN` | — | Bearer token (one of the broker's `MQLITE_TOKENS`) |

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

Minimal, 1:1 with the core API (kept small on purpose — over-specified tools make
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
| `peek` | browse without locking (optionally by `state`) |
| `stats` | queue counters by state |
| `redrive` | move dead letters back to active |
| `purge` | permanently delete dead letters |

Settlement is by `lock_token` from `receive` — delivery is at-least-once, so an agent
should treat handlers as idempotent. Full HTTP semantics: [api-reference.md](api-reference.md).
