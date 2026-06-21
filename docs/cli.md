# CLI reference (`mqlite`)

One binary, **two modes** — the same commands work against a local SQLite DB
(embedded) or a running broker (client), chosen by environment:

- **Embedded:** set `MQLITE_DB` (e.g. `file:./mq.db`, `:memory:`, `libsql://…`). The
  command opens the DB in-process.
- **Client:** set `MQLITE_ENDPOINT` (+ `MQLITE_TOKEN`). The command talks to that
  broker over HTTP. **Client mode wins if `MQLITE_ENDPOINT` is set.**

```bash
mqlite <command> [flags] [args]
```

## Connection (environment)

| Env | Meaning |
|---|---|
| `MQLITE_DB` | embedded DB DSN: `file:./mq.db` / `:memory:` / `libsql://<db>.turso.io` |
| `MQLITE_DB_AUTH_TOKEN` | auth token for a remote libSQL/Turso DSN |
| `MQLITE_ENDPOINT` + `MQLITE_TOKEN` | client mode: a running broker + its Bearer token |
| `MQLITE_TOKENS` | broker mode (`serve`): comma-separated tokens to accept |
| `MQLITE_SYNC` | `NORMAL` (default) / `FULL` / `OFF` durability (embedded/serve) |
| `MQLITE_DLQ_MAX_AGE` · `MQLITE_DLQ_MAX_COUNT` · `MQLITE_DLQ_MAX_BYTES` | broker DLQ retention (`serve`) |

The DB string / endpoint is **only** read from the environment, never compiled in.

## Commands

### `serve` — run the broker
```bash
MQLITE_DB=file:/data/mq.db MQLITE_TOKENS=mqk_dev mqlite serve --addr :8080
```
| Flag | Default | |
|---|---|---|
| `--addr` | `:8080` | listen address |

Serves the RPC API, `/metrics`, `/ui`, and the open `/` + `/healthz`.

### `create-queue <name>` — create/update a queue
```bash
mqlite create-queue orders --lock 30s --max-delivery 5 --dedup 10m --ordering strict_fifo
```
| Flag | Default | |
|---|---|---|
| `--lock` | 30s | Peek-Lock duration |
| `--max-delivery` | 10 | deliveries before a message goes to the DLQ |
| `--ttl` | 0 (unlimited) | default message TTL |
| `--dedup` | 0 (off) | dedup window |
| `--ordering` | `standard` | `standard` / `group_fifo` / `strict_fifo` |

### `subscribe <topic> <name>` — create a subscription
```bash
mqlite subscribe orders eu-orders --expr 'subject_parts[0]=="orders" && properties["region"]=="eu"'
```
| Flag | | |
|---|---|---|
| `--expr` | "" | subscription filter — an [expr](filters.md) boolean predicate; empty = match all |

### `send <queue> <body>` — enqueue a message
`body` of `-` reads stdin; or use `--file`.
```bash
mqlite send orders '{"id":1}' --group order-1 --message-id m1
echo '{"id":2}' | mqlite send orders -
```
| Flag | | |
|---|---|---|
| `--file` | read body from a file |
| `--message-id` | dedup / idempotency key |
| `--group` | group id (ordering / session key) |
| `--subject` | subject (label) |
| `--reply-to` | reply-to address |
| `--ttl` | per-message TTL |

### `receive <queue>` — consume (Peek-Lock)
By default it receives and auto-Completes. Use `--no-ack` to inspect without
settling, or `--delete` for at-most-once.
```bash
mqlite receive orders --max 10 --wait 5s
```
| Flag | Default | |
|---|---|---|
| `--max` | 1 | max messages |
| `--wait` | 0 | long-poll wait (e.g. `5s`) |
| `--no-ack` | false | leave messages locked (don't Complete) |
| `--delete` | false | receive-and-delete (at-most-once, no lock) |

### `peek <queue>` — browse without locking
```bash
mqlite peek orders --state dead_lettered --max 20
```
| Flag | Default | |
|---|---|---|
| `--state` | "" | `active`/`locked`/`deferred`/`scheduled`/`dead_lettered` |
| `--from` | 0 | start sequence number |
| `--max` | 16 | max messages |

### `metrics <queue>` — queue counters
```bash
mqlite metrics orders     # active / locked / deferred / scheduled / dead_lettered / total
```

### `list` — list queues & subscriptions
```bash
mqlite list
```

### `redrive <queue>` — move dead letters back to active
```bash
mqlite redrive orders --max 100              # back to source
mqlite redrive orders --to orders-replay     # to another queue
```
| Flag | Default | |
|---|---|---|
| `--to` | "" | target queue (default: back to source) |
| `--max` | 0 (all) | max messages |
| `--older-than` | 0 | only messages older than this |

### `purge-dlq <queue>` — delete dead letters
```bash
mqlite purge-dlq orders --older-than 24h
```
| Flag | Default | |
|---|---|---|
| `--max` | 0 (all) | max messages |
| `--older-than` | 0 | only messages older than this |

### `version` / `help`
```bash
mqlite version
mqlite help
```

## Examples end to end

```bash
# embedded: produce + consume against a local file, no broker
export MQLITE_DB=file:./mq.db
mqlite create-queue jobs
mqlite send jobs "hello"
mqlite receive jobs --wait 2s

# client: same commands against a running broker
export MQLITE_ENDPOINT=https://your-mqlite.fly.dev MQLITE_TOKEN=mqk_prod_xxx
mqlite list
mqlite metrics jobs
```

See also: [api-reference.md](api-reference.md) (HTTP), [mcp.md](mcp.md) (agents),
[examples.md](examples.md) (Go SDK).
