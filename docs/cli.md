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
| `MQLITE_TOKENS` | broker (`serve`) Bearer tokens; **unset → a `mqk_…` token is generated + printed**, `=off` disables auth |
| `MQLITE_SYNC` | `NORMAL` (default) / `FULL` / `OFF` / `EXTRA` durability (embedded/serve). An unrecognized value is **rejected at startup** — a typo never silently downgrades to `NORMAL`. |
| `MQLITE_DLQ_MAX_AGE` · `MQLITE_DLQ_MAX_COUNT` · `MQLITE_DLQ_MAX_BYTES` | broker DLQ retention (`serve`); on by default, disable with `MQLITE_DLQ_RETENTION=off` |

The DB string is **never compiled in** — it comes from the environment. The endpoint comes
from the environment too, or from `--endpoint` (below), which overrides it.

## Global flags (data + admin commands)

| Flag | Meaning |
|---|---|
| `--endpoint <url>` | client mode: a running broker (overrides `MQLITE_ENDPOINT`) |
| `--token <bearer>` | Bearer token for `--endpoint` (overrides `MQLITE_TOKEN`) |
| `--output text\|json` | `text` (default, human) or `json` (machine-readable, for scripting) |

They apply to the data and admin commands. **`serve`, `version` and `help` take none of them**
(a broker is not a client), and **`vacuum` is local-only** — it rejects `--endpoint` outright,
because reclaiming pages means opening the DB file directly.

An ambient `MQLITE_TOKEN` is sent only to the endpoint that environment names. If `--endpoint`
points at a *different* broker, the token is withheld (with a warning) rather than leaked to
another host — pass `--token` to authenticate there. `--token=` (empty) explicitly sends no
credential at all. Two spellings of the same broker (a trailing slash, `mqlite://h` vs
`http://h:6754`) are the same target and keep the token.

### `--output json` uses the wire's field names

Every message the CLI emits in `--output json` **is** the HTTP API's message object — the same
struct, so the same field names and encodings (`seq_number`, `body` base64, epoch-ms
timestamps). The CLI never invents or renames a key.

Two things are the CLI's own, and are *not* claims about the HTTP response:

- **The envelope.** `receive`/`peek` print a bare JSON **array** of messages; the HTTP API
  returns a `{"messages": [...]}` object.
- **The lock token.** It is emitted only where you need it to settle later — `receive --no-ack`
  and `receive-deferred`. A default auto-ack `receive` has already Completed the messages, so it
  omits `lock_token`; raw Peek-Lock Receive always returns one.

The CLI also doesn't expose *every* HTTP operation (batch send, attempt ids). For the complete
contract — all routes, all fields — see [api-reference.md](api-reference.md).

> **Settling across invocations needs a broker.** `receive --no-ack` locks a message and
> prints its `lock-token`; you settle it later with `complete/abandon/reject/defer/renew
> <queue> <seq> <token>`. This works against a running broker (client mode), which holds
> the lock between calls. In **embedded** mode each invocation opens the DB fresh and
> reclaims orphaned locks, so a `--no-ack` lock does not survive to the next command — use
> a broker (`mqlite serve` + `--endpoint`) when you need manual settle-later.

## Commands

### `serve` — run the broker
```bash
MQLITE_DB=file:/data/mq.db MQLITE_TOKENS=mqk_dev mqlite serve --addr :6754
```
| Flag | Default | |
|---|---|---|
| `--addr` | `:6754` | listen address |
| `--insecure-allow-remote` | `false` | with auth disabled, allow a non-loopback bind (otherwise refused) |

The listen address may also come from **`MQLITE_ADDR`** (precedence: `--addr` >
`MQLITE_ADDR` > `:6754`; a blank value is rejected). With auth disabled
(`MQLITE_TOKENS=off`) the broker **refuses a non-loopback bind** unless
`--insecure-allow-remote` is passed, and **`MQLITE_CORS` defaults to off**.

Serves the RPC API, `/metrics`, the open `/` + `/healthz`, and — unless
`MQLITE_UI=off` — the embedded admin console at `/ui`.

### `create-queue <name>` — create/update a queue
```bash
mqlite create-queue orders --lock 30s --max-delivery 5 --dedup 10m --ordering strict_fifo
```
| Flag | Default | |
|---|---|---|
| `--lock` | 0 → 30s | Peek-Lock duration (`0` inherits the engine default, 30s) |
| `--max-delivery` | 0 → 10 | deliveries before a message goes to the DLQ (`0` inherits 10) |
| `--ttl` | 0 (unlimited) | default message TTL |
| `--dedup` | 0 (off) | dedup window |
| `--ordering` | `standard` | `standard` / `group_fifo` / `strict_fifo` |
| `--dlq-max-age` | 0 (inherit) | per-queue DLQ retention: drop dead letters older than this |
| `--dlq-max-count` | 0 (inherit) | per-queue: keep at most N dead letters (`-1` = unbounded) |
| `--dlq-max-bytes` | 0 (inherit) | per-queue: cap dead-letter body bytes (`-1` = unbounded) |

### `subscribe <topic> <name>` — create a subscription
*(alias: `create-subscription`)*
```bash
mqlite subscribe orders eu-orders --expr 'subject_parts[0]=="orders" && properties["region"]=="eu"'
```
| Flag | | |
|---|---|---|
| `--expr` | "" | subscription filter — an [expr](concepts.md#subscription-filters-expr) boolean predicate; empty = match all |

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
By default it receives and auto-Completes. Use `--no-ack` to leave messages locked and
print each `lock-token` (settle them later — see settlement commands below), or `--delete`
for at-most-once.
```bash
mqlite receive orders --max 10 --wait 5s
```
| Flag | Default | |
|---|---|---|
| `--max` | 1 | max messages |
| `--wait` | 0 | long-poll wait (e.g. `5s`) |
| `--no-ack` | false | leave messages locked (don't Complete) |
| `--delete` | false | receive-and-delete (at-most-once, no lock) |

**Auto-ack never acknowledges output you can't see.** A message is only Completed *after* its
body is written to stdout successfully. If the write fails (broken pipe) the message is left
locked and redelivered; if stdout cannot deliver at all (closed at exec, or `/dev/null`) the
command refuses up front — nothing is claimed. Use `--delete` to drain explicitly, or
`--no-ack` if you only want the lock tokens.

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
Prints a human line by default; use `--output json` for the JSON object. (**Changed in
0.3.0:** `metrics` previously printed JSON unconditionally — scripts that parsed the old
default must add `--output json`.)
```bash
mqlite metrics orders                 # active / locked / deferred / scheduled / dead_lettered / total
mqlite metrics orders --output json   # machine-readable
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
Destructive. An **unbounded** purge requires an explicit `--all`; otherwise pass a bound
(`--max`/`--older-than`). Negative bounds are rejected (they would otherwise mean "no
limit"). Exactly one positional `<queue>` — a stray argument is an error, not ignored.
```bash
mqlite purge-dlq orders --older-than 24h    # bounded
mqlite purge-dlq orders --all               # delete the entire DLQ (explicit)
```
| Flag | Default | |
|---|---|---|
| `--max` | 0 | max messages to delete (0 = unbounded → `--all` required) |
| `--older-than` | 0 | only delete messages older than this (**minimum 1ms** — a sub-millisecond value would round to "no bound") |
| `--all` | false | delete the entire DLQ (required to run unbounded) |

### `vacuum` — reclaim free DB pages to the OS
Local maintenance (embedded; **stop the broker first** — the single-writer lock will
reject it otherwise). New local DBs use `auto_vacuum=INCREMENTAL`, so the default is a
no-lock `PRAGMA incremental_vacuum`; `--full` runs a full `VACUUM` (rewrites the file,
global lock). Not applicable to a remote Turso/libSQL store.
```bash
MQLITE_DB=file:/data/mq.db mqlite vacuum          # incremental
MQLITE_DB=file:/data/mq.db mqlite vacuum --full   # full rewrite
```

### `complete` / `abandon` / `reject` / `defer` / `renew` — settle a message
Settle a message received earlier with `receive --no-ack` (which printed its
`lock-token`). Positional args are always `<queue> <seq> <lock-token>`. Client mode only
in practice (a broker holds the lock between calls — see the note under Global flags).
```bash
mqlite complete orders 42 lt_9f3a…                 # done
mqlite abandon  orders 42 lt_9f3a… --delay 60s     # release for retry, after a backoff
mqlite reject   orders 42 lt_9f3a… --reason bad    # dead-letter it (--detail …)
mqlite defer    orders 42 lt_9f3a…                 # set aside for receive-deferred
mqlite renew    orders 42 lt_9f3a…                 # extend the lock lease
```

### `schedule <queue> <body>` — enqueue for future delivery
```bash
mqlite schedule orders '{"id":1}' --at 30m                     # a duration from now
mqlite schedule orders '{"id":1}' --at 2027-01-02T15:04:05Z    # or an absolute RFC3339 time (must be future)
```
| Flag | | |
|---|---|---|
| `--at` | required | delivery time: RFC3339 or a duration (`30m`, `2h`) |
| `--file` · `--message-id` · `--group` · `--subject` | | same as `send` |

Cancel a scheduled message before it activates with **`cancel <queue> <seq>`**.

### `receive-deferred <queue> --seq …` — fetch deferred messages by seq
```bash
mqlite receive-deferred orders --seq 42,57      # re-locks them and prints tokens to settle
```

### `status` — backend snapshot
```bash
mqlite status                 # backend, location, schema, ping, size, queue/subscription counts
```

### `list-subscriptions` — subscriptions with topic + filter
```bash
mqlite list-subscriptions
```

### `test-filter <expr>` — dry-run a subscription filter
Compiles the expression and, with a sample, reports whether it would match (nothing is
enqueued).
```bash
mqlite test-filter 'subject == "ord.created"'                       # just validate
mqlite test-filter 'subject == "ord.created"' --subject ord.created # validate + test a sample
```
| Flag | | |
|---|---|---|
| `--subject` · `--group` · `--prop k=v,k=v` · `--file` | | build the sample message |

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
