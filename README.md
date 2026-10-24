<p align="center"><img src="docs/logo.svg" width="72" height="72" alt="mqlite"></p>

# mqlite

[![CI](https://github.com/mqlitehq/mqlite/actions/workflows/ci.yml/badge.svg)](https://github.com/mqlitehq/mqlite/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/mqlitehq/mqlite@v0.4.1.svg)](https://pkg.go.dev/github.com/mqlitehq/mqlite@v0.4.1)
[![Go Report Card](https://goreportcard.com/badge/github.com/mqlitehq/mqlite)](https://goreportcard.com/report/github.com/mqlitehq/mqlite)
[![codecov](https://codecov.io/gh/mqlitehq/mqlite/branch/main/graph/badge.svg)](https://codecov.io/gh/mqlitehq/mqlite)
[![Supported release: v0.4.1](https://img.shields.io/badge/release-v0.4.1-blue)](https://github.com/mqlitehq/mqlite/releases/tag/v0.4.1)
[![Go](https://img.shields.io/github/go-mod/go-version/mqlitehq/mqlite)](go.mod)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A small, SQLite/Turso-backed online message queue with **Azure Service Bus–style
semantics** — Peek-Lock, retries, DLQ, scheduling, dedup, GroupID
ordering, topics — in a single pure-Go binary (no CGO).

> **Embed it like goqite, or serve it like a broker — the same engine.**
> Start in-process (with same-DB transactional enqueue), and upgrade to a
> network broker with one line when you outgrow it.

> **Release status:** v0.4.1 is the latest supported release; v0.3.3 is withdrawn
> and unsupported. Its public assets and exact image remain available for
> forensic or compatibility inspection and must not be deployed. v0.4.1 carries
> its retraction directive. Pin production installs to the exact v0.4.1 release or
> `ghcr.io/mqlitehq/mqlite:0.4.1`.

## Goal

mqlite aims to be the **smallest reliable queue you can drop into a system** — and
to stay friendly to both humans and AI agents:

- **Lightweight & flexible.** One pure-Go binary over a single SQLite file (or a
  remote Turso database) — no broker cluster, no ZooKeeper, no sidecar required.
  Embed it in your process or run it as a broker; move the storage from a
  `:memory:` test to a replicated Turso DB without changing a line of app code.
- **Reliable under concurrency.** At-least-once delivery with fencing tokens,
  single-broker crash recovery, idempotent send/receive/settle, and O(log n) claims
  on a deep backlog — built to take high enqueue/dequeue throughput without losing
  durably committed work. Delivery can repeat, so handlers must be idempotent.
- **Simple, unambiguous interface.** Every operation is one plain HTTP `POST` with
  a JSON body — curl-able, trivially scriptable, and easy for an LLM or agent to
  drive — with exactly one settlement verb per outcome and no aliased calls.
- **Service Bus flavor, not a clone.** Peek-Lock, `GroupID` sessions, DLQ,
  scheduling and dedup will feel familiar if you know Azure Service Bus — but the
  API is its own, shaped to be idiomatic Go and unambiguous rather than
  bug-for-bug compatible.

It targets most everyday queueing needs — background jobs, transactional
outbox/event delivery, topic fan-out to subscriptions, and rate-limited pipelines —
rather than competing with Kafka-scale streaming.

## Why mqlite

- **One file, one binary.** Local SQLite (`modernc.org/sqlite`, pure Go) or
  remote **Turso/libSQL** — the *same* SQL and semantics, selected by a
  connection string.
- **Service Bus semantics with explicit delivery modes.** Peek-Lock is
  at-least-once; receive-and-delete is the caller-selected at-most-once
  broker-removal path.
  Peek-Lock supports four-way settle
  (`Complete`/`Abandon`/`Reject`/`Defer`), visibility timeout with fencing
  tokens, `delivery_count` → DLQ, `Renew`, scheduled/deferred messages.
- **Approximate order by default, strict order opt-in.** Plain queues are
  competing-consumer. Pick a queue-level ordering mode at create time:
  `standard` (default — per-`GroupID` FIFO with cross-group parallelism),
  `group_fifo` (same, but a `GroupID` is required on every message), or
  `strict_fifo` (single global head-of-line FIFO across the whole queue).
  `GroupID` is an **ordering key** (= SQS MessageGroupId / ASB SessionId) — *not*
  a consumer group; competing consumers just `Receive` the same queue.
- **curl-able contract.** Every RPC is a plain HTTP `POST` to
  `/mqlite.v1.<Service>/<Method>` with a JSON body; the Go SDK is sugar on top.

## Layout

| Path | What |
|---|---|
| `engine/` | The queue core (Store + Service Bus semantics). Transport-agnostic. |
| `server/` | Connect-style JSON-over-HTTP broker + Bearer-token auth. |
| `wire/` | The JSON contract shared by server and client (one source of truth). |
| `.` (`package mqlite`) | Native Go SDK: remote `Client`, `Embedded` engine, `Receiver`. |
| `cmd/mqlite/` | Single-binary CLI: `serve`, `send`, `receive`, `peek`, … |

## Quickstart

Full copy-runnable programs (embedded, transactional outbox, remote consumer, curl):
[docs/examples.md](docs/examples.md).

### 1. Embedded (in-process — no broker, no HTTP, no second process)

mqlite's primary form is a **library you embed directly in your Go process**, exactly
like `goqite` or using `database/sql` against SQLite. `OpenEmbedded` gives you the
full queue — Send/Receive/Peek-Lock/DLQ/scheduling/topics — calling the storage
engine **in-process**. There is **no broker to run, no network hop, no JSON
serialization, no extra daemon**: your app and the queue are one binary backed by
one SQLite (or Turso) database. You only start an HTTP server if you *choose* to
(see §2) — the embedded path never opens a socket.

> **Single process, single writer.** Embedded mode is one process owning one
> database. `OpenEmbedded` takes an exclusive lock on the DB file, so a second
> process — or a second `OpenEmbedded` on the same file — fails fast with
> `ErrDBLocked` instead of racing it (two writers would corrupt crash recovery and
> claim ordering). Sharing one file DB across processes is **not supported**: when
> you need multiple processes or hosts, run the broker (§2) and connect over HTTP —
> that single broker is the one writer. (`:memory:` is private per handle, and a
> remote Turso DB is serialized server-side, so neither takes the local file lock.)
> A Turso database must still have exactly one active MQLite broker or embedded
> `Engine`. Replace that owner stop-then-start after confirming the old process has
> exited; rolling overlap against the same database is unsupported.

```go
ctx := context.Background()

// The whole MQ, in your process. file: local SQLite, or libsql://… for Turso.
eng, err := mqlite.OpenEmbedded(ctx, "file:./mq.db")
if err != nil { log.Fatal(err) }
defer eng.Close()

eng.CreateQueue(ctx, "orders", mqlite.QueueConfig{})

// produce
eng.SendOne(ctx, "orders", mqlite.OutMessage{Body: []byte("hello"), GroupID: "order-42"})

// consume (Peek-Lock): handle, then settle. at-least-once → handler must be idempotent.
msgs, _ := eng.Receive(ctx, "orders", mqlite.RecvOpts{Wait: 5 * time.Second})
for _, m := range msgs {
    if err := handle(m.Body); err != nil {
        _ = m.Abandon(ctx)   // retry below max; this failed attempt goes to DLQ at max
        continue
    }
    _ = m.Complete(ctx)      // remove it (idempotent under retries)
}

// or hands-off: a Receiver auto-settles (nil→Complete, err→Abandon) — still in-process.
eng.Receiver("orders", mqlite.WithConcurrency(4)).
    Run(ctx, func(ctx context.Context, m *mqlite.Message) error { return handle(m.Body) })
```

**⭐ Transactional outbox — the embedded superpower.** Because the queue lives in
the *same* SQLite database as your application tables, you can enqueue a message in
the **same transaction** as your business write. No dual-write, no outbox poller,
no lost events: either both commit or neither does.

```go
eng.Tx(ctx, func(tx *engine.EngineTx) error {
    // tx.Context(), not the outer ctx: see the note below.
    if _, err := tx.SQL().ExecContext(tx.Context(), `INSERT INTO orders_tbl(id) VALUES (1)`); err != nil {
        return err // your business write failed — roll back the message too
    }
    _, err := tx.SendOne("orders", engine.OutMessage{Body: []byte("order-created")})
    return err // commit both, or roll back both
})
```

> **Keep the callback transaction-bound.** On a **remote** (Turso/libSQL) store the callback may
> run **more than once**: a transaction that fails on a retryable connection/busy error is replayed
> from the start. The SQL rolls back, so your *data* stays correct — but anything that is not part
> of the transaction (an HTTP call, a charge, a counter in memory) will have happened twice. Local
> file and `:memory:` stores never retry, so the callback runs at most once there; an
> already-cancelled caller never enters it.

> **Local cancellation:** use `tx.Context()` for business SQL to protect SQLite from interruption,
> and check the original `ctx.Err()` between statements. Raw `tx.SQL()` calls bypass mqlite's
> per-statement guards and can still execute after cancellation. The callback must return before
> the writer is released; cancellation observed before commit rolls back all its writes, even if
> it returned nil. Use raw SQL only for application-owned tables; directly modifying mqlite's
> internal tables is unsupported. See the [cancellation contract](docs/conformance.md#13--cancellation-context-deadlines).

> Outgrow a single process? The *same* engine upgrades to a network broker with one
> call — `eng.Serve(ctx, ":6754")` — and remote clients speak the same semantics
> over HTTP (§2–§4). Embedded and broker are not two products; they are one engine.

### 2. Serve a broker

```bash
export MQLITE_DB="file:./mq.db"           # or libsql://<db>.turso.io
export MQLITE_DB_AUTH_TOKEN="<jwt>"        # only for remote Turso
export MQLITE_TOKENS="mqk_dev"             # administrator Bearer tokens
mqlite serve --addr :6754
```

### 3. Talk to it with curl

```bash
TOKEN=mqk_dev
# discovery: hit the root with no auth — a JSON card (name, version, status, endpoints)
curl http://127.0.0.1:6754/

# create the queue first — sending to a queue that doesn't exist is a 404
curl -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  --data '{"name":"orders","config":{}}' \
  http://127.0.0.1:6754/mqlite.v1.AdminService/CreateQueue

# send (body is base64)
curl -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  --data "{\"queue\":\"orders\",\"messages\":[{\"body\":\"$(printf hello | base64)\"}]}" \
  http://127.0.0.1:6754/mqlite.v1.QueueService/Send

# receive (long-poll 5s) → returns lock_token
curl -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  --data '{"queue":"orders","max_messages":1,"wait_time_ms":5000}' \
  http://127.0.0.1:6754/mqlite.v1.QueueService/Receive
```

> **`Send` response.** `/Send` returns `{"seq_numbers":[…]}` — one per message, in
> order. A *single* message that hits a dedup conflict (same `message_id`, different
> body) is rejected with **409**; in a *multi-message batch* the conflicting slot
> comes back as `0` (skipped, not enqueued) while the rest still commit.

Full endpoint, request/response, and error reference: [docs/api-reference.md](docs/api-reference.md).

### 3b. Admin console

The broker bakes a web console into the binary and serves it at **`/ui`** (e.g.
`http://127.0.0.1:6754/ui`) — no separate process or Node runtime. Paste your broker URL
+ token and you can list queues with live counts, browse messages, send/receive/settle,
edit subscription filters, inspect permanent or fixed subscription lifetimes in the
topic list, and redrive a DLQ. Current brokers use `Observe` alone for the five-second
poll; subscription inventory is refreshed on connection, explicit refresh, and
configuration changes instead of on message operations. It's on by default; set
`MQLITE_UI=off` to run headless. (The same console is also published standalone for
hosting elsewhere.)

Starting with v0.3.2, a configured monitor token opens only the read-only overview
and metrics views; administration still requires `manage`.

### 4. Or the Go SDK (remote)

```go
cli, _ := mqlite.Open(ctx, "http://127.0.0.1:6754", mqlite.WithToken("mqk_dev"))
seq, _ := cli.SendOne(ctx, "orders", mqlite.OutMessage{Body: []byte("hi")})

// hands-off consumer: nil -> Complete, error -> Abandon, auto lock renewal
cli.Receiver("orders", mqlite.WithAutoRenew(), mqlite.WithConcurrency(4)).
    Run(ctx, func(ctx context.Context, m *mqlite.Message) error {
        return process(m.Body) // MUST be idempotent (at-least-once)
    })
```

`delivery_count` counts claims, not time spent processing: `Receive` and explicit
`ReceiveDeferred` claims increment it; `Renew`, `RenewBatch`, `Abandon`, and the
scheduler do not. With `max_delivery_count=10`, the tenth ordinary Peek-Lock `Receive` is a
full final attempt — it may be renewed repeatedly and completed successfully. It
does not dead-letter merely because the claim reached 10; max-delivery handling
runs if that attempt is abandoned, its lease expires, or startup recovery finds it
still locked. `ReceiveDeferred` is an explicit-retrieval exception:
it increments the count but does not currently reject a claim merely because the
count is already at the configured maximum. See [Core concepts](docs/concepts.md#queue).

Message TTL is separate from both the delivery attempt and subscription lifetime.
A positive queue default is also the ceiling for an explicit Send TTL. The effective
TTL starts at the message's initial visibility time, so scheduled waiting does not
consume it; Renew and delayed Abandon do not restart or extend it. Expiry ends normal
delivery and either moves the message to the DLQ (the default) or discards it when
`dead_letter_on_expire=false`. Batched SDK sends may give each message a different
TTL; v0.4.1 preserves those values over HTTP instead of applying one item’s TTL to
the whole batch.

### 5. CLI

The `mqlite` binary is a first-party client for the common broker operations — the same
commands work embedded (against `MQLITE_DB`) or against a running broker (`--endpoint`):

```bash
mqlite create-queue orders --lock 30s --max-delivery 5 --dedup 10m --ordering strict_fifo
mqlite send orders "hello" --message-id m1 --group order-42 --reply-to replies
mqlite schedule orders "later" --at 30m          # future delivery; cancel <queue> <seq>
mqlite receive orders --wait 5s                  # auto-Complete; --no-ack prints lock tokens
mqlite complete orders 42 <lock-token>           # settle a --no-ack message (also abandon/reject/defer/renew)
mqlite peek orders --state dead_lettered
mqlite status                                    # backend / broker clock / ping / counts
mqlite observe --output json                     # unified observation; monitor or manage
mqlite subscribe replies instance-7 --ttl 15m   # fixed lifetime; omit --ttl for permanent
mqlite list-subscriptions --topic events --limit 100
mqlite delete-subscription events worker                 # empty-only
mqlite delete-subscription events worker --force         # explicitly discard retained work
mqlite test-filter 'subject == "x"'
mqlite metrics orders --output json              # machine-readable: the same keys as the HTTP API
mqlite redrive orders --max 100                  # DLQ → active
mqlite purge-dlq orders --older-than 24h         # delete dead-lettered (unbounded needs --all)
mqlite delete-queue orders                        # empty-only
mqlite delete-queue orders --force                # explicitly discard every retained state
```

The data and admin commands take the global flags `--endpoint <url>` / `--token <t>` (override
the environment) and `--output text|json` — `serve`, `version` and `help` don't, and `vacuum`
is local-only. Under `--output json` a message **is** the HTTP API's message object, key for
key. See the [CLI reference](docs/cli.md) for the full set.

Connection is read from `--endpoint`/`--token`, or from the environment:

| Env | Meaning |
|---|---|
| `MQLITE_DB` | DB DSN: `file:./mq.db`, `:memory:`, or `libsql://<db>.turso.io` (embedded/serve) |
| `MQLITE_DB_AUTH_TOKEN` | auth token for a remote libSQL/Turso DSN |
| `MQLITE_ENDPOINT` + `MQLITE_TOKEN` | client mode: talk to a running broker (wins if set) |
| `MQLITE_TOKENS` | comma-separated administrator Bearer tokens for `serve` |
| `MQLITE_MONITOR_TOKENS` | optional, distinct read-only credentials for `observe` and `/metrics`; administrator auth must remain enabled |
| `MQLITE_METRICS` | enable authenticated Prometheus `/metrics` on the API port (`on`/`true`/`1`; default `off`); requires administrator auth |
| `MQLITE_SYNC` | durability level: `NORMAL` (default) / `FULL` / `OFF` / `EXTRA` (an unknown value is rejected at startup) |
| `MQLITE_MAX_SUBSCRIPTIONS` | v0.4.0+: maximum active plus deleting subscriptions (default 10,000) |
| `MQLITE_DLQ_MAX_AGE` · `MQLITE_DLQ_MAX_COUNT` · `MQLITE_DLQ_MAX_BYTES` | asynchronous broker DLQ retention targets (defaults 14d / 1,000,000 per queue, drop-oldest; byte target off by default; `MQLITE_DLQ_RETENTION=off` disables broker-wide defaults) |

> The DB connection string is **only ever read from the environment** — it is
> never compiled into the binary. `.env.example` lists the variables mqlite reads;
> the binary does **not** auto-load a dotenv file, so export them into the process
> yourself — `set -a && . ./.env.local && set +a` for a shell, or an `Environment=`/
> compose `environment:` block for systemd/Docker.

Starting with v0.3.1, create additional keys without restarting the broker using
`mqlite key create --name producer --permissions send`. Keys can grant `send`,
`listen`, both, or `manage` (which also grants key issuance and revocation).
Configured administrator tokens remain supported. See [key commands](docs/cli.md#key-createlistrevoke--manage-persistent-access-keys)
and the [complete access-key guide](docs/access-keys.md).

Starting with v0.3.2, `mqlite observe` and configured `MQLITE_MONITOR_TOKENS`
provide read-only broker observation. See the [monitoring guide](docs/observability.md)
and [Prometheus/Grafana quickstart](ops/observability/README.md).

Prometheus scraping is opt-in: set `MQLITE_METRICS=on` or run `mqlite serve --metrics`.
The API then serves authenticated `/metrics`, and discovery includes
`"metrics":"/metrics"`. With metrics disabled, the route returns `404` and the
field is omitted. Give the collector a monitor token. To keep the route off the
public Internet, use a private broker or block `/metrics` in your public ingress;
Bearer authentication alone does not make the route private. Prometheus and Grafana
can run locally or in an existing platform; no second MQLite VM is required. See
[the monitoring guide](docs/observability.md).

### 6. MCP (drive it from an AI agent)

`mqlite-mcp` is a Model Context Protocol server that exposes the broker as agent
tools (create_queue, delete_queue, send, receive, complete, …) — a thin,
dependency-free stdio forwarder, in keeping with mqlite's "friendly to AI agents"
goal. Point it at a
broker with `MQLITE_ENDPOINT` + `MQLITE_TOKEN`. See [docs/mcp.md](docs/mcp.md).

## Documentation

**Use it** — pick your interface:

| | |
|---|---|
| [CLI reference](docs/cli.md) | every command + flags; works embedded or against a broker |
| [HTTP API reference](docs/api-reference.md) | endpoints, request/response, error codes |
| [MCP server](docs/mcp.md) | drive mqlite from an AI agent |
| [Examples](docs/examples.md) | copy-runnable Go (embedded · outbox · remote) + curl |

**Operate it:**
[Deployment](docs/deployment.md) (Docker/Fly/systemd/Turso) ·
[Production operations](docs/operations.md) (backup/restore/upgrade) ·
[Observability](docs/observability.md) (Prometheus/Grafana) ·
[Retention](docs/retention.md) ·
[Turso](docs/turso.md)

**Understand it:**
[Concepts & filters](docs/concepts.md) (model + `expr` filter language) ·
[Internals](docs/internals.md) (how it's built: schema, single-writer, fencing, recovery) ·
[Conformance / TCK](docs/conformance.md) ·
[Benchmarks, sizing & tuning](docs/benchmark.md) ·
[Dependencies](docs/dependencies.md)

## Durability

For a local SQLite file, mqlite uses **WAL** journaling. Successfully committed
state survives a normal process crash when the filesystem provides SQLite's
required locking and flush semantics: on restart, in-flight (`locked`) messages
are reclaimed and redelivered (at-least-once), and settled work stays settled.

The one knob is SQLite's `synchronous` level (database-wide — SQLite has no
per-queue setting):

| Level | Guarantee | Cost |
|---|---|---|
| `NORMAL` *(default)* | Protects acknowledged commits across an app/process crash. A sudden **OS or power loss** can lose recent commits that were not safely flushed. | Fast — fsync per WAL checkpoint, not per commit. |
| `FULL` | Requests an fsync before each commit returns; designed to survive OS/power loss when the storage stack honors flushes. | Slower under high enqueue throughput. |

Set it with the embedded option `mqlite.WithSynchronous("FULL")`, or `MQLITE_SYNC=FULL`
for the `serve`/CLI path. Remote **Turso** DSNs ignore this — durability there is the
server's responsibility.

> **Durability and retention boundary:** default Peek-Lock delivery is
> **at-least-once**, not exactly-once; receive-and-delete explicitly trades
> redelivery for at-most-once broker removal. Under `NORMAL`, a power cut can lose a
> handful of just-enqueued messages that hadn't reached disk yet; choose `FULL`
> if that window is unacceptable. Once a message is durably written, normal
> delivery retains it until a documented lifecycle action removes it. Explicit
> force deletion, subscription expiry,
> message TTL with discard, and configured DLQ retention are authorized discard
> paths; handlers must still tolerate redelivery.

## Retention

Completed messages are deleted on settle, but message TTL expiry only ends normal
delivery: it enters the DLQ by default and is discarded immediately only when queue
policy requests that. Retained active and dead-letter backlog therefore drives
storage use. Active work has no automatic retention ceiling and can grow when
producers outrun consumers. For dead-lettered work,
`serve` applies default retention targets: a background pass drops dead letters
oldest-first once they are older than **14 days** or beyond **1,000,000 per
queue** (`MQLITE_DLQ_MAX_AGE` / `MQLITE_DLQ_MAX_COUNT`; an optional
per-queue byte cap `MQLITE_DLQ_MAX_BYTES` is off by default;
`MQLITE_DLQ_RETENTION=off` disables the broker-wide defaults). Cleanup is
asynchronous and bounded, so a DLQ can temporarily
exceed an age, count, or byte target between passes or while the worker catches
up. That retention worker touches only dead letters; it never deletes
active, locked, deferred, or scheduled work. Other documented lifecycle actions
can remove retained work: message TTL may discard on expiry, fixed subscription
TTL authorizes forced cleanup, and explicit force deletion discards the selected
resource. The embedded library leaves engine-wide DLQ retention defaults off
unless you opt in with `mqlite.WithDLQRetention(...)`; positive per-queue bounds
still apply. Freed pages are reused. For local files, a bounded background
`incremental_vacuum` pass gradually returns eligible free pages after its threshold
is reached; cleanup lag and long readers can delay or limit shrinkage. Size storage
for retained data plus WAL, indexes, fragmentation, reclamation lag and backups.
A full `VACUUM` rewrite stays manual-only via `mqlite vacuum`. See
[docs/retention.md](docs/retention.md).

Starting with v0.4.0, queue and subscription deletion follow the same
bounded-storage principle. They
are empty-only by default; `--force` explicitly discards active, locked,
scheduled, deferred and dead-letter work. The accepting transaction moves the
resource out of the data plane and starts bounded cleanup. If cleanup finishes,
that commit removes the resource and returns `deleted`; otherwise it commits a
`deleting` marker that reserves the name, and background cleanup resumes after
restart. The final removal of a
subscription backing queue also removes its topic mapping. An implicit topic
becomes unavailable for fan-out once it has no active subscriptions. Any deleting
mapping continues to reserve that address until physical cleanup completes. A
locked delivery may already have produced an external side effect,
which deletion cannot undo. Serialize deletion
and same-name recreation, use globally unique one-use names for temporary
subscriptions, and wait until the relevant inventory no longer shows the name
before reusing it.

A subscription may also have a fixed lifetime. `SubscribeWithOptions`,
`subscribe --ttl`, HTTP `ttl_ms`, MCP and the console all store one absolute
deadline from the first successful creation. The deadline never moves when a
client retries, edits the filter, publishes or receives messages. At
`now >= expires_at_ms` the subscription immediately leaves fan-out and its data
plane; expiry is advance authorization to discard retained messages through the
same bounded cleanup path. Use explicit deletion when normal work finishes and
TTL as a backstop for a lost caller. A large unconsumed backlog still needs time
and disk to clean, and keeps its subscription-capacity slot until removal.

The default 10,000-subscription ceiling counts both active and deleting entries;
pending cleanup intentionally keeps its slot. It bounds metadata growth, while
publish work and stored message copies still grow with
`messages × matching subscriptions`. Treat 10,000 as a safety ceiling rather than
a low-latency fan-out promise, and measure the roster sizes used by your workload.

## Docker

```bash
# --pull forces the latest golang:1.27-alpine3.24 base (Go stdlib security patches) —
# use it for release builds so a cached old base layer can't ship known CVEs.
docker build --platform linux/amd64 --pull -t mqlite:dev .
docker run --platform linux/amd64 -p 6754:6754 -e MQLITE_TOKENS=mqk_dev mqlite:dev
# remote Turso instead of the local volume:
docker run --platform linux/amd64 -p 6754:6754 \
  -e MQLITE_DB=libsql://<db>.turso.io -e MQLITE_DB_AUTH_TOKEN=<jwt> \
  -e MQLITE_TOKENS=mqk_dev mqlite:dev
```

**Historical footprint sample:** ~11 MB static (CGO-free) binary, ~19 MB idle
RSS, and ~0.4 KB per 256-byte message in the recorded benchmark environment. These
are sizing inputs, not current-release limits. Fly.io machine and volume guidance:
[docs/benchmark.md](docs/benchmark.md).

Full copy-runnable recipes — Docker/GHCR, **Fly.io** (minimal-cost, scale-to-zero),
**systemd**, and **Turso** — with auth and verification:
[docs/deployment.md](docs/deployment.md). Wiring `/metrics` into Prometheus + Grafana
(scrape config, PromQL, alerts): [docs/observability.md](docs/observability.md).

## Tests

```bash
go test ./...                 # hermetic unit + invariant (TCK-style) tests
go test -bench=. ./engine     # embedded throughput benchmarks (msg/s)
go test ./engine -run TestMessageIntegrity      # accounting + content-hash sweep (default 1k)
MQLITE_INTEGRITY_N=500000 go test ./engine -run TestMessageIntegrity  # large sweep
# destructive live remote suite against a dedicated, disposable Turso test DB:
MQLITE_TEST_DB=libsql://<db>.turso.io MQLITE_TEST_DB_AUTH_TOKEN=<jwt> \
  MQLITE_TEST_DB_RESET_CONFIRM=1 \
  go test ./engine -run TestTursoIntegration -v
# crash-injection: hard-kill a worker and check recovery (separate Linux CI job; tag-gated):
make crash                    # = go test -race -tags crash_injection -count=1 ./test/crash/
```

`make crash` runs with the race detector; `go test -race ./...` skips this tagged suite.
It checks outbox atomicity, survival of producer-acknowledged commits, and orphaned-lock recovery
to active or DLQ at the delivery limit. It tests process death, not power loss.

The live Turso suite drops every MQLite-managed table before tests begin. Use only
a dedicated disposable database; the explicit reset confirmation is required even
when selecting one `TestTurso*` test with `-run`.

A contiguous `1..N` sequence is sent with random bodies (each hashed into a
property) and consumed concurrently with redelivery stress; the test accounts for
every sent value and verifies every body hash. Its uniqueness assertion is a
test-run invariant, not an exactly-once delivery promise. Local and cloud (Fly)
throughput, memory, and disk methodology and numbers:
[docs/benchmark.md](docs/benchmark.md). The large sweep also runs weekly in CI.

## Status

The latest supported release is **v0.4.1** (port 6754, schema token 7). Pin
production binaries to the v0.4.1 release and containers to
`ghcr.io/mqlitehq/mqlite:0.4.1`. v0.3.3 is withdrawn and unsupported; its public
assets and exact image remain available for forensic or compatibility inspection
and must not be deployed. v0.4.1 carries its retraction directive.

v0.4.1 is a schema-compatible patch for v0.4.0. It keeps the console, CLI and Go
SDK usable after a deleted resource contributes to the documented retired
observation aggregate, exposes the broker lifecycle clock and makes subscription
deadlines visible in the console's topic list and detail view, and preserves each
message TTL in remote mixed-TTL batches. Upgrade the v0.4.0 broker and Go client
together before relying on mixed batch TTLs; no database rebuild is required for
this patch upgrade.

v0.4.0 removes v0.3.3's public queue `Generation` contract and adds the corrected
resource lifecycle APIs described in [CHANGELOG.md](CHANGELOG.md).

**Moving a schema-5 database to v0.4.0 requires a fresh database.** Stop writers,
take a consistent backup, account for retained work in
every state, and preserve the old binary/database pair for rollback. Do not restamp
or point schema-7 code at the old file. Schema 6 belonged only to intermediate
unreleased lifecycle builds and is also refused. See
[upgrade and rollback](docs/operations.md#upgrade-and-rollback).

v0.4.0 also removes the deprecated Prometheus aliases
`mqlite_queue_total` and `mqlite_queue_oldest_message_age_ms`. Migrate to
`mqlite_queue_retained_messages` and
`mqlite_queue_oldest_message_age_seconds` before upgrading. The native JSON field
`oldest_message_age_ms` remains unchanged.

**Upgrading from v0.2.0 requires a new database.** There is no in-place schema
migration. Account for retained messages in every state, preserve the old binary
and a consistent database backup, and follow the
[upgrade and rollback procedure](docs/operations.md#upgrade-and-rollback).
Delivery remains at-least-once; consumers must be idempotent.

Hermetic unit + invariant (TCK-style) tests run in CI on every push, and live
Turso/libSQL round-trips run in the nightly workflow.

## License

[MIT](LICENSE) © The MQLite Authors
