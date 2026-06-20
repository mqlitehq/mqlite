<p align="center"><img src="docs/logo.svg" width="72" height="72" alt="mqlite"></p>

# mqlite

[![CI](https://github.com/mqlitehq/mqlite/actions/workflows/ci.yml/badge.svg)](https://github.com/mqlitehq/mqlite/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/mqlitehq/mqlite/branch/main/graph/badge.svg)](https://codecov.io/gh/mqlitehq/mqlite)

A small, SQLite/Turso-backed online message queue with **Azure Service Bus–style
semantics** — Peek-Lock, retries, DLQ, scheduling, dedup, GroupID
ordering, topics — in a single pure-Go binary (no CGO).

> **Embed it like goqite, or serve it like a broker — the same engine.**
> Start in-process (with same-DB transactional enqueue), and upgrade to a
> network broker with one line when you outgrow it.

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
  or double-delivering messages.
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
- **Service Bus semantics, honestly at-least-once.** Peek-Lock + four-way settle
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
> remote Turso DB is serialized server-side, so neither takes the lock.)

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
        _ = m.Abandon(ctx)   // release the lock → redelivered (or DLQ past max)
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
    tx.SQL().ExecContext(ctx, `INSERT INTO orders_tbl(id) VALUES (1)`)
    _, err := tx.SendOne("orders", engine.OutMessage{Body: []byte("order-created")})
    return err // commit both, or roll back both
})
```

> Outgrow a single process? The *same* engine upgrades to a network broker with one
> call — `eng.Serve(ctx, ":8080")` — and remote clients speak the same semantics
> over HTTP (§2–§4). Embedded and broker are not two products; they are one engine.

### 2. Serve a broker

```bash
export MQLITE_DB="file:./mq.db"           # or libsql://<db>.turso.io
export MQLITE_DB_AUTH_TOKEN="<jwt>"        # only for remote Turso
export MQLITE_TOKENS="mqk_dev"             # accepted Bearer tokens
mqlite serve --addr :8080
```

### 3. Talk to it with curl

```bash
TOKEN=mqk_dev
# discovery: hit the root with no auth — a JSON card (name, version, status, endpoints)
curl http://127.0.0.1:8080/

# create the queue first — sending to a queue that doesn't exist is a 404
curl -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  --data '{"name":"orders","config":{}}' \
  http://127.0.0.1:8080/mqlite.v1.AdminService/CreateQueue

# send (body is base64)
curl -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  --data "{\"queue\":\"orders\",\"messages\":[{\"body\":\"$(printf hello | base64)\"}]}" \
  http://127.0.0.1:8080/mqlite.v1.QueueService/Send

# receive (long-poll 5s) → returns lock_token
curl -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  --data '{"queue":"orders","max_messages":1,"wait_time_ms":5000}' \
  http://127.0.0.1:8080/mqlite.v1.QueueService/Receive
```

> **`Send` response.** `/Send` returns `{"seq_numbers":[…]}` — one per message, in
> order. A *single* message that hits a dedup conflict (same `message_id`, different
> body) is rejected with **409**; in a *multi-message batch* the conflicting slot
> comes back as `0` (skipped, not enqueued) while the rest still commit.

### 3b. Web UI (read-only ops panel)

The broker serves a read-only dashboard at **`http://<host>/ui`** — list queues with
live counts, browse messages by state, and (the one write action) redrive a DLQ.
The page loads without auth; its data calls use the Bearer token you paste in.

### 4. Or the Go SDK (remote)

```go
cli, _ := mqlite.Open(ctx, "http://127.0.0.1:8080", mqlite.WithToken("mqk_dev"))
seq, _ := cli.SendOne(ctx, "orders", mqlite.OutMessage{Body: []byte("hi")})

// hands-off consumer: nil -> Complete, error -> Abandon, auto lock renewal
cli.Receiver("orders", mqlite.WithAutoRenew(), mqlite.WithConcurrency(4)).
    Run(ctx, func(ctx context.Context, m *mqlite.Message) error {
        return process(m.Body) // MUST be idempotent (at-least-once)
    })
```

### 5. CLI

```bash
mqlite create-queue orders --lock 30s --max-delivery 5 --dedup 10m --ordering strict_fifo
mqlite send orders "hello" --message-id m1 --group order-42 --reply-to replies
mqlite receive orders --wait 5s
mqlite peek orders --state dead_lettered
mqlite metrics orders
mqlite redrive orders --max 100        # DLQ → active
mqlite purge-dlq orders --older-than 24h # delete dead-lettered messages
```

Connection is read from the environment:

| Env | Meaning |
|---|---|
| `MQLITE_DB` | DB DSN: `file:./mq.db`, `:memory:`, or `libsql://<db>.turso.io` (embedded/serve) |
| `MQLITE_DB_AUTH_TOKEN` | auth token for a remote libSQL/Turso DSN |
| `MQLITE_ENDPOINT` + `MQLITE_TOKEN` | client mode: talk to a running broker (wins if set) |
| `MQLITE_TOKENS` | comma-separated Bearer tokens that `serve` accepts |
| `MQLITE_SYNC` | durability level: `NORMAL` (default) / `FULL` / `OFF` |
| `MQLITE_DLQ_MAX_AGE` · `MQLITE_DLQ_MAX_COUNT` | broker DLQ retention (defaults 14d / 1,000,000 per queue, drop-oldest; `MQLITE_DLQ_RETENTION=off` disables) |

> The DB connection string is **only ever read from the environment** — it is
> never compiled into the binary. Copy `.env.example` → `.env.local` (gitignored).

## Durability

mqlite stores everything in SQLite/libSQL with **WAL** journaling. State is durable
across a normal process crash: on restart, in-flight (`locked`) messages are
reclaimed and redelivered (at-least-once), and settled work stays settled.

The one knob is SQLite's `synchronous` level (database-wide — SQLite has no
per-queue setting):

| Level | Guarantee | Cost |
|---|---|---|
| `NORMAL` *(default)* | Durable across an app/process crash. A sudden **OS or power loss** can drop the last few commits not yet checkpointed to the main DB file. | Fast — fsync per WAL checkpoint, not per commit. |
| `FULL` | Every commit is fsync'd before it returns — survives OS/power loss. | Slower under high enqueue throughput. |

Set it with the embedded option `mqlite.WithSynchronous("FULL")`, or `MQLITE_SYNC=FULL`
for the `serve`/CLI path. Remote **Turso** DSNs ignore this — durability there is the
server's responsibility.

> **What "no message loss" means here:** mqlite is honestly **at-least-once**, not
> exactly-once. Under `NORMAL`, a power cut can lose a handful of just-enqueued
> messages that hadn't reached disk yet; choose `FULL` if that window is
> unacceptable. Once a message is durably written it is delivered at least once and
> never silently dropped.

## Retention

Completed messages are deleted on settle and TTL'd messages expire, so a healthy
queue's size tracks its in-flight backlog. The one sink that can grow without bound
is the **dead-letter queue** — so `serve` bounds it by default: a background pass
drops dead letters oldest-first once they are older than **14 days** or beyond
**1,000,000 per queue** (`MQLITE_DLQ_MAX_AGE` / `MQLITE_DLQ_MAX_COUNT`, or
`MQLITE_DLQ_RETENTION=off`). Only the DLQ is ever touched — undelivered and in-flight
work is never auto-deleted. The embedded library leaves this off unless you opt in
with `mqlite.WithDLQRetention(...)`. Freed pages are reused (the file plateaus at
peak backlog rather than shrinking; `VACUUM` is manual). See
[docs/retention.md](docs/retention.md).

## Docker

```bash
# --pull forces the latest golang:1.25 base (newest Go stdlib security patches) —
# use it for release builds so a cached old base layer can't ship known CVEs.
docker build --platform linux/amd64 --pull -t mqlite:0.1.0 .
docker run --platform linux/amd64 -p 8080:8080 -e MQLITE_TOKENS=mqk_dev mqlite:0.1.0
# remote Turso instead of the local volume:
docker run --platform linux/amd64 -p 8080:8080 \
  -e MQLITE_DB=libsql://<db>.turso.io -e MQLITE_DB_AUTH_TOKEN=<jwt> \
  -e MQLITE_TOKENS=mqk_dev mqlite:0.1.0
```

**Footprint:** ~11 MB static (CGO-free) binary, ~19 MB idle RSS, ~0.4 KB per message
on disk — fits Fly's smallest 256 MB machine with room to spare. Measured numbers and
Fly.io machine/volume sizing: [docs/resource-profile.md](docs/resource-profile.md).

## Tests

```bash
go test ./...                 # hermetic unit + invariant (TCK-style) tests
go test -bench=. ./engine     # embedded throughput benchmarks (msg/s)
go test ./engine -run TestMessageIntegrity      # no-loss + content-hash sweep (default 1k)
MQLITE_INTEGRITY_N=500000 go test ./engine -run TestMessageIntegrity  # large sweep
# live remote round-trip against your own Turso DB:
MQLITE_TEST_DB=libsql://<db>.turso.io MQLITE_TEST_DB_AUTH_TOKEN=<jwt> \
  go test ./engine -run TestTursoIntegration -v
```

A contiguous `1..N` sequence is sent with random bodies (each hashed into a
property) and consumed concurrently with redelivery stress; the test asserts every
value arrives exactly once (**no loss**) and every body still matches its hash (**no
corruption**). Local + cloud (Fly) throughput/memory/disk methodology and numbers:
[docs/benchmark.md](docs/benchmark.md). The large sweep also runs weekly in CI.

## Status

v0.1 — the core is complete and tested (local SQLite + live Turso): hermetic
unit + invariant (TCK-style) tests run in CI on every push, and live
Turso/libSQL round-trips run in the nightly workflow. Not yet tagged for
release.

## License

[MIT](LICENSE) © mqlitehq
