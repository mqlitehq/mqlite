<p align="center"><img src="logo.svg" width="72" height="72" alt="mqlite"></p>

# mqlite

A small, SQLite/Turso-backed online message queue with **Azure Service Bus–style
semantics** — Peek-Lock, retries, DLQ, scheduling, dedup, MessageGroupId
ordering, topics — in a single pure-Go binary (no CGO).

> **Embed it like goqite, or serve it like a broker — the same engine.**
> Start in-process (with same-DB transactional enqueue), and upgrade to a
> network broker with one line when you outgrow it.

## Why mqlite

- **One file, one binary.** Local SQLite (`modernc.org/sqlite`, pure Go) or
  remote **Turso/libSQL** — the *same* SQL and semantics, selected by a
  connection string.
- **Service Bus semantics, honestly at-least-once.** Peek-Lock + four-way settle
  (`Complete`/`Abandon`/`DeadLetter`/`Defer`), visibility timeout with fencing
  tokens, `delivery_count` → DLQ, `RenewLock`, scheduled/deferred messages.
- **Approximate order by default, strict order opt-in.** Plain queues are
  competing-consumer; set a `SessionID` (= SQS MessageGroupId) for strict
  per-group ordering with cross-group parallelism.
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

### 1. Embedded (in-process, like goqite)

```go
ctx := context.Background()
eng, _ := mqlite.OpenEmbedded(ctx, "file:./mq.db")
defer eng.Close()
eng.CreateQueue(ctx, "orders", mqlite.QueueConfig{})

eng.Send(ctx, "orders", mqlite.OutMessage{Body: []byte("hello"), SessionID: "order-42"})

msgs, _ := eng.Receive(ctx, "orders", mqlite.WithWait(5*time.Second))
for _, m := range msgs { _ = m.Complete(ctx) }

// ⭐ same-DB transactional enqueue (business write + enqueue commit together):
eng.Tx(ctx, func(tx *engine.EngineTx) error {
    tx.SQL().ExecContext(ctx, `INSERT INTO orders_tbl(id) VALUES (1)`)
    _, err := tx.Send("orders", engine.OutMessage{Body: []byte("evt")})
    return err // commit both, or roll back both
})
```

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
# send (body is base64)
curl -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  --data "{\"queue\":\"orders\",\"messages\":[{\"body\":\"$(printf hello | base64)\"}]}" \
  http://127.0.0.1:8080/mqlite.v1.QueueService/Send

# receive (long-poll 5s) → returns lock_token
curl -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  --data '{"queue":"orders","max_messages":1,"wait_time_ms":5000}' \
  http://127.0.0.1:8080/mqlite.v1.QueueService/Receive
```

### 3b. Web UI (read-only ops panel)

The broker serves a read-only dashboard at **`http://<host>/ui`** — list queues with
live counts, browse messages by state, and (the one write action) redrive a DLQ.
The page loads without auth; its data calls use the Bearer token you paste in.

### 4. Or the Go SDK (remote)

```go
cli, _ := mqlite.Open(ctx, "http://127.0.0.1:8080", mqlite.WithToken("mqk_dev"))
seq, _ := cli.Send(ctx, "orders", mqlite.OutMessage{Body: []byte("hi")})

// hands-off consumer: nil -> Complete, error -> Abandon, auto lock renewal
cli.Receiver("orders", mqlite.WithAutoRenew(), mqlite.WithConcurrency(4)).
    Run(ctx, func(ctx context.Context, m *mqlite.Message) error {
        return process(m.Body) // MUST be idempotent (at-least-once)
    })
```

### 5. CLI

```bash
mqlite create-queue orders --lock 30s --max-delivery 5 --dedup 10m
mqlite send orders "hello" --message-id m1 --session order-42
mqlite receive orders --wait 5s
mqlite peek orders --state dead_lettered
mqlite metrics orders
mqlite redrive orders --max 100        # DLQ → active
```

Connection is read from the environment:

| Env | Meaning |
|---|---|
| `MQLITE_DB` | DB DSN: `file:./mq.db`, `:memory:`, or `libsql://<db>.turso.io` (embedded/serve) |
| `MQLITE_DB_AUTH_TOKEN` | auth token for a remote libSQL/Turso DSN |
| `MQLITE_ENDPOINT` + `MQLITE_TOKEN` | client mode: talk to a running broker (wins if set) |
| `MQLITE_TOKENS` | comma-separated Bearer tokens that `serve` accepts |

> The DB connection string is **only ever read from the environment** — it is
> never compiled into the binary. Copy `.env.example` → `.env.local` (gitignored).

## Docker

```bash
docker build --platform linux/amd64 -t mqlite:0.1.0 .
docker run --platform linux/amd64 -p 8080:8080 -e MQLITE_TOKENS=mqk_dev mqlite:0.1.0
# remote Turso instead of the local volume:
docker run --platform linux/amd64 -p 8080:8080 \
  -e MQLITE_DB=libsql://<db>.turso.io -e MQLITE_DB_AUTH_TOKEN=<jwt> \
  -e MQLITE_TOKENS=mqk_dev mqlite:0.1.0
```

## Tests

```bash
go test ./...                 # hermetic unit + invariant (TCK-style) tests
# live remote round-trip against your own Turso DB:
MQLITE_TEST_DB=libsql://<db>.turso.io MQLITE_TEST_DB_AUTH_TOKEN=<jwt> \
  go test ./engine -run TestTursoIntegration -v
```

## Status

v0.1 — the core is complete and tested (local SQLite + live Turso). See
[`BUILD-LOG.md`](BUILD-LOG.md) for what's implemented, what's deferred, and the
build/verification record.
