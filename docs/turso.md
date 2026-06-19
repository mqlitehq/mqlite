# Turso / libSQL: concurrency & embedded replicas (MQLITE-4)

MQLite runs the same engine on a local SQLite file or a remote
[Turso](https://turso.tech)/libSQL database — set `MQLITE_DB=libsql://<db>.turso.io`
(+ `MQLITE_DB_AUTH_TOKEN`). The remote path is **CGO-free**: it speaks Hrana over
HTTP via `libsql-client-go`, so the single static binary still has no native deps.
This note records how concurrency is handled and why embedded replicas are *not*
used.

## Concurrency model

MQLite is a **single logical writer** in both modes — one broker process owns the
queue. What differs is the connection pool:

```
┌─────────────────┬──────────────────────────┬────────────────────────────────────┐
│                 │ local file / :memory:    │ remote Turso / libSQL              │
├─────────────────┼──────────────────────────┼────────────────────────────────────┤
│ writer          │ 1 conn = the writer      │ broker is the writer; the Turso    │
│                 │ (file lock, MQLITE-6)    │ primary serializes commits         │
│ pool            │ MaxOpen=1, MaxIdle=1     │ MaxOpen=4, MaxIdle=2               │
│ idle handling   │ ConnMaxLifetime=0        │ IdleTime=3s, Lifetime=55s          │
│ why             │ atomic claims, no        │ recycle conns before Turso closes  │
│                 │ contention               │ idle Hrana streams                 │
└─────────────────┴──────────────────────────┴────────────────────────────────────┘
```

A small remote pool is safe because the Turso primary serializes writes; it is
*more* resilient than a single conn because a server-closed idle stream is dropped
and replaced instead of reused.

## Connection resilience (the remote hardening)

Turso closes idle Hrana streams; `libsql-client-go` then returns a **wrapped**
`driver.ErrBadConn` (`"stream is closed: driver: bad connection"`). Because it is
wrapped, `database/sql` will not transparently retry it, so MQLite does:

- **Bounded retry on connection errors only** — `maxConnAttempts = 3`. A closed
  stream means the statement never reached the server, so retrying on a fresh
  pooled connection cannot double-execute. `exec`, `query`, `queryRowScan` and the
  whole-transaction `inTx` all retry under this rule.
- **Never retry a logical error.** `retryable = remote && isConnErr`. A constraint
  violation or `no such table` is final — retrying could double-apply a committed
  write. Local stores never retry at all (a single conn is the single writer).
- **`busy_timeout(5000)`** absorbs transient `SQLITE_BUSY` at the driver level
  rather than surfacing it as an error.

The classifier that decides all this is unit-tested hermetically — see
`TestIsConnErr` / `TestRetryableAndAttempts` in `engine/db_retry_test.go` — so the
retry contract can't silently drift without creds.

## Embedded replicas — evaluated, not adopted

Turso embedded replicas keep a **local libSQL file that syncs from the remote
primary**: reads hit the local copy, writes go to the primary and sync back. They
are a big win for read-heavy, geo-distributed *read* workloads. They are the wrong
fit for MQLite, for two independent reasons:

1. **Async sync breaks queue consistency.** A queue's reads (`claim`, `peek`) run
   against the *same* rows it just wrote and need read-after-write consistency: a
   claim must see its own state change atomically. A replica that lags the primary
   by even a sync interval could re-hand-out an already-claimed message or hide a
   just-enqueued one — exactly the correctness the Peek-Lock model exists to
   prevent. The broker is the single writer, so it gains nothing from a read
   replica of its own writes.
2. **They require CGO.** Embedded replicas live in the native `go-libsql` binding,
   not the pure-Go `libsql-client-go` Hrana client MQLite uses. Adopting them would
   reintroduce CGO and cross-compilation pain — abandoning the CGO-free,
   single-static-binary goal that defines this project.

**For HA / geo-distribution:** co-locate the broker next to its Turso primary
(low-latency Hrana), or run independent broker + DB pairs per region. Revisit this
decision only if `libsql-client-go` ever ships pure-Go embedded replicas *and* a
synchronous read-your-writes mode.

## Validation status

```
hermetic (always, in CI)   engine/db_retry_test.go   conn-error retry classifier
live  (creds-gated, nightly) engine/turso_test.go     full lifecycle vs real Turso
```

Run the live suite against your own DB:

```sh
MQLITE_TEST_DB=libsql://<db>.turso.io MQLITE_TEST_DB_AUTH_TOKEN=<jwt> \
  go test ./engine -run TestTurso -v
```

Without `MQLITE_TEST_DB` the live tests skip; the hermetic classifier test always
runs.
