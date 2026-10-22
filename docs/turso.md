# Turso / libSQL: concurrency & embedded replicas (MQLITE-4)

MQLite runs the same engine on a local SQLite file or a remote
[Turso](https://turso.tech)/libSQL database — set `MQLITE_DB=libsql://<db>.turso.io`
(+ `MQLITE_DB_AUTH_TOKEN`). The remote path is **CGO-free**: it speaks Hrana over
HTTP via `libsql-client-go`, so the single static binary still has no native deps.
This note records how concurrency is handled and why embedded replicas are *not*
used.

## Concurrency model

MQLite has a **single logical owner** in both modes: each database has exactly one
active MQLite broker or embedded `Engine`. Replace that owner stop-then-start after
confirming the old process has exited. Do not use rolling overlap against the same
database. What differs between storage modes is the connection pool:

```
┌─────────────────┬──────────────────────────┬────────────────────────────────────┐
│                 │ local file / :memory:    │ remote Turso / libSQL              │
├─────────────────┼──────────────────────────┼────────────────────────────────────┤
│ write ownership │ 1 conn = local writer    │ one broker owns a 4-connection     │
│                 │ (file lock, MQLITE-6)    │ pool; Turso serializes commits     │
│ writer pool     │ MaxOpen=1, MaxIdle=1     │ MaxOpen=4, MaxIdle=2               │
│ observation     │ file: 1 read-only conn;  │ existing provider pool             │
│                 │ :memory: writer conn     │                                    │
│ idle handling   │ ConnMaxLifetime=0        │ IdleTime=3s, Lifetime=55s          │
│ why             │ atomic claims, no        │ recycle conns before Turso closes  │
│                 │ contention               │ idle Hrana streams                 │
└─────────────────┴──────────────────────────┴────────────────────────────────────┘
```

A small remote pool is safe within that one MQLite process because the Turso
primary serializes writes across its connections; this does not make two brokers
or embedded `Engine` instances safe. The pool is *more* resilient than a single
connection because a server-closed idle stream is dropped and replaced instead of
reused. Each broker process shares its expensive all-queue observation query for
about 10 seconds on both local and remote backends; separate brokers do not share
that cache. Per-queue `Stats` and each observation's lightweight storage probe
still use the backend shown above.

## Connection resilience (the remote hardening)

Turso closes idle Hrana streams; `libsql-client-go` then returns a **wrapped**
`driver.ErrBadConn` (`"stream is closed: driver: bad connection"`). Because it is
wrapped, `database/sql` will not transparently retry it, so MQLite does:

- **Bounded retry on transient errors only** — `maxConnAttempts = 6`.
- **Reads retry on any transport drop.** `retryable = remote && (isConnErr || isBusyErr)`
  gates `query`/`queryRowScan`: a read is idempotent, so replaying it on a fresh pooled
  connection is always safe. `isConnErr` spans the whole transport-failure family —
  `driver.ErrBadConn`, a closed Hrana stream, and their siblings `EOF` / `broken pipe` /
  `i/o timeout` (behind a proxy like Fly a dropped connection reads as `EOF`, not `RST`).
- **Writes retry only when the statement provably never applied** — `retryableWrite =
  remote && (ErrBadConn || isBusyErr)`. A closed stream (`ErrBadConn`) or a `SQLITE_BUSY`
  (lock never acquired) guarantees the write never ran, so replaying `exec` / the `inTx`
  commit cannot double-apply. The retry is bounded by `maxConnAttempts`; exhaustion is
  returned to the caller. The `busy_timeout(5000)` DSN pragma belongs to local modernc
  SQLite connections and does not apply to Turso/libSQL.
- **A lost write acknowledgement is _outcome-unknown_, not retried** (MQLITE-59). Any
  other transport drop on a write/commit — the primary may have durably committed before
  the ack was lost — surfaces as `ErrOutcomeUnknown` (HTTP `503 outcome_unknown`) instead
  of being blindly replayed into a double-insert. The caller reconciles by
  `message_id`/dedup before retrying; transparent recovery would need durable per-op
  idempotency, which is deferred. **`errors.Is(err, mqlite.ErrOutcomeUnknown)` works in
  both embedded and client mode.**
- **Never retry a logical storage error.** A constraint violation or `no such table`
  is final inside the engine. At the HTTP client boundary, lifecycle deletion is
  more conservative: every HTTP 5xx is `ErrOutcomeUnknown`, because a gateway can
  synthesize that response after the broker committed and its acknowledgement was
  lost. Local stores never retry at all (a single conn is the single writer).

> **Transport caveat.** The outcome-unknown guarantee holds for the documented remote
> transport `libsql://` (Hrana-over-HTTP). The `ws://`/`wss://` transports can wrap a
> response-read failure as `ErrBadConn`, under which a lost commit ack *would* be
> replayed — prefer `libsql://` for at-least-once remote writes.

The classifier that decides all this is unit-tested hermetically — see
`TestIsConnErr` / `TestRetryableWrite` / `TestRetryableAndAttempts` in
`engine/storage_test.go`, and the broker→SDK propagation in `TestFailOutcomeUnknown`
(server) + `TestRemoteOutcomeUnknownPropagates` (root) — so the retry contract can't
silently drift without creds.

Ordinary and AttemptID `Receive` transactions issue their fenced
`UPDATE messages ... RETURNING` before reading queue or receive-attempt metadata.
This avoids upgrading a deferred read snapshot after another pooled connection has
committed a write. The first update also verifies the queue lifecycle, absolute
subscription expiry and claim-relevant cached configuration. If it returns no row,
the same write transaction reads current metadata to distinguish an empty queue,
deletion/expiry, stale configuration and an AttemptID replay. Batch continuation
then uses the smaller indexed claim statement.

This write-first shape addresses claim transactions in one broker or embedded
`Engine`. Remote retries remain bounded, other operations may still surface
`SQLITE_BUSY`, and the change does not make overlapping brokers against one database
safe.

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
   prevent. The broker is the single logical owner, so it gains nothing from a read
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
hermetic (always, in CI)   engine/storage_test.go    conn-error retry classifier
live  (creds-gated, nightly) engine/turso_test.go     full lifecycle vs real Turso
```

Run the live suite only against a dedicated, disposable test database. `TestMain`
drops every MQLite-managed table before the selected tests begin, including when
`-run` selects one test, so the reset requires an explicit confirmation:

```sh
MQLITE_TEST_DB=libsql://<db>.turso.io MQLITE_TEST_DB_AUTH_TOKEN=<jwt> \
  MQLITE_TEST_DB_RESET_CONFIRM=1 \
  go test ./engine -run TestTurso -v
```

Without `MQLITE_TEST_DB` the live tests skip; the hermetic classifier test always
runs.
