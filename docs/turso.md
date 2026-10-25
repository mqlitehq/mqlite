# Turso / libSQL: concurrency & embedded replicas (MQLITE-4)

MQLite runs the same engine on a local SQLite file or a remote
[Turso](https://turso.tech)/libSQL database — set `MQLITE_DB=libsql://<db>.turso.io`
(+ `MQLITE_DB_AUTH_TOKEN`). The remote path is **CGO-free**: it speaks Hrana over
HTTP via `libsql-client-go`, so the single static binary still has no native deps.
This note records how concurrency is handled and why embedded replicas are *not*
used.

## Backend parity contract

Storage selection is a deployment detail, not a second API mode. Given the same
committed state, broker time and request, local SQLite and remote Turso/libSQL
MUST produce the same successful result, validation outcome, message/resource
state transition and logical sentinel error. The HTTP status/code mapping and Go
SDK `errors.Is` behavior therefore do not change when an operator changes the
broker's database DSN. In particular, scheduling, delivery counts, settlement,
ordering, deduplication, topic fan-out, deletion and TTL use one engine contract.

This is parity of deterministic queue behavior, not a claim that a local file and
a network service have identical failure or performance envelopes. The documented
differences are operational or failure-boundary concerns:

- latency, throughput and temporary availability caused by the remote round trip;
- remote transport/storage failures, including `ErrOutcomeUnknown` when a write
  may have committed but its acknowledgement was lost;
- retry and cancellation mechanics during a storage fault — notably, a remote
  transactional-outbox callback may run more than once and must keep its effects
  transaction-bound;
- deployment and maintenance controls: the local file lock, SQLite synchronous
  mode, file-size accounting and compaction have no identical remote operation;
  Turso still requires one logical MQLite owner but cannot use that local OS lock;
  and
- explicitly operational observation fields such as backend identity and
  connection-pool data.

Those differences MUST NOT silently change a successful operation's queue
semantics or turn a documented logical error into a backend-specific one. The
hermetic suite locks the shared engine and error mappings on every PR; the
credentials-gated nightly suite compares representative lifecycle, concurrency,
settlement, access-key and TTL contracts with a real Turso database.

## Concurrency model

MQLite has a **single logical owner** in both modes: each database has exactly one
active MQLite broker or embedded `Engine`. Replace that owner stop-then-start after
confirming the old process has exited. Do not use rolling overlap against the same
database. What differs between storage modes is the connection pool:

| | local file / `:memory:` | `libsql://` (HTTP Hrana) | `ws://` / `wss://` |
|---|---|---|---|
| write ownership | 1 connection = local writer (file lock, MQLITE-6) | one broker owns a 4-connection pool; Turso serializes commits | same remote ownership |
| writer pool | MaxOpen=1, MaxIdle=1 | MaxOpen=4, MaxIdle=0 | MaxOpen=4, MaxIdle=2 |
| observation | file: 1 read-only connection; `:memory:`: writer connection | provider-backed pool | provider-backed pool |
| lifecycle | ConnMaxLifetime=0 | one logical Hrana connection per `database/sql` lease | IdleTime=3s, Lifetime=55s |
| physical reuse | SQLite connection stays open | `http.DefaultTransport` still reuses TCP/TLS connections | the WebSocket itself is pooled |

A small remote pool is safe within that one MQLite process because the Turso
primary serializes writes across its connections; this does not make two brokers
or embedded `Engine` instances safe. On `libsql://`, a lease is one ordinary
operation or one complete transaction. Returning the lease discards its logical
Hrana connection before it can enter an idle slot or be handed directly to a
waiting caller; this does **not** tear down the shared HTTP transport or its
TCP/TLS keep-alive connections. Each broker process shares its expensive all-queue
observation query for about 10 seconds on both local and remote backends; separate
brokers do not share that cache. Per-queue `Stats` and each observation's
lightweight storage probe still use the backend shown above.

## Connection resilience (the remote hardening)

An HTTP Hrana response can succeed with a null baton, which means the server has
closed that logical stream. The pinned `libsql-client-go` records the closed state
but does not implement `driver.Validator`. Without the MQLite adapter,
`database/sql` can therefore put a known-closed connection in an idle slot or hand
it directly to a waiting caller; the next operation then fails locally with a
wrapped `driver.ErrBadConn` (`"stream is closed: driver: bad connection"`) before
making an HTTP request. An explicit `*sql.Conn` also cannot exchange its pinned
driver connection underneath the caller.

MQLite prevents that stale handoff for `libsql://`: its compatibility adapter
marks every logical connection invalid when the `database/sql` lease ends. A
transaction remains one uninterrupted lease through commit or rollback. This is
a connection-validity rule, not broader retry, caching or singleflight; physical
HTTP/TLS reuse remains owned by Go's shared transport. Normal failure handling is
still bounded as follows:

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

The null-baton, idle-pool, direct-waiter, authentication and transaction-lease
contracts are tested hermetically by
`TestHranaClosedStreamIsDiscardedBeforeFreshReuse` and
`TestHranaTransactionKeepsOneLease`. The classifier is pinned by
`TestIsConnErr` / `TestRetryableWrite` / `TestRetryableAndAttempts` in
`engine/storage_test.go`, and broker→SDK propagation by `TestFailOutcomeUnknown`
(server) + `TestRemoteOutcomeUnknownPropagates` (root), so the retry contract can't
silently drift without credentials.

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
decision only when a maintained pure-Go driver satisfies synchronous
read-your-writes semantics, the Go 1.21 floor and every supported release target.

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
