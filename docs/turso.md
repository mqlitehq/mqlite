# Turso / libSQL: concurrency & embedded replicas (MQLITE-4)

This guide follows `main`, including unreleased driver hardening. For an installed
v0.4.2 release, use the [Turso guide at that tag](https://github.com/mqlitehq/mqlite/blob/v0.4.2/docs/turso.md);
see [CHANGELOG.md](../CHANGELOG.md#unreleased) for pending behavior changes.

MQLite runs the same engine on a local SQLite file or a remote
[Turso](https://turso.tech)/libSQL database — set `MQLITE_DB=libsql://<db>.turso.io`
(+ `MQLITE_DB_AUTH_TOKEN`). The remote path is **CGO-free**: it speaks Hrana over
HTTP via `libsql-client-go`, so the single static binary still has no native deps.
This note records how concurrency is handled and why embedded replicas are *not*
used.

## Rust-based Turso technical preview

Unreleased `main` also accepts `turso://<database>.turso.io` for a Rust-based
Turso database. Supply its token through `MQLITE_DB_AUTH_TOKEN` or
`WithDBAuthToken`, just as for a `libsql://` database. MQLite uses the service's
[HTTP v2 compatibility endpoint](https://github.com/tursodatabase/turso/blob/65a31536512e3ae906277796f520785b7d68ba93/serverless/PROTOCOL.md#2-overview)
with its existing pure-Go driver and connection-validity adapter; it normalizes
the scheme inside the driver connection string. No native library or higher Go
source floor is required.

The Rust service is a technical preview. Validate its queue operations, repeated
opens, recovery and query performance with your workload before adopting it.
MQLite still uses ordinary `BEGIN` transactions and requires one broker / `Engine`
per database. A provider's concurrent-write capability does not enable
`BEGIN CONCURRENT` or make multiple MQLite owners safe. The new provider's SQL
compatibility and optimizer are independent of libSQL's; do not assume identical
throughput or query plans.

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

| | local file / `:memory:` | `libsql://` / `turso://` (HTTP Hrana) | `ws://` / `wss://` |
|---|---|---|---|
| write ownership | 1 connection = local writer (file lock, MQLITE-6) | one owner admits one write attempt at a time | same remote ownership |
| provider pool | MaxOpen=1, MaxIdle=1 | MaxOpen=4, MaxIdle=0 | MaxOpen=4, MaxIdle=2 |
| observation | file: 1 read-only connection; `:memory:`: writer connection | provider-backed pool | provider-backed pool |
| lifecycle | ConnMaxLifetime=0 | one logical Hrana connection per `database/sql` lease | IdleTime=3s, Lifetime=55s |
| physical reuse | SQLite connection stays open | `http.DefaultTransport` still reuses TCP/TLS connections | the WebSocket itself is pooled |

Remote mutations wait for one cancellable write permit before acquiring a SQL
connection. The permit covers one attempt through commit or rollback; retry
backoff releases it. Renewals precede settlements in separate FIFO queues, with
bounded bursts at each level so ordinary writes and maintenance also progress. Reads retain access to
the four-connection pool. This cannot interrupt an already running transaction
and does not make overlapping brokers or `Engine` instances safe.

Compatible HTTP Hrana `RenewBatch` calls already waiting at the tail of the
renewal FIFO can share one fenced `UPDATE ... RETURNING`. There is no collection
delay: an idle writer uses the original synchronous path. Groups only combine
the same queue and lease configuration, with at most 512 input positions and
bounded payload eligibility. At most two unfinished groups own shared execution;
incompatible or excess calls use the original FIFO path. Each caller retains
its own cancellation and token results, and a canceled caller cannot cancel a
live peer. Ending a caller's wait does not mean that an already issued shared
statement was undone. Shutdown still waits for the real shared SQL and connection to end.
Safe retries rejoin the FIFO tail under the same retry and fairness rules.

This internal adaptation keeps the same API and message contract across local
SQLite and remote backends. It reduces redundant renewal round trips; it cannot
preempt an active transaction or guarantee that an arbitrary network delay fits
inside a short lease. Latency and throughput still depend on the deployment.

A public `Tx` callback holds the write permit until that attempt finishes. Keep
callbacks short and use the supplied `EngineTx` for business SQL and enqueueing.
Complete the callback before waiting for another mutation through the same owner.

For HTTP Hrana, a direct queue batch transports bounded groups of parameterized
statements together while retaining one transaction and one commit for the whole
Send. Fan-out and expiring subscriptions retain their existing fenced paths.
Network latency still contributes to transaction and lease budgets.

On `libsql://`, a lease is one ordinary operation or one complete transaction. Returning the lease discards its logical
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
- **Reads retry on transport drops and definite remote aborts.**
  `retryable = remote && (isConnErr || isBusyErr || isExpiredRemoteTransactionErr)`
  gates `query`/`queryRowScan`: a read is idempotent, so replaying it on a fresh pooled
  connection is always safe. `isConnErr` spans the whole transport-failure family —
  `driver.ErrBadConn`, a closed Hrana stream, and their siblings `EOF` / `broken pipe` /
  `i/o timeout` (behind a proxy like Fly a dropped connection reads as `EOF`, not `RST`).
  A single provider HTTP error leaf with status 408, 429, 499 or 5xx also follows
  this policy. Before COMMIT is attempted, `inTx` discards the failed transaction
  and may reopen the entire callback under the same six-attempt bound. A numeric
  provider status takes precedence over SQLite words in its response body.
- **Writes and COMMIT retry only after a proven rejection.** Only a genuine
  before-dispatch `driver.ErrBadConn` or typed `SQLITE_BUSY` evidence permits
  transparent replay. Plain text such as `database is locked`, the Rust preview's
  exact `Tursodb error: Database is busy`, or an idle-transaction rollback message
  does not prove that an issued write or COMMIT was rejected. The pinned driver
  can discard a non-200 HTTP status and return exactly the same text as a normal
  SQL rejection. Those issued-write outcomes are `ErrOutcomeUnknown`, preserving
  the cause, instead of automatically repeating a possibly committed effect.
  Before COMMIT starts, a callback error can still discard and retry the entire
  transaction within the six-attempt budget. Local SQLite remains unchanged.
  Supported callbacks use `EngineTx.SQL()` only for business statements; they
  must not call its `Commit`/`Rollback` methods or issue transaction-control SQL.
  Engine owns those boundaries. Early caller commits bypass this stage contract.
- **Idle-transaction expiry is stage-sensitive.** The exact provider explanation
  that an interactive transaction was rolled back because its stream was idle
  identifies the known expiry condition for reads and pre-COMMIT callback retries.
  These paths reopen the whole transaction, rebuilding time-sensitive arguments.
  Exhaustion keeps the existing `connection` counter category and optional
  `transaction_expired` detail. The same untyped text received after an issued
  standalone write or COMMIT stays outcome-unknown; the driver no longer exposes
  enough protocol information to prove its origin. Generic timeout/rollback text,
  joined causes and checkpoint failures do not authorize extra retries.
- **A lost write acknowledgement is _outcome-unknown_, not retried** (MQLITE-59). Any
  other transport drop on a write/commit — the primary may have durably committed before
  the ack was lost — surfaces as `ErrOutcomeUnknown` (HTTP `503 outcome_unknown`) instead
  of being blindly replayed into a double-insert. The caller reconciles by
  `message_id`/dedup before retrying; transparent recovery would need durable per-op
  idempotency, which is deferred. **`errors.Is(err, mqlite.ErrOutcomeUnknown)` works in
  both embedded and client mode.**
  Transient provider HTTP responses on an issued standalone write or COMMIT are
  also outcome-unknown: a proxy 502 does not prove that the primary rejected the
  effect. They are never automatically replayed; the error preserves its provider
  cause and `HTTPStatus()` for reconciliation and diagnostics. HTTP authentication
  and other nontransient responses are returned without blind retries. Provider
  HTTP failures keep the existing `connection` storage-counter category; optional
  details carry the safe status and `provider_unavailable` description, with
  `outcome_unknown` remaining authoritative for ambiguous writes.
  When fallback decoding discards the HTTP status, diagnostics do not invent it.
  Coded fallback errors cannot promote a remote write into a safe `ErrBadConn`
  retry. Supported remote mutations hold an explicit connection or transaction;
  MQLite does not issue standalone writes through `sql.DB.ExecContext`.
- **Never retry a logical storage error.** A constraint violation or `no such table`
  is final inside the engine. At the HTTP client boundary, lifecycle deletion is
  more conservative: every HTTP 5xx is `ErrOutcomeUnknown`, because a gateway can
  synthesize that response after the broker committed and its acknowledgement was
  lost. Local stores never retry at all (a single conn is the single writer).

The HTTP compatibility adapter keeps COMMIT on the original transaction context.
Use a deadline or cancellation on that context: interrupting an already-started
COMMIT still means `ErrOutcomeUnknown`, not proof that the write was rolled back.
The adapter waits for the context-aware HTTP call to return rather than leaving
the commit running in a detached goroutine. ROLLBACK has a separate five-second
cleanup deadline, so canceled work can still attempt cleanup and returning from
an operation may include that cleanup interval (MQLITE-163).

This is not a hard deadline for every driver resource. Detached stream-close
requests retain their separate per-handle concurrency limit and five-second
deadline (MQLITE-164), described below. Cleanup cancellation does not confirm
remote release, and `Engine.Close` does not synchronously join that detached
work. A successful engine close is not proof that all provider streams are gone.

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

## Bounded HTTP stream cleanup

MQLite carries the pinned driver's HTTP v2 compatibility code internally, with a
narrow stream-cleanup patch (MQLITE-164). Its source revision, license and local
changes are recorded in `internal/libsqlcompat/NOTICE` and `UPSTREAM.json`. The
original pure-Go parser, HTTP client and TCP/TLS transport reuse are retained;
normal SQL, transaction acknowledgement and retry decisions are unchanged.

Stream cleanup is asynchronous and best effort. Each opened database handle has a fixed cap of
16 active cleanup requests, each with a five-second deadline created before the
cleanup goroutine starts. There is no cleanup queue. A saturated cap skips that
cleanup request rather than holding a SQL lease or write permit while waiting
for a cleanup slot. Completed cleanup workers report observed saturation with
one fixed warning at most per handle; logging is also best effort and never
runs on the foreground return path. Normal operations do
not wait for an extra cleanup round trip.

The deadline bounds the client's request, not the provider's resource lifetime.
Unconfirmed streams may remain allocated until the provider reclaims them under
its own expiration policy; no fixed provider timeout is assumed. Neither the
client deadline nor `Engine.Close` confirms remote resource release or transaction
rollback. An acknowledged commit stays successful, and a lost commit
acknowledgement remains outcome-unknown. This does not add synchronous cleanup
draining to `Close` or change the WebSocket and local SQLite paths.

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
