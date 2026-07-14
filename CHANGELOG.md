# Changelog

Hand-written summary of user-visible changes — **behavior changes first**, because
those are what an upgrade can feel. Commit-level notes are auto-generated on each
[GitHub Release](https://github.com/mqlitehq/mqlite/releases); this file only
records what changes semantics, adds capability, or fixes something you could hit.

mqlite is pre-1.0: any release may change behavior, and a schema change makes old
DB files unreadable by design (`ErrSchemaVersionMismatch` — recreate, don't migrate).

## Unreleased

### Behavior changes

- **Schema v3: `seq_number` is now allocated with `AUTOINCREMENT` and never reused**
  (MQLITE-71). Previously SQLite could recycle the highest `id` after its row was deleted,
  so a stale `Cancel` (which is fenced only by `seq_number`) could delete a *later* message
  that reused that id — real message loss. Sequence numbers are now strictly increasing and
  never reused (they may gap); a settled/cancelled seq is gone for good. **Schema-breaking:**
  pre-1.0, an existing DB is refused with `ErrSchemaVersionMismatch` and must be recreated —
  **before upgrading, stop all writers and back up** (`VACUUM INTO` or a stopped-broker copy),
  then start on a fresh DB. The broker never deletes your data automatically.
- **Scheduled multi-message `Send` is now atomic** (MQLITE-72): a mid-batch failure (e.g. a
  `group_fifo` item missing its `group_id`) rolls back the whole batch instead of leaving
  earlier items scheduled. A single scheduled message still distinguishes a dedup conflict
  (`409`) from a no-subscriber no-op (`seq 0`).
- **Updating a subscription filter no longer resets its backing queue's config**
  (MQLITE-73): re-subscribing changes only the mapping/filter; lock duration, delivery
  count, TTL, dedup, ordering and DLQ settings previously configured on the backing queue
  now survive (they were silently reset to defaults on every filter update).
- **Malformed `MQLITE_TOKENS` now fails startup instead of silently disabling auth**
  (MQLITE-69): a value that is non-blank but parses to no token (e.g. `","`, `" , "`) used
  to log "auth enabled" while running fully open. Only the exact `off` disables auth.
- **With auth disabled, CORS defaults off and a non-loopback bind is refused** (MQLITE-70):
  `MQLITE_TOKENS=off` on an all-interfaces address now errors — bind `127.0.0.1`, or pass the
  new `--insecure-allow-remote`. An explicit `MQLITE_CORS` still opts a wildcard/origin in.
- **The dev `docker-compose.yml` is secure by default** (MQLITE-74): it binds loopback only
  (`127.0.0.1`), ships no baked-in token (the broker generates one and logs it), and defaults
  CORS off.
- **`mqlite vacuum` (incremental) now actually reclaims disk** (MQLITE-78): it runs the full
  checkpoint → `incremental_vacuum` (to completion) → checkpoint sequence, so freed pages are
  returned to the OS and the file shrinks. Previously it freed a single page and never shrank
  the file while still reporting success.
- **A stdin message body over 16 MiB now errors instead of being silently truncated**
  (MQLITE-79): `mqlite send ... -` reads one byte past the cap and fails loud; use `--file`
  (uncapped) for larger payloads. The broker's own `413 message_too_large` remains the ceiling.
- **The MCP server gained `renew`, `defer`, and `receive_deferred` tools** (MQLITE-82): an
  agent can now hold a lock across long work and use the deferred-message lifecycle over MCP,
  not only the fallback raw-HTTP path (14 tools total).
- **Peek and Receive responses are bounded by a total body-byte budget** (MQLITE-80): a legal
  1000×1 MiB Peek no longer materializes ~1.3 GiB and OOMs a small VM. Both stop after the
  message that crosses a 32 MiB budget; Peek pages past it with `from_seq`, Receive locks only
  the messages it returns, and a single over-budget message is still delivered.
- **Default broker port is now `6754`, not `8080`** (MQLITE-84). `mqlite serve` with no
  `--addr` listens on `:6754`; the direct local endpoint, the admin console (`/ui`), and
  `mqlite-mcp`'s default `MQLITE_ENDPOINT` all move to `http://127.0.0.1:6754`. Container
  images `>= 0.3.0` `EXPOSE 6754` and default to it. There is no fallback to the old port
  and no port probing. Upgrading: if you relied on the previous default, set it explicitly
  — `mqlite serve --addr :8080` (or `MQLITE_ADDR=:8080`), and map the container with
  `-p 8080:6754` — and update SDK/CLI/MCP endpoints, health probes, and firewall rules
  copied from the old value.
- **New `MQLITE_ADDR`** sets the broker listen address; precedence is `--addr` >
  `MQLITE_ADDR` > `:6754`. A blank/whitespace value is rejected (it would otherwise bind
  port 80).
- **Custom DSN schemes supply the product port** when it is omitted: `mqlite://host` →
  `http://host:6754`, `mqlites://host` → `https://host:6754` (previously they fell through
  to `:80`/`:443`). Plain `http://`/`https://` keep standard 80/443; an explicit port
  always wins.
- **A remote write that loses its commit acknowledgement is no longer blindly retried**
  (MQLITE-59). On a Turso/libSQL store, only errors that *guarantee the statement never
  applied* (`driver.ErrBadConn`, `SQLITE_BUSY`) are replayed; any other transport drop on a
  write/commit — where the primary may have committed before the ack was lost, which behind
  a proxy typically surfaces as `EOF`/`broken pipe`/`i/o timeout`, not just `connection
  reset` — now returns the new `mqlite.ErrOutcomeUnknown` (broker: `503 outcome_unknown`)
  instead of silently double-inserting. `errors.Is(err, mqlite.ErrOutcomeUnknown)` works in
  both embedded and client mode; reconcile by `message_id`/dedup before retrying. Local
  file/`:memory:` stores never retried and are unaffected.
- **Request bodies are now strictly validated** (MQLITE-86): the JSON decoder rejects an
  **unknown field** and any data after the first object (a typo like `messsages` was
  previously dropped, turning a botched `Send` into a silent 200 no-op), and an empty
  queue name or an unknown `kind`/`ordering_mode` enum now returns `400 invalid_argument`
  (`mqlite.ErrInvalidArgument`) instead of leaking an opaque `500` from a SQLite CHECK.
  Agent-facing APIs fail loud and predictably.
- **The `/` discovery card now matches its documented, agent-facing shape** (MQLITE-87):
  `auth` is a string — `"bearer"` when RPCs need a token, `"none"` when auth is off (was a
  bool), and `endpoints` is the **complete array of every RPC route path** the broker
  serves (was a small labelled route-family map), with `health`/`metrics`/`ui` as separate
  well-known fields. The catalog is generated from the actual route registration and pinned
  by a golden test, so it can never silently drift from what's served.
- **Server/CLI hardening bundle** (MQLITE-88):
  - An unrecognized **`MQLITE_SYNC`** (e.g. `FULLL`) now **fails startup** with a clear error
    instead of silently running on `NORMAL` — a durability typo can't quietly weaken the
    guarantee. Accepted values: `NORMAL`/`FULL`/`OFF`/`EXTRA`. Unparseable size/DLQ limits
    (`MQLITE_MAX_MESSAGE_BYTES`, `MQLITE_DLQ_MAX_*`) now log a warning instead of vanishing.
  - The broker HTTP server gained a **60s `ReadTimeout`** (bounds a slow-drip request body;
    it doesn't touch the Receive long-poll — only `WriteTimeout`, still 0, would), its
    **shutdown grace went 5s → 25s** so an in-flight 20s long-poll drains on Ctrl-C instead
    of being cut, a **failed shutdown is surfaced** (not swallowed), and **"ready" is logged
    only after the listener actually binds** — a bind failure now surfaces as an error rather
    than a misleading "ready" line.
  - `mqlite receive` now **exits non-zero when a message fails to settle** (it previously
    printed a warning and exited 0, so automation saw success while messages redelivered).
  - **Broker DLQ retention no longer applies to one-shot CLI commands** — `send`/`receive`/etc.
    no longer start the retention janitor; only `serve` applies it (docs already documented it
    as serve-only).
- **Scheduling a past/now time is now rejected** (0.3.0): `Schedule`/`ScheduleBatch` (and the
  broker's Schedule route) return `invalid_argument` when the time is not in the future —
  `schedule` is future delivery; use `Send` for immediate. Enforced against the broker clock,
  so a CLI on a skewed host can't disagree.
- **`mqlite metrics` now prints a human line by default** (0.3.0), not JSON — it honored no
  `--output` flag before. Scripts that parsed the old JSON default must add `--output json`.
- **The `mqlite` CLI is now a first-party client for the common broker operations** (MQLITE-92,
  covering the everyday surface — not a lossless view of every wire field; use raw HTTP for the
  full contract): new commands
  `complete`/`abandon`/`reject`/`defer`/`renew` (settle a `receive --no-ack` message by
  `<queue> <seq> <lock-token>`), `schedule`/`cancel`, `receive-deferred`, `status`,
  `list-subscriptions`, and `test-filter`; global `--endpoint`/`--token` flags (override the
  env) and `--output text|json` for scripting (bodies base64, keys snake_case); and
  `receive --no-ack` now prints each `lock-token`. New SDK surface backs it —
  `Message.LockToken()`, `Client/Embedded.Message(queue, seq, token)`, `Status()`,
  `ListSubscriptions()`, `TestFilter()`. (Settling across separate invocations needs a running
  broker; embedded mode reclaims orphaned locks each open.)
- **CLI safety hardening** (MQLITE-93, follow-up review): `receive` now renders output
  **before** settling and settles the whole batch with a single `CompleteBatch` — a closed
  stdout / broken pipe exits non-zero and leaves the messages locked for redelivery instead
  of silently acknowledging a message the caller never received. Negative `purge-dlq`/`redrive`
  limits are rejected (they previously meant "no limit") at the engine boundary, so raw HTTP
  is covered too, and an unbounded `purge-dlq` now requires an explicit `--all`. The `--`
  terminator is honored (a message body keeps its flag-looking words). `--token=` sends no
  token, and `--endpoint` to a different host no longer forwards an ambient `MQLITE_TOKEN`.
- **Two data-loss holes closed** (MQLITE-96, round-2 review):
  - **`receive` refuses to auto-ack when stdout cannot deliver.** Closing fd 1 at exec
    (`mqlite receive q 1>&-`) does not fail a write — the Go runtime silently reopens the
    descriptor to the null device before `main`, so every "successful" write went to a black
    hole and the messages were Completed and deleted. `receive` now detects an
    undeliverable stdout (closed at exec, or `/dev/null`) and exits non-zero **before**
    claiming anything. `--delete` (explicit at-most-once drain) and `--no-ack` still work.
  - **A sub-millisecond age bound no longer means "no bound".** `purge-dlq --older-than 1ns`
    truncated to `0` ms, which the engine reads as *unbounded* — it deleted the entire DLQ
    while appearing bounded. Any positive duration now rounds **up** to 1 ms in the SDK
    (`Purge`/`Redrive`, so raw HTTP and embedded callers are covered), and the CLI rejects a
    positive sub-ms `--older-than` outright.
- **`--output json` now emits the wire shape exactly** (MQLITE-96). The CLI had a parallel,
  hand-copied schema that renamed `seq_number` to `seq` and dropped `visible_at_ms` /
  `locked_until_ms` (peek) and `uptime_ms` / `auth` (status), and called `db_size_bytes`
  `size_bytes`. A message in `--output json` is now literally the HTTP API's message object, so
  the CLI and a raw POST agree key for key and a field added to the wire can't go missing from
  the CLI. **Breaking for scripts** that read the old CLI-only keys: `seq` → `seq_number`,
  `size_bytes` → `db_size_bytes`.
- **A negative `scheduled_enqueue_time_ms` is rejected over raw HTTP** (MQLITE-96): it used to
  fall past the `> 0` check and enqueue an *active* message with a 200, so raw HTTP disagreed
  with embedded and the CLI, where the same value is refused.
- **CLI strictness** (MQLITE-96): `create-queue`/`subscribe`/`peek`/`metrics`/`list`/`vacuum`
  now require exact arity — a surplus positional is a typo (a misplaced flag, a value the shell
  split), and silently ignoring it is how you purge the wrong queue. `purge-dlq --all` can no
  longer be combined with `--max`/`--older-than` (they are alternatives, not a refinement).
- **Peek-Lock leases are renewed through settlement, not just through output** (MQLITE-96): on
  a high-latency link the `CompleteBatch` itself could outlast the lock, so a receive whose
  output the caller had already seen would be reclaimed mid-settle and redelivered.
- **`vacuum` no longer reports negative "freed" space** (MQLITE-96): a fresh DB materializes its
  schema pages as it is opened and vacuumed, so it can end up *larger*; growth is now reported
  as growth and reclaimed bytes never go below zero.
- **The endpoint token boundary is the endpoint the client actually dials** (MQLITE-96):
  re-passing your own endpoint with a trailing slash no longer withholds `MQLITE_TOKEN` and
  hands you a 401. Two endpoints count as the same broker exactly when the base URL the client
  would dial is identical — deliberately *not* a canonicalized URL comparison, because deciding
  which components (path, percent-escaping, query, fragment, IPv6 zone, host case) are
  "insignificant" is how an ambient credential ends up at somebody else's backend. Anything
  else is treated as a different broker: you get a warning and pass `--token`.
  New SDK helper: `mqlite.EndpointIdentity`.

- **Peek-Lock leases are renewed for the whole batch in one request** (MQLITE-97). Renewal was
  per-message, so N messages cost N round trips — and on a slow link a renewal pass took *longer
  than the lease it was renewing*: a 64-message batch at 50ms per call needs 3.2s against a 2s
  lock, and most of the locks expired mid-pass and redelivered. New `RenewBatch` operation
  (HTTP `/mqlite.v1.QueueService/RenewBatch`; `Client.RenewBatch` / `Embedded.RenewBatch`;
  `Engine.RenewBatch`) renews a whole batch in one request and ONE statement, so a renewal pass is
  one round trip no matter how big the batch. It renews at most `mqlite.MaxRenewBatch` (512)
  messages per call — deliberately one statement's worth, because that is what lets it promise
  that every `Ok` it returns means a *live* lease: across several statements the first batch's
  lease can expire, and be reaped, while a later one is still running. Receive hands out at most
  256 messages, so a consumer never meets the cap by accident. **Batch settlement and renewal are also
  set-based inside the engine** — a fixed number of SQL statements rather than one (or two) per
  message. On a remote Turso/libSQL store every statement is its own round trip, so the old
  item-by-item loops made a 256-message settle ~512 remote round trips: the same O(N) latency,
  one layer down, on the very RPC whose slowness lets the batch's own locks expire. The CLI stays
  compatible with an **older broker**: one that predates `RenewBatch` answers 404, and
  the CLI falls back to per-message `Renew` (what it used to do) rather than silently renewing
  nothing. New `mqlite.ErrUnsupported` reports "this broker does not serve that operation",
  distinct from `ErrNotFound` ("that queue/message does not exist"), which the same broker also
  answers 404 for. The renewal interval is also a fraction of the lease now (a third) rather than
  a fixed one-second floor — a queue whose lock
  duration was one second or less previously had its first renewal scheduled at or *after* its
  own expiry, so its lease could not be held at all.
- **A DLQ under its retention cap no longer logs a false ERROR every minute** (MQLITE-97): the
  cutoff query returns `sql.ErrNoRows` when there is nothing to purge — the normal steady state
  — and that was being logged as a failure. With the default cap of a million dead letters, that
  is very nearly every broker. Affected both the count and the byte bound.
- **CLI strictness, round 2** (MQLITE-97): sequence numbers must be positive (`--seq 0,-1` was
  accepted and quietly matched nothing); `version`, `help` and `serve` reject surplus arguments;
  `purge-dlq --all --max 0` is now the same contradiction as `--all --max 10` (the check reads
  whether the flag was *given*, not its value); and giving a body both as an argument and with
  `--file` is an error instead of silently letting the file win.

- **A settlement receipt now vouches only for the VERB THAT WROTE IT** (MQLITE-99). Receipts make a
  settle whose response was lost replayable — the same verb, same token, same success. They were
  not checking the verb, so `Abandon(T)` (which returns the message to `active`) left a receipt
  that a later `Complete(T)` read as "already completed": **the caller was told the message was
  gone while it sat in the queue, waiting to be handed to somebody else.** `CompleteBatch` reported
  the same false `ok`. Every cross-verb combination now fails with `ErrLockLost` / `ok=false` and
  leaves the message where it was; a same-verb replay is still an idempotent success. **This defect
  predates the batch work and ships in the released v0.2.0.**
- **`Engine.Tx` / `Embedded.Tx`: the callback may run more than once on a REMOTE store** (MQLITE-99,
  documentation). A transaction that fails on a retryable connection/busy error is replayed from
  the start. The SQL rolls back, so your data stays correct — but anything the callback does
  *outside* the transaction (an HTTP call, a charge, a counter) will have happened twice. Keep the
  callback transaction-bound. Local file and `:memory:` stores never retry, so it runs exactly once
  there.

- **Cancelling a request can no longer wedge or erase a local database** (MQLITE-98/100).
  Interrupting an in-flight statement on a local SQLite store LEAKS the connection: the pool then
  reports zero open connections while the file stays locked, and every later statement fails with
  `SQLITE_BUSY` — permanently. On `:memory:` the same event destroys the database outright, since
  it lives inside that connection ("no such table: messages"). One root cause, two faces; ~40% of
  runs in a 200-cancellation storm. **Reachable from ordinary code**: a timed-out HTTP request is
  enough, and the CLI's renewer cancels an in-flight renewal *by design* on every receive.
  A statement that is already EXECUTING on a local store is therefore no longer interrupted. The
  contract stays narrow, and cancellation keeps its meaning: an already-cancelled caller never
  starts a statement, and a caller waiting for the single writer keeps its own deadline and mutates
  nothing. Only a statement already running is allowed to finish — microseconds, on a store that
  does no network I/O. **Remote (Turso) stores are unchanged**: their statements can genuinely
  block, and a discarded connection there holds no local lock. New `EngineTx.Context()` is the
  context your own statements inside `Engine.Tx` should use.
- **Settlement receipts identify the REQUEST, not just the token** (MQLITE-100). **Schema v4** —
  pre-1.0, an existing DB is refused (`ErrSchemaVersionMismatch`) and must be recreated. A receipt
  now records `queue + seq_number + lock_token + operation`, and every part is load-bearing:
  matching the token alone meant `Complete(seqB, tokenA)` found A's completion receipt and reported
  **success for a message it never touched** — in the same queue, through `CompleteBatch`, and even
  across queues. The message stayed locked in the queue while the caller was told it was gone.
  A same-request replay is still an idempotent success.

## v0.2.0 — 2026-07-11

### Behavior changes

- **FIFO now holds across lock expiry** (MQLITE-56, #107). On ordered paths
  (`group_fifo`, `strict_fifo`, grouped messages on `standard`) an
  expired-but-not-yet-reaped lock keeps blocking its group; once the reaper runs,
  the head is redelivered first, in id order (or dead-lettered at
  `count ≥ max_delivery_count`). Previously successors could overtake the expired
  head in the ≤1s reaper window. Cost: a consumer timeout stalls its group for up
  to ~1s. Slow-but-alive consumers should `Renew`.
- **Queue, subscription and topic names are one disjoint namespace**
  (MQLITE-57, #108). Creations that previously succeeded and silently rerouted
  sends now fail with `409 name_conflict`: a topic naming a live queue or
  subscription, a queue named after a live topic, cross-kind `CreateQueue`
  upserts, and self-referential `Subscribe(topic == name)`. Same-kind reconfig
  upserts and `(topic, name)` re-subscribes still succeed.
- **Publishing to a topic where no subscription filter matches is a valid no-op**
  (seq `0`), no longer `409` (#82).
- **A per-message TTL is capped at the queue's `default_ttl_ms`** (ASB
  semantics), and `Peek` surfaces `expires_at` (#80).
- **Crash recovery applies `max_delivery_count`** (MQLITE-58): an orphaned lock
  already on its last allowed attempt dead-letters on startup (same rule as the
  reaper) instead of being redelivered past the bound.
- **A message expiring while still `scheduled` dead-letters like every other
  state** (MQLITE-61) — previously its TTL was honored only after activation.
- **Requests over the new 32 MiB body cap return `413 message_too_large`**, and
  `/uixyz`-style paths no longer ride the `/ui` auth exemption (MQLITE-64).

### Added

- **Embedded admin console** at `/ui` (`MQLITE_UI=off` to run headless) (#84,
  refreshed by MQLITE-55).
- `AdminService/Status` (desensitized runtime snapshot), `ListSubscriptions`,
  `TestFilter` (filter dry-run), and a `MQLITE_CORS` knob (#71–#74).
- **Per-RPC latency histograms** on `/metrics` (#96) and a lifetime
  completed-messages counter (MQLITE-54).
- Colorized broker log with a per-request access log (ms precision) (#76–#87).

### Performance

- **Receive claims its whole batch in one transaction** — on a 1-core box, drain
  went 591 → 1,507 msg/s and receive p50 2.1 s → 374 ms; the concurrency-128
  collapse and its DLQ poisoning are gone (MQLITE-50, #88).
- **Automatic disk reclamation**: `incremental_vacuum` driven to completion plus
  WAL checkpointing — an emptied queue's file actually shrinks (42.6 MB → ~0.4 MB
  in the repro; ~0.16 MB empty-queue footprint live) (MQLITE-53, #89–#91).

### Fixed

- The single-writer lock is keyed on the **canonical** DB path — relative,
  dot-segment, DSN-option and symlinked spellings of one file can no longer
  yield two writers (MQLITE-60).
- Unregistered `/mqlite.v1.*` paths no longer create never-evicted `/metrics`
  series (label-cardinality DoS, MQLITE-62), and a `crypto/rand` failure now
  crashes instead of minting all-zero lock tokens (MQLITE-63).
- Server hardening (MQLITE-64): Slowloris/idle timeouts on the broker,
  constant-time Bearer comparison, a schema-content golden test pinned to the
  schema version token, and `CompleteBatch` retry-replay hygiene.
- Release image ships `tzdata` (#70). Release binaries and the MCP server now
  report one shared version constant, CI-verified against the tag.

## v0.1.1 — 2026-06-23

Release pipeline (GoReleaser archives + GHCR multi-arch image), manual `vacuum`
command (MQLITE-31), secure-by-default auth (auto-generated `mqk_…` token when
`MQLITE_TOKENS` is unset, #68), expr subscription filters with `body_text` /
`body_json` (MQLITE-17/47), per-queue DLQ retention overrides (MQLITE-29), single
canonical schema (#65/#66).

## v0.1.0 — 2026-06-20

First release: Peek-Lock engine (retries, DLQ + redrive, scheduling, deferral,
dedup, `group_id` ordering, topics + filtered fan-out) over one SQLite file or
remote Turso/libSQL; embedded Go SDK and network broker on the same engine; CLI;
MCP server; conformance TCK; reproducible benchmark suite.
