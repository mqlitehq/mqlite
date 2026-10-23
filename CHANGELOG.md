# Changelog

Hand-written summary of user-visible changes — **behavior changes first**, because
those are what an upgrade can feel. Commit-level notes are auto-generated on each
[GitHub Release](https://github.com/mqlitehq/mqlite/releases); this file only
records what changes semantics, adds capability, or fixes something you could hit.

mqlite is pre-1.0: any release may change behavior. Incompatible schema changes
refuse old DB files (`ErrSchemaVersionMismatch` — recreate, don't migrate).
Compatible additions can preserve the existing schema token; read each release's
upgrade notes before replacing a broker.

## v0.4.0 — 2026-10-23

> **Upgrade from v0.3.2 or withdrawn v0.3.3:** port **6754** remains
> unchanged, but
> the canonical schema token changes from **5 → 7**. There is no in-place
> migration. Stop writers, take a consistent backup, account for every retained
> message and managed key, preserve the schema-5 binary/database pair, and start
> v0.4.0 with a fresh database. Schema 6 belonged only to intermediate
> unreleased lifecycle builds and is refused as well; it was never a tagged release.
>
> The v0.3.3 release remains withdrawn and unsupported. Its public assets and exact
> image remain available for forensic or compatibility inspection and must not be
> deployed. v0.4.0 carries the retraction directive, so Go module tools avoid
> v0.3.3. The withdrawn release's public queue `Generation` contract was removed;
> v0.4.0 addresses resources by name and requires one owner to serialize deletion
> with same-name recreation. Upgrade the broker, SDK, CLI, MCP and console together
> when adopting the corrected deletion APIs. v0.4.0 retains the bounded maintenance
> diagnostics and private diagnostic log first shipped in v0.3.3 while replacing
> v0.3.3's public queue-lifetime contract; the earlier release remains withdrawn
> as a whole.
>
> **Prometheus migration:** the aliases `mqlite_queue_total` and
> `mqlite_queue_oldest_message_age_ms`, deprecated in v0.3.2, are removed.
> Replace them with `mqlite_queue_retained_messages` and
> `mqlite_queue_oldest_message_age_seconds`; divide millisecond alert thresholds
> by 1,000. Native `Observe` JSON retains `queues[].oldest_message_age_ms`.

### Remove deprecated Prometheus aliases (MQLITE-129)

- Remove the `mqlite_queue_total` and `mqlite_queue_oldest_message_age_ms`
  compatibility families on the v0.4.0 boundary. The canonical
  retained-depth and age-in-seconds families remain exact projections of the
  native observation snapshot, and the bundled dashboard, rules and verifier use
  only those canonical names.
- Require the complete exposition fixture to pass `promtool check metrics`
  without the two former naming warnings. This changes Prometheus exposition
  only; the native JSON `oldest_message_age_ms` field remains available.

### Isolate deep-backlog observation reads (MQLITE-132)

- Make exact per-queue `Stats` count each literal retained state through the
  existing state-specific partial indexes. A sparse queue no longer scans
  unrelated messages; `completed` rows remain included in `total` as unexpected
  retained state, and oldest active/locked age stays exact.
- Give local file databases one read-only (`mode=ro` plus `query_only`)
  observation connection alongside the sole writer connection. `Stats`, the
  `Observe` storage probe and global queue refreshes use this WAL reader, so a
  deep monitoring query no longer occupies the connection used by
  Send/Receive/settlement. The writer invariant and cross-process advisory lock
  are unchanged. In-memory databases cannot share a second connection; remote
  Turso/libSQL continues to use its existing small provider-backed pool. Local
  storage-pool metrics aggregate both connections, so a file database reports
  `max_open=2`; `:memory:` remains `max_open=1`.
- Share the expensive all-queue gauge snapshot across `Observe`, `/metrics` and
  console polling for about 10 seconds. Concurrent refreshes collapse into one
  scan; a failed refresh is not cached. `sampled_at_ms` remains the time of the
  database sample, so callers can see that queue gauges are cached while runtime
  probes and process counters are collected for each request. Per-queue `Stats`
  remains fresh. A refresh still consumes CPU and storage bandwidth and retains
  a WAL snapshot for its duration.

### Reduce global observation aggregate work (MQLITE-134)

- Make the exact all-queue refresh step only the matching state counter for each
  retained message. The query keeps one grouped scan and preserves active,
  locked, deferred, scheduled, dead-lettered, total, unexpected and oldest
  active-or-locked semantics.
- Keep the database schema, observation API, cache and singleflight behavior
  unchanged. Refresh cost remains proportional to the rows currently retained in
  `messages`; completed messages normally leave that table when settled, so this
  is current backlog rather than cumulative broker throughput.
- Add mixed-state equivalence coverage, a six-state deep-backlog benchmark and a
  deterministic query-shape check that runs in the existing CI matrix.

### Make Turso receive claims write-first (MQLITE-136)

- Start ordinary and AttemptID receive transactions with the fenced claim update,
  then classify a zero-row result inside the same write transaction. This avoids a
  Turso/libSQL deferred read snapshot failing when it is upgraded after another
  pooled connection commits.
- Preserve ordering, lifecycle, batching, replay and observability semantics. The
  fix applies within one broker or embedded engine; remote retries remain bounded
  and overlapping owners of the same Turso database remain unsupported.

### Add destructive-operation audit evidence (MQLITE-131)

- Give each logged broker request a broker-generated correlation ID, return it in
  `X-Request-ID`, and record a safe `actor_kind` classification plus a managed-key
  public ID when one exists. Caller-supplied request IDs, credentials, token
  digests, message bodies and generated key secrets never become log fields.
- Record reconciliation evidence for queue and subscription deletion, including
  `force`, whether acceptance is known, and the accepted result. Purge and managed
  key changes likewise include their safe request bounds and confirmed effects.
- Keep this evidence operational rather than presenting it as a durable audit
  ledger. Operators must retain broker stderr externally; an unknown deletion
  outcome still requires read-only inventory reconciliation, and embedded brokers
  must supply a retained request logger when they need the same evidence.

### Enforce one strict JSON request object (MQLITE-130)

- Require every RPC body to contain exactly one non-null JSON object. This now
  includes `ListQueues`, legacy `ListSubscriptions`, `Status`, and `Observe`;
  direct HTTP callers must send `{}` instead of an empty body or `null`.
  Unknown fields, trailing JSON values, and request decoding past the configured
  body cap are rejected consistently. Official SDK, CLI, MCP, and console calls
  already send request objects.

### Add fixed subscription lifetimes (MQLITE-128)

- Add an optional fixed TTL to subscription creation across the engine, HTTP,
  Go SDK, CLI, MCP and console. The broker stores one absolute expiration from
  the first successful creation. Ordinary retries, filter updates, publishing,
  receiving and settlement never extend it. Omitting TTL keeps a new
  subscription permanent and preserves an existing lifetime; the same positive
  TTL is idempotent, while a different positive TTL conflicts before the filter
  can change.
- Treat expiry as advance authorization for forced deletion. At the exact
  deadline the subscription immediately leaves topic fan-out and every direct
  data-plane operation, even before background maintenance observes it. Leases
  cannot extend past the deadline, long polls wake there, and old receive or
  settlement receipts cannot revive work. The retained name remains reserved
  until restart-safe physical cleanup commits.
- Keep expiry cleanup bounded without making empty temporary subscriptions wait
  behind the one-resource remote cleanup rate. Indexed due transitions and
  512-subscription set-based metadata batches remove the common empty case while
  preserving transaction boundaries; subscriptions with messages continue
  through the existing row, byte and fairness budgets. Capacity still counts a
  logically expired subscription until cleanup removes it, so a large abandoned
  backlog creates deliberate backpressure instead of unbounded metadata growth.
- Preserve the legacy three-field subscription structs and complete list. New
  lifecycle pages add required `created_at_ms` and `expires_at_ms` fields (`0`
  means permanent), and report a due but not yet transitioned subscription as
  `deleting`. Advance the canonical database guard to schema token **7** for the
  absolute expiry column and due index; schema-5 and schema-6 files are refused,
  not migrated.
- Document the intended request/reply pattern: explicit deletion is the normal
  completion path and fixed TTL is a forgotten-cleanup backstop. High-rate
  callers should still use one stable per-instance reply subscription plus
  unique correlation IDs. When a temporary subscription is appropriate, give
  each lifetime a globally unique name and serialize same-name reuse because the
  public API deliberately has no Generation field.

### Delete and inventory subscriptions safely (MQLITE-127)

- Add empty-only subscription deletion and an explicit `force` option that
  durably authorizes discarding every retained message state. The accepting
  transaction immediately removes the subscription from topic fan-out and the
  data plane and starts bounded cleanup. It returns `deleted` if that cleanup
  removes the backing queue and mapping in the same commit; otherwise a durable
  `deleting` marker reserves the globally unique name while the restart-safe
  worker continues. The topic/name pair must match exactly. The implicit topic
  therefore becomes unavailable as soon as its last active subscription is retired.
- Keep the existing `ListSubscriptions` API as a complete, active-only inventory
  for compatibility, including its three-field Go `SubscriptionInfo`,
  `wire.SubscriptionJSON`, and JSON shape. Add `ListSubscriptionsPage` with
  lifecycle-aware `SubscriptionPageItem` / `wire.SubscriptionPageJSON` entries,
  topic filtering, opaque scope-bound keyset
  cursors, a 100-item default / 1,000-item maximum page, an exact per-request
  `total`, and an explicit `include_deleting` reconciliation view. A page series
  is not a frozen multi-request snapshot when the roster is changing.
- Bound active plus deleting subscriptions to 10,000 by default. New creation is
  rejected with `429 resource_exhausted` until physical cleanup releases a slot;
  an existing active subscription may still update its filter. Configure the
  bound with `Options.MaxSubscriptions`, `WithMaxSubscriptions`, or
  `MQLITE_MAX_SUBSCRIPTIONS`. The bound protects broker metadata, not fan-out
  latency: publishing remains proportional to messages times matching
  subscriptions.
- `CreateQueue` and `Subscribe` now require every queue, topic, and subscription
  name they create or update to be valid UTF-8 between 1 and 128 bytes. Existing
  valid UTF-8 names above 128 bytes remain readable and deletable through the
  management APIs, subject to the existing request/response size limits, but
  cannot be reconfigured under their old name after upgrading.
- Expose the lifecycle consistently through HTTP, the Go SDK, embedded mode,
  CLI, MCP, and the console. Destructive clients call their configured transport
  once; redirects, transport failures, every HTTP 5xx, and malformed
  acknowledgements require read-only reconciliation with the paged inventory.
  The SDK and MCP also validate complete, bounded, ordered subscription pages
  before using inventory as reconciliation evidence. This change retains schema
  token **6** and adds neither a Generation field nor subscription TTL; fixed TTL
  follows separately.

### Force-delete queues with bounded cleanup (MQLITE-126)

- Keep queue deletion empty-only by default. `force=true` is an explicit,
  persisted authorization to discard every retained message state plus queue-owned
  deduplication, settlement-receipt and receive-attempt records.
- Transition an accepted queue to `deleting` and start bounded cleanup in one
  transaction. If cleanup finishes, that commit removes the queue; otherwise the
  committed marker rejects queue work, reserves the name, and remains visible in
  `ListQueues` until continuation cleanup removes the row. Cleanup
  resumes after a restart; one foreground request removes at most 1,000 physical
  rows/about 8 MiB. Accepted pending deletes wake an event-driven worker, whose
  continuation passes are one second apart and each globally caps cleanup at 4,096
  rows/about 8 MiB. The ordinary loaded-resource path visits at most 32 queues
  locally or one queue on remote Turso/libSQL, so network round trips do not extend
  one write transaction across a large resource batch; the durable cursor still
  rotates fairly. One individually oversized row may exceed the byte target so
  cleanup always progresses.
  An uncertain remote delete commit arms a bounded 30-second discovery window;
  normal idle brokers perform no lifecycle polling.
- Keep the final queue-row retirement bounded even beside an unrelated deep
  backlog. Transactional target resolution and indexed parent guards preserve the
  queue/message relationship without an unindexed foreign-key scan or a trigger on
  every message insert; subscription child lookups gain a unique `subscription`
  index. `EngineTx.SQL` remains for caller-owned tables; direct writes to
  mqlite-owned tables are unsupported.
- Add force and deletion outcomes consistently to HTTP (`{name,force?}` and
  `{deleted,state}`), the Go SDK, `mqlite delete-queue --force`, MCP `delete_queue`
  and the console. The console requires exact-name confirmation and a separate
  force checkbox for a nonempty queue, including a warning about locked work whose
  external effects cannot be undone. The existing Go
  `DeleteQueue(ctx, name) error` method stays empty-only and returns `nil` for
  either accepted state; callers use `DeleteQueueWithOptions` for force and the
  detailed result. `QueueInfo` gains `State`; external Go code using positional
  composite literals must add the field or switch to keyed literals.
- Advance the canonical database guard to schema token **6** for durable lifecycle
  columns and cleanup indexes. Schema-5 databases are rejected rather than
  migrated: preserve the matching old binary and a consistent backup, account for
  retained work, and start this source with a fresh database.
- Return logically freed pages to later inserts immediately. Local database/WAL
  files shrink gradually: the 60-second reclaimer starts at 256 free pages and
  returns at most 256 pages per pass; explicit `mqlite vacuum` retains full
  incremental reclaim.

### Remove public queue generations (MQLITE-125)

- Remove `Generation`, generation-bearing deletion options, and `queue_changed`
  from the engine, HTTP API, Go SDK, CLI, MCP server, console, and documentation.
  Queue identity remains name-only; MQLITE-126 later adds a force-only deletion
  option without bringing lifetime identity into data-plane requests.
- This is an intentional breaking correction for v0.3.3 clients. The v0.3.3
  release is withdrawn and unsupported; its public artifacts remain available for
  forensic or compatibility inspection and must not be deployed. v0.4.0 carries
  its retraction directive; v0.3.2 clients are unaffected. This correction itself kept
  schema token **5**; MQLITE-126 advanced the v0.4.0 development line to token 6,
  and MQLITE-128 advanced the release schema to token 7.
- Make the name-reuse boundary explicit. Queue deletion and same-name recreation
  must be serialized by one owner. Do not replay an old deletion after recreation;
  reconcile an uncertain result before allowing the name to be reused.
- Treat every temporary subscription name as a globally unique, one-use identity.
  Use `correlation_id` to match replies. For frequent request/reply traffic, keep a
  stable shared reply subscription and dispatch replies by correlation ID instead
  of creating one subscription per request.

## v0.3.3 — 2026-10-13 (withdrawn; retracted by v0.4.0)

> **Withdrawn:** this release exposed queue lifetime metadata as a public API and
> spread it through deletion clients. That contract is removed in v0.4.0, which
> also declares `retract v0.3.3`. Use v0.4.0 or later, or remain on v0.3.2 when an
> immediate schema-5 to schema-7 cutover is not possible.

> **Upgrade from v0.3.2:** port **6754**, schema token **5**, message data and
> managed-key storage remain compatible. Keep one active broker per database and
> take a consistent backup before replacing it. Downgrading to v0.3.2 preserves
> data but removes queue deletion and detailed maintenance diagnostics.
> Remove `--diagnostic-log` from CLI arguments when running an older binary.

### Maintenance diagnostics and queue lifecycle (MQLITE-124)

- Report bounded maintenance failure history with the failed stage, safe cause,
  timestamp, fingerprint, consecutive failures and recovery status. Historical
  failure counts are explicitly separated from current health.
- Add optional `MQLITE_DIAGNOSTIC_LOG` / `--diagnostic-log` rotating private JSONL
  logs with credential redaction, restrictive permissions and synced records.
  Ambiguous transaction outcomes still require reconciliation before retrying.
- Add manage-only `DeleteQueue` to the HTTP API and Go SDK. Only empty plain queues
  can be deleted; messages in any retained state prevent deletion. Queue removal and
  cleanup of deduplication and replay metadata commit in one transaction.
- The deletion request shape shipped in this release is withdrawn and incompatible
  with the corrected name-only contract described above.
- Add exact-name deletion confirmation to the console, make canonical snapshots
  compact, collapse cumulative message activity by default and fix mobile overflow.
- Update the console build dependency `source-map-js` to its patched release and
  the CI lint analyzer for compatibility with the current Go toolchain.

## v0.3.2 — 2026-09-23

> **Upgrade from v0.3.1:** port **6754**, schema token **5**, message data and
> managed-key storage are unchanged. Stop the broker and take a consistent backup
> before replacing it. Prometheus scraping now requires `MQLITE_METRICS=on` or
> `--metrics`; it is disabled by default. Process counters restart from zero.
> Rolling back to v0.3.1 also requires its previous monitoring configuration: `Observe` and
> configured monitor credentials require v0.3.2. See the
> [upgrade and rollback procedure](docs/operations.md#upgrade-and-rollback).

### Unified observability (MQLITE-124)

- One typed engine observation supplies native `Observe`, SDK/CLI/MCP/console
  views and Prometheus. Queue aggregation uses a fixed number of reads, with
  explicit availability; failed reads never become healthy zero counts.
- Track confirmed message effects, storage outcomes/retries/pool waits,
  maintenance results and filter failures. HTTP measurements include missing,
  expired and revoked credentials and permission rejection, while retaining the
  existing post-authentication handler histogram semantics.
- Add configured read-only `MQLITE_MONITOR_TOKENS` / `WithMonitorTokens` for
  `Observe` and `/metrics`, independently of the managed-key database. The
  managed-key permissions and database schema remain unchanged.
- Make Prometheus `/metrics` opt-in on the existing API port with
  `MQLITE_METRICS=on`, `--metrics` or SDK `WithMetrics(true)`. Administrator auth
  must remain enabled; monitor and administrator credentials can scrape it.
  Disabled requests return `404`, and discovery omits `metrics`. Enabling it
  automatically adds `"metrics":"/metrics"` to discovery. No extra port, URL
  setting, process or VM is required.
- Include a runnable MQLite, Prometheus and Grafana starter with a provisioned
  dashboard, rules, repeatable verification scenarios and cloud deployment guidance.
- Preserve old metrics as exact compatibility projections. Prefer
  `mqlite_queue_retained_messages` to the misleading gauge name
  `mqlite_queue_total`, and seconds to the legacy millisecond age family. The
  two old names are deprecated for removal in v0.4.0.
- Fix Prometheus escaping for special queue names and coherent histogram
  snapshots under concurrent requests. Cross-queue redrive now preserves a legal
  empty message body instead of binding SQL NULL.

## v0.3.1 — 2026-09-20

> **Upgrade from v0.3.0:** port **6754** and schema token **5** are unchanged.
> Stop the broker and take a consistent backup before replacing it. Existing queue
> data is retained; startup adds the managed-key table and indexes. Keep a configured
> administrator token available for rollback. See the
> [upgrade and rollback procedure](docs/operations.md#upgrade-and-rollback).

### Persistent access keys (MQLITE-121)

- Create, list and revoke broker access keys at runtime through the HTTP API,
  Go SDK, `mqlite key` commands, MCP tools or the console. Records survive restart
  in the same database;
  only SHA-256 digests are stored, and a generated secret is returned once.
- Assign `send`, `listen`, their combination, or `manage`. Consumption includes
  message settlement and renewal. `manage` includes both data permissions and
  can issue or revoke other managed administrator keys. Existing `MQLITE_TOKENS`
  and `WithTokens` credentials remain administrators with the same operation
  rights; changing them still requires updating configuration.
- All newly generated tokens use `mqk_` plus 64 lowercase hexadecimal characters
  (256 random bits). Previously configured tokens remain accepted unchanged.
  Invalid, expired or revoked credentials return 401; insufficient permission
  returns 403 `permission_denied`. The SDK treats permission denial as permanent.
- A compatible `access_keys` table is added under **schema token 5**. Existing
  v0.3.0 queue data can be reused. Downgrading to v0.3.0 preserves these records
  but ignores database keys; retain a configured administrator for that rollback.
  Backups include key state, so restoring an older snapshot can also restore a
  subsequently revoked key. Review credentials before reopening restored traffic.
- The console adds an Access keys page with permission selection, pagination,
  expiry/revocation status and a one-time secret display. The MCP server adds
  `create_key`, `list_keys` and `revoke_key`.
  The [console source](https://github.com/mqlitehq/mqlite-web) is public, and its
  bundled third-party license notices are included in the embedded assets.
- Console expiry and scheduled delivery use an English calendar and fixed
  `yyyy-MM-dd HH:mm` local input, with strict invalid-date validation. Listed
  timestamps use `yyyy-MM-dd HH:mm:ss`; runtime-issued credentials are labeled
  "managed keys" to distinguish them from configured administrator tokens.
- Managed keys in the console are ordered newest first across pages. HTTP,
  SDK, CLI and MCP listing support `created_desc` ordering with a stable ID
  tie-breaker and indexed cursor pagination; the existing default ID order is
  retained for API clients and exact-ID recovery.
- Permissions apply to the whole broker. The console and `/metrics`
  require `manage`; ordinary application keys use the SDK, CLI or HTTP API.
  Explicit auth-off mode keeps the existing queue behavior and refuses key
  management. Bare or duplicate Authorization headers are rejected.
- The concepts guide and console's AI filter reference now correctly explain
  that `now()` reads the wall clock; use message timestamps for repeatable routing.
  Filter behavior is unchanged. The guide's Go SDK filter example is also corrected.

## v0.3.0 — 2026-09-10

> **Schema: this release is `schemaVersion = 5`.** mqlite keeps ONE canonical schema and never
> migrates (pre-1.0), so an existing DB from any earlier release is refused with
> `ErrSchemaVersionMismatch` and must be recreated. **Before upgrading, stop all writers and back
> up** (`VACUUM INTO`, or copy the complete data directory with the broker stopped), then start on a fresh DB — mqlite
> never deletes your data for you. Two changes below are what made the schema incompatible:
> `seq_number` allocation and the settlement-receipt key.

### Behavior changes

- **Receive-and-delete no longer returns a phantom lease deadline** (MQLITE-116).
  Both the initial response and receive-attempt replay now clear the deadline as
  well as the token. The deleted message has no lease to renew or settle; the SDK
  reports a zero `LockedUntil` time. Peek-lock responses retain their lease metadata.

- **Expired leases now reject settlement immediately** (MQLITE-114).
  Complete, Abandon, Reject, Defer, Renew and both batch variants require a matching
  token and an unexpired lease, including the interval before background recovery.
  Time is sampled after writer admission and again for every statement and remote
  retry. Previously an expired token could still settle or revive a locked row.
  Live receipts still replay only an exact previously committed settlement request;
  this remains valid after that request's original lease expires.

- **Runtime images patch their installed Alpine packages** (MQLITE-115).
  Image builds upgrade APKs within Alpine 3.24 before adding runtime dependencies,
  including the OpenSSL 3.5.8 fix for CVE-2026-14456. Selecting a supported base
  branch alone previously left older preinstalled libraries in the shipped image.
  CI scans the actual image's OS packages and fails on HIGH/CRITICAL findings;
  CI and release builds refresh the runtime layer instead of reusing its APK cache.
  Both architectures of the actual release image are scanned and run before
  publishing that same OCI archive with its digests preserved (MQLITE-119).

- **Release artifacts now require matching source and complete CI** (MQLITE-109).
  Both release workflows resolve the real tag, verify its version constant, and
  require all 12 CI jobs to succeed for that exact commit in one completed run
  attempt. Manual image builds use the tagged commit. RC images get only their
  full version tag; stable releases also update the minor and `latest` aliases.
  Docker uses Go 1.27 / Alpine 3.24, scans its actual binary with `govulncheck`,
  and tests authenticated message settlement and persistent-volume recovery.
  CI and GoReleaser also scan both archive binaries for all six release platforms,
  including Windows-specific dependency paths absent from a Linux source scan.
  Artifacts retain symbols for binary analysis while omitting DWARF debug data.
  The Go 1.21 embedding floor is unchanged.

- **Claim timestamps now follow writer admission** (MQLITE-108). `Receive` (plain or
  `AttemptID`, including receive-and-delete) and `ReceiveDeferred` use fresh broker time
  for each item and each remote retry when checking TTL and setting the lease deadline.
  Previously a wait for the single writer could consume the lease before a message was
  claimed, and deferred receives could deliver messages whose TTL had expired. Attempt-record
  expiry checks and creation timestamps also use fresh transaction-time observations.
- **`seq_number` is now allocated with `AUTOINCREMENT` and never reused**
  (MQLITE-71). Previously SQLite could recycle the highest `id` after its row was deleted,
  so a stale `Cancel` (which is fenced only by `seq_number`) could delete a *later* message
  that reused that id — real message loss. Sequence numbers are now strictly increasing and
  never reused (they may gap); a settled/cancelled seq is gone for good. Schema-breaking — see the
  note above.
- **Scheduled multi-message `Send` is now atomic** (MQLITE-72): a mid-batch failure (e.g. a
  `group_fifo` item missing its `group_id`) rolls back the whole batch instead of leaving
  earlier items scheduled. A single scheduled message still distinguishes a dedup conflict
  (`409`) from a no-subscriber no-op (`seq 0`).
- **Updating a subscription filter no longer resets its backing queue's config**
  (MQLITE-73): re-subscribing changes only the mapping/filter; lock duration, delivery
  count, TTL, dedup, ordering and DLQ settings previously configured on the backing queue
  now survive (they were silently reset to defaults on every filter update).
- **`Close()` is now a fenced lifecycle shutdown** (round-7/8 review campaign, MQLITE-101):
  `Close` shuts the admission gate first — any DB operation that starts after shutdown
  begins fails fast with `ErrClosed` — wakes empty long-poll `Receive`s with `ErrClosed`
  instead of letting them sleep out their poll window, and **waits for in-flight writes and
  transactions to finish before releasing the file lock**. Previously `Close` released the
  file lock immediately even while a foreground transaction was still writing: a second
  `Open` on the same file could succeed mid-shutdown and both engines could write at once
  (a double-writer window). Pinned by public-`Tx` and engine-level regression tests.
  Note: `Close` MUST NOT be called from inside a `Tx` callback — it waits for the callback,
  so that deadlocks; this is documented on `Tx` and `Close`.
- **`Abandon` with a backoff delay now holds the group's line** (MQLITE-66): on an
  ordered queue (`group_fifo`/`strict_fifo`, or grouped messages on `standard`),
  `Abandon` with `delay_ms > 0` used to requeue the head as `active` with a future
  `visible_at` — invisible to the head-of-line probe — so successors overtook the
  backing-off head during the backoff (out-of-order delivery). The head now parks in
  `scheduled` until the backoff lapses (the scheduler re-activates it), exactly like a
  delayed send, and the group stays blocked behind it. `Cancel` correspondingly only
  deletes scheduled messages that were never delivered (`delivery_count=0`) — a
  backoff-parked redelivery is not cancellable and stays under the retry/DLQ discipline.
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
  and no port probing. To keep an existing 8080 endpoint, configure a native broker with
  `mqlite serve --addr :8080` (or `MQLITE_ADDR=:8080`). For a container still listening on
  its new default 6754, publish `-p 8080:6754`; if its internal listener is also changed to
  8080, publish `-p 8080:8080`. Update SDK/CLI/MCP endpoints, health probes and firewall
  rules to match the chosen listener and mapping.
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
  settle whose response was lost replayable — the same request, the same success. They were
  not checking the verb, so `Abandon(T)` (which returns the message to `active`) left a receipt
  that a later `Complete(T)` read as "already completed": **the caller was told the message was
  gone while it sat in the queue, waiting to be handed to somebody else.** `CompleteBatch` reported
  the same false `ok`. Every cross-verb combination now fails with `ErrLockLost` / `ok=false` and
  leaves the message where it was; an exact-request replay is still an idempotent success. **This defect
  predates the batch work and ships in the released v0.2.0.**
- **`Engine.Tx` / `Embedded.Tx`: the callback may run more than once on a REMOTE store** (MQLITE-99,
  documentation). A transaction that fails on a retryable connection/busy error is replayed from
  the start. The SQL rolls back, so your data stays correct — but anything the callback does
  *outside* the transaction (an HTTP call, a charge, a counter) will have happened twice. Keep the
  callback transaction-bound. Local file and `:memory:` stores never retry, so it runs zero or one
  time there: an already-cancelled caller never enters the callback.

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
  nothing. A running statement finishes; mqlite-owned transaction statements check cancellation
  before starting each subsequent statement. Raw `EngineTx.SQL()` calls bypass those guards — see
  MQLITE-103 below. **Be clear about what this costs**: a local statement that has started cannot be
  cancelled, and there is no upper bound on how long it runs — `EngineTx.SQL()` executes arbitrary
  user SQL, a full `VACUUM` rewrites the file, a large `Purge` is a big `DELETE`. A write can
  therefore commit *after* its caller has given up, so keep treating a lost response as
  at-least-once and reconcile. **Remote (Turso) stores are unchanged**: their statements can
  genuinely block, so they keep real cancellation, and a discarded connection there holds no local
  lock. New `EngineTx.Context()` is the context your own statements inside `Engine.Tx` should use;
  the full contract is in `docs/conformance.md`.
- **Settlement receipts identify the REQUEST, not just the token** (MQLITE-100). Schema-breaking —
  see the note above. A receipt now records
  `queue + seq_number + lock_token + operation + args`, and every part is load-bearing — each one
  was a false success in its own right:

  - **the message.** Matching the token alone meant `Complete(seqB, tokenA)` found A's receipt and
    reported success for a message it never touched — in the same queue, through `CompleteBatch`,
    and even across queues. The message stayed locked while the caller was told it was gone.
  - **the verb.** An `Abandon(T)` — which returns the message to the queue — left a receipt a
    later `Complete(T)` read as "already completed".
  - **the arguments that change the effect.** `Abandon`'s `delay_ms` and `Reject`'s
    `reason`/`description`. `Abandon(T, 60s)` after `Abandon(T, 5s)` returned success while
    silently keeping the 5s delay: your backoff was discarded and the message came back early.

  A replay is the SAME request, and only that. Anything else, on a message you no longer hold, is
  `ErrLockLost` — which is the truth.

### Validation and contract documentation

- **Production operation and restore procedures are explicit** (MQLITE-110):
  read-only online backups, complete offline copies, fresh-directory restores,
  matching binary/schema rollback, single-owner replacement and `FULL` production
  examples. Monitoring distinguishes backlog gauges, observed completion counters
  and write readiness; the interactive concepts companion is reviewed with its
  Markdown source. The restore drill verifies complete logical snapshots and both
  directions of real v0.2.0/schema-2 compatibility refusal.
- **Raw transaction cancellation is documented at the public API boundary** (MQLITE-103).
  On local stores, business SQL must use the protected `tx.Context()` and callers must check
  the original context between their own statements. Raw `tx.SQL()` bypasses mqlite's statement
  guards and can execute after cancellation; cancellation observed before commit rolls back
  the whole transaction after the callback returns, even if it returns nil. Public file and
  `:memory:` tests cover that boundary without changing the API or cancellation mechanism.
- **Settlement replay checks cover the complete request identity** (MQLITE-104/105).
  The regression matrix covers single/batch replays, queue/sequence/token/operation mismatches,
  and effect-bearing arguments. The independent model uses structured receipt
  keys so Reject reason/description delimiters cannot collide; it also checks equivalent default
  reasons. These changes strengthen tests without changing settlement behavior.
- **Crash recovery covers the final delivery reaching the DLQ** (MQLITE-107). A hard-killed
  worker holds a message on its last permitted delivery; recovery must preserve the payload and
  delivery count, clear the lock, and dead-letter it with `MaxDeliveryCountExceeded`. Producer
  identities now use a cryptographically random process nonce plus a counter. Crash assertions
  cover atomicity and every producer-acknowledged commit observed by the harness, not power loss.
- **Contract wording and runnable examples are aligned** (MQLITE-106): exact-request settlement
  receipts, zero-or-one local transaction callbacks, acknowledged-commit crash evidence, the
  Stats example's `BASE_URL`, and the separate race-instrumented Linux crash CI job.

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
