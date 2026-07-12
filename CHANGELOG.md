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
