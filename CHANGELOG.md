# Changelog

Hand-written summary of user-visible changes — **behavior changes first**, because
those are what an upgrade can feel. Commit-level notes are auto-generated on each
[GitHub Release](https://github.com/mqlitehq/mqlite/releases); this file only
records what changes semantics, adds capability, or fixes something you could hit.

mqlite is pre-1.0: any release may change behavior, and a schema change makes old
DB files unreadable by design (`ErrSchemaVersionMismatch` — recreate, don't migrate).

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
