# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

mqlite is a small, **pure-Go (no CGO)** message queue with Azure Service Bus–style
semantics (Peek-Lock, retries, DLQ, scheduling, dedup, GroupID ordering, topics)
over a single SQLite file or a remote Turso/libSQL database. The same engine runs
either **embedded in-process** (like goqite) or as a **network broker**.

## Purpose & design priorities

mqlite aims to be **the smallest reliable queue you can drop into a system** — and
to stay friendly to both humans and AI agents. Priorities, in order:

1. **Lightweight & embeddable.** One pure-Go binary over one SQLite file (or a
   remote Turso DB). No broker cluster, no sidecar, no CGO. Embed it in-process or
   run it as a broker; move storage from `:memory:` to replicated Turso without
   touching app code.
2. **Reliable under concurrency.** At-least-once with fencing tokens, single-broker
   crash recovery, idempotent send/receive/settle, and O(log n) claims on a deep
   backlog — built to take high enqueue/dequeue throughput without losing or
   double-delivering messages.
3. **Simple, unambiguous interface.** Every operation is one HTTP POST with a JSON
   body — curl-able and easy for an LLM/agent to drive — with exactly one
   settlement verb per outcome and no aliased calls.
4. **Service Bus flavor, not a clone.** Peek-Lock, `GroupID` sessions, DLQ,
   scheduling and dedup feel familiar if you know Azure Service Bus, but the API is
   its own idiomatic-Go shape.

Target: everyday queueing (background jobs, transactional outbox, topic fan-out,
rate-limited pipelines), not Kafka-scale streaming. When a design choice is unclear,
prefer the option that keeps it **lightweight, simple, and correct under
concurrency** over the one that adds machinery.

## Language

**English is the project language.** All code, comments, identifiers, commit
messages, PR titles/descriptions, docs, and GitHub release notes are written in
English. (A conversation with the maintainer may happen in their language, but
nothing that lands in the repository or on GitHub does.)

## Commands

```bash
make build          # → bin/mqlite + bin/mqlite-bench
make test           # go test ./...   (hermetic unit + invariant tests)
make e2e            # ./test/run.sh — boots an ephemeral broker, runs curl+python+SDK blackbox suites
make bench          # Docker stress matrix → test/bench/out/
make clean          # delete ALL generated data (*.db, bin/, bench out, smoke dirs)

go test -race -count=1 ./...                 # what CI runs (race detector)
go test ./engine -run TestClaimDeepBacklogBounded -v  # a single test (package + -run regex)
go test ./engine -run 'TestClaim/<subtest>' -v        # a single table-driven subtest

# live remote round-trip (otherwise skipped); also runs in the Turso nightly workflow:
MQLITE_TEST_DB=libsql://<db>.turso.io MQLITE_TEST_DB_AUTH_TOKEN=<jwt> \
  go test ./engine -run TestTurso -v
```

Before committing, match CI locally: `go build ./...` · `go vet ./...` ·
`gofmt -l .` (must be empty) · `go mod tidy` (no diff) · `go test -race ./...` ·
`golangci-lint run`. CI additionally enforces **per-package coverage floors** (see
`coverage` job in `.github/workflows/ci.yml`: engine 72%, server 62%, wire 90%,
cmd/mqlite 52%, root 55%, total 65%) — a drop below any floor fails the build.

Go floor is **1.21** (`go.mod`); CI matrixes 1.21 + stable across linux/macos/windows.

## Releasing, versioning & dependencies

- **Tagging is gated.** Never create a git tag or GitHub Release without the
  maintainer's explicit go-ahead. Default version bumps are **patch** (semver)
  unless told otherwise.
- **Pre-tag checklist:** bump `internal/version/version.go` (the single version
  source both binaries report; release.yml refuses a tag that doesn't match it),
  update `CHANGELOG.md`, sync the docs' pinned version examples
  (README/deployment/api-reference), and — if mqlite-web changed — refresh the
  embedded console dist under `server/web/`. A `vX.Y.Z-rc.N` tag runs the full
  pipeline without touching `:latest` (prerelease auto-detected) — cheap dress
  rehearsal before the real tag. Delete the rc release + tag before promoting
  (GORELEASER_CURRENT_TAG pins the build to the pushed tag either way, but a
  dangling rc release invites confusion).
- **One ticket → one PR**; CI must be green before merge (the `gh pr checks` exit
  code is the gate); reference the Backlog id (`MQLITE-N`) in the commit/PR.
- **The `go 1.21` floor is deliberate** — it is the embedding-compatibility floor,
  so `modernc.org/sqlite` and `golang.org/x/sys` are frozen at the last versions
  that build on 1.21 (newer ones require go ≥ 1.23). Enforced by
  `TestGoModFloorStaysAt121` (in `sdk_test.go`) + Dependabot ignore rules and
  explained in `docs/dependencies.md`; `govulncheck`
  stays green at the pins. Release **binaries are built with the latest patched Go**
  (Docker `golang:1.25`, CI `stable`) so the shipped artifact carries current stdlib
  security fixes — the low floor governs who can *import* the SDK, not what compiles
  the release.
- Longer design notes that don't fit in code live in `docs/`: `dependencies.md`,
  `turso.md`, `retention.md`, `benchmark.md`.

## Architecture

One engine, two front-ends. Dependencies point inward; `engine/` knows nothing
about HTTP.

```
cmd/mqlite/         CLI: serve | send | receive | peek | metrics | redrive | ...
   │                (stdlib `flag` only — deliberately NO cobra/pflag; dependency-light)
   ▼
. (package mqlite)  Native Go SDK — the public API surface
   ├─ Embedded      in-process engine   (OpenEmbedded)  + Tx + Serve
   ├─ Client        remote HTTP client  (Open)
   ├─ Receiver      hands-off consume loop (nil→Complete, err→Abandon, auto-renew)
   └─ Message       settlement methods hang off *Message so lock tokens never leak
   │   Embedded and Client both implement the private `settler` / `receiveSource`
   │   interfaces (mqlite.go), so *Message and Receiver work identically on both.
   ▼
server/             Connect-style JSON-over-HTTP broker + Bearer auth + /metrics
   ▼ (shares ↓)
wire/               THE JSON contract (request/response structs + route paths).
   │                Single source of truth imported by BOTH server and client — keep
   ▼                them from drifting by changing types here, never in one side only.
engine/             Queue core: Store + Service Bus semantics. Transport-agnostic.
```

RPC routes are `/mqlite.v1.<Service>/<Method>` (defined once in `wire/wire.go`).
Every operation is one HTTP POST with a JSON body — curl-able by construction.

### The single-writer model (most important invariant)

- **Local file / `:memory:`** → `db.sql` uses `SetMaxOpenConns(1)`. That one
  connection *is* the single writer: `database/sql` serializes every caller, so
  there is zero file-lock contention and claims are atomic. (`engine/db.go`)
- **Embedded local file** additionally takes an OS advisory lock on a `<db>.lock`
  sidecar (`engine/lock_unix.go` / `lock_windows.go`). A second process — or second
  `OpenEmbedded` on the same file — fails fast with `ErrDBLocked`. Sharing a file DB
  across processes is unsupported; run the broker instead (one broker = one writer).
- **Remote Turso/libSQL** → small pool (`MaxOpenConns(4)`) because the Turso primary
  serializes writes server-side. Only the remote path **retries** transient errors
  (closed Hrana stream → `driver.ErrBadConn`; `database is locked` / SQLITE_BUSY)
  with backoff via `db.exec/query/inTx` — see `attempts()`/`retryable()`. Local never
  retries. All retries are safe because the failed statement never ran.

### Claim path — do not "simplify" the SQL

`engine/claim.go` is the hot path and the trickiest code in the repo.

- Only `state='active'` rows are claimable; the partial index
  `idx_msg_active(queue,id) WHERE state='active'` keeps claim O(log n) even under a
  deep backlog. Expired locks are reclaimed by the **reaper** (background loop), not
  on the claim path, so the hot path never scans locked rows.
- Ordering modes share one claim statement except `strict_fifo`:
  `standard`/`group_fifo` use `claimSQL` (per-group head-of-line); `strict_fifo` uses
  `claimStrictSQL` (global head-of-line). `group_fifo` only differs from `standard`
  by requiring a `GroupID` at send time.
- The head-of-line check is written as **one `EXISTS` per in-flight state**
  (deferred/scheduled/locked), each a single `state=` equality, so SQLite seeks a
  partial index. **Do NOT collapse them into one `NOT EXISTS ... state IN (...)`** —
  that plans as a backward rowid scan, O(n) per candidate / O(n²) to drain a blocked
  backlog (this was a real incident, MQLITE-22; the comments explain it). The matching
  indexes live in `engine/schema.go`.

### Idempotency & at-least-once

mqlite is honestly **at-least-once** — handlers must be idempotent. Three mechanisms:

- **Crash recovery** (`engine.Open`): on startup every orphaned `locked` row is reset
  to `active` (single-broker assumption). delivery_count was already bumped at claim.
- **`settlement_receipts`** table makes Complete/Abandon/Reject/Defer idempotent:
  a settle that affects 0 rows but finds a live receipt for the lock_token returns
  success (lost-response replay) instead of a spurious `ErrLockLost`. See
  `settleOp` in `engine/settle.go`. Settlement is **fenced on `lock_token`**.
- **`receive_attempts`** table makes Receive idempotent when the client passes an
  `AttemptID` (a retry replays the same batch / same lock tokens; `engine/recv_attempt.go`).

### Other engine subsystems

- **Background loops** (`engine/background.go`): reaper (1s, expired locks→active or
  DLQ), scheduler (1s, scheduled→active), TTL (10s, →DLQ/discard), dedup janitor
  (60s), aux janitor (60s, prunes receipts). All write through the single conn.
  Tests use `WithoutBackground()` + `RunMaintenanceOnce(ctx)` for deterministic time.
- **Long-poll notifier** (`engine/notify.go`): close-and-rotate channel per queue.
  Waiter takes the channel **before** re-checking the queue to avoid a lost wakeup.
  Single-process only — exactly mqlite's target.
- **Transactional outbox** (`engine/tx.go`, `Embedded.Tx`): embedded-only. Business
  writes + enqueue commit in one `*sql.Tx` ("business success ⇔ message enqueued").
- **Topics/subscriptions** (`engine/topic.go`, `engine/filter.go`): a topic fans a Send
  out to each subscription's backing queue (addressed by the bare subscription name).
  A subscription filter is one `expr-lang` boolean predicate over the message
  (`Filter{Expr}`), compiled+type-checked at Subscribe (cached per subscription) and
  run fail-closed at publish — see `docs/concepts.md` (§ Subscription filters).
  Receive/Redrive/Stats target the subscription by name.

### Conventions specific to this repo

- **All times are epoch milliseconds** (INTEGER, UTC) end to end. The clock is
  injectable (`Options.Now` / `WithClock`) for tests.
- **One SQL statement per `exec`** — remote Hrana (libSQL) requires it; schema is a
  `[]string` of single statements (`engine/schema.go`). Tables are `STRICT`.
- **Schema versioning**: there is a **single canonical schema** (`schemaStmts`); we do
  not keep version history or migrations. `schemaVersion` is just an opaque guard
  token. `initSchema` is CREATE-IF-NOT-EXISTS only, so it never alters an existing DB —
  instead `Open` **refuses** a DB whose recorded token differs
  (`ErrSchemaVersionMismatch`). Change the token whenever the schema changes
  incompatibly; pre-1.0 there is no migration, a stale DB is recreated.
- **DB DSN is read only from the environment**, never compiled in. Auth tokens are
  injected at `resolveDSN` time. `MQLITE_DB` (embedded/serve) vs
  `MQLITE_ENDPOINT`+`MQLITE_TOKEN` (client mode, wins if set); `MQLITE_TOKENS` =
  broker's accepted Bearer tokens; `MQLITE_CORS` = `Access-Control-Allow-Origin` the
  broker sends (unset → `*`, since RPCs still need a token; `off` disables — lets a
  browser console on another origin reach the broker); `MQLITE_SYNC` = durability knob
  (NORMAL/FULL/OFF).
- **Dedup-conflict batch semantics**: in a multi-message `Send`, a conflicting slot
  (same `message_id`, different body) comes back as seq `0` (skipped) while the rest
  commit; a single-message Send/Schedule surfaces it as `ErrDedupConflict` (HTTP 409).
- **Sentinel errors** live in `engine/types.go`; the server maps them to HTTP/Connect
  codes in `server.fail` / `settleOK`. Re-exported from `package mqlite` (mqlite.go)
  so `errors.Is` works in either mode.
- Comments reference tickets as `MQLITE-N` (design rationale) and `#N` (GitHub issues).

## Testing layers

| Layer | Where | Notes |
|---|---|---|
| Unit + invariant (TCK-style) | `*_test.go`, `engine/*_test.go` | Hermetic, temp dirs; CI runs with `-race`. `engine/main_test.go` is the harness. |
| Blackbox e2e | `test/run.sh` + `test/api_curl.sh`, `api_tests.py`, `sdkcheck/` | Boots a real broker; catches HTTP API drift the in-process tests can't. |
| Live Turso | `engine/turso_test.go` (gated by `MQLITE_TEST_DB`) | Skipped unless env set; runs in `turso-nightly.yml`. |
| Stress/bench | `test/bench/` (`make bench`) | Docker matrix. |

`make clean` removes every generated artifact (DBs, binaries, bench output, smoke
dirs); the regenerate targets recreate them. Source is never touched by clean.

### Test file organization (keep it tidy)

Test files are grouped **by theme, not one file per micro-feature** — a new test
joins the existing file for its subject (separated by a `// ─── section ───`
banner) instead of spawning `feature_x_test.go`. The current shape, and the rules
that keep it from sprawling again:

- **One thematic file per concern.** `engine/storage_test.go` holds the storage
  layer (durability pragma · schema-version guard · single-writer lock · remote
  retry classification); `engine/turso_test.go` holds every live-remote test;
  `engine/functional_test.go` / `engine/engine_test.go` hold queue semantics. At the
  root, `sdk_test.go` is the blackbox SDK suite (harness · remote/HTTP · embedded
  example · build guards). Add to these; don't fork a new file for each test.
- **Split a file out ONLY for a real structural reason**, not by topic:
  - a different package — white-box `package mqlite` (`receiver_internal_test.go`)
    can't live with blackbox `package mqlite_test`;
  - a build constraint — `engine/race_{enabled,disabled}_test.go` (`//go:build race`);
  - the `TestMain` harness — `engine/main_test.go`.
- **One-off / external tooling is not a package test.** Stress harnesses, curl/python
  blackbox drivers, and SDK smoke checks live under `test/` (`test/bench/`,
  `test/sdkcheck/`, `test/*.sh`/`*.py`), never as a `*_test.go` in a source package.
- When a test moves, **update its cross-references** — `docs/conformance.md` cites
  tests by `path:TestName`, and `docs/{dependencies,turso}.md` name files too.
