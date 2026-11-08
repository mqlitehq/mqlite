# Finite remote write acceptance

This opt-in nested module (`mqlite_perf_diag`) compares fresh local SQLite with
two explicitly approved disposable remote databases. Product engine files match
the source under test. `engine_phase_tracing` is false: the command observes
public outcomes, provider/broker HTTP and local `database/sql.DBStats`, without
private engine phase hooks. It does not certify production capacity, long soaks,
power-loss durability or every functional scenario.

## Modes and scope

- `quick`: native SDK C1, B1/B16/B64, plus separate dedup-off and 75ms provider
  delay controls. It measures bounded batch amplification.
- `contention`: unchanged original native and low-floor SDK C1/B1, C1/B64,
  C8/B1 and C32/B1 profiles. They send before draining, retain each worker's empty
  Receive, one-hour dedup, ten-minute leases, background maintenance and ordinary
  pool settings. Separate pool/admission controls cannot replace original results.
- `mixed`: real managed Receivers in both SDKs, concurrency four, automatic
  renewal and background maintenance. Each SDK runs a three-second and default
  thirty-second lease case. Four seed handlers hold for six or twelve seconds.
  After all four start, four separate B16 Sends add 64 new unique messages at
  0.75/1.75/2.75/3.75 seconds (short lease), or 2/5/8/10 seconds (default lease).
  This finite `periodic_b16` profile creates actual message INSERTs; it does not
  substitute duplicate-ID read-only replays for write contention. A concurrent
  Stats reader has a five-second sample deadline. New handlers take 50ms. Only
  the Receiver settles messages. B64 batch acceptance remains in the other modes.

Mixed cases require all 68 unique Send acknowledgements, exact bodies and
headers, one delivery per identity, no unknown outcomes, handler cancellation,
SDK error callback, duplicate or DLQ. On remote backends a successful Renew RPC interval must overlap a provider
request tagged `message_insert`; absence is a coverage failure. Provider events
and renewal intervals use a common local monotonic origin and expose their
relative start/end milliseconds. SQLite has no provider HTTP and reports that
coverage as not applicable; it still requires successful renewal and full SDK
lifecycle and identity acceptance. Successful Complete responses are counted only
when the SDK reads a complete valid 2xx body; this is explicitly named
`client_observed_complete_response`, not an internal SDK settlement hook.
Receiver cancellation/join, zero error callbacks and empty final metrics are
independent acceptance checks. Explicit low-level client settlement results
remain separate in the ledger.

`--case all` keeps each mode's order. An exact case ID selects only that case;
unknown, ambiguous and cross-mode selections fail before database access.
The mixed IDs are `mixed_native_lock3`, `mixed_native_lock30`,
`mixed_low-floor_lock3`, and `mixed_low-floor_lock30`. A focused PASS covers only
the selected scope. SQLite always runs before selected remote backends. Remote
pool controls are explicitly not applicable to SQLite's single-writer pool.

## Safety and reproducibility

Remote URLs and tokens come only from `MQLITE_DIAG_STANDARD_DB` /
`MQLITE_DIAG_STANDARD_TOKEN` and `MQLITE_DIAG_RUST_DB` /
`MQLITE_DIAG_RUST_TOKEN`. Original DSN spellings must match fixed SHA-256
allowlists. No command-line/workflow target or allowlist override exists.
Presence of legacy `MQLITE_TEST_DB` or `MQLITE_TEST_DB_RESET_CONFIRM`, even empty,
refuses execution. This executable never runs engine TestMain or resets tables.
Normal compatible engine initialization and startup recovery still apply.

An unpredictable namespace and exact `(backend, case, queue)` manifest are
written before any owner opens. Cleanup deletes only an exact manifest entry,
requires a completed deletion and queue-not-found readback, and never rewrites
failure/unknown outcomes into success. Existing evidence directories are refused.
Each owner closes before the next opens. Remote Close additionally requires a
200ms stable HTTP observation window with zero active requests/open bodies,
bounded by ten seconds. This observable barrier does not claim to join private
driver goroutines. An unjoined owner blocks all later cases.

SDK calls retain a 180-second caller deadline; cases have seven-minute and
backends eighteen-minute watchdogs. Mixed read samples use the stated five-second
bound. Cancellation/join is bounded by ten seconds. An outer watchdog or partial
artifact is failure, never a completed workload. Focused runs do not enlarge
these budgets. Provider events are bounded at 65,536 per backend; broker events
at 65,536 per case. Any drops prevent PASS. Request/response inspection is bounded
at one MiB to accommodate B64 batches without publishing SQL, arguments or bodies.

Only allowlisted JSON artifacts are public: identities/content hashes, safe error
classes, per-call latency, provider statement/HTTP counts and transport timing,
EOF/Close/late/drop counters, DBStats deltas, final metrics and cleanup evidence.
No URL, credential, lock token, SQL text, raw error, header or message body is
published. Default logging is suppressed; raw logs remain private on ephemeral
runners. Proxy configuration is a presence boolean only. Instrumentation copies
bounded bytes, so timings are not a zero-overhead benchmark. TTFB combines network
and service work; provider connection wait is not SQL-pool wait. Concurrent and
background provider events have case/phase correlation, not invented operation
attribution or summed private engine phases.

The manual-only registered workflow fixes source identity, Go 1.27.3 and SDK
`316b01c`, verifies frozen-base ancestry and all 70 production non-test Go files against
reviewed product commit `5d71a14`, uses contents-read permissions and the
shared `turso-verification-db` concurrency group without cancellation. It does
not deploy. Do not substitute destructive nightly/soak suites for this command.

## Hermetic commands

Run in this directory with the pinned adjacent `sdk-src` checkout:

```sh
go test -race -tags mqlite_perf_diag -count=1 -timeout=240s ./...
go vet -tags mqlite_perf_diag ./...
go run -tags mqlite_perf_diag . --validate-selection-only --mode mixed --case mixed_native_lock3
go run -tags mqlite_perf_diag . --backend local --mode mixed --out /tmp/mqlite-mixed-public
```
