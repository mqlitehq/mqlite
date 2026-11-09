<!-- External current-candidate rebind; no DB run or workflow dispatch. -->

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
SDK error callback, duplicate or DLQ. On remote backends a successful Renew must
start while the loopback broker has already received a real new Send and has
not returned that Send's response headers. The same Send must execute a complete,
strictly successful `message_insert` provider request and return its client ACK.
The observer correlates only a unique HTTP request ID validated against the
loopback trace's current in-flight method/path/operation registry, then inherited
through the actual request context into the SQL driver. Missing/unverified
context remains ID zero; unrelated requests and read-only dedup replays cannot
satisfy coverage. Every evidence item records the request ID and relative timing.
This proves a successful renewal coexisted with a pending real write request;
it does not infer private admission state or require INSERT execution to overlap
renewal. Priority renewal may correctly run before that queued Send's INSERT.
The original strict SQL-INSERT/Renew interval intersection remains a secondary
statistic. No correlated witness after the complete workload is a coverage
failure, separate from message integrity. Remote cases that return before the
coverage calculation report `not_evaluated`; they are not labeled as SQLite or
as a measured zero. Historical early-return artifacts retain their raw value;
offline interpretation marks that remote initialization value as unevaluated.
SQLite has no provider HTTP and reports coverage not applicable while still
requiring renewal and full SDK lifecycle/identity acceptance. Successful
Complete responses are counted only
when the SDK reads a complete valid 2xx body; this is explicitly named
`client_observed_complete_response`, not an internal SDK settlement hook.
Receiver cancellation/join, zero error callbacks and empty final metrics are
independent acceptance checks. Explicit low-level client settlement results
remain separate in the ledger.

`--case all` keeps each mode's order. An exact case ID selects only that case;
unknown, ambiguous and cross-mode selections fail before database access.
`--mode contention --case originals` selects exactly the original eight native
and low-floor profiles in their existing order. It omits only the three separate
pool/admission controls; `all` continues to select all eleven. No original
profile's message count, B64 batch size, C8/C32 concurrency, SDK, deadline,
backend budget or cleanup rules change. `originals` is rejected in other modes,
and every other group name is rejected.
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
and service work; provider connection wait is not SQL-pool wait. A nonzero
`broker_request_id` proves only the verified context association described above.
Events without that context (including background work) retain case/phase
correlation; no transaction identity, private queue order or engine phases are
invented.

The manual-only registered workflow fixes source identity, Go 1.27.3 and SDK
`93002d0`, verifies frozen-base ancestry and all 89 production non-test Go files against
reviewed product commit `9591bc5`, uses contents-read permissions and the
shared `turso-verification-db` concurrency group without cancellation. It does
not deploy. Do not substitute destructive nightly/soak suites for this command.

## Hermetic commands

Run in this directory with the pinned adjacent `sdk-source` checkout:

```sh
go test -race -tags mqlite_perf_diag -count=1 -timeout=240s ./...
go vet -tags mqlite_perf_diag ./...
go run -tags mqlite_perf_diag . --validate-selection-only --mode mixed --case mixed_native_lock3
go run -tags mqlite_perf_diag . --backend local --mode mixed --out /tmp/mqlite-mixed-public
```

The additional synthetic latency reproduction is isolated behind a second build
tag. It does not join the default 240-second controls or change their coverage.
The unchanged real SQLite/Hrana fixture supplies 200ms per provider HTTP call;
a separate wrapper adds 100ms only before renewal UPDATE requests. HTTP/TTFB
include both synthetic components, and cancellation during the wrapper wait can
prevent that SQL request from reaching the fixture. This vector is a scheduling
stress case, not a measurement of network latency. Its full mixed vector retains
the original three-second leases, four seed handlers, six-second seed work, 68 identities,
four B16 Sends, concurrency four, background maintenance and all outcome checks.
A separate seed-only vector omits the 64 new messages to isolate lease handoff.
Both SDK cases retain their seven-minute case budgets. The 16-minute Go test
watchdog bounds this explicit local two-case reproduction; it does not enlarge
the remote command's seven-minute case or eighteen-minute backend budget.

```sh
go test -race -tags 'mqlite_perf_diag mqlite_latency_repro' -count=1 \
  -run '^TestHranaMixed68RenewDelayVectorThreeSecondLease$' -timeout=16m ./...
```

A passing synthetic baseline is preserved as a passing baseline; it is not
rewritten as a failure or used as proof that a later candidate fixed that run.


## Infrastructure finalization boundary

The manual job has a 75-minute infrastructure limit. This does not change the 180-second RPC, seven-minute case, eighteen-minute backend, ten-second owner join, sixty-second cleanup, or forty-minute diagnostic process budgets. Immediately before the diagnostic launch, a monotonic-time gate requires forty minutes plus fifteen seconds of termination allowance and six minutes for final state, artifacts and scheduling margin.

An always-run, credential-free finalizer reads only this run's public process receipt, summary and exact manifest. Missing, partial, failed, timed-out or unconfirmed cleanup evidence produces `BLOCKED_UNCONFIRMED`; no automatic cleanup or retry follows. The historical interrupted-queue cleanup tool runs hermetic controls only. A non-started diagnostic is recorded as `NO_MQLITE_OWNER_STARTED` and never authorizes progression. Even confirmed cleanup requires independent artifact, source, ledger and quiet-barrier review before a separate manual run. These finite cases are not an uninterrupted eight-hour remote soak.


## Coalesced renewal receipt proof (MQLITE-190)

This derivative is pinned to runtime `9591bc5f9b62017705404555f9ad5fa98ea497e0`
and SDK `93002d0d47e2e9e876e8b4f4c8a66c5167f29b10`. It retains the original
managed mixed profiles, identity oracle and Send/INSERT/ACK primary witness.
A whole successful RenewBatch RPC must match one uniquely observed physical
statement using fenced-pair hashes and actual RETURNING deadlines, EOF and Close.
Anonymous single-pair groups are valid; many-to-one is reported only for multiple
verified all-true RPC identities sharing the same proven statement. Incomplete
HTTP200 observations are evidence gaps, never assumed rejections. Complete
explicit protocol errors do not supply successful ACKs or prove rollback.
`business_passed` preserves the original acceptance result; `passed` additionally
requires renewal membership proof for remote mixed cases. SQLite controls mark
that physical proof not applicable. These finite runs do not qualify an eight-hour
remote soak or release. One selected remote backend runs per reviewed dispatch.
