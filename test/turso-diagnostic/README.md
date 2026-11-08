# Finite Turso performance attribution

This nested diagnostic module is opt-in (`mqlite_perf_diag`). It does not change
the product's ordinary build or add the remote-only SDK to the main module.
It compares a local SQLite control with the two explicitly approved test
databases, using the same source, Go version, payload and operation budgets.
It does not certify production capacity, a soak, managed `Receiver` behavior,
power-loss durability, or all previous functional acceptance criteria.

## Safety and reproducibility

- Remote access is possible only through the `MQLITE_DIAG_STANDARD_DB` /
  `MQLITE_DIAG_STANDARD_TOKEN` and `MQLITE_DIAG_RUST_DB` /
  `MQLITE_DIAG_RUST_TOKEN` environment variables. Database DSNs must match
  the two fixed SHA-256 allowlist constants. No URL, token or allowlist override
  is accepted through command-line or workflow inputs.
- Any presence of `MQLITE_TEST_DB` or `MQLITE_TEST_DB_RESET_CONFIRM`, including
  an empty value, refuses the run. The command does not run engine `TestMain`,
  drop tables, reset the schema, or delete other queues. Normal `engine.Open`
  still performs its ordinary compatible initialization and startup recovery.
- A manifest lists every exact queue name before the first engine opens. Every
  queue has an unpredictable run namespace. Cleanup accepts only an exact
  `(backend, case, queue)` entry in this manifest, force-deletes that queue,
  requires `Deleted && State == "deleted"`, then verifies queue-not-found by
  readback. A failed test stays failed after cleanup. Cleanup can discard this
  run's retained messages and never makes unknown outcomes successful.
  An output directory containing prior run/preflight evidence is refused, so a
  rerun cannot overwrite an earlier FAIL with a later PASS.
- SQLite, standard Turso and Rust Turso run sequentially. Every case closes its
  HTTP server and engine before the next owner opens. An outer timeout, unjoined
  request, driver or Close blocks all remaining owners. A pending send result is
  recorded as pending; it is not called rejected or acknowledged.
  The pinned driver also starts private close-only HTTP goroutines. After Close,
  remote cases require 200ms of unchanged HTTP counters with no active request
  or open body, bounded by ten seconds. `owner_released` means the engine Close
  returned and this observable HTTP barrier passed; it **does not prove that
  private driver goroutines were joined**. That limitation is explicit in the
  artifact. A failed quiet barrier blocks subsequent owners.
- Each SDK API call keeps a **180 second caller deadline**. A case has a seven
  minute outer watchdog; each backend has an eighteen minute outer budget.
  Cancellation gets a bounded ten second join. A stuck process is also bounded
  by the Actions process/job watchdogs. Shortening product retries or deadlines
  to make a run pass is outside this diagnostic.
  Long all-case runs can exhaust that backend budget. Follow-up attribution
  should dispatch an exact case, rather than enlarge the budget or reinterpret
  a canceled case as a completed workload.
- The workflow is manual-only, contents-read, does not persist checkout
  credentials, fixes Go 1.27.3 and SDK commit `316b01c`, verifies the exact
  workflow source and ancestry of frozen base `e27f4ce`, and uses the existing
  `turso-verification-db` concurrency group with cancellation disabled. This
  serializes Actions users of that group; external owners still need coordination.

## Modes

SQLite runs first. `--backend standard`, `rust`, or `both` selects the dedicated
remote comparisons. `--backend local` is hermetic and uses only fresh temporary
SQLite files. It is not exposed as a dispatch choice.

`--case all` is the default and retains the original case order and settings.
`--case <exact-id>` runs only that case from the selected mode; unknown IDs,
cross-mode IDs, empty values and whitespace/list variants are refused before
the first engine opens. The manifest contains only the selected queues and
`case_selection` in the manifest/report records the scope. A focused PASS is
not acceptance of the omitted profiles. SQLite pool controls remain explicitly
not applicable, including when one is the only selected case.

The manual workflow's `case_id` choices expose all contention case IDs. Use
`mode=contention` for a non-`all` workflow selection. Its early
`--validate-selection-only` call requires no credentials, writes no artifacts
and accesses no database; it rejects an incompatible selection before the
read-only protocol baseline and before the separately scoped interrupted-queue
cleanup. Failure of that exact cleanup blocks every new workload owner.

`quick` runs only the native SDK, through a real loopback HTTP broker. It is a
small attribution experiment, **not acceptance of the original four profiles**:

| Case | Concurrency | Batch / exact messages | Dedup | Background |
|---|---:|---:|---:|---|
| isolated B1 | 1 | 1 / 16 | 1 hour | off |
| isolated B16 | 1 | 16 / 16 | 1 hour | off |
| isolated B64 | 1 | 64 / 64 | 1 hour | off |
| separate dedup-off B16 control | 1 | 16 / 16 | off | off |
| separate provider-delay B16 control | 1 | 16 / 16 | 1 hour | off |

The last case injects 75ms **before each matching provider HTTP request**, only
during send/drain. Actual delay, not-sent requests and network/request time are
reported separately. SQLite has zero provider HTTP and receives no artificial
delay. This control measures HTTP amplification; it is not a weak-network or
failure-recovery certification.

`contention` preserves the V11 performance workload for **both** native and
remote-only SDKs, with a 256-byte payload, one-hour dedup, ten-minute locks,
ordinary background maintenance and the original database pool defaults:

| Original profile | Concurrency | Batch | Exact messages |
|---|---:|---:|---:|
| C1/B1 | 1 | 1 | 16 |
| C1/B64 | 1 | 64 | 64 |
| C8/B1 | 8 | 1 | 32 |
| C32/B1 | 32 | 1 | 32 |

The original send-then-drain structure, each worker's empty Receive, full-content
oracle, all failures and caller deadline remain. This is low-level
`Receive` plus real client `CompleteBatch` acknowledgement, **not managed
`Receiver.Run` throughput**. Additional native C32/B1 pool-1, pool-4 and
admission-1 counterfactuals have separate case IDs and cannot replace a failed
original. Pool-limit controls are explicitly not applicable to local SQLite;
its single-writer invariant is preserved.

## Evidence and interpretation

The first case of each backend records two fixed `SELECT 1` warmup calls and
five measured calls outside setup and workload timing. Open, setup, send, drain
and Close are separate. The workflow also runs a separate read-only protocol
baseline before any MQLite owner; any baseline failure stops the later workload.

Every operation retains an exact, bounded sample, an opaque ID and a safe error
class. Quantiles use the documented floor-rank method over all samples; they
are not clipped at 100ms. A loopback-only diagnostic header maps API operations
to tag-only engine contexts. It is not a product or provider request header.
Provider HTTP observations are correlated to case/phase; simultaneous worker
and background calls are **not** assigned a fabricated exact operation ID.
Instrumentation adds callbacks and bounded copying, so these diagnostic timings
are not a zero-overhead production benchmark.

The public artifact contains:

- an identity ledger with acknowledged and observed sequences, send result,
  receive count, content hash and real per-item client settlement result;
- explicit unknown, rejected, pending, missing, duplicate, corrupt, unsettled and
  DLQ counts, final stats/readback and exact-queue cleanup evidence;
- API latency, provider HTTP counts, connection reuse, DNS/TCP/TLS/TTFB/body and
  separately measured synthetic delay; EOF, Close, active bodies, late completion
  and dropped trace samples remain visible;
- engine attempt, BEGIN, statement, callback, commit-mutex, COMMIT, rollback and
  total spans, plus before/after local `database/sql.DBStats` counters.

Provider observations have a finite 65,536-event bound per backend. If it is
exhausted, the dropped count still makes instrumentation incomplete and prevents
a PASS. The larger bound and focused runs avoid repeating the prior 8,192-event
overflow; they do not retroactively validate that earlier incomplete trace.

`DBStats.WaitDuration` is an aggregate workload pool counter. HTTP GetConn wait
is a different measure. `begin_total` includes pool acquisition plus driver
BEGIN. SQL/row lifetime, transaction and total spans overlap and **must not be
summed as disjoint service stages**. TTFB includes network and server processing;
it is not pure network RTT. Dedup `no_rows` is an expected lookup result, not
automatically an operation failure. The public Observability API is not polled
inside timing windows because it would add database queries.

`passed` requires the exact requested identity count, all positive Send ACKs,
one intact delivery per identity, every client CompleteBatch item acknowledged,
no operation/unknown/accounting failure, empty final queue including DLQ,
joined owner, complete tracing and confirmed cleanup. No later control or
successful retry rewrites an earlier failed case. Missing/partial artifact data
or a watchdog exit must not be interpreted as success.

Only the fixed public JSON directory is uploaded. Default logs are suppressed,
and unfiltered process logs stay private on the ephemeral runner. Artifacts never
contain provider URLs, credentials, SQL text, headers, response bodies, raw error
strings, message bodies or local secret files. Proxy configuration is recorded
only as a presence boolean. The test does not change proxy or TLS configuration.

## Hermetic commands

Run from this directory with the adjacent `sdk-src` checkout at the pinned SDK
commit. These commands do not need remote credentials:

```sh
go test -race -tags mqlite_perf_diag -count=1 -timeout=240s ./...
go vet -tags mqlite_perf_diag ./...
go run -tags mqlite_perf_diag . --backend local --mode quick --out /tmp/mqlite-diag-public
go run -tags mqlite_perf_diag . --validate-selection-only --mode contention --case original_native_c32_b1
go run -tags mqlite_perf_diag . --backend local --mode contention --case original_native_c32_b1 --out /tmp/mqlite-diag-focused-public
```

Live execution is maintainer-controlled through the manual workflow on this
test branch. Do not dispatch main's ordinary nightly or soak workflow as a
substitute: those use the destructive integration harness.
