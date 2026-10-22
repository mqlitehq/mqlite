# mqlite benchmarks, sizing & tuning

Measured, reproducible numbers for the mqlite engine across realistic workloads —
**not** a single throughput figure — plus what they mean for **deployment sizing** and
**tuning**. The suite exercises throughput, write amplification, **whole-process memory
+ reclamation**, **DB-file bloat vs reclamation**, **KV-property (enriched) messages**, a
**body-size sweep**, and **load ramp / consumer churn**. Run **locally** (a fast
multi-core box) and on a deliberately tiny **cloud** box so the two tell different truths.

> Philosophy: measure before claiming. Each result below names its harness and
> environment; committed harnesses include `test/bench/` and native Go benchmarks.
> These are historical workload measurements, not v0.3.2 capacity guarantees.
> Recheck the current binary
> with your message sizes, concurrency, queue count and monitoring load before
> selecting a memory or CPU limit.

## Sizing & deployment (Fly.io)

The historical sample below ran a single broker on a 256 MB Fly machine. Treat
these measurements as starting points for a workload-specific sizing test; they
do not establish a safe backlog threshold or the current release image size.

```
┌─────────────┬──────────────────────────┬──────────────────────────────────────┐
│ resource    │ measured (1 broker)      │ Fly choice                            │
├─────────────┼──────────────────────────┼──────────────────────────────────────┤
│ memory      │ 19 MB idle               │ 256 MB used in the historical test   │
│             │ 38 MB @ 50k msgs queued  │ validate current peak memory first   │
│             │                          │ no universal backlog threshold        │
│ disk        │ ~0.4 KB / 256 B message  │ 1 GB volume  (~1.1M planned msgs)     │
│             │ + ~4 MB WAL (constant)   │   size = backlog × 0.6 KB × 1.5       │
│ vCPU        │ tiny/op; 1000s/s batched │ 1 shared vCPU in the measured setup   │
│ image       │ 10.9 MiB static binary   │ distroless/scratch ≈ 15 MB image,     │
│             │   (CGO-free)             │   cold start < 1 s (scale-to-zero ok) │
└─────────────┴──────────────────────────┴──────────────────────────────────────┘
```

**Topology:** mqlite is **single-writer**: run one broker or embedded `Engine` per
database. A local process holds a file lock, so run one machine per volume and do
not horizontally scale several machines onto the same SQLite file. Turso/libSQL
provides managed remote storage, but it does not make overlapping brokers safe; see
[turso.md](turso.md).

Disk is sized by **peak backlog depth**, not lifetime throughput (`Complete`/`Purge`
delete rows); files plateau at the high-water mark, and a background `incremental_vacuum`
returns freed pages to the OS over time (see §3):

| backlog depth (256 B msgs) | DB on disk | Fly volume |
|--------------:|-----------:|:-----------|
| 100k          | ~40 MB     | 1 GB       |
| 1M            | ~0.4 GB    | 1 GB       |
| 5M            | ~2 GB      | 3 GB       |

## Environments

| | Local (baseline) | Cloud (constrained) |
|---|---|---|
| Where | Docker (`run-bench.sh`) on the dev machine | Fly.io app `mqlite-bench`, region `sin` |
| CPU | 12 vCPU, arm64 (Apple Silicon, linuxkit VM) | **shared-cpu-1x → `GOMAXPROCS=1`** (a *fraction* of one amd64 core, CPU-steal) |
| Memory | 8 GB | **256 MB** (≈207 MB usable) |
| Disk | overlayfs in the Docker VM | machine rootfs **overlayfs** (no volume mounted for the bench) |
| Probe | `/proc/self/{io,stat}` + 200 ms RSS/heap/file sampler | same binary, same matrix |

Honesty notes: the local host is arm64 under Docker Desktop's linuxkit VM, so its
12 cores flatter parallel scenarios and the disk is virtualised. The Fly box is the
**single-fractional-core reality** of the cheapest deploy. Disk write-bytes are
measured via `/proc/self/io` on both; absolute disk latency is not the point here —
**relative** write amplification and the memory/file behaviour are, and those match
across platforms.

## How to reproduce

```bash
# local — builds the bench image, runs the matrix on the container's own fs
BENCH_DUR=3s ./test/bench/run-bench.sh          # → test/bench/out/results.json

# opt-in queue create/send/force-delete/converge churn
BENCH_ONLY=lifecycle_churn BENCH_DUR=2s ./test/bench/run-bench.sh

# cloud — Fly remote-builds test/bench/Dockerfile, runs once on a 256MB machine
cd mqlite && FLY_API_TOKEN=... flyctl deploy . \
  --config ../deploy-fly/bench.toml --dockerfile test/bench/Dockerfile \
  --app mqlite-bench --ha=false --wait-timeout 30 --yes
flyctl logs --app mqlite-bench          # [done] lines + tables ; then destroy the app
```

Each scenario runs `dur` seconds (here 3s), then an **idle + GC + FreeOSMemory**
phase so we can measure what is *reclaimed*. A 200 ms background sampler records
RSS + Go heap + DB/WAL file sizes the whole time; each scenario prints a `[done]`
line as it finishes (so a constrained box that runs out of time still yields the
scenarios that completed).

The runner fails closed on workload operation errors: fixed-volume drains must match
their exact prefill, mixed workloads reconcile accepted messages with completed plus
remaining messages, and result-file failures produce a nonzero exit. For remote `-db`
runs, use a dedicated non-production database. Each process uses a unique queue
namespace, force-deletes only that namespace after every scenario, and drives bounded
cleanup; an interrupted process can still leave its private namespace behind.

The opt-in `lifecycle_churn` scenario uses a fixed set of 64 queue names and never
reuses a name until deletion has converged. Each cycle creates the queues, sends
small batches, accepts forced deletion, and runs bounded maintenance until every
name is absent. Four queues carry 1,024 messages so the test covers pending cleanup;
the other 60 carry 16 messages. The live message-body payload stays below 1 MiB.
Its result reports immediate and pending acknowledgements, maintenance passes,
convergence latency, peak payload, and DB/WAL size. Here `ops/s` means fully
converged queue lifecycles per second; the generic latency columns measure the
initial delete acknowledgement.

A 2-second native force-deletion smoke run on an Apple M1 Pro completed
640 queue lifecycles (10 cycles) at about 312 lifecycles/s. All 640 deletions were
accepted, 40 required background-style cleanup, convergence p99/max was
42.9/43.0 ms, and DB+WAL peaked at 5.7 MiB before ending at 0.8 MiB. This short run
is a regression signal, not a deployment capacity claim; reproduce it on the target
CPU, filesystem, durability mode, message sizes, and remote backend when applicable.

Native Go benchmarks cover topic fan-out, subscription create/empty-delete churn,
and exact-total subscription pagination:

```bash
go test ./engine -run '^$' \
  -bench 'Benchmark(TopicFanout|SubscriptionCreateDelete|ListSubscriptionsPage)' \
  -benchmem -count=5
```

On an Apple M1 Pro with Go 1.26.9, one recorded run measured
382.402 µs per create/delete lifecycle with 100 retained subscriptions and
389.064 µs with 10,000 retained subscriptions (1.017×). A 100-item page with
an exact total measured 0.660 ms at 1,000 subscriptions and 5.245 ms at 10,000.
A smoke with 5,000 queued messages acknowledged force deletion in 4.100 ms and
converged in one 30.707 ms maintenance pass. These control-plane measurements
found no strong lifecycle regression, but the exact count scales with inventory;
do not treat the 10,000-subscription safety ceiling as a low-latency guarantee.

The native suite also covers fixed-lifetime fan-out and indexed subscription-expiry
passes:

```bash
go test ./engine -run '^$' \
  -bench 'Benchmark(TopicFanoutTTL32|SubscriptionExpiry)' \
  -benchmem -count=5
```

On the same Apple M1 Pro with Go 1.26.9, seven alternating one-second rounds
compared the fixed-lifetime implementation with the same suite before TTL support. Median latency
changed by +6.52% for `SendOne`, +0.67% for `SendBatch64`, +1.14% for 32-way
topic fan-out, +0.31% for 256-way fan-out, and -0.28% for receive plus complete.
All remained below the 10% investigation threshold. The 32-way fixed-lifetime
fan-out median was 1.456 ms; it added 32 allocations and 257 bytes per publish
relative to permanent subscriptions in the same run, with no measured latency
penalty beyond host variation.

An indexed expiry scan over 10,000 future deadlines took 63.468 µs per no-work
pass (15,756 passes/s). Transitioning a full 1,000-subscription due batch took
3.562 ms (280,749 subscriptions/s). A controlled burst created 10,000
subscriptions with one 5,000-message backlog, then advanced them to one common
deadline. Data-plane fencing was immediate; 19 manually driven bounded passes
discarded the backlog and removed all metadata in 338 ms. With normal background
workers, 10,000 empty subscriptions converged in 1.292 s. A permanent queue had
no send errors during cleanup; its send p50 stayed near 0.1 ms, while p99 rose
from 0.687 ms before expiry to 12.003 ms during the burst (34.499 ms maximum).
This is the expected bounded tail-latency cost of many simultaneous expirations,
not a capacity promise; stagger deadlines and load-test the target backend.

Adding required lifecycle timestamps to 100-item subscription pages changed the
1,000-item inventory median from 0.658 ms to 0.717 ms (+8.87%) and the 10,000-item
median from 5.187 ms to 5.376 ms (+3.65%). Page memory fell from about 39 KB to
30 KB after exact preallocation; allocation counts stayed effectively flat.

## Observation cost under a deep current backlog

The native observation benchmarks use 300 queues with 300,000 messages retained at
the same time. This is current backlog, not cumulative broker throughput: completed
messages normally leave the table when settled. Run the committed fixtures with:

```bash
go test ./engine -run '^$' \
  -bench 'Benchmark(ObservationDeepBacklog|SendDuringDeepObservation)$' \
  -benchmem -count=3
```

Per-queue `Stats` uses state-specific indexes. Global `Observe` must still scan the
retained rows when its roughly 10-second shared cache needs a refresh; cache hits
still allocate and serialize the full queue response. The deliberately hostile
`continuous_observe` benchmark calls observation as fast as possible and is a
stress case, not a recommended scrape interval.

The final aggregate query replaced per-state `SUM` expressions with selective
`COUNT ... FILTER` aggregates while keeping one grouped scan. On an Apple M1 Pro
with Go 1.26.9, three measured refreshes in each of three runs produced:

| Current retained distribution | Earlier `SUM` median | Current `FILTER` median | Median change |
|---|---:|---:|---:|
| 300,000 active | 248.369 ms | 210.754 ms | -15.14% |
| 50,000 in each of six states | 244.973 ms | 210.518 ms | -14.06% |

This isolates aggregate-expression cost; it is not a deployment capacity result.
The refresh remains proportional to retained rows and competes for CPU, page cache,
storage bandwidth, and WAL checkpoint headroom even though a local file uses a
separate read-only observation connection.

A five-pair local-file pressure run compared baseline and monitored 20-second
phases at 1,000 scheduled sends/s. Each monitored phase made the expected global
refreshes and per-queue calls; every admitted message completed and each phase
ended with zero backlog. The preselected paired-median 10% investigation threshold
failed for every foreground p99 measure:

| Measure | Paired-median monitored change |
|---|---:|
| Throughput | -1.06% |
| Dropped offered slots | +1.05 percentage points |
| Send p99 | +18.50% |
| Receive p99 | +14.80% |
| Complete p99 | +53.03% |
| End-to-end p99 | +154.12% |

Host variation was material and individual pairs moved in both directions, so the
run supports stable throughput and completed-work accounting at that offered load.
It does **not** show that monitoring has no foreground tail-latency cost. A separate
500 sends/s run also contained one severe adverse window, so it did not establish a
tail-latency-safe operating point.

Monitored WAL peaks were 4.32–5.34 times their paired baselines in the 1,000/s run.
After each phase, a passive checkpoint reported no busy reader and all frames
checkpointed; a following truncating checkpoint left a zero-byte WAL. Observation
can therefore cause transient WAL growth even when the file is reclaimable after
the reader finishes. Start production scraping at 15 seconds, then measure refresh
duration, WAL growth, and foreground p99 with the target backlog and hardware.

## Remote Turso trend soak

The manually dispatched **Turso performance soak** Action exercises one
package-internal Engine directly against the dedicated verification database.
It is separate from the HTTP broker and uses four closed-loop lanes: two standard
queues, one `group_fifo` queue and one `strict_fifo` queue. Each lane repeatedly
sends one unique 256-byte body, receives and verifies that exact body, then
completes it before starting the next cycle. This keeps the live message working
set bounded while exercising normal background maintenance.

The four logical lanes share a fixed admission limit of one harness database
call in flight (`foreground_max_in_flight=1` in `metadata.json`). Their initial
starts use nominal 0, 250, 500 and 750 ms offsets, recorded as a 250 ms lane-start
stagger in the metadata. Against the four-connection remote pool, this avoids a
synchronized startup burst and keeps foreground call demand below the pool limit,
providing headroom for normal background maintenance. The profile is deliberately
a sustainable stability and trend load. It is not a capacity test, and its
throughput must not be read as the backend's maximum.

The admission limit applies only to calls made by the harness. Engine background
workers bypass it, and it does not reserve the Turso primary's serialized write
lock.

Choose 2, 10 or 30 minutes when dispatching the workflow. Start with 2 minutes to
verify credentials, cleanup and evidence collection; use 30 minutes for the
intended sustained trend check. The workflow and the existing Turso nightly suite
share one repository-wide concurrency lock because both use `TestMain` to reset
the same database. Both workflows have a fail-closed allowlist for the
verification host before tests can reset it. Never point either workflow at a
production database.

Each minute, and once more for a final partial window, the artifact records
per-lane and overall cycles/s; Send, Receive, Complete and end-to-end p50/p95/p99
and maximum latency; empty/error counts; queue backlog; settlement-receipt
counts; storage outcomes and retries; pool waits; background-maintenance pass
state; Go heap, RSS, goroutines, GC and process CPU where the runner exposes them.
The maintenance snapshot is copied from process memory and does not issue an
additional remote database query. `metadata.json`, `windows.jsonl`,
`summary.json`, `summary.md` and the test log are retained for 14 days. Errors,
body mismatches, accounting failures, final backlog or exact cleanup failures
fail the run, while the evidence written before a failure remains uploadable.

A single non-final `Stats` or settlement-receipt-count sample that fails with the
`connection` category is recorded as a gap and may recover at the next scheduled
sample; the harness does not issue an extra query to force recovery. During that
gap, the overall and per-lane backlog and settlement-receipt fields are `null`,
and older values are not presented as final state. A second monitoring failure
anywhere in the run, any other operation or error category (including a
working-set invariant), or a failed final sample fails the soak. Each window
records structured monitoring status, and the summary records the failed-sample
count and recovery status.

A single failed maintenance pass may recover on that task's next complete pass.
Two consecutive failed passes, or one still unrecovered at the end of the run,
fail the soak. Recovered failures remain visible in the cumulative maintenance
counters and bounded diagnostic history. The final maintenance judgement is a
process-memory snapshot taken after `Engine.Close` has stopped and joined the
background workers; the harness does not trigger an extra maintenance pass to
change the result. Thus the last `windows.jsonl` entry is the pre-cleanup workload
sample, while `summary.json.final_maintenance` is the post-close terminal state.
A runner cancellation or outer test timeout can interrupt final cleanup; the
next run's `TestMain` reset and namespaced startup cleanup remove that residue.

The first-to-last window change is descriptive and has no performance threshold
because GitHub-hosted runner placement, shared CPU and network path add noise.
Turso does not expose physical database or WAL file sizes through this client;
those JSON fields are `null`, never a misleading zero. Settlement receipts have
30-minute retention, so a 30-minute run's final receipt count is a current
retention snapshot rather than a lifetime completion total.

## 1 · Throughput & write amplification

256 B bodies unless noted. `wB/op` = bytes physically written to disk per op (write
amplification). **The local box is 12 fast cores; the cloud box is one fraction of a
throttled core** — read the two columns as "engine character" vs "cheapest real box".

| scenario | sync | local ops/s | cloud ops/s | local wB/op | cloud wB/op |
|---|---|---:|---:|---:|---:|
| produce_p1 | NORMAL | 15,926 | **4,000** | 9,740 | 9,696 |
| produce_p4 | NORMAL | 17,449 | **567** | 9,744 | 9,667 |
| produce_p8 | NORMAL | 17,963 | **322** | 9,745 | 9,635 |
| batch_16_p4 | NORMAL | 30,748 | 1,600 | 1,979 | 1,928 |
| batch_64_p4 | NORMAL | 35,146 | **6,000** | 1,090 | 1,045 |
| e2e_4x4 | NORMAL | 4,413 | 197 | 58,539 | 55,809 |
| sessions_64g | NORMAL | 3,789 | 164 | 71,016 | 67,794 |
| produce_p4_FULL | **FULL** | 1,556 | 468 | 13,743 | 13,682 |
| props_p4 | NORMAL | 15,909 | 1,000 | 10,577 | 10,493 |
| props_batch64 | NORMAL | 24,161 | **6,700** | 1,641 | 1,622 |

**Write amplification is platform-independent** — every cloud `wB/op` is within ~3 %
of local. It's a property of SQLite + WAL (a 256 B message costs ~9.7 KB written
single, ~1.1 KB batched), not of the host. That makes capacity planning portable.

**On a single fractional core, concurrency *hurts*** — `produce_p1` 4.0k >
`produce_p4` 567 > `produce_p8` 322. With `GOMAXPROCS=1` there's no parallelism to
exploit, so more producers just add lock contention + scheduler/GC thrash on the one
writer. On the cheap box the right pattern is **one producer that batches**, not many
concurrent single-sends.

**Batching is the great equaliser** — `batch_64` does 6.0k/s on the *same* throttled
core that only manages 567/s of single `produce_p4` (≈10×), at ~1/9 the write
amplification. Enriched batched sends (`props_batch64`) hold 6.7k/s.

**FULL durability** costs ~11× locally; on the already-CPU-bound cloud box the gap
narrows (468 vs 567) because fsync isn't the bottleneck there — the core is.

## 2 · Memory footprint after a short idle

Whole-process **RSS** (not just Go heap), peak during load vs after the harness's
short 1.9-second idle and forced GC. This end snapshot describes immediate process
state; it does not imply that the 60-second storage janitor has run.

| scenario | local peak → short idle | cloud peak → short idle |
|---|---:|---:|
| produce_p8 | 34 → 27 MB | 27 → 20 MB |
| batch_64_p4 | 32 → 27 MB | 28 → 23 MB |
| props_batch64 | 33 → 29 MB | 29 → 25 MB |
| size_16KB | 33 → 29 MB | 27 → 23 MB |
| drain_4c (200k msgs) | 33 → 28 MB | — (see §7) |

**Peak RSS was 27–34 MB across the scenarios listed above.** The local 200k-message
drain and 16 KB body scenarios stayed within that measured range. SQLite stores
the backlog on disk, but this does not bound all broker memory. RSS declined after
load stopped; no sustained growth was observed in these runs. Larger messages,
more concurrent requests or queues, and monitoring can change the footprint.
Measure the current workload before choosing a memory limit.

## 3 · DB-file footprint and reclamation

`bloat_50k` now inserts exactly 50,000 messages once, samples the full backlog,
drains exactly that count with `Complete`, and rejects any send, receive, settle,
or accounting error. `file_bytes_per_msg` uses the count actually accepted during
prefill. Older results from this scenario are not comparable: the previous harness
prefilled twice, rounded each tail batch up, and still divided file size by 50,000.

The scenario's `db_final_mb` is the immediate state after the same 1.9-second idle
used by the other scenarios. It is useful for comparing the peak with the drained
file, but it does not verify background page reclamation. Local file databases use
`auto_vacuum=INCREMENTAL`; the background janitor checks every 60 seconds and
reclaims a bounded number of pages once its freelist threshold is met. Use a run
that spans that interval, or measure an explicit `Compact(false)`, when evaluating
reclamation. Remote Turso manages its own storage.

## 4 · Enriched messages — KV properties

`props_*` send a realistic message: a `Properties` map (tenant / trace_id /
event_type / priority / source / schema_ver) plus CorrelationID, Subject,
ContentType — "normal usage", vs a bare body.

| scenario | local ops/s | fileB/msg | vs bare |
|---|---:|---:|---|
| produce_p4 (bare) | 17,449 | 415 | — |
| props_p4 | 15,909 | 693 | +280 B/msg on disk, ~9 % slower |
| batch_64_p4 (bare) | 35,146 | 373 | — |
| props_batch64 | 24,161 | 664 | +291 B/msg, ~31 % slower batched |

Properties are cheap on a single send (~9 % throughput, +280 B/msg) but cost more when
batched (~31 %): once commit overhead is amortised away, the per-message JSON-encode of
the KV map becomes the dominant work.

## 5 · Body-size sweep

4 producers, NORMAL, body 64 B → 16 KB:

| body | local ops/s | cloud ops/s | wB/op (≈both) | fileB/msg |
|---|---:|---:|---:|---:|
| 64 B | 17,205 | 1,200 | 9,122 | 217 |
| 256 B | 17,449 | 567 | 9,744 | 415 |
| 1 KB | 13,991 | 912 | 12,980 | 1,489 |
| 4 KB | 12,547 | 454 | 22,205 | 4,740 |
| 16 KB | 7,988 | 259 | 46,939 | 17,091 |

On-disk storage scales ~linearly with body size plus a small fixed row+index overhead
(~150–250 B). Throughput is commit-bound up to ~256 B, then byte-bound for large
bodies. Default body cap is 1 MiB (`MaxMessageBytes`).

## 6 · Load ramp & consumer churn (local)

- **rampdown_4x4**: produce hard, then stop producing while consumers drain to idle.
  RSS peak 31 MB → 27 MB after the load goes away — memory tracks the workload *down*,
  not a one-way ratchet.
- **churn_consumers**: producers steady while consumer cohorts repeatedly go
  **online/offline**. RSS stays ~28–34 MB across cycles with **no stair-step growth** —
  connect/disconnect doesn't leak.

## 7 · Cloud reality — what a 256 MB fractional core changes

The Fly `shared-cpu-1x` is `GOMAXPROCS=1` with CPU-steal. Versus the 12-core local box:

1. **Memory stayed low in the measured scenarios.** Peak RSS was 27–29 MB for
   the cloud scenarios in §2, with reclamation after load. This does not establish
   the memory required by other workloads or current releases.
2. **Write amplification: no change.** Within 3 % of local everywhere — portable
   capacity planning.
3. **Throughput: core-bound, and concurrency backfires.** Single-send multi-producer
   collapses (567/s at p4, 322/s at p8) because there's no second core. **Batch, and
   throughput recovers to thousands/s** (6.0–6.7k).
4. **Tail latency: throttling shows up.** p99 hits the 100 ms histogram ceiling on most
   multi-goroutine scenarios (CPU-steal stalls); single-producer scenarios keep p99
   under ~5 ms.

**The bottom line for the cheapest box:** **with batching, ~6k msg/s sustained** on a
shared-cpu-1x; **without batching (concurrent single sends), only hundreds/s.** The
"thousands per second" figure holds *only when* the client batches.

## Tuning knobs

mqlite ships sensible defaults; most deployments change nothing. In rough order of impact:

| knob | default | when to change |
|---|---|---|
| **batch size** (`messages:[…]`) | — | the biggest lever — ~9× less write amplification, ~2× throughput. Batch whenever you can. |
| **`MQLITE_SYNC`** | `NORMAL` | `FULL` only if a power cut losing the last few commits is unacceptable; it costs ~11–18× on single sends, so pair it with batching. |
| `wal_autocheckpoint` | 1000 pages (~4 MB) | matches the observed WAL plateau; raise for fewer checkpoints at the cost of a larger WAL + slower crash recovery. |
| `cache_size` | ~2 MB | the working set (queue heads + indexes) is small; raise only after profiling shows cache pressure under deep backlogs — it costs RAM on a 256 MB machine. |
| `mmap_size` | off | `modernc.org/sqlite` is pure-Go; don't assume C-SQLite mmap gains — measure first. |
| writer/provider pool | local writer 1 / remote provider 4 | **don't change** — the local writer pool stays at one for atomic claims; local files also have one read-only observation connection. The remote pool is scoped to one broker and Turso serializes commits. |

`busy_timeout=5000` and `temp_store=MEMORY` are set on local modernc SQLite
connections. They do not apply to remote Turso/libSQL connections, whose transient
contention uses the bounded retry policy described in [turso.md](turso.md).

## Findings summary

1. **Low measured memory with reclamation** — peak RSS was 27–34 MB in the
   scenarios in §2. No sustained growth was observed during the recorded churn
   and ramp-down tests; validate the current workload separately.
2. **Batch to go fast** — ~9× less write amplification, and on a single core the
   difference between *hundreds/s* and *thousands/s*.
3. **Write amplification is a portable constant** (~9.7 KB/op single, ~1.1 KB batched)
   — same on arm64 local and amd64 cloud.
4. **Single-writer model**: more producers ≠ more write throughput; on one core they
   make it *worse*. Prefer one batching producer.
5. **Files plateau at the peak backlog**, then a background `incremental_vacuum` returns
   freed pages to the OS over time; size disk for the peak, not the average.
6. **NORMAL vs FULL** is an ~11× durability dial (local); pick per host failure model.
7. **Properties are affordable**; large bodies are byte-bound and storage-linear.

The historical source artifacts were captured in `test/bench/out/results.json`
and the `mqlite-bench` Fly logs; they are not shipped in this repository. Treat
the table as historical evidence and rerun the commands above before using the
numbers for a current deployment decision.
