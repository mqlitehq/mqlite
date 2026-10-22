# mqlite benchmarks, sizing & tuning

Measured, reproducible numbers for the mqlite engine across realistic workloads —
**not** a single throughput figure — plus what they mean for **deployment sizing** and
**tuning**. The suite exercises throughput, write amplification, **whole-process memory
+ reclamation**, **DB-file bloat vs reclamation**, **KV-property (enriched) messages**, a
**body-size sweep**, and **load ramp / consumer churn**. Run **locally** (a fast
multi-core box) and on a deliberately tiny **cloud** box so the two tell different truths.

> Philosophy: measure before claiming. Every number here is produced by
> `test/bench/` and can be reproduced with the commands below. These are historical
> workload measurements, not v0.3.2 capacity guarantees. Recheck the current binary
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
│ disk        │ ~0.4 KB / 256 B message  │ 1 GB volume  (~2M messages)           │
│             │ + ~4 MB WAL (constant)   │   size = backlog × 0.6 KB × 1.5       │
│ vCPU        │ tiny/op; 1000s/s batched │ 1 shared vCPU in the measured setup   │
│ image       │ 10.9 MiB static binary   │ distroless/scratch ≈ 15 MB image,     │
│             │   (CGO-free)             │   cold start < 1 s (scale-to-zero ok) │
└─────────────┴──────────────────────────┴──────────────────────────────────────┘
```

**Topology:** mqlite is **single-writer** (one process holds a file lock per DB), so run
**one machine per volume** — do not horizontally scale several machines onto the same
SQLite file. Need HA / multi-region? Point the broker at a Turso/libSQL DSN instead of a
local file ([turso.md](turso.md)).

Disk is sized by **peak backlog depth**, not lifetime throughput (`Complete`/`Purge`
delete rows); files plateau at the high-water mark, and a background `incremental_vacuum`
returns freed pages to the OS over time (MQLITE-53, see §3):

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

The opt-in `lifecycle_churn` scenario uses a fixed set of 64 queue names and never
reuses a name until deletion has converged. Each cycle creates the queues, sends
small batches, accepts forced deletion, and runs bounded maintenance until every
name is absent. Four queues carry 1,024 messages so the test covers pending cleanup;
the other 60 carry 16 messages. The live message-body payload stays below 1 MiB.
Its result reports immediate and pending acknowledgements, maintenance passes,
convergence latency, peak payload, and DB/WAL size. Here `ops/s` means fully
converged queue lifecycles per second; the generic latency columns measure the
initial delete acknowledgement.

A 2-second native smoke run on an Apple M1 Pro while developing MQLITE-126 completed
640 queue lifecycles (10 cycles) at about 312 lifecycles/s. All 640 deletions were
accepted, 40 required background-style cleanup, convergence p99/max was
42.9/43.0 ms, and DB+WAL peaked at 5.7 MiB before ending at 0.8 MiB. This short run
is a regression signal, not a deployment capacity claim; reproduce it on the target
CPU, filesystem, durability mode, message sizes, and remote backend when applicable.

MQLITE-127 also adds native Go benchmarks for topic fan-out, subscription
create/empty-delete churn, and exact-total subscription pagination:

```bash
go test ./engine -run '^$' \
  -bench 'Benchmark(TopicFanout|SubscriptionCreateDelete|ListSubscriptionsPage)' \
  -benchmem -count=5
```

On an Apple M1 Pro with Go 1.26.9, the final PR3 engine candidate measured
382.402 µs per create/delete lifecycle with 100 retained subscriptions and
389.064 µs with 10,000 retained subscriptions (1.017×). A 100-item page with
an exact total measured 0.660 ms at 1,000 subscriptions and 5.245 ms at 10,000.
A smoke with 5,000 queued messages acknowledged force deletion in 4.100 ms and
converged in one 30.707 ms maintenance pass. These control-plane measurements
found no strong lifecycle regression, but the exact count scales with inventory;
do not treat the 10,000-subscription safety ceiling as a low-latency guarantee.

MQLITE-128 extends that native suite with fixed-lifetime fan-out and indexed
subscription-expiry passes:

```bash
go test ./engine -run '^$' \
  -bench 'Benchmark(TopicFanoutTTL32|SubscriptionExpiry)' \
  -benchmem -count=5
```

On the same Apple M1 Pro with Go 1.26.9, seven alternating one-second rounds
compared the final TTL engine with the merged MQLITE-127 baseline. Median latency
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

MQLITE-132 adds native observation benchmarks over 300,000 retained messages in
300 queues:

```bash
go test ./engine -run '^$' \
  -bench 'Benchmark(ObservationDeepBacklog|SendDuringDeepObservation)$' \
  -benchmem -count=3
```

This fixture is 300,000 rows present at the same time, not 300,000 messages
processed over the broker's lifetime. Its original form makes every row active;
completed messages are normally deleted at settlement and do not accumulate as
history.

`ObserveAllQueuesRefresh` forces a new grouped scan on each iteration;
`ObserveAllQueuesCacheHit` prewarms the process-local snapshot. A three-run sample
on an Apple M1 Pro with Go 1.26.9 at commit `0407013` used the exact command above.
Median per-queue `Stats` fell from 21.152 ms on the pre-change engine to 1.716 ms
with the indexed query. The candidate's full-refresh median was 291.638 ms versus
251.412 ms in the earlier baseline run; the 16% difference in separate, noisy
runs needs investigation rather than a no-regression claim. A cache hit took
1.454 ms, about 201 times faster than that refresh. The cached call still builds
the complete response and allocated about 1.1 MB in this 300-queue benchmark.

The send benchmark's `continuous_observe` mode is deliberately hostile: one
goroutine requests observations as fast as it can instead of following a scrape
interval. In the same three-run sample, median send time was 111.542 µs without
that load and 135.724 µs with it (+21.7%); throughput moved from 8,965 to 7,368
messages/s (-17.8%).

An earlier five-pair, 10-second pressure run at the same commit alternated phase
order and used two observation loops (5-second and 15-second cadences) plus
per-second `Stats`. Its paired median p99 change was +0.7% and paired median
throughput change was -2.9%, but the offered load was unbounded and two of five
p99 changes exceeded 10% (+18.7% and +127.9%). This exploratory run does not
establish an acceptance result or per-trial latency bound. Its monitored WAL
peaked between 16.0 and 39.5 MiB. A post-phase passive checkpoint reported
`busy=0` with all logged frames checkpointed in every trial.

A second run measured the exact runtime commit `8722e65` at a fixed offered load.
It used five alternating baseline/monitored pairs, 5 seconds of warmup and 20
seconds of measurement at 1,000 scheduled sends/s. Each monitored phase ran three
per-second `Stats` callers, a 5-second global observer and a 15-second global
observer. The harness cleared the warmup snapshot immediately before measurement
and verified exactly two global refreshes, two distinct sample timestamps, 60
successful `Stats` calls, four successful 5-second observations and two successful
15-second observations per monitored phase. All 20,000 offered slots per phase
were accounted for, every admitted message completed, and every phase ended with
zero backlog.

| Measure | Baseline absolute median | Monitored absolute median | Absolute-median change | Paired-median change |
|---|---:|---:|---:|---:|
| Throughput | 984.6/s | 981.0/s | -0.37% | -1.06% |
| Dropped offered slots | 1.54% | 1.90% | +0.36 pp | +1.05 pp |
| Send p99 | 0.936 ms | 1.000 ms | +6.87% | **+18.50%** |
| Receive p99 | 1.634 ms | 1.842 ms | +12.72% | **+14.80%** |
| Complete p99 | 0.532 ms | 0.728 ms | +36.96% | **+53.03%** |
| End-to-end p99 | 5.791 ms | 5.837 ms | +0.79% | **+154.12%** |

The preselected paired-median 10% investigation threshold was exceeded by all
four foreground p99 measures. Across the five pairs, throughput changes ranged
from -2.21% to +2.64%, drop changes from -2.53 to +2.18 percentage points, Send
p99 changes from -15.08% to +41.82%, Receive from -10.05% to +23.08%, Complete
from -40.03% to +78.14%, and end-to-end from -50.06% to +164.33%. The difference
between the absolute and paired medians shows substantial host variation; it does
not override the failed paired gate. These results support stable throughput and
bounded backlog at this offered load, but they do not support a claim that global
observation has no foreground tail-latency cost.

The harness completed successfully because its workload accounting, refresh
count, error, backlog and WAL-reclamation assertions passed. That `PASS` is not a
performance pass; the paired tail-latency gate above failed.

Baseline WAL peaks were 3.96–3.97 MiB; monitored peaks were 17.14–21.15 MiB,
or 4.32–5.34 times their paired baselines. After every phase, a passive checkpoint
reported `busy=0` with all frames checkpointed, and the following truncating
checkpoint left a zero-byte WAL. Long-lived observation snapshots can therefore
cause transient WAL expansion even though the file was reclaimable after each
reader finished in this run.

Cache hits and a separate reader reduce writer-connection interference, but a
global refresh still scans the deep backlog and shares CPU, page cache and storage
with foreground work. Size and scrape against the target CPU, storage, queue
inventory and backlog, and measure foreground p99 on the deployment host.

The same fixed-phase harness also characterized a bounded 500 scheduled sends/s.
Its five-pair result supports a throughput/backlog range only; it does not replace
the failed 1,000/s gate or establish a tail-latency-safe operating point.

| Measure | Baseline absolute median | Monitored absolute median | Absolute-median change | Paired-median change |
|---|---:|---:|---:|---:|
| Throughput | 486.10/s | 486.95/s | +0.175% | +1.483% |
| Dropped offered slots | 2.78% | 2.61% | -0.170 pp | -1.450 pp |
| Send p99 | 3.620 ms | 4.929 ms | +36.174% | -9.442% |
| Receive p99 | 5.503 ms | 6.076 ms | +10.415% | -1.177% |
| Complete p99 | 1.204 ms | 1.074 ms | -10.787% | -27.905% |
| End-to-end p99 | 8.544 ms | 9.574 ms | +12.063% | -28.305% |

The favorable paired medians conceal one severe adverse window. In pair 3,
throughput changed by -11.772%, the drop rate by +11.630 percentage points, and
Send, Receive, Complete and end-to-end p99 by +222.861%, +96.104%, +951.156% and
+2,370.167%; monitored end-to-end p99 was 125.041 ms. Baseline drop rates ranged
from 1.21% to 8.04%, so host variation was material even without observation.
Each monitored phase still recorded exactly two global refreshes, no monitoring
errors or missed calls, and zero ending backlog. Baseline WAL peaks were
3.96–3.98 MiB and monitored peaks were 10.29–13.03 MiB; every passive checkpoint
fully drained its frames and every following truncating checkpoint left a
zero-byte WAL.

MQLITE-134 keeps the same one-scan query and replaces per-state `SUM`
expressions with selective `COUNT ... FILTER` aggregates. The committed benchmark
also adds an even six-state fixture. On the same Apple M1 Pro with Go 1.26.9, the
`5d7a163` benchmark fixture ran both the `SUM` query text from `6ed7f88` and the
candidate `FILTER` query three times with three measured refreshes per run:

```bash
go test ./engine -run '^$' \
  -bench '^BenchmarkObservationDeepBacklog/ObserveAllQueuesRefresh(MixedStates)?$' \
  -benchmem -benchtime=3x -count=3
```

| Current retained distribution | `SUM` median (range) | `FILTER` median (range) | Median change |
|---|---:|---:|---:|
| 300,000 active | 248.369 ms (237.138–249.820) | 210.754 ms (209.695–214.042) | -15.14% |
| 50,000 in each of six states, mixed within every queue | 244.973 ms (238.955–250.814) | 210.518 ms (207.978–216.448) | -14.06% |

Median allocation counts stayed at 3,594 per all-active refresh and 3,294 per
six-state refresh; reported bytes per operation differed by less than 0.1%. A
separate seven-round query harness also covered all-locked and 70%-active skewed
data and reported lower medians. These temporary local-file measurements
isolate the aggregate expression; they are not a deployment capacity result.

A three-pair local-file check then used a 300,000-row cold active backlog, 1,000
scheduled sends/s, three per-second `Stats` callers, a 5-second UI observer and a
15-second Prometheus observer. Each 16-second monitored phase performed exactly
two global refreshes. With `FILTER`, the paired median monitored-versus-baseline
throughput change was -0.36% and the offered-slot drop change was +0.34 percentage
points. Every admitted message completed, every phase ended with zero hot-queue
backlog, and monitored WAL peaks of 14.1–16.8 MiB were fully checkpointed and
truncated. The median of the per-trial maximum UI observation latency was 273.334
ms, versus 338.873 ms in the old-query control.

Foreground tails remained noisy and did not pass the earlier 10% investigation
threshold: candidate paired-median changes were -29.6% Send p99, +37.4% Receive
p99, +35.5% Complete p99 and +337.1% end-to-end p99, with individual pairs moving
in both directions. Three short sequential pairs cannot attribute those tail
changes to the query. The result supports lower refresh cost and completed work
accounting, not a claim that observation has no foreground latency cost.

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

## 2 · Memory footprint & reclamation

Whole-process **RSS** (not just Go heap), peak during load vs after the idle+GC
phase — "how much memory, and is it released?"

| scenario | local peak → end | cloud peak → end |
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

## 3 · DB-file bloat vs reclamation

`bloat_50k` fills 50k messages then **drains every one** (Complete = `DELETE`),
sampling the file the whole time (local):

```
 t_ms     db      wal     (queue state)
    1   16.1MB   4.0MB    filling
 2209   32.2MB   7.7MB    full (50k resident)
 3403   32.2MB   7.7MB    drained — queue EMPTY
27018   32.2MB   7.7MB    still 32.2MB after 24s idle
```

**The file does not shrink when the queue empties.** mqlite deletes the rows, but
SQLite has no auto-VACUUM, so freed pages go to the **freelist and are reused** by
the next messages — the on-disk high-water mark persists. The same signature shows in
*every* scenario's `dbPeak == dbEnd` on both boxes. This is correct, predictable
behaviour (steady-state queues plateau; no per-message file thrash), but **size disk
for the peak backlog, not the average**. *(A known property, not a surprise.)*

> **Update (MQLITE-53):** a background pass now runs `PRAGMA incremental_vacuum` when the
> freelist grows, so a local file DB **does** return freed pages to the OS over time —
> without a manual `VACUUM` or stopping the broker. The table above predates that and
> shows the old plateau-forever behaviour; with auto-reclaim the file shrinks after a
> drain instead of holding the high-water mark. (`auto_vacuum=INCREMENTAL` is set at DB
> creation; remote Turso manages its own storage.)

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
| connection pool | local 1 / remote 4 | **don't change** — local=1 *is* the single writer (atomic claims); remote=4 is tuned for Turso's Hrana streams. |

`busy_timeout=5000` and `temp_store=MEMORY` are already set by mqlite.

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
   freed pages to the OS over time (MQLITE-53); size disk for the peak, not the average.
6. **NORMAL vs FULL** is an ~11× durability dial (local); pick per host failure model.
7. **Properties are affordable**; large bodies are byte-bound and storage-linear.

_Raw data: `test/bench/out/results.json` (local) and the `mqlite-bench` Fly logs
(cloud, 14 scenarios captured before the heaviest 200k-prefill scenarios on the
1-core box)._
