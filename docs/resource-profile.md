# Resource profile & Fly.io sizing (MQLITE-27)

How much does an MQLite broker actually cost to run? Short answer: **less than you
think.** A pure-Go single binary with an embedded SQLite engine — no JVM, no
sidecar, no page-cache-hungry log segments. It comfortably fits Fly's *smallest*
machine with room to spare.

## TL;DR — recommended Fly deployment

```
┌─────────────┬──────────────────────────┬──────────────────────────────────────┐
│ resource    │ measured (1 broker)      │ Fly choice                            │
├─────────────┼──────────────────────────┼──────────────────────────────────────┤
│ memory      │ 19 MB idle               │ shared-cpu-1x · 256 MB  (>6× headroom)│
│             │ 38 MB @ 50k msgs queued  │   bump to 512 MB only for >500k       │
│             │                          │   in-flight backlogs                  │
│ disk        │ ~0.4 KB / 256 B message  │ 1 GB volume  (~2M messages)           │
│             │ + ~4 MB WAL (constant)   │   size = backlog × 0.6 KB × 1.5       │
│ vCPU        │ tiny per op; 1000s/s     │ 1 shared vCPU is ample                │
│ image       │ 10.9 MiB static binary   │ distroless/scratch ≈ 15 MB image,     │
│             │   (CGO-free)             │   cold start < 1 s (scale-to-zero ok) │
└─────────────┴──────────────────────────┴──────────────────────────────────────┘
```

Topology: MQLite is **single-writer** (one process holds a file lock per DB), so
run **one machine per volume** — do not horizontally scale several machines onto
the same SQLite file. Need HA / multi-region? Point the broker at a Turso/libSQL
DSN instead of a local file (separate concern, see `MQLITE_DB`).

## Measured numbers

Apple-Silicon (arm64) host, `GOMAXPROCS=12`, pure-Go SQLite (`modernc.org/sqlite`,
no CGO), `synchronous=NORMAL` + WAL unless noted. Linux/amd64 (what Fly runs) puts
Go RSS at parity or slightly lower; throughput depends on the host's real cores.

### Memory — the binding Fly constraint

```
mqlite serve, idle (just started, queue empty) ...... 19 MB RSS
mqlite serve, 50,000 × 256 B messages queued ........ 38 MB RSS
engine heap under sustained produce load ............ 4–10 MB (Go HeapAlloc)
```

Messages live in SQLite (on disk), not pinned in RAM — so RSS tracks the *working
set* (receive/peek buffers + page cache + Go heap), not the total backlog. A queue
with millions of dormant messages still serves from a small resident footprint.
**256 MB is never the bottleneck for a lightweight queue.**

### Disk — volume sizing

```
50,000 × 256 B messages → 15.3 MB main db + 4.0 MB WAL = ~405 bytes / message
                                                          (~150 B overhead: row +
                                                           two partial indexes)
WAL caps at ~4 MB (checkpoint threshold), constant — not per-message growth.
```

Rules of thumb (256 B payloads):

| backlog depth | DB on disk | Fly volume |
|--------------:|-----------:|:-----------|
| 100k          | ~40 MB     | 1 GB       |
| 1M            | ~0.4 GB    | 1 GB       |
| 5M            | ~2 GB      | 3 GB       |

Steady-state disk = **backlog depth, not lifetime throughput**: `Complete`/`Purge`
delete rows. Note SQLite keeps the file at its high-water mark (free pages are
reused, not returned to the OS) unless you `VACUUM`; size the volume for peak
backlog, not average.

### Throughput & latency (macOS in-process — latency detail)

```
scenario           sync     ops/s    p50      p99
produce (1 prod)   NORMAL   17.4k    50 µs    165 µs
produce (4 prod)   NORMAL   17.4k    201 µs   765 µs
produce (4 prod)   FULL     12.3k    285 µs   919 µs   ← macOS only; on Linux FULL is ~18× (below)
```

A single writer serves tens of thousands of msg/s. `MQLITE_SYNC` (NORMAL ↔ FULL) is
the durability ↔ throughput lever and batch sends (`messages: [...]`) amortise the
commit — both quantified on real hardware in **Disk I/O & CPU (Linux)** below. (The
macOS FULL penalty above looks mild only because APFS fsync is buffered.)

## Disk I/O & CPU (Linux)

Measured on Linux with real `/proc/self` IO+CPU (`test/bench/run-bench.sh`; 256 B
messages, 3 s/scenario, NORMAL + WAL unless noted). The host is a Docker linuxkit VM
(arm64, fast local SSD) — **not** Fly's shared-cpu-1x — so read the *relative* costs,
not the absolute ops/s; a Fly smoke test would pin the absolutes.

```
scenario             ops/s   cpu%   write-bytes/msg   note
produce 1 producer   19.5k    92%      9.7 KB         one commit = whole-page WAL write
produce 4 producers  18.6k    91%      9.7 KB         one writer; more producers don't add throughput
batch 16 / commit    33.7k    99%      2.0 KB         amortised fsync
batch 64 / commit    36.2k   100%      1.1 KB         9× less write amplification than 1-by-1
send+receive+complete 3.6k    84%       58 KB         three commits per message
produce, FULL sync   1.05k    11%     13.7 KB         fsync per commit — see finding 3
```

Three findings that matter:

1. **One writer ≈ one core.** Produce saturates ~0.9 of a single core at ~18–19k
   msg/s, and adding producers does *not* raise throughput — the single writer is
   the ceiling, by design. Under NORMAL sync the write path is **CPU-bound**, not
   I/O-bound (pure-Go SQLite). On Fly's 1 shared vCPU expect proportionally less but
   still thousands/s, well above typical load.
2. **Write amplification is real; batching is the cure.** A 256 B message costs
   ~**9.7 KB** of writes sent one-at-a-time (each commit flushes whole 4 KB WAL
   pages); batching 64 per commit drops that to ~**1.1 KB/msg (9×)** and nearly
   doubles throughput. *Send arrays, not singletons.*
3. **FULL sync is ~18× slower on real hardware.** Single-send throughput falls
   18.6k → **1.05k** msg/s under `synchronous=FULL`, CPU dropping to 11% — the broker
   is now waiting on real `fsync` disk barriers, not computing. (macOS made this look
   like ~30 % because APFS fsync is cheap/buffered; Linux is the honest figure.) If
   you need FULL durability, **batch** to amortise the fsync.

## Tuning knobs

MQLite ships sensible defaults; most deployments change nothing. In rough order of
impact:

| knob | default | when to change |
|---|---|---|
| **batch size** (`messages:[…]`) | — | the biggest lever — 9× less write amplification, ~2× throughput. Batch whenever you can. |
| **`MQLITE_SYNC`** | `NORMAL` | `FULL` only if a power cut losing the last few commits is unacceptable; it costs ~18× on single sends, so pair it with batching. |
| `wal_autocheckpoint` | 1000 pages (~4 MB) | matches the observed WAL plateau; raise for fewer checkpoints at the cost of a larger WAL + slower crash recovery. |
| `cache_size` | ~2 MB | the working set (queue heads + indexes) is small; raise only after profiling shows cache pressure under deep backlogs — it costs RAM on a 256 MB machine. |
| `mmap_size` | off | `modernc.org/sqlite` is a pure-Go reimplementation; don't assume C-SQLite mmap gains — measure before relying on it. |
| connection pool | local 1 / remote 4 | **don't change** — local=1 *is* the single writer (atomic claims); remote=4 is tuned for Turso's Hrana streams. |

`busy_timeout=5000` and `temp_store=MEMORY` are already set by MQLite.

Reproduce the Linux numbers (Docker, native arch, real `/proc`):

```sh
./test/bench/run-bench.sh                 # 5 s/scenario, 256 B
BENCH_DUR=10s BENCH_MSG=1024 ./test/bench/run-bench.sh
# → test/bench/out/results.json  (ops/s, cpu%, write_bytes_per_op, db/wal MB, p50..p999)
```

## How these numbers were taken

- **RSS**: `mqlite serve` started on a temp file DB, `ps -o rss=` sampled idle, then
  after 50k messages pushed over HTTP.
- **Disk / heap / throughput**: `go run ./test/bench -only produce -dur 3s`, reading
  `db_size_mb + wal_size_mb` and `heap_mb` from the JSON output.
- **Disk I/O & CPU (Linux)**: `BENCH_DUR=3s ./test/bench/run-bench.sh` in Docker —
  the bench's `/proc/self` probe reports `cpu_pct` and `write_bytes_per_op` only on
  Linux, so this runs in a container, not on the macOS host.
- **Binary**: `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' ./cmd/mqlite`.
