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

### Throughput & latency (in-process engine, indicative)

```
scenario           sync     ops/s    p50      p99
produce (1 prod)   NORMAL   17.4k    50 µs    165 µs
produce (4 prod)   NORMAL   17.4k    201 µs   765 µs
produce (4 prod)   FULL     12.3k    285 µs   919 µs   ← fsync-per-commit cost ≈ -30%
```

Per-message CPU is tiny; a single shared vCPU serves thousands of msg/s — far above
typical lightweight-queue load. `MQLITE_SYNC` (NORMAL ↔ FULL) is the durability
↔ throughput/IOPS lever; batch sends (`messages: [...]`) amortise the commit.

## Caveats — what still needs a Linux run

CPU-seconds-per-op and disk **IOPS/bytes-per-op** are read from `/proc/self`, which
exists only on Linux — on the macOS host used here those fields read 0. The absolute
numbers above for memory, disk footprint, throughput and latency are valid
cross-platform; **per-op CPU and IOPS should be confirmed on Linux/Fly**.

The bench harness is ready for exactly that (Docker, native-arch, real `/proc`):

```sh
./test/bench/run-bench.sh                 # 5 s/scenario, 256 B
BENCH_DUR=10s BENCH_MSG=1024 ./test/bench/run-bench.sh
# → test/bench/out/results.json  (ops/s, p50..p999, cpu%, write_bytes_per_op, db/wal MB)
```

## How these numbers were taken

- **RSS**: `mqlite serve` started on a temp file DB, `ps -o rss=` sampled idle, then
  after 50k messages pushed over HTTP.
- **Disk / heap / throughput**: `go run ./test/bench -only produce -dur 3s`, reading
  `db_size_mb + wal_size_mb` and `heap_mb` from the JSON output.
- **Binary**: `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' ./cmd/mqlite`.
