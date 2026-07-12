# mqlite benchmark tool (local SQLite)

Drives the **embedded engine** (no HTTP) through a high-frequency request matrix,
runs system monitoring inside **Docker**, and uses an in-process probe reading
`/proc/self` to attribute CPU/disk per scenario.

| File | Role |
|---|---|
| `main.go` (this dir) | load generator: 9 scenarios + µs latency histogram + `/proc/self/io`·`/proc/self/stat` probes, writes `results.json` |
| `Dockerfile` | bench image (golang + sysstat/procps), **native arch** (does not force amd64, to avoid qemu distortion) |
| `entry.sh` | in-container: start `iostat`/`vmstat` sampling → run bench → tear down, record `env.txt` |
| `run-bench.sh` | host: build image → run (DB on the container fs, not a bind-mount) → `docker cp` the results into `out/` |

## Run

```bash
cd mqlite
./test/bench/run-bench.sh                                 # 5s/scenario, 256B
BENCH_DUR=10s BENCH_MSG=1024 ./test/bench/run-bench.sh    # custom duration / body size
```

Artifacts land in `test/bench/out/`: `results.json`, `iostat.log`, `vmstat.log`,
`env.txt`, and each scenario's `*.db`.

## Backend: local file vs remote Turso (MQLITE-41)

By default each scenario opens its own local SQLite file (local-disk stress). Two
switches run the same matrix against **remote Turso**, for a "local SSD vs cloud Turso"
comparison:

- `-db libsql://<host>` (or `BENCH_DB`): all scenarios share this one remote database,
  each isolated by its own queue; the auth token comes from the `MQLITE_DB_AUTH_TOKEN`
  environment variable. File-size metrics don't apply in remote mode.
- `-prefillcap N` (or `BENCH_PREFILLCAP`): caps the drain/bloat prefill volume so it's
  feasible on a slow remote backend (~tens-to-hundreds of ms/op); pass the same cap to
  all three backends so the numbers compare item by item.

> Each remote enqueue is one durable Hrana commit round-trip (~45–57ms, even in-region),
> so throughput is 100–1000× lower than a local SSD. The three-way comparison report is
> `MQLite-cloud-bench-report.{md,html}` (raw data in `bench-3way-raw/`).

## Scenarios

produce ×{1,4,8 producers} · batch ×{16,64} · e2e(4×4) · drain(200k prefill, then empty)
· sessions(64 groups) · produce FULL (fsync control).

For the full results and analysis, see the stress report in the design repo
(`mqlite-stress-report.{md,html}`).

> Caveat: Docker Linux VM (Apple Silicon), not bare metal; ratios carry over, absolute
> numbers do not. The probe adds a low-single-digit-percent overhead on µs-scale operations.
