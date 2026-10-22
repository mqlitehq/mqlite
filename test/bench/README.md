# mqlite benchmark tool (local SQLite)

Drives the **embedded engine** (no HTTP) through a high-frequency request matrix,
runs system monitoring inside **Docker**, and uses an in-process probe reading
`/proc/self` to attribute CPU/disk per scenario.

| File | Role |
|---|---|
| `main.go` (this dir) | load generator: scenario matrix + µs latency histogram + `/proc/self/io`·`/proc/self/stat` probes, writes `results.json` |
| `Dockerfile` | Linux amd64 bench image (Go 1.27 on Debian trixie + sysstat/procps) |
| `entry.sh` | in-container: start `iostat`/`vmstat` sampling → run bench → tear down, record `env.txt` |
| `run-bench.sh` | host: build image → run (DB on the container fs, not a bind-mount) → `docker cp` the results into `out/` |

## Run

```bash
cd mqlite
./test/bench/run-bench.sh                                 # 5s/scenario, 256B
BENCH_DUR=10s BENCH_MSG=1024 ./test/bench/run-bench.sh    # custom duration / body size
BENCH_ONLY=lifecycle_churn BENCH_DUR=2s ./test/bench/run-bench.sh
```

Artifacts land in `test/bench/out/`: `results.json`, `iostat.log`, `vmstat.log`,
`env.txt`, and each scenario's `*.db`.

Builds and runs use `--platform linux/amd64`. Use an amd64 Linux host for performance
measurements; an ARM host may emulate the image, so a successful smoke run there
only verifies functionality. `run-bench.sh` is the local-file Docker wrapper; run
the Go command directly for a remote backend.

## Backend: local file vs remote Turso (MQLITE-41)

By default each scenario opens its own local SQLite file (local-disk stress). Run the
same matrix against **remote Turso** with the benchmark flags directly:

```bash
MQLITE_DB_AUTH_TOKEN=<jwt> go run ./test/bench \
  -db libsql://<host> -prefillcap 100 -out /tmp/mqlite-bench-results.json
```

- `-db libsql://<host>`: all scenarios share this one remote database,
  each isolated by a process-unique queue namespace; the auth token comes from the
  `MQLITE_DB_AUTH_TOKEN` environment variable. The runner force-deletes only that
  namespace after each scenario and drives bounded cleanup to convergence. File-size
  metrics don't apply in remote mode.
- `-prefillcap N`: caps the drain/bloat prefill volume so it's
  feasible on a slow remote backend (~tens-to-hundreds of ms/op); pass the same cap to
  every compared backend so the numbers compare item by item.

Use a dedicated non-production database for remote benchmarks. The workload is
intentionally write-heavy, and a killed process cannot run its final cleanup. A
normal run exits nonzero if any send, receive, completion, accounting, cleanup, or
result-file operation fails; a printed `[fail]` line or nonzero exit means partial
numbers are diagnostic only. Unprefixed queues and queues from another run namespace
are never cleanup targets.

Remote timings depend on region, network path, and backend load; each enqueue also
includes a durable Hrana commit round trip. See the repository's
[benchmark methodology and shipped results](../../docs/benchmark.md). Historical
comparison reports or raw runs kept outside this repository are external evidence:
they are not available or reproducible from a source checkout unless supplied
separately.

## Scenarios

produce ×{1,4,8 producers} · batch ×{16,64} · e2e(4×4) · drain(200k prefill, then empty)
· sessions(64 groups) · produce FULL (fsync control) · message properties · body-size
sweep · exact fill/drain footprint · load ramp-down · consumer churn.

The fill/drain scenario samples its full backlog and then the state after a short
1.9-second idle. The local page-reclaim janitor runs every 60 seconds, so this
scenario reports immediate file state; use a sustained run or an explicit
`Compact(false)` measurement to evaluate page reclamation.

`lifecycle_churn` is opt-in because create/delete churn is expensive on a remote
backend. Each cycle reuses a fixed 64-name working set only after the previous
lifetime is absent: create, batch-send small messages, force-delete, then run
bounded maintenance to convergence. Four queues exceed the foreground cleanup
batch so the result records both immediate and pending deletion acknowledgements.
The scenario caps bodies at 128 bytes and keeps live body payload below 1 MiB per
cycle. Its JSON result adds accepted/pending delete counts, maintenance passes,
convergence latency, and peak live payload; the standard sampler records DB/WAL,
RSS, CPU, and write amplification. Generic `ops/s` is fully converged queue
lifecycles per second, while the generic latency columns are force-delete
acknowledgement latency.

For results and analysis shipped with the code, see
[docs/benchmark.md](../../docs/benchmark.md). Generated files under
`test/bench/out/` describe only the local run that produced them. Any report or raw
dataset kept outside this repository is external evidence and must identify its
own source revision, environment, and collection method before comparison.

> Record the host architecture, virtualization, and emulation alongside results.
> The in-process probe adds overhead to microsecond-scale operations.
