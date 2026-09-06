# Production acceptance

These commands exercise a candidate without creating a tag, release or image push.
Run them after the semantic, artifact and operations checks, on one clean, fixed
source revision. Keep all evidence outside the code repository.

## Build and run

Requirements: a Linux or macOS host, Docker with Linux/amd64 support and cgroup v2, Python 3.9+ with SQLite,
and at least 10 GiB free on the host. This is a bounded correctness and resource
exercise, not a throughput benchmark.

~~~sh
MQ_REVISION=$(git rev-parse HEAD)
MQ_VERSION=$(sed -n 's/^const Version = "\(.*\)"$/\1/p' internal/version/version.go)
docker build --platform linux/amd64 --pull --no-cache \
  --build-arg VERSION="$MQ_VERSION" --build-arg REVISION="$MQ_REVISION" \
  -t mqlite:acceptance .
docker build --platform linux/amd64 --pull --no-cache -f test/production/Dockerfile \
  --build-arg VERSION="$MQ_VERSION" --build-arg REVISION="$MQ_REVISION" \
  -t mqlite-acceptance:runner .

python3 test/production/run.py \
  --broker-image mqlite:acceptance --runner-image mqlite-acceptance:runner \
  --expected-revision "$MQ_REVISION" --expected-version "$MQ_VERSION" \
  --output /absolute/path/outside/repo/production-run \
  --duration-seconds 86400 --background
~~~

Both builds scan the compiled executable. The supervisor verifies the full source
revision, clean working tree, image architecture, source/version/revision labels,
actual executable checksums and authenticated broker schema/storage status. The
completed runner must report this exact provenance, workload configuration and
requested duration, with elapsed workload time inside the supervised interval. It
runs the verified image IDs, so later retagging does not change the candidate.
The database schema expectation is currently 5; changing it requires reviewing the
restore fixtures and acceptance contract.

The supervisor starts two resource-limited containers on one private internal
network, with no published ports. A fresh broker database and a separate embedded
outbox database use different directories. The runner never opens the broker
database while it is running. Authentication tokens are generated for this run,
passed through environment variables, and excluded from saved container metadata.
Containers use the host user's UID/GID so their restricted evidence files remain
readable to the supervisor on Linux bind mounts.

The background option returns a PID and supervisor log path. On macOS it also starts
caffeinate with idle-sleep prevention while that supervisor runs. Manual sleep,
lid closure or interruption still makes a progress gap fail. Follow status.json
and the supervisor log. To stop the run, send SIGTERM to that PID; the supervisor
stops its own containers and records failure. A missing final result after SIGKILL
or a host failure is incomplete, never a pass.

Use --duration-seconds 60 for a short integration check. A successful short run
reports **SMOKE**, with production_ready set to false. The --allow-dirty option
permits only such short development checks. It cannot enable a 24-hour verdict
on dirty source. Use --docker with an absolute executable path if needed.

## Passing criteria and budgets

| Area | Requirement |
| --- | --- |
| Workload integrity | All eight [workload lanes](soak/README.md) verify independent expected identities, complete payload hashes, metadata and planned multiplicities |
| Duration | At least 24 hours of valid workload activity, with all recipes exercised; wall-clock passage alone is insufficient |
| Progress | Every lane continues to verify batches; a gap over 120 seconds or supervisor sampling gap over 120 seconds fails |
| Final state | All fixture queues/subscriptions converge to empty through verified settlement; business outbox and pending batch records are empty |
| Physical storage | After both owners stop, both DBs pass integrity and foreign-key checks with zero retained messages |
| Runtime | No unexpected error logs, OOM, container restart or broker exit; continuous log readers remain connected throughout |
| Memory | Each container limited to 512 MiB, no extra swap; anonymous memory growth after a 30-minute warmup is at most 128 MiB above its warmup median |
| Disk | Each data directory at most 512 MiB, all run files at most 2 GiB, host free disk at least 10 GiB and container filesystem free disk at least 2 GiB |
| CPU and logs | One CPU per container; Docker logs rotate at 10 MB × 2, while a continuous reader retains cumulative counts/digests and first error samples |

The resource supervisor records anonymous memory separately from filesystem cache.
It samples roughly every 10 seconds and maintains a bounded warmup window. These
are acceptance budgets for this controlled workload, not public sizing guarantees
or an application latency SLA. Fixed latency histograms and semantic recipe counts
are recorded by the runner.

Receipt, receive-attempt and dedup records have retention windows; zero auxiliary
rows is not a final-state requirement. Final message/outbox convergence and
bounded auxiliary growth are separate checks.

## Evidence and interpretation

- metadata.json / provenance.json: source, image and executable identities,
  configured budgets, process/container IDs and final runtime states.
- runner/: independent batch manifests, acknowledgements, observations, verified
  cumulative ledger, progress, fixed latency histograms and the runner verdict.
- resources.jsonl: resource and progress-time series.
- prometheus.jsonl / prometheus-final.txt: authenticated queue metrics and actual
  RPC latency histograms, sampled each minute and at the final boundary. Receive
  includes planned long-poll waits; batch latency also includes deliberate delays.
- logs.json / log-summary files: continuously observed log counts/digests,
  first errors and reader outcomes; rotation cannot reset an error count.
- result.json: joint final verdict, written atomically only after the runner
  finishes, both owners stop and physical database checks pass.
- broker-data/ / outbox-data/: retained complete database directories.

Only the supervisor's final **PASS** together with exit 0 qualifies the soak.
A runner-only PASS, STARTED/RUNNING/SMOKE, missing file, failed sample or interrupted
supervisor does not. The soak is one acceptance layer: also run the
[disk-full drill](disk-full.md), [backup/restore drill](restore/README.md),
the full race suite, end-to-end and crash tests, release image smoke, actual archive
scans and the dedicated live Turso/integrity workflows on the same candidate.
Review all evidence before requesting release approval.

Containers are stopped but retained for inspection, together with the private
network. After reviewing the recorded IDs and preserving evidence, remove only
this run's containers and network. Do not run a global Docker prune. The database
directories remain until explicitly removed.

## Supervisor controls

Run the standalone controls as a non-root Linux/macOS user. They need Python and
no Docker daemon; all subprocesses and filesystem fixtures are local and temporary.

~~~sh
python3 test/production/supervisor_controls.py \
  --output /absolute/new/supervisor-controls
python3 test/production/soak_controls.py \
  --output /absolute/new/soak-controls
~~~

Supervisor controls exercise real log-reader subprocesses, concurrent evidence
file creation/replacement/deletion, permission failures and injected I/O errors.
They also reject false workload duration/provenance, nonzero exits, OOM/restarts
and retained business rows in a real SQLite control database. The exact runtime
guards are used. These are acceptance-tool controls, not a broker soak; actual
container integration and at least 24 hours of workload still need to pass.
