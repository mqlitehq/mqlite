# Backup, restore, and schema rollback drill

Run this before approving a candidate's operational readiness. It uses the current
source's real embedded SDK and CLI, plus an actual v0.2.0 CLI for schema 2. It does
not publish a release, run a soak, fill a disk, or modify an existing database.

## Run

Requirements: a POSIX host, Python 3.9+ with SQLite, the `sqlite3` CLI with
`VACUUM INTO` support, Go, and a native v0.2.0 executable built from its unmodified
tag. Use a patched Go toolchain to build both old and candidate executables.
The runner defaults to Go 1.27.1; `--go-toolchain` selects another installed or
downloadable toolchain without changing the module's Go 1.21 floor.

From the repository root:

```sh
python3 test/production/restore/run.py \
  --old-binary /absolute/path/to/mqlite-v0.2.0 \
  --output /absolute/path/to/evidence/new-restore-run
```

The output directory must not exist. Keep evidence outside the code repository.
The runner builds its SDK fixture worker and the candidate CLI from the current
source into that directory. Binary hashes, versions, Go build information, source
revision and dirty status are recorded.
The same worker executable seeds and opens both restored stores. No second
MQLite engine opens the live source.

## Required checks

| Check | Passing evidence |
| --- | --- |
| Fixture completeness | Independent send manifest matches every message column, full binary body and identity; all ten fixture tables (eight broker tables, SQLite sequence, business outbox) are nonempty |
| Live backup | Source process remains open at a quiescent barrier; committed business data exists in WAL and is absent from an isolated main-file-only negative control; read-only `VACUUM INTO` preserves the complete logical snapshot |
| Cold backup | Worker closes successfully; entire source directory is copied and every file hash matches |
| Restore before opening | Fresh directory; `integrity_check=ok`, no foreign-key errors; every schema object, table, column, typed value and row matches the source, including business data and auxiliary tables |
| Embedded fixture startup recovery | Only orphan locks change: below-limit rows become active; final-attempt rows become DLQ with `MaxDeliveryCountExceeded`; both clear token/deadline and retain all other fields |
| Restored operations | Full identity/body/metadata checks through fresh Receive, deferred Pick, scheduled activation, DLQ redrive, dedup replay/conflict, receipt replay, attempt replay and stale-token fencing; subscription filter includes and excludes the intended messages |
| Restored credentials | Active key retains exact metadata and authenticates; expired/revoked keys are rejected; all three digest-only records stay unchanged |
| Convergence | Every queue's Stats and all-state Peek are empty; the entire messages table is empty; committed business data remains exact; backup copies remain unchanged |
| Schema refusal | Actual v0.2.0 creates schema 2 with a different physical schema; current binary refuses it and old binary refuses the current schema, with complete logical snapshots unchanged in both directions |
| Rollback | Old snapshot restored into another fresh directory; old binary returns the exact expected identity/body and completes the message |

The retained fixture includes active, locked, deferred, scheduled and dead-lettered
rows, standard/group FIFO/strict FIFO configurations, two subscriptions including a
real filter, and committed/rolled-back `Embedded.Tx` business operations.
Completion deletes a message; its receipt and retired highest sequence number
exercise `settlement_receipts` and `sqlite_sequence` without inventing a completed
row. The full snapshot also covers `dedup`, `receive_attempts`, `meta` and
`access_keys`. Three test keys cover active, expired and revoked credentials; the
private fixture manifest retains their test secrets, while full database snapshots
must contain only independently computed SHA-256 digests and exact metadata.

The clock is fixed and background workers are disabled while taking snapshots.
After reopening, the worker advances the clock by 60 seconds and runs maintenance
to verify scheduled activation. This is a deterministic storage/recovery drill;
concurrent traffic and process-death tests are separate acceptance layers.

An old receive-attempt response can still contain its original token after
restoration. The drill explicitly requires that token's settlement to fail after
recovery, then obtains and settles a fresh claim. A cached response does not renew
ownership.

## Read-only discipline

Live snapshots and hot backups open the source with `mode=ro`; every Python
connection closes explicitly. External tooling never writes or checkpoints the
live database. Hot backup invokes `sqlite3 -readonly` with `VACUUM INTO` and a new
destination. Preserve this restriction when changing the drill: the
[SQLite WAL-reset advisory](https://www.sqlite.org/wal.html#the_wal_reset_bug)
describes a race involving concurrent writers/checkpointers in older SQLite.

A cleanly stopped WAL database may have no WAL/SHM sidecars, and native SQLite can
refuse a normal read-only open in that state. For a **stopped** store with no
nonempty WAL, the driver uses `mode=ro&immutable=1`. If a stopped store still has
WAL data, it uses normal read-only mode so that data is included. `immutable=1`
is never used on the live source. The main-file-only negative control is an
isolated incomplete copy; it is never used as a restore source.

## Results

Success requires process exit 0 and `result.json` with `status: "PASS"`. A failed
command, invariant mismatch, worker error/timeout, or unexpected engine log exits
nonzero and records `FAIL`. Missing or incomplete results are not a pass.

Evidence includes the independent fixture manifest; full source, backup,
post-open and final snapshots; executable/build metadata; exact command outputs
and worker logs; immutable backup files; and SHA-256 digests. The complete-table
fixture allowlist deliberately fails when schema tables change, requiring a review
of the new surface rather than silently skipping it.

See [operations](../../../docs/operations.md) for the operator procedure.

## v0.3.2 schema-boundary recovery

Durable queue lifecycle and fixed subscription expiration change the schema token
from 5 to 7. There is no in-place migration: opening a schema-5 database with this
candidate must fail without changing its schema or data. Schema 6, used by the
intermediate lifecycle implementation, is rejected by the engine compatibility
tests as well. Keep the matching v0.3.2 binary and a backup available until the new
deployment has been validated. Verify both sides of the released boundary with the
published v0.3.2 archive, its `checksums.txt`, the binary extracted from that
archive, a Git repository containing the v0.3.2 commit, and the clean final
candidate checkout:

```sh
python3 test/production/restore/compat.py \
  --old-binary /absolute/path/to/released-v0.3.2/mqlite \
  --old-archive /absolute/path/to/mqlite_0.3.2_darwin_arm64.tar.gz \
  --old-checksums /absolute/path/to/v0.3.2/checksums.txt \
  --old-source /absolute/path/to/repository-containing-v0.3.2 \
  --new-source /absolute/path/to/clean-v0.4.0-source \
  --go-toolchain go1.27.1 \
  --output /absolute/path/to/evidence/new-compat-run
```

The runner is offline with respect to release provenance: it pins the published
v0.3.2 checksum-manifest digest, verifies the archive checksum, and requires the
supplied old binary to match the archive member byte for byte before executing it.
It exports both source revisions with `git archive` from isolated Git metadata
that ignores repository-local replace refs, attributes and configuration, then
builds the fixture and candidate with `GOWORK=off`, read-only module metadata and
the selected toolchain.
The candidate HEAD must be clean and report exactly v0.4.0; untracked and ignored
files cannot enter either source build.

The script creates a complete schema-5 fixture, lets the released binary perform
its normal orphan recovery, and takes an exact logical snapshot. The candidate
must reject that database as schema 5 while expecting schema 7, leaving the
snapshot byte-for-byte equivalent at the logical level. The released binary must
then reopen the same database and complete a verified send/peek round trip. In the
reverse direction, the candidate creates a fresh schema-7 database and the
released binary must reject it while leaving its complete snapshot unchanged.
Evidence contains all six snapshot hashes, source commits and trees, source
archive hashes, build commands and environment, the actual Go version, and hashes
for the release archive, checksum manifest, both binaries and the fixture worker.
