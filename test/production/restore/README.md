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
| Fixture completeness | Independent send manifest matches every message column, full binary body and identity; all nine current tables are nonempty |
| Live backup | Source process remains open at a quiescent barrier; committed business data exists in WAL and is absent from an isolated main-file-only negative control; read-only `VACUUM INTO` preserves the complete logical snapshot |
| Cold backup | Worker closes successfully; entire source directory is copied and every file hash matches |
| Restore before opening | Fresh directory; `integrity_check=ok`, no foreign-key errors; every schema object, table, column, typed value and row matches the source, including business data and auxiliary tables |
| Startup recovery | Only orphan locks change: below-limit rows become active; final-attempt rows become DLQ with `MaxDeliveryCountExceeded`; both clear token/deadline and retain all other fields |
| Restored operations | Full identity/body/metadata checks through fresh Receive, deferred Pick, scheduled activation, DLQ redrive, dedup replay/conflict, receipt replay, attempt replay and stale-token fencing; subscription filter includes and excludes the intended messages |
| Convergence | Every queue's Stats and all-state Peek are empty; the entire messages table is empty; committed business data remains exact; backup copies remain unchanged |
| Schema refusal | Actual v0.2.0 creates schema 2 with a different physical schema; current binary refuses it and old binary refuses the current schema, with complete logical snapshots unchanged in both directions |
| Rollback | Old snapshot restored into another fresh directory; old binary returns the exact expected identity/body and completes the message |

The retained fixture includes active, locked, deferred, scheduled and dead-lettered
rows, standard/group FIFO/strict FIFO configurations, two subscriptions including a
real filter, and committed/rolled-back `Embedded.Tx` business operations.
Completion deletes a message; its receipt and retired highest sequence number
exercise `settlement_receipts` and `sqlite_sequence` without inventing a completed
row. The full snapshot also covers `dedup`, `receive_attempts` and `meta`.

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
