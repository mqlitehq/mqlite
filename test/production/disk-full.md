# Isolated disk-full recovery drill

Run the actual local release candidate image against a private 64 MiB Docker
tmpfs. Python 3.9+ standard library and Docker are the only host dependencies.
The image must already exist locally and provide the release image's Alpine
utilities and `mqlite` binary. The runner never pulls, builds, or publishes images.

```sh
python3 test/production/disk_full.py mqlite:candidate \
  --expected-revision FULL_40_CHARACTER_COMMIT_SHA \
  --expected-version 0.3.0 \
  --output /absolute/path/to/new/disk-full-evidence
```

Use `--docker /absolute/path/to/docker` when Docker is not on `PATH`. The output
directory must be new. OCI source, exact version, exact revision, Linux/amd64
architecture, and CLI/runtime version must agree before fault injection. The image
ID, labels/digests, binary SHA-256, schema token, FULL synchronous configuration,
commands, timestamps, endpoint, process exits, and OOM state are recorded. An RC
image has its full RC OCI version and the binary's base source version.

## What passes

1. Start one authenticated broker in a dedicated container with a read-only root,
   256 MiB memory limit, no swap allowance, one CPU, bounded logs, and a randomly
   assigned host port published on `127.0.0.1` only. The database and filler share
   only that container's 64 MiB `/fault` tmpfs; input bodies and evidence stay out
   of it. Filler never targets the host filesystem.
2. Persist independent cryptographic identities, random **whole 256 KiB bodies**,
   expected metadata, and acknowledgements on the host before relying on them.
   Retain four acknowledged messages, including one locked message.
3. Fill `/fault` to zero available blocks. A fresh HTTP Send must return an
   explicit disk-full error without acknowledgement. A separate real CLI Send
   must exit nonzero with the same disk-full cause and no success output. Both
   denied identities must remain absent from all subsequent observations.
4. Remove only the filler; acknowledge another message; kill only the broker
   process with SIGKILL. Copy the entire closed DB directory, including any WAL
   and SHM files. Read-only SQLite integrity/foreign-key checks and complete
   identity, sequence, state, count, body, and metadata reconciliation must pass.
5. Restart the broker **inside the same running container**, prove the tmpfs
   identity and holder process survived, and acknowledge a fresh message. Receive
   every acknowledged message, compare the full body and metadata, prove orphaned
   lock recovery increments the count only on the new claim and changes its
   token, then Complete it. No purge is used to hide unknown rows.
6. Require no extra delivery, empty Peeks for all five retained states, and zero
   Stats. Stop the broker cleanly, copy the complete DB directory again, and
   require a read-only integrity/foreign-key check with zero message rows.
   Unexpected broker errors fail; disk-full errors are accepted only during the
   deliberate full-filesystem interval. Exactly two Send access-log failures are
   correlated with the separately checked HTTP/CLI disk-full responses (the access
   log itself omits error details). OOM or a holder/container restart fails.

`result.json` becomes `PASS` atomically and the process exits 0 only after every
check and cleanup succeeds. On failure it becomes `FAIL`, exits nonzero, and
preserves the dedicated container and available evidence; the runner prints its
exact name. It stops the broker process and attempts a complete failure snapshot
while keeping the container and tmpfs available. Inspect that container before explicitly removing it. Successful
runs remove only their own container and its own anonymous volumes.

## Reading the evidence

`expected.jsonl`, per-message `.bin` files, `acknowledged.jsonl`, and
`observed.jsonl` are independent records of intent, broker acknowledgement, and
actual delivery. `retained-before-restart/` proves retained acknowledged data
survived disk exhaustion and process death; `drained-final/` proves verified
settlement and no residual rows. Their adjacent JSON files include file hashes
and SQLite check results. The complete directories are exported with an in-container
tar stream because Docker's copy endpoint need not expose tmpfs mounts. Read-only
checks use existing WAL files; only a closed snapshot without a WAL uses SQLite's
immutable mode, so verification never silently skips a present WAL.
`fault.jsonl`, `commands.jsonl`, `runtime.jsonl`,
`container.jsonl`, `broker.log`, and `log-checks.jsonl` explain the fault and
recovery boundaries. The ephemeral Bearer token is not written to evidence.

A run on an earlier image is a harness rehearsal, not final candidate acceptance.
Repeat this command against the frozen candidate's exact image and source SHA.
This is process-failure and disk-capacity testing; a tmpfs does not simulate
power loss, physical disk failure, or a production storage performance profile.
