# Mixed workload acceptance runner

Use [the host supervisor](../README.md) for candidate acceptance. It fixes the
source revision and image identities, bounds resources, collects broker logs and
metrics, and independently checks stopped databases and container exit states.
This command supplies the mixed workload and its correctness oracle. Its own
successful result alone does not establish production readiness.

## Run and inspect

Build with the same patched Go toolchain as the candidate:

~~~sh
go build -o /tmp/mqlite-soak ./test/production/soak
MQLITE_TOKEN=... /tmp/mqlite-soak \
  --endpoint http://127.0.0.1:6754 \
  --outbox-db /absolute/new/outbox.db \
  --evidence /absolute/new/evidence \
  --duration 24h \
  --provenance /absolute/candidate-provenance.json
/tmp/mqlite-soak --verify-ledger /absolute/new/evidence
~~~

The token comes from an environment variable, never a command argument or ledger.
The broker must be dedicated, empty and use local SQLite. The outbox is a
different, new SQLite file opened by this process with synchronous FULL; the
broker must use FULL too. Existing outbox files/sidecars and nonempty evidence
directories are refused. Queues from a failed run are not reused. All clocks must
be synchronized: observed enqueue and lease times are compared with driver time.
The supervisor runs both containers on the same host.

**SMOKE** means the requested run was shorter than 24 hours. **PASS** requires a
requested duration of at least 24 hours and elapsed monotonic time at least that
long, completion of every recipe below, final reconciliation, and a successful
ledger audit. Waiting after a short workload never turns it into a 24-hour pass.
A failure, interruption, missing result, pending batch or failed supervisor check
does not pass. This driver does not resume interrupted evidence.

## Eight concurrent lanes

Each lane has at most one unverified batch. Sends inside a batch have an explicit
order; goroutine scheduling never defines the expected FIFO order.

| Lane | Independent expected outcome and checks |
|---|---|
| Ordinary | Four identities per batch; dedup replay preserves sequence. Receive attempt replay preserves the exact identities, tokens, counts and timestamps. Two out of three batches exercise single and batch renewal, which must retain live, non-shortened leases, followed by single or batch completion and exact-request replay. The third uses receive-delete with no token or lease. A different settlement verb cannot reuse a completion receipt. |
| Group FIFO | Inputs are A0, A1, B0, B1. Abandoning A0 with backoff blocks A1 while B0 and B1 progress. A0 then returns with count 2, followed by A1. |
| Strict FIFO | A locked, deferred, or backoff-delayed head blocks the tail. Picking the deferred head and redelivering after backoff advance its delivery count exactly before the tail can be received. |
| Retry / DLQ | Alternates delivery-limit dead-lettering, explicit rejection with exact reason/detail, and lease expiry. An expired token must fail even before the background reaper; it must also fail after reclaim. Retained full contents and counts are checked before redrive; redrive resets the count and issues a different token. |
| Scheduled | Two messages must be excluded before their requested schedule and eventually arrive afterward with full contents intact. |
| Deferred / TTL | One deferred message is excluded from ordinary receive and retrieved by sequence. A second expires while deferred and is excluded even from pick. Batches alternate TTL dead-lettering and discard. The DLQ row is fully verified before a count-checked purge; discard requires absence after expiry and verifies any retained row while waiting. |
| Topics | Two independent input identities fan out to an unfiltered subscription; only the EU/keep identity belongs in the filtered subscription. Both exact target sets are drained and checked for extra copies. |
| Outbox | Two transactions commit independent business IDs, amounts, full bodies and enqueues; a third intentionally rolls back both business and queue writes. Business rows are matched exactly before consuming the two messages, then only those verified rows are deleted. |

Each expected plan uses a fresh random 128-bit run seed, lane, batch number and
input index. Message bodies are deterministically reconstructed from these
identities using SHA-256 blocks; the run cycles through full binary bodies of
256–1,152 bytes. Expected properties include multiple fields and Unicode.
Scheduling additionally uses the batch creation timestamp.

The online receive oracle compares every body byte, identity and metadata field;
acknowledged sequence numbers; unique identity/sequence mappings; exact delivery
counts; fresh or replayed fencing tokens; and enqueue/lease timestamps. Expected
identity sets reject missing, duplicate, extra and late deliveries. Settlements
are checked for the exact expected success or error. Retained rows use the same
full-content comparison plus state, count, sequence, original enqueue time, TTL,
lease absence and dead-letter details. A batch is verified only after all its
expected terminal identities and acknowledgements are present and every target
queue is empty in both all-state Peek and Stats.

## Evidence and limits

| File | Meaning |
|---|---|
| metadata.json | Configuration, seed, recipe version, start time, Go build information, broker status and supplied provenance. |
| pending/&lt;lane&gt;/expected.json | Independently generated expected manifest, synced before sending. Bodies can be reconstructed from identity and length. |
| pending/&lt;lane&gt;/ack.jsonl | Synced operation requests, accepted results and expected rejections for the current batch. |
| pending/&lt;lane&gt;/observed.jsonl | Synced observed identities, full-body hashes/lengths, metadata, ownership timestamps and retained-state details for the current batch. |
| batches.jsonl | After online reconciliation, a synced, chained summary containing the plan hash, acknowledgement/observation digests and counts, recipe hits, delivery counters and timestamps. |
| resources.jsonl | Periodic Go memory/goroutine measurements and outbox auxiliary-table retention checks. |
| progress.json, result.json | Current progress and the final runner verdict. The host supervisor supplies additional acceptance evidence. |

After the verified summary is synced, that batch's pending journals are removed
to bound storage. Completed history retains hashes and counters, not every
historical observation or payload. The offline audit reconstructs expected plans,
checks their hashes, the summary chain, recipe/counter contracts, lane activity,
duration and final result consistency. It cannot recompare observations whose
journals have already been removed, and the hashes are not signed attestation.
Full-content matching happens online before compaction. Failed pending journals
remain available for diagnosis.

Defaults are a 1.5-second minimum batch period per lane, a 45-second batch timeout,
512 MiB for runner evidence and 512 MiB for the outbox plus sidecars. One-second
monitoring fails on a heartbeat gap over 10 seconds, wall/monotonic clock drift
over 5 seconds, or a lane without a verified batch for 120 seconds. The host
supervisor adds memory, CPU, disk, log and container checks. At completion every
broker/outbox queue must be empty in all states, the business table must be empty,
every recipe must have run, and no pending batch may remain. Final database
integrity is checked by the supervisor after clean shutdown.

This is bounded mixed-traffic correctness and stability acceptance. Separate
release checks cover throughput benchmarks, message-integrity volume, remote
Turso, process crashes, disk exhaustion, backup/restore and packaged artifacts.

## Oracle controls

~~~sh
python3 test/production/soak_controls.py \
  --output /absolute/new/oracle-controls
~~~

The standalone control tool compiles a temporary copy of the actual runner code
with a different entry point. Valid and corrupted messages call the same online
receive, retained-state, reconciliation and fencing oracles used during the soak.
It checks payload changes at the beginning, middle and end; metadata, sequence,
count and ownership corruption; missing/duplicate/extra identities; and recipe
counter drift. Its delivery-field golden requires review when the SDK gains an
exported message field.

Separate synthetic history controls check reconstruction, chain integrity,
missing evidence, recipe/count drift and false duration/status claims. Semantic
mutations are rehashed so they must fail the contract check, not merely the hash
check. Synthetic histories and classifier boundary tests are control fixtures;
they are never evidence that a real workload ran for 24 hours.
