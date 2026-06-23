# Retention & archival — design (MQLITE-21)

MQLite targets high throughput, and a high-throughput store on SQLite grows fast.
If nothing ever shrinks the database, write latency climbs, the volume fills, and
the broker eventually stalls — the opposite of the "lightweight, reliable" goal.
This note is the design investigation: what MQLite keeps today, how other queues
bound their storage, the full design space (triggers, actions, safe deletion,
space reclamation, archival), and the *smallest* subset to ship first (Tier 1 shipped
without a schema change; Tier 2 added per-queue columns once that was worthwhile).

## What MQLite retains today

```
┌────────────────┬───────────────────────────────────┬──────────────────────────┐
│ message state  │ lifetime                          │ removed by               │
├────────────────┼───────────────────────────────────┼──────────────────────────┤
│ completed      │ none — row is DELETEd at settle   │ Complete / receive+delete│
│ active         │ until received & completed         │ consumer                 │
│ locked         │ lock_duration, then reclaimed      │ reapLocks loop           │
│ deferred       │ until ReceiveDeferred + completed  │ consumer                 │
│ scheduled      │ until its enqueue time / Cancel    │ activation or Cancel     │
│ expired (TTL)  │ default_ttl_ms, then →DLQ or drop  │ expireTTL loop           │
│ dead_lettered  │ **forever, until manual action**   │ Redrive / Purge (manual) │
└────────────────┴───────────────────────────────────┴──────────────────────────┘
aux tables (dedup, settlement_receipts, receive_attempts) are already GC'd by the
cleanupDedup / cleanupExpiredAux background loops.
```

Everything has a bounded lifetime **except `dead_lettered`**. Completed work is
deleted immediately (no "completed message archive" to grow), locks self-heal, TTL
is enforced. So the steady-state size of a *healthy* queue is its in-flight backlog
— which is self-limiting if consumers keep up. **The one unbounded sink is the
DLQ.**

## The gap: an unbounded DLQ

Dead-lettered messages persist until an operator runs `Redrive` or `purge-dlq`. A
poison-message storm or an unattended broker grows the volume without limit — at
~0.4 KB/message ([benchmark.md](benchmark.md)) a runaway producer
fills a 1 GB Fly volume with ~2.5M dead letters. The DLQ needs a *default ceiling*,
not just a manual broom. (A secondary, milder risk: a queue whose consumers fall
permanently behind grows its active backlog — but that is a capacity problem, not a
retention one; retention must never delete undelivered work.)

## How other queues bound their storage

| system | knobs | over-limit policy | reclamation |
|---|---|---|---|
| **Kafka** | `retention.ms`, `retention.bytes`; log compaction (keep latest per key) | delete oldest **segment** (cheap, whole-file) | drop closed segment files |
| **RabbitMQ** | message/queue TTL, `max-length`, `max-length-bytes` | `drop-head` (evict oldest) or `reject-publish` | per-queue; lazy queues page to disk |
| **Redis Streams** | `MAXLEN`, `MINID` via `XTRIM` / `XADD` | trim oldest; `~` = **approximate** trim for speed | in-memory, radix-tree reclaim |
| **NATS JetStream** | `MaxMsgs`, `MaxBytes`, `MaxAge`; `retention=limits\|interest\|workqueue` | `discard=old\|new` | per-stream block deletion |
| **AWS SQS** | message retention period (max 14 days) | auto-delete past retention | managed |

**Takeaways for MQLite:**

1. Every mature system bounds storage by some combination of **age / count / bytes**
   — not just age. MQLite already has per-message **TTL** (≈ SQS retention) and a
   DLQ; what it lacks is a bound on the *DLQ* itself.
2. The common over-limit policies are **drop-oldest** (Kafka/Rabbit drop-head,
   Redis/NATS) vs **reject-new** (Rabbit reject-publish, NATS discard=new). For a
   DLQ, drop-oldest is the sane default (a dead letter losing the *oldest* failures
   first is acceptable; refusing to dead-letter new failures is not).
3. Kafka's cheap **segment deletion** and Redis's **approximate** trim both say the
   same thing: *bulk, amortized deletion beats per-message work*. On SQLite that
   maps to **batched, range-based DELETEs that never hold a long write lock** —
   see below.

## Design space

A retention policy is `(trigger, action, scope)`:

- **Trigger** (any combination): `max_age` (dead letters older than T), `max_count`
  (keep at most N per queue), `max_bytes` (cap the DLQ's byte footprint). Plus a
  manual trigger — which already exists as `purge-dlq`.
- **Action**: `purge` (delete) or `archive-then-purge` (export to cold storage,
  then delete). **Never** touch undelivered / in-flight / unsettled messages — only
  `dead_lettered` (and, by extension, already-`completed` rows, which are already
  deleted at settle).
- **Scope**: broker-wide default, optionally overridden per queue (a short-lived
  high-volume queue vs a long-retention audit queue).

## Safe deletion under high concurrency

This is the part that matters for the North Star: the broker is a **single writer**,
so a careless retention sweep that takes a long write lock would stall *all*
producers. Rules:

- **Batch, don't sweep.** Delete in bounded chunks (`Purge` already supports
  `Max`): `DELETE ... WHERE id IN (SELECT id ... ORDER BY id LIMIT k)`. Each batch
  is a short transaction; the writer yields between batches.
- **Rate-limit.** Sleep briefly between batches so retention is a background trickle,
  not a burst that competes with live traffic. Reuse the existing janitor cadence.
- **Range by primary key.** Deleting by an indexed predicate (`state='dead_lettered'
  AND enqueued_at < cutoff`, ordered by `id`) seeks the `idx_msg_dlq` index instead
  of scanning — the same per-state-index discipline as the claim path (MQLITE-22).
- **WAL-friendly.** Many small commits let WAL checkpoints interleave; one giant
  DELETE balloons the WAL and blocks checkpointing.

## Space reclamation

Deleting rows frees SQLite *pages* but does **not** shrink the file — the freed
pages sit on the free list and the file stays at its high-water mark. Options, in
order of cost:

- **Reuse (default).** Free pages are reused by later inserts. For a queue at steady
  state this is exactly right — the file plateaus at peak backlog and churns in
  place. No action needed.
- **`PRAGMA incremental_vacuum`** (requires `auto_vacuum=INCREMENTAL` set at DB
  creation). Returns a bounded number of free pages to the OS *without a full
  rewrite or a global lock* — a good fit for a background janitor that wants to give
  disk back gradually after a one-off DLQ flush.
- **`VACUUM`** rewrites the whole DB and takes a **global write lock** for the
  duration — unacceptable on the hot path; only ever in an explicit maintenance
  window (CLI command), never automatic.

Recommendation: rely on page reuse; expose `incremental_vacuum` as an *opt-in*
maintenance step (CLI), and document that `VACUUM` is manual-only.

## Archive sink interface (if archival is ever built)

v0.1 ships **no** archival subsystem — but if demand appears, the seam is a single
narrow interface invoked by the retention janitor *before* it deletes a batch:

```go
// ArchiveSink receives dead letters about to be purged. Implementations write to
// cold storage (file/JSONL, object storage, another SQLite). A nil sink = purge
// without archiving (today's behaviour).
type ArchiveSink interface {
    Archive(ctx context.Context, msgs []PeekedMessage) error
}
```

Contract: the janitor only deletes a batch **after** `Archive` returns nil, so a
sink failure stops deletion (at-least-once export, no silent loss). This stays a
*future* extension point, not v0.1 code — the operator pattern below covers real
needs without it.

### Archival as an operator pattern (v0.1)

The existing primitives already compose into archival; document, don't build:

- **Inspect / export:** `Peek(state=dead_lettered)` pages the DLQ; a small script
  writes the bodies wherever you want before purging.
- **Move aside:** `Redrive(--to archive-<queue>)` shovels dead letters into a plain
  archive queue that a separate, slow consumer drains to cold storage.

## Recommendation — tiered, minimal

### Tier 1 (SHIPPED, no schema change): broker-default DLQ retention by age + count + bytes

A `reapDLQ` background loop (alongside the existing janitors) bounds the DLQ by
**age, per-queue count, and per-queue body bytes** — drop-oldest, **batched +
rate-limited**, touching **only `state='dead_lettered'`** (undelivered / in-flight /
scheduled work is never deleted). All three are broker-wide settings enforced over
existing columns (`enqueued_at`, `length(body)`), so Tier 1 needed no schema change.

**On by default for the broker** so it is safe to run online long-term out of the
box (the maintainer's call — the one unbounded sink should not silently fill the
disk). The engine itself defaults to *off* (zero bounds), so the embedded library
never auto-deletes a caller's dead letters unless asked:

```
# mqlite serve (broker) — defaults applied unless overridden:
MQLITE_DLQ_MAX_AGE=336h        # default 14d; dead letters older (by enqueued_at) are dropped
MQLITE_DLQ_MAX_COUNT=1000000   # default 1,000,000 dead letters per queue (drop oldest)
MQLITE_DLQ_MAX_BYTES=0         # optional per-queue body-byte cap; 0 = off (deployment-specific)
MQLITE_DLQ_RETENTION=off       # disable entirely

# embedded library — off unless you opt in:
mqlite.WithDLQRetention(14*24*time.Hour, 1_000_000, 0)
```

Implemented in `engine/background.go` (`reapDLQ`), `engine.Options.{DLQMaxAgeMs,
DLQMaxCount,DLQMaxBytes}`, the `WithDLQRetention` embedded option, and the broker
defaults in `cmd/mqlite` (`embeddedOpts`). Tested in `engine/retention_dlq_test.go`
(drop-oldest by each dimension, age boundary, non-DLQ-untouched, off-by-default).

### Tier 2 (SHIPPED): per-queue overrides

Per-queue overrides (MQLITE-29) add three columns to the `queues` table so a
high-volume queue can keep dead letters an hour while an audit queue keeps them a month
(the Tier-1 bounds are broker-wide):

```
queues.dlq_max_age_ms   INTEGER NOT NULL DEFAULT 0
queues.dlq_max_count    INTEGER NOT NULL DEFAULT 0
queues.dlq_max_bytes    INTEGER NOT NULL DEFAULT 0
```

Each value resolves against the broker/engine default (`reapDLQ` → `effectiveBound`):

| per-queue value | effective bound |
|---|---|
| `0` (default) | **inherit** the broker/engine default |
| `> 0` | this queue's own drop-oldest bound |
| `< 0` (e.g. `-1`) | **explicitly unbounded** — opt out of the broker default |

Set them via `QueueConfig{DLQMaxAge, DLQMaxCount, DLQMaxBytes}` (SDK; `DLQMaxAge` is a
`time.Duration`), the wire `config.{dlq_max_age_ms,dlq_max_count,dlq_max_bytes}`, or
`mqlite create-queue --dlq-max-age 336h --dlq-max-count 1000 --dlq-max-bytes 10485760`.
`reapDLQ` now resolves each DLQ-bearing queue's effective bounds and applies age /
count / bytes per queue (still drop-oldest, batched, `state='dead_lettered'` only).

> These columns changed the schema **incompatibly**. mqlite keeps a single canonical
> schema with no migrations, so (pre-1.0, experimental) `Open` refuses any database
> created before this change (`ErrSchemaVersionMismatch`) — recreate it.

## Caveat: age is measured from `enqueued_at`

`Purge(OlderThanMs)` ages dead letters by their original `enqueued_at`, not the
moment they were dead-lettered (there is no `dead_lettered_at` column — adding one
is a Tier-2 schema change). In practice a message is dead-lettered soon after
enqueue, so `enqueued_at` is a close, arguably-more-honest proxy for total message
age. Tier 1 documents this; Tier 2 can add `dead_lettered_at` for a precise
dead-letter-relative TTL if ever needed.

## Sub-tickets

Tracked in Plane as children of MQLITE-21:

```
MQLITE-28  Tier 1: reapDLQ loop + MQLITE_DLQ_MAX_AGE/MAX_COUNT/MAX_BYTES, default   SHIPPED
           ON for the broker; batched + rate-limited; only state='dead_lettered'; no schema.
MQLITE-29  Tier 2: per-queue overrides (dlq_max_age_ms / dlq_max_count /            SHIPPED
           dlq_max_bytes) — schema change; inherit(0) / override(>0) / unbounded(<0).
MQLITE-30  Document the operator archival pattern (Peek/Redrive/Purge) in README. docs
MQLITE-31  incremental_vacuum maintenance step (CLI `vacuum`, --full) for disk     SHIPPED
           give-back; new local DBs default to auto_vacuum=INCREMENTAL.
```

Status: **MQLITE-28 and MQLITE-29 are shipped** — the broker bounds its DLQ by age +
count + bytes on by default (Tier 1), and each queue may override those bounds with its
own age/count/bytes (Tier 2). **MQLITE-30** is a short README addition;
**MQLITE-31** only when a deployment actually needs to return disk to the OS after a
large DLQ flush.
