# Retention & archival — design (MQLITE-21)

MQLite targets high throughput, and a high-throughput store on SQLite grows fast.
If nothing ever shrinks the database, write latency climbs, the volume fills, and
the broker eventually stalls — the opposite of the "lightweight, reliable" goal.
This note is the design investigation: what MQLite keeps today, how other queues
bound their storage, the full design space (triggers, actions, safe deletion,
space reclamation, archival), and the *smallest* subset to ship for v0.1 without
re-opening the frozen schema (MQLITE-25).

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
~0.4 KB/message ([resource-profile.md](resource-profile.md)) a runaway producer
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

### Tier 1 (v0.1, no schema change): broker-level DLQ age retention

One broker-wide knob, off by default:

```
MQLITE_DLQ_TTL=168h        # 0 / unset  → keep forever (today's behaviour)
```

A new `reapDLQ` background loop (alongside the existing janitors) runs, per queue,
the equivalent of `Purge(OlderThanMs = DLQ_TTL)` — **batched and rate-limited** per
the rules above. No new column, no migration, fully backward-compatible. This is
the 80 % solution and the only piece that must ship for v0.1.

### Tier 2 (next schema bump): per-queue overrides + count/bytes triggers

When the schema is next bumped for another reason, add the per-queue policy:

```
ALTER TABLE queues ADD COLUMN dead_letter_ttl_ms INTEGER NOT NULL DEFAULT 0;
-- optional: dead_letter_max_count, dead_letter_max_bytes, dead_lettered_at
```

so a high-volume queue keeps dead letters an hour while an audit queue keeps them a
month, and count/bytes caps join age. **Do not bump the schema solely for this** —
bundle it with the next schema change (respects MQLITE-25).

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
MQLITE-28  Tier 1: reapDLQ background loop + MQLITE_DLQ_TTL (off by default),     v0.1?
           batched + rate-limited; reuses Purge(OlderThanMs); no schema change.
MQLITE-29  Tier 2: per-queue dead_letter_ttl_ms + count/bytes caps               next schema
           (+ optional dead_lettered_at); bundled with the next schema bump.
MQLITE-30  Document the operator archival pattern (Peek/Redrive/Purge) in README. docs
MQLITE-31  Opt-in incremental_vacuum maintenance step (CLI) for disk give-back.   later
```

Recommendation: **MQLITE-28** is the only piece worth considering for v0.1 (and
even then, manual `purge-dlq` plus the documented operator pattern already make the
DLQ manageable — so v0.1 need not block on it). Hold **MQLITE-29** for the next
schema change; **MQLITE-30** is a short README addition; **MQLITE-31** only when a
deployment actually needs to return disk to the OS after a large DLQ flush.
