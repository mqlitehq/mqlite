# Retention & archival — design (MQLITE-21)

A queue that never forgets eventually fills its disk. This note pins down what
MQLite keeps, where the one real gap is (the dead-letter queue), and the
*smallest* change that closes it without re-opening the frozen schema (MQLITE-25)
or bolting an archival subsystem onto a deliberately lightweight broker.

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
deleted immediately (no "completed message archive" to grow), locks self-heal,
TTL is enforced. The DLQ is the only unbounded sink.

## The gap: an unbounded DLQ

Dead-lettered messages persist until an operator runs `Redrive` or `purge-dlq`.
A poison-message storm or an unattended broker therefore grows the volume without
limit — at ~0.4 KB/message (see [resource-profile.md](resource-profile.md)) a
runaway producer can fill a 1 GB Fly volume with ~2.5M dead letters. The DLQ needs
a default ceiling, not just a manual broom.

## Design goals

- **Simple, opt-in, backward-compatible.** Default behaviour unchanged (keep
  forever); retention is something you turn on.
- **Reuse what exists.** `Purge(OlderThanMs)` already deletes dead letters by age
  (`enqueued_at < now − ttl`); the background-loop pattern already runs four
  janitors. Retention is one more janitor, not a new subsystem.
- **Respect the schema freeze (MQLITE-25).** A per-queue config column is a schema
  bump; don't unfreeze the schema for this feature alone.
- **No archival subsystem.** A pluggable "export before delete" pipeline is more
  machinery than a lightweight queue should carry. Archival stays an *operator
  pattern* built from existing primitives.

## Proposal — two tiers

### Tier 1 (v0.1, no schema change): broker-level default DLQ retention

A single broker-wide knob, off by default:

```
MQLITE_DLQ_TTL=168h        # 0 / unset  → keep forever (today's behaviour)
```

A new background loop (`reapDLQ`, alongside the existing janitors) periodically
runs, for each queue, the equivalent of `Purge(OlderThanMs = DLQ_TTL)`. No new
column, no migration, fully backward-compatible. This is the 80 % solution and the
only piece that needs to ship for v0.1.

### Tier 2 (next schema version): per-queue override

When the schema is next bumped for another reason, add:

```
ALTER TABLE queues ADD COLUMN dead_letter_ttl_ms INTEGER NOT NULL DEFAULT 0;
-- (optional) ADD COLUMN dead_lettered_at INTEGER   -- precise age basis, see caveat
```

so a high-volume queue can keep dead letters for an hour while an audit queue keeps
them for a month. Until then, the broker-level default covers every queue. **Do not
bump the schema solely for this** — bundle it with the next schema change.

### Archival (v0.1): an operator pattern, not a feature

The existing primitives already compose into archival; document, don't build:

- **Inspect / export:** `Peek(state=dead_lettered)` pages the DLQ; a small script
  writes the bodies wherever you want (S3, a file, another DB) before purging.
- **Move aside:** `Redrive(--to archive-<queue>)` shovels dead letters into a plain
  archive queue that a separate, slow consumer drains to cold storage.

A pluggable archival hook (`OnPurge(msg)`) is **explicitly out of scope** for v0.1 —
revisit only if real demand appears.

## Caveat: age is measured from `enqueued_at`

`Purge(OlderThanMs)` ages dead letters by their original `enqueued_at`, not the
moment they were dead-lettered (there is no `dead_lettered_at` column — adding one
is a Tier-2 schema change). In practice a message is usually dead-lettered soon
after enqueue, so `enqueued_at` is a close and arguably-more-honest proxy for total
message age. Tier 1 documents this; Tier 2 can add `dead_lettered_at` if a precise
dead-letter-relative TTL is ever required.

## Proposed sub-tickets

```
MQLITE-21a  Tier 1: reapDLQ background loop + MQLITE_DLQ_TTL (off by default).   v0.1
            Reuses Purge(OlderThanMs); per-queue iteration; no schema change.
MQLITE-21b  Tier 2: per-queue dead_letter_ttl_ms (+ optional dead_lettered_at),  next schema
            bundled with the next schema-version bump. Not standalone.
MQLITE-21c  Document the operator archival pattern (Peek/Redrive/Purge) in README. docs
MQLITE-21d  (optional) max-DLQ-depth cap — purge oldest beyond N. Only on demand.  later
```

Recommendation: ship **21a** for v0.1; hold **21b** for the next schema change;
**21c** is a short README addition; **21d** only if a user asks.
