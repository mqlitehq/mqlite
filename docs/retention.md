# Retention, deletion, and storage reclamation

This page follows current `main`, which reports v0.4.1 as an unreleased candidate.
The latest supported release is v0.4.0; use that tag's documentation for deployed
v0.4.0 brokers. v0.3.3 is withdrawn and unsupported; its public artifacts remain
available for forensic or compatibility inspection and must not be deployed. The
v0.4.1 candidate retains v0.4.0's retraction of v0.3.3. v0.3.2 has DLQ retention
and automatic local reclamation, but the work budgets below describe current
`main` unless a version difference is stated.
v0.3.2 does not have the
queue and subscription lifecycle APIs introduced in v0.4.0.

## What removes retained messages

Default Peek-Lock delivery is at-least-once. Receive-and-delete is the explicit
at-most-once broker-removal alternative and removes a message before returning it;
retrying the same receive attempt ID can replay its saved response. A durable
message stays retained until a documented lifecycle action removes or moves it:

| Stored state | Normal lifecycle | Other authorized removal |
|---|---|---|
| `active` | Receive, then Complete or receive-and-delete | message TTL with discard; forced queue/subscription deletion; subscription expiry |
| `locked` | Complete, or lock expiry returns it to `active` / moves it to the DLQ at the delivery limit | message TTL with discard; forced queue/subscription deletion; subscription expiry |
| `deferred` | ReceiveDeferred, then settle | message TTL with discard; forced queue/subscription deletion; subscription expiry |
| `scheduled` | activation at its due time; Cancel only for a scheduled send that has never been delivered | message TTL with discard; forced queue/subscription deletion; subscription expiry |
| `dead_lettered` | Redrive or Purge | configured DLQ retention; forced queue/subscription deletion; subscription expiry |

Complete and receive-and-delete remove the row immediately. A message TTL either
moves an expired message to the DLQ or discards it according to
`dead_letter_on_expire`. The queue default is also a ceiling for an explicit Send
TTL. Its clock starts at the message's initial visibility time, so scheduled waiting
does not consume it; later delayed Abandon does not restart it. Expired locks are
reclaimed by the reaper and can be delivered again, so consumers must be idempotent.

Deduplication rows, settlement receipts, and receive-attempt records have their
own expiry-based background cleanup. They are operational replay metadata, not a
message archive; those expiry deletes are not row-budgeted per pass.

## DLQ retention

`mqlite serve` applies default retention targets to dead letters. Every 60 seconds
the retention worker deletes the oldest matching dead letters in bounded batches.
These are asynchronous cleanup targets, not admission limits: a queue can
temporarily exceed its age, count, or byte target between passes or while cleanup
catches up. The worker never deletes active, locked, deferred, or scheduled rows.

| Setting | Broker default | Meaning |
|---|---:|---|
| `MQLITE_DLQ_MAX_AGE` | `336h` (14 days) | delete dead letters older than this, measured from original enqueue time |
| `MQLITE_DLQ_MAX_COUNT` | `1000000` per queue | keep at most this many dead letters |
| `MQLITE_DLQ_MAX_BYTES` | `0` | optional per-queue cap on dead-letter body bytes; zero disables this bound |
| `MQLITE_DLQ_RETENTION` | enabled | set to `off` to disable all broker defaults |

Embedded engines leave engine-wide DLQ retention defaults disabled unless the
application passes `WithDLQRetention(maxAge, maxCount, maxBytes)`. A positive
per-queue bound still enables that dimension for its queue.

Each queue can override the engine or broker defaults through `QueueConfig`, the
wire `config` object, or `mqlite create-queue`:

| Per-queue value | Effective behavior |
|---:|---|
| `0` | inherit the engine or broker default |
| positive | use this queue-specific bound |
| negative | explicitly leave that dimension unbounded |

The fields are `dlq_max_age_ms`, `dlq_max_count`, and `dlq_max_bytes`. Age uses
`enqueued_at`, not the time at which the message entered the DLQ. Count and byte
limits drop the oldest dead letters first.

Monitor DLQ count, age, and storage size even when defaults are enabled. A growing
active backlog is a consumer-capacity problem and is deliberately outside the DLQ
retention policy.

## Queue and subscription retirement

On `main`, queue and subscription deletion is empty-only by default. `force=true`
is durable authorization to discard all retained states, including locked work,
plus resource-owned deduplication and replay metadata. A locked delivery may
already have caused an external effect; deleting its row cannot undo that effect.

A fixed subscription TTL grants the same discard authorization in advance. Its
absolute deadline is saved on first creation and does not move when the filter is
updated, a message is published or received, or creation is retried. At the
deadline the subscription immediately leaves topic fan-out and its data plane.
Physical cleanup follows in the background. Explicit deletion remains the normal
completion path; TTL is a forgotten-cleanup backstop.

The accepting transaction changes the resource to `deleting` and starts bounded
cleanup. If that cleanup finishes, the same commit removes the resource and the
call returns `deleted`; otherwise the committed `deleting` marker reserves the
name while restart-safe cleanup removes its rows. The foreground request removes
at most 1,000 rows and targets about 8 MiB. Each continuation pass removes
at most 4,096 physical rows and targets about 8 MiB. Its ordinary per-resource
path visits at most 32 local resources, or one such resource on remote Turso/libSQL.
A separate set-based path can finalize up to 512 empty subscriptions
without consuming that 32-or-1 resource-count allowance; their mapping and backing
queue rows still consume the shared row budget. One oversized row may exceed the
byte target so cleanup can still make progress.

The default subscription capacity is 10,000 active plus deleting mappings. A
mapping continues to occupy a slot until physical cleanup finishes. Large forced
deletes or simultaneous expiry waves can therefore create cleanup backlog, grow
the WAL, delay name reuse, and temporarily reject new subscription creation.

For routine request/reply traffic, keep a stable reply subscription and dispatch
with `correlation_id`. If a temporary subscription is necessary, use a globally
unique one-use name, delete it explicitly when finished, add a fixed TTL as a
backstop, and stagger deadlines. Drain before deletion when practical. Reserve
force for intentional abandonment.

One controller must serialize deletion and same-name recreation. After an
uncertain response, use the read-only paged inventory with deleting entries
included. Wait until the name is absent before recreating it, and do not replay an
old delete after a replacement exists.

## Local file reclamation

Deleting a row makes SQLite pages reusable immediately; it does not promise that
the database or WAL file will shrink at once. New local databases use
`auto_vacuum=INCREMENTAL`.

On `main`, the background reclaimer runs every 60 seconds. Once at least 256 pages
are free, it checkpoints and returns at most 256 pages per pass to the operating
system. One-off large deletions can therefore need several passes. v0.3.2 uses the
same interval and threshold but attempts to drain the current freelist in one pass;
it does not have `main`'s 256-page work cap. WAL size can also grow while a reader
holds a snapshot, then shrink after checkpointing when that reader finishes.

Operator commands provide explicit reclamation:

- `mqlite vacuum` checkpoints and exhausts the current incremental-vacuum
  freelist. It is the normal manual choice after a large purge.
- `mqlite vacuum --full` rewrites the database and holds the global writer. Use it
  only in a deliberate maintenance window.

Remote Turso/libSQL manages its physical storage. The client does not expose its
database or WAL file size, and local vacuum commands do not apply.

Size local storage for the peak simultaneous backlog, WAL growth, backup space,
and the time needed for bounded cleanup and reclamation. Alert on free disk,
retained/deleting inventory, maintenance failures, and oldest message age rather
than assuming file size will fall immediately after a delete.

## Exporting dead letters before deletion

MQLite has no built-in archive sink. An operator can use existing operations:

1. Quiesce or otherwise coordinate changes to the selected DLQ range.
2. Read dead letters with `Peek(state=dead_lettered)`, advancing `from` past the
   last exported sequence number, and durably write them to external storage.
3. Verify the external copy.
4. After quiescing, purge only after the export covers the intended DLQ. Use
   `max` or `older_than` bounds where appropriate. Alternatively, Redrive to a
   queue whose consumer writes each message to the archive before completing it.

Peek is non-destructive and does not make the export and purge one transaction.
Redrive moves each selected internal batch atomically: source deletion and target
insertion commit together. Delivery from the dedicated queue to an external
archive remains at-least-once, so the archive consumer must be idempotent; Redrive
does not make the external archival handoff one transaction.
