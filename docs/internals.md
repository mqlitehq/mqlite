# How mqlite works — building a reliable queue on one SQLite file

> This is the teaching document: how mqlite is actually built, table by table and
> decision by decision — and *why* this construction yields a queue you can trust.
> Nothing here is required to use mqlite; all of it helps you predict what it will
> do. Companion reading: [concepts.md](concepts.md) (the model from the outside),
> [conformance.md](conformance.md) (every promise below, pinned as a test).

---

## 1 · The bet: a queue is rows plus discipline

Strip any message queue to its skeleton and you find three ideas:

1. a **durable list** of messages,
2. a **claim** operation that hands a message to exactly one consumer at a time,
3. **rules for failure** — what happens when that consumer never comes back.

Kafka builds this on a replicated log. RabbitMQ builds it on a custom broker with
its own storage engine. mqlite's bet is that for a huge class of workloads —
background jobs, outboxes, fan-out, pipelines — all three ideas fit inside
something you already trust: **a SQLite database with ACID transactions**.

A message is a row. A state is a column. A transition is one `UPDATE` guarded by a
`WHERE` clause — which under a transaction is an atomic compare-and-swap. Get the
schema and the transitions right, and the reliability story stops being clever
code and becomes **database invariants**.

The rest of this document is those invariants, in the order they earn their keep.

## 2 · The schema: seven tables, one of which matters most

Everything lives in seven `STRICT` tables (`engine/schema.go`). Six carry the
machinery; one carries your data:

| Table | Job |
|---|---|
| `messages` | **the queue itself** — every message, in every state |
| `queues` | per-queue config: lock duration, retry bound, TTL, dedup window, ordering mode, DLQ retention |
| `subscriptions` | the topic fan-out roster (`topic → subscription` + optional filter) |
| `dedup` | duplicate-detection window |
| `settlement_receipts` | makes acknowledgements idempotent (§7) |
| `receive_attempts` | makes receives idempotent (§7) |
| `meta` | one row: the schema guard token (§9) |

The heart, abridged:

```sql
CREATE TABLE messages (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,  -- SQLite rowid = the sequence number
    queue          TEXT NOT NULL REFERENCES queues(name),
    state          TEXT NOT NULL DEFAULT 'active'
                       CHECK (state IN ('active','locked','deferred',
                                        'scheduled','completed','dead_lettered')),
    visible_at     INTEGER NOT NULL DEFAULT 0,   -- delivery time (epoch ms)
    locked_until   INTEGER NOT NULL DEFAULT 0,   -- lock lease expiry
    lock_token     TEXT,                         -- the fencing token (§4)
    delivery_count INTEGER NOT NULL DEFAULT 0,
    expires_at     INTEGER NOT NULL DEFAULT 0,   -- TTL deadline; 0 = none
    body           BLOB NOT NULL,
    -- ... message_id, group_id, subject, properties, dead_letter_reason ...
) STRICT;
```

Three deliberate choices hide in this table:

- **`id INTEGER PRIMARY KEY AUTOINCREMENT` *is* SQLite's rowid** — strictly increasing
  and never reused, even after the row is deleted (it may gap). That single column gives
  mqlite its sequence numbers, its FIFO order, *and* its fastest possible index,
  for free.
- **State is data, not memory.** A message being "locked" is a row value, not a
  broker-side object or a TCP connection. Kill the process at any instant and the
  queue's exact state survives in the file — recovery (§6) is a `SELECT`, not a
  reconstruction.
- **Every timestamp is epoch milliseconds** in plain `INTEGER` columns, and the
  clock is injectable — which is why the failure paths in the test suite run
  deterministically instead of with `sleep()`.

## 3 · The single-writer model: concurrency by subtraction

Here is the design decision everything else leans on. SQLite allows one writer at
a time — most systems fight that; mqlite **builds on it**:

- **In-process, the connection pool has size one** (`SetMaxOpenConns(1)`).
  `database/sql` queues every caller onto that connection, so *all writes are
  serialized before SQLite ever sees them*. There is no lock contention, no
  `SQLITE_BUSY` retry loop, no interleaving to reason about. Two goroutines
  cannot half-claim the same message because their statements cannot overlap.
- **Across processes, an advisory lock says no.** An embedded local file also
  takes an OS lock on a `<db>.lock` sidecar (keyed on the *canonical* path, so
  `./mq.db` and a symlinked spelling collide). A second process opening the same
  file fails fast with `ErrDBLocked` instead of corrupting claims. Want multiple
  processes? Run the broker — one broker *is* the one writer.
- **The file is in WAL mode**, so at the file level readers never block the
  writer and never see torn state. (In embedded mode reads still funnel through
  the same single connection; WAL is what makes that safe and cheap.) `synchronous=NORMAL` is the default
  durability point; `FULL` is a knob (`MQLITE_SYNC`) when an fsync per commit is
  worth ~4–11× of your local throughput (on remote Turso it's a no-op — the
  server owns durability).
- **Remote Turso/libSQL** relaxes the pool to 4 connections because the Turso
  primary serializes writes server-side; it is also the only path that retries
  transient errors — locally a failed statement never ran, so there is nothing
  to retry.

The punchline: mqlite doesn't *implement* mutual exclusion between consumers —
it **arranges to never need it**. Concurrency control by subtraction.

## 4 · Claiming: Peek-Lock is one atomic UPDATE

The hot path — a consumer asking for work — is a single statement
(`engine/claim.go`, abridged):

```sql
UPDATE messages
   SET state = 'locked',
       locked_until   = :now + :lock_duration,
       lock_token     = :random_128_bit_token,
       delivery_count = delivery_count + 1
 WHERE id = (
       SELECT m.id FROM messages m
        WHERE m.queue = :queue AND m.state = 'active'
          AND m.visible_at <= :now
          AND (m.expires_at = 0 OR m.expires_at > :now)
          -- head-of-line probes for grouped messages (§5) go here
        ORDER BY m.id ASC
        LIMIT 1)
RETURNING id, body, delivery_count, ...;
```

One statement, three jobs:

1. **Select** the eligible head (oldest `active`, already visible, not expired).
2. **Claim** it — flip to `locked`, stamp a lease and a token, count the attempt.
3. **Return** the full message to the caller.

Because the single writer executes this atomically, "select then claim" can never
race: whoever's UPDATE runs first gets the row; the next one gets the next row.
A batch `Receive` runs this in a loop **inside one transaction** — one commit,
one fsync, for the whole batch (that change alone took a 1-vCPU broker from 591
to 1,507 msg/s of sustained drain).

**The lock is a lease plus a fencing token.** `locked_until` is a timestamp —
not a connection, not a heartbeat — so a dead consumer's lock simply *expires*.
`lock_token` is 128 random bits handed only to the claimer, and every
acknowledgement must present it:

```sql
DELETE FROM messages WHERE id = :seq AND queue = :q AND lock_token = :token;
```

If the lock expired and the message was redelivered to someone else, the old
token no longer matches — the stale consumer's `Complete`/`Abandon` affects zero
rows and comes back `lock_lost`, instead of silently acknowledging *someone
else's* in-flight delivery. That `WHERE lock_token = ?` clause is the entire
fencing mechanism. (`Renew` is the same trick: one token-guarded UPDATE that
extends `locked_until`.)

## 5 · Staying fast: partial indexes and an O(n²) war story

A claim must find "the oldest active message" without caring how many locked,
scheduled or dead-lettered rows sit around it. The tool is SQLite's **partial
indexes**:

```sql
CREATE INDEX idx_msg_active ON messages(queue, id) WHERE state = 'active';
```

Only active rows live in this index, pre-sorted by `(queue, id)` — so the claim's
inner SELECT is one index seek, O(log n), even with a 200k-row backlog behind a
lock. Each background sweep gets its own partial index the same way (locked by
expiry, scheduled by due time, DLQ by queue, TTL by deadline).

The war story that shaped this (MQLITE-22): grouped messages — on the FIFO
modes, and on `standard` queues whenever a message carries a `group_id` — must
also check that no *earlier* message of the same group is still in flight: a
head-of-line probe.
Written the obvious way — one big `NOT EXISTS (... WHERE ((b.state='locked' AND
b.locked_until > :now) OR b.state IN ('deferred','scheduled')) AND b.id < m.id)`
— SQLite's planner can't match the `OR`/`IN` mixture to any partial index and
falls back to a **backward rowid scan**: O(n) per candidate, O(n²) to drain a
blocked backlog. On a 49k-message queue that was 1.29 seconds *per claim*. The
fix is mechanical once you see it — **one `EXISTS` per state**, each a single
`state = '...'` equality the planner can match to a partial index:

```sql
   ... AND NOT (
       EXISTS (SELECT 1 FROM messages b WHERE b.queue=m.queue
                 AND b.group_id=m.group_id AND b.state='deferred'  AND b.id<m.id)
    OR EXISTS (... b.state='scheduled' ...)
    OR EXISTS (... b.state='locked'    ...))
```

Same logic, three orders of magnitude faster (at a 20k blocked backlog:
29.7 s → 24 ms per claim) — and, since v0.2.0, the `locked` probe deliberately
ignores lease expiry, so a crashed consumer's group stays *in order* (the reaper
redelivers the head first) instead of letting successors overtake it.

The deeper lesson mqlite took from this: **query plans are behavior, so they are
tested**. `TestClaimPlanPinning` runs `EXPLAIN QUERY PLAN` in CI and fails if the
claim ever stops seeking `idx_msg_active`, or if any probe plans as a scan. The
O(n²) bug cannot quietly return.

## 6 · Failure is the normal path

Everything above assumes things go wrong constantly. The machinery that absorbs
it is small and boring on purpose:

**Background loops** (all writing through the same single connection):

| Loop | Cadence | Job |
|---|---|---|
| reaper | 1 s | expired locks → back to `active` — or to the DLQ once `delivery_count ≥ max` |
| scheduler | 1 s | `scheduled` rows whose time arrived → `active` |
| TTL sweep | 10 s | expired messages → DLQ (or discard), from *any* non-terminal state |
| janitors | 60 s | prune old receipts, dedup entries; return freed pages to the OS |

**Crash recovery is one statement.** On `Open`, every `locked` row is an orphan
of a previous process (locks are rows, remember) and is reclaimed — back to
`active`, or straight to the dead-letter state if it died on its final allowed
attempt. Same rule as the reaper; a crash never buys an extra delivery.

**Retries are bounded by data.** `delivery_count` is incremented *at claim time*
inside the claim UPDATE itself, so no failure mode can lose count. When it
reaches the queue's `max_delivery_count`, the reaper/recovery route the message
to `state='dead_lettered'` — the DLQ is not a separate queue, just a state you
can `Peek`, `Redrive` (back to active, count reset) or `Purge`, with retention
bounds (age/count/bytes) so an unattended broker doesn't grow forever.

## 7 · Honest at-least-once, and the two receipt tables

Exactly-once delivery across crashes is not a thing anyone can sell you: after a
crash, "did my acknowledgement land before the power died?" is unanswerable, so
someone must retry, so somebody may see a message twice. mqlite says this out
loud — **delivery is at-least-once; make handlers idempotent** — and then works
to make the *window* small and the *retries* safe:

- **`settlement_receipts`** — when a settle (Complete/Abandon/Reject/Defer)
  succeeds, a receipt keyed by the lock token is written *in the same
  transaction* (kept ~30 minutes). If the client's response got lost and it
  retries, the settle matches zero rows — but finds the receipt, and returns
  success instead of a spurious `lock_lost`. Acknowledgements are idempotent.
- **`receive_attempts`** — a client may tag `Receive` with an `attempt_id`. The
  claimed batch is recorded under that id (same transaction again); a retry of
  the same attempt **replays the exact same messages and lock tokens** instead of
  claiming fresh ones and burning delivery counts.
- **`dedup`** — on the producer side, a `message_id` within the queue's dedup
  window collapses resends into the original row (no id? the body's SHA-256 is
  used as a content-addressed key). Same id with a *different* body is a loud
  conflict, never a silent overwrite — a single send gets `409`; in a batch the
  offending slot is skipped (seq `0`) while the rest commit.

Notice the pattern: each idempotency mechanism is **a small table written in the
same transaction as the operation it protects**. No timeouts-and-hope, no
distributed coordination — the ACID transaction *is* the coordination.

## 8 · The payoff: the queue that lives inside your transaction

Because the queue is a SQLite database, the embedded mode can do something no
external broker can offer:

```go
err := mq.Tx(ctx, func(tx *engine.EngineTx) error {
    if _, err := tx.SQL().ExecContext(ctx,
        `INSERT INTO orders(id, total) VALUES(?, ?)`, 42, 1999); err != nil {
        return err            // rollback: no order, no event
    }
    _, err := tx.SendOne("order-events",
        engine.OutMessage{Body: []byte(`{"order":42}`)})
    return err                // commit: order AND event, atomically
})
```

Your business tables and the `messages` table share one file, so one `*sql.Tx`
covers both. The dual-write problem — "the row committed but the event didn't" —
is not mitigated here; it is **unrepresentable**. This is the transactional
outbox pattern with no outbox table, no poller, and no CDC pipeline.

## 9 · What keeps it true

Every claim in this document is enforced by something that fails loudly:

- **The conformance suite** ([conformance.md](conformance.md)) — the invariants
  (fencing, ordering across lock expiry, recovery bounds, idempotent replays,
  namespace disjointness…) each cite the hermetic test that pins them; CI runs
  them with the race detector on three OSes.
- **Plan-pinning tests** — `EXPLAIN QUERY PLAN` output is asserted, so the
  performance shape is a regression-tested contract, not a hope (§5).
- **A schema golden test** — the DDL's hash is pinned to the schema guard token
  in `meta`. mqlite keeps a *single canonical schema* and refuses to open a file
  from a different one (`ErrSchemaVersionMismatch`); pre-1.0 there are no silent
  migrations, and the golden test makes forgetting the token bump impossible.
- **An integrity sweep** — a randomized send/receive/crash workload asserting
  every message arrives (no loss), arrives intact (content hash), and never
  exceeds its delivery bound.

## 10 · What this design refuses to do

The same choices that buy the reliability set the ceiling, and it's better you
hear it from us:

- **One writer, one node.** No clustering, no failover replicas, no horizontal
  write scaling. (Remote Turso adds storage-level durability and geo-replication
  — at ~45 ms per durable commit, it buys safety, not speed.)
- **Write amplification is physics.** A single 256-byte send costs ~9.7 KB of
  WAL+index writes; batching amortizes it to ~1 KB. That's SQLite's B-tree
  telling you to batch — so batch.
- **Not a streaming log.** Completed messages are deleted, not retained for
  replay; consumer groups over history are Kafka's job, not this one's.

If your problem fits inside those lines — one node, thousands-per-second, must
never lie to you — then the whole stack under your queue is a ~30 MB process,
one file you can copy with `cp`, and a set of promises you can re-verify by
running `go test`.

## 11 · Appendix: swapping the engine — the contract a new store must sign

Does any of this *have* to be SQLite? No. The engine needs a set of
**guarantees**, not a brand — SQLite is simply the implementation that delivers
all of them in one pure-Go file. If you ever wanted to port mqlite onto another
database, an embedded KV store, or something self-built, this is the contract
your store signs. Each item maps back to the section that depends on it.

**The eight core guarantees:**

| # | Your store must provide | Because of |
|---|---|---|
| G1 | **Durable, ordered enqueue** with a strictly-increasing, never-recycled sequence number | §2 — seq is the ordering anchor *and* the settlement handle |
| G2 | **Atomic claim**: select the eligible head *and* lock it as one indivisible operation — two consumers can never win the same message | §4 — the claim UPDATE |
| G3 | **Per-state ordered lookup in O(log n)** on a deep backlog: "oldest active in queue Q", "expired locks", "due scheduled", per group | §5 — the partial indexes |
| G4 | **Secondary lookups**: dedup by message id, DLQ scans, expiry by deadline | §§6–7 |
| G5 | **Multi-row ACID transactions** — and one that the *application can join* with its own tables | §7 receipts, §8 the outbox |
| G6 | **Crash recovery from data alone**: after `kill -9`, the committed state is complete and consistent — locks must be rows, not memory | §2, §6 |
| G7 | **Range deletes with space reclamation** — retention and purge must return disk, not just tombstone | §6 |
| G8 | **Embeddability**: in-process, no CGO, no sidecar, ideally one copyable file | the whole point |

**Four more that this document's fine print implies** — easy to miss, fatal to skip:

- **Conditional writes (CAS).** Every settlement is `... WHERE lock_token = ?`
  (§4). The store needs an atomic compare-on-field update, or fencing collapses
  and a stale consumer can acknowledge someone else's delivery.
- **A serialization story for the claim path.** mqlite gets atomicity by
  funneling all writes through one connection (§3). Your store either offers the
  same single-writer discipline, or true serializable transactions — snapshot
  isolation with write skew is *not* enough for "exactly one claimer wins".
- **Auxiliary writes in the same transaction.** The idempotency receipts (§7)
  only work because the receipt commits *atomically with* the operation it
  protects. A store that can't put the receipt in the same commit turns
  "idempotent" into "usually idempotent".
- **A tunable durability point** (§3's `NORMAL`/`FULL`) and a **multi-process
  guard** (§3's advisory lock) — or a documented equivalent for each.

**The good news: your migration test suite already exists.** The
[conformance TCK](conformance.md) is written against behavior, not against
SQLite — a store that passes it (with the race detector on) has proven G1–G7.
Two things do *not* port and must be re-invented per engine: the plan-pinning
tests (§5's `EXPLAIN QUERY PLAN` assertions are SQLite's dialect — you owe your
new engine an equivalent performance pin, or the O(n²) class returns silently)
and the schema golden test (pin whatever your layout artifact is).

**How the usual candidates grade against this bar:**

- **Another embedded SQL database** — clears everything by construction; the
  work is porting SQL dialect and the performance pins.
- **Embedded KV/LSM stores** (bbolt, Badger, Pebble) — G1/G2/G6 are fine; G3/G4
  mean hand-building every index the schema got for free; and G5's second half
  fails: your application cannot put *its* tables inside *their* transaction, so
  **the outbox — the killer feature — is lost**, and on LSM engines G7's space
  story needs care.
- **An external store** (Redis, Postgres-as-a-service, a broker) — may clear
  G1–G7, but fails G8, and with it the outbox and the "one file you can `cp`"
  operations story. At that point you don't want mqlite — a Postgres-native queue (pgmq, River) fits better.
- **A self-built log/BTree** — every guarantee above lands on your desk at once,
  including the ones you only discover under `kill -9`. You would be rewriting
  the hardest 20 % of SQLite to save the easiest 80 %.

That grading is why mqlite runs on SQLite today: in a weighted evaluation of six
candidates it was the only one delivering all eight guarantees — and uniquely
the two differentiators, the shared-transaction outbox and the Turso remote.
The engine keeps the question open behind a narrow store boundary (enqueue /
claim / settle / recover / in-tx), but the price of admission is this appendix,
with the conformance suite as the judge.

---

*Want the view from outside? [concepts.md](concepts.md). The wire-level details?
[api-reference.md](api-reference.md). The receipts for the performance claims?
[benchmark.md](benchmark.md).*
