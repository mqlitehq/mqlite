# Conformance — correct mqlite behavior (TCK)

This is the normative spec of how mqlite **must** behave — the invariants that define
"correct," independent of any one implementation detail. Each is enforced by a
hermetic test (cross-referenced); together these are mqlite's TCK. If a change would
break a **MUST** here, it is a behavior change, not a refactor.

mqlite is honestly **at-least-once**: a durably-written message is delivered at least
once and never silently dropped; handlers must be idempotent. (§3)

## 1 · Peek-Lock lifecycle

- **1.1** `Receive` claims an `active` message, moving it to `locked` for the queue's
  lock duration, and returns a `lock_token`. *(engine/functional_test.go)*
- **1.2** A `locked` message MUST be invisible to other `Receive` calls until it is
  settled or its lock expires — no message is delivered to two consumers at once.
  *(engine/functional_test.go `TestCompetingConsumersNoDouble`)*
- **1.3** Each claim increments `delivery_count`. *(engine/engine_test.go)*
- **1.4** When a lock expires, the reaper returns the message to `active` (redelivery)
  — or to `dead_lettered` once `delivery_count >= max_delivery_count`. *(engine/engine_test.go)*
- **1.5** Claims are O(log n) on a deep backlog (a partial index on `active` rows; the
  reaper, not the claim path, reclaims expired locks). *(engine/claim_plan_test.go)*

## 2 · Settlement (fenced on `lock_token`)

Exactly one verb per outcome; each is fenced on the `lock_token` from `Receive`.

- **2.1 Complete** removes the message. *(engine/functional_test.go)*
- **2.2 Abandon** returns it to `active` for redelivery (or `dead_lettered` if over
  max); `delay_ms` re-hides it for backoff. *(engine/engine_test.go)*
- **2.3 Reject** moves it to `dead_lettered` with a reason. *(engine/functional_test.go)*
- **2.4 Defer** sets it aside; it is retrieved later by seq via `ReceiveDeferred`.
  Normal `Receive` never returns a deferred message. In an **ordered** queue a deferred
  message **holds the head-of-line** — later messages in its group (or the whole queue
  under `strict_fifo`) are not claimable until it is retrieved and settled; other groups
  proceed. There is no automatic recovery without a TTL (the reaper never reclaims
  deferred rows); recover a lost seq via `Peek state=deferred`.
  *(engine/functional_test.go: TestDeferHoldsHeadOfLine)*
- **2.5 Renew** extends the lock by the queue's lock duration. *(engine/ga_fixes_test.go)*
- **2.6** Settling with a wrong/expired token MUST fail with `ErrLockLost` (HTTP 409)
  — **except** an idempotent replay (a live settlement receipt for that token) returns
  success. *(engine/ga_fixes_test.go)*
- **2.7 CompleteBatch** settles many messages in one transaction with the same per-item
  fencing + idempotency; a stale token yields `ok=false`, never failing the batch.
  *(engine/complete_batch_test.go)*
- **2.8 RenewBatch** extends a whole batch's leases in ONE statement, fenced per item on its own
  `lock_token`, and reports the deadline it committed. `ok=true` means the lease was live when the
  broker's STATEMENT finished — the answer still has to travel, so `locked_until_ms` is the
  authoritative fact and the caller compares it against its own clock. A renewal whose write
  outlived the lock reports `ok=false` rather than handing back a lock the caller never held, and a
  renewal never *shortens* a lease. Capped at 512 messages — a claim about a live lease is only
  keepable within a single statement.
  *(engine/complete_batch_test.go)*

## 3 · Idempotency & at-least-once

- **3.1** On `Open`, every orphaned `locked` row (a crash leftover) MUST be
  reclaimed under the same rule as the reaper: back to `active`, or to
  `dead_lettered` (`MaxDeliveryCountExceeded`) once `delivery_count >=
  max_delivery_count` — a crash never buys an extra delivery.
  *(engine/engine_test.go: TestCrashRecoveryRespectsMaxDelivery)*
- **3.2** A settle whose response was lost MUST replay as success, not `ErrLockLost`
  (`settlement_receipts`, fenced on `lock_token`). *(engine/ga_fixes_test.go)*
- **3.3** A `Receive` retried with the same `AttemptID` MUST replay the same batch /
  same lock tokens, not double-deliver. *(engine/ga_fixes_test.go)*
- **3.4** No message loss + no content corruption: a contiguous `1..N` sequence with
  random bodies (hashed into a property), consumed concurrently with redelivery, is
  delivered completely (every value once) and intact (each body matches its hash).
  *(engine/integrity_test.go `TestMessageIntegrity`)*

## 4 · Deduplication

- **4.1** Within a queue's `dedup_window`, a repeat `message_id` (empty → body SHA-256)
  MUST collapse to a single enqueue. *(engine/functional_test.go, engine/turso_test.go)*
- **4.2** A *single* `Send` whose `message_id` conflicts with a different body MUST
  return `ErrDedupConflict` (HTTP 409). *(server/send_dedup_test.go)*
- **4.3** In a *multi-message* `Send`, a conflicting slot comes back as seq `0`
  (skipped) while the rest of the batch commits. *(wire `SendResponse`; server/send_dedup_test.go)*

## 5 · Ordering modes

- **5.1 `standard`** — per-`GroupID` FIFO with cross-group parallelism; messages with no
  `GroupID` are unordered. Claim eligibility is identical to `group_fifo` — the only
  difference is `group_fifo` additionally requires a `GroupID` at send time. *(engine/functional_test.go)*
- **5.2 `group_fifo`** — strict FIFO per `GroupID` (head-of-line per group); a send
  without a `GroupID` MUST be rejected with `ErrGroupRequired`. *(engine/functional_test.go)*
- **5.3 `strict_fifo`** — global FIFO: at most one message in flight for the queue at a
  time. *(engine/functional_test.go, engine/claim_plan_test.go)*
- **5.4** Head-of-line MUST survive lock expiry: an expired-but-not-yet-reaped lock
  still blocks its group (`strict_fifo`: the whole queue) — successors are never
  delivered ahead of the expired head, and once the reaper resettles it the head is
  redelivered first, in id order (or dead-lettered at `count ≥ max`, which unblocks
  the group). The accepted cost is a group stall of up to one reaper interval on a
  consumer timeout. *(engine/functional_test.go: TestFIFOHoldsAcrossLockExpiry)*

## 6 · Dead-letter queue

- **6.1** A message reaching `max_delivery_count` MUST be dead-lettered with reason
  `MaxDeliveryCountExceeded`. *(engine/engine_test.go)*
- **6.2** `Redrive` moves dead letters back to `active` (or to a target queue); `Purge`
  permanently deletes them (scoped by `Max`/`OlderThanMs`). *(engine/redrive — functional tests)*

## 7 · Scheduling, deferral & TTL

- **7.1 Schedule** keeps a message hidden (`scheduled`, `visible_at` in the future)
  until its time, then the scheduler activates it; `Cancel` deletes a not-yet-active
  scheduled message. *(engine/functional_test.go)*
- **7.2 TTL** — an expired message (`expires_at`) MUST move to the DLQ when the queue
  has `dead_letter_on_expire`, else be discarded. The two branches cover the **same
  state set** — every non-terminal state including `scheduled`: a row the scheduler
  has not yet activated when its TTL lapses (broker downtime, scheduler lag) is
  dead-lettered in place, not resurrected first. (TTL is anchored at `visible_at`,
  so for scheduled messages the clock starts at their delivery time.)
  *(engine/functional_test.go: TestTTLScheduledToDeadLetter)*

## 8 · DLQ retention

- **8.1** With a bound set, `reapDLQ` MUST drop **oldest-first** dead letters past the
  max age, per-queue count, or per-queue body bytes — and MUST touch **only**
  `state='dead_lettered'` (never undelivered/in-flight/scheduled work).
  *(engine/retention_dlq_test.go)*
- **8.2** With no bound (the engine default), the DLQ is unbounded — no auto-deletion.
  *(engine/retention_dlq_test.go `TestDLQRetentionDisabledByDefault`)*
- **8.3** Each queue's effective bound is its own override resolved against the engine
  default (`effectiveBound`): `0` inherits the default, `>0` overrides it, `<0` is
  explicitly unbounded. *(engine/retention_dlq_test.go `TestEffectiveBound`,
  `TestDLQRetentionPerQueueOverride`, `TestDLQRetentionPerQueueInheritAndOptOut`)*

## 9 · Auth & errors (broker)

- **9.1** When `MQLITE_TOKENS` is set, every endpoint MUST require a valid
  `Authorization: Bearer` token **except** the open `/` (discovery), `/healthz`, and
  the static admin console under `/ui` (when enabled — its own API calls still carry a
  token); a missing/invalid token → 401 `unauthenticated`.
  *(server/errors_test.go, server/index_test.go, server/console_test.go)*
- **9.2** Errors use a JSON envelope `{code,message}` with the documented HTTP status:
  400 `invalid_argument`/`group_required` · 401 `unauthenticated` · 404 `not_found` ·
  409 `already_exists`/`lock_lost`/`name_conflict` · 413 `message_too_large` · 500
  `internal`. *(server/errors_test.go; see [api-reference.md](api-reference.md))*

## 10 · Storage & schema invariants

- **10.1** Local file / `:memory:` use a single writer (`SetMaxOpenConns(1)`); a second
  process / second `OpenEmbedded` on the same file MUST fail fast with `ErrDBLocked`.
  *(engine/storage_test.go)*
- **10.2** `Open` MUST refuse a DB whose recorded `schemaVersion` differs from the
  binary's (`ErrSchemaVersionMismatch`) rather than running mismatched DDL against it.
  *(engine/storage_test.go)*
- **10.3** All times are epoch-ms (UTC); the clock is injectable for deterministic
  tests. The remote (Turso) path retries transient errors with backoff; the local
  path never retries. *(engine/storage_test.go, engine/turso_test.go)*

## 11 · Subscription filters

- **11.1** A subscription filter is one `expr` boolean predicate over the message; an
  empty filter matches every message. It is compiled + type-checked at `Subscribe`,
  and a malformed / unknown-field / non-boolean expression MUST be rejected with
  `ErrInvalidFilter` (HTTP 400 `invalid_argument`) and **not** stored — no backing
  queue is created. *(engine/filter_test.go, server/errors_test.go)*
- **11.2** The filter is evaluated at **publish** against the message env (core fields,
  `enqueued_at`/`visible_at`, the derived `subject_parts`/`body_size`/`property_keys`,
  and the body fields `body_text`/`body_json` — projected only when referenced, with
  `body_json` decoded only for a JSON content type, else `{}`); a message is routed to
  a subscription if and only if its filter returns true.
  `enqueued_at` is the publish time and `visible_at` is the delivery time (equal for an
  immediate send, the scheduled time for a delayed one), so a delay is
  `visible_at - enqueued_at`. *(engine/filter_test.go `TestFilterFanoutConditions`,
  `TestFilterScheduledMessageDelay`)*
- **11.3** Evaluation is **fail-closed**: a filter that errors or panics at runtime MUST
  leave the message unrouted to that subscription (logged) — never crashing the broker
  and never matching by default. *(engine/filter_test.go `TestEvalFilterFailClosed`)*
- **11.4** Publishing to a topic that **no** subscription filter accepts is a valid
  no-op (`SendOne`/`Schedule` return `0, nil`), not an `ErrDedupConflict`.
  *(engine/filter_test.go `TestFilterReSubscribeRecompiles`)*

## 12 · Topics & naming

- **12.1** Plain-queue names, subscription names (their backing queues) and topic
  names are **one disjoint namespace**. Every collision MUST be rejected with
  `ErrNameConflict` (HTTP 409) at creation time, in both directions: `Subscribe`
  rejects a topic naming any existing queues row (plain or backing), a subscription
  name that is a plain queue / another topic's subscription / a live topic, and
  `topic == name`; `CreateQueue` rejects a name that is live as a topic and any
  cross-kind upsert. Same-kind upserts (queue reconfig, `(topic,name)` re-subscribe)
  stay open. A failed creation MUST leave nothing behind (guards + inserts are one
  transaction).
  *(engine/functional_test.go: TestTopicQueueNamespaceDisjoint,
  TestTopicSubscriptionIsolation)*
- **12.2** Because names are disjoint, send/publish resolution (topic-first, else
  queue, else `ErrQueueNotFound`) is **unambiguous** — a `Send` can never be silently
  rerouted between a queue and a same-named topic.
  *(engine/functional_test.go: TestTopicQueueNamespaceDisjoint)*

---

*This spec is the contract a non-SQLite storage backend (see the Store-interface
research) would have to satisfy to be a conformant mqlite. CI runs every referenced
test with `-race`; the large no-loss sweep also runs weekly.*
