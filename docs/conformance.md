# Conformance — correct mqlite behavior (TCK)

This is the normative spec of how mqlite **must** behave — the invariants that define
"correct," independent of any one implementation detail. Each is enforced by a
hermetic test (cross-referenced); together these are mqlite's TCK. If a change would
break a **MUST** here, it is a behavior change, not a refactor.

Default Peek-Lock delivery is **at-least-once**: redelivery is possible, so
handlers must be idempotent. Receive-and-delete is the caller's explicit
at-most-once broker-removal path and removes a message before returning it; the
same receive attempt ID can replay its saved response. Other documented
discard paths include message TTL configured to discard, DLQ retention, forced
resource deletion, and fixed subscription expiry. (§1.7, §3, §8, §14)

## 1 · Peek-Lock lifecycle

- **1.1** `Receive` claims an `active` message, moving it to `locked` for the queue's
  lock duration, and returns a `lock_token`. *(engine/functional_test.go)*
- **1.2** A `locked` message MUST be invisible to other `Receive` calls until it is
  settled, its lock expires, or single-broker startup recovery invalidates the
  orphaned lock. Within one running broker, no live lease is granted twice.
  *(engine/functional_test.go `TestCompetingConsumersNoDouble`)*
- **1.3** Each newly granted `Receive` or `ReceiveDeferred` claim increments
  `delivery_count` exactly once. Renewal, settlement, lock reaping, scheduled
  activation and replay of a live receive attempt MUST NOT increment it.
  *(engine/engine_test.go, engine/ga_fixes_test.go, engine/functional_test.go)*
- **1.4** When a lock expires, the reaper returns the message to `active` (redelivery)
  — or to `dead_lettered` once `delivery_count >= max_delivery_count`. *(engine/engine_test.go)*
- **1.5** Claims are O(log n) on a deep backlog (a partial index on `active` rows; the
  reaper, not the claim path, reclaims expired locks). *(engine/claim_plan_test.go)*
- **1.6 Claim time.** `Receive` (with or without an `AttemptID`, including receive-and-delete)
  and `ReceiveDeferred` MUST evaluate TTL eligibility and lease deadlines using fresh broker time
  after writer admission, for each claimed item and each remote retry. A message whose TTL has
  expired at that check MUST NOT be claimed. Waiting for the writer or processing an earlier batch
  item MUST NOT consume a later item's lease before its claim begins.
  *(engine/functional_test.go: TestClaimTimeAfterWriterAdmission, TestClaimBatchUsesEachItemsTime,
  TestClaimBatchTTLBetweenItems, TestReceiveDeferredMixedItemsAndCommittedPrefix;
  engine/storage_test.go: TestClaimTimeRefreshesOnRemoteRetry)*

- **1.7 Receive-and-delete** removes each returned row and grants no lease: its
  lock token is empty and deadline is zero, including a cached attempt replay.
  Peek-lock retains its token and lease deadline. *(sdk_test.go: TestSDKReceiveDeleteLeaseShape)*

## 2 · Settlement (fenced on `lock_token`)

Exactly one verb per outcome; new effects require the `lock_token` from `Receive`
and an unexpired lease, with the exact-request replay exception in §2.6.

- **2.1 Complete** removes the message. *(engine/functional_test.go)*
- **2.2 Abandon** returns it to `active` for redelivery (or `dead_lettered` at/above
  max). With `delay_ms > 0` it parks in `scheduled` until the backoff lapses
  (scheduler re-activates it); on an **ordered** queue the parked head holds
  head-of-line exactly like a scheduled send — successors cannot overtake it
  (§5.5). Neither Abandon nor scheduled activation increments `delivery_count`;
  the next claim does. *(engine/engine_test.go; engine/functional_test.go:
  TestAbandonDelayHoldsGroupOrder)*
- **2.3 Reject** moves it to `dead_lettered` with a reason. *(engine/functional_test.go)*
- **2.4 Defer** sets it aside; it is retrieved later by seq via `ReceiveDeferred`.
  Normal `Receive` never returns a deferred message. In an **ordered** queue a deferred
  message **holds the head-of-line** — later messages in its group (or the whole queue
  under `strict_fifo`) are not claimable until it is retrieved and settled; other groups
  proceed. There is no automatic recovery without a TTL (the reaper never reclaims
  deferred rows); recover a lost seq via `Peek state=deferred`. A successful
  `ReceiveDeferred` increments `delivery_count` but does not gate that explicit
  by-sequence claim on `max_delivery_count`; it can therefore return a count above
  the ordinary delivery budget. Subsequent settlement follows the usual rules.
  *(engine/functional_test.go: TestDeferHoldsHeadOfLine)*
- **2.5 Renew** MUST preserve the same state, token, and `delivery_count`. For a
  queue or subscription without a fixed expiry it sets the deadline to
  `max(old locked_until, broker now + lock_duration)`; a fixed subscription
  lifetime caps the candidate deadline. It MUST NOT shorten a lease or revive one
  whose current deadline has passed. *(engine/ga_fixes_test.go)*
- **2.6** Settling with a wrong/expired token MUST fail with `ErrLockLost` (HTTP 409)
  — **except** an idempotent replay with a live receipt for the exact request (§3.2) returns
  success. Matching the token alone is insufficient: new settlement and renewal effects
  require a locked row with `locked_until_ms > now`, including before the reaper runs.
  The equality boundary is expired. Time is sampled after local writer admission and
  afresh for each settlement statement and remote retry; waiting cannot preserve an
  expired lease or receipt. A live exact-request receipt can still replay a previously
  committed effect after the original lease expires.
  *(engine/ga_fixes_test.go; engine/complete_batch_test.go:
  TestSettlementLeaseDeadline, TestSettlementReceiptIdentityMatrix,
  TestSettlementReceiptTimeAfterWriterAdmission, TestCompleteBatchRefreshesTimeBetweenChunks;
  engine/storage_test.go: TestSettlementTimeRefreshesOnRemoteRetry;
  sdk_test.go: TestSDKSettlementDeadline)*
- **2.7 CompleteBatch** settles many messages in one transaction with the same per-item
  fencing + idempotency; a stale token without an exact-request receipt yields `ok=false`,
  never failing the batch. Single `Complete` and a `CompleteBatch` item share the completed
  operation identity, so the same successful request can replay in either direction.
  *(engine/complete_batch_test.go: TestCompleteBatch, TestSettlementReceiptIdentityMatrix)*
- **2.8 RenewBatch** extends a whole batch's leases in ONE statement, fenced per item on its own
  `lock_token`, and reports the deadline it committed. `ok=true` means the lease was live when the
  broker's STATEMENT finished — the answer still has to travel, so `locked_until_ms` is the
  authoritative fact and remote callers map it from broker time onto a local monotonic clock. A renewal whose write
  outlived the lock reports `ok=false` rather than handing back a lock the caller never held, and a
  renewal never *shortens* a lease. Each successful item uses the same deadline rule
  as §2.5 and preserves state, token, and `delivery_count`. Capped at 512 messages —
  a claim about a live lease is only keepable within a single statement.
  *(engine/complete_batch_test.go)*

## 3 · Idempotency & at-least-once

- **3.1** On `Open`, every orphaned `locked` row (a crash leftover) MUST be
  reclaimed under the same rule as the reaper: back to `active`, or to
  `dead_lettered` (`MaxDeliveryCountExceeded`) once `delivery_count >=
  max_delivery_count` — a crash never buys an extra delivery.
  *(engine/engine_test.go: TestCrashRecoveryRespectsMaxDelivery)*
- **3.2** While its exact-request receipt is live, a settle whose response was lost MUST
  replay as success, not `ErrLockLost` — and ONLY when it is the same request.
  A `settlement_receipt` identifies
  `queue + seq_number + lock_token + operation + effect-bearing arguments`; a call that
  differs in ANY of them is not a replay and MUST return `ErrLockLost`, never a success
  that silently keeps the first call's effect.
  *(engine/ga_fixes_test.go; engine/complete_batch_test.go: TestReceiptsAreBoundToTheirMessage,
  TestReceiptsAreBoundToTheirArguments, TestSettlementReceiptsAreVerbSpecific,
  TestSettlementReceiptIdentityMatrix;
  engine/model_test.go: TestEngineMatchesTheModel, TestModelRejectReceiptArguments)*
- **3.3** A Peek-Lock `Receive` retried with the same `AttemptID` MUST replay the
  same batch and tokens only while every cached token still owns the corresponding
  live, unexpired lock. If any token is stale, the broker MUST invalidate the whole
  cached batch and safely perform a new claim in the same transaction; it MUST NOT
  return a stale or partially owned batch. A tokenless Receive-and-Delete response
  remains replayable for the attempt lifetime. A live non-empty saved response is
  bound to its original receive mode; an empty result creates no record, and an
  expired record no longer binds the ID. Unknown modes MUST be rejected. Record and
  lease expiry MUST be checked against fresh time after writer admission.
  *(engine/ga_fixes_test.go; engine/functional_test.go:
  TestReceiveAttemptExpiryAfterWriterWait,
  TestReceiveAttemptDoesNotReplayStaleLeaseAfterRestart,
  TestReceiveAttemptDoesNotReplayDeadLetteredLeaseAfterRestart,
  TestReceiveAndDeleteAttemptStillReplaysAfterRestart,
  TestReceiveAttemptDoesNotReplayExpiredLease,
  TestReceiveAttemptRejectsModeMismatchWithoutDestroyingReplay,
  TestReceiveRejectsUnknownModeWithoutMutation,
  TestEmptyReceiveAttemptDoesNotBindMode,
  TestExpiredReceiveAttemptDoesNotBindMode,
  TestReceiveAttemptInvalidatesWholePartiallySettledBatch)*
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
- **5.5** Head-of-line MUST survive an explicit backoff: `Abandon` with `delay_ms > 0`
  parks the head in `scheduled` — a state the head-of-line probe covers — so
  successors are never delivered ahead of a backing-off head; when the backoff lapses
  the scheduler re-activates it and it is redelivered first. Other groups (other
  queues' worth of work under `standard`/`group_fifo`) proceed during the backoff.
  *(engine/functional_test.go: TestAbandonDelayHoldsGroupOrder)*

## 6 · Dead-letter queue

- **6.1** Reaching `max_delivery_count` on an ordinary Peek-Lock `Receive` MUST NOT itself
  dead-letter the message: the Nth ordinary Peek-Lock claim is a full final attempt and may
  be renewed and completed. If that locked attempt is Abandoned, its lease expires,
  or it is recovered at startup with `delivery_count >= max_delivery_count`, it MUST be
  dead-lettered with reason `MaxDeliveryCountExceeded` instead of becoming active
  for an ordinary Peek-Lock N+1 claim. This ordinary retry bound does not override the
  explicit `ReceiveDeferred` behavior in §2.4. *(engine/engine_test.go)*
- **6.2** `Redrive` moves dead letters back to `active` (or to a target queue); `Purge`
  permanently deletes them (scoped by `Max`/`OlderThanMs`). *(engine/redrive — functional tests)*

## 7 · Scheduling, deferral & TTL

- **7.1 Schedule** keeps a message hidden (`scheduled`, `visible_at` in the future)
  until its time, then the scheduler activates it; `Cancel` deletes a scheduled
  message that has never been delivered (`delivery_count=0`) — a backoff-parked
  redelivery (§2.2) is NOT cancellable and stays under the retry/DLQ discipline.
  Scheduler activation MUST NOT increment `delivery_count`.
  *(engine/functional_test.go: TestAbandonDelayHoldsGroupOrder)*
- **7.2 TTL** — an expired message (`expires_at`) MUST move to the DLQ when the queue
  has `dead_letter_on_expire`, else be discarded. The two branches cover the **same
  state set** — every non-terminal state including `scheduled`. The TTL sweep can
  expire a scheduled row directly. If the scheduler observes an overdue row first,
  it may change that row to `active`, but claim eligibility still excludes it at
  `now >= expires_at`; the TTL sweep then terminates it without delivery. The queue
  default is both the default and ceiling for an explicit message TTL. Expiry is
  anchored at the message's initial `visible_at`, so for scheduled messages the
  clock starts at their delivery time; later delayed Abandon and scheduler
  activation MUST NOT recompute it. Message TTL is independent of the lock lease:
  it may expire a locked row, and Renew MUST NOT extend it. Embedded and remote
  batch Send MUST preserve each item's requested TTL; a uniform HTTP batch may use
  the batch-wide field, while a mixed batch uses per-message fields. A request that
  combines a positive batch-wide TTL with positive per-message TTLs, or supplies a
  negative TTL in either location, MUST fail as invalid. The Go SDK MUST also reject
  a positive message TTL below the wire's 1 ms precision before sending or writing,
  and an effective deadline that overflows epoch milliseconds MUST fail without
  committing any item in the batch.
  *(engine/functional_test.go: TestTTLScheduledToDeadLetter,
  TestTTLCapAtQueueDefault, TestNegativeMessageTTLRejectedBeforeWrite,
  TestMessageTTLExpiryOverflowRejectedAtomically,
  TestMessageTTLDeadlineSurvivesRenewAndDelayedRetry; sdk_test.go:
  TestBatchTTLRemoteEmbeddedParity, TestNegativeMessageTTLSDKParity,
  TestRemoteBatchTTLWireCompatibility; server/send_dedup_test.go:
  TestServerSendPerMessageTTL; engine/turso_test.go:
  TestTursoMessageTTLParity; test/api_tests.py: t_message_ttl_contract)*

## 8 · DLQ retention

- **8.1** With a bound set, `reapDLQ` MUST drop **oldest-first** dead letters past the
  max age, per-queue count, or per-queue body bytes — and MUST touch **only**
  `state='dead_lettered'` (never undelivered/in-flight/scheduled work). These are
  asynchronous retention targets rather than admission limits; a DLQ MAY exceed
  them temporarily between bounded worker passes.
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
  token); a missing/invalid/expired/revoked token → 401 `unauthenticated`.
  *(server/errors_test.go, server/index_test.go, server/console_test.go)*
- **9.2** Errors use a JSON envelope `{code,message}` with the documented HTTP status;
  `not_found` may add the backward-compatible `resource` discriminator `queue` or
  `subscription`:
  400 `invalid_argument`/`group_required` · 401 `unauthenticated` · 403
  `permission_denied` · 404 `not_found` · 409
  `already_exists`/`lock_lost`/`name_conflict`/`key_conflict`/
  `queue_not_empty`/`subscription_not_empty` · 413 `message_too_large` ·
  429 `resource_exhausted` · 500 `internal` · 503 `outcome_unknown`.
  *(server/errors_test.go; see [api-reference.md](api-reference.md))*
- **9.3** Every registered RPC MUST bind a permission in the route registration.
  Send/Schedule/Cancel require send; all other QueueService methods require listen;
  subscription upserts and filter dry-runs require listen; all other AdminService
  and all AuthService methods require manage, except Observe and the opt-in `/metrics` endpoint also
  accept configured read-only monitor credentials. Both empty-only and forced
  subscription deletion require manage. Metrics
  MUST be disabled by default, return 404 while disabled, and appear as `/metrics`
  in discovery only while enabled. Enabling metrics MUST require administrator
  auth. Manage includes both data permissions. Configured and managed
  administrators MUST have identical operation rights, including issuing manage
  keys. Insufficient permission MUST
  fail before the handler can read or change protected data.
  *(server/errors_test.go: TestAccessKeyCompletePermissionMatrix,
  TestListenSubscriptionProvisioningBoundary;
  server/metrics_test.go: TestMetricsDefaultOffAndAnonymousObserveDenied,
  TestMetricsEnabledRequiresAdministratorAuthentication, TestMetricsPermissionMatrix,
  TestMetricsDisabledSkipsDatabaseAuthentication)*
- Configured monitor credentials MUST NOT grant message access or administration,
  MUST be distinct from administrators, and MUST remain usable for fault telemetry
  without database-backed authentication. Failed collection MUST omit queue gauges
  and explicitly report unavailable data, preserving real in-memory counters.
  *(server/metrics_test.go: TestMonitorConfigurationFailsClosed,
  TestObserveStorageFailureAvailability, TestObserveAuthOutcomesAndWholeRequestCoverage)*
- **9.4** Managed keys MUST survive restart and store only a SHA-256 digest of
  their token. New tokens MUST be `mqk_` plus 64 lowercase hex characters from
  a secure random source. Existing configured tokens remain accepted exactly.
  Only the successful creation response returns a secret; list, errors and logs
  MUST NOT return tokens or digests. A duplicate public ID MUST NOT mint a second
  credential. Auth-off mode MUST refuse key management.
  *(engine/storage_test.go: TestAccessKeyLifecycle, TestAccessKeyValidation;
  server/errors_test.go: TestAccessKeyManagementLifecycleAndSecrecy,
  TestAccessKeyErrorsDoNotEchoSecrets; token_test.go)*
- **9.5** Successful revocation MUST reject subsequent authentication; already
  authorized requests may finish. Static configuration takes precedence over a
  matching database record. The SDK MUST treat permission denial as permanent in
  receive, settlement and renewal paths, without unauthorized batch fallback.
  *(server/errors_test.go: TestAccessKeyRevocationAndExpiryBoundaries;
  sdk_test.go: TestManagedKeySDKAuthorization; receiver_internal_test.go;
  cmd/mqlite/safety_test.go)*
- **9.6** Access-key lists MUST retain ID-ascending order by default. Explicit
  `created_desc` MUST order the complete result by creation time descending, then
  ID descending, with stable cursor pagination. Revocation MUST NOT move a key;
  insertion ahead of a cursor MUST NOT repeat or skip existing keys on later
  pages. Unknown sort values and unknown creation-order cursors MUST fail with
  `invalid_argument`. SDKs and tools MUST reject incorrectly ordered responses.
  *(engine/storage_test.go: TestAccessKeyCreationOrderPagination,
  TestAccessKeySortValidationAndIndex; server/errors_test.go:
  TestAccessKeyCreationOrderHTTP; wire/wire_test.go:
  TestAccessKeyCreationOrderResponseValidation; test/api_tests.py)*
- **9.7** Every HTTP response MUST carry `X-Mqlite-Server-Time-Ms`, sampled from
  the same injectable epoch-millisecond broker clock used for lease and TTL
  decisions when the response is first written. The additive header is valid on
  both success and error responses; a client MUST NOT use a proxy-generated
  failure or redirect as evidence of broker time. A remote lease scheduler MUST
  apply transport and timestamp-precision uncertainty conservatively, reserve
  observed round-trip headroom for the next renewal, and MUST NOT extend a lease
  merely because its local wall clock falls inside that interval. A confirmed
  exact lease shorter than the managed Receiver's safe renewal interval MUST fail
  closed rather than enter an unbounded renewal loop.
  CORS-enabled responses MUST list the header in `Access-Control-Expose-Headers`.
  *(server/errors_test.go: TestJSONResponsesCarryServerTime;
  receiver_internal_test.go: TestClientMapsBrokerDeadlineOntoLocalMonotonicClock,
  TestClientLongPollNeverExtendsLeaseFromLocalWallClock,
  TestClientLongPollObservationDoesNotContaminateShortRenewal,
  TestClientRejectsPathologicallyShortConfirmedLease)*
- **9.8** A managed Receiver that receives HTTP 499 while its own controlling
  context is still live MUST treat it as transient. A Receive replay MUST retain
  the same attempt ID. If the caller context is already canceled, it MUST stop
  without another request. *(receiver_internal_test.go:
  TestReceiverRetriesBrokerCanceledReceiveWithSameAttemptID,
  TestReceiverDoesNotRetryBrokerCanceledResponseAfterCallerCancel)*

## 10 · Storage & schema invariants

- **10.1** Local file / `:memory:` use a single writer (`SetMaxOpenConns(1)`); a second
  process / second `OpenEmbedded` on the same file MUST fail fast with `ErrDBLocked`.
  A local file MAY use one separate `mode=ro` plus `query_only` WAL connection
  for observation reads; it MUST reject writes and checkpoints and share the
  engine close gate. The global queue snapshot MAY be reused for about 10
  seconds, MUST preserve its original
  `sampled_at_ms` and internal topology fence as one cache entry, MUST coalesce
  concurrent refreshes, and MUST NOT cache a failed refresh. Observe
  `server_time_ms` MUST be read separately from the same injectable lifecycle
  clock and MUST NOT be retained in that queue snapshot cache. Per-queue `Stats`
  remains a fresh indexed read.
  *(engine/storage_test.go, engine/observability_performance_test.go)*
- **10.2** `Open` MUST refuse a DB whose recorded `schemaVersion` differs from the
  binary's (`ErrSchemaVersionMismatch`) rather than running mismatched DDL against it.
  *(engine/storage_test.go)*
- **10.3** All times are epoch-ms (UTC); the clock is injectable for deterministic
  tests. The remote (Turso) path retries transient errors with backoff; the local
  path never retries. A successful null-baton `libsql://` response closes that
  logical Hrana stream: it MUST NOT be retained in an idle slot or handed directly
  to a waiting caller. One transaction MUST remain on one logical stream through
  commit or rollback, and the local pool MUST remain MaxOpen=1 with no connection
  lifetime recycling. *(engine/storage_test.go:
  TestHranaClosedStreamIsDiscardedBeforeFreshReuse,
  TestHranaTransactionKeepsOneLease,
  TestMemoryDBSurvivesRepeatedFreshStatements; engine/turso_test.go)*
- **10.4** Access-key table and index additions remain independently additive
  inside the canonical schema. Verify complete backup/restore content,
  including active, expired and revoked credentials.
  An incompatible existing object named `access_keys` or
  `idx_access_keys_created` MUST fail rather than be overwritten or silently trusted.
  *(engine/storage_test.go: TestAccessKeySchemaAdditiveCompatibility,
  TestAccessKeySchemaConflict, TestAccessKeyCreationIndexUpgradeAndConflicts;
  test/production/restore/run.py, compat.py)*
- **10.5** Durable resource lifecycle and fixed subscription-expiry columns and
  indexes make the v0.4.0 schema token 7 incompatible with the older schema-5
  layout and intermediate schema-6 layout. Each binary MUST refuse the other's
  database rather than adding columns, restamping the token or partially opening
  it. Operators must create a fresh database and preserve the matching old
  binary/backup pair. *(engine/storage_test.go:
  TestSchemaVersionGuard, TestSchemaContentPinnedToVersionToken)*
- **10.6 Backend parity.** Given the same committed state, broker time and request,
  local SQLite and remote Turso/libSQL MUST produce the same successful result,
  validation outcome, state transition and logical sentinel error. Latency,
  temporary availability, remote transport failures (including
  `ErrOutcomeUnknown`), remote transaction-callback replay and explicitly
  operational deployment, maintenance and observation behavior MAY differ; they
  MUST NOT silently change deterministic queue semantics.
  *(engine/turso_test.go: TestTursoIntegration, TestTursoExtended,
  TestTursoMessageTTLParity, TestTursoConcurrent, TestTursoBatchSettle;
  engine/storage_test.go; server/errors_test.go; sdk_test.go)*

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
- **12.2** Because names are disjoint, send/publish resolution is **unambiguous**:
  an active plain-queue or backing-queue name addresses that queue; an implicit
  topic name addresses its active subscription rows; any other name returns
  `ErrQueueNotFound`. Behavior after directly altering internal tables to create a
  queue/topic collision is outside the public contract.
  *(engine/functional_test.go: TestTopicQueueNamespaceDisjoint)*
- **12.3 Subscription lifecycle.** Deletion MUST select the exact `(topic,name)`
  pair. By default it MUST reject every retained message state with
  `ErrSubscriptionNotEmpty`; force is durable authorization to discard all states.
  Acceptance MUST atomically exclude the backing queue from fan-out and the data
  plane and start bounded cleanup. If that cleanup finishes in the accepting
  transaction, it MUST remove the queue and mapping and return `deleted`. Otherwise
  it MUST commit a `deleting` marker that preserves the mapping and name until a
  later transaction removes both. Restart MUST resume accepted pending cleanup
  without another force authorization. The last active subscription's retirement
  makes its implicit topic unavailable immediately.
- **12.4 Bounded inventory.** The compatibility `ListSubscriptions` operation MUST
  return every active subscription in `(topic,name)` order. The paged operation
  MUST use the same order, default to 100, reject limits above 1,000, return an
  exact total for the request's scope and optionally include deleting entries.
  Every paged row MUST carry required non-negative `created_at_ms` and
  `expires_at_ms`; zero expiry means permanent and a positive expiry MUST be
  greater than creation. An expired retained row is logically deleting.
  Its opaque cursor MUST be bound to the topic and include-deleting scope. One
  request is a consistent snapshot; pages issued as separate requests do not
  promise a frozen snapshot under concurrent changes.
- **12.5 Capacity.** The engine MUST bound active plus deleting subscriptions
  (default 10,000). Creating a new mapping at the bound returns
  `ErrSubscriptionLimit` (HTTP 429 `resource_exhausted`); re-subscribing an active
  exact pair may update its filter. A deleting entry MUST retain its slot until
  final physical cleanup. Public `CreateQueue(kind=subscription)` MUST NOT create
  a backing queue without a subscription mapping.
- **12.6 Fixed subscription lifetime.** A positive creation TTL MUST store one
  absolute deadline from the first successful transaction. Omitted/zero TTL on a
  new subscription makes it permanent; on an existing subscription it preserves
  the original lifetime. The same positive TTL is idempotent, while a different
  positive TTL MUST return `ErrNameConflict` before changing the filter. Retry,
  filter update, message activity and restart MUST NOT extend the deadline. At
  `now >= expires_at` fan-out and every direct data-plane effect MUST reject the
  subscription, cached metadata included. Locks and long polls MUST end no later
  than the deadline; Renew MUST NOT extend it, and idempotency records MUST NOT
  authorize work after it.
  *(engine/functional_test.go, engine/engine_test.go,
  engine/complete_batch_test.go; sdk_test.go; server/subscriptions_test.go)*
- **12.7 Expiry cleanup.** Expiry is durable advance authorization to discard all
  retained message states. The indexed scanner MUST transition due subscriptions
  to the existing restart-safe deletion lifecycle, while correctness remains
  independent of scan timing. Empty subscriptions MAY use set-based cleanup only
  when mapping and backing rows are removed in one transaction with exact row
  counts, including when foreign keys are off. Nonempty subscriptions remain
  subject to the global row/byte/fairness bounds, and every retained row keeps its
  capacity slot until physical removal. *(engine/lifecycle_test.go,
  engine/storage_test.go)*

## 13 · Cancellation (context deadlines)

Cancelling a call is not a way to *undo* it, and on a local store it is not a latency knob
either. The rules below are the whole contract; they are stated here, and not only in the
changelog, because a caller can observe every one of them.

- **13.1** A mqlite operation whose context is already cancelled MUST NOT execute anything. This holds
  at both boundaries — before the wait for the writer, and again after the connection is in
  hand — and before entering a local `Tx` callback.
  *(engine/cancel_test.go: TestPreCancelledNeverExecutes,
  TestCancelInTheHandoffWindowStartsNothing, TestNothingBeginsInTheLastWindowBeforeAStatement;
  sdk_test.go: TestEmbeddedTxCancellation)*
- **13.2** A caller waiting for the single writer keeps its own deadline: cancelling while
  queued MUST return promptly and MUST mutate nothing.
  *(engine/cancel_test.go: TestCancelWhileWaitingWritesNothing)*
- **13.3** On a **local** store (file or `:memory:`), a statement that has already begun
  executing MUST be allowed to finish. It is not interrupted, and **there is no upper bound on
  how long it takes** — `EngineTx.SQL()` runs arbitrary user SQL, a full `VACUUM` rewrites the
  file, a large `Purge` is a big `DELETE`. A write can therefore commit *after* its caller has
  given up. This is deliberate: interrupting a local statement leaks the driver connection,
  which leaves a file DB locked (`SQLITE_BUSY`, permanently) or destroys a `:memory:` DB
  outright. *(engine/cancel_test.go: TestCancelStormLeavesTheDatabaseUsable,
  TestCancelledSettleWritesLeaveTheDatabaseUsable)*
- **13.4** Inside a local transaction, **mqlite-owned statements** (including `EngineTx.SendOne`)
  check the caller's context before each statement: one already running finishes, and a statement
  whose check observes cancellation MUST NOT begin. `EngineTx.SQL()` exposes a raw `*sql.Tx`
  outside those guards for caller-owned tables; directly modifying mqlite-owned tables is outside
  the supported contract. Its statements MUST use `tx.Context()` to avoid interrupting local SQLite;
  that context is deliberately not cancellable, so raw SQL can still execute after the caller
  cancels. Callers MUST check the original context between their own statements and return promptly.
  Mqlite cannot interrupt the callback or release the single writer before it returns. Cancellation
  observed after the callback and before commit MUST roll back the whole transaction, including raw
  SQL writes, even if the callback returned nil.
  *(engine/cancel_test.go: TestCancelledTransactionStopsIssuingStatements,
  TestTxCancelledBetweenStatementsRollsBack; sdk_test.go: TestEmbeddedTxCancellation)*
- **13.5** On a **remote** (Turso/libSQL) store, statement cancellation is real and unchanged:
  those statements can genuinely block on the network, and a discarded connection there holds
  no local lock.
- **13.6** Follows from 13.3: a lost response never means "it did not happen". Cancellation
  does not change the at-least-once contract — reconcile, do not assume.

## 14 · Concurrency invariants

The properties a user cannot work around, asserted under real parallelism rather than by asking the
engine to grade itself — an engine with a bug reports that everything is fine.

- **14.1 Exclusive delivery.** While a consumer holds a **live** lock on a message, that message
  MUST NOT be delivered to anyone else. Exclusivity *is* the lease: past `locked_until` the holder
  owns nothing, and a redelivery then is the at-least-once contract working, not a violation.
  *(engine/concurrency_test.go: TestConcurrentConsumersNeverShareAMessage)*
- **14.2 The fence must fence.** When a lock expires and the message is reassigned, a settle from
  the OLD holder MUST be refused (`ErrLockLost`) and the new holder's MUST succeed. "Exactly one
  winner" is not sufficient — the winner must be the holder of the live lock.
  *(engine/concurrency_test.go: TestConcurrentSettlementPicksExactlyOneWinner)*
- **14.3 No zombie delivery.** A completed message MUST NOT be delivered again.
  *(engine/concurrency_test.go)*
- **14.4 No loss.** Every message sent is eventually completed — not most of them.
  *(engine/concurrency_test.go)*
- **14.5 Shutdown fences the store.** `Close` closes admission before anything else: DB work
  that begins after shutdown starts MUST fail fast with `ErrClosed`; work already admitted —
  a statement, row scan, transaction callback, or commit in flight — MUST complete before the
  file lock is released, and a second `Open` on the same file during that window MUST be
  refused (`ErrDBLocked`); an empty long-poll `Receive` MUST be woken with `ErrClosed` rather
  than sleeping out its poll window against a closed engine. `Close` MUST NOT be called from
  inside a `Tx` callback — it waits for the callback, so that deadlocks.
  *(sdk_test.go: TestEmbeddedCloseWaitsForPublicTx · engine/storage_test.go:
  TestCloseWaitsForInFlightWritesBeforeReleasingTheLock, TestCloseWakesLongPollWaiters)*

## 15 · Crash recovery (process death)

Asserted by killing a real process hard (SIGKILL / TerminateProcess — no cleanup, no flush) while it
is mid-transaction, restarting, and checking what survived. This covers process death, NOT power
loss: a hard kill does not lose data the OS already accepted, so it exercises application-level
recovery, not filesystem durability.

- **15.1 Outbox atomicity across a crash.** A transaction that writes a business row AND enqueues a
  message commits both or neither. After recovery the set of business rows equals the set of
  messages exactly: never an order without its message, never a message without its order.
  What each crash mode establishes, stated exactly:
  - *Coordinated mode (deterministic):* every run crashes the producer with a transaction held open
    **between its two writes**, so a torn callback — or an outbox split into two transactions — is
    caught on every run.
  - *Random mode (activity deterministic, placement probabilistic):* each kill is **gated** on
    stream-order evidence — two post-challenge ACKs, one discounted as a possible pre-challenge
    straggler — so **every** kill window provably contains a commit made after the liveness
    challenge, on any host: the evidence is a wait condition on the kill, not a throughput floor a
    slow runner could trip (per-window counts are logged). On a normally loaded host the evidence
    exists long before the kill timer expires, so the kill stays purely timer-driven. A frozen work
    loop never produces the evidence and fails deterministically. **Where** each kill lands within
    that live stream is probabilistic, so a crash falling exactly inside a hypothetical
    two-*commit* gap is supplementary coverage, not a per-run guarantee.

  *(test/crash/crash_test.go: TestCrashOutboxAtomicity)*
- **15.2 Orphaned locks reset on restart.** After a crash while messages are `locked`, `Open` reclaims
  every orphaned lock — to `active`, or to `dead_lettered` with `MaxDeliveryCountExceeded` if the
  message was already on its last permitted delivery (the same rule the reaper applies, §3.1). Either
  way nothing is stranded `locked`. The last-delivery case preserves the message body and delivery
  count, clears the lock, and excludes it from normal receive. A cached Peek-Lock
  attempt MUST NOT replay a token invalidated by this recovery; its retry either
  obtains a currently valid new lease or observes no available message.
  *(test/crash/crash_test.go: TestCrashRecoveryResetsOrphanedLocks,
  TestCrashRecoveryDeadLettersLastDelivery,
  TestCrashRecoveryInvalidatesReceiveAttemptLease; engine/functional_test.go:
  TestReceiveAttemptDoesNotReplayStaleLeaseAfterRestart,
  TestReceiveAttemptDoesNotReplayDeadLetteredLeaseAfterRestart)*
- **15.3 Acknowledged commits survive recovery.** Every producer commit acknowledged to and
  observed by the harness before a kill MUST still be present after recovery. The harness checks
  those acknowledged transaction identities against both recovered business rows and messages.
  Commits whose acknowledgements were not observed remain subject to the all-or-nothing check
  in §15.1. *(test/crash/crash_test.go: TestCrashOutboxAtomicity)*

Run: `make crash` (race-instrumented, tag-gated; a separate Linux CI job, excluded from the default
package matrix).

---

*This spec is the contract a non-SQLite storage backend (see the Store-interface
research) would have to satisfy to be a conformant mqlite. Hermetic package tests run with `-race`
in the OS/Go matrix; tagged crash tests run with `-race` in their own Linux job. Live Turso tests
run separately without `-race`; the large no-loss sweep also runs weekly without `-race`.*
