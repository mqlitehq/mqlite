# mqlite HTTP API reference

The broker speaks **Connect-style JSON over HTTP**. Every operation is a single
`POST` to `/mqlite.v1.<Service>/<Method>` with a JSON body — curl-able by
construction. This document is generated from the wire contract in
[`wire/wire.go`](../wire/wire.go) (the single source of truth shared by the broker
and the Go SDK, so the two can't drift) and the server's error mapping.

This reference describes v0.4.2, the latest supported release. v0.3.3 is
withdrawn and unsupported; use the documentation in the v0.3.2 tag when working
with its smaller management surface.

Every RPC body must be exactly one non-null JSON object, including operations
whose request is otherwise empty (`{}`). Unknown fields and data after that object
are rejected. Repeated fields retain Go's `encoding/json` behavior: later scalar
values replace earlier ones, while repeated object and map values may merge.

## Conventions

- **Transport:** HTTP `POST`, `Content-Type: application/json`. Discovery and
  health use open `GET` endpoints; opt-in metrics uses authenticated `GET` on the same port.
- **Go SDK transport:** RPC clients never follow HTTP redirects. The default
  client keeps up to 64 idle connections per broker for concurrent receivers and
  releases its own idle connections on `Client.Close`. A client passed through
  `WithHTTPClient` remains caller-owned; MQLite preserves its transport and timeout
  but still disables redirects for RPCs. Size a custom transport's
  `MaxIdleConnsPerHost` to at least Receiver concurrency plus effective prefetch
  (the managed pipeline's maximum network work in flight). If an application has
  replaced the process-wide `http.DefaultTransport` with an opaque wrapper,
  MQLite preserves that wrapper for instrumentation/mock compatibility and does
  not close it; its underlying connection-pool sizing remains the application's
  responsibility because a generic `http.RoundTripper` cannot be cloned or tuned.
- **`body`** is **base64** in JSON (Go marshals `[]byte` as base64).
- **Timestamps** are **epoch milliseconds** (UTC) integers. Message durations use
  `*_ms`; observability durations explicitly named `*_seconds` retain fractional seconds.
- **Broker time header:** every broker response includes
  `X-Mqlite-Server-Time-Ms`, the UTC epoch-millisecond value from the same clock
  used for lease, TTL, scheduling, and settlement decisions. The managed Go
  client trusts it only on successful broker responses and maps absolute lease
  deadlines onto its local monotonic clock with a conservative transport budget.
  Older clients ignore the additive header. With an older broker, the client can
  use HTTP `Date` at its one-second precision; a short lower bound dominated by
  that precision uses bounded degraded-clock pacing. Without either header the
  public absolute deadline is retained, but the managed Receiver reports one
  diagnostic, confirms the lease before dispatch, and keeps a bounded
  untrusted-clock cadence instead of using that foreign timestamp as a local
  schedule. New and reconfigured queues accept either `0` (the 30-second default)
  or a lock duration of at least 1 second. Legacy databases containing a
  subsecond queue remain readable, but this no-header fallback may fail closed
  when it cannot prove enough lease remains. Raw HTTP callers should preserve one
  time header, map broker time, or synchronize clocks.
  CORS-enabled brokers expose this header to browser clients.
- **Sequence numbers** (`seq_number`) are broker-assigned integers, strictly increasing
  and **never reused** (they may gap) — **the handle you settle with**, valid only while
  the message lives. You never set them.
- **Delivery mode:** default Peek-Lock delivery is at-least-once and may repeat,
  so consumers must be idempotent. Receive-and-delete is an explicit at-most-once
  broker-removal path that removes a message before returning it; an explicit
  retry with the same receive attempt ID can replay its saved response.
  Message TTL configured to discard, DLQ retention, forced resource deletion and
  fixed subscription expiry are explicit removal paths; see
  [retention.md](retention.md).
- **One settlement verb per outcome** — `Complete` / `Abandon` / `Reject` / `Defer`,
  no aliases. New effects require the `lock_token` from `Receive` and an unexpired
  lease; exact-request receipt replays are described below.

### Size limits

The message **body** is capped per message, and the whole request body is capped
per RPC call (so an unbounded batch can't OOM the broker before per-message
checks run). There is **no enforced length limit** on the string metadata fields (`subject`, `message_id`, `group_id`, `correlation_id`, `reply_to`,
`content_type`, `properties` keys/values) — they are stored as SQLite `TEXT`; keep them
reasonable (they count toward the JSON request you send, not toward the body cap).

| limit | default | knob |
|---|---|---|
| `body` size | **1 MiB** → `413 message_too_large` over it | `MQLITE_MAX_MESSAGE_BYTES` (serve) / `Options.MaxMessageBytes` (embedded) |
| request body per RPC call | **32 MiB** → `413 message_too_large` | `server.MaxBodyBytes` (bodies are base64; split very large batches) |
| `max_messages` per `Receive` | 256 (max) | request field |
| `wait_time_ms` long-poll | 20000 (max) | request field |
| `Peek` `max` | 32 default, 1000 max | request field |
| retained subscriptions | 10,000 active + deleting | `MQLITE_MAX_SUBSCRIPTIONS` (serve) / `Options.MaxSubscriptions` or `WithMaxSubscriptions` (embedded) |
| filter `expr` source | capped length + AST node count | internal (see [concepts.md](concepts.md#subscription-filters-expr)) |

## The base URL

Every example below is written against `$BASE_URL`, because the port is the one thing a
reader cannot guess — and guessing it wrong is the whole reason the examples say it out
loud now.

```bash
# Talking to the broker directly. mqlite serves JSON-over-HTTP on TCP 6754 and terminates
# no TLS of its own, so this is plain http. When enabled, /metrics uses the
# same listener; terminate TLS in a reverse proxy when required.
BASE_URL=http://127.0.0.1:6754

# Behind a reverse proxy. Public HTTPS on 443 (which the URL omits, as usual) terminates at
# the proxy, which forwards to the broker's internal http://…:6754.
BASE_URL=https://mq.example.com
```

6754 is **not** a second, binary protocol — there is no binary protocol. It is the port the
same JSON-over-HTTP API listens on when nothing is proxying for you.

The discovery card's `endpoints` are route **paths**, deliberately relative: they resolve
against the origin that served the card, so they inherit its scheme, host and port. Fetch the
card from `$BASE_URL` and the paths are usable as-is.
The optional `metrics` field is the same-origin path `/metrics` when enabled;
it is omitted by default. Resolve it against `$BASE_URL` like the other paths.

## Auth

Use one `Authorization: Bearer <token>` header. The broker accepts configured
administrator tokens in `MQLITE_TOKENS` (comma-separated), plus managed access keys
stored in its database. **Secure by default:** if `MQLITE_TOKENS` is **unset**,
`mqlite serve` generates and prints an administrator token at startup. Newly
generated tokens always use `mqk_` plus 64 lowercase hexadecimal characters:
256 random bits, 68 ASCII characters in total. Existing configured tokens remain
valid without changing their format. Token values are case-sensitive.

Set `MQLITE_TOKENS=off` to explicitly disable auth (loopback by default; a remote
bind needs the explicit insecure override). In that mode, the existing queue API
remains open and key-management RPCs are forbidden. An embedded `Serve` without
`WithTokens` has the same explicit auth-off behavior.

With auth enabled, missing, malformed, unknown, expired or revoked credentials
return `401 unauthenticated`; a valid key without the required permission returns
`403 permission_denied`. A key-store failure is a server error, never an auth
bypass. Bare tokens, other authentication schemes and duplicate Authorization
headers are rejected.

### Permissions

Permissions apply to **all queues and subscriptions in this broker**. Use `send`,
`listen`, both, or `manage`; `manage` includes the other two and may issue new
administrator keys. This is consumption permission, not a read-only observer:
receiving, settling and renewing change message state.

| Required permission | Operations |
|---|---|
| `send` | `QueueService/Send`, `Schedule`, `Cancel` |
| `listen` | `QueueService/Receive`, `ReceiveDeferred`, `Peek`, `Stats`, `Complete`, `CompleteBatch`, `Abandon`, `Reject`, `Defer`, `Renew`, `RenewBatch`; `AdminService/Subscribe`, `TestFilter` |
| `manage` | All other `AdminService` methods, all `AuthService` methods, and `/metrics` |
| configured monitor | `AdminService/Observe` and `/metrics` only |

Both receive modes require `listen`, including receive-and-delete. Batch and
idempotent replay paths use the same permission checks as the original operation.
Subscription inventory and deletion require `manage`; `force` changes the deletion
precondition, not its permission. A fixed TTL is creation-time advance authorization
for automatic cleanup and cannot be added to or changed on an existing subscription.
The console's administration views require `manage`. A configured monitor token
opens only its observability views. The public static page alone grants no API access.

In v0.3.2 and later, `MQLITE_MONITOR_TOKENS` configures comma-separated read-only
monitoring credentials alongside administrator authentication. The embedded equivalent is
`WithMonitorTokens`. Monitor and administrator credentials must be distinct;
configuring monitor tokens with authentication disabled is rejected. Monitor
credentials authenticate without querying the key database, so collection failures
remain observable. They cannot peek at bodies, receive, send, change configuration
or issue keys. They are rotated through configuration and a broker restart, not
the managed-key API; `monitor` is not a managed-key permission. See
[observability](observability.md) for a ready-to-run monitoring stack.

Configured administrators and managed `manage` keys have identical operation
rights. Configured credentials are checked first: explicitly placing an existing
managed token in `MQLITE_TOKENS` grants it administrator rights independently of
its database permissions, expiry or revocation. Remove it from configuration to
withdraw that static grant. Database key operations never alter environment
configuration.

### Observe

Available in **v0.3.2 and later**; older brokers do not implement this route.

`POST /mqlite.v1.AdminService/Observe` with `{}` returns the canonical broker
observation used by `/metrics`, the SDK `Observe`, CLI `observe`, MCP `observe`
and console. Requires `manage` or a configured monitor credential.

| Field | Meaning |
|---|---|
| `server_time_ms` | Broker lifecycle clock near response assembly; absent or zero means unavailable, including on an older compatible broker |
| `sampled_at_ms`, `started_at_ms`, `backend`, `version` | Queue sampling instant, engine lifetime and non-sensitive identity |
| `access` | Request capability: `manage`, `monitor` or `anonymous`; SDK embedded mode uses `embedded` |
| `collection` | Queue collection `state`, `last_success_at_ms`, latest queue-gauge refresh `duration_seconds`, bounded `error_code` |
| `queue_count`, `subscription_count`, `queues` | Actual retained state counts and age; counts are usable only when collection is `available` |
| `runtime` | Schema version, read-probe availability/latency and local footprint availability/bytes; no DSN or path |
| `messages` | Engine-lifetime `{queue,event,count}` effects, recorded after confirmed commits |
| `storage` | Bounded operation outcomes, durations, retries and database connection pool measurements |
| `maintenance`, `filters` | Maintenance enablement, results/durations/last success and filter failures |
| `http` | Request outcomes/full elapsed time, final authentication outcomes and post-auth handler histograms |

Successful authentication returns HTTP 200 even if queue collection is
`unavailable`: `queues` is then `null`, with an explicit failure code and the
last successful collection time. In-memory counters remain available. Never
interpret absent queues or unavailable sizes/probes as zero. Authentication itself
still fails closed; a managed key may be unusable while its database is unavailable.
Pure embedded observations mark the HTTP domain `not_applicable`.

The observation is sampled over an interval, not a transaction spanning every
runtime counter. Its queue gauges share one aggregate SQL read and one engine
clock value. Counters reset on engine/process restart; they are not a durable
message ledger. Histograms contain cumulative finite buckets and a total count
that is also the Prometheus `+Inf` bucket. The existing handler histogram excludes
authentication; whole-request duration includes it and normal Receive long polls.
`server_time_ms` is read separately near response assembly, from the same broker
clock used for lifecycle deadlines. It can therefore advance while a cache hit
correctly preserves an older `sampled_at_ms`.

After a queue or subscription is physically deleted, its process-lifetime message
effects move to the documented `messages[].queue == ""` retired aggregate. v0.4.1
corrects the Go decoder and bundled console to accept that special label while
still requiring a complete, unique event domain. Its event semantics and schema
token are unchanged from v0.4.0; v0.4.1 separately adds the optional,
forward-compatible `server_time_ms` observation field described above.

The shared decoder bounds observations to 32 MiB and rejects incomplete objects,
inconsistent availability and malformed histogram data. The complete JSON schema
is represented by [`wire/observability.go`](../wire/observability.go) and
[`engine/observability.go`](../engine/observability.go); metric semantics and
compatibility aliases are specified in [observability](observability.md).

```bash
curl -fsS "$BASE_URL/mqlite.v1.AdminService/Observe" \
  -H "Authorization: Bearer $MQLITE_TOKEN" \
  -H 'Content-Type: application/json' -d '{}'
```

### CreateKey / ListKeys / RevokeKey

These methods are available in v0.3.1 and later; v0.3.0 does not implement
them. All paths are `/mqlite.v1.AuthService/<Method>` and require `manage` with
authentication enabled.

| Method | Request | Response |
|---|---|---|
| `CreateKey` | `id`, `name`, `permissions`, optional `expires_at_ms` | `{"key": <AccessKey>, "token": "mqk_…"}` |
| `ListKeys` | optional `after_id`, `limit`, `sort` | `{"keys": [<AccessKey>], "next_after_id": "…"}`; cursor omitted at the end |
| `RevokeKey` | `id` | `{"ok": true}`; already revoked records succeed again |

`AccessKey` metadata contains `id`, `name`, `permissions`, `created_at_ms`,
`expires_at_ms` and `revoked_at_ms`. Zero expiry means no expiry; zero revocation
time means not revoked. Expired and revoked records remain visible to managers.
Metadata never contains a token or its digest. Lists include only managed keys;
limit defaults to 100 and cannot exceed 1000. Omitted or empty `sort` means
`id_asc` (public ID ascending). Use `sort: "created_desc"` for newest first:
creation time descending, then public ID descending for equal timestamps.

Pass the previous page's `next_after_id` as `after_id`, retaining the same sort.
For `created_desc`, the cursor must identify an existing key; an unknown cursor
or unsupported sort returns `400 invalid_argument`. Revocation and expiry do not
remove records or change their position. Return to the first page to see records
newer than the current cursor. Pagination is not a snapshot of concurrent inserts.
The `id_asc` mode also accepts a valid ID that is not present, which supports
looking up a retained creation ID using its immediate predecessor and `limit: 1`.

Before creation, generate and retain a public ID: exactly 32 lowercase hexadecimal
characters representing 128 random bits. The SDK provides `GenerateKeyID()`; the
CLI generates one if `--id` is omitted. This ID is separate from the secret and
lets you identify a creation whose response was lost. Names are nonempty UTF-8
labels up to 128 bytes without leading/trailing whitespace or NUL. Names need not
be unique. `permissions` accepts `send`, `listen`, or `manage`; any combination
containing `manage` is normalized to `["manage"]`. Expiry is epoch milliseconds,
either zero or a future instant.

The server generates the secret and stores only its SHA-256 digest. Save the
creation response securely: no later operation can recover the same secret. A
duplicate ID returns `409 key_conflict` rather than producing or revealing a key.
Do not automatically retry a creation after an ambiguous response. Reconcile the
retained ID, revoke an undelivered key when confirmed, and use a new ID for its
replacement. An absent row does not prove that an outstanding request cannot
still commit. Never guess which same-named key to revoke.

Revocation rejects subsequent authentication attempts immediately. Requests
already authorized, including a long-poll Receive, may finish. Subsequent
settlement and renewal still require a valid key. Issued keys are independent:
revoking their creator does not revoke them. Change permissions by issuing a
replacement key, moving clients, then revoking the old key.

## Open endpoints (no auth)

| Method | Path | Returns |
|---|---|---|
| `GET` | `/` | JSON discovery card, or the console for an interactive browser (see below) |
| `GET` | `/healthz` | `200 ok` (liveness) |
| `GET` | `/ui` | Embedded admin console (when enabled — see below) |

```bash
curl "$BASE_URL/"                    # what is this? (no auth)
# → {"name":"mqlite","version":"<the broker's version>","description":"...","status":"ok",
#    "auth":"bearer","docs":"https://github.com/mqlitehq/mqlite",
#    "endpoints":["/mqlite.v1.QueueService/Send", ...every RPC route...],
#    "health":"/healthz","ui":"/ui"}
curl "$BASE_URL/healthz"             # ok
```

An interactive browser that explicitly prefers `text/html` receives a temporary
redirect from `/` to `/ui/` when the embedded console is enabled. Requests with no
`Accept` header, `*/*`, or `application/json` continue to receive the exact JSON
card above; the response varies on `Accept`. A headless broker (`MQLITE_UI=off`)
always keeps `/` as JSON. This preserves machine discovery while making direct
browser navigation useful.

The card's `auth` is `"bearer"` when RPCs require a token or `"none"` when auth is off,
so an agent can branch on it directly. `endpoints` is the **complete** list of RPC route
paths (it always matches what the broker serves — a pinned contract). `health` is
the relative liveness path. The optional `metrics` field is `"/metrics"` when
`--metrics` or `MQLITE_METRICS=on` enables the exporter on the API port. It is
omitted when disabled, and the route returns `404` even with valid credentials.
Enabling metrics requires administrator auth; the advertised route still requires
a monitor or administrator credential. No separate URL configuration is needed.

`/ui` serves the **embedded admin console** (a static single-page app baked into the
binary) and is auth-exempt — the page itself loads without a token; its API calls
carry one you paste in. It's on by default; set `MQLITE_UI=off` to run headless, in
which case `/ui` 404s and the card's optional `ui` field is omitted.

## Other endpoints

| Method | Path | Auth | Returns |
|---|---|---|---|
| `GET` | `/metrics` (when enabled) | Bearer with `manage` or configured monitor | Prometheus text: `mqlite_queue_messages{queue,state}` gauges |

## QueueService

All paths are `/mqlite.v1.QueueService/<Method>`.

### Send

Enqueue one or more messages in one transaction.

- **Request** `SendRequest`: `queue` (string), `messages` ([Message]),
  `scheduled_enqueue_time_ms` (int, optional), batch-wide `ttl_ms` (int,
  optional). Each input Message may instead carry its own `ttl_ms`; do not combine
  a positive batch-wide value with positive per-message values.
- **Response** `SendResponse`: `seq_numbers` ([int]) — one per input message, positional.
- **Dedup:** in a multi-message batch, a slot that hits a dedup conflict (same
  `message_id`, different body) comes back as `0` (skipped) while the rest commit. A
  **single-message** Send instead returns `409 already_exists`.

The batch-wide `ttl_ms` applies to every message and remains the compact form for a
uniform batch. Per-message `ttl_ms` supports a mixed-TTL batch. When no positive
batch-wide TTL is present, zero or omission uses the queue default for that item;
otherwise it inherits the positive batch-wide value. MQLite v0.4.1 clients use
the batch-wide field when every item has the same TTL and the per-message field
only when values differ.
An older strict broker rejects that newer mixed-TTL shape instead of silently
changing the TTLs.

A positive queue `default_ttl_ms` is both the default and the ceiling: the
effective TTL is the smaller positive value of the message/request and queue
values; if only one is positive, that value applies; if neither is positive,
expiry is unlimited. Negative TTLs and positive values below the wire's 1 ms
precision are invalid, as is an effective deadline that cannot fit in epoch
milliseconds. At enqueue, `expires_at_ms = initial visible_at_ms + effective
TTL`, so a scheduled Send starts its TTL at its scheduled visibility time rather
than when the request is accepted.
Later renewal, delayed Abandon, and scheduler activation do not move
`expires_at_ms`. This message TTL is unrelated to
`AdminService/Subscribe.ttl_ms`, which is the fixed lifetime of the subscription
resource and its backing queue.

```bash
curl -H "Authorization: Bearer $T" -H 'Content-Type: application/json' \
  --data "{\"queue\":\"orders\",\"messages\":[{\"body\":\"$(printf hi|base64)\"}]}" \
  "$BASE_URL/mqlite.v1.QueueService/Send"
```

### Receive

Peek-Lock (default) or Receive-and-Delete; long-polls up to `wait_time_ms`.
Receive-and-Delete removes the row before returning it and grants no lease:
`lock_token` is empty and `locked_until_ms` is zero (omitted when empty), including
replays with the same receive attempt ID. The SDK exposes a zero `LockedUntil` time.
Every newly granted `Receive` claim increments `delivery_count`. A Peek-Lock
`receive_attempt_id` returns its saved claim without another increment only while
every saved token still owns a live lease. If embedded startup recovery, broker
recovery of an expired lease, settlement, or ordinary lease expiry invalidates any
token, the broker discards that stale response and the retry performs a safe new
claim; it never returns ownership the caller no longer has.
Use an attempt ID only to retry the exact Receive whose response was not observed.
Once the response arrives, do not reuse that ID for another Receive or after
settling part of its batch. Invalidating a partially stale cached response does
not revoke the other live leases from the original response: their original
tokens remain valid until settlement or lease expiry, preventing concurrent
delivery while the original caller may still be processing them. A replacement
claim may therefore return fewer messages or different currently eligible work.
Receive-and-Delete keeps replaying its tokenless saved response for the attempt lifetime.
A live non-empty saved response also binds that attempt ID to its original
`receive_mode`; reusing it with the other mode is `400 invalid_argument`. Empty
results are not saved, and an expired record no longer binds the ID.

- **Request** `ReceiveRequest`: `queue`, `max_messages` (int, default 1, max 256),
  `wait_time_ms` (int, long-poll; max 20000), `receive_mode` (int: `0` peek-lock,
  `1` receive-and-delete; any other value is `400 invalid_argument`),
  `receive_attempt_id` (string, optional idempotency key —
  a Peek-Lock retry replays the same batch while all saved leases remain valid;
  Receive-and-Delete replays its tokenless saved response).
- **Response** `ReceiveResponse`: `messages` ([Message]) — each carries a
  `lock_token` to settle with (peek-lock mode).

```bash
curl -H "Authorization: Bearer $T" -H 'Content-Type: application/json' \
  --data '{"queue":"orders","max_messages":1,"wait_time_ms":5000}' \
  "$BASE_URL/mqlite.v1.QueueService/Receive"
# → {"messages":[{
#      "seq_number":42,"lock_token":"lt_9f3a…","delivery_count":1,
#      "body":"aGk=","message_id":"order-42","group_id":"cust-7",
#      "enqueued_at_ms":1750000000000,"locked_until_ms":1750000030000
#    }]}
# settle it: POST .../Complete {"queue":"orders","seq_number":42,"lock_token":"lt_9f3a…"}
```

### Complete / Abandon / Reject / Defer / Renew

All take `SettleRequest` and return `SettleResponse` `{ "ok": true }`. Settling with
an expired or wrong `lock_token` → `409 lock_lost`, unless a live settlement receipt
matches the exact request: `queue`, `seq_number`, `lock_token`, operation, and the
arguments that change its effect (`delay_ms`, or Reject's reason/description).
This replay rule applies to Complete/Abandon/Reject/Defer; Renew extends a live lease.
New settlement and renewal effects require `locked_until_ms > now`: the exact deadline
already counts as expired, even if the background reaper has not run. The broker samples
time after local writer admission and afresh for each statement and remote retry. A
live exact-request receipt can replay a previously committed settlement after its
original lease deadline; it cannot authorize a different request or revive a lease.

| Method | Extra fields | Effect |
|---|---|---|
| `Complete` | — | delete the message (done) |
| `Abandon` | `delay_ms` (int, optional) | release the lock without incrementing `delivery_count`: below max, redeliver after `delay_ms` (a delayed head keeps holding its group's line on ordered queues); at max, move to the DLQ |
| `Reject` | `dead_letter_reason`, `dead_letter_description` (optional) | move to the DLQ |
| `Defer` | — | set aside; retrieve later by seq via `ReceiveDeferred` |
| `Renew` | — | extend the same live lease without changing state, token, or `delivery_count`; cannot revive an expired lease |

`SettleRequest`: `queue`, `seq_number`, `lock_token` (+ the extras above).

For a queue or subscription without a fixed expiry, Renew commits
`locked_until_ms = max(current locked_until_ms, broker_now + lock_duration_ms)`.
It never shortens a lease, and rapid renewals do not stack durations. For a fixed-life
subscription, its absolute expiry caps the candidate deadline. Renew before the
current deadline; once the lease is expired, Renew fails with `409 lock_lost`.

`max_delivery_count` is evaluated when a locked attempt fails, not when its final
ordinary Peek-Lock claim is granted. For example, with a maximum of 10, an ordinary Peek-Lock `Receive`
may return `delivery_count=10`; that consumer can renew the lease repeatedly and
Complete successfully. If it instead Abandons, lets the lease expire, or leaves the
lock for embedded startup recovery, the message goes to the DLQ and is not ordinarily
claimed an eleventh time. Broker startup preserves that final attempt while its lease
is unexpired; expiry still applies the same DLQ boundary. `ReceiveDeferred` currently
differs, as described below.

> **Defer + ordering.** A deferred message keeps its head-of-line position: in a
> `group_fifo`/`strict_fifo` (or otherwise grouped) queue the messages behind it — its
> group, or the **whole queue** under `strict_fifo` — stay blocked until it is retrieved
> via `ReceiveDeferred` and settled (other groups proceed). Normal `Receive` never
> returns a deferred message, and with no TTL there is no auto-recovery, so recover a
> lost `seq_number` via `Peek` with `state=deferred`.

> **Abandon + ordering.** With `delay_ms > 0` the message parks as `scheduled` until the
> backoff lapses and — like a deferred head — holds its head-of-line position on ordered
> queues: its group (the whole queue under `strict_fifo`) is not delivered ahead of it.
> Below max, no delay makes it redeliver immediately; at max, Abandon moves it to
> the DLQ regardless of `delay_ms`. Abandon and the scheduler do not
> increment `delivery_count`; only the next claim does. Because `Cancel` is restricted
> to never-delivered scheduled sends (`delivery_count=0`), it cannot remove a retry
> parked here.

> **Three separate deadlines.** A lock lease controls one attempt. Message TTL controls
> normal-delivery eligibility and can expire while locked; expiry dead-letters the
> message by default or discards it when queue policy says so. A fixed subscription
> lifetime controls the entire subscription and caps its locks. Renew changes only
> the lease; it does not extend message TTL or subscription lifetime.

### CompleteBatch

Complete many messages in **one round-trip** — settle a whole received batch without
a Complete-per-message N+1 (the cheap path for draining).

- **Request** `CompleteBatchRequest`: `queue`, `messages` ([{`seq_number`, `lock_token`}]).
- **Response** `CompleteBatchResponse`: `results` ([{`seq_number`, `ok`}]) — `ok=false`
  for a stale/expired lock on that item (not fatal to the batch). Each item is fenced +
  idempotent exactly like `Complete`.

The Go SDK requires exactly one result per requested message in the same order,
with no renewal deadline on a completion result. A malformed successful
acknowledgement returns `ErrOutcomeUnknown`, because some completions may already
have committed.

### RenewBatch

Extend the lock lease of a whole received batch in **one round-trip**.

Renewing message-by-message costs one request per message, so on a slow link a renewal pass can
take **longer than the lease it is renewing** — a 64-message batch at 50 ms per call needs 3.2 s
against a 2 s lock, and most of the locks expire mid-pass.

- **Request** `RenewBatchRequest`: `queue`, `messages` ([{`seq_number`, `lock_token`}]).
  **At most 512 messages per call** — more returns `400 invalid_argument`.
- **Response** `RenewBatchResponse`: `results` ([{`seq_number`, `ok`, `locked_until_ms`}]).
  `ok=false` for a stale/expired lock on that item (not fatal to the batch); each item is fenced
  on its own `lock_token`, exactly like `Renew`.

`locked_until_ms` is the absolute broker deadline that was actually committed — renew again before
it. And `ok=true` means the lease was live **when the broker's statement finished** — not when the
answer reaches you. The result still has to be mapped, encoded and travel back, so on a very short
lease it can lapse on the way. Current brokers return `X-Mqlite-Server-Time-Ms` from the same clock
as the lease; the managed Receiver combines that observation with local monotonic time and a
conservative transport budget instead of directly comparing two hosts' wall clocks. It performs a
bounded synchronous Renew before dispatch when a deadline is unusable, separates renewal cadence
from the request watchdog, reserves two observed round trips for the next request, and never extends
a lease merely because the local wall clock falls in a wide uncertainty interval. A confirmed exact
lease below the 50 ms minimum cadence fails closed instead of entering a renewal hot loop. Older
brokers use HTTP `Date` when available; without either time header they retain the public absolute
deadline but never use it as a managed local schedule. The Receiver emits one diagnostic, confirms
ownership before dispatch, and keeps a conservative 250ms cadence. Because no relative lease
duration is available in that fallback, preserve one time header. New and
reconfigured queues enforce a minimum explicit lock duration of 1 second; zero
selects the 30-second default. Legacy subsecond configurations remain readable
but can fail closed when safe renewal cannot be proven.
Raw `RenewBatch` callers should map the broker clock likewise (or keep clocks synchronized) and
must not turn clock offset alone into `ErrLockLost`. (If the write itself outlived the lock, you get
`ok=false` rather than a lock you never really held.) Like single-message Renew, the batch
operation leaves token, state, and
`delivery_count` unchanged and commits the later of the existing deadline and the
broker's fresh `now + lock_duration` candidate (capped by a fixed subscription
lifetime).

The Go SDK requires exactly one ordered result per request item. A successful
renewal must have a positive `locked_until_ms`; a failed renewal must not claim a
deadline. Malformed renewal results are rejected rather than treated as renewed.

> **Why 512?** Renewal is a claim about the *future* ("this lease is live"), and that claim is
> only keepable within a single statement: across several, the first batch's lease can expire —
> and be reaped — while a later one is still running. `Receive` hands out at most 256 messages,
> so a consumer never meets the limit by accident. (`CompleteBatch` has no such limit, because
> completion is terminal: it reports what already happened.)

### ReceiveDeferred

Lock previously-`Defer`'d messages by sequence number.

- **Request** `ReceiveDeferredRequest`: `queue`, `seq_numbers` ([int]).
- **Response** `ReceiveResponse`: `messages` ([Message]).
- Each successful by-sequence claim increments `delivery_count`. It currently does
  **not** reject a deferred message merely because its count is already at
  `max_delivery_count`; after retrieval, normal settlement/DLQ rules still apply.
  Consequently, the ordinary retry path is bounded at its Nth claim, but this
  explicit recovery path can produce a count above N.

### Schedule

Enqueue for future delivery (same wire shape as `Send`).

- **Request** `SendRequest` with `scheduled_enqueue_time_ms` set (epoch ms).
- **Response** `SendResponse`: `seq_numbers`.

### Cancel

Delete a scheduled send that has never been delivered (`delivery_count=0`). A
delayed `Abandon` may also be in `scheduled`, but it cannot be cancelled.

- **Request** `CancelRequest`: `queue`, `seq_number`. **Response** `SettleResponse`.

### Peek

Browse without locking or settling (triage; recover a deferred seq).

- **Request** `PeekRequest`: `queue`, `from_seq` (int, optional), `state` (string,
  optional: `active`/`locked`/`deferred`/`scheduled`/`dead_lettered`), `max` (int,
  default 32, max 1000).
- **Response** `PeekResponse`: `messages` ([Message], includes `state` +
  `dead_letter_reason`/`description`).

### Stats

- **Request** `MetricsRequest`: `queue`.
- **Response** `MetricsResponse`: `queue`, `active`, `locked`, `deferred`,
  `scheduled`, `dead_lettered`, `total`, `oldest_message_age_ms`. The age is measured
  from the oldest active or locked message; it is zero when neither state is present.

```bash
curl -H "Authorization: Bearer $T" -H 'Content-Type: application/json' \
  --data '{"queue":"orders"}' "$BASE_URL/mqlite.v1.QueueService/Stats"
# → {"queue":"orders","active":128,"locked":3,"deferred":0,"scheduled":5,
#    "dead_lettered":2,"total":138,"oldest_message_age_ms":41200}
```

## AdminService

All paths are `/mqlite.v1.AdminService/<Method>`.

### DeleteQueue

v0.4.0 uses the corrected name-only contract below. The incompatible request
shape shipped in the withdrawn, unsupported v0.3.3 release; its public artifacts
remain available for forensic or compatibility inspection and must not be
deployed. v0.4.0 carries the `retract v0.3.3` directive for Go module tools.

Deletes a **plain queue**. Requires administrator/manage permission. The default
is empty-only; force is a separate, explicit authorization to discard retained
work.

- POST `/mqlite.v1.AdminService/DeleteQueue`
- Request: `{"name":"orders"}` or `{"name":"orders","force":true}`.
- A completed deletion returns `{"deleted":true,"state":"deleted"}`. If bounded
  cleanup remains, acceptance returns
  `{"deleted":false,"state":"deleting"}`. Both are successful, authoritative
  outcomes; any other acknowledgement shape is `outcome_unknown` to a client.
- Without force, any retained message row blocks deletion, including active,
  locked, scheduled, deferred, completed and dead-lettered states: HTTP 409,
  `queue_not_empty`. Drain or settle them first. With force, all of those rows are
  permanently discarded. A locked delivery may already have caused an external
  side effect; deleting its broker record cannot undo that effect.
- Subscription backing queues are rejected with HTTP 400, `invalid_argument`;
  this operation does not remove topic subscriptions.
- The accepting transaction changes the queue to `deleting` and starts bounded
  cleanup. If cleanup finishes, the same commit removes the queue and returns
  `deleted`; otherwise it commits the `deleting` marker. From that point, queue
  operations are rejected without taking effect. Name-addressed
  operations such as Send, Receive, Peek and Stats return `not_found`; a stale
  lock settlement may instead report `lock_lost`. An existing long poll is woken,
  `CreateQueue` with that name returns
  `name_conflict`, and `ListQueues` continues to expose the reserved name with
  `state:"deleting"`. The discard authorization is durable, and background
  cleanup needs no follow-up delete.
- One foreground call removes at most 1,000 physical queue-owned rows and targets
  about 8 MiB. A single larger row is still removed so cleanup can progress. A
  pending delete wakes a background pass that globally caps cleanup at 4,096 rows
  and targets about 8 MiB. The ordinary path visits at most 32 loaded resources on
  a local store or one loaded resource on remote Turso/libSQL, bounding remote
  round trips inside the write transaction. A separate set-based path can finalize
  up to 512 empty subscriptions without using that resource-count allowance; its
  mapping and backing rows still use the shared row budget. A full 512-subscription
  fast-path batch schedules its next pass after 10 ms. Other required continuation
  passes are paced one second apart; an idle broker does not poll for lifecycle work. The
  persisted cursor rotates through pending names fairly on both backends, so a
  large or bursty deletion cannot monopolize the single writer.
  Startup resumes one bounded pass before crash recovery. The row and name vanish
  only after messages, deduplication, settlement receipts and receive-attempt
  records are gone. Message sequence numbers are not reset, and process event
  counters remain historical.
- Missing queues return HTTP 404, `not_found`. Once a deleting name disappears,
  this is also the completed state observed by a later reconciliation request.
- Stop producers and consumers before retiring a queue. The transactional
  empty-only check prevents deletion of a concurrent acknowledged send. Force
  deliberately replaces that protection with discard permission.
- Queue names are reusable, and the public API deliberately has no lifetime token.
  One owner must serialize deletion and same-name recreation. Wait for a successful
  `state:"deleted"` response, or confirm that `ListQueues` no longer contains a
  previously deleting name, before creating its replacement. Never replay an old
  deletion after recreation.
- The SDK and MCP client each call their configured HTTP transport once and do not
  follow HTTP redirects. Transport failures, redirects, every HTTP 5xx response
  (including a gateway-generated response), and invalid successful
  acknowledgements are `outcome_unknown`. Configure custom transports and proxies
  not to retry these destructive routes. Keep the name reserved and inspect
  `ListQueues`. Under the single-owner rule, an absent row confirms the deletion;
  a `deleting` row means cleanup is still running. Never use another delete as a
  status check or automatically retry it. The engine performs up to 30 one-second
  discovery passes after an uncertain remote database commit so a late-visible
  retirement can enter normal cleanup; startup recovery is the durable fallback.
  Concurrent delete/recreate controllers are unsupported.
- When access logging is retained, the operation record includes its server request
  ID, safe actor classification, queue, force decision, acceptance, and returned
  lifecycle state. See [destructive-operation evidence](observability.md#access-logs-and-destructive-operation-evidence).

Go SDK: `client.DeleteQueue(ctx, name) error` and the equivalent embedded method
retain their empty-only signature. They return `nil` once deletion is accepted,
including when bounded cleanup continues in the background. Use
`DeleteQueueWithOptions(ctx, name, mqlite.DeleteQueueOpts{Force: true})` to
authorize discard and receive the `Deleted` / `State` outcome. Reconcile a
`deleting` or unknown outcome read-only via `ListQueues`; background cleanup
progresses without another delete.

The public identity remains the queue name; force does not introduce a lifetime
token. Durable deletion and fixed-lifetime metadata make v0.4.0 schema token 7,
which refuses schema-5 and schema-6 files rather than migrating them. Schema 6
belonged only to intermediate unreleased builds and was never tagged.
The console exposes **delete queue** in a plain queue's detail page and requires
an exact name confirmation. A nonempty queue additionally requires a force
checkbox and shows the locked-delivery side-effect warning. If an accepted deletion
is still cleaning up, the console reconciles with `ListQueues`; it does not issue a
second delete that could target a same-name replacement. Older brokers report the
operation as unsupported.

### DeleteSubscription

Available in v0.4.0.

Deletes one exact topic/subscription pair. It uses the same durable, bounded
lifecycle as `DeleteQueue`, but owns both the topic roster mapping and the
subscription's backing queue.

Requires `manage` for both empty-only and forced deletion. The `force` field is
explicit discard authorization, not a separate permission tier.

- POST `/mqlite.v1.AdminService/DeleteSubscription`
- Request: `{"topic":"events","name":"worker"}` or the same object with
  `"force":true`. A wrong topic for a globally unique subscription name is
  `404 not_found`; it never selects the subscription by name alone.
- Response: `{"deleted":true,"state":"deleted"}` or
  `{"deleted":false,"state":"deleting"}`. Without force, any retained
  message state returns `409 subscription_not_empty`. Force is the caller's
  durable authorization to discard all states, including locked deliveries.
- The accepted transaction marks the backing queue `deleting` and begins bounded
  cleanup. If cleanup finishes, the same commit removes the mapping and backing
  queue and returns `deleted`; otherwise the committed mapping and name remain
  available for reconciliation while the subscription is excluded from fan-out
  and direct queue operations. Retiring the last active
  subscription therefore makes the implicit topic unavailable for fan-out
  immediately. A deleting mapping keeps the topic address reserved until physical
  cleanup completes.
- The SDK methods are `DeleteSubscription` (empty-only) and
  `DeleteSubscriptionWithOptions(..., DeleteSubscriptionOpts{Force:true})`.
  The built-in destructive clients call their HTTP transport once and classify
  redirects, transport failures, every HTTP 5xx, and invalid success bodies as an
  uncertain outcome. Reconcile that outcome with `ListSubscriptionsPage` using
  the same topic and `include_deleting:true`; do not replay the deletion.
- Subscription identity is still name-only. One owner must serialize deletion
  and recreation, and temporary subscription names should be globally unique and
  used for only one lifetime.
- Retained access logs record the topic/subscription target, force decision,
  acceptance and returned lifecycle state without storing a Bearer token. See
  [destructive-operation evidence](observability.md#access-logs-and-destructive-operation-evidence).

### CreateQueue

Create a queue, or **update** one (idempotent **upsert** on `name`: calling it again
with the same name **and the same `kind`** overwrites that queue's config — existing
messages are untouched). Names are one flat namespace shared with subscriptions and
topics: a `name` that is live as a **topic**, or that belongs to an entity of a
**different kind** (e.g. a plain create over a subscription's backing queue), is
rejected with `409 name_conflict` instead of silently taking over
([concepts → provisioning](concepts.md#provisioning--what-you-create-vs-what-is-created-for-you)).
**Response** `{}` (Empty).

**Request** `CreateQueueRequest`: `name` (valid UTF-8, 1–128 bytes) + `config`
(`QueueConfigJSON`, all fields optional — omit one to take its default). Every duration is **epoch milliseconds**.
Config is **queue-level**: it's set here, not per `Send`/`Receive`.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `kind` | string | `queue` | `queue` (normal). `subscription` can reconfigure an existing legitimate backing queue through the advanced engine/wire surface, but cannot create a backing queue without a topic mapping; use `Subscribe` to create one. |
| `lock_duration_ms` | int (ms) | `30000` (30s) | the **peek-lock window**. At `locked_until_ms`, new settlement and renewal fail immediately; the reaper later returns the row to `active` below the delivery limit or dead-letters it at the limit. `Renew` uses this duration to extend the same live attempt; the committed deadline is never earlier than the existing one. There is **no per-`Receive` override** — it's fixed by the queue. `0` selects the 30s default; an explicit value must be at least `1000`. Existing databases with an older subsecond value still open, but an explicit reconfiguration must satisfy this minimum. |
| `max_delivery_count` | int | `10` | ordinary delivery-attempt budget. The Nth ordinary Peek-Lock `Receive` is a usable final attempt; it is dead-lettered with `reason=MaxDeliveryCountExceeded` only if that locked attempt is abandoned, its lease expires, or it is recovered on startup at `delivery_count >= N`. `ReceiveDeferred` increments the count but currently does not use N as an admission check. `0` → the default of 10. |
| `default_ttl_ms` | int (ms) | `0` = unlimited | default **and ceiling** for message TTL. A shorter positive Send `ttl_ms` is honored; a longer one is capped. For a scheduled Send the effective TTL starts at scheduled visibility. On expiry normal delivery ends and the message goes to the DLQ or is discarded — see `dead_letter_on_expire`. |
| `dead_letter_on_expire` | bool | `true` | on TTL expiry: `true` → move to the DLQ; `false` → discard silently. |
| `dedup_window_ms` | int (ms) | `0` = **off** | duplicate-detection window. `0` disables dedup entirely (then `message_id` is just metadata). `>0` collapses repeats of the same `message_id` within the window — see [The `Message` object → `message_id`, dedup & uniqueness](#message_id-dedup--uniqueness). |
| `ordering_mode` | string | `standard` | `standard` (per-`GroupID` FIFO, cross-group parallel; ungrouped messages unordered) · `group_fifo` (same, but every send **must** carry a `group_id` → else `400 group_required`) · `strict_fifo` (one global head-of-line FIFO for the whole queue). See [conformance §5](conformance.md#5--ordering-modes). |
| `dlq_max_age_ms` | int (ms) | `0` = inherit | per-queue DLQ retention override: drop dead letters older than this. `0` inherits the broker default, `-1` = unbounded. See [retention.md](retention.md). |
| `dlq_max_count` | int | `0` = inherit | keep at most N dead letters (oldest dropped first). `0` inherits, `-1` = unbounded. |
| `dlq_max_bytes` | int | `0` = inherit | cap total dead-letter **body** bytes. `0` inherits, `-1` = unbounded. |

The DLQ values configure asynchronous retention targets. They do not reject new
dead letters or synchronously enforce a hard cap, so a queue may temporarily
exceed them between worker passes or while bounded cleanup catches up.

```bash
curl -H "Authorization: Bearer $T" -H 'Content-Type: application/json' \
  --data '{"name":"orders","config":{
            "lock_duration_ms":60000,
            "max_delivery_count":5,
            "dedup_window_ms":300000,
            "ordering_mode":"group_fifo"
          }}' \
  "$BASE_URL/mqlite.v1.AdminService/CreateQueue"
# → {}
```

### Subscribe

Register subscription `name` under `topic` (creates its backing queue). Requires
`listen`; `manage` includes `listen`.

This is a broker-wide upsert, not create-only ownership. A listener that knows an
existing `(topic,name)` can update its filter, and a new permanent or temporary
subscription consumes the shared capacity and adds publish fan-out. Subscription
inventory and explicit deletion remain `manage` operations. Listen-only clients
should set a fixed TTL on temporary subscriptions and leave early retirement to a
manager.

- **Request** `SubscribeRequest`: `topic`, `name`, `filter` (optional —
  `{"expr": "<predicate>"}`, an [expr boolean over the message](concepts.md#subscription-filters-expr); empty/omitted
  matches all). Topic and subscription names are valid UTF-8 strings of 1–128
  bytes. A malformed expr or name is rejected with `400 invalid_argument`.
  **Response** `{}`.
- The same route accepts `SubscribeWithOptionsRequest`, which adds optional
  `ttl_ms` in v0.4.0. A positive integer stores a fixed lifetime
  from the first successful creation; omitted or zero creates a permanent
  subscription when the name is new, and preserves an existing subscription's
  lifetime. JSON `null`, fractions,
  strings, negative values and integer overflow, plus Go SDK durations between
  zero and one millisecond, return `400 invalid_argument`. The Go methods are `Subscribe` and
  `SubscribeWithOptions(..., SubscribeOptions{TTL: 15*time.Minute})`; the legacy
  request struct and method are unchanged.
- Re-subscribing the same `(topic,name)` with TTL omitted or zero preserves the original
  deadline while allowing a filter update. Repeating its original positive TTL is
  idempotent and also does not extend the deadline. A different positive TTL,
  including an attempt to turn a permanent subscription into a temporary one,
  returns `409 name_conflict` before the filter changes. A temporary subscription
  cannot be made permanent in place; delete it and create a new lifetime.
- At `now >= expires_at_ms` the subscription immediately leaves topic fan-out and
  every backing-queue operation. Expiry is preauthorization for forced bounded
  cleanup, including retained messages. The name remains reserved until physical
  cleanup completes. This guarantee does not undo external effects already caused
  by a locked delivery. Creation and enforcement use the broker host's UTC clock;
  keep that clock synchronized because a forward step can expire a subscription
  early and a backward step can delay a deadline that has not yet been durably
  transitioned to `deleting`.
- The bundled console limits entered TTLs to 3,650 fixed 24-hour days so their
  millisecond value remains exact in browser JavaScript. Direct HTTP and Go SDK
  clients retain the API's wider checked-integer range.
- Name conflicts are rejected with `409 name_conflict`: a `topic` that names an
  existing queue or subscription, a `name` that belongs to a plain queue / another
  topic's subscription / a live topic, and `topic == name`. Re-subscribing the same
  `(topic, name)` is the idempotent filter-update path and succeeds.
- Active and deleting subscriptions share a configurable global capacity. The
  default is 10,000. Creating a new mapping at capacity returns
  `429 resource_exhausted`; updating the filter of an existing active mapping is
  still allowed. A deleting mapping releases its slot only after bounded physical
  cleanup completes.
- A database created by an older build may contain a `kind=subscription` backing
  queue with no topic mapping. When capacity is available, repair it by calling
  `Subscribe` for the intended topic/name, then use `DeleteSubscription` if the
  orphan should be retired. Do not edit MQLite-owned tables directly.

```bash
curl -H "Authorization: Bearer $T" -H 'Content-Type: application/json' \
  --data '{"topic":"replies","name":"request-01J...","ttl_ms":900000}' \
  "$BASE_URL/mqlite.v1.AdminService/Subscribe"
# → {}
```

### ListQueues

The `state` field and deleting-resource visibility below are available in v0.4.0.

- **Request** `{}`. **Response** `ListQueuesResponse`: `queues` ([{`name`, `kind`,
  `state`, `lock_duration_ms`, `max_delivery_count`, `default_ttl_ms`,
  `dedup_window_ms`}]). `state` is `active` or `deleting`. Deleting queues remain
  listed while their name is reserved, but their data plane is unavailable. A
  fixed-lifetime subscription is reported as `deleting` at its deadline even if
  the periodic transition has not yet persisted the state; the legacy fields do
  not expose its deadline.

### ListSubscriptions

- Requires `manage` because the result inventories broker-wide names and filters.
- **Request** `{}`. **Response** `{subscriptions: [{topic, name, expr}]}` — every
  active subscription with its topic and [filter expression](concepts.md#subscription-filters-expr) (`expr` empty = match
  all). This compatibility-only route is active/unexpired-only and intentionally
  not paged. It is deprecated for management and reconciliation because it cannot
  expose deleting name reservations. The configured subscription ceiling bounds
  its maximum size; new integrations should use the paged route below. The Go SDK
  retains the original three-field `SubscriptionInfo` type for this method.

### ListSubscriptionsPage

Available in v0.4.0.

- Requires `manage`, including when it is used to reconcile deletion or expiry.
- POST `/mqlite.v1.AdminService/ListSubscriptionsPage`.
- Request fields: `topic` (optional exact topic), `after` (an opaque cursor from
  the previous response), `limit` (`0` or omitted = 100, maximum 1,000), and
  `include_deleting` (default false).
- Response:
  `{"subscriptions":[{"topic","name","expr","state","created_at_ms","expires_at_ms"}],"total":N,"next_after":"..."}`.
  `state` is `active` or `deleting`; `next_after` is omitted on the final page.
  Both timestamps are required epoch-millisecond integers. `expires_at_ms=0`
  means permanent; otherwise it is greater than `created_at_ms` and never moves.
  An expired row is reported as `deleting` immediately, even before the periodic
  transition has persisted that state. The default view excludes such rows;
  `include_deleting=true` includes them for reconciliation.
  `total` counts the complete selected scope at the snapshot used for that
  request and is independent of `after`/`limit`. Go page entries use
  `SubscriptionPageItem`, whose required `State` field is separate from the
  legacy `SubscriptionInfo` type.
- Ordering is `(topic,name)` and pagination is keyset-based. Cursors are opaque
  and bound to `topic` plus `include_deleting`; reusing one with a different
  scope returns `400 invalid_argument`. `limit` may change between pages.
- Each request is internally consistent. A sequence of requests is not a frozen
  database snapshot, so concurrent creates and deletes can change later pages and
  their `total`. Consumers that need a stable administrative export should quiesce
  subscription changes for the traversal.
- The Go SDK and MCP tool reject a successful page that is null, incomplete,
  oversized, out of order, outside the requested topic/state scope, or inconsistent
  with its `total`, page size, or cursor progress. Such a response is not valid
  evidence for destructive-operation reconciliation.

### TestFilter

- Requires `listen`; it evaluates only the caller-provided expression and sample
  and does not read or mutate broker data.
- **Request** `{expr, message?}` — dry-run a [filter expression](concepts.md#subscription-filters-expr): it
  compiles `expr` and, if `message` (a sample, body base64) is given, evaluates it
  exactly as publish-time fan-out would (nothing is enqueued).
- **Response** `{valid, error?, ran, matched}` — `valid` = compiled; `error` = the
  precise compile error or a runtime/eval error; `ran` = a sample was evaluated;
  `matched` = the sample would route to a subscription using this filter.

### Redrive

Move dead-lettered messages back to active (or to another queue). Each internal
batch is atomic: a same-queue state change commits as one unit, and a cross-queue
source deletion commits with its target insertion. With `rate_per_sec`, each
throttled chunk is a separate atomic batch. Later delivery to a consumer remains
at-least-once, so handlers and external effects must remain idempotent.

- **Request** `RedriveRequest`: `queue`, `target` (string, optional — cross-queue),
  `max` (int), `older_than_ms` (int), `rate_per_sec` (int). **Response**
  `RedriveResponse`: `moved` (int).

### Purge

Permanently delete dead-lettered messages.

- **Request** `PurgeRequest`: `queue`, `max` (int), `older_than_ms` (int; both zero
  purges the whole DLQ). **Response** `PurgeResponse`: `purged` (int). Retained
  access logs include both requested bounds and the confirmed purge count; see
  [destructive-operation evidence](observability.md#access-logs-and-destructive-operation-evidence).

### Status

A desensitized runtime snapshot for an ops view — never includes a connection string
or auth token.

- **Request** `{}`. **Response** `StatusResponse`: `version`, `backend`
  (`memory` | `local file` | `remote libSQL/Turso`), `remote` (bool), `location` (a
  local path, or a masked remote host like `libsql://***.turso.io`), `schema_version`,
  `ping_ms` (a `SELECT 1` read round-trip; `-1` if it failed), `db_size_bytes` (local
  on-disk footprint: db + WAL + shm; `0` for memory/remote), `queues`, `subscriptions`,
  `server_time_ms` (UTC epoch milliseconds from the broker clock used for lifecycle
  deadlines), `uptime_ms`, `auth` (bool). An absent or zero `server_time_ms` means
  the clock is unavailable, including when a current client connects to an older
  broker that omitted it.

## The `Message` object

One shape for both send **input** and receive/peek **output** (fields are omitted when
empty). "Dir" = whether *you set it on send* (**in**) or *the broker sets it on*
*receive/peek* (**out**).

| Field | Type | Dir | Meaning · constraints |
|---|---|---|---|
| `body` | base64 | in | the payload, **opaque** to the broker (it never parses it). The only size-capped field — **≤ 1 MiB** by default (`413` over it). Empty body is allowed. |
| `message_id` | string | in | optional **dedup / idempotency key**. No effect unless the queue has dedup on (`dedup_window_ms > 0`); empty → the body's SHA-256 is used. Not required to be unique. **See the next section.** |
| `group_id` | string | in | **ordering / session key** — messages with the same `group_id` are FIFO among themselves (= SQS MessageGroupId / ASB SessionId). **Required** on every send to a `group_fifo` queue (else `400 group_required`); `strict_fifo` is one global FIFO and does **not** need a `group_id`. Not a consumer group. |
| `subject` | string | in | free-form **routing label** (= ASB Label). **Not unique**, may repeat or be empty. Split on `.` into `subject_parts` for filters (`"orders.eu.new"` → `["orders","eu","new"]`). |
| `correlation_id`, `reply_to`, `content_type` | string | in | free-form ASB-style metadata; the broker stores but never interprets them (`content_type` only hints filter `body_json` decoding). |
| `properties` | object<string,string> | in | custom **string→string** headers, stored verbatim; visible to filters as `properties["k"]`. Values must be strings. |
| `ttl_ms` | int (ms) | in | optional per-message TTL for mixed-TTL batch Send; without a positive batch-wide TTL, zero/omitted uses the queue default. A positive value must be at least 1 ms and is mutually exclusive with a positive batch-wide `SendRequest.ttl_ms`; all effective TTLs are still capped by a positive queue default. |
| `seq_number` | int | out | broker-assigned, strictly increasing, **never reused** (may gap); the handle you settle/peek with while the message lives. |
| `delivery_count` | int | out | how many `Receive` / `ReceiveDeferred` claims have been granted. Settlement, renewal, and scheduled activation do not increment it. |
| `lock_token` | string | out | fencing token from peek-lock `Receive`; settle with it. |
| `state` | string | out | `Peek` only: `active`/`locked`/`deferred`/`scheduled`/`dead_lettered`. |
| `enqueued_at_ms`, `expires_at_ms`, `visible_at_ms`, `locked_until_ms` | int | out | epoch-ms timestamps. |
| `dead_letter_reason`, `dead_letter_description` | string | out | populated for DLQ messages. |

### `message_id`, dedup & uniqueness

The single most-asked question: **can the sender set `message_id`, and what happens on a
collision?**

- **Yes, the sender sets it** (it's an optional input field). It is **not** required to
  be unique, and the broker assigns nothing here — leave it empty and the field just
  stays empty (the *dedup* path, if active, falls back to the body's SHA-256).
- **What the broker does with it depends entirely on the queue's `dedup_window_ms`:**

| queue dedup | effect of `message_id` |
|---|---|
| **off** (`dedup_window_ms = 0`, the **default**) | purely informational metadata. **No uniqueness is enforced and a collision is impossible** — send the same `message_id` a thousand times and you get a thousand independent messages (each its own `seq_number`). Use it as an idempotency hint your *own* consumer checks. |
| **on** (`dedup_window_ms > 0`) | within the sliding window, keyed by **`(queue, message_id)`**: <br>• same id **+ same body** → collapses to one enqueue (the duplicate is silently dropped; you get the original `seq_number` back). <br>• same id **+ different body** → a **conflict**: a single `Send` returns `409 already_exists` (`ErrDedupConflict`); in a multi-message `Send` that one slot comes back as `seq 0` (skipped) while the rest commit. <br>• empty id → the body's SHA-256 is the key (content-addressed dedup). |

- **Dedup is per-queue.** The key is `(queue, message_id)`, so the same `message_id` in two
  different queues — or fanned out to two subscriptions' backing queues — never collides;
  each is independent.
- **So "can self-specifying cause a problem?"** Only if (a) the queue has dedup **on** and
  (b) you reuse an id within the window **with a different body** — that's the deliberate
  conflict signal. With dedup off (default), or with identical bodies, there is no problem.

## Errors

Errors are a JSON envelope `{"code": "...", "message": "..."}` with an HTTP
status. A `not_found` response may add an optional `resource` discriminator
(`"queue"` or `"subscription"`); older clients safely ignore this additive field:

| HTTP | `code` | When |
|---|---|---|
| 400 | `invalid_argument` | malformed JSON, a non-object or `null` request body, an **unknown field**, trailing data after the object, a bad filter expression, an empty queue name, or an unknown enum (`kind` / `ordering_mode`). The decoder is strict — a typo'd field is a 400, not a silently-dropped no-op (`mqlite.ErrInvalidArgument`). |
| 400 | `group_required` | a `group_fifo` send with no `group_id` |
| 401 | `unauthenticated` | missing/invalid Bearer token (auth is on by default; only `MQLITE_TOKENS=off` disables it) |
| 403 | `permission_denied` | a valid key lacks permission, or key management was attempted with auth disabled (`mqlite.ErrPermissionDenied`) |
| 404 | `not_found` | the queue, exact topic/subscription pair, or message doesn't exist; or an unknown path. Missing queues and subscriptions include the corresponding `resource` when the broker can identify it. |
| 409 | `already_exists` | single-message dedup conflict (same id, different body) |
| 409 | `name_conflict` | a queue/subscription/topic name collision at creation time, including a name reserved by a deleting queue — the namespace is one and disjoint (`Subscribe`/`CreateQueue`) |
| 409 | `key_conflict` | a managed key ID or token digest is already registered (`mqlite.ErrKeyConflict`); no existing secret is returned |
| 409 | `lock_lost` | settle with an expired or wrong `lock_token` |
| 409 | `queue_not_empty` | deletion found retained messages in at least one state |
| 409 | `subscription_not_empty` | subscription deletion found retained messages in at least one state |
| 429 | `resource_exhausted` | the active plus deleting subscription capacity is full (`mqlite.ErrSubscriptionLimit`) |
| 413 | `message_too_large` | a message body over `MaxMessageBytes` (default 1 MiB), or request decoding that exceeds `server.MaxBodyBytes` (default 32 MiB) |
| 499 | `canceled` | the request context was canceled. A managed Receiver whose own context is still live treats a received 499 as transient and retries the same Receive attempt; caller cancellation still stops immediately. |
| 503 | `unavailable` | graceful broker drain fenced a `Receive` before its first claim round. The response includes `Retry-After: 1` (also exposed to browser code when CORS is enabled); retry with the **same** `receive_attempt_id`, because this request may itself be replaying a claim whose earlier response was lost. A long poll that had already completed a confirmed empty round instead wakes with `200` and an empty `messages` list. |
| 503 | `outcome_unknown` | a remote (Turso/libSQL) write lost its commit acknowledgement — it **may or may not** have applied. **Do not blindly retry**; reconcile messages by `message_id`/dedup, or key operations by their retained public key ID (`mqlite.ErrOutcomeUnknown`). Only occurs against a remote store, never a local file/`:memory:`. |
| 500 | `internal` | unexpected server error |

The `503 outcome_unknown` row describes a response generated by the broker. The
Go SDK treats every HTTP 5xx received for a lifecycle delete as
`ErrOutcomeUnknown`, even when its envelope says `internal`, because an
intermediary may have generated that response after the broker committed.

The Go SDK re-exports sentinel errors such as `mqlite.ErrLockLost`,
`ErrDedupConflict`, `ErrInvalidArgument` and `ErrOutcomeUnknown`, so `errors.Is`
works in both embedded and client mode. An accepted queue or subscription
deletion is a successful operation even when its typed HTTP result is
`state:"deleting"`.

With a resource-aware broker, a missing queue matches both
`mqlite.ErrQueueNotFound` and the broader `mqlite.ErrNotFound`; this is the same
hierarchy as embedded mode. A new client connected to an older broker receives
the legacy envelope without `resource` and can only match `ErrNotFound`.

The Go SDK bounds ordinary successful response bodies at 64 MiB and error bodies
at 64 KiB. Outside lifecycle deletion's explicit `ErrOutcomeUnknown` cases, a
non-2xx response is available through `errors.As` as `*mqlite.HTTPError`; its
`StatusCode` and `HTTPStatus()` retain the HTTP status even when an intermediary
returned no JSON error code. Redirect errors report only the status and never copy
the untrusted `Location` value into the error.
