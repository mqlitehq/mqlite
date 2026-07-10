# mqlite HTTP API reference

The broker speaks **Connect-style JSON over HTTP**. Every operation is a single
`POST` to `/mqlite.v1.<Service>/<Method>` with a JSON body — curl-able by
construction. This document is generated from the wire contract in
[`wire/wire.go`](../wire/wire.go) (the single source of truth shared by the broker
and the Go SDK, so the two can't drift) and the server's error mapping.

## Conventions

- **Transport:** HTTP `POST`, `Content-Type: application/json`. (The open discovery,
  health, and metrics endpoints below are `GET`.)
- **`body`** is **base64** in JSON (Go marshals `[]byte` as base64).
- **Timestamps** are **epoch milliseconds** (UTC) integers; so are durations (`*_ms`).
- **Sequence numbers** (`seq_number`) are broker-assigned monotonic integers, unique
  per queue — **the handle you settle with**. You never set them.
- **At-least-once:** consumers must be idempotent. A message is delivered at least
  once and never silently dropped (see [retention.md](retention.md) for what is and
  isn't auto-deleted).
- **One settlement verb per outcome** — `Complete` / `Abandon` / `Reject` / `Defer`,
  no aliases. Each is fenced on the `lock_token` from `Receive`.

### Size limits

Only the **body** is capped. There is **no enforced length limit** on the string
metadata fields (`subject`, `message_id`, `group_id`, `correlation_id`, `reply_to`,
`content_type`, `properties` keys/values) — they are stored as SQLite `TEXT`; keep them
reasonable (they count toward the JSON request you send, not toward the body cap).

| limit | default | knob |
|---|---|---|
| `body` size | **1 MiB** → `413 message_too_large` over it | `MQLITE_MAX_MESSAGE_BYTES` (serve) / `Options.MaxMessageBytes` (embedded) |
| `max_messages` per `Receive` | 256 (max) | request field |
| `wait_time_ms` long-poll | 20000 (max) | request field |
| `Peek` `max` | 32 default, 1000 max | request field |
| filter `expr` source | capped length + AST node count | internal (see [concepts.md](concepts.md#subscription-filters-expr)) |

## Auth

Bearer token via `Authorization: Bearer <token>`. The broker accepts the tokens in
`MQLITE_TOKENS` (comma-separated). **Secure by default:** if `MQLITE_TOKENS` is
**unset**, `mqlite serve` **generates a random token** (`mqk_…`, 128-bit) and prints it
at startup — the broker is never silently wide open. Set `MQLITE_TOKENS=off` to
explicitly disable auth (localhost/LAN only). When auth is on, every endpoint needs the
token **except** the open ones below. A missing/invalid token → `401 unauthenticated`.

## Open endpoints (no auth)

| Method | Path | Returns |
|---|---|---|
| `GET` | `/` | JSON discovery card (see below) |
| `GET` | `/healthz` | `200 ok` (liveness) |
| `GET` | `/ui` | Embedded admin console (when enabled — see below) |

```bash
curl https://<host>/                 # what is this? (no auth)
# → {"name":"mqlite","version":"0.1.1","description":"...","status":"ok",
#    "auth":"bearer","docs":"https://github.com/mqlitehq/mqlite",
#    "endpoints":["/mqlite.v1.QueueService/Send", ...]}
curl https://<host>/healthz          # ok
```

`/ui` serves the **embedded admin console** (a static single-page app baked into the
binary) and is auth-exempt — the page itself loads without a token; its API calls
carry one you paste in. It's on by default; set `MQLITE_UI=off` to run headless, in
which case `/ui` 404s and is dropped from the discovery card's `endpoints` list.

## Other endpoints

| Method | Path | Auth | Returns |
|---|---|---|---|
| `GET` | `/metrics` | Bearer | Prometheus text: `mqlite_queue_messages{queue,state}` gauges |

## QueueService

All paths are `/mqlite.v1.QueueService/<Method>`.

### Send

Enqueue one or more messages in one transaction.

- **Request** `SendRequest`: `queue` (string), `messages` ([Message]),
  `scheduled_enqueue_time_ms` (int, optional), `ttl_ms` (int, optional).
- **Response** `SendResponse`: `seq_numbers` ([int]) — one per input message, positional.
- **Dedup:** in a multi-message batch, a slot that hits a dedup conflict (same
  `message_id`, different body) comes back as `0` (skipped) while the rest commit. A
  **single-message** Send instead returns `409 already_exists`.

```bash
curl -H "Authorization: Bearer $T" -H 'Content-Type: application/json' \
  --data "{\"queue\":\"orders\",\"messages\":[{\"body\":\"$(printf hi|base64)\"}]}" \
  https://<host>/mqlite.v1.QueueService/Send
```

### Receive

Peek-Lock (default) or Receive-and-Delete; long-polls up to `wait_time_ms`.

- **Request** `ReceiveRequest`: `queue`, `max_messages` (int, default 1, max 256),
  `wait_time_ms` (int, long-poll; max 20000), `receive_mode` (int: `0` peek-lock,
  `1` receive-and-delete), `receive_attempt_id` (string, optional idempotency key —
  a retry replays the same batch / same lock tokens).
- **Response** `ReceiveResponse`: `messages` ([Message]) — each carries a
  `lock_token` to settle with (peek-lock mode).

```bash
curl -H "Authorization: Bearer $T" -H 'Content-Type: application/json' \
  --data '{"queue":"orders","max_messages":1,"wait_time_ms":5000}' \
  https://<host>/mqlite.v1.QueueService/Receive
# → {"messages":[{
#      "seq_number":42,"lock_token":"lt_9f3a…","delivery_count":1,
#      "body":"aGk=","message_id":"order-42","group_id":"cust-7",
#      "enqueued_at_ms":1750000000000,"visible_at_ms":1750000000000
#    }]}
# settle it: POST .../Complete {"queue":"orders","seq_number":42,"lock_token":"lt_9f3a…"}
```

### Complete / Abandon / Reject / Defer / Renew

All take `SettleRequest` and return `SettleResponse` `{ "ok": true }`. Settling with
an expired or wrong `lock_token` → `409 lock_lost`.

| Method | Extra fields | Effect |
|---|---|---|
| `Complete` | — | delete the message (done) |
| `Abandon` | `delay_ms` (int, optional) | release the lock → redelivered after `delay_ms` |
| `Reject` | `dead_letter_reason`, `dead_letter_description` (optional) | move to the DLQ |
| `Defer` | — | set aside; retrieve later by seq via `ReceiveDeferred` |
| `Renew` | — | extend the lock by the queue's lock duration |

`SettleRequest`: `queue`, `seq_number`, `lock_token` (+ the extras above).

> **Defer + ordering.** A deferred message keeps its head-of-line position: in a
> `group_fifo`/`strict_fifo` (or otherwise grouped) queue the messages behind it — its
> group, or the **whole queue** under `strict_fifo` — stay blocked until it is retrieved
> via `ReceiveDeferred` and settled (other groups proceed). Normal `Receive` never
> returns a deferred message, and with no TTL there is no auto-recovery, so recover a
> lost `seq_number` via `Peek` with `state=deferred`.

### CompleteBatch

Complete many messages in **one round-trip** — settle a whole received batch without
a Complete-per-message N+1 (the cheap path for draining).

- **Request** `CompleteBatchRequest`: `queue`, `messages` ([{`seq_number`, `lock_token`}]).
- **Response** `CompleteBatchResponse`: `results` ([{`seq_number`, `ok`}]) — `ok=false`
  for a stale/expired lock on that item (not fatal to the batch). Each item is fenced +
  idempotent exactly like `Complete`.

### ReceiveDeferred

Lock previously-`Defer`'d messages by sequence number.

- **Request** `ReceiveDeferredRequest`: `queue`, `seq_numbers` ([int]).
- **Response** `ReceiveResponse`: `messages` ([Message]).

### Schedule

Enqueue for future delivery (same wire shape as `Send`).

- **Request** `SendRequest` with `scheduled_enqueue_time_ms` set (epoch ms).
- **Response** `SendResponse`: `seq_numbers`.

### Cancel

Delete a not-yet-activated scheduled message.

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
  `scheduled`, `dead_lettered`, `total`, `oldest_message_age_ms`.

```bash
curl -H "Authorization: Bearer $T" -H 'Content-Type: application/json' \
  --data '{"queue":"orders"}' https://<host>/mqlite.v1.QueueService/Stats
# → {"queue":"orders","active":128,"locked":3,"deferred":0,"scheduled":5,
#    "dead_lettered":2,"total":138,"oldest_message_age_ms":41200}
```

## AdminService

All paths are `/mqlite.v1.AdminService/<Method>`.

### CreateQueue

Create a queue, or **update** one (idempotent **upsert** on `name`: calling it again
with the same name **and the same `kind`** overwrites that queue's config — existing
messages are untouched). Names are one flat namespace shared with subscriptions and
topics: a `name` that is live as a **topic**, or that belongs to an entity of a
**different kind** (e.g. a plain create over a subscription's backing queue), is
rejected with `409 name_conflict` instead of silently taking over
([concepts → provisioning](concepts.md#provisioning--what-you-create-vs-what-is-created-for-you)).
**Response** `{}` (Empty).

**Request** `CreateQueueRequest`: `name` (string) + `config` (`QueueConfigJSON`, all
fields optional — omit one to take its default). Every duration is **epoch milliseconds**.
Config is **queue-level**: it's set here, not per `Send`/`Receive`.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `kind` | string | `queue` | `queue` (normal) or `subscription` (a topic's backing queue — normally created via `Subscribe`, not by hand). |
| `lock_duration_ms` | int (ms) | `30000` (30s) | the **peek-lock window**: how long a `Receive`'d message stays invisible to other consumers before its lock expires and it's redelivered. `Renew` extends a held lock by this same amount. There is **no per-`Receive` override** — it's fixed by the queue. `0` → the 30s default. |
| `max_delivery_count` | int | `10` | how many delivery attempts before a message is dead-lettered (`reason=MaxDeliveryCountExceeded`). Reached at `>=` (the Nth delivery is the last). `0` → the default of 10. |
| `default_ttl_ms` | int (ms) | `0` = unlimited | per-message time-to-live applied when a `Send` doesn't set its own `ttl_ms`. On expiry the message goes to the DLQ or is discarded — see `dead_letter_on_expire`. |
| `dead_letter_on_expire` | bool | `true` | on TTL expiry: `true` → move to the DLQ; `false` → discard silently. |
| `dedup_window_ms` | int (ms) | `0` = **off** | duplicate-detection window. `0` disables dedup entirely (then `message_id` is just metadata). `>0` collapses repeats of the same `message_id` within the window — see [The `Message` object → `message_id`, dedup & uniqueness](#message_id-dedup--uniqueness). |
| `ordering_mode` | string | `standard` | `standard` (per-`GroupID` FIFO, cross-group parallel; ungrouped messages unordered) · `group_fifo` (same, but every send **must** carry a `group_id` → else `400 group_required`) · `strict_fifo` (one global head-of-line FIFO for the whole queue). See [conformance §5](conformance.md#5--ordering-modes). |
| `dlq_max_age_ms` | int (ms) | `0` = inherit | per-queue DLQ retention override: drop dead letters older than this. `0` inherits the broker default, `-1` = unbounded. See [retention.md](retention.md). |
| `dlq_max_count` | int | `0` = inherit | keep at most N dead letters (oldest dropped first). `0` inherits, `-1` = unbounded. |
| `dlq_max_bytes` | int | `0` = inherit | cap total dead-letter **body** bytes. `0` inherits, `-1` = unbounded. |

```bash
curl -H "Authorization: Bearer $T" -H 'Content-Type: application/json' \
  --data '{"name":"orders","config":{
            "lock_duration_ms":60000,
            "max_delivery_count":5,
            "dedup_window_ms":300000,
            "ordering_mode":"group_fifo"
          }}' \
  https://<host>/mqlite.v1.AdminService/CreateQueue
# → {}
```

### Subscribe

Register subscription `name` under `topic` (creates its backing queue).

- **Request** `SubscribeRequest`: `topic`, `name`, `filter` (optional —
  `{"expr": "<predicate>"}`, an [expr boolean over the message](concepts.md#subscription-filters-expr); empty/omitted
  matches all). A malformed expr is rejected with `400 invalid_argument`. **Response** `{}`.
- Name conflicts are rejected with `409 name_conflict`: a `topic` that names an
  existing queue or subscription, a `name` that belongs to a plain queue / another
  topic's subscription / a live topic, and `topic == name`. Re-subscribing the same
  `(topic, name)` is the idempotent filter-update path and succeeds.

### ListQueues

- **Request** `{}`. **Response** `ListQueuesResponse`: `queues` ([{`name`, `kind`,
  `lock_duration_ms`, `max_delivery_count`, `default_ttl_ms`, `dedup_window_ms`}]).

### ListSubscriptions

- **Request** `{}`. **Response** `{subscriptions: [{topic, name, expr}]}` — every
  subscription with its topic and [filter expression](concepts.md#subscription-filters-expr) (`expr` empty = match
  all). `ListQueues` shows the backing queues; this exposes the topic + filter it omits.

### TestFilter

- **Request** `{expr, message?}` — dry-run a [filter expression](concepts.md#subscription-filters-expr): it
  compiles `expr` and, if `message` (a sample, body base64) is given, evaluates it
  exactly as publish-time fan-out would (nothing is enqueued).
- **Response** `{valid, error?, ran, matched}` — `valid` = compiled; `error` = the
  precise compile error or a runtime/eval error; `ran` = a sample was evaluated;
  `matched` = the sample would route to a subscription using this filter.

### Redrive

Move dead-lettered messages back to active (or to another queue).

- **Request** `RedriveRequest`: `queue`, `target` (string, optional — cross-queue),
  `max` (int), `older_than_ms` (int), `rate_per_sec` (int). **Response**
  `RedriveResponse`: `moved` (int).

### Purge

Permanently delete dead-lettered messages.

- **Request** `PurgeRequest`: `queue`, `max` (int), `older_than_ms` (int; both zero
  purges the whole DLQ). **Response** `PurgeResponse`: `purged` (int).

### Status

A desensitized runtime snapshot for an ops view — never includes a connection string
or auth token.

- **Request** `{}`. **Response** `StatusResponse`: `version`, `backend`
  (`memory` | `local file` | `remote libSQL/Turso`), `remote` (bool), `location` (a
  local path, or a masked remote host like `libsql://***.turso.io`), `schema_version`,
  `ping_ms` (a `SELECT 1` read round-trip; `-1` if it failed), `db_size_bytes` (local
  on-disk footprint: db + WAL + shm; `0` for memory/remote), `queues`, `subscriptions`,
  `uptime_ms`, `auth` (bool).

## The `Message` object

One shape for both send **input** and receive/peek **output** (fields are omitted when
empty). "Dir" = whether *you set it on send* (**in**) or *the broker sets it on*
*receive/peek* (**out**).

| Field | Type | Dir | Meaning · constraints |
|---|---|---|---|
| `body` | base64 | in | the payload, **opaque** to the broker (it never parses it). The only size-capped field — **≤ 1 MiB** by default (`413` over it). Empty body is allowed. |
| `message_id` | string | in | optional **dedup / idempotency key**. No effect unless the queue has dedup on (`dedup_window_ms > 0`); empty → the body's SHA-256 is used. Not required to be unique. **See the next section.** |
| `group_id` | string | in | **ordering / session key** — messages with the same `group_id` are FIFO among themselves (= SQS MessageGroupId / ASB SessionId). **Required** on every send to a `group_fifo`/`strict_fifo` queue (else `400 group_required`). Not a consumer group. |
| `subject` | string | in | free-form **routing label** (= ASB Label). **Not unique**, may repeat or be empty. Split on `.` into `subject_parts` for filters (`"orders.eu.new"` → `["orders","eu","new"]`). |
| `correlation_id`, `reply_to`, `content_type` | string | in | free-form ASB-style metadata; the broker stores but never interprets them (`content_type` only hints filter `body_json` decoding). |
| `properties` | object<string,string> | in | custom **string→string** headers, stored verbatim; visible to filters as `properties["k"]`. Values must be strings. |
| `seq_number` | int | out | broker-assigned, monotonic, **unique per queue**; the handle you settle/peek with. |
| `delivery_count` | int | out | how many times this message has been delivered (claimed). |
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

Errors are a JSON envelope `{"code": "...", "message": "..."}` with an HTTP status:

| HTTP | `code` | When |
|---|---|---|
| 400 | `invalid_argument` | malformed JSON or a bad field |
| 400 | `group_required` | a `group_fifo`/`strict_fifo` send with no `group_id` |
| 401 | `unauthenticated` | missing/invalid Bearer token (auth is on by default; only `MQLITE_TOKENS=off` disables it) |
| 404 | `not_found` | the queue or message doesn't exist; or an unknown path |
| 409 | `already_exists` | single-message dedup conflict (same id, different body) |
| 409 | `name_conflict` | a queue/subscription/topic name collision at creation time — the namespace is one and disjoint (`Subscribe`/`CreateQueue`) |
| 409 | `lock_lost` | settle with an expired or wrong `lock_token` |
| 413 | `message_too_large` | body over `MaxMessageBytes` (default 1 MiB) |
| 499 | `canceled` | the client canceled the request |
| 500 | `internal` | unexpected server error |

The Go SDK re-exports the sentinel errors (`mqlite.ErrLockLost`, `ErrDedupConflict`,
…) so `errors.Is` works in both embedded and client mode.
