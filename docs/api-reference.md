# mqlite HTTP API reference

The broker speaks **Connect-style JSON over HTTP**. Every operation is a single
`POST` to `/mqlite.v1.<Service>/<Method>` with a JSON body — curl-able by
construction. This document is generated from the wire contract in
[`wire/wire.go`](../wire/wire.go) (the single source of truth shared by the broker
and the Go SDK, so the two can't drift) and the server's error mapping.

## Conventions

- **Transport:** HTTP `POST`, `Content-Type: application/json`. (The open discovery,
  health, metrics, and UI endpoints below are `GET`.)
- **`body`** is **base64** in JSON (Go marshals `[]byte` as base64).
- **Timestamps** are **epoch milliseconds** (UTC) integers; so are durations (`*_ms`).
- **Sequence numbers** (`seq_number`) are per-queue monotonic integers.
- **At-least-once:** consumers must be idempotent. A message is delivered at least
  once and never silently dropped (see [retention.md](retention.md) for what is and
  isn't auto-deleted).
- **One settlement verb per outcome** — `Complete` / `Abandon` / `Reject` / `Defer`,
  no aliases. Each is fenced on the `lock_token` from `Receive`.

## Auth

Bearer token via `Authorization: Bearer <token>`. The broker accepts the tokens in
`MQLITE_TOKENS` (comma-separated). If `MQLITE_TOKENS` is unset, **auth is disabled**
(localhost/LAN downgrade). When auth is on, every endpoint needs the token **except**
the open ones below. A missing/invalid token → `401 unauthenticated`.

## Open endpoints (no auth)

| Method | Path | Returns |
|---|---|---|
| `GET` | `/` | JSON discovery card: `{name, version, status, auth, docs, endpoints}` |
| `GET` | `/healthz` | `200 ok` (liveness) |
| `GET` | `/ui` | read-only ops dashboard (HTML; its data calls still use the token) |

```bash
curl https://<host>/                 # what is this? (no auth)
curl https://<host>/healthz          # ok
```

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

## AdminService

All paths are `/mqlite.v1.AdminService/<Method>`.

### CreateQueue

Create or update a queue (idempotent on name).

- **Request** `CreateQueueRequest`: `name`, `config` (`QueueConfigJSON`):
  - `kind` (`queue` default | `subscription`)
  - `lock_duration_ms` (default 30000)
  - `max_delivery_count` (default 10; over it → DLQ)
  - `default_ttl_ms` (0 = unlimited)
  - `dead_letter_on_expire` (bool, default true)
  - `dedup_window_ms` (0 = dedup disabled)
  - `ordering_mode` (`standard` default | `group_fifo` | `strict_fifo`)
- **Response** `{}` (Empty).

### Subscribe

Register subscription `name` under `topic` (creates its backing queue).

- **Request** `SubscribeRequest`: `topic`, `name`, `filter` (optional — equality-AND +
  subject prefix). **Response** `{}`.

### ListQueues

- **Request** `{}`. **Response** `ListQueuesResponse`: `queues` ([{`name`, `kind`,
  `lock_duration_ms`, `max_delivery_count`, `default_ttl_ms`, `dedup_window_ms`}]).

### Redrive

Move dead-lettered messages back to active (or to another queue).

- **Request** `RedriveRequest`: `queue`, `target` (string, optional — cross-queue),
  `max` (int), `older_than_ms` (int), `rate_per_sec` (int). **Response**
  `RedriveResponse`: `moved` (int).

### Purge

Permanently delete dead-lettered messages.

- **Request** `PurgeRequest`: `queue`, `max` (int), `older_than_ms` (int; both zero
  purges the whole DLQ). **Response** `PurgeResponse`: `purged` (int).

## The `Message` object

Shared shape for send input and receive/peek output (fields omitted when empty):

| Field | Type | Notes |
|---|---|---|
| `body` | base64 | the payload (opaque to the broker) |
| `message_id` | string | dedup / idempotency key; empty → body SHA-256 when dedup on |
| `group_id` | string | ordering / session key (FIFO per group) |
| `correlation_id`, `reply_to`, `subject`, `content_type` | string | ASB-style metadata |
| `properties` | object<string,string> | custom KV headers (broker doesn't interpret) |
| `seq_number` | int | assigned on enqueue (output) |
| `delivery_count` | int | times delivered (output) |
| `lock_token` | string | fencing token to settle with (output, peek-lock) |
| `state` | string | output of `Peek` |
| `enqueued_at_ms`, `expires_at_ms`, `visible_at_ms`, `locked_until_ms` | int | epoch ms (output) |
| `dead_letter_reason`, `dead_letter_description` | string | output for DLQ messages |

## Errors

Errors are a JSON envelope `{"code": "...", "message": "..."}` with an HTTP status:

| HTTP | `code` | When |
|---|---|---|
| 400 | `invalid_argument` | malformed JSON or a bad field |
| 400 | `group_required` | a `group_fifo`/`strict_fifo` send with no `group_id` |
| 401 | `unauthenticated` | missing/invalid Bearer token (when `MQLITE_TOKENS` is set) |
| 404 | `not_found` | the queue or message doesn't exist; or an unknown path |
| 409 | `already_exists` | single-message dedup conflict (same id, different body) |
| 409 | `name_conflict` | reusing a subscription/queue name across topics |
| 409 | `lock_lost` | settle with an expired or wrong `lock_token` |
| 413 | `message_too_large` | body over `MaxMessageBytes` (default 1 MiB) |
| 499 | `canceled` | the client canceled the request |
| 500 | `internal` | unexpected server error |

The Go SDK re-exports the sentinel errors (`mqlite.ErrLockLost`, `ErrDedupConflict`,
…) so `errors.Is` works in both embedded and client mode.
