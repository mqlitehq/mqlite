# Core concepts & subscription filters

> Goal: explain MQLite's model once, clearly, and place it against the comparable
> services from Azure Service Bus, AWS (SQS + SNS) and GCP Pub/Sub. MQLite is designed
> from first principles (one SQLite file, embeddable, agent-friendly), but conceptually
> it sits closest to Azure Service Bus, so ASB is the primary reference frame and the
> differences are called out where they matter. Sections 1–5 are the model; sections
> 6–11 are the **subscription-filter language** (`expr`).

---

## 1. The whole picture in one diagram

```
  ┌──────────────────────────── two delivery targets ─────────────────────────┐
  │                                                                            │
  │   A) send straight to a Queue        B) publish to a Topic, fan out        │
  │                                                                            │
  │   producer                           producer                              │
  │      │ send "orders"                    │ publish "events"                 │
  │      ▼                                  ▼                                   │
  │   ┌────────────┐                ┌──── topic "events" ────┐                 │
  │   │ queue      │                │ (just a set of subs;   │                 │
  │   │  orders    │                │  stores nothing itself)│                 │
  │   │ ┌────────┐ │                └─┬─────────┬──────────┬─┘                 │
  │   │ │ active │ │       filter match│  match  │  no match│                  │
  │   │ ├────────┤ │                  ▼         ▼          ✗ (no copy)         │
  │   │ │ locked │ │            sub "audit"  sub "billing"                     │
  │   │ ├────────┤ │            ┌────────┐   ┌────────┐                        │
  │   │ │  DLQ   │ │            │backing │   │backing │  ← each subscription   │
  │   │ └────────┘ │            │ queue  │   │ queue  │    is an independent   │
  │   └────────────┘            └────────┘   └────────┘    queue (+ own DLQ)   │
  │      consumer                  consumer     consumer                       │
  └────────────────────────────────────────────────────────────────────────────┘
```

One sentence to remember: **a Topic stores nothing — it is only a routing rule that
copies a message into a number of subscription queues. The thing that actually stores
messages, can be consumed, and can dead-letter is always a Queue** (including the
backing queue behind a subscription).

---

## 2. The five concepts

### Queue
A named message container, and the only entity that actually stores messages. A message
runs this state machine — a row is in exactly one state at a time; below `count` =
`delivery_count`, `max` = the queue's `max_delivery_count`, and `✗ removed` means the row
is deleted from the database:

```
                   ┌───────────┐
  Send(scheduled)  │ scheduled │   scheduler, when due ─► active
  ───────────────► └─────┬─────┘   cancel ─► ✗ removed
                         │
                         ▼
  Send             ┌──────────┐ ◄── requeue: abandon / lock-expiry, count < max  (reaper)
  ───────────────► │  active  │ ◄── redrive: from dead_lettered (count → 0)
                   └────┬─────┘
                        │ receive / claim (count++)
                        ▼
                   ┌──────────┐ ──── defer ──────► ┌──────────┐
                   │  locked  │ ◄─ ReceiveDeferred  │ deferred │
                   └────┬─────┘    (by seq)         └──────────┘
                        │ complete ─► ✗ removed
                        │ renew ─► (stays locked)
                        │ reject, or abandon / lock-expiry, count ≥ max:
                        ▼
                   ┌───────────────┐ ◄── TTL expiry (active / locked / deferred;
                   │ dead_lettered │      ✗ removed if dead_letter_on_expire = 0)
                   └───────┬───────┘
                           │ purge / DLQ retention ─► ✗ removed
```

Every transition, with its trigger and the condition under which it fires:

| From | Trigger | To | When |
|---|---|---|---|
| _(new)_ | `Send` | active | delivered immediately |
| _(new)_ | `Send` scheduled | scheduled | enqueue time is in the future |
| scheduled | scheduler loop | active | enqueue time reached |
| scheduled | `Cancel` | ✗ removed | cancel before it activates |
| active | `Receive` / claim | locked | `count`++; lock acquired |
| locked | `Complete` | ✗ removed | fenced on `lock_token` |
| locked | `Abandon` | active | `count < max`; optional backoff delay |
| locked | `Abandon` | dead_lettered | `count ≥ max` |
| locked | lock expiry (reaper) | active | `count < max`; auto-redelivered |
| locked | lock expiry (reaper) | dead_lettered | `count ≥ max` (`MaxDeliveryCountExceeded`) |
| locked | `Reject` | dead_lettered | explicit, with a reason |
| locked | `Defer` | deferred | set aside; fetched later by seq |
| locked | `Renew` | locked | extends the lock lease |
| deferred | `ReceiveDeferred` | locked | fetched by seq; `count`++ |
| active / locked / deferred | TTL expiry | dead_lettered | queue has `dead_letter_on_expire=1` (`TTLExpired`) |
| active / locked / deferred / scheduled | TTL expiry | ✗ removed | queue has `dead_letter_on_expire=0` (discard) |
| dead_lettered | `Redrive` | active | back to this (or a target) queue; `count` reset to 0 |
| dead_lettered | `Purge` | ✗ removed | manual deletion |
| dead_lettered | DLQ retention | ✗ removed | background janitor bounds (age / count / bytes); touches **only** dead-letter rows |

- **The DLQ is not a separate queue.** It is messages in the same queue with
  `state='dead_lettered'`. A message lands there when its `delivery_count` reaches
  `max_delivery_count` (i.e. `≥`), on an explicit `reject`, or on TTL expiry (when the
  queue's `dead_letter_on_expire=1`). Inspect with `peek state=dead_lettered`, send back
  with `redrive`, or delete with `purge`.
- **Two redelivery paths.** `Abandon` is an explicit, client-driven settlement; **lock
  expiry** is automatic — the reaper (~1s) reclaims a lock held past `lock_duration`
  without settling. Both redeliver while `count < max` and dead-letter once `count ≥ max`.
- You can have any number of independent plain queues.

### Topic
A **logical name** standing for "the set of subscriptions attached to it".

- A topic is **not an explicitly created/stored object** — its existence is exactly
  "there are rows in `subscriptions` with `topic=<name>`". **A topic with zero
  subscriptions does not exist.**
- A topic **holds no messages of its own**: no inbox, no buffer, no retention.

### Subscription
The relationship binding a Topic to a backing queue, with an optional Filter.

- Each subscription has its **own independent backing queue** (own consumption, own
  `seq_number`s, own DLQ).
- A subscription belongs to exactly **one** topic (guarded by `ErrNameConflict`).
- "A topic has many queues" is, precisely: **each subscription owns one queue, and the
  topic fans out through those subscriptions.**

### Filter
A **single expr-lang boolean expression** on a subscription, evaluated per message at
publish.

- Empty expression = match everything.
- e.g. `subject_parts[0] == "orders" && properties["tier"] == "gold"`.
- Compiled and type-checked at `Subscribe`; a bad one is rejected (`ErrInvalidFilter`
  → 400). See §6–11 below for the full filter language and expression environment.

### Provisioning — what you create vs. what is created for you
| Entity | How it comes to exist | Primitive |
|---|---|---|
| Plain Queue | **You create it explicitly**; `Send` to a non-existent queue errors (no auto-create) | `CreateQueue(name)` |
| Topic | **Implicit** — no create call; springs into existence with its first subscription, gone when the last is removed | *(no `CreateTopic`)* |
| Subscription **+ its backing queue** | **Created together in one call** — `Subscribe` provisions the backing queue for you | `Subscribe(topic, name, filter)` |

- There is **no "create topic" operation** and **no same-named queue behind a topic**.
  The backing queue is named after the **subscription**, not the topic.
- You never pre-create a queue to attach a subscription to — one `Subscribe` call
  builds the backing queue and the fan-out mapping.
- **One flat namespace, disjoint by construction.** Plain-queue names, subscription
  names (= their backing-queue names) and topic names never overlap; conflicts fail
  loud with `ErrNameConflict` (HTTP 409) at creation time, in both directions:
  `Subscribe` rejects a **topic** that names an existing queue or subscription, a
  **subscription name** that belongs to a plain queue, to another topic's
  subscription, or to a live topic, and the degenerate `topic == name`;
  `CreateQueue` rejects a name that is live as a topic, and a **cross-kind upsert**
  (a plain `CreateQueue` over a subscription's backing queue, or `kind=subscription`
  over a plain queue) — a deliberate same-kind reconfig stays open. (ASB behaves the
  same way: queues and topics share one entity namespace, so a same-name create
  conflicts instead of silently rerouting sends.) The guards apply to creations from
  this version on; a pre-existing colliding pair in an old DB keeps resolving
  topic-first and now fails loud on any attempt to re-create either side.
- Name resolution at publish/send: a name with subscription rows → **topic** (fan-out);
  otherwise it must be an **existing queue**; otherwise the call errors. Because the
  namespace is disjoint, this resolution is **unambiguous** — a name can never be a
  topic and a queue at the same time, so a `Send` is never silently rerouted.

```
You only ever do two kinds of "create":
  CreateQueue(name)             -> one independent work queue (must exist before Send)
  Subscribe(topic, sub, filter) -> auto-creates backing queue=sub + attaches to topic
                                    (the topic comes into being as a side effect)
```

> Difference from ASB: in ASB both the Topic and the Subscription are created
> explicitly; MQLite makes "create topic" implicit and "create the subscription's
> queue" automatic — two fewer steps.

### Message ID vs Sequence Number (the two ids people conflate)
| | Who assigns it | When fanned out to N subs | Purpose |
|---|---|---|---|
| `message_id` | optionally supplied by the app | **identical in every copy** | idempotency/dedup key (dedup is per-queue: `(queue, message_id)`) |
| `seq_number` | assigned by the broker (globally monotonic) | **different in every copy** | the handle to locate + settle on consume; the ASB SequenceNumber analogue |

---

## 3. Direct answers to common questions

**Q: A message fans out to two subscriptions — are the IDs the same?**
The `message_id` is **the same** (copied verbatim); the `seq_number` is **different**
(each backing queue assigns its own globally-monotonic id). The two are fully
independent rows, settled separately with their own `seq_number` + `lock_token`.

**Q: I add a filtered subscription under a topic — do historical messages get routed to
it?**
**No.** Fan-out happens **only at publish time**, against the subscriptions that exist
then. There is no replay, no offset rewind. A new subscription only receives messages
published **after** it was created (and matching its filter). (Unlike Kafka; same as
ASB / SNS+SQS.)

**Q: A topic gets a message with no subscriptions at the time — dropped? buffered? a
default inbox?**
**No inbox; the message is not retained.** Two cases: ① subscriptions exist but none
match → the message is **silently dropped** (not an error); ② no subscription names
that target at all → it falls back to "must be an existing queue", delivered if one
exists, otherwise an error. **To retain a message you must have a matching subscription
before it is published.**

---

## 4. Comparison with cloud services

### Concept mapping
| MQLite | Azure Service Bus | AWS | GCP Pub/Sub |
|---|---|---|---|
| Queue | Queue | SQS Queue | (Topic + single subscription, roughly) |
| Topic | Topic | SNS Topic | Topic |
| Subscription | Subscription | SNS→SQS subscription | Subscription |
| Filter (expr-lang) | SQL/Correlation Filter | SNS Filter Policy | Subscription Filter |
| peek-lock + complete | Peek-Lock + Complete | Visibility Timeout + Delete | ack / nack |
| `seq_number` | SequenceNumber | (none) | (none, only ackId) |
| DLQ (sub-state) | DLQ (sub-queue) | Redrive Policy → DLQ | Dead-letter Topic |
| dedup window | Duplicate Detection | SQS FIFO dedup (5min) | none native (DIY) |
| group_id ordering | Session | SQS FIFO MessageGroupId | Ordering Key |

### Behavioral differences (the ones that bite)
| Dimension | MQLite | ASB | SNS+SQS | GCP Pub/Sub | Kafka |
|---|---|---|---|---|---|
| New sub gets history | ❌ | ❌ | ❌ | ⚠️ seek + retention can replay | ✅ from offset 0 |
| Topic stores messages | ❌ routing only | ❌ | SNS ❌ (SQS does) | ✅ topic retention | ✅ log is the store |
| No matching subscriber | dropped | dropped | dropped | dropped | still in the log |
| Fan-out copies independent | ✅ own seq+DLQ | ✅ | ✅ | ✅ | ⚠️ one log, many groups |
| Deployment shape | **single SQLite file / embeddable** | managed cluster | managed | managed | cluster |
| Ordering model | group_id FIFO (optional) | Session | FIFO queue | Ordering Key | per-partition |

### One-line positioning
- **Like ASB**: peek-lock + DLQ + Topic/Subscription + Filter + SequenceNumber + dedup
  map almost one-to-one. If you know Azure Service Bus the learning curve is near zero.
- **Unlike Kafka / parts of GCP Pub/Sub**: MQLite is **subscription-based, keeps no
  log, cannot replay**. It is a "work queue + topic fan-out", not a "message log /
  stream". For replay / event sourcing choose something like Kafka.
- **MQLite's own trade-off**: the whole broker is a single SQLite file — embeddable
  in-process, zero-ops, agent-friendly (ships with an MCP server). The price is exactly
  the "no replay / no topic retention" simplifications above — deliberate, not missing.

---

## 5. Speedbumps (easy to trip on)

- Treating a Topic as "an inbox that stores my messages" — it doesn't; subscribe first.
- Assuming fan-out copies share one `seq_number` — they don't; settle each separately.
- Assuming a new subscription back-fills history — it can't.
- Treating the DLQ as a queue you create separately — it is a state of the same queue.
- Publishing to a zero-subscription topic name expecting it to buffer — it actually
  looks for a same-named queue, or errors.

---

# Subscription filters (expr)

A **subscription filter** decides, per message, whether *this* subscription receives a
copy of a published message. A filter is a single [expr-lang](https://expr-lang.org)
boolean expression — for example:

```
subject_parts[0] == "orders" && properties["tier"] == "gold"
```

An **empty filter matches every message**. The expression is **type-checked and
compiled once when you subscribe** (a bad one is rejected immediately with
`400 invalid_argument`) and **run once per message at publish time** (the per-message
gate in the topic fan-out from §1–§3):

```
 Subscribe ──► compile + type-check ──► cache the program     (bad expr → 400)
 Publish   ──► for each subscription: run program(message) ──► route when true
```

Because the filter runs at publish, it sees the message's own fields and timestamps;
evaluation is deterministic and replayable (it never reads a wall clock).

## 6. Setting a filter

| surface | how |
|---|---|
| CLI | `mqlite subscribe orders orders-gold --expr 'properties["tier"]=="gold"'` |
| Go SDK | `cli.Subscribe(ctx, "orders", "orders-gold", &mqlite.Filter{Expr: ` + "`properties[\"tier\"]==\"gold\"`" + `})` |
| HTTP | `POST /mqlite.v1.AdminService/Subscribe` body `{"topic":"orders","name":"orders-gold","filter":{"expr":"..."}}` |

Re-subscribing with the same name and a new `expr` replaces the filter (recompiled on
the next publish). Omit the filter (or use an empty `expr`) to receive everything.

## 7. The message environment

The variables a filter can reference. Unknown names are a compile error (so a typo is
caught at `Subscribe`, not silently at runtime).

### Core

| variable | type | notes |
|---|---|---|
| `subject` | string | the routing label (= ASB Label) |
| `properties` | map | custom string headers — `properties["k"]`, `"k" in properties` (absent key → `""`) |
| `group_id` | string | ordering/session key |
| `correlation_id` | string | |
| `reply_to` | string | |
| `message_id` | string | dedup/idempotency id |
| `content_type` | string | e.g. `application/json` |

### Time

Both are the message's own timestamps (epoch-derived `time.Time`, UTC), not a
wall-clock read.

| variable | type | value |
|---|---|---|
| `enqueued_at` | time | when the message was published — and since fan-out runs at publish, this *is* "now" |
| `visible_at` | time | when it becomes deliverable: equal to `enqueued_at` for an immediate send, the scheduled time for a delayed/`Schedule`d one (never null) |

Compute a delay by subtraction (`time - time` → duration):

```
visible_at - enqueued_at > days(1)        # only route significantly-delayed messages
enqueued_at.Hour() >= 9 && enqueued_at.Hour() <= 21   # business-hours publish window
```

### Derived

Computed from the message; always defined (cannot error on absence).

| variable | type | example |
|---|---|---|
| `subject_parts` | []string | `"orders.eu.new"` → `["orders","eu","new"]` — MQTT-style hierarchy: `subject_parts[0] == "orders"` |
| `body_size` | int | byte length — `body_size < 4096` (route by size, not content) |
| `property_keys` | []string | sorted property names — `len(property_keys) > 0`, `"tier" in property_keys` |

### Body content

Route on the payload itself. These are **projected only when your filter references
them**, so filters that don't touch the body pay nothing.

| variable | type | example |
|---|---|---|
| `body_text` | string | the raw body as text — `body_text contains "urgent"` (always defined; `""` for an empty body) |
| `body_json` | map | the body decoded as a JSON object — `body_json.amount > 100`, `body_json["tier"] == "gold"`, `"k" in body_json` |

`body_json` is **only decoded when `content_type` looks like JSON** (or is unset);
an explicit non-JSON type, an empty body, invalid JSON, or a non-object JSON
(array/scalar) all yield an empty object `{}`. So `body_json` itself is never null —
but reaching into an **absent field** (`body_json.amount` when there is no `amount`)
yields `nil`, and comparing that (`nil > 100`) is a runtime error → **fail-closed**
(the message isn't routed to that subscription, logged). Guard with a presence check
when the field may be missing:

```
"amount" in body_json && body_json.amount > 100
```

## 8. Language

Standard expr operators and builtins are available:

- comparison `== != < <= > >=`, boolean `&& || !` (or `and`/`or`/`not`), membership `in`
- strings: `startsWith`, `endsWith`, `contains`, `matches` (regex)
- collections: `len()`, `all()`, `any()`, `none()`, `filter()`, `map()`, indexing `x[0]`

### Durations

For the time fields, durations are spelled with **type-checked helpers** (recommended,
unambiguous) or an extended `duration()`:

| form | meaning |
|---|---|
| `seconds(n)` `minutes(n)` `hours(n)` `days(n)` `weeks(n)` | a duration; `days`/`weeks` are fixed (24h / 7d) |
| `duration("90m")` `duration("1d12h")` `duration("2w")` | string form; Go's units **plus** `d` (=24h) and `w` (=7d) |

There is no month/year unit — calendars make them ambiguous as fixed spans. For
"older than a month", use `days(30)`.

```
visible_at - enqueued_at == hours(2)
visible_at - enqueued_at > duration("1d")
```

### expr-lang reference

The filter engine **is** [`expr-lang/expr`](https://github.com/expr-lang/expr) (pinned
at **v1.17.8**), so its full language — every operator, literal form, and built-in — is
available and documented upstream. Only two things on this page are MQLite-specific: the
**message environment** in §7 (the variables a filter sees) and the fail-closed boolean
contract in §10; everything else is stock expr. When in doubt about a builtin's exact
behavior, the upstream reference is authoritative.

| What | Where |
|---|---|
| Source / project | <https://github.com/expr-lang/expr> |
| Language reference (canonical) | <https://expr-lang.org/docs/language-definition> |
| Operators | <https://expr-lang.org/docs/language-definition#operators> |
| String builtins | <https://expr-lang.org/docs/language-definition#string-functions> |
| Array/collection builtins | <https://expr-lang.org/docs/language-definition#array-functions> |
| Date/duration builtins | <https://expr-lang.org/docs/language-definition#date-functions> |
| Number builtins | <https://expr-lang.org/docs/language-definition#number-functions> |
| Type-conversion builtins | <https://expr-lang.org/docs/language-definition#type-conversion-functions> |
| Docs home | <https://expr-lang.org/docs/getting-started> |

> Caveat: a few upstream builtins that do IO or are non-deterministic are irrelevant
> here — the filter env exposes no such values and the evaluator is sandboxed
> (memory-safe, side-effect-free, always-terminating; see §10). The duration helpers
> `days()`/`weeks()`/`duration("…d…w")` are MQLite extensions on top of expr's date
> functions, not part of stock expr.

## 9. Filter examples

```
subject_parts[0] == "orders"                              # topic hierarchy
properties["tier"] == "gold" && properties["region"] == "eu"
"priority" in properties && properties["priority"] == "high"
subject startsWith "payment." and not (properties["test"] == "true")
body_size < 4096                                          # small messages only
visible_at - enqueued_at > days(1)                        # delayed > 1 day
len(subject_parts) >= 2 && subject_parts[1] == "eu"
"amount" in body_json && body_json.amount > 100           # route on payload (guarded)
body_text contains "urgent"
```

## 10. Filter safety

Filters are safe to accept from untrusted callers — the env is the only input and
there is no IO:

- expr is **memory-safe, side-effect-free, and always-terminating** by design (no file
  or network access, no unbounded loops).
- A filter must be a **boolean** (`expr.AsBool`); a non-bool expression is rejected at
  `Subscribe`.
- Resource bounds: the source length and compiled AST size are capped.
- **Fail-closed:** if a filter errors at runtime (e.g. `subject_parts[3]` on a
  two-part subject) or panics, the message is **not** routed to that subscription and
  the error is logged — never a broker crash, never a silent match.

## 11. Further reading

- **expr language** — source [`github.com/expr-lang/expr`](https://github.com/expr-lang/expr),
  full reference at <https://expr-lang.org/docs/language-definition> (see §8 for the
  per-category builtin links).
- **Why expr** — [dependencies.md](dependencies.md) explains why `expr-lang/expr` is a
  stable long-term dependency (pinned v1.17.8).
- **Normative invariants** — the conformance spec ([conformance.md](conformance.md) §11)
  for the filter rules MQLite guarantees.
