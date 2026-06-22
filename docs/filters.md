# Subscription filters (expr)

A topic fans every published message out to its subscriptions. A **subscription
filter** decides, per message, whether *this* subscription receives a copy. A filter
is a single [expr-lang](https://expr-lang.org) boolean expression — for example:

```
subject_parts[0] == "orders" && properties["tier"] == "gold"
```

An **empty filter matches every message**. The expression is **type-checked and
compiled once when you subscribe** (a bad one is rejected immediately with
`400 invalid_argument`) and **run once per message at publish time**.

```
 Subscribe ──► compile + type-check ──► cache the program     (bad expr → 400)
 Publish   ──► for each subscription: run program(message) ──► route iff true
```

Because the filter runs at publish, it sees the message's own fields and timestamps;
evaluation is deterministic and replayable (it never reads a wall clock).

## Setting a filter

| surface | how |
|---|---|
| CLI | `mqlite subscribe orders orders-gold --expr 'properties["tier"]=="gold"'` |
| Go SDK | `cli.Subscribe(ctx, "orders", "orders-gold", &mqlite.Filter{Expr: ` + "`properties[\"tier\"]==\"gold\"`" + `})` |
| HTTP | `POST /mqlite.v1.AdminService/Subscribe` body `{"topic":"orders","name":"orders-gold","filter":{"expr":"..."}}` |

Re-subscribing with the same name and a new `expr` replaces the filter (recompiled on
the next publish). Omit the filter (or use an empty `expr`) to receive everything.

## The message environment

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

## Language

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

## Examples

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

## Safety

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

See [dependencies.md](dependencies.md) for why `expr-lang/expr` is a stable long-term
dependency, and the conformance spec ([conformance.md](conformance.md) §11) for the
normative filter invariants.
