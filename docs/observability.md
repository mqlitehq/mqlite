# Observability

The broker exposes Prometheus metrics at **`GET /metrics`** (behind Bearer auth, like
the RPCs — a scraper passes the token; only `/` and `/healthz` are open). This guide
wires `/metrics` into Prometheus + Grafana and suggests alerts.

## Metrics

Prometheus text format (`text/plain; version=0.0.4`). Per-queue **gauges**
(subscriptions appear as their backing queue name) plus a per-RPC latency **histogram**:

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `mqlite_queue_messages` | gauge | `queue`, `state` | messages by state: `active`, `locked`, `deferred`, `scheduled`, `dead_lettered` |
| `mqlite_queue_total` | gauge | `queue` | total messages in the queue |
| `mqlite_queue_oldest_message_age_ms` | gauge | `queue` | age of the oldest message (ms) |
| `mqlite_messages_completed_total` | counter | `queue` | messages successfully completed, cumulative since broker start |
| `mqlite_rpc_duration_seconds` | histogram | `rpc`, `le` | RPC handler latency by method (`_bucket` + `_sum` + `_count`) |

```
# HELP mqlite_queue_messages Messages in a queue by state.
# TYPE mqlite_queue_messages gauge
mqlite_queue_messages{queue="orders",state="active"} 42
...
mqlite_queue_total{queue="orders"} 45
mqlite_queue_oldest_message_age_ms{queue="orders"} 1873
# TYPE mqlite_messages_completed_total counter
mqlite_messages_completed_total{queue="orders"} 1280431
# TYPE mqlite_rpc_duration_seconds histogram
mqlite_rpc_duration_seconds_bucket{rpc="QueueService/Receive",le="0.01"} 37
mqlite_rpc_duration_seconds_bucket{rpc="QueueService/Receive",le="+Inf"} 84
mqlite_rpc_duration_seconds_sum{rpc="QueueService/Receive"} 1.426651
mqlite_rpc_duration_seconds_count{rpc="QueueService/Receive"} 84
```

`mqlite_messages_completed_total` is a **lifetime count of processed (Completed)
messages** — it keeps growing after the message row is deleted, so you can answer "how
many were handled" even on an empty queue. It is **in-process and resets on broker
restart** (no durable counter); `rate()` / `increase()` absorb the reset, so processed
throughput stays correct across restarts:
```promql
sum(rate(mqlite_messages_completed_total[5m])) by (queue)   # processed msg/s
increase(mqlite_messages_completed_total[1h])               # processed in the last hour
```

The histogram makes a **slow dequeue visible** — e.g. p99 receive latency in Grafana:
```promql
histogram_quantile(0.99, sum by (le) (rate(mqlite_rpc_duration_seconds_bucket{rpc="QueueService/Receive"}[5m])))
```
A rising `Receive` / `CompleteBatch` tail is the signature of dequeue contention (the
claim path serialising on the single writer). The `rpc` label is the shortened RPC name
(`/mqlite.v1.QueueService/Send` → `QueueService/Send`); only RPCs are timed, not
`/metrics` / `/healthz` / `/ui`.

Quick check:

```bash
curl -H "Authorization: Bearer $MQLITE_TOKEN" https://<host>/metrics
```

## Access log

When a request logger is configured, the broker emits **one line per RPC** with
per-request context, so lines are distinguishable and long durations are explained.
The level is the HTTP status (`2xx` info · `4xx` warn · `5xx` error), and the RPC path
is shortened (`/mqlite.v1.QueueService/Send` → `QueueService/Send`):

```
QueueService/Send          status=200 queue=orders n=1 msg_id=order-42 dur=2ms
QueueService/CompleteBatch status=200 queue=orders n=16              dur=324ms
QueueService/Complete      status=200 queue=orders seq=42            dur=1ms
QueueService/Complete      status=409 queue=orders seq=42 code=lock_lost dur=1ms
QueueService/Receive       status=200 queue=orders msgs=3           dur=8ms
```

Fields: `queue` (every queue/admin op), `msgs` (`Receive`/`Peek`) and `n`
(`Send`/`CompleteBatch`/`Redrive`/`Purge`) counts, `seq` (single settles), `msg_id`
(single `Send`, when supplied), and `code` on a `4xx`/`5xx` (e.g. `lock_lost`,
`not_found`). **An empty `Receive` (`msgs=0`) is logged at Debug**, not Info — an idle
long-poll that returns nothing (up to `wait_time_ms`, max 20s) is expected noise, so the
default Info stream stays clean; enable Debug to see them. (A *slow* `Receive` that does
return messages stays at Info, so genuine slowness is still visible.)

## Prometheus scrape config

`/metrics` needs the Bearer token, so set `authorization` on the scrape job:

```yaml
scrape_configs:
  - job_name: mqlite
    scheme: https              # http if you terminate TLS elsewhere
    metrics_path: /metrics
    authorization:
      type: Bearer
      credentials: mqk_prod_CHANGEME   # one of MQLITE_TOKENS (use a file in real setups)
    static_configs:
      - targets: ["your-mqlite.fly.dev"]
```

> On Fly with scale-to-zero, scraping `/metrics` wakes the machine — fine, but it
> means the broker won't fully idle while Prometheus polls. Lengthen `scrape_interval`
> or accept it stays warm.

## Useful PromQL

```promql
# backlog waiting to be processed, per queue
mqlite_queue_messages{state="active"}

# dead-letter queue size (poison messages) — watch this
mqlite_queue_messages{state="dead_lettered"}

# in-flight (locked) right now
mqlite_queue_messages{state="locked"}

# oldest message age in seconds (is anything stuck?)
mqlite_queue_oldest_message_age_ms / 1000

# total enqueue rate (msgs/s) over 5m
sum(rate(mqlite_queue_total[5m])) by (queue)

# is the DLQ growing? (per-minute increase)
increase(mqlite_queue_messages{state="dead_lettered"}[5m])
```

## Grafana

Point Grafana at the Prometheus datasource and build a per-queue board:

- **Backlog** (timeseries): `mqlite_queue_messages{state="active"}` legend `{{queue}}`.
- **DLQ** (timeseries / stat): `mqlite_queue_messages{state="dead_lettered"}`.
- **In-flight**: `mqlite_queue_messages{state="locked"}`.
- **Oldest age (s)**: `mqlite_queue_oldest_message_age_ms / 1000`.
- **Total**: `mqlite_queue_total`.

Use a `queue` template variable: `label_values(mqlite_queue_total, queue)`.

## Alerting

```yaml
groups:
  - name: mqlite
    rules:
      - alert: MqliteDLQGrowing
        expr: increase(mqlite_queue_messages{state="dead_lettered"}[15m]) > 0
        for: 15m
        annotations: { summary: "DLQ on {{ $labels.queue }} is growing (poison messages)" }

      - alert: MqliteBacklogStuck
        expr: mqlite_queue_oldest_message_age_ms > 300000   # 5 min
        for: 5m
        annotations: { summary: "Oldest message on {{ $labels.queue }} is >5m old — consumers behind?" }

      - alert: MqliteBacklogHigh
        expr: mqlite_queue_messages{state="active"} > 10000
        for: 10m
        annotations: { summary: "Backlog on {{ $labels.queue }} > 10k active" }
```

Tune thresholds to your throughput (see [benchmark.md](benchmark.md) for real numbers).
The DLQ is the one sink that grows unbounded if you don't act — it's bounded by
default ([retention.md](retention.md)), but a growing DLQ still means messages are
failing and worth an alert.
