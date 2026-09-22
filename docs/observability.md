# Observability

The canonical `Observe` interface, configured monitor credentials and the
monitoring starter require **v0.3.2 or later**. Earlier releases expose only the
legacy metrics described under [compatibility](#compatibility-from-v031).

MQLite exposes one canonical observation through `AdminService/Observe`, the Go
SDK, CLI, MCP, console and Prometheus `/metrics`. Queue snapshots and committed
message effects originate in the engine. HTTP request measurements originate in
the server; pure embedded use marks that domain `not_applicable`. Grafana derives
rates, time windows and percentiles from these values instead of maintaining
another set of message counters.

The Prometheus endpoint is deliberately separate from the broker API. The API
listener never serves `/metrics`; it returns `404` even for an administrator
credential. A metrics listener is disabled by default and is enabled with
`MQLITE_METRICS_ADDR` or `mqlite serve --metrics-addr`. It uses the same process,
engine and counters, so it does not require a second writer or VM:

```sh
MQLITE_DB=file:/data/mq.db \
MQLITE_TOKENS="$ADMIN_TOKEN" \
MQLITE_MONITOR_TOKENS="$MONITOR_TOKEN" \
MQLITE_METRICS_ADDR=127.0.0.1:9091 \
mqlite serve --addr :6754
```

The example binds locally for development. In a deployment, replace the loopback
address with the broker's private interface or service address and keep port 9091
out of public ingress. The monitor credential is still required; a private
network is an additional boundary, not a replacement for Bearer authentication.
When the listener is disabled, the discovery card leaves `metrics` empty.

For a runnable broker, Prometheus and Grafana stack, see
[the local observability demo](../ops/observability/README.md). For an existing
platform, use the [cloud and Kubernetes guide](observability-cloud.md).

## Read-only monitoring access

Configure a separate scraper credential in `MQLITE_MONITOR_TOKENS`, alongside
configured administrator credentials in `MQLITE_TOKENS`. Monitoring credentials
can read `/metrics` and `AdminService/Observe`. They cannot send, receive, peek at
message bodies, manage queues, inspect administrative configuration or issue keys.
Administrator credentials and managed `manage` keys retain monitoring access.

Configured monitor credentials do not query the database during authentication,
so they can observe storage failure. Managed keys still require their current
revocation and expiry state to be checked; no monitoring cache bypasses that check.
Do not reuse an administrator token as a monitor token. Configuration changes
require a broker restart; the demo's startup wrapper reads separate secret files.

A Prometheus job should reference a mounted secret file:

```yaml
scrape_configs:
  - job_name: mqlite
    # The native listener is plain HTTP. Use https only when a TLS proxy
    # terminates TLS in front of the private listener.
    scheme: http
    metrics_path: /metrics
    scrape_interval: 15s
    scrape_timeout: 10s
    authorization:
      type: Bearer
      credentials_file: /run/secrets/mqlite-monitor.token
    static_configs:
      - targets: [mqlite-metrics.internal.example:9091]
```

If a TLS proxy fronts the listener, use the correct TLS CA rather than disabling
certificate checks. Grafana connects to Prometheus and does not need the MQLite
token. See the
[Prometheus authorization reference](https://prometheus.io/docs/prometheus/latest/configuration/configuration/).

## Availability and freshness

The native snapshot includes `sampled_at_ms`, a collection status, last successful
collection time and elapsed collection duration. A failed queue read is unavailable,
not an empty list of healthy queues. Prometheus responds with available in-process
measurements and `mqlite_collection_success 0`, omitting queue gauges that could not
be collected. Failure before the endpoint, including invalid scrape credentials,
causes Prometheus `up` to become zero instead.

Queue counts are actual retained rows from a grouped database query. They describe
stored states, not guaranteed immediate delivery eligibility: visibility, TTL,
lease expiry awaiting maintenance and ordering rules can affect claims. Different
measurement domains are sampled at different instants. The observation is not an
atomic transaction encompassing every database, HTTP and in-process counter.

`/healthz` is open process liveness. `mqlite_storage_read_available` reports a real
storage read probe; success does not prove writes, end-to-end delivery or data
integrity. Failed read latency is absent, not zero. Local storage size is unavailable
for memory/remote backends, not a fabricated zero-byte database.

### Collection cost and interval

Each observation collects the full queue inventory with one grouped message scan
and a storage read probe. The scan includes retained rows, so its cost grows with
backlog as well as queue count. A local SQLite database uses the same single-writer
connection for application work and collection; monitoring is not free or isolated
from message-operation latency.

Start production scraping at 15 seconds, measure collection duration and actual
send/receive latency under the expected retained backlog, then adjust the scrape
and native/console polling intervals. Each native observation or Prometheus scrape
collects its own snapshot. Grafana dashboard refreshes query Prometheus instead
of adding direct broker scans. The local demonstration uses a faster interval to
make transitions visible; choose production cadence from measured workload needs.

RPC/result and storage/error dimensions have fixed vocabularies, but their
zero-initialized counters and histogram buckets still produce many series. Queue
dimensions grow with the configured inventory. Measure scrape response size,
duration and series count before increasing inventory or polling frequency; a
working small demo does not establish a large-deployment capacity limit.

## Metric contract

Prometheus uses text exposition 0.0.4. Labels are bounded categories except for
configured entity names. Credentials, key IDs, message IDs, payloads, expressions,
raw URLs and free-text errors are never labels. Durations use seconds; timestamps
use Unix seconds in Prometheus and epoch milliseconds in native JSON.

| Family | Type | Labels / meaning |
| --- | --- | --- |
| `mqlite_info` | gauge, value 1 | `version`, `backend`, `schema_version` |
| `mqlite_start_time_seconds` | gauge | Engine start timestamp |
| `mqlite_sample_timestamp_seconds` | gauge | Snapshot timestamp |
| `mqlite_collection_success` | gauge | 1 when queue collection succeeds, otherwise 0 |
| `mqlite_collection_status` | gauge, value 1 | `state`, bounded `error_code` |
| `mqlite_collection_last_success_timestamp_seconds` | gauge | Last successful collection, 0 before one succeeds |
| `mqlite_collection_duration_seconds` | gauge | Queue collection elapsed time |
| `mqlite_entities` | gauge | `kind=queue|subscription`; queue count excludes subscriptions and topic routing names |
| `mqlite_queue_messages` | gauge | `queue`, `state=active|locked|deferred|scheduled|dead_lettered` |
| `mqlite_queue_retained_messages` | gauge | `queue`; all retained rows including unexpected states |
| `mqlite_queue_unexpected_messages` | gauge | `queue`; retained rows outside the five normal states |
| `mqlite_queue_oldest_message_age_seconds` | gauge | `queue`; original enqueue age of oldest active/locked row, 0 if none |
| `mqlite_message_events_total` | counter | `queue`, `event`; confirmed effects defined below |
| `mqlite_rpc_requests_total` | counter | `rpc`, `code`; registered RPC requests including authentication rejection |
| `mqlite_rpc_request_duration_seconds_total` | counter | Same labels; cumulative whole-request elapsed time, including authentication |
| `mqlite_rpc_duration_seconds` | histogram | `rpc`; post-authentication handler latency, preserving the existing timing scope |
| `mqlite_authentication_total` | counter | `outcome=success|missing|invalid|expired|revoked|permission_denied|backend_error` |
| `mqlite_storage_operations_total` | counter | `operation`, `outcome`, `error_code` |
| `mqlite_storage_operation_duration_seconds` | histogram | Same labels; storage-operation elapsed time |
| `mqlite_storage_retries_total` | counter | Same labels; safe retry attempts within the observed operation |
| `mqlite_storage_pool_connections` | gauge | `state=max_open|open|in_use|idle` |
| `mqlite_storage_pool_waits_total` | counter | Connection-pool waits |
| `mqlite_storage_pool_wait_duration_seconds_total` | counter | Cumulative pool waiting time, distinct from SQL execution |
| `mqlite_storage_read_available` | gauge | Storage read probe succeeded, 0 or 1 |
| `mqlite_storage_ping_seconds` | gauge | Read round trip; omitted on failure |
| `mqlite_storage_size_available` | gauge | Local footprint can be reported, 0 or 1 |
| `mqlite_storage_size_bytes` | gauge | Local DB plus WAL/shared-memory footprint; omitted if unavailable |
| `mqlite_maintenance_enabled` | gauge | `task`; disabled/inapplicable task is 0 |
| `mqlite_maintenance_runs_total` | counter | `task`, `outcome=success|error|interrupted` |
| `mqlite_maintenance_duration_seconds` | histogram | `task`; maintenance pass elapsed time |
| `mqlite_maintenance_last_success_timestamp_seconds` | gauge | `task`; last successful pass, 0 before success |
| `mqlite_filter_failures_total` | counter | Routing failures, `stage=compile|evaluate`; normal filter mismatch is not an error |

Histograms serialize finite cumulative `_bucket` samples, a `+Inf` bucket,
`_count` and `_sum`. Whole-request duration currently supplies a sum and request
count, not a histogram: their ratio is a mean, never a percentile.

Storage operations are `read`, `write` and `transaction`; outcomes are `ok`,
`error`, `rejected` and `outcome_unknown`. `rejected` with error code `application`
means the operation was rejected by application/transaction callback logic; it is
not evidence of a database outage. Counts represent logical engine storage-wrapper
calls, not individual messages or every SQL statement inside a transaction.
Retries are counted separately. Internal maintenance and observation reads are
included; collecting a snapshot itself contributes read/probe operations.
Error categories are empty on success or one of
`canceled`, `closed`, `busy`, `connection`, `full`, `corrupt`, `io`,
`application`, `outcome_unknown`, `other`. Fixed storage outcome/error combinations
and registered RPC/result combinations are initialized to zero, so the first
event can be detected after an earlier successful scrape. Use `rate`/`increase`
across observed samples; a first scrape after an event cannot reconstruct its time.
Maintenance tasks are `locks`, `scheduled`, `ttl`,
`dedup`, `receipts`, `retention`, `reclaim`. Check enabled status and each task's
actual cadence before alerting on its last-success time.
Canceled passes during normal shutdown are `interrupted`, not successful passes
or maintenance errors; they do not advance the last-success timestamp.
Filter counters cover compilation/evaluation failures while routing published
messages. Invalid `Subscribe` configuration is an RPC `invalid_argument` result;
deliberate `TestFilter` validation is not a routing failure.

### Message effects

Counters increase for confirmed effects, after transaction commit. A rollback,
transaction retry or exact receive/settlement replay does not create a second
committed effect. An uncertain remote outcome is not classified as a known
rollback. Counters are process-local and reset at restart; unobserved activity
before a crash can be lost. They are operational measurements, not a durable
billing or business audit ledger.
Custom raw SQL through `EngineTx.SQL` is not attributed to message-effect events.
Queue gauges still report the actual retained database rows, including unexpected
states; do not use event counters to reconstruct changes made outside the normal
message operations.

| Event | Meaning |
| --- | --- |
| `enqueued` | New queue copies; topic fanout counts actual copies, not producer requests |
| `scheduled` | Enqueued copies initially scheduled; a subset of enqueue activity |
| `deduplicated`, `dedup_conflict` | Suppressed duplicates and conflicting dedup attempts |
| `delivered`, `redelivered` | Committed claims; redelivery is a subset, not additional unique messages |
| `completed`, `receive_deleted` | Successful completion removals and receive-and-delete removals |
| `abandoned`, `deferred`, `rejected` | Confirmed settlement effects |
| `dead_lettered` | Transitions into the DLQ, including automatic paths |
| `ttl_discarded`, `retention_deleted` | TTL discard and dead-letter retention deletion |
| `purged`, `canceled`, `redriven` | Administrative DLQ purge, scheduled cancellation and redrive effects |
| `lock_expired`, `recovered`, `activated` | Expired-lease handling, startup recovery and scheduled activation |

Some events overlap. For example, rejection can also enter the DLQ; do not sum all
events into a message ledger. A claim does not prove a response reached the client,
and completion means the consumer reported success, not independently verified
business-side processing. Broker redelivery, client HTTP retry and storage retry
are separate concepts.

### Compatibility from v0.3.1

All five pre-existing families remain available. These compatibility views read
canonical values; they do not maintain separate counters:

| Existing family | Canonical mapping |
| --- | --- |
| `mqlite_queue_messages` | Same queue state gauges |
| `mqlite_queue_total` | Same value as `mqlite_queue_retained_messages`; still a gauge |
| `mqlite_queue_oldest_message_age_ms` | Canonical seconds multiplied by 1000 |
| `mqlite_messages_completed_total` | `mqlite_message_events_total{event="completed"}` |
| `mqlite_rpc_duration_seconds` | Same post-authentication handler histogram |

The ambiguous `queue_total` and millisecond age names are deprecated for new
consumers; use the canonical names. They are retained until the next minor version
(v0.4.0), when any removal requires an explicit migration. No independent lifetime
completion counter is introduced.

## Dashboard and useful queries

The provisioned dashboard offers instance/queue selection, traffic, backlog,
request/authentication results, storage, maintenance and active alerts. Empty
samples remain “No data” or “Not available”; they are not replaced with green zeros.

```promql
# Stored queue states, not immediate claim eligibility
mqlite_queue_messages{state="active"}

# Observed completion throughput; rate handles observed counter resets
sum by (instance, queue) (rate(mqlite_message_events_total{event="completed"}[5m]))

# Handler p95; Receive includes intentional long-poll waiting
histogram_quantile(0.95, sum by (instance, rpc, le) (
  rate(mqlite_rpc_duration_seconds_bucket[5m])))

# Whole-request arithmetic mean, including authentication
sum by (instance, rpc) (rate(mqlite_rpc_request_duration_seconds_total[5m]))
/
sum by (instance, rpc) (rate(mqlite_rpc_requests_total[5m]))

# Newly observed dead-letter transitions, not net DLQ depth change
increase(mqlite_message_events_total{event="dead_lettered"}[5m])
```

Apply `rate`/`increase` to counters, not queue depth gauges. `delta` on depth is net
retained-state change and cannot measure failure throughput. Do not subtract sends
and receives to infer loss, count locked rows as live consumers, or sum the same
database snapshot from duplicate collectors. Requests may contain batches. Alert
thresholds should match the application's SLA and minimum request volume.

## Runbooks

### Scrape or collection failure

Inspect the Prometheus target's `lastError` first. A down target can mean a stopped
broker, route/TLS failure, invalid scraper credentials or an endpoint failure.
With a working configured monitor credential, `up=1` and `collection_success=0`
means metrics are reachable but the queue snapshot failed. Check bounded storage
errors and broker logs; do not treat omitted gauges as zero. Preserve evidence
before intervention. After recovery, verify a fresh successful snapshot and a
controlled send/receive/complete canary.

### Queue backlog and dead letters

Compare oldest active/locked age, depth, delivery and completion trends with the
application's intended schedule. Check consumers, lease duration and ordering.
Scheduled/deferred work may be intentional. Inspect dead-letter reasons and fix
the failure before redrive. Retention, purge and redrive can reduce depth while new
failures continue. Unexpected retained states require investigation, not deletion
just to silence the graph. Filter failure is distinct from a normal non-match.

### Authentication and integration

Use aggregate outcomes to distinguish missing/invalid credentials, revoked or
expired keys and insufficient permissions. Client connection failures that never
reach MQLite need client telemetry. Grafana datasource failure is a different hop
from Prometheus scrape failure. Rotate the configured monitor token by allowing
both old/new distinct monitoring tokens temporarily, restarting the broker,
updating the scraper file, confirming at least two successful scrapes, then removing
the old token and restarting. Never grant the scraper send/manage privileges to
fix a routing error.

### Storage and maintenance

Check the read probe, operation outcome, error category, retry count and pool
waiting. `outcome_unknown` requires idempotent reconciliation; a blind resend can
produce duplicates. A healthy read does not prove write availability. Check
maintenance enabled status and cadence before declaring a stalled task. Reuse
host/container monitoring for free disk, CPU, OOM and volume health. For disk-full,
restore or integrity incidents, follow [operations.md](operations.md#incident-actions).

## Logs, alerts and validation

Request logs retain operation/status context, queue and item counts; empty Receive
responses are debug noise by default. Logs can help diagnose a slow nonempty
request without placing payloads, credentials or unbounded errors into metric
labels. Normal long polling can raise Receive latency without a storage problem.

The [example rules](../ops/observability/prometheus/rules.yml) cover sustained scrape,
collection/storage failure, stale collection, unexpected states, backlog age,
retained DLQ, repeated auth failures, uncertain outcomes, maintenance and filter
failures. Tune them before production. Prometheus evaluates conditions; configure
Alertmanager or the platform's notification receiver separately. The local demo's
short `MQLiteDemoDeadLetter` rule is explicitly a test rule, not production policy.

The [real stack verifier](../test/observability/verify.py) checks authenticated
scraping, monitor isolation, canonical/native agreement, actual message identity
and body, replay, redelivery, scheduled/deferred work, DLQ firing/resolution,
TTL discard/dead-letter handling, retention deletion, configuration/routing filter
errors, credential outcomes, Grafana provisioning and every panel query. Deliberate
invalid queries and missing datasources verify that the checker rejects failures.
`--faults` also
interrupts only that isolated stack to verify scrape-auth recovery, Grafana
connection failure/recovery and persistence/counter reset after broker restart.
Managed-cloud/Kubernetes deployment validation remains environment-specific.
