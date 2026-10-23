# Observability

The canonical `Observe` interface, configured monitor credentials and the
monitoring starter require **v0.3.2 or later**. Earlier releases expose only the
original metrics described under [compatibility](#compatibility-and-v040-migration).

MQLite exposes one canonical observation through `AdminService/Observe`, the Go
SDK, CLI, MCP, console and Prometheus `/metrics`. Queue snapshots and committed
message effects originate in the engine. HTTP request measurements originate in
the server; pure embedded use marks that domain `not_applicable`. Grafana derives
rates, time windows and percentiles from these values instead of maintaining
another set of message counters.

Prometheus scraping is disabled by default. Enable authenticated `/metrics` on
the existing API port with `MQLITE_METRICS=on` or `mqlite serve --metrics`:

```sh
MQLITE_DB=file:/data/mq.db \
MQLITE_TOKENS="$ADMIN_TOKEN" \
MQLITE_MONITOR_TOKENS="$MONITOR_TOKEN" \
MQLITE_METRICS=on \
mqlite serve --addr 127.0.0.1:6754
```

This local example exposes both the API and metrics on loopback. The exporter
shares the broker's process, engine and counters; it needs no extra port or VM.
The Go SDK uses `WithMetrics(true)` alongside `WithTokens(...)` and optionally
`WithMonitorTokens(...)`. Enabling metrics without administrator auth is rejected.

When enabled, `GET /` automatically includes `"metrics":"/metrics"`; clients
resolve this against the same origin. When disabled, discovery omits the field
and `/metrics` returns `404` even with valid credentials.

In production, use a private broker address or an ingress that blocks `/metrics`
from the public Internet while allowing private collectors. API and metrics
share a port, so a port-level firewall cannot separate them. Bearer authentication
protects the data but does not hide a publicly reachable route. The monitor
credential remains required on private networks.

For a runnable broker, Prometheus and Grafana stack, see
[the local observability demo](../ops/observability/README.md). For an existing
platform, use the [cloud and Kubernetes guide](observability-cloud.md).

## Access logs and destructive-operation evidence

`mqlite serve` enables request logging to stderr. Each emitted record has a
broker-generated 32-character `request_id`; the same value is returned in the
`X-Request-ID` response header and exposed to configured CORS clients. The broker
does not trust or copy a caller-supplied request ID. An enabled-CORS preflight is
deliberately not logged, and a successful empty `Receive` is emitted at Debug so
an Info-level handler may filter it. The ID correlates one HTTP attempt with one
log record; it is not an idempotency key and does not authorize retrying a destructive request.

`actor_kind` distinguishes `anonymous`, `unverified`, `static_admin`,
`static_monitor`, and `managed_key`. A successfully authenticated managed key also
records its public `actor_key_id`, unless that ID equals a configured static
credential. Static administrator and monitor credentials have no public ID, and
invalid, expired, or revoked credentials cannot be safely attributed, so those
records never invent one. Authentication headers, token
digests, message bodies, and generated key secrets are not copied into access-log
fields. Resource names and public IDs are logged by design; never put a secret in
either.

The following management records include operation-specific evidence in addition
to status, error code, actor, request ID, and duration:

| RPC | Fields |
|---|---|
| `DeleteQueue` | `queue`, `force`, `acceptance` (`accepted`, `rejected`, or `unknown`), then `result_state` and `deleted` for an accepted result |
| `DeleteSubscription` | `topic`, `sub`, `force`, the same acceptance and result fields |
| `Purge` | `queue`, requested `max` and `older_than_ms` bounds, and confirmed `purged` count |
| `CreateKey`, `RevokeKey` | safe syntactically valid public `key_id`; a value equal to a configured static credential is omitted; successful operations add `result_state=created` or `revoked` |

These records are operational evidence, not a persistent audit table. A success
record describes the broker result; an `acceptance=unknown` deletion still needs
read-only reconciliation. Logs do not remove the rule that one owner must
serialize deletion with same-name recreation. Resource names and public key IDs
may themselves be sensitive operational metadata, so restrict log access.

The default access log exists only on stderr. Container and service platforms may
discard it quickly, and `MQLITE_DIAGNOSTIC_LOG` deliberately excludes routine
requests. Forward stderr to an external log store and set its retention to cover
your incident-response and destructive-request reconciliation window. Embedded
brokers must pass a retained logger with `WithRequestLog`; a JSON `slog` handler is
recommended when machines consume the evidence. Log delivery and storage
durability remain the operator's responsibility.

## Maintenance failure diagnosis

v0.4.0 adds optional `maintenance[].diagnostics` and
`storage.operations[].last_error` fields to Observe. v0.3.2 and older snapshots
remain valid; missing diagnostics mean **not reported**, not healthy. Existing
Prometheus labels and cumulative error buckets are unchanged.

Each maintenance task reports its latest outcome (`never`, `ok`, `failed`, or
`interrupted`), last run time, consecutive failed passes, recovery time, and the
latest eight failed steps. One pass can fail at several steps but increments the
failure counter only once. Canceled/closed operations remain interruptions. A
successful pass clears the consecutive streak and records recovery; it does not
clear cumulative failures or history. All snapshot counters/history reset when the
broker process restarts. A failed poll makes the console's previous data stale.

Error details include a timestamp, generated explanation, category, HTTP status or
SQLite code where available, and an opaque fingerprint. Categories distinguish
provider authentication, rate limits, provider availability, DNS, TLS, timeouts,
protocol, constraints, schema, and storage failures. These descriptions summarize
the observed error; an HTTP 503 alone cannot establish the provider's root cause.
Unknown commit outcomes remain explicitly ambiguous and must be reconciled before
retrying. Diagnostic changes do not broaden write retries.

A storage bucket's count covers **all** occurrences in that legacy bucket;
`last_error` describes only its latest failure. Neither a historical count nor a
successful maintenance pass proves message integrity or high availability. Empty
queues still run background database maintenance, so failures can occur while no
clients are sending or receiving.

### Private diagnostic log

Structured stderr events `maintenance.failure`, `maintenance.interrupted`,
`maintenance.recovered`, `storage.interrupted`, and `storage.failure` record the task/step or operation.
Failure events include the same fingerprint as Observe and a bounded provider
explanation with common credentials and URLs redacted. Public monitor responses
contain generated explanations only, never the raw provider/SQL error. Private
logs may still contain sensitive database error context; keep them operator-only.

To retain evidence beyond the platform's stderr retention, enable a separate file:

```sh
MQLITE_DIAGNOSTIC_LOG=/data/mqlite-diagnostics.jsonl mqlite serve
# Equivalent: mqlite serve --diagnostic-log /data/mqlite-diagnostics.jsonl
```

The parent directory must exist. The file is disabled by default; an invalid
configured path fails startup. It appends across restarts, uses mode `0600`, syncs
each record, and rotates at 4 MiB with three archives (`.1` through `.3`, newest
first): at most 16 MiB of retained diagnostic records, not a time-based retention
promise. Routine requests and successful maintenance passes are excluded. A
successful recovery is logged once per failure streak. A disk write failure still
emits the original event to stderr plus one warning per logging failure streak.

Mount the file on persistent storage to survive machine replacement/deployment;
Fly's ephemeral root filesystem is insufficient. Export archives elsewhere if
evidence must survive volume loss or sustained errors filling the rotation budget.
The file is independent of the queue database, and is not replayed into Observe
history after restart. It is diagnostic evidence, not a message backup or failover
mechanism. Embedded applications can supply their own logger with `WithLogger`.

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
    # terminates TLS in front of the private broker.
    scheme: http
    metrics_path: /metrics
    scrape_interval: 15s
    scrape_timeout: 10s
    authorization:
      type: Bearer
      credentials_file: /run/secrets/mqlite-monitor.token
    static_configs:
      - targets: [mqlite.internal.example:6754]
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

Queue gauges come from an exact grouped database query at the reported
`sampled_at_ms`. One process-local gauge snapshot is shared across native
`Observe`, `/metrics` and console requests for about 10 seconds. A cache hit
preserves the original timestamp instead of presenting cached rows as new.
Message writes and polling do not extend the fixed cache deadline; a queue create
or delete invalidates the snapshot. The deadline begins when a refresh finishes,
so after an unusually slow scan the gauges can trail current messages by the scan
duration plus roughly one cache window. Per-queue `Stats` remains a fresh indexed
read.

Counts describe stored states at the sample time, not guaranteed immediate
delivery eligibility: visibility, TTL, lease expiry awaiting maintenance and
ordering rules can affect claims. Runtime probes, process counters and queue
gauges are sampled at different instants. The observation is not an atomic
transaction encompassing every database, HTTP and in-process counter. Collection
duration is the elapsed time of the gauge refresh that produced `sampled_at_ms`;
it remains unchanged on a cache hit.

`/healthz` is open process liveness. `mqlite_storage_read_available` reports a real
storage read probe; success does not prove writes, end-to-end delivery or data
integrity. Failed read latency is absent, not zero. Local storage size is unavailable
for memory/remote backends, not a fabricated zero-byte database.

### Collection cost and interval

Each observation runs a storage read probe. A cache refresh also collects the full
queue inventory with one grouped message scan. State-filtered aggregates avoid
updating every state counter for every row, but the exact refresh still reads all
retained rows. Concurrent refreshes share that scan, and successful rows are
reused for about 10 seconds; failed scans are not cached, so later requests can
retry. Refresh cost therefore grows with current backlog as well as queue count.
Here retained means rows present in `messages` at the sample time, not lifetime
throughput. Completed messages are normally deleted immediately; active, locked,
deferred, scheduled and dead-lettered rows remain until their corresponding
lifecycle action, while an abnormal retained `completed` row is still counted in
`total` and `unexpected`. Exact per-queue `Stats` uses one literal branch per
message state and the existing queue-prefixed partial indexes, so it does not scan
unrelated queues.

The cache retains one queue-gauge slice whose size grows with queue inventory,
not backlog depth. Each request receives its own slice and still allocates and
serializes the full response.

A local file database runs `Stats`, the `SELECT 1` storage probe and global refresh
on one dedicated `mode=ro` plus `query_only` connection. It cannot write or
checkpoint, and WAL reads do not occupy the sole writer connection, so a deep
refresh does not sit in front of Send, Receive or
settlement. Reader work serializes on that one connection. A refresh still
consumes CPU and storage bandwidth and holds a WAL snapshot until it finishes;
long scans can delay checkpoint progress and grow the WAL. `:memory:` cannot use a
second connection without becoming a different database, so its refresh and
probe still share the sole writer connection. Remote Turso/libSQL observations
use the existing small provider pool and consume provider query resources.

Start production scraping at 15 seconds, measure collection duration and actual
send/receive latency under the expected retained backlog, then adjust the scrape
and native/console polling intervals. Requests inside the roughly 10-second gauge
window share one snapshot, but each request still runs a storage probe and builds
its response; aggressive polling is not free. Grafana dashboard refreshes query
Prometheus instead of adding direct broker requests. The local demonstration uses
a faster interval to make transitions visible; choose production cadence from
measured workload needs.

RPC/result and storage/error dimensions have fixed vocabularies, but their
zero-initialized counters and histogram buckets still produce many series. Queue
dimensions grow with the configured inventory, including active subscription
backing queues. The default 10,000-subscription ceiling is a metadata guard, not a
promise that a 10,000-entity observation is cheap. Measure scrape response size,
duration, memory and series count before increasing inventory or polling
frequency; a working small demo does not establish a large-deployment capacity
limit.

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
| `mqlite_collection_duration_seconds` | gauge | Latest queue-gauge refresh elapsed time; unchanged on cache hits |
| `mqlite_entities` | gauge | `kind=queue|subscription`; queue count excludes subscriptions and topic routing names |
| `mqlite_queue_messages` | gauge | `queue`, `state=active|locked|deferred|scheduled|dead_lettered` |
| `mqlite_queue_retained_messages` | gauge | `queue`; all retained rows including unexpected states |
| `mqlite_queue_unexpected_messages` | gauge | `queue`; retained rows outside the five normal states |
| `mqlite_queue_oldest_message_age_seconds` | gauge | `queue`; original enqueue age of oldest active/locked row, 0 if none |
| `mqlite_message_events_total` | counter | `queue`, `event`; confirmed effects defined below; `queue=""` combines retired queues |
| `mqlite_rpc_requests_total` | counter | `rpc`, `code`; registered RPC requests including authentication rejection |
| `mqlite_rpc_request_duration_seconds_total` | counter | Same labels; cumulative whole-request elapsed time, including authentication |
| `mqlite_rpc_duration_seconds` | histogram | `rpc`; post-authentication handler latency, preserving the existing timing scope |
| `mqlite_authentication_total` | counter | `outcome=success|missing|invalid|expired|revoked|permission_denied|backend_error` |
| `mqlite_storage_operations_total` | counter | `operation`, `outcome`, `error_code` |
| `mqlite_storage_operation_duration_seconds` | histogram | Same labels; storage-operation elapsed time |
| `mqlite_storage_retries_total` | counter | Same labels; safe retry attempts within the observed operation |
| `mqlite_storage_pool_connections` | gauge | `state=max_open|open|in_use|idle`; local files aggregate the one writer and one observation reader |
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
`dedup`, `receipts`, `retention`, `reclaim`, `subscription_expiry`,
`lifecycle`. `subscription_expiry` is the one-second indexed transition of due
fixed-lifetime subscriptions; a full 1,000-row due batch continues after a 10 ms
yield, and data-plane expiry does not wait for the scan. `lifecycle` is the
event-driven bounded cleanup pass for queue and subscription backing queues
already marked deleting. A full 512-row empty-subscription batch continues after
a 10 ms yield. Loaded-message cleanup, errors and confirmation passes remain
paced at one second; an idle store does not run the task, so a quiet
`last_success` timestamp is expected. An uncertain remote delete
commit temporarily schedules up to 30 one-second discovery passes even when the
first inventory read is empty. Check enabled status and
each task's actual cadence before alerting on its last-success time.
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

After physical queue or subscription deletion completes, MQLite removes that
backing queue's named counter series and folds its existing counts into one bounded
retired aggregate. Native
observations and Prometheus expose this aggregate with `queue=""`; the empty label
does not name an addressable queue. It combines all retired queue names and any
late transaction effects from an older queue-topology epoch. Recreating the same
name starts a fresh named series while the prior counts remain in the aggregate.
Include `queue=""` when querying total process activity, or filter it out when a
query is intended to describe current named queues only.

v0.4.1 corrects the official Go decoder and bundled console to accept this
documented empty label. v0.4.0 already emitted the aggregate, but its strict
clients rejected the otherwise complete observation after such a retirement.
The wire shape, event semantics and schema token do not change in v0.4.1.

Raw SQL through `EngineTx.SQL` is supported only for caller-owned tables; those
writes produce no message-effect events. Direct access to mqlite-owned tables is
unsupported and can bypass lifecycle and observability invariants.

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

### Compatibility and v0.4.0 migration

Prometheus scraping has been disabled by default since v0.3.2. On upgrade, enable
`MQLITE_METRICS=on` or `--metrics` to keep scraping the same API port, and configure
a monitor token for the collector.

v0.4.0 removes the two ambiguous aliases whose removal was announced in v0.3.2.
Update recording rules, alerts, dashboards and direct PromQL before replacing the
broker:

| Removed in v0.4.0 | Replacement |
| --- | --- |
| `mqlite_queue_total` | `mqlite_queue_retained_messages` (same gauge value) |
| `mqlite_queue_oldest_message_age_ms` | `mqlite_queue_oldest_message_age_seconds` (divide old thresholds by 1,000) |

The native `Observe` JSON contract is unchanged: `queues[].oldest_message_age_ms`
still uses milliseconds. The following original Prometheus families remain as
compatibility projections of canonical measurements; they do not maintain
separate counters:

| Existing family | Canonical mapping |
| --- | --- |
| `mqlite_queue_messages` | Same queue state gauges |
| `mqlite_messages_completed_total` | `mqlite_message_events_total{event="completed"}` |
| `mqlite_rpc_duration_seconds` | Same post-authentication handler histogram |

No independent lifetime completion counter is introduced.

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

DeleteQueue, DeleteSubscription and ListSubscriptionsPage are additive RPC
observation extensions. New brokers include their request counters plus the
`queue_not_empty`, `subscription_not_empty` and `resource_exhausted` outcomes.
`ListQueues.state` and the paged subscription inventory with
`include_deleting=true`, rather than the canonical observation's queue gauges,
identify resources whose cleanup is pending. The `subscription_expiry` and
`lifecycle` maintenance rows report transition and cleanup health; a force or
expiry-authorized discard does not masquerade as normal
completion or retention message effects. The required decoder baseline is
unchanged, so new clients continue to accept observations from older brokers.
An occupied-resource or capacity rejection is an application outcome, not a
storage failure.
