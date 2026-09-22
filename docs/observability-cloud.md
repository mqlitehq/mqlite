# Cloud and Kubernetes monitoring

These examples require **MQLite v0.3.2 or later**, including configured monitor
credentials and the canonical metric families.

MQLite exposes native Prometheus metrics on an optional, authenticated listener
separate from the public API listener. The same dashboard and metric definitions
work with a private Prometheus collector or a compatible managed service. Keep the
broker's metrics authenticated: configure a distinct `MQLITE_MONITOR_TOKENS`
credential, give only that credential to the collector, and keep administrator
credentials out of monitoring configuration. See [observability.md](observability.md)
for the metric contract and the runnable local stack.

## Common integration

```text
MQLite :9091/metrics <-- private route + monitor Bearer token -- collector
                                                          |
                                              Prometheus-compatible storage
                                                          |
                                               existing Grafana + rules
```

1. Locate the collector in the broker's private network, or provide an authenticated
   TLS endpoint reachable only from approved collectors. Configure the correct CA;
   do not disable TLS certificate verification. Bind the exporter with
   `MQLITE_METRICS_ADDR` (for example `:9091`) and allow that port only from the
   collector's private network. The public API listener (`6754`) does not serve
   `/metrics`.
2. Configure `/metrics` on the private metrics listener, the actual metrics port,
   a 15-second starting interval and a shorter scrape timeout. Use a secret file or
   your collector's secret reference; do not put the credential into target URLs,
   labels or dashboard JSON.
3. Verify `up{job="mqlite"} == 1` and `mqlite_collection_success == 1`. Scrape success
   alone does not establish database availability or successful business processing.
4. Import the [standard dashboard](../ops/observability/grafana/dashboards/mqlite.json),
   select the platform datasource, and adopt the [rule groups](../ops/observability/prometheus/rules.yml).
   Keep the `job="mqlite"` label or deliberately adjust the rules to your collector.
5. Configure the platform's notification receiver and test delivery separately.
   Firing rules prove condition evaluation, not that an email or page was delivered.

The [existing-broker job](../ops/observability/prometheus/existing-broker.example.yml)
is a reusable starting point. Prometheus supports Bearer authorization through
`credentials_file`; a cloud console may expose a different secret-binding mechanism.
Do not paste a local filesystem path into a managed service unless that service
actually mounts that file. [Prometheus configuration reference](https://prometheus.io/docs/prometheus/latest/configuration/configuration/).

## Alibaba Cloud: ECS or ACK

For ECS, Managed Service for Prometheus supports application endpoints reachable
by its collectors. Place the collector in the correct VPC and configure custom
metric collection for the private broker address and port. Limit security-group
access to the collector's actual source network; a publicly reachable metrics port
is unnecessary. The official guide also covers reachable non-ECS machines in the
same VPC. [Alibaba Cloud application metric collection](https://www.alibabacloud.com/help/en/cms/cloudmonitor-2-0/collect-application-metrics-of-ecs-deployment-to-the-prometheus-instance).

Use your service's custom scrape job and credential support to supply the monitor
Bearer token. Confirm the generated scrape configuration, target status and actual
query result. If the selected managed collector cannot securely attach the header,
run a small private Prometheus collector with the supplied job and use the
platform's supported remote-write integration; do not switch off MQLite auth.
ECS custom job discovery and update procedures are documented in
[service discovery rules](https://www.alibabacloud.com/help/en/arms/prometheus-monitoring/manage-service-discovery-rules-for-ecs-environments).

For ACK, use your installed collector's supported ServiceMonitor/PodMonitor or
custom-job mechanism. The [Kubernetes examples](../ops/observability/kubernetes/README.md)
target upstream Prometheus Operator. Check the actual CRD version, namespace
selectors and Secret access in your ACK installation before applying them.

## Fly.io

Fly's documented automatic custom-metrics configuration provides a port and path.
It does not document a per-target MQLite Bearer credential in that stanza, so adding
`[metrics]` alone is not a verified way to scrape an authenticated broker. Keep the
broker protected. A private collector using the reusable Prometheus job can attach
the monitor token and scrape the broker's internal DNS address, for example
`your-mqlite-app.internal:9091`. Configure MQLite with
`MQLITE_METRICS_ADDR=fly-local-6pn:9091`; expose only that port through Fly's
private network or an authenticated tunnel and verify reachability from the
collector. For a local check, `fly proxy 9091:9091 --app <app>` binds the proxy
to loopback. Do not add a public service for the exporter. The public API remains
on `6754`.
Fly's private network is scoped by organization/network configuration; it is not
a replacement for application authentication. [Fly custom metrics](https://fly.io/docs/monitoring/metrics/),
[private networking](https://fly.io/docs/networking/private-networking/).

If you use Fly's managed Prometheus-compatible query endpoint in Grafana, its Fly
access token authenticates Grafana to that service. It is separate from the MQLite
monitor token used for scraping. Follow Fly's documented token-type authorization
scheme. Fly's managed metrics service does not supply alert notifications; configure
your Grafana/Prometheus alerting path explicitly. The official metrics guide explains
these boundaries and its retention limits. [Fly metrics authentication and alerting](https://fly.io/docs/monitoring/metrics/).

Keep the broker's single-writer SQLite volume model. Ensure the monitored Machine
is running when a private collector scrapes it; do not assume a stopped Machine is
healthy or that every private request uses Fly Proxy auto-start. Monitoring cadence
and scale-to-zero policy need a deliberate choice for the actual routing setup.

## Azure: VM or AKS

A private VM collector can use the same TLS/Bearer job. For AKS with Azure managed
Prometheus, use its documented custom scrape mechanism and namespace-scoped Secret
access. Azure's managed ServiceMonitor/PodMonitor CRDs use a different API group
from upstream Prometheus Operator, so the supplied upstream manifest cannot be
copied unchanged. Check the collector's supported fields, RBAC and Kubernetes
version-specific requirements. [Azure custom scrape CRDs and authentication](https://learn.microsoft.com/en-us/azure/azure-monitor/containers/prometheus-metrics-scrape-crd).

For an existing upstream operator installation on AKS, use the upstream
[Kubernetes examples](../ops/observability/kubernetes/README.md). Do not run duplicate
scrape jobs unless their replica labels and downstream deduplication are designed;
otherwise dashboards may double-count process counters or duplicate database gauges.

## Production handoff checklist

| Concern | Required check |
| --- | --- |
| Credentials | Monitor token differs from admin; no body access, queue management or key issuance; secret never appears in labels |
| Networking | Collector can reach the selected private/TLS endpoint; public access remains restricted |
| Persistence | Broker database and monitoring history have suitable volumes, retention and backup policies |
| Rules | Thresholds fit application SLA; notification routing was actually tested |
| Freshness | Missing scrape, failed queue collection and unavailable backend measurements remain distinguishable from zero |
| Scale | Record scrape duration and series count; queue labels scale with configured queues, not message count |
| Host health | Reuse cloud/node/container monitoring for disk space, CPU, memory, OOM and volume health |
| Upgrades | Pin reviewed images; validate exporter protocol, dashboard queries and alerts after upgrade |

No Alibaba Cloud, Fly.io, Azure managed service or Kubernetes cluster is created by
the local demo. Provider setup above is reusable guidance, with environment-specific
validation steps. Local integration evidence must not be presented as a managed-cloud
deployment test.
