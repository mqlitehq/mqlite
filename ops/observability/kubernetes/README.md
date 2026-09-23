# Kubernetes integration

These templates connect an existing MQLite broker to an existing Prometheus
Operator installation. They do not install a cluster, monitoring operator or a
second broker. Set your namespace and confirm the Deployment/container names,
pod labels, operator selectors, private routing and certificate policy first.
The patch enables authenticated `/metrics` on the existing API port (`6754`).
The `mqlite-metrics` ClusterIP gives Prometheus a stable target for that port;
it does not isolate metrics from the other API paths.

Create the dedicated monitor token as a Secret from a private local file. Avoid
putting its value in YAML, command history, a dashboard or a ConfigMap:

```sh
kubectl -n YOUR_NAMESPACE create secret generic mqlite-monitor-token \
  --from-file=token=/secure/path/mqlite-monitor.token
kubectl -n YOUR_NAMESPACE patch deployment mqlite --type=strategic \
  --patch-file=broker-monitor-env.patch.yaml --dry-run=server
kubectl -n YOUR_NAMESPACE apply --dry-run=server -f service.yaml -f service-monitor.yaml
```

Review the server dry-run output, then apply the same commands without the dry-run
flag. Restart/rollout the broker when changing its configured monitor token. A
local SQLite broker must remain a single writer; preserve the deployment's safe
single-instance update strategy and persistent volume. Do not use these examples
to introduce overlapping writers during a rolling update.

The Secret belongs in the ServiceMonitor's namespace, and the operator needs
permission to read it. Configure namespace and ServiceMonitor label selectors in
your existing Prometheus resource. `jobLabel: app` sets the scrape job to
`mqlite`, matching the provided rules. Restrict broker pod access to approved
clients and collectors with your network policy. NetworkPolicy controls pod/port
access, not HTTP paths: both Services reach the same port. If the API has public
ingress, configure that ingress to deny `/metrics` from public traffic and verify
the denial. A separate ClusterIP Service alone does not make the route private;
otherwise keep metrics disabled or use a private-only broker.

Validate the target and a real query before importing the dashboard:

```promql
up{job="mqlite"}
mqlite_collection_success{job="mqlite"}
```

Apply the same rule groups through your platform's PrometheusRule or managed-rule
mechanism; do not maintain a second independent Grafana rule copy. Import
`../grafana/dashboards/mqlite.json` and select your existing datasource.

The Compose stack and API scenarios are executable locally. These Kubernetes
templates are optional integration examples; a successful local Compose test
does not validate your cluster's CRDs, RBAC, network policy or managed collector.
Use the server dry-run and target/query checks in your target environment.

Reference: [Prometheus Operator ServiceMonitor API](https://prometheus-operator.dev/docs/api-reference/api/#monitoring.coreos.com/v1.ServiceMonitor).
