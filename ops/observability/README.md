# MQLite, Prometheus and Grafana

This isolated local stack builds the current MQLite checkout and runs three
services. It does not attach an existing broker database or modify cloud resources.
For production or an existing monitoring platform, use
[the observability guide](../../docs/observability.md) and
[cloud/Kubernetes integration](../../docs/observability-cloud.md).

## Start

Requirements: Linux or macOS, Git, Docker with Compose, Python 3, and available
loopback ports. All container builds and this example's runtime platform are
explicitly `linux/amd64`.
An ARM host uses Docker's emulation; this does not benchmark native ARM performance.

Use the matching release tag so the broker, dashboard and rules come from the same
version. Before that tag is published, use the exact reviewed candidate commit
for a release rehearsal instead.

```sh
git clone --branch v0.3.2 --depth 1 https://github.com/mqlitehq/mqlite.git
cd mqlite/ops/observability
python3 init.py
docker compose up --build -d
docker compose ps
```

Initialization writes a private `.env` pointer and generates three different random
credentials. Secrets and persistent data live **outside the source checkout**, by
default at `$XDG_STATE_HOME/mqlite-observability-032`, or
`~/.local/state/mqlite-observability-032` when XDG_STATE_HOME is unset. To select
another location, set `OBS_DATA_DIR` before the first initialization. Existing
credentials are preserved. Files are mode 0600 and directories 0700; services run
as your numeric UID/GID so file permissions need not be relaxed.

| Service | Local URL | Credentials |
| --- | --- | --- |
| MQLite console | http://127.0.0.1:17654/ui/ | Administrator token in `OBS_DATA_DIR/secrets/admin.token` |
| MQLite metrics | http://127.0.0.1:17654/metrics | Monitor token in `OBS_DATA_DIR/secrets/monitor.token`; loopback only |
| Prometheus | http://127.0.0.1:19190 | Loopback only; its broker monitor token is mounted privately |
| Grafana | http://127.0.0.1:13000/d/mqlite-observability | User `admin`; password in `OBS_DATA_DIR/secrets/grafana-admin.token` |

Open the indicated credential file locally when signing in. Tokens/passwords are
not printed by initialization or verification and are not stored in dashboards.
Prometheus receives only `secrets/monitor.token`, never the administrator token.
The broker reads the two distinct credentials at startup; Grafana uses its Docker
secret-file configuration. Compose file secrets protect access through the host's
filesystem permissions; they are not an encrypted secret manager.

Override `MQLITE_OBS_PORT`, `PROMETHEUS_OBS_PORT` or `GRAFANA_OBS_PORT` if a port
is already in use, exporting the same values when running the verifier. API and
metrics share container port `6754`; Prometheus scrapes `mqlite:6754` over the
private Compose network. Keep all published addresses bound to loopback. A tunnel
can expose these local ports to your own workstation without publishing management
interfaces to the Internet.

The example sets `MQLITE_METRICS=on`, so the open `GET /` card automatically
includes `"metrics":"/metrics"`. It resolves on the same host and port as the
API and requires a monitor or administrator credential. Disable metrics to remove
the discovery field and return `404` for scrapes.

## Exercise actual scenarios

From the repository root:

```sh
python3 test/observability/verify.py
python3 test/observability/verify.py --faults --output /tmp/mqlite-observability-proof.json
```

The driver uses real broker, Prometheus and Grafana APIs. It sends and checks a
message's identity and complete body, confirms idempotent settlement, creates
backlog, abandonment/redelivery, deferred/scheduled work and dead letters. It
waits for real TTL discard/dead-letter transitions, retention deletion, invalid
filter configuration, routing evaluation failures, and an alert to fire and
resolve. It verifies read-only monitor isolation,
expired/revoked/invalid credentials, native/Prometheus agreement, control-character
label ingestion, datasource provisioning and every dashboard query. Deliberately
invalid expressions and a missing datasource verify that the checker rejects
broken configuration instead of accepting an HTTP success envelope.

`--faults` temporarily gives the scraper an invalid token, stops Prometheus, and
restarts the demo broker. It checks scrape and datasource failure/recovery,
production alert firing/resolution, persistence and process-counter reset. These
actions target this Compose project only and restore the token/service in `finally`
blocks. Do not run the driver against an existing production deployment.

The test leaves named `obsdemo-*` queues for inspection: active backlog,
scheduled/deferred work and an intentional dead letter. A repeat run resets only
those queues via public operations. If an interrupted prior test still holds a
live lease, wait for its expiry before retrying.

### Keep the dashboard moving

Start live traffic directly, without repeating verification or interrupting any
monitoring service. This example requests one hour:

```sh
python3 test/observability/verify.py --traffic-only --traffic-seconds 3600
```

Open [the live queue view](http://127.0.0.1:13000/d/mqlite-observability?var-queue=obsdemo-live&from=now-5m&to=now&refresh=5s).
It selects only `obsdemo-live`, the last five minutes and a five-second refresh,
so the verifier's intentionally retained backlog/dead letters do not dominate the
view. The general dashboard keeps its normal All-queues, 15-minute default.
Watch the status cards and the retained-depth, enqueue/delivery, settlement, and
dead-letter/removal trends. Allow two rounds to collect visible history.

The driver alternates 24- and 48-message waves. It publishes 3 or 6 messages per
second, holds the backlog for ten seconds, then drains at 6 or 12 messages per
second. Each round also abandons and verifies three real redeliveries, rejects
three messages, leaves them in the DLQ for ten seconds, and redrives/completes them.
Delivered sequence numbers and full bodies must match the acknowledged sends;
each round checks that the queue is empty after settlement. These are real API
operations, not generated Prometheus samples.

The timed generation interval starts after startup ownership/queue checks.
`--traffic-seconds` limits new production. At the deadline, or after Ctrl-C, the
driver stops adding work and drains its current round; finishing bounded HTTP
requests can extend elapsed time past the requested duration. It stops only the
traffic process and leaves all three services running. `--output /tmp/live.json`
optionally writes the actual duration, stop reason and counters after traffic has
finished. Omitting `--traffic-only` runs verification first, followed by the
requested traffic; its final report includes both phases. An output file is marked
`RUNNING` during execution and `FAIL` on error, so failed traffic cannot leave a
successful verification report for an incomplete combined run.

Only this reserved demo queue is changed. A process lock prevents two drivers
from resetting each other's work; a pre-existing live lease or unexpected stored
state makes startup fail before cleanup. Restart cleanup is limited to 96 old
active/dead-lettered messages. Each running round retains at most 48 messages and
finishes empty. A two-minute TTL and bounded DLQ retention cover leftovers after
an unexpected failure; known live leases are abandoned on failure. Do not use
`obsdemo-live` for application traffic or run other consumers against it.

The short `MQLiteDemoDeadLetter` rule is demonstration-only. Normal production
thresholds remain in `prometheus/rules.yml` and should be tuned to application SLA.
The stack evaluates alert conditions; it does not send email, Slack or paging
notifications. Configure an existing Alertmanager/managed receiver and test its
delivery separately.

## Inspect and stop

Prometheus `/targets` shows the actual scrape error; `/alerts` shows rule state.
Grafana provides instance and queue selectors, backlog/flow/error panels, storage
and maintenance measurements. No traffic, failed collection and missing data have
different meanings; the dashboard does not convert missing samples into green zero.

The **Firing alerts** table lists each alert's severity, name, instance, queue and
state, with task/stage labels when present. Warnings are orange, critical alerts
red and informational alerts blue. Selecting a queue keeps its alerts and
broker-wide alerts without a queue label. Open **Alert runbooks** from the panel
for investigation steps. An empty result is not proof of health: check the scrape
and collection status cards; datasource failures remain visible errors.

```sh
docker compose logs --tail 100 mqlite prometheus grafana
docker compose stop
docker compose start
```

`stop` preserves all three data directories. `down` removes containers/network and
also preserves the external bind-mounted data. Removing `OBS_DATA_DIR` deletes the
database, monitoring history and credentials; do so only when deliberately resetting
this demo, after stopping its services. Grafana reads its initial administrator
password when its database is created; changing the secret file later is not a
Grafana password rotation procedure.

Prometheus retention is bounded to seven days or 1 GB, whichever limit applies.
This does not bound MQLite backlog or Grafana data; monitor free disk separately.
Use the broker's normal retention/backup procedures before treating a deployment
as production.

## Reuse the configuration

- Existing Prometheus: copy the authenticated job from
  `prometheus/existing-broker.example.yml`, enable `MQLITE_METRICS=on`, point it
  at the broker's private API address (`6754` without a TLS proxy), and mount its
  monitor token. Add the operating rules; omit `demo-rules.yml`. Keep `/metrics`
  off public ingress with path filtering or a private-only broker.
- Existing Grafana: import `grafana/dashboards/mqlite.json` and select the existing
  Prometheus datasource. The local provisioning files use UID `mqlite-prometheus`.
- Kubernetes: adapt [the Service/ServiceMonitor and broker environment patch](kubernetes/README.md)
  to the installed operator, namespace, Secret and selectors; first use server dry-run.
- Managed clouds: follow [the reusable cloud guide](../../docs/observability-cloud.md)
  for Alibaba Cloud, Fly.io and Azure. A locally passing stack is not evidence that
  provider-specific RBAC, authentication or networking has been tested.

## Validate and update pins

```sh
cd ../..
python3 test/observability/check_config.py
```

After initialization, this uses the pinned `promtool` to validate Compose,
Prometheus configuration, operating/demo rules and the entire exposition fixture.
It accepts exactly the two legacy-name warnings documented in the compatibility
contract, and verifies that malformed exposition is rejected. Run the real-stack
driver as well: parsing configuration alone cannot prove credentials, networking,
scrapes, alert transitions or Grafana queries work.

The source broker is built with the repository Dockerfile, including its compiled
binary vulnerability scan. The monitoring images are pinned by version and registry
manifest digest, verified on 2026-09-21:

| Component | Version | Manifest index SHA-256 |
| --- | --- | --- |
| Prometheus | 3.13.3 LTS | `6976aa8a60fec930796ce5772b8d12da7a318a5daa8d40d69c5c7819a05eeed7` |
| Grafana OSS | 13.2.2 | `ac461fb352abc50da10a51c7d02462e9c05488f11f53f14b3ad79a8145f638a0` |

Review [official Prometheus releases](https://prometheus.io/download/) and
[official Grafana releases](https://github.com/grafana/grafana/releases), resolve
the new image with `docker buildx imagetools inspect`, then repeat configuration
and full real-stack verification before updating both pins. These pins identify
the tested software; they do not promise future vulnerability-free operation.
Provisioning and password-file behavior follow
[Grafana provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/)
and [Docker configuration](https://grafana.com/docs/grafana/latest/setup-grafana/configure-docker/).
