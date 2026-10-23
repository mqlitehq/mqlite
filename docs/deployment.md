# Deploying mqlite

mqlite is **one pure-Go binary** (or one container image) over **one SQLite file**
(or a remote Turso DB). Run it embedded in your process, or as a network broker —
this guide covers the broker (`mqlite serve`). Pick a target:

- [Docker / GHCR](#docker--ghcr) — quickest; the published multi-arch image.
- [Fly.io](#flyio-minimal-cost) — a minimal-cost, scale-to-zero recipe.
- [systemd](#systemd-bare-metal) — a single binary on a VM.
- [Turso](#turso-remote-libsql) — remote replicated storage instead of a local volume.
- [Production operations](operations.md) — backups, restoration, upgrades and incident actions.

## Configuration (all targets)

Everything is read from the environment — the DB string is never compiled in.

| Env | Meaning |
|---|---|
| `MQLITE_DB` | `file:/data/mq.db` (local) or `libsql://<db>.turso.io` (remote) |
| `MQLITE_DB_AUTH_TOKEN` | auth token for a remote libSQL/Turso DSN |
| `MQLITE_TOKENS` | comma-separated administrator Bearer tokens (**set this in production**) |
| `MQLITE_MONITOR_TOKENS` | v0.3.2+: optional comma-separated credentials for `Observe` and `/metrics` only; requires auth and distinct administrator credentials |
| `MQLITE_METRICS` | v0.3.2+: opt-in authenticated `/metrics` on the API port; `on`/`true`/`1` enables, `off`/`false`/`0` disables (default); requires administrator auth |
| `MQLITE_SYNC` | durability: `NORMAL` (default) / `FULL` / `OFF` / `EXTRA` (local file only); an unrecognized value is rejected at startup |
| `MQLITE_MAX_SUBSCRIPTIONS` | v0.4.0+: active plus deleting subscription capacity (default 10,000); pending cleanup continues to occupy a slot |
| `MQLITE_DLQ_MAX_AGE` · `MQLITE_DLQ_MAX_COUNT` · `MQLITE_DLQ_MAX_BYTES` | asynchronous DLQ retention targets (defaults 14d / 1,000,000 per queue; byte target off; `MQLITE_DLQ_RETENTION=off` disables broker-wide defaults) — see [retention.md](retention.md) |
| `MQLITE_MAX_MESSAGE_BYTES` | reject larger bodies (default 1 MiB) |
| `MQLITE_UI` | serve the embedded admin console at `/ui` (default on; `off` runs headless) |

> **Auth (secure by default):** if `MQLITE_TOKENS` is **unset**, `serve` **generates a
> random `mqk_…` token and prints it at startup** — the broker is never silently open.
> Set `MQLITE_TOKENS` to your own token(s) for a stable value (rotate by updating it),
> or `MQLITE_TOKENS=off` to explicitly disable auth — but then the broker **refuses a
> non-loopback bind**: bind `127.0.0.1` explicitly, or pass `--insecure-allow-remote` to
> expose it (and `MQLITE_CORS` defaults to off while auth is off). The `/`
> discovery and `/healthz` endpoints stay open, as does the static `/ui` console when
> enabled (its API calls still carry a token); everything else needs
> `Authorization: Bearer <token>`.

> **Admin console:** the broker bakes a static web console into the binary and serves
> it at `/ui` (e.g. `http://localhost:6754/ui`) — no separate process or Node runtime.
> Paste your broker URL + token in the console to drive every queue operation. Set
> `MQLITE_UI=off` to disable it for headless deployments.

The broker listens on `:6754` by default (`mqlite serve --addr :6754`). Full endpoint
and error reference: [api-reference.md](api-reference.md).

## Docker / GHCR

> **Version and upgrade compatibility.**
> The latest supported release is **v0.4.1**, with default port **6754** and
> schema token **7**. Pin the exact v0.4.1 binary or image rather than relying on
> a moving image alias. v0.3.3 is withdrawn, unsupported, and retracted by
> v0.4.0 and later; its public assets and exact image remain available for
> forensic or compatibility inspection and must not be deployed.
>
> v0.4.x cannot open a v0.3.x schema-5 database and no in-place migration exists.
> Schema 6 was used only by intermediate untagged lifecycle builds and is also
> refused. Preserve the matching schema-5 binary/database pair, take a consistent
> backup, account for retained work, and start v0.4.1 with a fresh database.
> v0.4.0 also removes the deprecated Prometheus aliases described in
> [observability](observability.md#compatibility-and-v040-migration).
> **v0.2.0 uses port 8080 and schema token 2.** Follow the
> [upgrade and rollback procedure](operations.md#upgrade-and-rollback) before
> replacing it.

v0.4.0 → v0.4.1 is a same-schema replacement. It fixes console, CLI and Go SDK
observation validation after deleting a resource with process-lifetime message
effects; it does not require a database rebuild.

The exact supported image is multi-arch (amd64 + arm64):

```bash
# v0.4.1, default port 6754
docker run -d --name mqlite -p 6754:6754 \
  -v mqlite-data:/data \
  -e MQLITE_DB=file:/data/mq.db \
  -e MQLITE_TOKENS=mqk_prod_CHANGEME \
  -e MQLITE_MONITOR_TOKENS=mqk_monitor_CHANGEME \
  -e MQLITE_SYNC=FULL \
  ghcr.io/mqlitehq/mqlite:0.4.1
```

- The named volume `mqlite-data` persists the SQLite file across restarts.
- Pin exact `0.4.1` in production. Images `>= 0.3.0` listen on `6754`;
  **`0.2.x` and earlier listen on `8080`**.
- Verify: `curl http://localhost:6754/` (discovery card) and `/healthz`. Metrics is
  disabled by default. For a private broker or an ingress that blocks public
  `/metrics`, add `-e MQLITE_METRICS=on` and give the collector a monitor token.
  Scraping uses the same API port; enabled discovery includes `"metrics":"/metrics"`.

## Fly.io (minimal cost)

A scale-to-zero, single-machine recipe — cheapest way to run it online. Save as
`fly.toml`:

```toml
app            = "your-mqlite"
primary_region = "sin"            # pick a region near you

[build]
  image = "ghcr.io/mqlitehq/mqlite:0.4.1"   # latest supported exact image

[env]
  MQLITE_DB = "file:/data/mq.db"            # SQLite on the persistent volume
  MQLITE_SYNC = "FULL"                      # sync every acknowledged local commit

[[mounts]]
  source      = "data"                       # the volume created below
  destination = "/data"

[http_service]
  internal_port        = 6754
  force_https          = true
  auto_stop_machines   = "stop"             # fully stop when idle (cheapest)
  auto_start_machines  = true               # start on the next request
  min_machines_running = 0                  # scale to zero

[[vm]]
  size   = "shared-cpu-1x"                  # lowest shared CPU
  memory = "256mb"                          # lowest memory
```

```bash
fly apps create your-mqlite
fly volume create data --size 1 --region sin     # 1 GB SQLite volume (region-bound)
fly secrets set MQLITE_TOKENS=mqk_prod_CHANGEME \
                MQLITE_MONITOR_TOKENS=mqk_monitor_CHANGEME # broker secrets, not in fly.toml
fly deploy --ha=false                            # single machine
curl https://your-mqlite.fly.dev/                # discovery card
```

The public recipe leaves metrics disabled. Enabling `MQLITE_METRICS=on` also
makes `/metrics` reachable through this public HTTP service; authentication
protects its contents but does not make the route private.

For private monitoring without another Fly Machine, remove `[http_service]` and
any public `[[services]]` entries, set `MQLITE_ADDR=fly-local-6pn:6754` and
`MQLITE_METRICS=on`, and connect clients and the collector through Fly's private
network. From your workstation, `fly proxy 6754:6754 --app your-mqlite` opens a
loopback tunnel; Prometheus can scrape `http://127.0.0.1:6754/metrics` using the
monitor token. Start the Machine before connecting: direct private access does
not provide the public recipe's autostart behavior.

If the API must stay public, enable metrics only when your existing ingress
actually blocks public `/metrics` while preserving private collector access;
otherwise leave it disabled. See [cloud and Kubernetes monitoring](observability-cloud.md#flyio)
and Fly's [private networking](https://fly.io/docs/networking/private-networking/)
and [proxy](https://fly.io/docs/flyctl/proxy/) references.

**Cost:** with `auto_stop_machines="stop"` + `min_machines_running=0` the machine
runs only while serving requests (cold-starts in seconds, stops when idle), so the
idle storage charges remain while the machine is stopped. The historical workloads
in [benchmark.md](benchmark.md) measured roughly 25–34 MB RSS; these are not memory
limits for other workloads or versions. Size the current broker using your message
sizes, concurrency, queue count and monitoring load, and measure peak memory under
that traffic. Continuous requests, including monitoring, can keep the machine active.

## systemd (bare metal)

Build or download the binary, then run it as a service:

```bash
go build -o /usr/local/bin/mqlite ./cmd/mqlite   # or grab a release binary
```

`/etc/systemd/system/mqlite.service`:

```ini
[Unit]
Description=mqlite broker
After=network.target

[Service]
# Bind loopback: the reverse proxy below is the only public entry point, so the broker
# is not reachable on the LAN even without a firewall rule. Use --addr :6754 only if you
# deliberately want it on all interfaces (and then add a firewall rule).
ExecStart=/usr/local/bin/mqlite serve --addr 127.0.0.1:6754
Environment=MQLITE_DB=file:/var/lib/mqlite/mq.db
Environment=MQLITE_TOKENS=mqk_prod_CHANGEME
Environment=MQLITE_SYNC=FULL
Restart=on-failure
DynamicUser=yes
StateDirectory=mqlite          # creates/owns /var/lib/mqlite

[Install]
WantedBy=multi-user.target
```

```bash
systemctl daemon-reload && systemctl enable --now mqlite
curl http://localhost:6754/healthz
```

Put a TLS-terminating reverse proxy (Caddy/nginx) in front for anything public; it
connects to the broker on `127.0.0.1:6754`, which is the only interface the broker binds
above — so the proxy (with TLS + whatever access control you add) is the single entry
point, not a bypassable layer over an all-interfaces socket.

Runtime-managed keys in v0.3.1 and later can be created and revoked without a
restart. Use a configured administrator or managed `manage` key for console and
key administration; use `send`/`listen` for applications. In v0.3.2 and later, use
distinct configured `MQLITE_MONITOR_TOKENS` for a read-only metrics scraper or
console monitoring views. These credentials are changed through configuration
and a broker restart.
See [key commands](cli.md#key-createlistrevoke--manage-persistent-access-keys) and
[key rotation](operations.md#key-rotation).

## Backup, restore and upgrades

Follow the [production runbook](operations.md#consistent-backups) for read-only
online snapshots, offline directory copies, isolated restore validation and rollback.
Use a fresh restore directory so an old WAL/SHM cannot attach to the snapshot.

**v0.2.0 databases use schema 2; v0.3.0/v0.3.1/v0.3.2 and withdrawn
v0.3.3 use schema 5; v0.4.0 and v0.4.1 use schema 7.**
Intermediate untagged lifecycle builds used schema 6. There is no in-place
migration between these tokens. Before testing a different token, account for
retained work in every state, keep the old binary/database pair and a consistent
backup, and create the v0.4.x database separately. Do not restamp an old file.
Within the tagged schema-5 line, v0.3.0 → v0.3.1 adds compatible managed-key
storage and v0.3.1 → v0.3.2 leaves the schema unchanged. `Observe` and configured
monitor credentials require v0.3.2; restore compatible monitoring configuration
when rolling back. Follow the runbook before a v0.4.x fresh-database cutover
and metric-name migration.
See [upgrade and rollback](operations.md#upgrade-and-rollback).

## Turso (remote libSQL)

Use replicated remote storage instead of a local volume — no disk to manage, and the
same engine. Point `MQLITE_DB` at the libSQL URL and pass the token:

```bash
MQLITE_DB=libsql://<db>.turso.io \
MQLITE_DB_AUTH_TOKEN=<jwt> \
MQLITE_TOKENS=mqk_prod_CHANGEME \
  mqlite serve
```

The same env works in Docker (`-e MQLITE_DB=... -e MQLITE_DB_AUTH_TOKEN=...`) or Fly
(`fly secrets set MQLITE_DB_AUTH_TOKEN=...`); drop the `[[mounts]]` volume since data
lives in Turso. Durability is the Turso server's responsibility — `MQLITE_SYNC` is
ignored for remote DSNs. See [turso.md](turso.md).

Run exactly one active MQLite broker or embedded `Engine` per Turso database. To
replace it, stop the old owner, confirm its process has exited, and only then start
the replacement. Rolling overlap against the same database is unsupported. The
remote connection pool is safe only within one MQLite process; it does not provide
cross-process ownership.

## Verify any deploy

```bash
curl https://<host>/                       # discovery: {name, version, status, ...} (open)
curl https://<host>/healthz                # ok (open)
# authed round-trip:
T=mqk_prod_CHANGEME
curl -H "Authorization: Bearer $T" -H 'Content-Type: application/json' \
  --data '{"name":"orders","config":{}}' \
  https://<host>/mqlite.v1.AdminService/CreateQueue
```

## Production checklist

- [ ] `MQLITE_TOKENS` set (auth on); rotate by updating it.
- [ ] HTTPS in front (Fly `force_https`, or a reverse proxy for systemd/Docker).
- [ ] Persistent storage sized for the **peak** backlog — deleted pages are reusable
      immediately, while local DB/WAL files shrink gradually. Every 60 seconds the
      broker reclaims at most 256 pages once at least 256 are free
      ([retention.md](retention.md)).
- [ ] DLQ retention left on (default) or tuned for your volume.
- [ ] Pinned image/binary version and recorded checksum/digest.
- [ ] `MQLITE_SYNC=FULL` for acknowledged local commits that must survive power loss.
- [ ] One active broker per database; no overlapping replacement.
- [ ] Verified backup and rehearsed restore meet the application's RPO/RTO.
- [ ] Free-disk, restart/error and queue-age alerts; an authenticated write/consume canary.
- [ ] Enable `MQLITE_METRICS=on` for authenticated Prometheus scraping on the API port; use private routing or public-ingress path filtering to keep `/metrics` private — see [observability.md](observability.md).
