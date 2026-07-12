# Deploying mqlite

mqlite is **one pure-Go binary** (or one container image) over **one SQLite file**
(or a remote Turso DB). Run it embedded in your process, or as a network broker —
this guide covers the broker (`mqlite serve`). Pick a target:

- [Docker / GHCR](#docker--ghcr) — quickest; the published multi-arch image.
- [Fly.io](#flyio-minimal-cost) — a minimal-cost, scale-to-zero recipe.
- [systemd](#systemd-bare-metal) — a single binary on a VM.
- [Turso](#turso-remote-libsql) — remote replicated storage instead of a local volume.

## Configuration (all targets)

Everything is read from the environment — the DB string is never compiled in.

| Env | Meaning |
|---|---|
| `MQLITE_DB` | `file:/data/mq.db` (local) or `libsql://<db>.turso.io` (remote) |
| `MQLITE_DB_AUTH_TOKEN` | auth token for a remote libSQL/Turso DSN |
| `MQLITE_TOKENS` | comma-separated Bearer tokens the broker accepts (**set this in production**) |
| `MQLITE_SYNC` | durability: `NORMAL` (default) / `FULL` / `OFF` / `EXTRA` (local file only); an unrecognized value is rejected at startup |
| `MQLITE_DLQ_MAX_AGE` · `MQLITE_DLQ_MAX_COUNT` · `MQLITE_DLQ_MAX_BYTES` | DLQ retention bounds (defaults 14d / 1,000,000 per queue; byte cap off; `MQLITE_DLQ_RETENTION=off` to disable) — see [retention.md](retention.md) |
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

The published image is multi-arch (amd64 + arm64):

```bash
docker run -d --name mqlite -p 6754:6754 \
  -v mqlite-data:/data \
  -e MQLITE_DB=file:/data/mq.db \
  -e MQLITE_TOKENS=mqk_prod_CHANGEME \
  ghcr.io/mqlitehq/mqlite:0.3.0
```

- The named volume `mqlite-data` persists the SQLite file across restarts.
- Pin a version tag (`:0.3.0`) in production; `:0.3` tracks patches, `:latest` the newest release. Images `>= 0.3.0` listen on `6754`.
- Verify: `curl http://localhost:6754/` (discovery card) and `/healthz`.

## Fly.io (minimal cost)

A scale-to-zero, single-machine recipe — cheapest way to run it online. Save as
`fly.toml`:

```toml
app            = "your-mqlite"
primary_region = "sin"            # pick a region near you

[build]
  image = "ghcr.io/mqlitehq/mqlite:0.3.0"   # use the public image; no build on Fly

[env]
  MQLITE_DB = "file:/data/mq.db"            # SQLite on the persistent volume

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
fly secrets set MQLITE_TOKENS=mqk_prod_CHANGEME  # broker auth (a Fly secret, not in fly.toml)
fly deploy --ha=false                            # single machine
curl https://your-mqlite.fly.dev/                # discovery card
```

**Cost:** with `auto_stop_machines="stop"` + `min_machines_running=0` the machine
runs only while serving requests (cold-starts in seconds, stops when idle), so the
steady-state cost is essentially the 1 GB volume. mqlite uses ~25–34 MB RSS for any
workload, so 256 MB has ~8× headroom — see [benchmark.md](benchmark.md). For sizing
and a full cost note, [benchmark.md](benchmark.md).

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
Environment=MQLITE_SYNC=NORMAL
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

## Backup & restore

The queue lives in one SQLite file — but a **live** broker also holds `-wal`/`-shm`
sidecars, so a bare `cp` of the main file mid-write can capture a torn state. Take a
consistent backup one of two ways:

- **Hot (broker running)** — one consistent snapshot, no downtime:
  ```bash
  sqlite3 /var/lib/mqlite/mq.db "VACUUM INTO '/backup/mq-$(date +%F).db'"
  ```
  (The file is standard SQLite, so the `sqlite3` CLI works on it even though mqlite
  itself is pure-Go.)
- **Cold (broker stopped)** — `systemctl stop mqlite`, then copy **all** of `mq.db`,
  `mq.db-wal`, `mq.db-shm` (or checkpoint first so the WAL folds into `mq.db` and you can
  copy just that one file), then start again.

Restore: stop the broker, put the backup at `MQLITE_DB`'s path, start it. Never restore
onto a running broker. On Turso, durability and backups are the server's responsibility.

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
- [ ] Persistent storage sized for the **peak** backlog — the file grows to peak, then a
      background janitor returns freed pages to the OS (`incremental_vacuum`) so it shrinks
      back gradually as the queue drains ([retention.md](retention.md)).
- [ ] DLQ retention left on (default) or tuned for your volume.
- [ ] Pinned image/binary version.
- [ ] Scrape `/metrics` (Prometheus) for queue depths — see [api-reference.md](api-reference.md).
