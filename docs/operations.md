# Production operations

This runbook describes the unreleased v0.3.0 source on `main` (port 6754,
schema token 5). The latest published release is v0.2.0 (port 8080, schema token 2).
See [deployment](deployment.md) for installation and [observability](observability.md)
for metric definitions and alerts.

## Operating boundaries

- Run one active broker per database. A local file has one MQLite owner and one
  SQL connection; use HTTP clients for other processes. A second broker sharing a
  Turso database is also unsupported: startup recovery assumes the previous owner
  has stopped. Do not perform a rolling overlap against the same database.
- Keep local storage on a persistent local filesystem or attached block volume,
  with working flush semantics. SQLite WAL is unsuitable for a network filesystem.
  Size for the peak backlog, WAL, temporary operations, backups and log retention.
- Use `MQLITE_SYNC=FULL` for production when an acknowledged commit must survive an
  OS or power failure. The default `NORMAL` protects against process crashes but
  can lose recent commits after power loss. `FULL` still depends on the storage
  stack honoring flushes; SIGKILL tests do not simulate a power failure.
- Set stable `MQLITE_TOKENS`, terminate HTTPS, and keep the broker behind the proxy
  or a private network. Every token grants the same API access; there are no
  per-queue roles. Keep credentials in a restricted environment/secret file.
- Consumers must be idempotent. Restarts, lost responses and restoring an older
  snapshot can repeat work whose external side effects already happened. Reuse
  stable `MessageID`/body, receive `AttemptID`, and the exact settlement request
  when retrying within their documented retention windows; do not treat an HTTP
  timeout as proof that a write failed.

Choose and record the application's recovery objectives before launch: acceptable
snapshot age (RPO), recovery time (RTO), backlog age, and capacity limits. Measure
restore time with representative data. A successful backup command alone is not a
restore test.

## Startup and routine checks

Pin the binary checksum or image digest and record its source commit, Go version,
MQLite version and schema token. Use the same configuration when rehearsing and
running the service, especially durability, message limits and DLQ retention.

`GET /healthz` is a process liveness check; its handler does not query storage.
Use authenticated `AdminService/Status` to inspect the backend (`ping_ms >= 0`),
then a dedicated canary queue to verify a complete write/consume/settle cycle.
A successful read does not prove that disk space remains for a write.

```bash
export MQLITE_ENDPOINT=http://127.0.0.1:6754
# Set MQLITE_TOKEN from your secret manager; do not paste it into shell history.
mqlite status --output json
mqlite create-queue ops-canary
mqlite send ops-canary "canary-$(date -u +%s)"
mqlite receive ops-canary
mqlite metrics ops-canary --output json
```

The CLI `receive` completes messages after printing them unless `--no-ack` is set.
The final canary queue total must be zero. Run one canary consumer at a time and
keep it separate from business traffic. A periodic canary should check the returned
body against its own sent identity and fail on an unexpected or missing message.

Monitor process/container restarts, memory, free disk, DB plus WAL size, recent
backup age, RPC errors and queue age/depth. See [observability](observability.md)
for the difference between backlog gauges and the completion counter. Review DLQ
messages before retention removes them; retention is a bounded failure buffer,
not an archive. Leave space for a new backup and WAL growth during a long read.

For token rotation, configure `MQLITE_TOKENS=old,new`, restart the single broker,
move clients and scrapers to the new token, then remove the old token and restart.
Token changes take effect at startup. Quiesce clients during each replacement and
verify the canary before resuming normal traffic.

## Consistent backups

These procedures are for a local SQLite file. Turso backups and point-in-time
recovery belong to the remote service; rehearse that provider's restore process
and keep one active MQLite owner during the switchover.

Protect backups like the live database: they include message bodies, properties
and any business tables used by the embedded outbox. Keep a verified copy outside
the database's failure domain, together with the matching binary/image identity
and configuration needed to restore it. Store credentials separately.

### Online snapshot

Use a read-only connection to the live source and a new destination filename:

```bash
umask 077
MQ_DB=/var/lib/mqlite/mq.db
MQ_BACKUP_DIR=/var/backups/mqlite
mkdir -p "$MQ_BACKUP_DIR"
MQ_SNAPSHOT="$MQ_BACKUP_DIR/mq-$(date -u +%Y%m%dT%H%M%SZ).db"
sqlite3 -readonly "$MQ_DB" '.timeout 10000' "VACUUM INTO '$MQ_SNAPSHOT'"
test "$(sqlite3 -readonly "$MQ_SNAPSHOT" 'PRAGMA integrity_check')" = ok
test -z "$(sqlite3 -readonly "$MQ_SNAPSHOT" 'PRAGMA foreign_key_check')"
sqlite3 -readonly "$MQ_SNAPSHOT" "SELECT value FROM meta WHERE key='schema_version'"
sha256sum "$MQ_SNAPSHOT" > "$MQ_SNAPSHOT.sha256"
```

Run the commands with error checking (`set -e` in a script). A failed or interrupted
snapshot is not a backup; preserve the error and choose a new filename for the next
attempt. Verify the checksum after copying the snapshot to backup storage.

`VACUUM INTO` makes a transactionally consistent snapshot, including business and
auxiliary tables. Do not copy only the main file while the broker is running, and
do not write or run an external checkpoint against the live source. Keep the
backup connection read-only and short-lived. SQLite's documented
[WAL-reset issue](https://www.sqlite.org/wal.html#the_wal_reset_bug) requires multiple
connections concurrently writing/checkpointing; MQLite's single connection and
this backup restriction avoid that topology. See [dependency policy](dependencies.md).

### Offline directory copy

Stop producers and consumers, then stop the broker and confirm it has exited.
Copy the complete data directory while no process can change it; include the main
DB and any remaining `-wal`/`-shm` files. A clean shutdown normally checkpoints and
removes these files, but their absence must not be assumed after a crash.
Keep the matching set together. Do not delete a WAL file to make a backup look
self-contained. Restart the original service only after the directory copy finishes.

For the systemd recipe, `systemctl stop mqlite` must finish before copying
`/var/lib/mqlite/`. For Docker, stop the container and copy its named volume using
a helper with a read-only source mount. A volume snapshot taken while the broker
is still writing needs its own documented consistency guarantees.

## Restore into a fresh directory

1. Keep the original data and backup unchanged. Verify the backup checksum and
   run `integrity_check` and `foreign_key_check` on the saved snapshot or a working
   copy. Record its schema token and the binary version that created it.
2. Create an empty restore directory or volume. An online `VACUUM INTO` snapshot is
   a standalone DB; copy that file into the empty directory. For an offline copy,
   restore the complete matching directory. Never place a snapshot beside WAL/SHM
   files from another database lifetime.
3. Give the broker account read/write access to that directory. Start the matching
   binary against it on an isolated endpoint, with authentication and `FULL`, while
   business clients remain stopped. Do not attach the original database as well.
4. Verify configuration, subscriptions, message identities and body hashes against
   recorded expectations. On startup every orphaned lock is reclaimed: below the
   queue's delivery limit it becomes active; at the limit it goes to DLQ with
   `MaxDeliveryCountExceeded`. Delivery count and payload survive, and the old lock
   token is invalid. Old receive-attempt snapshots may replay old handles; that
   does not give those handles ownership of a recovered message.
5. Check deferred/scheduled/DLQ work and topic filters, then run the authenticated
   canary. A restore replays the snapshot's state: later completions, sends, dedup
   records and external business effects require reconciliation against the
   application's own records. TTL and retention clocks continue to matter.
6. Stop the isolated broker, point the service at the restored directory and start
   its single owner. Resume clients only after validation. Record actual recovery
   time and recovered snapshot age; retain the original for investigation.

The repeatable [restore drill](../test/production/restore/README.md) verifies all
schema tables and columns, complete payloads, auxiliary records, outbox data and
the expected recovery transformations. Run it for a candidate and rehearse the
same procedure on representative operational data before relying on a backup.

## Upgrade and rollback

Check schema tokens before replacing a binary. Version numbers alone do not
establish database compatibility, and MQLite does not run schema migrations.

| Situation | Procedure |
|---|---|
| Same schema token | Quiesce clients, stop the old owner, take a consistent backup, start the candidate against a restored copy, validate, then replace the owner. Keep the old binary and snapshot for rollback. |
| Different schema token | Keep the old database with its old binary. Account for all retained work, then create a new database using the candidate. Do not edit the schema token or point the candidate at the old file as a migration. |
| Rollback before candidate accepts business writes | Stop the candidate and restore the matched old binary plus old snapshot/configuration; verify before resuming clients. |
| Rollback after candidate accepts business writes | First preserve candidate data and reconcile new messages and external effects. Blindly restoring the old snapshot would discard those new writes. |

For **v0.2.0 → v0.3.0**, schema **2 → 5** and default port **8080 → 6754** both
change. Before the cutover, stop producers and account for **every queue and
subscription in every state**, including future scheduled work, deferred work and
DLQ messages. Drain retained messages with the old version, or make an explicit
application-level replay plan with stable business identities. A queue that has
no active messages may still contain scheduled, locked, deferred or DLQ work.

Create the candidate database in a new volume/directory; update the port, reverse
proxy, clients and probes together. Preserve the old binary, configuration and
snapshot. The new binary must reject a real schema-2 file and the old binary must
reject a schema-5 file without changing its logical contents; the restore drill
checks both directions. There is no general message export/import migration tool
in this release, so do not improvise a direct SQL table copy between schemas.

## Incident actions

| Symptom | First actions | Verify before resuming |
|---|---|---|
| Full disk / `SQLITE_FULL` | Stop ingress, preserve logs and acknowledged message identities. Expand storage or remove unrelated expired logs/backups; never remove the live DB or WAL. A full `VACUUM` needs working space and is unsuitable as an emergency first step. | Backend status, a fresh canary, successful replay/reconciliation of ambiguous requests, and retained-message integrity. |
| Repeated broker crashes | Stop restart loops long enough to preserve the complete DB directory and logs. Check disk, memory/OOM and configuration. Restart only one owner. | Recovery of orphaned locks, DLQ reason/count, canary and consumer idempotency. |
| Old backlog / growing DLQ | Check consumers, lock expiry/renewal and handler failures. Inspect reasons and payloads; fix the cause before bounded redrive. | Backlog age falls and verified redriven identities complete. |
| Storage/remote timeouts | Check backend reachability and latency. Treat lost write responses as ambiguous; keep retries within the idempotency contract. | Authenticated status plus a write/consume canary. |
| `ErrSchemaVersionMismatch` | Stop the attempted cutover. Recover the matching binary/database pair. | Schema token and the correct old or new port; no token restamping. |

[SQLite backup semantics](https://www.sqlite.org/lang_vacuum.html) and
[WAL file handling](https://www.sqlite.org/wal.html#the_wal_file) describe the storage
rules behind these procedures.
