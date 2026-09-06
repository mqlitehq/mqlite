#!/usr/bin/env python3
"""Run backup/restore and real schema rollback drills in a new evidence directory."""

import argparse
import base64
import copy
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import re
import select
import shutil
import sqlite3
import subprocess
import time


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def save(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")
    temporary.replace(path)


def quote(name):
    return '"' + name.replace('"', '""') + '"'


def cell(value):
    # Preserve SQLite storage types too, including INTEGER versus REAL.
    if isinstance(value, bytes):
        return ["blob", value.hex()]
    if value is None:
        return ["null", None]
    return [{int: "integer", float: "real", str: "text"}[type(value)], value]


def readonly_uri(db, stopped=False):
    uri = db.resolve().as_uri() + "?mode=ro"
    wal = Path(str(db) + "-wal")
    # A stopped, checkpointed WAL database can lack WAL/SHM sidecars. Native
    # SQLite may refuse a plain read-only open; immutable is safe only here.
    if stopped and (not wal.exists() or wal.stat().st_size == 0):
        uri += "&immutable=1"
    return uri


def snapshot(db, stopped=False):
    with closing(sqlite3.connect(readonly_uri(db, stopped), uri=True)) as conn:
        conn.execute("BEGIN")
        require(conn.execute("PRAGMA integrity_check").fetchall() == [("ok",)], "integrity_check failed: " + str(db))
        require(not conn.execute("PRAGMA foreign_key_check").fetchall(), "foreign_key_check failed: " + str(db))
        schema = conn.execute("SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name").fetchall()
        tables = {}
        for kind, name, _, _ in schema:
            if kind != "table":
                continue
            columns = conn.execute("PRAGMA table_xinfo(" + quote(name) + ")").fetchall()
            names = [column[1] for column in columns]
            rows = [[cell(v) for v in row] for row in conn.execute(
                "SELECT " + ",".join(map(quote, names)) + " FROM " + quote(name))]
            tables[name] = {"columns": columns, "rows": sorted(rows, key=canonical)}
        # JSON round-trip gives identical list representation for later comparisons.
        return json.loads(canonical({"schema": schema, "tables": tables}))


def rows(snap, table):
    data = snap["tables"][table]
    names = [column[1] for column in data["columns"]]
    return [{name: (bytes.fromhex(v[1]) if v[0] == "blob" else v[1])
             for name, v in zip(names, row)} for row in data["rows"]]


def compare(actual, expected, label):
    require(canonical(actual) == canonical(expected), label + ": logical snapshot differs (see saved manifests)")


def check_seed(snap, fixture):
    required = {"queues", "subscriptions", "messages", "dedup", "settlement_receipts",
                "receive_attempts", "meta", "sqlite_sequence", "business_orders"}
    require(set(snap["tables"]) == required, "fixture table surface changed; review complete oracle")
    for table in required:
        require(bool(rows(snap, table)), "fixture did not populate " + table)
    expected = []
    for item in fixture["entries"]:
        m = item["message"]
        expected.append({
            "id": item["sequence"], "queue": item["queue"], "state": item["state"],
            "visible_at": item["visible_at"], "locked_until": item["locked_until"],
            "lock_token": item["token"] or None, "delivery_count": item["deliveries"],
            "enqueued_at": fixture["now"], "expires_at": fixture["now"] + m["TTL"] // 1000000 if m["TTL"] else 0,
            "message_id": m["MessageID"], "correlation_id": m["CorrelationID"],
            "reply_to": m["ReplyTo"], "group_id": m["GroupID"], "content_type": m["ContentType"],
            "subject": m["Subject"], "properties": json.dumps(m["Properties"], sort_keys=True, separators=(",", ":"), ensure_ascii=False),
            "body": base64.b64decode(m["Body"]), "dead_letter_reason": item["reason"] or None,
            "dead_letter_description": item["description"] or None,
        })
    # Compare typed full rows against the independently generated send manifest.
    to_cells = lambda data: sorted([{key: cell(value) for key, value in row.items()} for row in data], key=canonical)
    compare(to_cells(rows(snap, "messages")), to_cells(expected), "seed identities/content/all message columns")
    outbox = next(item for item in fixture["entries"] if item["queue"] == "outbox")
    compare(to_cells(rows(snap, "business_orders")), to_cells([{
        "id": fixture["business"], "amount": 12345, "payload": base64.b64decode(outbox["message"]["Body"])
    }]), "committed business outbox and rolled-back absence")
    require({row["state"] for row in rows(snap, "messages")} ==
            {"active", "locked", "deferred", "scheduled", "dead_lettered"}, "fixture missing lifecycle state")
    queues = {row["name"]: row for row in rows(snap, "queues")}
    require({row["ordering_mode"] for row in queues.values()} == {"standard", "group_fifo", "strict_fifo"}, "fixture missing ordering configuration")
    require(len(rows(snap, "subscriptions")) == 2 and any(row["filter_json"] for row in rows(snap, "subscriptions")), "fixture missing subscription/filter")
    locked = [row for row in rows(snap, "messages") if row["state"] == "locked"]
    require({row["delivery_count"] >= queues[row["queue"]]["max_delivery_count"] for row in locked} == {False, True}, "fixture missing both recovery branches")
    require(rows(snap, "sqlite_sequence") == [{"name": "messages", "seq": fixture["receipt"]["sequence"]}], "retired sequence was not preserved")
    require(fixture["receipt"]["sequence"] > max(item["sequence"] for item in fixture["entries"]), "fixture must retire highest committed ID")


def after_open(before):
    expected = copy.deepcopy(before)
    limits = {row["name"]: row["max_delivery_count"] for row in rows(before, "queues")}
    data = expected["tables"]["messages"]
    col = {column[1]: i for i, column in enumerate(data["columns"])}
    for row in data["rows"]:
        if row[col["state"]][1] != "locked":
            continue
        dead = row[col["delivery_count"]][1] >= limits[row[col["queue"]][1]]
        row[col["state"]] = cell("dead_lettered" if dead else "active")
        row[col["locked_until"]] = cell(0)
        row[col["lock_token"]] = cell(None)
        if dead:
            row[col["dead_letter_reason"]] = cell("MaxDeliveryCountExceeded")
    data["rows"].sort(key=canonical)
    return expected


class Worker:
    def __init__(self, helper, mode, db, manifest, out):
        self.out = out
        self.output = ""
        self.errors = out.with_suffix(".stderr.log").open("w")
        self.proc = subprocess.Popen([str(helper), mode, str(db), str(manifest)],
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                     stderr=self.errors, text=True)
        try:
            require(select.select([self.proc.stdout], [], [], 60)[0], "worker did not reach barrier")
            self.output = self.proc.stdout.readline()
            require(json.loads(self.output) == {"event": "ready", "mode": mode}, "invalid worker barrier")
        except BaseException:
            self.abort()
            raise

    def finish(self, command):
        try:
            tail, _ = self.proc.communicate(command + "\n", timeout=60)
            self.output += tail
            require(self.proc.returncode == 0, "worker failed; see " + str(self.out))
            require(json.loads(tail)["event"] == "passed", "worker did not pass")
        finally:
            self.abort()

    def abort(self):
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=10)
        self.out.with_suffix(".stdout.log").write_text(self.output)
        self.errors.close()


def command(args, log, env=None, expected=0):
    result = subprocess.run([str(arg) for arg in args], env=env, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=180)
    log.write_text("command=" + canonical([str(arg) for arg in args]) + "\n" + result.stdout +
                   "\nexit_code=" + str(result.returncode) + "\n")
    require((result.returncode == 0) if expected == 0 else (result.returncode != 0), "unexpected command exit; see " + str(log))
    return result.stdout


def cli(binary, db, out, label, *args, refusal=False, output_json=True):
    env = {key: value for key, value in os.environ.items() if not key.startswith("MQLITE_")}
    env["MQLITE_DB"] = "file:" + str(db)
    flags = ["--output", "json"] if output_json else []
    return command([binary, *args, *flags], out / (label + ".log"), env, expected=1 if refusal else 0)


def readonly_backup(sqlite, source, destination, log, stopped=False):
    require(not destination.exists(), "backup destination already exists")
    # External tools never write/checkpoint the active source connection.
    target = str(destination).replace("'", "''")
    command([sqlite, "-readonly", readonly_uri(source, stopped), "VACUUM INTO '" + target + "'"], log)


def tree_hashes(root):
    return {str(path.relative_to(root)): digest(path) for path in sorted(root.rglob("*")) if path.is_file()}


def schema_drill(args, out):
    old = out / "schema2-original" / "mq.db"
    new = out / "schema-current" / "mq.db"
    rollback = out / "schema2-rollback" / "mq.db"
    for db in (old, new, rollback):
        db.parent.mkdir()
    cli(args.old_binary, old, out, "old-create", "create-queue", "rollback", output_json=False)
    cli(args.old_binary, old, out, "old-send", "send", "rollback", "old-payload-café", "--message-id", "old-identity", output_json=False)
    before = snapshot(old, stopped=True)
    save(out / "schema2-before.json", before)
    old_schema = dict((row["key"], row["value"]) for row in rows(before, "meta"))["schema_version"]
    require(old_schema == "2", "legacy binary did not create genuine schema 2")
    old_rows = rows(before, "messages")
    require(len(old_rows) == 1 and old_rows[0]["message_id"] == "old-identity" and
            old_rows[0]["body"] == "old-payload-café".encode(), "legacy seed identity/content differs")
    old_backup = out / "schema2-backup.db"
    readonly_backup(args.sqlite, old, old_backup, out / "schema2-backup.log", stopped=True)
    compare(snapshot(old_backup, stopped=True), before, "legacy backup")
    refusal = cli(args.candidate, old, out, "candidate-refuses-schema2", "metrics", "rollback", refusal=True)
    current_schema_match = re.search(r'expects "([^"]+)"', refusal)
    require('database schema is "2"' in refusal and current_schema_match, "candidate failed without schema mismatch reason")
    current_schema = current_schema_match.group(1)
    after = snapshot(old, stopped=True)
    save(out / "schema2-after-refusal.json", after)
    compare(after, before, "candidate refusal modified legacy database")
    # Assert physical schema really differs, beyond the metadata guard token.
    cli(args.candidate, new, out, "current-create", "create-queue", "fresh")
    cli(args.candidate, new, out, "current-send", "send", "fresh", "new-payload-café", "--message-id", "new-identity")
    fresh = snapshot(new, stopped=True)
    save(out / "current-before-refusal.json", fresh)
    require(fresh["schema"] != before["schema"], "legacy and candidate physical schemas unexpectedly identical")
    reverse = cli(args.old_binary, new, out, "legacy-refuses-current", "metrics", "fresh", refusal=True, output_json=False)
    require('database schema is "' + current_schema + '"' in reverse and 'expects "2"' in reverse, "legacy failed without reverse schema mismatch reason")
    fresh_after = snapshot(new, stopped=True)
    save(out / "current-after-refusal.json", fresh_after)
    compare(fresh_after, fresh, "legacy refusal modified candidate database")
    shutil.copy2(old_backup, rollback)
    compare(snapshot(rollback, stopped=True), before, "legacy restored snapshot")
    delivered = cli(args.old_binary, rollback, out, "legacy-rollback-consume", "receive", "rollback", output_json=False)
    # v0.2.0 predates --output json. Pin its complete text response, not a body
    # substring; duplicated deliveries or completion warnings must also fail.
    expected_line = 'seq=' + str(old_rows[0]["id"]) + ' deliveries=1 message-id=old-identity body="old-payload-café"\n'
    require(delivered == expected_line, "legacy rollback full response differs")
    final = snapshot(rollback, stopped=True)
    save(out / "schema2-rollback-final.json", final)
    require(not rows(final, "messages"), "legacy auto-complete did not consume rollback message")
    return {"old_schema": old_schema, "candidate_schema": current_schema,
            "old_backup_sha256": digest(old_backup), "rollback_message_id": "old-identity"}


def execute(args, out):
    repo = Path(__file__).resolve().parents[3]
    env = dict(os.environ, GOTOOLCHAIN=args.go_toolchain, CGO_ENABLED="0")
    helper = out / "restore-fixture"
    command(["go", "build", "-trimpath", "-ldflags=-w", "-o", helper, "./test/production/restore"], out / "helper-build.log", env)
    args.candidate = out / "mqlite-candidate"
    command(["go", "build", "-trimpath", "-ldflags=-w", "-o", args.candidate, "./cmd/mqlite"], out / "candidate-build.log", env)
    old_version = command([args.old_binary, "version"], out / "old-version.log")
    require(old_version.strip() == "mqlite 0.2.0", "old binary must be real v0.2.0")
    metadata = {
        "git_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip(),
        "git_status": subprocess.check_output(["git", "status", "--short"], cwd=repo, text=True),
        "candidate_version": command([args.candidate, "version"], out / "candidate-version.log").strip(),
        "candidate_sha256": digest(args.candidate), "old_binary_sha256": digest(args.old_binary),
        "helper_sha256": digest(helper), "toolchain": args.go_toolchain,
        "old_binary_build_info": command(["go", "version", "-m", args.old_binary], out / "old-build-info.log"),
        "candidate_build_info": command(["go", "version", "-m", args.candidate], out / "candidate-build-info.log"),
        "sqlite_cli": command([args.sqlite, "-version"], out / "sqlite-version.log").strip(),
        "python_sqlite": sqlite3.sqlite_version,
        "clock": "fixed seed time, then +60 seconds for scheduled activation; background disabled",
    }
    save(out / "metadata.json", metadata)
    source = out / "live-source"
    source.mkdir()
    (source / "operator-sidecar.json").write_text('{"purpose":"verify complete cold directory copy"}\n')
    db, manifest = source / "mq.db", out / "fixture.json"
    worker = Worker(helper, "seed", db, manifest, out / "seed")
    try:
        fixture = json.loads(manifest.read_text())
        before = snapshot(db)
        save(out / "live-before.json", before)
        check_seed(before, fixture)
        wal = Path(str(db) + "-wal")
        require(wal.exists() and wal.stat().st_size > 32, "live source must contain committed WAL data")
        # Negative control: an isolated main-file-only copy omits the WAL commit.
        # It is evidence of an incomplete backup, and is never restored/approved.
        # Immutable reads apply to this offline copy, never to the live source.
        incomplete = out / "incomplete-main-only.db"
        shutil.copy2(db, incomplete)
        with closing(sqlite3.connect(readonly_uri(incomplete, stopped=True), uri=True)) as conn:
            table = conn.execute("SELECT name FROM sqlite_schema WHERE name='business_orders'").fetchall()
            main_rows = conn.execute("SELECT id FROM business_orders").fetchall() if table else []
            require((fixture["business"],) not in main_rows, "fixture did not retain business write solely in WAL")
        metadata["live_wal_bytes"] = wal.stat().st_size
        save(out / "metadata.json", metadata)
        hot = out / "hot-backup.db"
        readonly_backup(args.sqlite, db, hot, out / "hot-vacuum.log")
        hot_snap = snapshot(hot, stopped=True)
        save(out / "hot-backup.json", hot_snap)
        compare(hot_snap, before, "live read-only VACUUM INTO")
        live_after = snapshot(db)
        save(out / "live-after-backup.json", live_after)
        compare(live_after, before, "hot backup modified source")
        worker.finish("stop")
    finally:
        worker.abort()
    stopped = snapshot(db, stopped=True)
    save(out / "cold-source.json", stopped)
    compare(stopped, before, "quiescent close changed logical source")
    cold = out / "cold-backup"
    shutil.copytree(source, cold)
    compare(tree_hashes(cold), tree_hashes(source), "complete cold directory file hashes")
    compare(snapshot(cold / "mq.db", stopped=True), before, "cold directory copy")
    backup_hashes = {"hot": digest(hot), "cold": tree_hashes(cold)}
    expected = after_open(before)
    save(out / "expected-after-open.json", expected)
    for kind in ("hot", "cold"):
        restored = out / (kind + "-restored")
        if kind == "hot":
            restored.mkdir()
            shutil.copy2(hot, restored / "mq.db")
        else:
            shutil.copytree(cold, restored)
        restored_db = restored / "mq.db"
        compare(snapshot(restored_db, stopped=True), before, kind + " restored before engine open")
        worker = Worker(helper, "restore", restored_db, manifest, out / (kind + "-verify"))
        try:
            actual = snapshot(restored_db)
            save(out / (kind + "-after-open.json"), actual)
            compare(actual, expected, kind + " complete orphan recovery transformation")
            worker.finish("verify")
        finally:
            worker.abort()
        final = snapshot(restored_db, stopped=True)
        save(out / (kind + "-final.json"), final)
        require(not rows(final, "messages"), kind + " restore left unexpected messages")
        compare(final["tables"]["business_orders"], before["tables"]["business_orders"], kind + " business content changed")
    compare({"hot": digest(hot), "cold": tree_hashes(cold)}, backup_hashes, "verification mutated immutable backups")
    schema = schema_drill(args, out)
    save(out / "metadata.json", metadata)
    return {"status": "PASS", "backup_sha256": backup_hashes, "schema_drill": schema,
            "logical_snapshot_sha256": hashlib.sha256(canonical(before).encode()).hexdigest(),
            "fixture_message_count": len(fixture["entries"]), "tables": sorted(before["tables"])}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="new, nonexistent evidence directory")
    parser.add_argument("--old-binary", type=Path, required=True, help="actual v0.2.0 executable")
    parser.add_argument("--go-toolchain", default="go1.27.1")
    parser.add_argument("--sqlite", default="sqlite3")
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.old_binary = args.old_binary.resolve()
    require(not args.output.exists(), "output must be a new directory")
    args.output.mkdir(parents=True)
    os.chdir(Path(__file__).resolve().parents[3])
    started = time.time()
    try:
        result = execute(args, args.output)
    except BaseException as exc:
        save(args.output / "result.json", {"status": "FAIL", "error": str(exc), "elapsed_seconds": time.time() - started})
        raise
    result["elapsed_seconds"] = time.time() - started
    save(args.output / "result.json", result)
    print(canonical(result))


if __name__ == "__main__":
    main()
