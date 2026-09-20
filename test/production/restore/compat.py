#!/usr/bin/env python3
"""Prove schema-5 access keys survive real v0.3.0 upgrade/rollback/upgrade.

Use the released v0.3.0 broker and a clean checkout of its source for the existing
full message-state fixture. All snapshots are read-only; only brokers write data.
Access token plaintext stays in memory and is never written to evidence logs.
"""

import argparse
import base64
import copy
import hashlib
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request

from run import Worker, after_open, canonical, cell, cli, compare, digest, require, rows, save, snapshot


BASE_SHA = "86a85f36443c15e07dc99c78f045680a93a4c612"
AUTH = "/mqlite.v1.AuthService/"
QUEUE = "/mqlite.v1.QueueService/"


def environment(db):
    env = {key: value for key, value in os.environ.items() if not key.startswith("MQLITE_")}
    env.update(MQLITE_DB="file:" + str(db), MQLITE_SYNC="FULL", MQLITE_UI="off")
    return env


class Broker:
    def __init__(self, binary, db, admin, log):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        self.endpoint = "http://127.0.0.1:" + str(port)
        self.log = log.open("w")
        env = environment(db)
        env["MQLITE_TOKENS"] = admin
        self.proc = subprocess.Popen([str(binary), "serve", "--addr", "127.0.0.1:" + str(port)],
                                     env=env, stdout=self.log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                require(self.proc.poll() is None, "broker exited before readiness; see " + str(log))
                try:
                    with urllib.request.urlopen(self.endpoint + "/healthz", timeout=1) as response:
                        if response.status == 200:
                            return
                except (urllib.error.URLError, TimeoutError):
                    time.sleep(0.025)
            raise RuntimeError("broker readiness timed out")
        except BaseException:
            self.stop()
            raise

    def rpc(self, path, token, body, status=200):
        request = urllib.request.Request(self.endpoint + path, data=canonical(body).encode(),
                                         headers={"Authorization": "Bearer " + token,
                                                  "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                actual, payload = response.status, response.read()
        except urllib.error.HTTPError as error:
            actual, payload = error.code, error.read()
        require(actual == status, path + ": unexpected HTTP " + str(actual) + ", expected " + str(status))
        return json.loads(payload)

    def stop(self):
        try:
            if self.proc.poll() is None:
                self.proc.send_signal(signal.SIGTERM)
                try:
                    self.proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.proc.kill()
                    self.proc.wait(timeout=10)
                    raise RuntimeError("broker did not stop gracefully")
            require(self.proc.returncode == 0, "broker did not exit successfully")
        finally:
            self.log.close()


def old_surface(snap):
    result = copy.deepcopy(snap)
    result["schema"] = [obj for obj in result["schema"] if obj[2] != "access_keys"]
    result["tables"].pop("access_keys", None)
    return result


def check_old_fixture(snap):
    expected = {"queues", "subscriptions", "messages", "dedup", "settlement_receipts",
                "receive_attempts", "meta", "sqlite_sequence", "business_orders"}
    require(set(snap["tables"]) == expected, "v0.3.0 fixture table surface changed")
    require(all(rows(snap, table) for table in expected), "old fixture left a table empty")
    require({row["state"] for row in rows(snap, "messages")} ==
            {"active", "locked", "deferred", "scheduled", "dead_lettered"}, "old message states missing")
    require({row["ordering_mode"] for row in rows(snap, "queues")} ==
            {"standard", "group_fifo", "strict_fifo"}, "old ordering modes missing")
    require(rows(snap, "meta") == [{"key": "schema_version", "value": "5"}], "old schema is not 5")


def assert_key_states(broker, admin, tokens, ids):
    broker.rpc(QUEUE + "Peek", tokens["active"], {"queue": "active"})
    for name in ("expired", "revoked"):
        response = broker.rpc(QUEUE + "Peek", tokens[name], {"queue": "active"}, 401)
        require(response["code"] == "unauthenticated", "inactive key classification changed")
    listed = broker.rpc(AUTH + "ListKeys", admin, {})["keys"]
    require(len(listed) == 3 and {key["id"] for key in listed} == set(ids.values()), "key list changed")
    require(all(set(key) == {"id", "name", "permissions", "created_at_ms", "expires_at_ms", "revoked_at_ms"}
                for key in listed), "key metadata surface exposed an unexpected field")
    return listed


def run(args, out):
    source_sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=args.old_source, text=True).strip()
    require(source_sha == BASE_SHA, "old fixture source must be the exact released v0.3.0 commit")
    require(not subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=no"],
                                        cwd=args.old_source, text=True).strip(), "old fixture source is dirty")
    old_version = subprocess.check_output([str(args.old_binary), "version"], text=True).strip()
    require(old_version == "mqlite 0.3.0", "old broker is not the released v0.3.0")
    new_version = subprocess.check_output([str(args.new_binary), "version"], text=True).strip()
    require(new_version != old_version, "candidate must report a distinct patch version")
    fixture = out / "v030-fixture"
    build_env = dict(os.environ, GOTOOLCHAIN="go1.27.1")
    result = subprocess.run(["go", "build", "-trimpath", "-o", str(fixture), "./test/production/restore"],
                            cwd=args.old_source, env=build_env, text=True, capture_output=True, timeout=180)
    (out / "fixture-build.log").write_text(result.stdout + result.stderr)
    require(result.returncode == 0, "failed to build the original fixture")
    db, manifest = out / "mq.db", out / "old-message-manifest.json"
    worker = Worker(fixture, "seed", db, manifest, out / "old-seed")
    try:
        seeded = snapshot(db)
        check_old_fixture(seeded)
        save(out / "01-old-seed.json", seeded)
        worker.finish("stop")
    finally:
        worker.abort()
    compare(snapshot(db, stopped=True), seeded, "old seed close")
    # The actual released executable opens the old fixture before any candidate.
    cli(args.old_binary, db, out, "old-first-peek", "peek", "active")
    opened = snapshot(db, stopped=True)
    compare(opened, after_open(seeded), "actual old binary orphan recovery")
    cli(args.old_binary, db, out, "old-create-roundtrip", "create-queue", "compat-roundtrip")
    before = snapshot(db, stopped=True)
    save(out / "02-old-before-upgrade.json", before)
    admin = "mqk_" + secrets.token_hex(32)
    tokens, ids = {}, {name: secrets.token_hex(16) for name in ("active", "expired", "revoked")}
    current = Broker(args.new_binary, db, admin, out / "candidate-first.log")
    try:
        initial = snapshot(db)
        compare(old_surface(initial), before, "candidate first open: all old schema/data")
        require(rows(initial, "access_keys") == [], "candidate did not add an empty independent table")
        extras = [obj for obj in initial["schema"] if obj not in before["schema"]]
        require(len(extras) == 4 and all(obj[2] == "access_keys" for obj in extras) and
                {obj[1] for obj in extras} == {"access_keys", "sqlite_autoindex_access_keys_1",
                                               "sqlite_autoindex_access_keys_2", "idx_access_keys_created"},
                "candidate added objects outside the key table, unique indexes and creation-order index")
        for name, permissions in (("active", ["manage"]), ("expired", ["send"]), ("revoked", ["listen"])):
            request = {"id": ids[name], "name": name, "permissions": permissions}
            if name == "expired":
                request["expires_at_ms"] = int(time.time() * 1000) + 2000
                expiry = request["expires_at_ms"]
            response = current.rpc(AUTH + "CreateKey", admin, request)
            tokens[name] = response["token"]
        current.rpc(AUTH + "RevokeKey", tokens["active"], {"id": ids["revoked"]})
        time.sleep(max(0, expiry / 1000 - time.time()) + 0.025)
        metadata = assert_key_states(current, admin, tokens, ids)
    finally:
        current.stop()
    upgraded = snapshot(db, stopped=True)
    save(out / "03-candidate-with-keys.json", upgraded)
    compare(old_surface(upgraded), before, "key creation/auth/revoke preserved every old table")
    for key in rows(upgraded, "access_keys"):
        require(key["token_hash"] == hashlib.sha256(tokens[key["name"]].encode()).digest(),
                "stored key digest does not match its one-time secret")
    legacy = Broker(args.old_binary, db, admin, out / "old-rollback.log")
    try:
        compare(snapshot(db), upgraded, "old rollback preserved complete schema and key rows")
        legacy.rpc(QUEUE + "Peek", tokens["active"], {"queue": "active"}, 401)
        legacy.rpc(QUEUE + "Peek", admin, {"queue": "active"})
        legacy.rpc(AUTH + "ListKeys", admin, {}, 404)
        sent_at = int(time.time() * 1000)
        message = {"body": base64.b64encode(b"rollback-write").decode(), "message_id": "rollback-write"}
        sent = legacy.rpc(QUEUE + "Send", admin, {"queue": "compat-roundtrip", "messages": [message]})
        sent_until = int(time.time() * 1000)
        require(len(sent["seq_numbers"]) == 1, "old broker failed to write a message")
        sequence = sent["seq_numbers"][0]
        peek = legacy.rpc(QUEUE + "Peek", admin, {"queue": "compat-roundtrip"})["messages"]
        require(len(peek) == 1 and peek[0]["body"] == message["body"], "old broker could not read its new message")
    finally:
        legacy.stop()
    rolled_back = snapshot(db, stopped=True)
    save(out / "04-old-after-write.json", rolled_back)
    expected = copy.deepcopy(upgraded)
    inserted = next(row for row in rows(rolled_back, "messages") if row["id"] == sequence)
    require(sent_at <= inserted["enqueued_at"] <= sent_until and inserted["visible_at"] == inserted["enqueued_at"],
            "old send timestamp is outside the operation")
    expected_row = {column[1]: None for column in expected["tables"]["messages"]["columns"]}
    expected_row.update(id=sequence, queue="compat-roundtrip", state="active", visible_at=inserted["enqueued_at"],
                        locked_until=0, delivery_count=0, enqueued_at=inserted["enqueued_at"], expires_at=0,
                        message_id="rollback-write", body=b"rollback-write")
    compare({key: cell(value) for key, value in inserted.items()},
            {key: cell(value) for key, value in expected_row.items()}, "every field of old broker write")
    expected["tables"]["messages"]["rows"].append([cell(expected_row[column[1]])
                                                     for column in expected["tables"]["messages"]["columns"]])
    expected["tables"]["messages"]["rows"].sort(key=canonical)
    require(sequence == rows(upgraded, "sqlite_sequence")[0]["seq"] + 1, "old broker lost sequence high-water mark")
    expected["tables"]["sqlite_sequence"]["rows"] = [[cell("messages"), cell(sequence)]]
    compare(rolled_back, expected, "only the independently predicted old send changed data")
    current = Broker(args.new_binary, db, admin, out / "candidate-second.log")
    try:
        compare(snapshot(db), rolled_back, "second upgrade preserved every schema/data field")
        compare(assert_key_states(current, admin, tokens, ids), metadata, "key metadata across rollback/upgrade")
        current.rpc(AUTH + "RevokeKey", tokens["active"], {"id": ids["active"]})
        current.rpc(QUEUE + "Peek", tokens["active"], {"queue": "active"}, 401)
        current.rpc(QUEUE + "Peek", admin, {"queue": "active"})
    finally:
        current.stop()
    final = snapshot(db, stopped=True)
    save(out / "05-candidate-revoked.json", final)
    expected = copy.deepcopy(rolled_back)
    columns = {column[1]: index for index, column in enumerate(expected["tables"]["access_keys"]["columns"])}
    final_keys = {key["id"]: key for key in rows(final, "access_keys")}
    for row in expected["tables"]["access_keys"]["rows"]:
        if row[columns["id"]][1] == ids["active"]:
            require(final_keys[ids["active"]]["revoked_at"] > 0, "self-revocation not persisted")
            row[columns["revoked_at"]] = cell(final_keys[ids["active"]]["revoked_at"])
    expected["tables"]["access_keys"]["rows"].sort(key=canonical)
    compare(final, expected, "only final self-revocation changed the database")
    for path in out.iterdir():
        if path.suffix in (".log", ".json"):
            text = path.read_text()
            require(all(secret not in text for secret in [admin, *tokens.values()]), "access secret leaked into evidence")
    return {"status": "PASS", "old_version": old_version, "new_version": new_version,
            "old_source_sha": source_sha, "old_binary_sha256": digest(args.old_binary),
            "new_binary_sha256": digest(args.new_binary), "fixture_binary_sha256": digest(fixture),
            "schema_version": "5", "old_tables_verified": sorted(before["tables"]),
            "snapshots": 5, "key_states": ["active", "expired", "revoked"],
            "old_runtime_rejects_database_keys": True, "old_runtime_read_write": True,
            "upgraded_runtime_reuses_and_revokes_database_keys": True, "plaintext_secrets_in_evidence": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--old-binary", type=Path, required=True)
    parser.add_argument("--old-source", type=Path, required=True)
    parser.add_argument("--new-binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True, help="new, nonexistent evidence directory")
    args = parser.parse_args()
    for field in ("old_binary", "old_source", "new_binary", "output"):
        setattr(args, field, getattr(args, field).resolve())
    args.output.mkdir(parents=True, exist_ok=False)
    try:
        result = run(args, args.output)
    except BaseException as error:
        save(args.output / "result.json", {"status": "FAIL", "error": str(error)})
        raise
    save(args.output / "result.json", result)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
