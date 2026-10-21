#!/usr/bin/env python3
"""Prove schema-7 refusal leaves a released schema-5 database recoverable.

Use the released v0.3.2 binary and source to create the full schema-5 fixture.
The candidate must reject it without changing schema or data. The matching old
binary must then reopen that same database and complete a read/write round trip.
The reverse check proves the old binary also rejects a fresh schema-7 database
without changing it; cross-schema use requires a new database or restored backup.
"""

import argparse
import base64
import json
import os
import re
import subprocess
from pathlib import Path

from run import (Worker, after_open, canonical, cell, cli, compare, digest,
                 require, rows, save, snapshot)


BASE_SHA = "8232296ea871c9550b58e13328b729ac771cffe8"


def check_old_fixture(snap):
    expected = {"queues", "subscriptions", "messages", "dedup", "settlement_receipts",
                "receive_attempts", "meta", "sqlite_sequence", "business_orders", "access_keys"}
    require(set(snap["tables"]) == expected, "v0.3.2 fixture table surface changed")
    require(all(rows(snap, table) for table in expected), "old fixture left a table empty")
    require({row["state"] for row in rows(snap, "messages")} ==
            {"active", "locked", "deferred", "scheduled", "dead_lettered"},
            "old message states missing")
    require({row["ordering_mode"] for row in rows(snap, "queues")} ==
            {"standard", "group_fifo", "strict_fifo"}, "old ordering modes missing")
    require(rows(snap, "meta") == [{"key": "schema_version", "value": "5"}],
            "old schema is not 5")


def typed_rows(snap, table, omit_id=None):
    values = rows(snap, table)
    if omit_id is not None:
        values = [row for row in values if row.get("id") != omit_id]
    return sorted([{key: cell(value) for key, value in row.items()} for row in values],
                  key=canonical)


def require_schema_refusal(output, actual, expected, label):
    match = re.search(r'expects "([^"]+)"', output)
    require('database schema is "' + actual + '"' in output and match and
            match.group(1) == expected, label + " did not report the exact schema mismatch")


def run(args, out):
    source_sha = subprocess.check_output(["git", "rev-parse", "HEAD"],
                                         cwd=args.old_source, text=True).strip()
    require(source_sha == BASE_SHA,
            "old fixture source must be the exact released v0.3.2 commit")
    require(not subprocess.check_output(
        ["git", "status", "--porcelain", "--untracked-files=no"],
        cwd=args.old_source, text=True).strip(), "old fixture source is dirty")
    old_version = subprocess.check_output([str(args.old_binary), "version"], text=True).strip()
    require(old_version == "mqlite 0.3.2", "old broker is not the released v0.3.2")
    new_version = subprocess.check_output([str(args.new_binary), "version"], text=True).strip()
    require(new_version != old_version, "candidate must report a distinct patch version")

    fixture = out / "v032-fixture"
    build_env = dict(os.environ, GOTOOLCHAIN=args.go_toolchain)
    result = subprocess.run(["go", "build", "-trimpath", "-o", str(fixture),
                             "./test/production/restore"], cwd=args.old_source,
                            env=build_env, text=True, capture_output=True, timeout=180)
    (out / "fixture-build.log").write_text(result.stdout + result.stderr)
    require(result.returncode == 0, "failed to build the original fixture")

    db, manifest = out / "schema5.db", out / "old-message-manifest.json"
    worker = Worker(fixture, "seed", db, manifest, out / "old-seed")
    try:
        seeded = snapshot(db)
        check_old_fixture(seeded)
        save(out / "01-old-seed.json", seeded)
        worker.finish("stop")
    finally:
        worker.abort()
    compare(snapshot(db, stopped=True), seeded, "old seed close")

    # A real released binary performs orphan-lock recovery before the candidate
    # ever sees the database. This is the exact schema-5 state to preserve.
    cli(args.old_binary, db, out, "old-first-peek", "peek", "active")
    opened = snapshot(db, stopped=True)
    compare(opened, after_open(seeded), "actual old binary orphan recovery")
    cli(args.old_binary, db, out, "old-create-roundtrip", "create-queue",
        "compat-roundtrip", output_json=False)
    before = snapshot(db, stopped=True)
    save(out / "02-schema5-before-refusal.json", before)

    refusal = cli(args.new_binary, db, out, "candidate-refuses-schema5",
                  "metrics", "active", refusal=True)
    require_schema_refusal(refusal, "5", "7", "candidate")
    after_refusal = snapshot(db, stopped=True)
    save(out / "03-schema5-after-refusal.json", after_refusal)
    compare(after_refusal, before, "candidate refusal modified schema-5 database")

    # The matching binary remains the recovery path after the refused open.
    payload = "rollback-write-café"
    sent = json.loads(cli(args.old_binary, db, out, "old-write-after-refusal", "send",
                          "compat-roundtrip", payload, "--message-id", "rollback-write"))
    sequence = sent.get("seq")
    require(isinstance(sequence, int) and sequence > 0, "old binary did not acknowledge its write")
    peeked = json.loads(cli(args.old_binary, db, out, "old-read-after-refusal",
                            "peek", "compat-roundtrip"))
    require(len(peeked) == 1 and peeked[0].get("seq_number") == sequence and
            base64.b64decode(peeked[0].get("body", "")) == payload.encode(),
            "matching old binary could not read back its post-refusal write")
    recovered = snapshot(db, stopped=True)
    save(out / "04-schema5-old-roundtrip.json", recovered)
    for table in before["tables"]:
        if table not in ("messages", "sqlite_sequence"):
            compare(recovered["tables"][table], before["tables"][table],
                    "old recovery changed unrelated table " + table)
    compare(typed_rows(recovered, "messages", sequence), typed_rows(before, "messages"),
            "old recovery changed pre-existing messages")
    inserted = next((row for row in rows(recovered, "messages") if row["id"] == sequence), None)
    require(inserted is not None and inserted["queue"] == "compat-roundtrip" and
            inserted["state"] == "active" and inserted["message_id"] == "rollback-write" and
            inserted["body"] == payload.encode(), "old recovery write contents differ")
    before_sequence = rows(before, "sqlite_sequence")[0]["seq"]
    require(sequence == before_sequence + 1 and
            rows(recovered, "sqlite_sequence") == [{"name": "messages", "seq": sequence}],
            "old recovery lost the sequence high-water mark")

    # Pin the other side of the boundary: a fresh candidate creates schema 7,
    # and the released schema-5 binary refuses it without altering it.
    fresh = out / "schema7.db"
    cli(args.new_binary, fresh, out, "candidate-create-schema7", "create-queue", "fresh")
    schema7 = snapshot(fresh, stopped=True)
    save(out / "05-schema7-fresh.json", schema7)
    require(rows(schema7, "meta") == [{"key": "schema_version", "value": "7"}],
            "candidate did not create schema 7")
    queue_columns = {column[1] for column in schema7["tables"]["queues"]["columns"]}
    require({"lifecycle_state", "delete_started_at", "expires_at"}.issubset(queue_columns),
            "schema 7 is missing queue lifecycle or expiration columns")
    reverse = cli(args.old_binary, fresh, out, "old-refuses-schema7",
                  "metrics", "fresh", refusal=True)
    require_schema_refusal(reverse, "7", "5", "released binary")
    schema7_after = snapshot(fresh, stopped=True)
    save(out / "06-schema7-after-old-refusal.json", schema7_after)
    compare(schema7_after, schema7, "old refusal modified schema-7 database")

    return {
        "status": "PASS",
        "old_version": old_version,
        "new_version": new_version,
        "old_source_sha": source_sha,
        "old_binary_sha256": digest(args.old_binary),
        "new_binary_sha256": digest(args.new_binary),
        "fixture_binary_sha256": digest(fixture),
        "old_schema_version": "5",
        "new_schema_version": "7",
        "old_tables_verified": sorted(before["tables"]),
        "snapshots": 6,
        "candidate_refusal_preserved_schema5": True,
        "matching_old_runtime_read_write": True,
        "old_refusal_preserved_schema7": True,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--old-binary", type=Path, required=True)
    parser.add_argument("--old-source", type=Path, required=True)
    parser.add_argument("--new-binary", type=Path, required=True)
    parser.add_argument("--go-toolchain", default="go1.27.1",
                        help="GOTOOLCHAIN value for building the historical fixture")
    parser.add_argument("--output", type=Path, required=True,
                        help="new, nonexistent evidence directory")
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
