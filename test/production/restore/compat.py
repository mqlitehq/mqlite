#!/usr/bin/env python3
"""Prove schema-7 refusal leaves a released schema-5 database recoverable.

Use the published v0.3.2 archive, binary, and exact source commit to create the
full schema-5 fixture. Build the candidate from an archive of its clean HEAD.
The candidate must reject schema 5 without changing schema or data, and the
matching old binary must remain usable. The reverse check proves the old binary
also rejects a fresh schema-7 database without changing it.
"""

import argparse
import base64
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import tarfile
import tempfile
import zipfile

from run import (Worker, after_open, canonical, cell, cli, command, compare,
                 digest, require, rows, save, snapshot)


BASE_SHA = "8232296ea871c9550b58e13328b729ac771cffe8"
OLD_VERSION = "mqlite 0.3.2"
NEW_VERSION = "mqlite 0.4.0"
# SHA-256 of the checksums.txt asset published with v0.3.2. This offline trust
# anchor prevents a matching, locally fabricated archive and manifest from
# being accepted as the released artifact.
V032_CHECKSUMS_SHA256 = "8964707b9ebe2c52e31ae564d062aad61ad83ca7dac2fb295f83af72845a3887"
VERSION_RE = re.compile(r'^const Version = "([^"]+)"$', re.MULTILINE)
SNAPSHOTS = (
    "01-old-seed.json",
    "02-schema5-before-refusal.json",
    "03-schema5-after-refusal.json",
    "04-schema5-old-roundtrip.json",
    "05-schema7-fresh.json",
    "06-schema7-after-old-refusal.json",
)


def sha256_bytes(value):
    return hashlib.sha256(value).hexdigest()


def parse_checksums(value):
    entries = {}
    require(value, "release checksum file is empty")
    for line in value.splitlines():
        match = re.fullmatch(r"([0-9a-f]{64})  ([A-Za-z0-9][A-Za-z0-9._-]*)", line)
        require(match is not None, "release checksum file has an invalid line")
        checksum, name = match.groups()
        require(name not in entries, "release checksum file has a duplicate artifact")
        entries[name] = checksum
    return entries


def release_binary(archive_path):
    try:
        if archive_path.name.endswith((".tar.gz", ".tgz")):
            with tarfile.open(archive_path, "r:gz") as archive:
                members = [item for item in archive.getmembers() if item.name == "mqlite"]
                require(len(members) == 1 and members[0].isfile(),
                        "release tar archive must contain exactly one regular mqlite member")
                stream = archive.extractfile(members[0])
                require(stream is not None, "release tar mqlite member is unreadable")
                return stream.read(), members[0].name
        if archive_path.name.endswith(".zip"):
            with zipfile.ZipFile(archive_path) as archive:
                members = [item for item in archive.infolist()
                           if item.filename == "mqlite.exe"]
                require(len(members) == 1 and not members[0].is_dir(),
                        "release zip archive must contain exactly one mqlite.exe member")
                with archive.open(members[0]) as stream:
                    return stream.read(), members[0].filename
    except (tarfile.TarError, zipfile.BadZipFile, OSError) as error:
        raise RuntimeError("invalid release archive: " + str(error)) from error
    raise RuntimeError("release archive must be .tar.gz, .tgz, or .zip")


def verify_old_release(old_binary, old_archive, old_checksums):
    for path, label in ((old_binary, "old binary"), (old_archive, "old archive"),
                        (old_checksums, "old checksums")):
        require(path.is_file(), label + " is not a regular file")
    checksums_hash = digest(old_checksums)
    require(checksums_hash == V032_CHECKSUMS_SHA256,
            "checksum file is not the published v0.3.2 checksums.txt")
    entries = parse_checksums(old_checksums.read_text())
    require(old_archive.name in entries,
            "release archive is not listed in the v0.3.2 checksum file")
    archive_hash = digest(old_archive)
    require(archive_hash == entries[old_archive.name],
            "release archive checksum does not match v0.3.2 checksums.txt")
    archived_binary, member = release_binary(old_archive)
    binary_bytes = old_binary.read_bytes()
    require(binary_bytes == archived_binary,
            "old binary does not exactly match the binary in the release archive")
    return {
        "archive": old_archive.name,
        "archive_sha256": archive_hash,
        "checksums": old_checksums.name,
        "checksums_sha256": checksums_hash,
        "binary_member": member,
        "binary_sha256": sha256_bytes(archived_binary),
    }


def git_command(repo, *args, binary=False):
    result = subprocess.run(["git", *args], cwd=repo, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=not binary, timeout=60)
    detail = (result.stderr if not binary else result.stderr.decode(errors="replace")).strip()
    require(result.returncode == 0,
            "git " + " ".join(args) + " failed" + (": " + detail if detail else ""))
    return result.stdout


def source_identity(repo, revision, expected_version, require_clean=False):
    require(repo.is_dir(), "source path is not a directory: " + str(repo))
    commit = git_command(repo, "rev-parse", "--verify", revision + "^{commit}").strip()
    tree = git_command(repo, "rev-parse", "--verify", commit + "^{tree}").strip()
    require(re.fullmatch(r"[0-9a-f]{40}", commit) is not None,
            "source commit is not a full SHA-1")
    require(re.fullmatch(r"[0-9a-f]{40}", tree) is not None,
            "source tree is not a full SHA-1")
    version_source = git_command(repo, "show", commit + ":internal/version/version.go")
    versions = VERSION_RE.findall(version_source)
    require(versions == [expected_version],
            "source version must be exactly " + expected_version)
    status = git_command(repo, "status", "--porcelain=v1", "--untracked-files=all")
    if require_clean:
        require(status == "", "candidate source is dirty: " + status.rstrip())
    return {"commit": commit, "tree": tree, "status_porcelain": status,
            "version": expected_version}


def git_archive(repo, revision):
    data = git_command(repo, "archive", "--format=tar", revision, binary=True)
    return data, sha256_bytes(data)


def extract_git_archive(data, destination):
    destination.mkdir()
    try:
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as archive:
            for member in archive.getmembers():
                relative = PurePosixPath(member.name)
                require(relative.parts and not relative.is_absolute() and
                        ".." not in relative.parts,
                        "git archive contains an unsafe path")
                target = destination.joinpath(*relative.parts)
                if member.isdir():
                    target.mkdir(parents=True, exist_ok=True)
                    continue
                require(member.isfile(), "git archive contains an unsupported entry")
                require(not target.exists(), "git archive contains a duplicate entry")
                target.parent.mkdir(parents=True, exist_ok=True)
                stream = archive.extractfile(member)
                require(stream is not None, "git archive member is unreadable")
                with target.open("xb") as output:
                    while True:
                        block = stream.read(1024 * 1024)
                        if not block:
                            break
                        output.write(block)
                target.chmod(member.mode & 0o777)
    except tarfile.TarError as error:
        raise RuntimeError("invalid git archive: " + str(error)) from error


def build_environment(toolchain):
    env = dict(os.environ)
    env.update({"GOTOOLCHAIN": toolchain, "GOWORK": "off",
                "GOFLAGS": "-mod=readonly", "CGO_ENABLED": "0"})
    return env


def build_from_revision(repo, revision, output, package, log, env, go_version):
    archive_data, archive_hash = git_archive(repo, revision)
    build_command = ["go", "build", "-mod=readonly", "-buildvcs=false",
                     "-trimpath", "-o", str(output), package]
    record = {
        "command": build_command,
        "environment": {key: env[key] for key in
                        ("GOTOOLCHAIN", "GOWORK", "GOFLAGS", "CGO_ENABLED")},
        "go_version": go_version,
        "source_archive_sha256": archive_hash,
        "source_revision": revision,
    }
    try:
        with tempfile.TemporaryDirectory(prefix=".compat-source-", dir=output.parent) as temporary:
            source = Path(temporary) / "source"
            extract_git_archive(archive_data, source)
            result = subprocess.run(build_command, cwd=source, env=env, text=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                    timeout=180)
        record.update({"exit_code": result.returncode, "output": result.stdout})
    except BaseException as error:
        record["error"] = str(error)
        save(log, record)
        raise
    save(log, record)
    require(result.returncode == 0, "build failed; see " + str(log))
    require(output.is_file(), "build succeeded without producing " + str(output))
    return {"command": build_command, "environment": record["environment"],
            "go_version": go_version, "source_archive_sha256": archive_hash,
            "binary_sha256": digest(output)}


def require_version(actual, expected, label):
    require(actual == expected, label + " must report exactly " + expected)


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
    released = verify_old_release(args.old_binary, args.old_archive, args.old_checksums)
    old_version = command([args.old_binary, "version"], out / "old-version.log").strip()
    require_version(old_version, OLD_VERSION, "old broker")
    old_source = source_identity(args.old_source, BASE_SHA, "0.3.2")
    require(old_source["commit"] == BASE_SHA,
            "old fixture source does not contain the exact v0.3.2 commit")
    candidate_source = source_identity(args.new_source, "HEAD", "0.4.0", require_clean=True)

    build_env = build_environment(args.go_toolchain)
    go_version = command(["go", "version"], out / "go-version.log", env=build_env).strip()
    fixture = out / "v032-fixture"
    fixture_build = build_from_revision(args.old_source, BASE_SHA, fixture,
                                        "./test/production/restore",
                                        out / "fixture-build.log", build_env, go_version)
    candidate = out / "mqlite-candidate"
    candidate_build = build_from_revision(args.new_source, candidate_source["commit"],
                                          candidate, "./cmd/mqlite",
                                          out / "candidate-build.log", build_env, go_version)
    # Fail if HEAD or its full porcelain state changed while the binary was built.
    candidate_after_build = source_identity(args.new_source, "HEAD", "0.4.0",
                                             require_clean=True)
    require(candidate_after_build == candidate_source,
            "candidate source changed while its binary was built")

    new_version = command([candidate, "version"], out / "candidate-version.log").strip()
    require_version(new_version, NEW_VERSION, "candidate")

    provenance = {
        "released": {
            **released,
            "fixture_binary_sha256": fixture_build["binary_sha256"],
            "source_archive_sha256": fixture_build["source_archive_sha256"],
            "source_commit": old_source["commit"],
            "source_status_porcelain": old_source["status_porcelain"],
            "source_tree": old_source["tree"],
            "version_output": old_version,
        },
        "candidate": {
            "binary_sha256": candidate_build["binary_sha256"],
            "source_archive_sha256": candidate_build["source_archive_sha256"],
            "source_commit": candidate_source["commit"],
            "source_tree": candidate_source["tree"],
            "source_status_porcelain": candidate_source["status_porcelain"],
            "version_output": new_version,
        },
        "build": {
            "configured_toolchain": args.go_toolchain,
            "go_version": go_version,
            "fixture": fixture_build,
            "candidate": candidate_build,
        },
    }
    save(out / "provenance.json", provenance)

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

    refusal = cli(candidate, db, out, "candidate-refuses-schema5",
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
    cli(candidate, fresh, out, "candidate-create-schema7", "create-queue", "fresh")
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

    snapshot_hashes = {name: digest(out / name) for name in SNAPSHOTS}
    provenance["snapshot_sha256"] = snapshot_hashes
    save(out / "provenance.json", provenance)
    return {
        "status": "PASS",
        "old_version": old_version,
        "new_version": new_version,
        "old_source_sha": old_source["commit"],
        "old_source_tree": old_source["tree"],
        "new_source_sha": candidate_source["commit"],
        "new_source_tree": candidate_source["tree"],
        "old_binary_sha256": released["binary_sha256"],
        "new_binary_sha256": candidate_build["binary_sha256"],
        "fixture_binary_sha256": fixture_build["binary_sha256"],
        "snapshot_sha256": snapshot_hashes,
        "provenance": provenance,
        "old_schema_version": "5",
        "new_schema_version": "7",
        "old_tables_verified": sorted(before["tables"]),
        "snapshots": len(SNAPSHOTS),
        "candidate_refusal_preserved_schema5": True,
        "matching_old_runtime_read_write": True,
        "old_refusal_preserved_schema7": True,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--old-binary", type=Path, required=True,
                        help="extracted mqlite binary from the published v0.3.2 archive")
    parser.add_argument("--old-archive", type=Path, required=True,
                        help="published v0.3.2 .tar.gz or .zip release archive")
    parser.add_argument("--old-checksums", type=Path, required=True,
                        help="published v0.3.2 checksums.txt")
    parser.add_argument("--old-source", type=Path, required=True,
                        help="git repository containing the exact v0.3.2 commit")
    parser.add_argument("--new-source", type=Path, required=True,
                        help="clean v0.4.0 candidate git checkout; HEAD is archived and built")
    parser.add_argument("--go-toolchain", default="go1.27.1",
                        help="GOTOOLCHAIN value for both source-archive builds")
    parser.add_argument("--output", type=Path, required=True,
                        help="new, nonexistent evidence directory")
    args = parser.parse_args()
    for field in ("old_binary", "old_archive", "old_checksums", "old_source",
                  "new_source", "output"):
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
