#!/usr/bin/env python3
"""Exercise a local candidate image against a private 64 MiB full filesystem."""

import argparse
import base64
import contextlib
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import sqlite3
import subprocess
import sys
import tarfile
import time
import urllib.error
import urllib.request


FULL = re.compile(r"database or disk is full|SQLITE_FULL|no space left on device", re.I)
STATES = ("active", "locked", "scheduled", "deferred", "dead_lettered")
BODY_SIZE = 256 * 1024


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def atomic_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


class Drill:
    def __init__(self, args):
        self.args = args
        self.out = args.output.resolve()
        self.out.mkdir(parents=True, exist_ok=False)
        self.run_id = secrets.token_hex(12)
        self.container = "mqlite-disk-full-" + self.run_id
        self.queue = "disk-full-" + self.run_id
        self.token = "mqk_" + secrets.token_hex(32)
        self.env = dict(os.environ, MQLITE_TOKENS=self.token, MQLITE_TOKEN=self.token)
        self.expected, self.acks = {}, {}
        self.endpoint = ""
        self.http = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        self.live = False
        self.broker_live = False
        self.log_offset = 0
        self.started = time.monotonic()
        self.result = {"status": "RUNNING", "started_at": now(), "run_id": self.run_id,
                       "container": self.container, "synchronous": "FULL",
                       "tmpfs_bytes": 64 * 1024 * 1024, "body_bytes": BODY_SIZE}
        atomic_json(self.out / "result.json", self.result)

    def record(self, filename, value):
        with (self.out / filename).open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(dict(at=now(), **value), sort_keys=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    def docker(self, *args, required=True, timeout=30, input_text=None):
        command = [self.args.docker, *args]
        result = subprocess.run(command, capture_output=True, text=True, timeout=timeout,
                                env=self.env, input=input_text)
        self.record("commands.jsonl", {"argv": command, "returncode": result.returncode,
                                      "stdout": result.stdout, "stderr": result.stderr})
        if required:
            check(result.returncode == 0, "Docker command failed: " + result.stderr.strip())
        return result

    def shell(self, script, required=True):
        return self.docker("exec", self.container, "sh", "-c", script, required=required)

    def request(self, path, body=None, token=True):
        data = None if body is None else json.dumps(body).encode()
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = "Bearer " + self.token
        request = urllib.request.Request(self.endpoint + path, data=data, headers=headers)
        try:
            with self.http.open(request, timeout=10) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            with error:
                return error.code, error.read()

    def rpc(self, service, method, body):
        status, raw = self.request("/mqlite.v1." + service + "Service/" + method, body)
        check(status == 200, f"{service}/{method}: HTTP {status}: {raw[:2048]!r}")
        return json.loads(raw)

    def inspect(self):
        # Container inspection includes the ephemeral auth token; never persist it.
        result = subprocess.run([self.args.docker, "inspect", self.container], capture_output=True,
                                text=True, check=True, timeout=30, env=self.env)
        info = json.loads(result.stdout)[0]
        environment = dict(item.split("=", 1) for item in info["Config"]["Env"] if "=" in item)
        check(environment.get("MQLITE_SYNC") == "FULL", "container is not configured for FULL synchronous mode")
        state = {"state": info["State"], "restart_count": info["RestartCount"],
                 "image": info["Image"], "tmpfs": info["HostConfig"]["Tmpfs"]}
        self.record("container.jsonl", state)
        check(not state["state"]["OOMKilled"], "container was OOM-killed")
        check(state["state"]["Running"], "holder container stopped unexpectedly")
        check(state["restart_count"] == 0, "holder container was restarted (tmpfs proof lost)")
        return state

    def logs(self, phase, allow_full=False):
        text = self.shell("cat /tmp/broker.log").stdout
        delta = text[self.log_offset:]
        self.log_offset = len(text)
        (self.out / "broker.log").write_text(text, encoding="utf-8")
        errors = [line for line in delta.splitlines()
                  if re.search(r"\b(WARN|ERRO|ERROR|FATAL|PANIC)\b|panic:|status=[45]\d\d", line, re.I)
                  and not (phase == "before-full" and "AdminService/Status status=401 code=unauthenticated" in line)]
        self.record("log-checks.jsonl", {"phase": phase, "errors": errors, "allow_full": allow_full})
        # Request access logs intentionally omit the error message. Correlate
        # exactly two failed Send logs with the two independently checked
        # HTTP/CLI disk-full responses, rather than accepting arbitrary 500s.
        denied_sends = [line for line in errors if re.search(
            r"QueueService/Send status=500 queue=" + re.escape(self.queue) + r" code=internal dur=\S+$", line)]
        if allow_full:
            check(len(denied_sends) == 2, "expected exactly two disk-full Send access logs")
        check(all(allow_full and (FULL.search(line) or line in denied_sends) for line in errors),
              f"unexpected broker errors during {phase}: {errors}")

    def start_broker(self):
        self.docker("exec", "-d", self.container, "sh", "-c",
                    "rm -f /tmp/broker.exit; mqlite serve --addr :6754 >>/tmp/broker.log 2>&1 & "
                    "echo $! >/tmp/broker.pid; wait $!; echo $? >/tmp/broker.exit")
        self.broker_live = True
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                status, raw = self.request("/healthz", token=False)
                if status == 200 and raw.strip() == b"ok":
                    break
            except (OSError, urllib.error.URLError):
                pass
            time.sleep(0.2)
        else:
            raise RuntimeError("broker did not become healthy within 30 seconds")
        status = self.rpc("Admin", "Status", {})
        check(status.get("version") == self.args.expected_version.partition("-rc.")[0]
              and status.get("auth") is True and status.get("backend") == "local file"
              and status.get("location") == "/fault/db/mq.db"
              and status.get("schema_version") == "5" and status.get("ping_ms", -1) >= 0,
              f"unexpected candidate runtime metadata: {status}")
        self.record("runtime.jsonl", status)
        check(self.shell("cat /fault/identity").stdout == self.run_id,
              "tmpfs identity changed across broker restart")
        self.inspect()

    def stop_broker(self, signal="TERM"):
        check(signal in ("TERM", "KILL"), "invalid stop signal")
        self.shell('kill -' + signal + ' "$(cat /tmp/broker.pid)"')
        for _ in range(100):
            ended = self.shell("test -f /tmp/broker.exit && cat /tmp/broker.exit", required=False)
            if ended.returncode == 0:
                code = int(ended.stdout.strip())
                check(code == (137 if signal == "KILL" else 0), f"unexpected broker exit: {code}")
                self.broker_live = False
                self.record("processes.jsonl", {"signal": signal, "exit_code": code})
                return
            time.sleep(0.1)
        raise RuntimeError("broker process did not stop")

    def message(self, label):
        identity = self.run_id + "-" + label + "-" + secrets.token_hex(8)
        body = secrets.token_bytes(BODY_SIZE)
        path = self.out / (identity + ".bin")
        with path.open("xb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        message = {"message_id": identity, "body": base64.b64encode(body).decode(),
                   "subject": "disk full " + label, "group_id": identity,
                   "content_type": "application/octet-stream", "correlation_id": self.run_id,
                   "reply_to": "disk-full-replies", "properties": {"run": self.run_id, "label": label}}
        self.expected[identity] = message
        self.record("expected.jsonl", {"message": {k: v for k, v in message.items() if k != "body"},
                                       "body_file": path.name, "body_sha256": hashlib.sha256(body).hexdigest(),
                                       "body_bytes": len(body)})
        return message

    def send(self, message):
        sent = self.rpc("Queue", "Send", {"queue": self.queue, "messages": [message]})
        seqs = sent.get("seq_numbers")
        check(isinstance(seqs, list) and len(seqs) == 1 and isinstance(seqs[0], int) and seqs[0] > 0,
              f"send did not acknowledge one positive sequence: {sent}")
        identity = message["message_id"]
        check(identity not in self.acks and seqs[0] not in self.acks.values(), "duplicate acknowledgement")
        self.acks[identity] = seqs[0]
        self.record("acknowledged.jsonl", {"message_id": identity, "seq_number": seqs[0]})

    def verify_wire(self, actual, deliveries):
        identity = actual.get("message_id")
        check(identity in self.acks, f"unacknowledged/extra message received: {identity}")
        expected = self.expected[identity]
        check(all(actual.get(key) == value for key, value in expected.items()),
              f"full body or metadata mismatch: {identity}")
        check(actual.get("seq_number") == self.acks[identity]
              and actual.get("delivery_count") == deliveries and actual.get("lock_token")
              and actual.get("locked_until_ms", 0) > actual.get("enqueued_at_ms", 0) > 0,
              f"invalid delivery metadata: {identity}")
        self.record("observed.jsonl", {"message_id": identity, "seq_number": actual["seq_number"],
                                      "delivery_count": deliveries, "lock_token": actual["lock_token"],
                                      "body_sha256": hashlib.sha256(base64.b64decode(actual["body"])).hexdigest()})

    def copy_database(self, name):
        check(not self.broker_live, "cannot copy a live SQLite directory")
        directory = self.out / name
        directory.mkdir()
        # Docker's archive/cp endpoint need not expose tmpfs mounts. Read the
        # closed directory from inside the still-running holder instead.
        archive_path = self.out / (name + ".tar")
        command = [self.args.docker, "exec", self.container, "tar", "-C", "/fault/db", "-cf", "-", "."]
        with archive_path.open("xb") as stream:
            result = subprocess.run(command, stdout=stream, stderr=subprocess.PIPE, timeout=30)
            stream.flush()
            os.fsync(stream.fileno())
        self.record("commands.jsonl", {"argv": command, "returncode": result.returncode,
                                      "stdout_file": archive_path.name, "stderr": result.stderr.decode()})
        check(result.returncode == 0, "closed DB directory copy failed")
        with tarfile.open(archive_path) as archive:
            members = archive.getmembers()
            check(sum(member.size for member in members) <= 64 * 1024 * 1024, "snapshot exceeded tmpfs bound")
            for member in members:
                if member.isdir() and member.name == ".":
                    continue
                check(member.isfile() and member.name in
                      ("./mq.db", "./mq.db-wal", "./mq.db-shm", "./mq.db.lock"),
                      "unexpected entry in closed DB directory: " + member.name)
                with archive.extractfile(member) as source:
                    (directory / Path(member.name).name).write_bytes(source.read())
        return directory

    def snapshot(self, name, locked=None, drained=False):
        directory = self.copy_database(name)
        hashes = {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                  for path in directory.iterdir() if path.is_file()}
        db_path = directory / "mq.db"
        query = "?mode=ro"
        if not (directory / "mq.db-wal").exists():
            # A clean close checkpoints and removes sidecars. This immutable,
            # closed copy needs no new WAL/SHM; never ignore an existing WAL.
            query += "&immutable=1"
        with contextlib.closing(sqlite3.connect(db_path.as_uri() + query, uri=True)) as db:
            db.execute("PRAGMA query_only=ON")
            check(db.execute("PRAGMA integrity_check").fetchall() == [("ok",)], "integrity_check failed")
            check(db.execute("PRAGMA foreign_key_check").fetchall() == [], "foreign_key_check failed")
            db.row_factory = sqlite3.Row
            rows = db.execute("SELECT * FROM messages ORDER BY id").fetchall()
            check({row["message_id"] for row in rows} == (set() if drained else set(self.acks))
                  and len(rows) == (0 if drained else len(self.acks)), "offline missing/extra/duplicate rows")
            for row in rows:
                expected = self.expected[row["message_id"]]
                check(row["queue"] == self.queue and row["id"] == self.acks[row["message_id"]]
                      and row["body"] == base64.b64decode(expected["body"]), "offline identity/full body mismatch")
                for key in ("message_id", "subject", "group_id", "content_type", "correlation_id", "reply_to"):
                    check(row[key] == expected[key], "offline metadata mismatch: " + key)
                check(json.loads(row["properties"]) == expected["properties"], "offline properties mismatch")
                is_locked = locked and row["message_id"] == locked["message_id"]
                check(row["state"] == ("locked" if is_locked else "active")
                      and row["delivery_count"] == (1 if is_locked else 0), "offline state/count mismatch")
                if is_locked:
                    check(row["lock_token"] == locked["lock_token"], "offline lock token mismatch")
        # Read-only verification may update a copied SHM index, never the DB or WAL.
        for filename, digest in hashes.items():
            if filename.endswith((".db", "-wal")):
                check(hashlib.sha256((directory / filename).read_bytes()).hexdigest() == digest,
                      "read-only verification changed " + filename)
        atomic_json(self.out / (name + ".json"), {"files_sha256": hashes, "rows": len(rows),
                    "integrity_check": "ok", "foreign_key_check": [], "full_content": "PASS"})

    def run(self):
        image = json.loads(self.docker("image", "inspect", self.args.image).stdout)[0]
        labels = image["Config"].get("Labels") or {}
        check(image["Os"] == "linux" and image["Architecture"] == "amd64", "image is not linux/amd64")
        for key, value in {"source": "https://github.com/mqlitehq/mqlite", "version": self.args.expected_version,
                           "revision": self.args.expected_revision}.items():
            check(labels.get("org.opencontainers.image." + key) == value, "OCI identity mismatch: " + key)
        self.result.update(image_id=image["Id"], image_labels=labels, image_digests=image.get("RepoDigests", []))
        self.docker("run", "-d", "--platform", "linux/amd64", "--pull", "never", "--name", self.container,
                    "--read-only", "--memory=256m", "--memory-swap=256m", "--cpus=1", "--pids-limit=64",
                    "--tmpfs", "/fault:rw,nosuid,noexec,size=67108864",
                    "--tmpfs", "/tmp:rw,nosuid,noexec,size=8388608", "--tmpfs", "/data:rw,size=1048576",
                    "--publish", "127.0.0.1::6754", "--log-opt", "max-size=2m", "--log-opt", "max-file=2",
                    "-e", "MQLITE_TOKENS", "-e", "MQLITE_SYNC=FULL", "-e", "MQLITE_DB=file:/fault/db/mq.db",
                    "-e", "MQLITE_UI=off", "--entrypoint", "sh", image["Id"],
                    "-c", "while :; do sleep 60; done")
        self.live = True
        self.initial_state = self.inspect()["state"]
        port = self.docker("port", self.container, "6754/tcp").stdout.strip()
        check(re.fullmatch(r"127\.0\.0\.1:\d+", port), "published endpoint is not loopback")
        self.endpoint = "http://" + port
        self.result["endpoint"] = self.endpoint
        version = self.docker("exec", self.container, "mqlite", "version").stdout.strip()
        check(version == "mqlite " + self.args.expected_version.partition("-rc.")[0], "binary version mismatch")
        self.result["binary_version"] = version
        self.result["binary_sha256"] = self.shell("sha256sum /usr/local/bin/mqlite").stdout.split()[0]
        self.shell("mkdir -p /fault/db; printf %s " + self.run_id + " > /fault/identity")
        self.start_broker()
        unauthenticated, _ = self.request("/mqlite.v1.AdminService/Status", {}, token=False)
        check(unauthenticated == 401, "broker accepted an unauthenticated RPC")
        self.rpc("Admin", "CreateQueue", {"name": self.queue, "config": {"lock_duration_ms": 300000}})
        for index in range(4):
            self.send(self.message("before-full-" + str(index)))
        locked = self.rpc("Queue", "Receive", {"queue": self.queue, "max_messages": 1})["messages"][0]
        self.verify_wire(locked, 1)
        denied_http, denied_cli = self.message("denied-http"), self.message("denied-cli")
        # docker cp into a read-only-root container can reject even a tmpfs path.
        self.docker("exec", "-i", self.container, "sh", "-c", "base64 -d >/tmp/denied.bin",
                    input_text=denied_cli["body"])
        check(self.shell("sha256sum /tmp/denied.bin").stdout.split()[0]
              == hashlib.sha256(base64.b64decode(denied_cli["body"])).hexdigest(),
              "CLI input file differs from the independent expected body")
        self.logs("before-full")
        self.record("fault.jsonl", {"event": "fill-start"})
        fill = self.shell("dd if=/dev/zero of=/fault/filler bs=1048576", required=False)
        check(fill.returncode != 0 and FULL.search(fill.stderr), "filler did not reach ENOSPC")
        space = self.shell("df -Pk /fault").stdout
        check(int(space.splitlines()[-1].split()[3]) == 0, "fault filesystem still has free blocks")
        status, raw = self.request("/mqlite.v1.QueueService/Send", {"queue": self.queue, "messages": [denied_http]})
        self.record("fault.jsonl", {"event": "http-send", "status": status, "response": raw.decode()})
        check(status >= 400 and FULL.search(raw.decode()) and "seq_numbers" not in json.loads(raw),
              "full disk send did not return an explicit failure without acknowledgement")
        cli = self.docker("exec", "-e", "MQLITE_TOKEN", "-e", "MQLITE_ENDPOINT=http://127.0.0.1:6754",
                          self.container, "mqlite", "send", self.queue, "--file", "/tmp/denied.bin",
                          "--message-id", denied_cli["message_id"], "--output", "json", required=False)
        check(cli.returncode != 0 and FULL.search(cli.stderr) and not cli.stdout.strip(),
              "full disk CLI send did not exit nonzero with a disk-full error")
        self.result["full_http_status"], self.result["full_cli_exit"] = status, cli.returncode
        time.sleep(1.2)
        self.logs("disk-full", allow_full=True)
        self.inspect()
        self.shell("rm /fault/filler")
        self.record("fault.jsonl", {"event": "space-recovered"})
        self.send(self.message("after-space-recovery"))
        self.logs("after-space-recovery")
        self.stop_broker("KILL")
        self.snapshot("retained-before-restart", locked=locked)
        self.start_broker()
        self.send(self.message("after-process-restart"))
        self.logs("after-process-restart")
        observed = set()
        for _ in range(len(self.acks)):
            messages = self.rpc("Queue", "Receive", {"queue": self.queue, "max_messages": 1}).get("messages", [])
            check(len(messages) == 1, "missing message while draining")
            message = messages[0]
            identity = message.get("message_id")
            check(identity not in observed, "unexpected duplicate delivery")
            self.verify_wire(message, 2 if identity == locked["message_id"] else 1)
            if identity == locked["message_id"]:
                check(message["lock_token"] != locked["lock_token"], "restart reused a stale lock token")
            complete = self.rpc("Queue", "Complete", {"queue": self.queue, "seq_number": message["seq_number"],
                                                       "lock_token": message["lock_token"]})
            check(complete.get("ok") is True, "completion was not acknowledged")
            observed.add(identity)
        check(observed == set(self.acks), "acknowledged and consumed identity sets differ")
        check(not self.rpc("Queue", "Receive", {"queue": self.queue, "max_messages": 1}).get("messages"),
              "unexpected extra delivery after drain")
        for state in STATES:
            check(not self.rpc("Queue", "Peek", {"queue": self.queue, "state": state, "max": 100}).get("messages"),
                  "unexpected residual message in " + state)
        stats = self.rpc("Queue", "Stats", {"queue": self.queue})
        check(all(stats.get(state) == 0 for state in (*STATES, "total")), "nonempty final stats")
        self.logs("final-drain")
        self.stop_broker()
        self.snapshot("drained-final", drained=True)
        final = self.inspect()["state"]
        check(final["Pid"] == self.initial_state["Pid"] and final["StartedAt"] == self.initial_state["StartedAt"],
              "holder process changed; tmpfs lifetime was not preserved")
        self.result.update(acknowledged=len(self.acks), consumed=len(observed), denied=2,
                           final_stats=stats, container_oom_killed=False, broker_restarts=1,
                           holder_restarts=0, retained_snapshot="retained-before-restart", final_snapshot="drained-final")
        self.docker("rm", "-f", "-v", self.container)
        self.live = False
        self.result["container_preserved"] = False

    def finish(self, error=None):
        if error is not None:
            self.result["error"] = str(error)
            self.result["container_preserved"] = self.live
            if self.live:
                try:
                    self.inspect()
                    (self.out / "broker.log").write_text(self.shell("cat /tmp/broker.log", required=False).stdout)
                    if self.broker_live:
                        self.stop_broker("KILL")
                    self.copy_database("failure-snapshot")
                except Exception as capture_error:
                    self.result["capture_error"] = str(capture_error)
        self.result.update(status="FAIL" if error else "PASS", ended_at=now(),
                           elapsed_seconds=round(time.monotonic() - self.started, 3))
        atomic_json(self.out / "result.json", self.result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("image", help="local linux/amd64 image; no pulling or publishing")
    parser.add_argument("--expected-revision", required=True, help="exact 40-character OCI commit SHA")
    parser.add_argument("--expected-version", required=True, help="exact OCI X.Y.Z or X.Y.Z-rc.N")
    parser.add_argument("--output", required=True, type=Path, help="new evidence directory (must not exist)")
    parser.add_argument("--docker", default="docker", help="Docker executable")
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.expected_revision):
        parser.error("--expected-revision must be a full lowercase commit SHA")
    if not re.fullmatch(r"\d+\.\d+\.\d+(?:-rc\.[1-9]\d*)?", args.expected_version):
        parser.error("--expected-version must be X.Y.Z or X.Y.Z-rc.N")
    drill = None
    try:
        drill = Drill(args)
        drill.run()
        drill.finish()
    except BaseException as error:
        if drill:
            drill.finish(error)
        print(f"FAIL disk-full drill: {error}", file=sys.stderr)
        if drill and drill.live:
            print(f"Preserved container: {drill.container}; evidence: {drill.out}", file=sys.stderr)
        return 1
    print(f"PASS disk-full drill: {drill.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
