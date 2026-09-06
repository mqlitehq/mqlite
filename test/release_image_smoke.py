#!/usr/bin/env python3
"""Smoke-test a built linux/amd64 Docker image using only the Python stdlib.

Usage: python3 test/release_image_smoke.py IMAGE [--expected-version X.Y.Z[-rc.N]]
       [--expected-revision COMMIT_SHA]
The OCI version must match exactly; RC binaries report the base X.Y.Z version.
Uses an ephemeral loopback port and removes its containers and named volume.
"""
import argparse
import base64
import json
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def image_version(value):
    if not re.fullmatch(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-rc\.([1-9][0-9]*))?", value):
        raise argparse.ArgumentTypeError("expected X.Y.Z or X.Y.Z-rc.N (N >= 1, no leading zeroes)")
    return value


def smoke(args):
    run_id = "mqlite-image-smoke-" + uuid.uuid4().hex
    volume = run_id + "-data"
    token = "mqk_" + uuid.uuid4().hex
    containers = [run_id + "-first", run_id + "-restart"]
    live_containers = set()
    volume_created = False
    endpoint = ""
    # Local broker traffic must not depend on a runner's HTTP proxy settings.
    http = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def docker(*command, required=True):
        result = subprocess.run(
            [args.docker, *command], capture_output=True, text=True, timeout=60
        )
        if required and result.returncode != 0:
            raise RuntimeError(f"docker {command[0]} failed: {result.stderr.strip()}")
        return result

    def request(path, body=None, auth=None, timeout=5):
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(endpoint + path, data=data)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        if auth is not None:
            req.add_header("Authorization", "Bearer " + auth)
        try:
            with http.open(req, timeout=timeout) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            with error:
                return error.code, error.read()

    def rpc(service, method, body):
        path = f"/mqlite.v1.{service}Service/{method}"
        status, raw = request(path, body, token)
        check(status == 200, f"{method}: HTTP {status}: {raw!r}")
        return json.loads(raw)

    def start(container):
        nonlocal endpoint
        live_containers.add(container)
        docker(
            "run", "--detach", "--platform", "linux/amd64", "--pull", "never",
            "--name", container, "--publish", "127.0.0.1::6754",
            "--mount", f"type=volume,source={volume},target=/data",
            "--env", "MQLITE_TOKENS=" + token,
            "--env", "MQLITE_SYNC=FULL", args.image,
        )
        binding = json.loads(docker("inspect", container).stdout)[0]
        ports = binding["NetworkSettings"]["Ports"]["6754/tcp"]
        check(len(ports) == 1 and ports[0]["HostIp"] == "127.0.0.1",
              f"unexpected published ports: {ports!r}")
        endpoint = "http://127.0.0.1:" + ports[0]["HostPort"]
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                status, raw = request("/healthz", timeout=2)
                if status == 200 and raw.strip() == b"ok":
                    break
            except (OSError, urllib.error.URLError):
                pass
            time.sleep(0.25)
        else:
            raise RuntimeError("broker did not become healthy within 30 seconds")

        status, raw = request("/")
        check(status == 200, f"discovery: HTTP {status}")
        card = json.loads(raw)
        check(card.get("name") == "mqlite" and card.get("status") == "ok"
              and card.get("auth") == "bearer" and card.get("health") == "/healthz",
              f"unexpected discovery metadata: {card!r}")
        version = card.get("version", "")
        check(re.fullmatch(r"\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?", version),
              f"invalid image version: {version!r}")
        if args.expected_version is not None:
            expected_binary_version = args.expected_version.partition("-rc.")[0]
            check(version == expected_binary_version,
                  f"binary version {version!r} != expected {expected_binary_version!r}")
        cli_version = docker("exec", container, "mqlite", "version").stdout.strip()
        check(cli_version == "mqlite " + version,
              f"CLI/discovery version mismatch: {cli_version!r}, {version!r}")

        paths = card.get("endpoints", [])
        check(isinstance(paths, list) and paths, "discovery has no RPC endpoints")
        # Check every advertised RPC and metrics with both absent and wrong tokens.
        for path in [*paths, "/metrics"]:
            for auth in (None, "invalid-" + token):
                status, raw = request(path, None if path == "/metrics" else {}, auth)
                check(status == 401 and json.loads(raw).get("code") == "unauthenticated",
                      f"{path} accepted absent/invalid credentials: HTTP {status}")

        runtime = rpc("Admin", "Status", {})
        check(runtime.get("version") == version and runtime.get("auth") is True
              and runtime.get("backend") == "local file" and runtime.get("remote") is False
              and runtime.get("location") == "/data/mq.db"
              and isinstance(runtime.get("schema_version"), str) and runtime["schema_version"]
              and runtime.get("ping_ms", -1) >= 0 and runtime.get("db_size_bytes", 0) > 0,
              f"unexpected runtime metadata: {runtime!r}")
        print(f"PASS {container}: health, version {version}, authentication, local storage", flush=True)
        return runtime

    queue = "release-smoke"

    def message(label):
        return {
            "message_id": label,
            "correlation_id": "correlation-" + label,
            "reply_to": "replies",
            "group_id": "group-" + label,
            "content_type": "application/octet-stream",
            "subject": "release image smoke",
            "properties": {"test": "release-image", "label": label},
            "body": base64.b64encode(b"release smoke\x00\xff\n" + label.encode()).decode(),
        }

    def receive(count):
        result = rpc("Queue", "Receive", {"queue": queue, "max_messages": max(count, 1)})
        messages = result.get("messages")
        check(isinstance(messages, list) and len(messages) == count,
              f"expected {count} received messages: {result!r}")
        return messages

    def verify_message(actual, expected, seq, deliveries):
        check({key: actual.get(key) for key in expected} == expected,
              f"message body or metadata changed: {actual!r}")
        check(actual.get("seq_number") == seq and actual.get("delivery_count") == deliveries
              and actual.get("lock_token")
              and actual.get("enqueued_at_ms", 0) > 0
              and actual.get("locked_until_ms", 0) > actual["enqueued_at_ms"],
              f"invalid received message state: {actual!r}")

    def complete(message):
        result = rpc("Queue", "Complete", {
            "queue": queue, "seq_number": message["seq_number"],
            "lock_token": message["lock_token"],
        })
        check(result.get("ok") is True, f"complete failed: {result!r}")

    def drained():
        receive(0)
        stats = rpc("Queue", "Stats", {"queue": queue})
        states = ("active", "locked", "deferred", "scheduled", "dead_lettered", "total")
        check(all(stats.get(state) == 0 for state in states), f"queue not drained: {stats!r}")

    try:
        image = json.loads(docker("image", "inspect", args.image).stdout)[0]
        check(image["Os"] == "linux" and image["Architecture"] == "amd64",
              "smoke image must be built for linux/amd64")
        labels = image["Config"].get("Labels") or {}
        expected_labels = {
            "source": "https://github.com/mqlitehq/mqlite",
            "version": args.expected_version,
            "revision": args.expected_revision,
        }
        for name, expected in expected_labels.items():
            if expected is not None:
                actual = labels.get("org.opencontainers.image." + name)
                check(actual == expected,
                      f"OCI {name} {actual!r} != expected {expected!r}")
        print("PASS OCI image identity", flush=True)
        docker("volume", "create", volume)
        volume_created = True
        initial = start(containers[0])
        created = rpc("Admin", "CreateQueue", {
            "name": queue, "config": {"lock_duration_ms": 300000},
        })
        check(created == {}, f"unexpected create queue result: {created!r}")
        first = message("complete-before-restart")
        sent = rpc("Queue", "Send", {"queue": queue, "messages": [first]})["seq_numbers"]
        check(len(sent) == 1 and sent[0] > 0, f"invalid send result: {sent!r}")
        received = receive(1)[0]
        verify_message(received, first, sent[0], 1)
        complete(received)
        drained()
        print("PASS authenticated create/send/receive/complete and empty queue", flush=True)

        pending = [message("locked-before-restart"), message("active-before-restart")]
        seqs = rpc("Queue", "Send", {"queue": queue, "messages": pending})["seq_numbers"]
        check(len(seqs) == 2 and seqs[0] > sent[0] and seqs[1] > seqs[0],
              f"invalid pending send result: {seqs!r}")
        locked = receive(1)[0]
        verify_message(locked, pending[0], seqs[0], 1)
        stats = rpc("Queue", "Stats", {"queue": queue})
        check(stats.get("locked") == 1 and stats.get("active") == 1 and stats.get("total") == 2,
              f"unexpected state before restart: {stats!r}")
        docker("stop", "--time", "10", containers[0])
        docker("rm", containers[0])
        live_containers.remove(containers[0])
        restarted = start(containers[1])
        check(restarted["schema_version"] == initial["schema_version"]
              and restarted.get("queues") == 1, "queue/schema metadata did not survive restart")
        recovered = receive(2)
        for index, actual in enumerate(recovered):
            verify_message(actual, pending[index], seqs[index], 2 if index == 0 else 1)
            if index == 0:
                check(actual["lock_token"] != locked["lock_token"], "restart reused the old lock token")
                check(actual["enqueued_at_ms"] == locked["enqueued_at_ms"], "restart changed enqueue time")
            complete(actual)
        drained()
        print("PASS same-volume container replacement: active and locked messages survive and settle", flush=True)
    except BaseException:
        for container in sorted(live_containers):
            logs = docker("logs", "--tail", "100", container, required=False)
            if logs.returncode == 0:
                print(f"{container} logs:\n{logs.stdout}{logs.stderr}", file=sys.stderr)
        raise
    finally:
        cleanup = [("rm", "--force", name) for name in sorted(live_containers)]
        if volume_created:
            cleanup.append(("volume", "rm", "--force", volume))
        failures = []
        for command in cleanup:
            try:
                result = docker(*command, required=False)
                if result.returncode != 0:
                    failures.append(result.stderr.strip())
            except (OSError, subprocess.TimeoutExpired) as error:
                failures.append(str(error))
        if failures:
            raise RuntimeError("Docker cleanup failed: " + "; ".join(failures))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("image", help="local linux/amd64 image to test")
    parser.add_argument("--expected-version", type=image_version, metavar="X.Y.Z[-rc.N]",
                        help="exact OCI version; binary/discovery must report its base X.Y.Z")
    parser.add_argument("--expected-revision", help="require this OCI revision (commit SHA)")
    parser.add_argument("--docker", default="docker", help="Docker executable (default: docker)")
    args = parser.parse_args()
    try:
        smoke(args)
    except (RuntimeError, OSError, ValueError, KeyError, subprocess.TimeoutExpired) as error:
        print(f"FAIL release image smoke: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
