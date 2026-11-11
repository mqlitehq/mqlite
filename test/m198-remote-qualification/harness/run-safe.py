#!/usr/bin/env python3
"""Run the bounded compatibility process without publishing opaque child output."""
import hashlib
import json
import os
import platform
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import tempfile
import time


def group_exists(pgid):
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        if platform.system() != "Darwin":
            raise
        # Darwin can retain an empty process-group identity briefly after its
        # last member exits. Confirm absence through the process inventory.
        groups = subprocess.check_output(
            ["/bin/ps", "-A", "-o", "pgid="], timeout=2).split()
        if str(pgid).encode() in groups:
            raise
        return False


def stop_owned_group(proc):
    """Reap the leader and remove its dedicated POSIX group, even after leader exit."""
    pgid = proc.pid
    if pgid <= 1 or pgid == os.getpgrp():
        raise RuntimeError("Refusing to signal an unowned or controller group")
    existed, last_error = True, None
    try:
        try:
            os.killpg(pgid, signal.SIGTERM)
        except ProcessLookupError:
            existed = False
        except PermissionError as error:
            last_error = error
        if existed:
            time.sleep(0.2)  # The leader's wait cannot provide descendants' grace time.
    finally:
        # A controller interruption during TERM grace must still kill/reap the
        # dedicated group; a reaped leader is not proof its descendants exited.
        proc.poll()
        if existed:
            for attempt in range(3):
                try:
                    os.killpg(pgid, signal.SIGKILL)
                    last_error = None
                    break
                except ProcessLookupError:
                    last_error = None
                    break
                except PermissionError as error:
                    last_error = error
                    proc.poll()
                    if attempt < 2:
                        time.sleep(0.05)
        try:
            proc.wait(timeout=0.5)
        except subprocess.TimeoutExpired:
            raise RuntimeError("Owned subprocess leader did not join") from None
        deadline = time.monotonic() + 2
        try:
            while group_exists(pgid) and time.monotonic() < deadline:
                time.sleep(0.02)
            remains = group_exists(pgid)
        except PermissionError:
            remains = True
        if not remains:
            last_error = None
        if last_error is not None or remains:
            raise RuntimeError("Owned subprocess group cleanup was not confirmed") from None
    return {"group_existed_after_wait": existed, "leader_joined": True, "group_gone": True}


def execute(command, timeout, env):
    process = subprocess.Popen(command, env=env, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, start_new_session=True)
    digest = hashlib.sha256()
    count = 0
    timed_out = False
    deadline = time.monotonic() + timeout
    selector = None
    stopped = False
    cleanup = None
    try:
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ)
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                timed_out = True
                stopped = True
                cleanup = stop_owned_group(process)
                break
            for key, _ in selector.select(min(remaining, 1)):
                block = os.read(key.fd, 65536)
                if block:
                    digest.update(block)
                    count += len(block)
                else:
                    selector.unregister(key.fileobj)
        if not timed_out and process.poll() is None:
            try:
                process.wait(timeout=max(0, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                timed_out = True
    finally:
        try:
            if not stopped:
                cleanup = stop_owned_group(process)
        finally:
            if selector is not None:
                selector.close()
            process.stdout.close()
    return {"exit": process.returncode, "timed_out": timed_out, "cleanup": cleanup,
            "opaque_output_bytes": count, "opaque_output_sha256": digest.hexdigest()}


def main():
    output = Path(os.environ["M164_EVIDENCE_DIR"])
    output.mkdir(mode=0o700, parents=True, exist_ok=True)
    evidence = {"scope": "bounded standard-backend compatibility smoke", "stages": []}
    env = dict(os.environ)
    env.update({"GOTOOLCHAIN": "local", "GOMAXPROCS": "2", "GOFLAGS": "",
                "GOENV": "off", "GOWORK": "off"})
    env.pop("MQLITE_TEST_DB_RESET_CONFIRM", None)
    try:
        with tempfile.TemporaryDirectory(prefix="m164-compat-") as folder:
            binary = Path(folder) / "compat"
            build_env = {key: value for key, value in env.items() if not key.startswith("MQLITE_")}
            built = execute(["go", "build", "-p", "2", "-trimpath", "-o", str(binary), "."], 120, build_env)
            evidence["stages"].append({"stage": "build", **built})
            if built["exit"] != 0 or built["timed_out"]:
                return 1
            evidence["binary_sha256"] = hashlib.sha256(binary.read_bytes()).hexdigest()
            ran = execute([str(binary)], 310, env)
            evidence["stages"].append({"stage": "run", **ran})
            if ran["exit"] != 0 or ran["timed_out"]:
                return 1
            result = json.loads((output / "evidence.json").read_text())
            return 0 if result.get("pass") is True else 1
    finally:
        (output / "supervisor.json").write_text(json.dumps(evidence, indent=2) + "\n")


if __name__ == "__main__":
    try:
        code = main()
    except BaseException:
        # Unexpected compiler/provider/runtime output is hashed in memory only.
        # Never forward an exception, child stderr, token, URL, SQL or panic text.
        code = 1
    print("Compatibility supervisor completed." if code == 0 else "Compatibility supervisor failed; inspect sanitized evidence.")
    sys.exit(code)
