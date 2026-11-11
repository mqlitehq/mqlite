#!/usr/bin/env python3
"""Run the bounded compatibility process without publishing opaque child output."""
import hashlib
import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import tempfile
import time


def stop_owned_group(process):
    """Only the POSIX group created by this Popen belongs to this controller."""
    group_existed = True
    last_error = None
    try:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            group_existed = False
        except PermissionError as error:
            last_error = error
        if group_existed:
            # The direct parent may already be reaped. Its wait cannot supply
            # a grace period for descendants that inherited a pipe.
            time.sleep(0.2)
    finally:
        try:
            process.poll()
            if group_existed:
                for attempt in range(3):
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                        last_error = None
                        break
                    except ProcessLookupError:
                        last_error = None
                        break
                    except PermissionError as error:
                        # Reap a terminated Darwin leader before retrying the
                        # owned group within 100ms. Persistent denial fails.
                        last_error = error
                        process.poll()
                        if attempt < 2:
                            time.sleep(0.05)
        finally:
            try:
                process.wait(timeout=0.5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=0.5)
    if group_existed and last_error is not None:
        raise RuntimeError("owned subprocess group cleanup was not confirmed") from None


def execute(command, timeout, env):
    process = subprocess.Popen(command, env=env, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, start_new_session=True)
    digest = hashlib.sha256()
    count = 0
    timed_out = False
    deadline = time.monotonic() + timeout
    selector = None
    stopped = False
    try:
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ)
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                timed_out = True
                stopped = True
                stop_owned_group(process)
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
                stop_owned_group(process)
        finally:
            if selector is not None:
                selector.close()
            process.stdout.close()
    return {"exit": process.returncode, "timed_out": timed_out,
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
