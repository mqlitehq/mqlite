#!/usr/bin/env python3
"""Supervise an isolated candidate broker and mixed-load verifier; never publish."""
import argparse
from collections import deque
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import stat
import statistics
import subprocess
import sys
import threading
import time
import uuid

MIB = 1024 * 1024
GIB = 1024 * MIB


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def save(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")
    temporary.replace(path)


def tree_size(path):
    def walk_error(error):
        if not isinstance(error, FileNotFoundError):
            raise error

    total = 0
    for root, _, files in os.walk(path, onerror=walk_error):
        for name in files:
            try:
                info = os.stat(Path(root) / name)
            except FileNotFoundError:
                # Pending batches disappear and JSON files are replaced atomically.
                continue
            if stat.S_ISREG(info.st_mode):
                total += info.st_size
    return total


def file_hash(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(MIB), b""):
            h.update(block)
    return h.hexdigest()


class Logs:
    """Continuously consume Docker logs so rotation cannot erase an error verdict."""
    def __init__(self, docker, name, output):
        self.lines = 0
        self.errors = 0
        self.samples = []
        self.digest = hashlib.sha256()
        self.failure = None
        self.lock = threading.Lock()
        self.output = output
        self.proc = subprocess.Popen([docker, "logs", "--follow", "--timestamps", name],
                                     stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        self.thread = threading.Thread(target=self.read, daemon=True)
        self.thread.start()

    def read(self):
        try:
            for raw in iter(lambda: self.proc.stdout.readline(MIB + 1), b""):
                require(len(raw) <= MIB, "unexpected oversized log line")
                line = raw.decode("utf-8", errors="replace")
                with self.lock:
                    self.lines += 1
                    self.digest.update(raw)
                    if re.search(r'\b(?:ERRO|ERROR|FATAL|PANIC)\b|level=error\b|"level"\s*:\s*"error"|panic:|fatal error:',
                                 line, re.IGNORECASE):
                        self.errors += 1
                        if len(self.samples) < 20:
                            self.samples.append(line.rstrip())
                        save(self.output, self.report_unlocked())
        except BaseException as exc:
            self.failure = str(exc)
        finally:
            self.proc.stdout.close()

    def report_unlocked(self):
        return {"lines": self.lines, "errors": self.errors, "first_errors": self.samples,
                "sha256": self.digest.hexdigest(), "reader_failure": self.failure,
                "reader_exit_code": self.proc.poll()}

    def report(self):
        with self.lock:
            return self.report_unlocked()

    def finish(self):
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.terminate()
            self.proc.wait(timeout=10)
        self.thread.join(timeout=10)
        require(not self.thread.is_alive(), "log monitor did not stop")
        save(self.output, self.report())


def verify_runner(out, expected_status, duration_seconds, provenance, supervised_seconds):
    """Bind the completed workload verdict and its configuration to this run."""
    result_path = out / "runner" / "result.json"
    require(result_path.is_file(), "runner exited without a result")
    runner = json.loads(result_path.read_text())
    require(runner.get("status") == expected_status, "runner verdict does not match requested duration")
    require(runner.get("production_ready") is (expected_status == "PASS"), "runner production flag mismatch")
    require(runner.get("requested_seconds") == duration_seconds
            and runner.get("validated_activity_seconds") == duration_seconds,
            "runner requested duration or validated activity mismatch")
    elapsed = runner.get("elapsed_seconds")
    require(isinstance(elapsed, (int, float)) and duration_seconds <= elapsed <= supervised_seconds,
            "runner elapsed time is outside supervised workload interval")
    details = json.loads((out / "runner" / "metadata.json").read_text())
    require(details.get("provenance") == provenance
            and json.loads((out / "provenance.json").read_text()) == provenance,
            "runner provenance differs from this candidate run")
    config = details.get("config", {})
    require(config.get("duration_ns") == duration_seconds * 1_000_000_000
            and config.get("endpoint") == "http://broker:6754"
            and config.get("outbox_db") == "/data/outbox.db"
            and config.get("evidence") == "/evidence/runner"
            and config.get("token_env") == "MQLITE_TOKEN", "runner workload configuration mismatch")
    return runner


def execute(args, out):
    def docker(*parts, timeout=45, required=True, env=None):
        result = subprocess.run([args.docker, *map(str, parts)], text=True,
                                capture_output=True, timeout=timeout, env=env)
        if required:
            require(result.returncode == 0, "docker " + parts[0] + ": " + result.stderr.strip())
        return result

    repo = Path(__file__).resolve().parents[2]
    source_sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()
    dirty = subprocess.check_output(["git", "status", "--porcelain"], cwd=repo, text=True).strip()
    require(source_sha == args.expected_revision, "source HEAD differs from expected revision")
    require(not dirty or (args.allow_dirty and args.duration_seconds < 86400),
            "production acceptance requires a clean source; --allow-dirty is smoke-only")
    require(shutil.disk_usage(out).free >= 10 * GIB, "host free disk below 10 GiB before startup")
    images = {}
    for role, image in (("broker", args.broker_image), ("runner", args.runner_image)):
        info = json.loads(docker("image", "inspect", image).stdout)[0]
        labels = info.get("Config", {}).get("Labels") or {}
        require(info.get("Os") == "linux" and info.get("Architecture") == "amd64",
                role + " image must be linux/amd64")
        for key, value in (("revision", args.expected_revision), ("version", args.expected_version),
                           ("source", "https://github.com/mqlitehq/mqlite")):
            require(labels.get("org.opencontainers.image." + key) == value,
                    role + " image label mismatch: " + key)
        images[role] = {"reference": image, "id": info["Id"], "labels": labels,
                        "repo_digests": info.get("RepoDigests", [])}

    run_id = "mqlite-acceptance-" + uuid.uuid4().hex[:12]
    names = {"broker": run_id + "-broker", "runner": run_id + "-runner"}
    metadata = {
        "run_id": run_id, "source_sha": source_sha, "source_dirty": dirty,
        "version": args.expected_version, "images": images, "containers": names, "network": run_id,
        "duration_seconds": args.duration_seconds, "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "supervisor_pid": os.getpid(), "supervisor_sha256": file_hash(Path(__file__)),
        "budgets": {"memory_bytes_each": 512 * MIB, "cpus_each": 1,
                    "db_wal_bytes_each": 512 * MIB, "run_artifacts_bytes": 2 * GIB,
                    "host_free_bytes_min": 10 * GIB, "vm_free_bytes_min": 2 * GIB,
                    "sample_gap_seconds_max": 120, "anonymous_growth_after_warmup_bytes": 128 * MIB,
                    "warmup_seconds": 1800, "log_bytes_each": 20_000_000},
        "durability": "FULL", "phase": "starting",
    }
    save(out / "metadata.json", metadata)
    (out / "broker-data").mkdir()
    (out / "outbox-data").mkdir()
    network_created, started, monitors = False, [], {}
    token = "mqk_" + uuid.uuid4().hex
    env = dict(os.environ, MQLITE_TOKENS=token, MQLITE_TOKEN=token)
    begin = time.monotonic()
    previous = begin
    max_gap = 0
    warmup = {name: deque(maxlen=20) for name in names}
    baseline, peaks = {}, {}
    report = None

    def inspect(role):
        raw = json.loads(docker("inspect", names[role]).stdout)[0]
        state = raw["State"]
        # Keep only non-secret runtime state, never Config.Env.
        return {"state": state, "restart_count": raw["RestartCount"]}

    def collect(role):
        mem = docker("exec", names[role], "cat", "/sys/fs/cgroup/memory.stat").stdout
        memory = {key: int(value) for key, value in (ln.split() for ln in mem.splitlines())}
        free = int(docker("exec", names[role], "df", "-Pk", "/").stdout.splitlines()[-1].split()[3]) * 1024
        data = out / ("broker-data" if role == "broker" else "outbox-data")
        size = tree_size(data)
        return {"anonymous_bytes": memory["anon"], "file_cache_bytes": memory["file"],
                "data_directory_bytes": size, "vm_free_bytes": free}

    try:
        docker("network", "create", "--internal", run_id)
        network_created = True
        limits = ["--detach", "--platform", "linux/amd64", "--pull", "never", "--init",
                  "--user", str(os.getuid()) + ":" + str(os.getgid()),
                  "--memory=512m", "--memory-swap=512m", "--cpus=1", "--pids-limit=128",
                  "--log-opt", "max-size=10m", "--log-opt", "max-file=2",
                  "--network", run_id]
        # Register the unique name before a Docker RPC can time out after creation.
        started.append("broker")
        docker("run", *limits, "--name", names["broker"], "--network-alias", "broker",
               "--mount", "type=bind,src=" + str(out / "broker-data") + ",dst=/data",
               "--env", "MQLITE_TOKENS", "--env", "MQLITE_SYNC=FULL",
               images["broker"]["id"], env=env)
        monitors["broker"] = Logs(args.docker, names["broker"], out / "broker-log-summary.json")
        def status():
            response = docker("exec", "--env", "MQLITE_ENDPOINT=http://127.0.0.1:6754",
                              "--env", "MQLITE_TOKEN", names["broker"], "mqlite", "status",
                              "--output", "json", env=env, required=False)
            return json.loads(response.stdout) if response.returncode == 0 else {}

        def scrape_metrics():
            metrics = docker("exec", "--env", "MQLITE_TOKEN", names["broker"], "sh", "-c",
                             'wget -q -O - --header "Authorization: Bearer $MQLITE_TOKEN" '
                             'http://127.0.0.1:6754/metrics', env=env).stdout
            require("mqlite_rpc_duration_seconds_bucket" in metrics, "RPC histogram scrape missing")
            return metrics

        deadline = time.monotonic() + 45
        while True:
            try:
                state = status()
                if state.get("ping_ms", -1) >= 0:
                    break
            except (OSError, json.JSONDecodeError):
                pass
            require(time.monotonic() < deadline, "authenticated storage readiness timed out")
            time.sleep(0.25)
        require(state.get("version") == args.expected_version and state.get("schema_version") == "5"
                and state.get("backend") == "local file" and state.get("auth") is True,
                "unexpected broker version/schema/storage/auth")
        metadata["status_at_start"] = state
        metadata["broker_binary_sha256"] = docker("exec", names["broker"], "sha256sum", "/usr/local/bin/mqlite").stdout.split()[0]
        metadata["runner_binary_sha256"] = docker("run", "--rm", "--platform", "linux/amd64", "--pull", "never",
                                                  "--network", "none", "--entrypoint", "sha256sum",
                                                  images["runner"]["id"], "/usr/local/bin/soak").stdout.split()[0]
        metadata["phase"] = "running"
        save(out / "provenance.json", metadata)
        started.append("runner")
        runner_started = time.monotonic()
        docker("run", *limits, "--name", names["runner"],
               "--mount", "type=bind,src=" + str(out / "outbox-data") + ",dst=/data",
               "--mount", "type=bind,src=" + str(out) + ",dst=/evidence",
               "--env", "MQLITE_TOKEN", images["runner"]["id"],
               "--endpoint", "http://broker:6754", "--token-env", "MQLITE_TOKEN",
               "--outbox-db", "/data/outbox.db", "--evidence", "/evidence/runner",
               "--duration", str(args.duration_seconds) + "s",
               "--provenance", "/evidence/provenance.json", env=env)
        monitors["runner"] = Logs(args.docker, names["runner"], out / "runner-log-summary.json")
        save(out / "metadata.json", metadata)
        previous = time.monotonic()
        last_metrics = -60.0
        with (out / "resources.jsonl").open("w", buffering=1) as samples, \
                (out / "prometheus.jsonl").open("w", buffering=1) as histograms:
            while True:
                now = time.monotonic()
                gap = now - previous
                previous = now
                max_gap = max(max_gap, gap)
                require(gap <= 120, "supervisor sampling gap exceeded 120 seconds")
                elapsed = now - begin
                states = {role: inspect(role) for role in names}
                for role, value in states.items():
                    require(not value["state"]["OOMKilled"] and value["restart_count"] == 0,
                            role + " OOM/restart")
                    logs = monitors[role].report()
                    require(logs["errors"] == 0 and logs["reader_failure"] is None,
                            role + " unexpected error log; see log summary")
                    if value["state"]["Running"] and (
                            monitors[role].proc.poll() is not None or not monitors[role].thread.is_alive()):
                        # A normally finishing runner may exit after the first
                        # inspect but before its log stream reaches EOF.
                        value = states[role] = inspect(role)
                    require(not (value["state"]["Running"] and
                                 (monitors[role].proc.poll() is not None or not monitors[role].thread.is_alive())),
                            role + " continuous log reader stopped unexpectedly")
                require(states["broker"]["state"]["Running"], "broker exited during run")
                if not states["runner"]["state"]["Running"]:
                    require(states["runner"]["state"]["ExitCode"] == 0, "runner exited nonzero")
                    # The runner cannot exec after exit. The broker shares the
                    # same VM filesystem and is still running at this boundary.
                    require(collect("broker")["vm_free_bytes"] >= 2 * GIB, "final VM free-space budget")
                    (out / "prometheus-final.txt").write_text(scrape_metrics())
                    break
                metrics = {}
                for role in names:
                    try:
                        metrics[role] = collect(role)
                    except RuntimeError as exc:
                        # docker exec can race with a successful workload exit.
                        # Re-enter the terminal branch, which checks its exit,
                        # OOM, result and final storage, without requiring exec.
                        if role != "runner" or not str(exc).startswith("docker exec:"):
                            raise
                        value = inspect(role)
                        if value["state"]["Running"]:
                            raise
                        break
                if len(metrics) != len(names):
                    continue
                for role, metric in metrics.items():
                    anon = metric["anonymous_bytes"]
                    peaks[role] = max(peaks.get(role, 0), anon)
                    require(metric["data_directory_bytes"] <= 512 * MIB, role + " DB/WAL budget exceeded")
                    require(metric["vm_free_bytes"] >= 2 * GIB, "VM filesystem free space below 2 GiB")
                    if elapsed < 1800:
                        warmup[role].append(anon)
                    else:
                        if role not in baseline:
                            require(bool(warmup[role]), "warmup samples missing")
                            baseline[role] = statistics.median(warmup[role])
                        require(anon <= baseline[role] + 128 * MIB,
                                role + " anonymous memory grew >128 MiB after warmup")
                free = shutil.disk_usage(out).free
                size = tree_size(out)
                require(free >= 10 * GIB and size <= 2 * GIB, "host free space or evidence budget exceeded")
                sample = {"elapsed_seconds": elapsed, "time_unix": time.time(), "roles": metrics,
                          "host_free_bytes": free, "run_bytes": size,
                          "logs": {role: monitor.report() for role, monitor in monitors.items()}}
                samples.write(json.dumps(sample, sort_keys=True) + "\n")
                if elapsed - last_metrics >= 60:
                    histograms.write(json.dumps({"elapsed_seconds": elapsed, "time_unix": time.time(),
                                                 "metrics": scrape_metrics()}, sort_keys=True) + "\n")
                    last_metrics = elapsed
                save(out / "status.json", {"status": "RUNNING", "sample": sample,
                                         "containers": names, "max_sample_gap_seconds": max_gap})
                require(elapsed <= args.duration_seconds + 180, "runner exceeded bounded final drain deadline")
                time.sleep(10)
        expected_status = "PASS" if args.duration_seconds >= 86400 else "SMOKE"
        runner = verify_runner(out, expected_status, args.duration_seconds,
                               metadata, time.monotonic() - runner_started)
        report = {"status": expected_status, "production_ready": expected_status == "PASS",
                  "elapsed_seconds": time.monotonic() - begin, "runner": runner,
                  "anonymous_peak_bytes": peaks, "warmup_baseline_bytes": baseline,
                  "max_sample_gap_seconds": max_gap, "source_sha": source_sha, "images": images}
    finally:
        # Stop only this run's containers. Keep them and all data for inspection;
        # removal is an explicit later cleanup, not part of the acceptance verdict.
        primary_error = sys.exc_info()[1]
        cleanup_errors = []
        for role in reversed(started):
            try:
                stopped = docker("stop", "--time", "30", names[role], required=False)
                require(stopped.returncode == 0, role + " stop: " + stopped.stderr.strip())
            except Exception as exc:
                cleanup_errors.append(str(exc))
                try:
                    docker("kill", names[role], required=False)
                except Exception as kill_error:
                    cleanup_errors.append(str(kill_error))
        for role, monitor in monitors.items():
            try:
                monitor.finish()
            except Exception as exc:
                cleanup_errors.append(role + " log cleanup: " + str(exc))
        metadata["containers_after_stop"] = {}
        for role in started:
            try:
                state = inspect(role)
                metadata["containers_after_stop"][role] = state
                require(not state["state"]["Running"], role + " still running after cleanup")
            except Exception as exc:
                cleanup_errors.append(role + " final inspect: " + str(exc))
        metadata["cleanup_errors"] = cleanup_errors
        metadata["network_created"] = network_created
        metadata["phase"] = "stopped"
        save(out / "metadata.json", metadata)
        save(out / "logs.json", {role: monitor.report() for role, monitor in monitors.items()})
        if primary_error is None:
            require(not cleanup_errors, "cleanup failed; see metadata.json")
    for role, monitor in monitors.items():
        logs = monitor.report()
        require(logs["errors"] == 0 and logs["reader_failure"] is None, role + " final error log")
        require(logs["reader_exit_code"] == 0, role + " log reader failed")
    for role in started:
        state = metadata["containers_after_stop"][role]
        require(not state["state"]["Running"] and state["state"]["ExitCode"] == 0
                and not state["state"]["OOMKilled"] and state["restart_count"] == 0,
                role + " final state/exit/OOM/restart")
    # The verifier already checked all-state API convergence. Validate stopped
    # physical stores too, including their final complete WAL set.
    from contextlib import closing
    import sqlite3
    for role, filename in (("broker", "broker-data/mq.db"), ("outbox", "outbox-data/outbox.db")):
        db = out / filename
        require(db.is_file(), role + " database missing")
        uri = db.resolve().as_uri() + "?mode=ro"
        wal = Path(str(db) + "-wal")
        if not wal.exists() or wal.stat().st_size == 0:
            uri += "&immutable=1"
        with closing(sqlite3.connect(uri, uri=True)) as conn:
            require(conn.execute("PRAGMA integrity_check").fetchall() == [("ok",)], role + " integrity failed")
            require(not conn.execute("PRAGMA foreign_key_check").fetchall(), role + " foreign-key errors")
            require(conn.execute("SELECT COUNT(*) FROM messages").fetchone() == (0,), role + " retained messages")
            if role == "outbox":
                require(conn.execute("SELECT COUNT(*) FROM soak_business").fetchone() == (0,),
                        "outbox retained business rows")
    report["logs"] = {role: monitor.report() for role, monitor in monitors.items()}
    require(shutil.disk_usage(out).free >= 10 * GIB and tree_size(out) <= 2 * GIB,
            "final host free space or evidence budget exceeded")
    for directory in ("broker-data", "outbox-data"):
        require(tree_size(out / directory) <= 512 * MIB, "final data directory budget: " + directory)
    report["final_database_checks"] = "integrity_check, foreign_key_check, zero messages: both stores; zero outbox business rows"
    report["runner_result_sha256"] = file_hash(out / "runner" / "result.json")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--broker-image", required=True)
    parser.add_argument("--runner-image", required=True)
    parser.add_argument("--expected-revision", required=True)
    parser.add_argument("--expected-version", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--duration-seconds", type=int, default=86400)
    parser.add_argument("--docker", default="docker")
    parser.add_argument("--allow-dirty", action="store_true", help="short smoke only")
    parser.add_argument("--background", action="store_true", help="detach supervisor and preserve a log/PID")
    args = parser.parse_args()
    require(args.duration_seconds >= 30, "duration must be at least 30 seconds")
    require(re.fullmatch(r"[0-9a-f]{40}", args.expected_revision), "expected revision must be a full SHA")
    require(re.fullmatch(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", args.expected_version),
            "expected version must be a plain source version")
    args.output = args.output.resolve()
    require(not args.output.exists(), "output must be a new directory")
    if args.background:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        log = args.output.with_name(args.output.name + ".supervisor.log")
        with log.open("x") as stream:
            proc = subprocess.Popen([sys.executable, "-u", str(Path(__file__).resolve()),
                                     *[value for value in sys.argv[1:] if value != "--background"]],
                                    stdout=stream, stderr=subprocess.STDOUT, start_new_session=True,
                                    stdin=subprocess.DEVNULL)
        if sys.platform == "darwin" and shutil.which("caffeinate"):
            subprocess.Popen(["caffeinate", "-i", "-w", str(proc.pid)],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
        print(json.dumps({"status": "STARTED", "pid": proc.pid, "output": str(args.output), "log": str(log)}))
        return
    args.output.mkdir(parents=True)
    def interrupted(signum, _frame):
        raise RuntimeError("supervisor interrupted by signal " + str(signum))
    signal.signal(signal.SIGTERM, interrupted)
    started = time.time()
    try:
        result = execute(args, args.output)
    except BaseException as exc:
        failed = {"status": "FAIL", "production_ready": False,
                  "error": str(exc), "elapsed_seconds": time.time() - started}
        save(args.output / "result.json", failed)
        save(args.output / "status.json", failed)
        raise
    save(args.output / "result.json", result)
    save(args.output / "status.json", result)
    print(json.dumps({"status": result["status"], "output": str(args.output)}, sort_keys=True))


if __name__ == "__main__":
    main()
