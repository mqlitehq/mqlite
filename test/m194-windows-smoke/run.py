"""Run only the declared, source-bound Windows Go1.21 smoke diagnostic."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import time
from windows_job import owned_run, controls

ROOT = Path(__file__).resolve().parents[2]
SUPPORT = Path(__file__).resolve().parent

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")

def verify(plan):
    manifest = SUPPORT / "input-manifest.json"
    assert digest(manifest) == os.environ["M194_WINDOWS_DIAG_BUNDLE_SHA256"]
    expected = json.loads(manifest.read_text())
    assert all(digest(ROOT / name) == value for name, value in expected.items())
    allowed = set(expected) | {"test/m194-windows-smoke/input-manifest.json", ".github/workflows/m194-windows-smoke.yml"}
    changed = set(subprocess.check_output(["git", "diff", "--name-only", plan["product_source"], "HEAD"], cwd=ROOT, text=True).splitlines())
    assert changed == allowed
    assert not subprocess.check_output(["git", "status", "--porcelain=v1"], cwd=ROOT).strip()
    entries = subprocess.check_output(["git", "ls-tree", "-r", "-z", plan["product_source"]], cwd=ROOT).split(b"\0")
    preserved = 0
    for entry in entries:
        if not entry:
            continue
        header, raw_name = entry.split(b"\t", 1)
        mode, kind, oid = header.decode().split()
        name = os.fsdecode(raw_name)
        assert kind == "blob" and mode in ("100644", "100755")
        if name == "engine/turso_test.go":
            continue
        raw = (ROOT / name).read_bytes()
        assert hashlib.sha1(b"blob " + str(len(raw)).encode() + b"\0" + raw).hexdigest() == oid
        preserved += 1
    assert preserved == 250
    return {name: digest(ROOT / name) for name in expected}

def run(argv, environment, log, seconds):
    result = owned_run(argv, ROOT, environment, log, seconds)
    result["log_sha256"] = digest(log)
    save(log.with_suffix(".supervisor.json"), result)
    assert result["leader_joined"] and result["owned_tree_gone"] and result["within_inclusive_budget"]
    return result

def audit(events, expected_test, repetitions):
    passed = [e for e in events if e.get("Action") == "pass" and e.get("Test") == expected_test]
    failures = [e.get("Test", e.get("Package")) for e in events if e.get("Action") == "fail"]
    skipped = [e.get("Test", e.get("Package")) for e in events if e.get("Action") == "skip"]
    observations = []
    marker = "WINDOWS_SMOKE_CALL_FAILURE "
    for event in events:
        output = event.get("Output", "")
        if marker in output:
            observations.append(json.loads(output.split(marker, 1)[1].strip()))
    unexpected = sorted({e["Test"] for e in events if "Test" in e and e["Test"] != expected_test})
    return {"passed": len(passed) == repetitions and not failures and not skipped and not unexpected, "target_passes": len(passed), "failed": failures, "skipped": skipped, "unexpected_tests": unexpected, "failure_observations": observations}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    plan = json.loads((SUPPORT / "plan.json").read_text())
    save(output / "plan.json", plan)
    result = {"diagnostic_completed": False, "qualification": False, "commands": [], "at": datetime.datetime.now(datetime.timezone.utc).isoformat()}
    before = None
    try:
        assert platform.system() == "Windows"
        assert (os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_EVENT_NAME"], os.environ["GITHUB_REF"]) == ("mqlitehq/mqlite", "push", "refs/heads/diagnostic/m194-windows-smoke")
        before = verify(plan)
        save(output / "source-before.json", before)
        environment = {k: v for k, v in os.environ.items() if not k.startswith(("MQLITE_", "MQLITEC_", "TURSO_", "LIBSQL_"))}
        environment.update(GOWORK="off", GOENV="off", GOFLAGS="", GOTOOLCHAIN="local")
        info = json.loads(subprocess.check_output(["go", "env", "-json", "GOVERSION", "GOOS", "GOARCH"], cwd=ROOT, env=environment))
        assert info["GOVERSION"].startswith(plan["go_version_prefix"]) and info["GOOS"] + "/" + info["GOARCH"] == plan["platform"]
        save(output / "toolchain.json", {"go": info, "host_cpus": os.cpu_count(), "gomaxprocs": environment.get("GOMAXPROCS", "unset")})
        control_output = output / "supervisor-controls"
        control_output.mkdir()
        result["supervisor_controls"] = controls(control_output, ROOT, environment)
        binary = output / "engine-smoke.test.exe"
        built = run(["go", "test", "-c", "-race", "-o", str(binary), "./engine"], environment, output / "build.log", plan["build_deadline_seconds"])
        result["commands"].append(built)
        assert built["exit"] == 0 and not built["timed_out"] and built["exception_type"] is None
        result["binary_sha256"] = digest(binary)
        argv = ["go", "tool", "test2json", "-t", "-p", "github.com/mqlitehq/mqlite/engine", str(binary), "-test.v=test2json", "-test.run=^" + plan["test"] + "$", "-test.count=" + str(plan["repetitions"]), "-test.failfast", "-test.timeout=" + str(plan["go_internal_deadline_seconds"]) + "s"]
        observed = run(argv, environment, output / "probe.jsonl", plan["workload_deadline_seconds"])
        result["commands"].append(observed)
        events = [json.loads(line) for line in (output / "probe.jsonl").read_text().splitlines()]
        result["observation"] = audit(events, plan["test"], plan["repetitions"])
        result["diagnostic_completed"] = observed["exit"] == 0 and not observed["timed_out"] and observed["exception_type"] is None and result["observation"]["passed"]
    except BaseException as error:
        result["failure_type"] = type(error).__name__
    finally:
        try:
            after = verify(plan)
            result["source_unchanged"] = before is not None and before == after
        except BaseException:
            result["source_unchanged"] = False
        result["diagnostic_completed"] = result["diagnostic_completed"] and result["source_unchanged"]
        result["interpretation"] = "Bounded non-reproduction only; original CI failure and unobserved Windows host cause remain." if result["diagnostic_completed"] else "Preserved diagnostic failure or incomplete result; not a passing candidate CI."
        save(output / "result.json", result)
    return 0 if result["diagnostic_completed"] else 1

if __name__ == "__main__":
    raise SystemExit(main())
