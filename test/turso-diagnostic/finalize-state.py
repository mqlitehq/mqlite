#!/usr/bin/env python3
"""Read only the current public receipt/summary/manifest; never open a database."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import re
import sys

LIMIT = 128 * 1024 * 1024
SCHEMA = "turso-performance-attribution-v1"


def read_current(folder, name):
    path = folder / name
    if not path.exists():
        return None, None
    assert not path.is_symlink() and path.is_file() and path.stat().st_size <= LIMIT
    data = path.read_bytes()
    assert len(data) <= LIMIT
    value = json.loads(data)
    assert isinstance(value, dict)
    return value, hashlib.sha256(data).hexdigest()


def classify(folder, outcome):
    result = {
        "schema": "finite-owner-final-state-v1", "state": "BLOCKED_UNCONFIRMED",
        "blocked": True, "diagnostic_step_outcome": outcome if outcome in ("success", "failure", "cancelled", "skipped", "") else "unknown",
        "diagnostic_launch_attempted": False, "diagnostic_process_wait_returned": False,
        "diagnostic_process_exit_code": None, "owner_may_have_started": True,
        "cleanup_confirmed": False, "owned_resources": 0, "verified_cleanup_resources": 0,
        "not_created_local_control_resources": 0,
        "current_manifest_sha256": None, "current_summary_sha256": None,
        "eligible_for_independent_review": False, "automatic_progression_permitted": False,
        "manual_review_required": True, "requires_exact_manifest_reconciliation": True,
        "reads_credentials": False, "database_requests": 0, "automatic_cleanup_attempted": False,
        "hidden_driver_goroutines_joined_claim": False,
        "reason": "current_owner_and_cleanup_unconfirmed",
    }
    try:
        process, _ = read_current(folder, "process.json")
        summary, summary_hash = read_current(folder, "summary.json")
        manifest, manifest_hash = read_current(folder, "manifest.json")
        result["current_manifest_sha256"] = manifest_hash
        result["current_summary_sha256"] = summary_hash
        if process is None and summary is None and manifest is None and outcome in ("", "skipped"):
            result.update(state="NO_MQLITE_OWNER_STARTED", blocked=False, owner_may_have_started=False,
                          requires_exact_manifest_reconciliation=False, reason="diagnostic_step_not_started")
            return result
        if (isinstance(process, dict) and process.get("diagnostic_launch_attempted") is False
                and process.get("launch_refused_before_owner") is True and summary is None and manifest is None
                and outcome == "skipped" and process.get("process_wait_returned") is False
                and process.get("process_exit_code") is None and process.get("watchdog_terminated") is False):
            result.update(state="NO_MQLITE_OWNER_STARTED", blocked=False, owner_may_have_started=False,
                          requires_exact_manifest_reconciliation=False, reason="remaining_budget_refused_before_owner")
            return result
        assert isinstance(process, dict)
        result["diagnostic_launch_attempted"] = process.get("diagnostic_launch_attempted") is True
        result["diagnostic_process_wait_returned"] = process.get("process_wait_returned") is True
        code = process.get("process_exit_code")
        if type(code) is int and 0 <= code <= 255:
            result["diagnostic_process_exit_code"] = code
        assert result["diagnostic_launch_attempted"] and result["diagnostic_process_wait_returned"]
        assert outcome == "success" and type(code) is int and code == 0
        assert process.get("watchdog_terminated") is False
        assert isinstance(summary, dict) and isinstance(manifest, dict)
        assert summary.get("schema") == manifest.get("schema") == SCHEMA
        run = manifest.get("run")
        assert isinstance(run, str) and re.fullmatch(r"[0-9a-f]{16}", run)
        assert summary.get("run") == run and summary.get("case_selection") == manifest.get("case_selection")
        assert summary.get("passed") is True and summary.get("blocked") is False
        assert isinstance(summary.get("finished"), str) and summary["finished"]
        queues = manifest.get("exact_owned_queues")
        assert isinstance(queues, list) and queues
        expected = set()
        for entry in queues:
            assert isinstance(entry, dict) and set(entry) == {"backend", "case", "queue"}
            alias, case, queue = entry["backend"], entry["case"], entry["queue"]
            assert alias in ("sqlite", "standard", "rust")
            assert isinstance(case, str) and re.fullmatch(r"[A-Za-z0-9_/-]{1,96}", case)
            assert isinstance(queue, str) and re.fullmatch(r"diag_" + run + r"_[0-9]{2}_(native|low_floor)", queue)
            key = alias, case, queue
            assert key not in expected
            expected.add(key)
        result["owned_resources"] = len(expected)
        seen = set()
        backends = summary.get("backends")
        assert isinstance(backends, list) and backends
        seen_backends = set()
        for backend in backends:
            assert isinstance(backend, dict)
            alias = backend.get("backend")
            assert alias in ("sqlite", "standard", "rust") and alias not in seen_backends
            seen_backends.add(alias)
            assert backend.get("passed") is True and backend.get("blocked") is False
            provider = backend.get("provider_http")
            assert isinstance(provider, dict) and all(type(provider.get(k)) is int and provider[k] == 0 for k in ("active", "open_bodies", "dropped"))
            cases = backend.get("cases")
            assert isinstance(cases, list) and cases
            for case in cases:
                assert isinstance(case, dict) and isinstance(case.get("plan"), dict)
                assert case.get("owner_released") is True and case.get("blocked") is False
                assert not case.get("failure")
                if case.get("status") == "not_applicable_local_single_writer":
                    assert alias == "sqlite" and case["plan"].get("id") in ("control_pool1_c32_b1", "control_pool4_c32_b1")
                    assert case.get("passed") is False
                    matches = {key for key in expected if key[:2] == (alias, case["plan"]["id"])}
                    assert len(matches) == 1
                    key = next(iter(matches))
                    assert key not in seen and case.get("queue") in (None, "", key[2])
                    seen.add(key)
                    result["not_created_local_control_resources"] += 1
                    continue
                key = alias, case["plan"].get("id"), case.get("queue")
                assert key in expected and key not in seen
                seen.add(key)
                assert case.get("passed") is True and case.get("status") == "passed"
                cleanup = case.get("cleanup")
                assert isinstance(cleanup, dict)
                assert all(cleanup.get(k) is True for k in ("attempted", "exact_owned_queue", "deleted", "absent_readback"))
                assert cleanup.get("state") == "deleted" and not cleanup.get("failure")
                provider = case.get("provider_http")
                assert isinstance(provider, dict) and all(type(provider.get(k)) is int and provider[k] == 0 for k in ("active", "open_bodies", "dropped"))
                assert type(case.get("broker_http_dropped")) is int and case["broker_http_dropped"] == 0
                quiet = case.get("provider_quiet_barrier")
                assert isinstance(quiet, dict)
                if alias != "sqlite":
                    assert quiet.get("required") is True and quiet.get("observed_quiet") is True
                    assert type(quiet.get("quiet_window_ms")) in (int, float) and math.isfinite(quiet["quiet_window_ms"]) and quiet["quiet_window_ms"] >= 200
                result["verified_cleanup_resources"] += 1
        assert seen == expected
        result.update(state="CURRENT_MANIFEST_CLEANUP_CONFIRMED", blocked=False, cleanup_confirmed=True,
                      eligible_for_independent_review=True, requires_exact_manifest_reconciliation=False,
                      reason="finite_process_exit_and_exact_manifest_cleanup_observed")
    except BaseException:
        # Never retain raw errors, paths, JSON values, URLs or credential material.
        pass
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--public", type=Path, required=True)
    parser.add_argument("--diagnostic-outcome", required=True)
    args = parser.parse_args()
    result = classify(args.public, args.diagnostic_outcome)
    args.public.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (args.public / "owner-final-state.json").open("x") as stream:
        stream.write(json.dumps(result, indent=2) + "\n")
    print("Finite owner state:", result["state"])
    return 1 if result["blocked"] else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        print("Finite owner state could not be recorded; progression forbidden.", file=sys.stderr)
        sys.exit(1)
