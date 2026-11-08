#!/usr/bin/env python3
"""Hermetic final-receipt controls; imports no product or remote driver."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
MODULE = HERE / "finalize-state.py"
if not MODULE.exists():
    MODULE = HERE / "finalize_state.py"
spec = importlib.util.spec_from_file_location("finite_finalizer", MODULE)
finalizer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(finalizer)
budget_path = HERE / "pre-owner-budget.py"
if not budget_path.exists():
    budget_path = HERE / "pre_owner_budget.py"
budget_spec = importlib.util.spec_from_file_location("finite_budget", budget_path)
budget = importlib.util.module_from_spec(budget_spec)
budget_spec.loader.exec_module(budget)
RUN = "1234567890abcdef"
QUEUE = "diag_" + RUN + "_00_native"


def inputs():
    process = {"diagnostic_launch_attempted": True, "process_wait_returned": True,
               "process_exit_code": 0, "watchdog_terminated": False}
    manifest = {"schema": finalizer.SCHEMA, "run": RUN, "case_selection": "all",
                "exact_owned_queues": [{"backend": "standard", "case": "mixed_native_lock3", "queue": QUEUE}]}
    zero = {"active": 0, "open_bodies": 0, "dropped": 0}
    case = {"plan": {"id": "mixed_native_lock3"}, "queue": QUEUE, "status": "passed",
            "passed": True, "blocked": False, "owner_released": True, "provider_http": zero,
            "broker_http_dropped": 0, "provider_quiet_barrier": {"required": True, "observed_quiet": True, "quiet_window_ms": 200},
            "cleanup": {"attempted": True, "exact_owned_queue": True, "deleted": True, "state": "deleted", "absent_readback": True}}
    summary = {"schema": finalizer.SCHEMA, "run": RUN, "case_selection": "all", "finished": "2026-01-01T00:00:00Z",
               "passed": True, "blocked": False, "backends": [{"backend": "standard", "passed": True,
               "blocked": False, "provider_http": zero, "cases": [case]}]}
    return copy.deepcopy({"process.json": process, "manifest.json": manifest, "summary.json": summary})


class FinalStateControls(unittest.TestCase):
    def test_budget_boundary_admits(self):
        self.assertTrue(budget.sufficient(100.0, 100.0 + budget.JOB_SECONDS - budget.REQUIRED_SECONDS))

    def test_budget_boundary_refuses_late(self):
        self.assertFalse(budget.sufficient(100.0, 100.1 + budget.JOB_SECONDS - budget.REQUIRED_SECONDS))

    def test_budget_refuses_invalid_clocks(self):
        for start, now in [(float("nan"), 100), (1, float("inf")), (100, 99), (1, 5000)]:
            self.assertFalse(budget.sufficient(start, now))

    def check(self, documents, state="BLOCKED_UNCONFIRMED", outcome="success"):
        with tempfile.TemporaryDirectory() as directory:
            for name, value in documents.items():
                (Path(directory) / name).write_text(json.dumps(value))
            result = finalizer.classify(Path(directory), outcome)
        self.assertEqual(result["state"], state)
        self.assertFalse(result["automatic_progression_permitted"])
        self.assertEqual(result["database_requests"], 0)
        if state == "BLOCKED_UNCONFIRMED":
            self.assertTrue(result["blocked"])
            self.assertFalse(result["cleanup_confirmed"])
            self.assertFalse(result["eligible_for_independent_review"])
            self.assertTrue(result["requires_exact_manifest_reconciliation"])
        return result

    def test_complete_exact_cleanup(self):
        result = self.check(inputs(), "CURRENT_MANIFEST_CLEANUP_CONFIRMED")
        self.assertTrue(result["cleanup_confirmed"])
        self.assertEqual(result["verified_cleanup_resources"], 1)

    def test_not_started(self):
        self.check({}, "NO_MQLITE_OWNER_STARTED", "skipped")

    def test_budget_refused_before_owner(self):
        self.check({"process.json": {"diagnostic_launch_attempted": False, "launch_refused_before_owner": True,
                                   "process_wait_returned": False, "process_exit_code": None, "watchdog_terminated": False}}, "NO_MQLITE_OWNER_STARTED", "skipped")

    def test_refusal_receipt_must_be_consistent(self):
        receipt = {"diagnostic_launch_attempted": False, "launch_refused_before_owner": True,
                   "process_wait_returned": False, "process_exit_code": None, "watchdog_terminated": False}
        self.check({"process.json": receipt}, outcome="success")
        receipt.update(process_wait_returned=True, process_exit_code=0)
        self.check({"process.json": receipt}, outcome="skipped")

    def test_uncreated_sqlite_pool_control(self):
        data = inputs()
        data["manifest.json"]["exact_owned_queues"][0].update(backend="sqlite", case="control_pool1_c32_b1")
        backend = data["summary.json"]["backends"][0]
        backend["backend"] = "sqlite"
        backend["cases"] = [{"plan": {"id": "control_pool1_c32_b1"}, "status": "not_applicable_local_single_writer",
                             "owner_released": True, "passed": False, "blocked": False, "queue": ""}]
        result = self.check(data, "CURRENT_MANIFEST_CLEANUP_CONFIRMED")
        self.assertEqual(result["not_created_local_control_resources"], 1)
        self.assertEqual(result["verified_cleanup_resources"], 0)

    def test_remote_pool_control_cannot_claim_not_created(self):
        data = inputs()
        case = data["summary.json"]["backends"][0]["cases"][0]
        case.update(status="not_applicable_local_single_writer", passed=False)
        self.check(data)

    def test_nonfinite_quiet_window_refused(self):
        data = inputs()
        data["summary.json"]["backends"][0]["cases"][0]["provider_quiet_barrier"]["quiet_window_ms"] = float("inf")
        self.check(data)

    def test_unknown_failed_spawn(self):
        self.check({}, outcome="failure")

    def test_timeout_overrides_passed_summary(self):
        data = inputs(); data["process.json"].update(process_exit_code=124, watchdog_terminated=True)
        self.check(data, outcome="failure")

    def test_nonzero_exit(self):
        data = inputs(); data["process.json"]["process_exit_code"] = 1
        self.check(data, outcome="failure")

    def test_missing_summary(self):
        data = inputs(); del data["summary.json"]
        self.check(data)

    def test_missing_manifest(self):
        data = inputs(); del data["manifest.json"]
        self.check(data)

    def test_partial_manifest_coverage(self):
        data = inputs(); data["manifest.json"]["exact_owned_queues"].append({"backend": "rust", "case": "mixed_native_lock3", "queue": QUEUE})
        self.check(data)

    def test_manifest_run_mismatch(self):
        data = inputs(); data["summary.json"]["run"] = "0000000000000000"
        self.check(data)

    def test_cleanup_not_confirmed(self):
        data = inputs(); data["summary.json"]["backends"][0]["cases"][0]["cleanup"]["absent_readback"] = False
        self.check(data)

    def test_owner_not_released(self):
        data = inputs(); data["summary.json"]["backends"][0]["cases"][0]["owner_released"] = False
        self.check(data)

    def test_remote_quiet_unknown(self):
        data = inputs(); data["summary.json"]["backends"][0]["cases"][0]["provider_quiet_barrier"]["observed_quiet"] = False
        self.check(data)

    def test_provider_request_active(self):
        data = inputs(); data["summary.json"]["backends"][0]["provider_http"]["active"] = 1
        self.check(data)

    def test_provider_trace_dropped(self):
        data = inputs(); data["summary.json"]["backends"][0]["cases"][0]["provider_http"]["dropped"] = 1
        self.check(data)

    def test_broker_trace_dropped(self):
        data = inputs(); data["summary.json"]["backends"][0]["cases"][0]["broker_http_dropped"] = 1
        self.check(data)

    def test_string_false_is_not_boolean_pass(self):
        data = inputs(); data["summary.json"]["passed"] = "false"
        self.check(data)

    def test_integer_one_is_not_boolean_pass(self):
        data = inputs(); data["summary.json"]["passed"] = 1
        self.check(data)

    def test_boolean_is_not_exit_code(self):
        data = inputs(); data["process.json"]["process_exit_code"] = False
        self.check(data)

    def test_unjoined_process(self):
        data = inputs(); data["process.json"]["process_wait_returned"] = False
        self.check(data)

    def test_summary_not_finished(self):
        data = inputs(); data["summary.json"]["finished"] = ""
        self.check(data)

    def test_malformed_sensitive_input_is_not_disclosed(self):
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / "summary.json").write_text("secret-canary-libsql://not-a-real-host")
            result = finalizer.classify(Path(directory), "failure")
        self.assertEqual(result["state"], "BLOCKED_UNCONFIRMED")
        self.assertNotIn("secret-canary", json.dumps(result))

    def test_input_symlink_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            (path / "outside").write_text("{}")
            (path / "summary.json").symlink_to(path / "outside")
            result = finalizer.classify(path, "success")
        self.assertEqual(result["state"], "BLOCKED_UNCONFIRMED")


if __name__ == "__main__":
    unittest.main(verbosity=2)
