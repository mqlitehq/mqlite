#!/usr/bin/env python3
"""Independent bounded supervisor controls; these are not a broker soak."""
import argparse
import ast
import copy
import errno
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
PARSER = argparse.ArgumentParser(description=__doc__)
PARSER.add_argument("--output", required=True, type=Path, help="new control evidence directory")
ARGS = PARSER.parse_args()
HERE = ARGS.output.resolve()
HERE.mkdir(parents=True, exist_ok=False)
SOURCE = Path(__file__).resolve().with_name("run.py")
SOURCE_TEXT = SOURCE.read_text()
SOURCE_HASH = hashlib.sha256(SOURCE_TEXT.encode()).hexdigest()
spec = importlib.util.spec_from_file_location('production_supervisor_under_test', SOURCE)
SUP = importlib.util.module_from_spec(spec)
spec.loader.exec_module(SUP)
DETAILS = {}


def actual_guard(message, namespace):
    """Execute the exact guard AST from run.py, without copying its condition."""
    matches = [node for node in ast.walk(ast.parse(SOURCE_TEXT))
               if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call)
               and isinstance(node.value.func, ast.Name) and node.value.func.id == 'require'
               and any(isinstance(child, ast.Constant) and isinstance(child.value, str)
                       and message in child.value for child in ast.walk(node))]
    if len(matches) != 1:
        raise AssertionError('expected exactly one source guard: ' + message)
    scope = dict(SUP.__dict__)
    scope.update(namespace)
    exec(compile(ast.Module(body=matches, type_ignores=[]), str(SOURCE), 'exec'), scope)


class Controls(unittest.TestCase):
    def setUp(self):
        self.temp = Path(tempfile.mkdtemp(prefix='case-', dir=HERE))
        self.addCleanup(shutil.rmtree, self.temp)
        self.fake = self.temp / 'fake-docker'
        self.fake.write_text('#!' + sys.executable + '\n' + '''
import json
from pathlib import Path
import sys
import time
root = Path(__file__).resolve().parent
assert sys.argv[1:4] == ['logs', '--follow', '--timestamps'], sys.argv
case = json.loads((root / (sys.argv[4] + '.json')).read_text())
for line in case['lines']:
    sys.stdout.buffer.write(line.encode())
    sys.stdout.buffer.flush()
    if case.get('interval'):
        time.sleep(case['interval'])
if case.get('wait'):
    until = time.monotonic() + 8
    while not (root / 'release').exists() and time.monotonic() < until:
        time.sleep(0.01)
sys.exit(case.get('exit', 0))
''')
        self.fake.chmod(0o700)

    def monitor(self, name, lines, **options):
        (self.temp / (name + '.json')).write_text(json.dumps(dict(lines=lines, **options)))
        monitor = SUP.Logs(str(self.fake), name, self.temp / (name + '-summary.json'))
        def cleanup():
            (self.temp / 'release').touch()
            monitor.finish()
        self.addCleanup(cleanup)
        return monitor

    def wait_lines(self, monitor, count):
        until = time.monotonic() + 3
        while monitor.report()['lines'] < count and time.monotonic() < until:
            time.sleep(0.005)
        self.assertGreaterEqual(monitor.report()['lines'], count)

    def test_01_normal_info_and_warning_stream(self):
        lines = ['2026-09-06T00:00:00Z INFO listening port=6754\n',
                 '2026-09-06T00:00:01Z WARN retrying attempt=1\n',
                 '2026-09-06T00:00:02Z {"level":"info","message":"ready"}\n',
                 '2026-09-06T00:00:03Z level=warn temporary backoff\n']
        monitor = self.monitor('normal', lines, interval=0.01)
        monitor.finish()
        report = monitor.report()
        self.assertEqual(report['lines'], len(lines))
        self.assertEqual(report['errors'], 0)
        self.assertIsNone(report['reader_failure'])
        self.assertEqual(report['reader_exit_code'], 0)
        self.assertEqual(report['sha256'], hashlib.sha256(''.join(lines).encode()).hexdigest())
        DETAILS['normal'] = report

    def test_02_error_severity_surface_and_persistent_first_samples(self):
        severities = ['ERRO background loop failed', 'ERROR background loop failed',
                      'FATAL shutdown failed', 'PANIC unexpected state', 'panic: unexpected state',
                      'fatal error: all goroutines asleep', 'level=error storage failure',
                      '{"level":"error","message":"storage failure"}',
                      'erro lowercase', 'fatal lowercase', 'ERROR: colon severity']
        lines = ['2026-09-06T00:00:00Z ' + message + '\n' for message in severities]
        lines += ['2026-09-06T00:00:00Z ERRO repeated ' + str(i) + '\n' for i in range(30)]
        monitor = self.monitor('severities', lines, interval=0.002, wait=True)
        self.wait_lines(monitor, len(lines))
        persisted = json.loads((self.temp / 'severities-summary.json').read_text())
        self.assertEqual(persisted['errors'], len(lines))
        self.assertEqual(persisted['first_errors'], [line.rstrip() for line in lines[:20]])
        self.assertEqual(persisted['sha256'], hashlib.sha256(''.join(lines).encode()).hexdigest())
        self.assertIsNone(monitor.proc.poll(), 'summary must be persisted before stream exit')
        with self.assertRaisesRegex(RuntimeError, 'unexpected error log'):
            actual_guard('unexpected error log; see log summary', {'role': 'broker', 'logs': monitor.report()})
        (self.temp / 'release').touch()
        monitor.finish()
        DETAILS['error_severities'] = monitor.report()

    def test_03_continuous_stream_early_exit_is_rejected_even_with_exit_zero(self):
        monitor = self.monitor('continuous', ['INFO initial ready\n'], wait=True)
        self.wait_lines(monitor, 1)
        namespace = {'role': 'broker', 'value': {'state': {'Running': True}},
                     'monitors': {'broker': monitor}}
        actual_guard('continuous log reader stopped unexpectedly', namespace)
        (self.temp / 'release').touch()
        monitor.finish()
        self.assertEqual(monitor.report()['reader_exit_code'], 0)
        with self.assertRaisesRegex(RuntimeError, 'continuous log reader stopped'):
            actual_guard('continuous log reader stopped unexpectedly', namespace)
        namespace['value']['state']['Running'] = False
        actual_guard('continuous log reader stopped unexpectedly', namespace)
        DETAILS['early_exit_zero'] = monitor.report()

    def test_04_nonzero_exit_and_reader_failure_fail_the_actual_guards(self):
        monitor = self.monitor('nonzero', ['INFO log process exits unsuccessfully\n'], exit=7)
        monitor.finish()
        self.assertEqual(monitor.report()['reader_exit_code'], 7)
        with self.assertRaisesRegex(RuntimeError, 'log reader failed'):
            actual_guard(' log reader failed', {'role': 'broker', 'logs': monitor.report()})
        DETAILS['nonzero_exit'] = monitor.report()
        oversized = self.monitor('oversized', ['x' * (SUP.MIB + 1) + '\n'])
        oversized.finish()
        self.assertIn('oversized log line', oversized.report()['reader_failure'])
        with self.assertRaisesRegex(RuntimeError, 'unexpected error log'):
            actual_guard('unexpected error log; see log summary', {'role': 'broker', 'logs': oversized.report()})
        DETAILS['reader_failure'] = oversized.report()

    def test_05_directory_sampling_tolerates_real_pending_file_and_directory_churn(self):
        root = self.temp / 'evidence'
        root.mkdir()
        stable_bytes = 0
        for i in range(4):
            data = bytes([i]) * (1024 + i)
            (root / ('stable-' + str(i))).write_bytes(data)
            stable_bytes += len(data)
        stop = threading.Event()
        failures, operations = [], [0, 0]
        def mutate(index):
            try:
                while not stop.is_set():
                    pending = root / ('pending-' + str(index))
                    pending.mkdir()
                    (pending / 'manifest.json').write_bytes(b'x' * 32)
                    (pending / 'ack.tmp').write_bytes(b'y' * 48)
                    (pending / 'ack.tmp').replace(pending / 'ack.json')
                    (pending / 'manifest.json').unlink()
                    (pending / 'ack.json').unlink()
                    pending.rmdir()
                    operations[index] += 1
            except BaseException as exc:
                failures.append(repr(exc))
        workers = [threading.Thread(target=mutate, args=(i,)) for i in range(2)]
        for worker in workers:
            worker.start()
        readings = []
        try:
            for _ in range(2000):
                measured = SUP.tree_size(root)
                self.assertGreaterEqual(measured, stable_bytes)
                readings.append(measured)
        finally:
            stop.set()
            for worker in workers:
                worker.join(timeout=5)
                self.assertFalse(worker.is_alive())
        self.assertFalse(failures)
        self.assertTrue(all(count > 10 for count in operations), operations)
        self.assertEqual(SUP.tree_size(root), stable_bytes)
        DETAILS['directory_churn'] = {'samples': len(readings), 'mutations_per_worker': operations,
                                      'stable_bytes': stable_bytes, 'min_bytes': min(readings), 'max_bytes': max(readings)}

    def test_06_real_permission_failure_and_injected_io_errors_are_not_swallowed(self):
        root = self.temp / 'permission'
        nested = root / 'restricted'
        nested.mkdir(parents=True)
        target = nested / 'retained.json'
        target.write_text('retained evidence')
        self.assertNotEqual(os.geteuid(), 0, 'real permission control requires a non-root account')
        nested.chmod(0)
        try:
            with self.assertRaises(PermissionError):
                SUP.tree_size(root)
        finally:
            nested.chmod(0o700)
        original_stat = os.stat
        def stat_io(path, *args, **kwargs):
            if Path(path) == target:
                raise OSError(errno.EIO, 'injected stat I/O failure', str(path))
            return original_stat(path, *args, **kwargs)
        with patch.object(SUP.os, 'stat', side_effect=stat_io):
            with self.assertRaises(OSError) as caught:
                SUP.tree_size(root)
        self.assertEqual(caught.exception.errno, errno.EIO)
        original_scandir = os.scandir
        def scandir_io(path):
            if Path(path) == nested:
                raise OSError(errno.EIO, 'injected walk I/O failure', str(path))
            return original_scandir(path)
        with patch.object(SUP.os, 'scandir', side_effect=scandir_io):
            with self.assertRaises(OSError) as caught:
                SUP.tree_size(root)
        self.assertEqual(caught.exception.errno, errno.EIO)
        self.assertEqual(SUP.tree_size(root), len('retained evidence'))
        DETAILS['filesystem_errors'] = {'real_EACCES_rejected': True, 'injected_stat_EIO_rejected': True,
                                         'injected_walk_EIO_rejected': True}

    def test_07_completed_workload_is_bound_to_this_run_and_interval(self):
        root = self.temp / "verdict"
        (root / "runner").mkdir(parents=True)
        provenance = {"run_id": "control-candidate", "source_sha": "a" * 40,
                      "images": {"broker": {"id": "sha256:candidate"}}}
        result = {"status": "SMOKE", "production_ready": False, "requested_seconds": 60,
                  "validated_activity_seconds": 60, "elapsed_seconds": 61}
        details = {"provenance": provenance, "config": {
            "duration_ns": 60_000_000_000, "endpoint": "http://broker:6754",
            "outbox_db": "/data/outbox.db", "evidence": "/evidence/runner",
            "token_env": "MQLITE_TOKEN"}}
        def write(r, d, p):
            SUP.save(root / "runner" / "result.json", r)
            SUP.save(root / "runner" / "metadata.json", d)
            SUP.save(root / "provenance.json", p)
        write(result, details, provenance)
        self.assertEqual(SUP.verify_runner(root, "SMOKE", 60, provenance, 65), result)
        variants = [
            ("result", "status", "PASS"), ("result", "production_ready", True),
            ("result", "requested_seconds", 86400), ("result", "validated_activity_seconds", 59),
            ("result", "elapsed_seconds", 59), ("result", "elapsed_seconds", 66),
            ("result", "elapsed_seconds", float("nan")),
            ("config", "duration_ns", 86_400_000_000_000),
            ("config", "endpoint", "http://different:6754"),
            ("config", "outbox_db", "/data/mq.db"),
            ("config", "evidence", "/evidence/other"), ("config", "token_env", "OTHER_TOKEN"),
            ("metadata-provenance", "source_sha", "b" * 40),
            ("saved-provenance", "run_id", "another-run"),
        ]
        rejected = []
        for target, key, value in variants:
            with self.subTest(target=target, key=key, value=value):
                r, d, p = copy.deepcopy(result), copy.deepcopy(details), copy.deepcopy(provenance)
                destination = {"result": r, "config": d["config"],
                               "metadata-provenance": d["provenance"], "saved-provenance": p}[target]
                destination[key] = value
                write(r, d, p)
                with self.assertRaises(RuntimeError):
                    SUP.verify_runner(root, "SMOKE", 60, provenance, 65)
                rejected.append({"target": target, "field": key, "value": str(value)})
        DETAILS["verdict_binding"] = {"valid_control": True, "rejected_mutations": rejected,
                                      "production_acceptance": False}

    def test_08_final_state_and_physical_outbox_must_both_be_clean(self):
        state = {"state": {"Running": False, "ExitCode": 0, "OOMKilled": False}, "restart_count": 0}
        actual_guard(" final state/exit/OOM/restart", {"role": "runner", "state": state})
        rejected = []
        for key, value in (("Running", True), ("ExitCode", 137), ("OOMKilled", True), ("restart_count", 1)):
            candidate = copy.deepcopy(state)
            if key == "restart_count":
                candidate[key] = value
            else:
                candidate["state"][key] = value
            with self.assertRaises(RuntimeError):
                actual_guard(" final state/exit/OOM/restart", {"role": "runner", "state": candidate})
            rejected.append(key)
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute("CREATE TABLE soak_business (id TEXT)")
            actual_guard("outbox retained business rows", {"conn": conn})
            conn.execute("INSERT INTO soak_business VALUES ('unreconciled')")
            with self.assertRaises(RuntimeError):
                actual_guard("outbox retained business rows", {"conn": conn})
        finally:
            conn.close()
        DETAILS["final_state"] = {"rejected_fields": rejected, "actual_sqlite_business_row_rejected": True}


if __name__ == '__main__':
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(Controls)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    unchanged = SOURCE_HASH == hashlib.sha256(SOURCE.read_bytes()).hexdigest()
    passed = result.wasSuccessful() and unchanged
    report = {'control_status': 'PASS' if passed else 'FAIL',
              'production_acceptance': False, 'scope': 'bounded logs, filesystem, verdict/provenance and final-state controls; no workload soak',
              'supervisor': str(SOURCE), 'supervisor_sha256_at_import': SOURCE_HASH,
              'supervisor_sha256_at_end': hashlib.sha256(SOURCE.read_bytes()).hexdigest(),
              'tests': result.testsRun, 'failures': len(result.failures), 'errors': len(result.errors),
              'details': DETAILS}
    (HERE / 'results.json').write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
    sys.exit(not passed)
