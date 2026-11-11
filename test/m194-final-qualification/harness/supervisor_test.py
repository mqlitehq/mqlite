#!/usr/bin/env python3
"""No network: prove opaque child output and bounded process supervision."""
from contextlib import redirect_stdout
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

# Child interpreters in the process-group controls inherit this setting.
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
sys.dont_write_bytecode = True

spec = importlib.util.spec_from_file_location("supervisor", Path(__file__).with_name("run-safe.py"))
supervisor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(supervisor)


class SupervisorTests(unittest.TestCase):
    def test_child_output_is_hashed_not_forwarded_or_persisted(self):
        fixture = "private-test-fixture-not-a-real-key"
        env = dict(os.environ, OPAQUE_TEST_FIXTURE=fixture)
        output = io.StringIO()
        with redirect_stdout(output):
            result = supervisor.execute([sys.executable, "-c", "import os; print(os.environ['OPAQUE_TEST_FIXTURE'])"], 5, env)
        self.assertEqual(result["exit"], 0)
        self.assertFalse(result["timed_out"])
        self.assertEqual(result["opaque_output_sha256"], hashlib.sha256((fixture + "\n").encode()).hexdigest())
        self.assertEqual(output.getvalue(), "")
        self.assertNotIn(fixture, json.dumps(result))

    def test_noncooperative_child_has_a_finite_process_boundary(self):
        result = supervisor.execute([sys.executable, "-c", "import time; time.sleep(30)"], 0.1, dict(os.environ))
        self.assertTrue(result["timed_out"])
        self.assertNotEqual(result["exit"], 0)

    def test_dead_parent_with_inherited_stdout_cannot_outlive_the_controller_budget(self):
        with tempfile.TemporaryDirectory(prefix="compat-controller-") as folder:
            owned = Path(folder) / "owned-group.json"
            parent = ("import json,os,pathlib,subprocess,sys; "
                      "child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(3)']); "
                      "pathlib.Path(" + repr(str(owned)) + ").write_text(json.dumps("
                      "{'parent':os.getpid(),'group':os.getpgrp(),'child':child.pid}))")
            controller = ("import importlib.util,json,os,sys; "
                          "spec=importlib.util.spec_from_file_location('runner'," + repr(str(Path(__file__).with_name("run-safe.py"))) + "); "
                          "runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner); "
                          "print(json.dumps(runner.execute([sys.executable,'-c'," + repr(parent) + "],0.1,dict(os.environ))))")
            process = subprocess.Popen([sys.executable, "-c", controller], stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT, start_new_session=True)
            finished = False
            output = b""
            try:
                output, _ = process.communicate(timeout=0.8)
                finished = True
            except subprocess.TimeoutExpired:
                pass
            finally:
                if owned.exists():
                    values = json.loads(owned.read_text())
                    self.assertEqual(values["parent"], values["group"])
                    self.assertNotIn(values["group"], (0, 1, os.getpgrp(), process.pid))
                    try:
                        self.assertEqual(os.getpgid(values["child"]), values["group"])
                        os.killpg(values["group"], signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                if process.poll() is None:
                    try:
                        output, _ = process.communicate(timeout=1)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.communicate(timeout=1)
                process.stdout.close()
            self.assertTrue(finished, "controller ignored its deadline after the direct parent exited; owned descendants were cleaned and joined")
            self.assertEqual(process.returncode, 0, "controller did not return a sanitized result")
            result = json.loads(output)
            self.assertTrue(result["timed_out"])

    def test_controller_exception_and_keyboard_interrupt_reap_the_owned_child(self):
        original_popen = subprocess.Popen
        original_selector = supervisor.selectors.DefaultSelector
        for error in (RuntimeError("controlled selector failure"), KeyboardInterrupt()):
            with self.subTest(error=type(error).__name__):
                children = []
                def spawn(*args, **kwargs):
                    child = original_popen(*args, **kwargs)
                    children.append(child)
                    return child
                class InterruptedSelector(original_selector):
                    def select(self, timeout=None):
                        raise error
                with patch.object(supervisor.subprocess, "Popen", side_effect=spawn), \
                        patch.object(supervisor.selectors, "DefaultSelector", InterruptedSelector):
                    with self.assertRaises(type(error)):
                        supervisor.execute([sys.executable, "-c", "import time; time.sleep(3)"], 1, dict(os.environ))
                self.assertEqual(len(children), 1)
                self.assertIsNotNone(children[0].poll())
                self.assertTrue(children[0].stdout.closed)

    def test_persistent_signal_denial_is_a_bounded_failure_not_success(self):
        original_popen, original_killpg = subprocess.Popen, os.killpg
        children = []
        def spawn(*args, **kwargs):
            child = original_popen(*args, **kwargs)
            children.append(child)
            return child
        def deny_final_signal(group, sig):
            if sig == signal.SIGKILL:
                raise PermissionError("controlled final-signal denial")
            return original_killpg(group, sig)
        with patch.object(supervisor.subprocess, "Popen", side_effect=spawn), \
                patch.object(supervisor.os, "killpg", side_effect=deny_final_signal):
            with self.assertRaisesRegex(RuntimeError, "cleanup was not confirmed"):
                supervisor.execute([sys.executable, "-c", "import time; time.sleep(3)"], 0.1, dict(os.environ))
        self.assertIsNotNone(children[0].poll())
        self.assertTrue(children[0].stdout.closed)


if __name__ == "__main__":
    unittest.main()
