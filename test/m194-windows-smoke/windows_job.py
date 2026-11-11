"""Own Windows command trees with a pre-execution gate and kill-on-close job."""
import ctypes
from ctypes import wintypes
import json
import os
from pathlib import Path
import subprocess
import sys
import time

class WindowsJob:
    def __init__(self):
        assert os.name == "nt" and ctypes.sizeof(ctypes.c_void_p) == 8
        class Basic(ctypes.Structure):
            _fields_ = [("user", ctypes.c_longlong), ("kernel", ctypes.c_longlong), ("flags", wintypes.DWORD), ("min_ws", ctypes.c_size_t), ("max_ws", ctypes.c_size_t), ("active_limit", wintypes.DWORD), ("affinity", ctypes.c_size_t), ("priority", wintypes.DWORD), ("scheduling", wintypes.DWORD)]
        class IO(ctypes.Structure):
            _fields_ = [(name, ctypes.c_ulonglong) for name in ("read_ops", "write_ops", "other_ops", "read_bytes", "write_bytes", "other_bytes")]
        class Extended(ctypes.Structure):
            _fields_ = [("basic", Basic), ("io", IO), ("process_memory", ctypes.c_size_t), ("job_memory", ctypes.c_size_t), ("peak_process", ctypes.c_size_t), ("peak_job", ctypes.c_size_t)]
        class Accounting(ctypes.Structure):
            _fields_ = [(name, ctypes.c_longlong) for name in ("user", "kernel", "period_user", "period_kernel")] + [(name, wintypes.DWORD) for name in ("faults", "total", "active", "terminated")]
        assert ctypes.sizeof(Extended) == 144 and ctypes.sizeof(Accounting) == 48
        self.accounting_type = Accounting
        self.api = ctypes.WinDLL("kernel32", use_last_error=True)
        signatures = {
            "CreateJobObjectW": ([ctypes.c_void_p, wintypes.LPCWSTR], wintypes.HANDLE),
            "SetInformationJobObject": ([wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD], wintypes.BOOL),
            "AssignProcessToJobObject": ([wintypes.HANDLE, wintypes.HANDLE], wintypes.BOOL),
            "QueryInformationJobObject": ([wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD, ctypes.c_void_p], wintypes.BOOL),
            "TerminateJobObject": ([wintypes.HANDLE, wintypes.UINT], wintypes.BOOL),
            "CloseHandle": ([wintypes.HANDLE], wintypes.BOOL),
        }
        for name, (args, result) in signatures.items():
            function = getattr(self.api, name)
            function.argtypes, function.restype = args, result
        self.handle = self.api.CreateJobObjectW(None, None)
        if not self.handle:
            raise ctypes.WinError(ctypes.get_last_error())
        limits = Extended()
        limits.basic.flags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE; no breakaway.
        if not self.api.SetInformationJobObject(self.handle, 9, ctypes.byref(limits), ctypes.sizeof(limits)):
            code = ctypes.get_last_error()
            self.close()
            raise ctypes.WinError(code)

    def assign(self, process):
        if not self.api.AssignProcessToJobObject(self.handle, wintypes.HANDLE(int(process._handle))):
            raise ctypes.WinError(ctypes.get_last_error())

    def active(self):
        data = self.accounting_type()
        if not self.api.QueryInformationJobObject(self.handle, 1, ctypes.byref(data), ctypes.sizeof(data), None):
            raise ctypes.WinError(ctypes.get_last_error())
        return data.active

    def terminate(self):
        if not self.api.TerminateJobObject(self.handle, 1):
            raise ctypes.WinError(ctypes.get_last_error())

    def close(self):
        if self.handle:
            self.api.CloseHandle(self.handle)
            self.handle = None


def worker(command_file, gate_file):
    # -I -S prevents Python startup customization from spawning before assignment.
    # The only child-creation point follows the supervisor's post-assignment gate.
    until = time.monotonic() + 30
    while not gate_file.exists():
        if time.monotonic() >= until:
            return 99
        time.sleep(0.01)
    argv = json.loads(command_file.read_text())
    return subprocess.call(argv)


def owned_run(argv, cwd, environment, log, seconds, job_factory=WindowsJob,
              spawn=subprocess.Popen, now=time.monotonic, pause=time.sleep,
              interrupt_after_gate=False):
    started = now()
    deadline, active_until = started + seconds, started + seconds - 5
    assert seconds > 5
    command_file, gate = log.with_suffix(".command.json"), log.with_suffix(".gate")
    command_file.write_text(json.dumps(argv) + "\n")
    assert not gate.exists()
    process, job, assigned = None, None, False
    timed_out, failure, joined, tree_gone = False, None, False, False
    code, active_at_cleanup = None, None
    try:
        job = job_factory()
        with log.open("wb") as output:
            process = spawn([sys.executable, "-I", "-S", "-B", str(Path(__file__).resolve()), "--worker", str(command_file), str(gate)], cwd=cwd, env=environment, stdout=output, stderr=subprocess.STDOUT, creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
            job.assign(process)
            assigned = True
            gate.write_text("assigned\n")
            if interrupt_after_gate:
                raise KeyboardInterrupt()
            try:
                code = process.wait(timeout=max(0, active_until - now()))
            except subprocess.TimeoutExpired:
                timed_out = True
    except BaseException as error:
        failure = type(error).__name__
    finally:
        cleanup_deadline = min(deadline, now() + 5)
        try:
            if process is not None:
                if assigned:
                    active_at_cleanup = job.active()
                    if active_at_cleanup:
                        job.terminate()
                    while job.active() and now() < cleanup_deadline:
                        pause(min(0.01, max(0, cleanup_deadline - now())))
                    tree_gone = job.active() == 0
                else:
                    # Assignment failed while the isolated worker was still gated:
                    # it cannot have spawned the requested command or descendants.
                    process.kill()
                    tree_gone = True
                process.wait(timeout=max(0, cleanup_deadline - now()))
                joined = True
                if code is None:
                    code = process.returncode
        except BaseException as error:
            failure = failure or type(error).__name__
        finally:
            if job is not None:
                job.close()  # Kernel kill-on-close also covers abrupt supervisor exit.
            if gate.exists():
                gate.unlink()
    elapsed = now() - started
    return {"argv": argv, "exit": code, "timed_out": timed_out, "exception_type": failure,
            "elapsed_seconds": elapsed, "declared_inclusive_seconds": seconds,
            "within_inclusive_budget": elapsed <= seconds, "leader_joined": joined,
            "owned_tree_gone": tree_gone, "command_was_assigned_before_gate": assigned,
            "active_processes_at_cleanup": active_at_cleanup}

def controls(output, cwd, environment):
    declarations = [
        ("normal", [sys.executable, "-I", "-S", "-c", "pass"], 10, False),
        ("orphan-descendant", [sys.executable, "-I", "-S", "-c", "import subprocess,sys;subprocess.Popen([sys.executable,'-I','-S','-c','import time;time.sleep(60)'])"], 10, False),
        ("timeout-tree", [sys.executable, "-I", "-S", "-c", "import time;time.sleep(60)"], 6, False),
        ("interrupt", [sys.executable, "-I", "-S", "-c", "import time;time.sleep(60)"], 10, True),
    ]
    results = []
    deadline = time.monotonic() + 60
    for name, argv, seconds, interrupt in declarations:
        assert deadline - time.monotonic() >= seconds
        observed = owned_run(argv, cwd, environment, output / (name + ".log"), seconds, interrupt_after_gate=interrupt)
        results.append({"name": name, **observed})
        (output / "controls.json").write_text(json.dumps(results, indent=2) + "\n")
        assert observed["leader_joined"] and observed["owned_tree_gone"] and observed["within_inclusive_budget"]
        if name == "timeout-tree":
            assert observed["timed_out"]
        elif name == "interrupt":
            assert observed["exception_type"] == "KeyboardInterrupt"
        else:
            assert observed["exit"] == 0 and not observed["timed_out"] and observed["exception_type"] is None
        if name == "orphan-descendant":
            assert observed["active_processes_at_cleanup"] >= 1
    return results

if __name__ == "__main__":
    assert len(sys.argv) == 4 and sys.argv[1] == "--worker"
    raise SystemExit(worker(Path(sys.argv[2]), Path(sys.argv[3])))
