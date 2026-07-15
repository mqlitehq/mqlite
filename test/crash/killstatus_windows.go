//go:build crash_injection && windows

package crash

import "os"

// killedByUs reports whether the process died from the harness's kill. Process.Kill on Windows is
// TerminateProcess, which ends the process with exit code 1. A Go panic exits 2, the worker's own
// fail() exits 3, and a clean return is 0 — so exit code 1 is the terminate we injected.
func killedByUs(ps *os.ProcessState) bool {
	return ps != nil && ps.Exited() && ps.ExitCode() == 1
}
