//go:build crash_injection && !windows

package crash

import (
	"os"
	"syscall"
)

// killedByUs reports whether the process died from the SIGKILL the harness injected — and NOT from
// some other signal. On Linux ProcessState.Exited() is false for EVERY signal termination, so a
// worker that took a real SIGSEGV or SIGABRT on its own would otherwise be accepted as the intended
// crash (codex). Only the parent-injected SIGKILL counts.
func killedByUs(ps *os.ProcessState) bool {
	if ps == nil {
		return false
	}
	ws, ok := ps.Sys().(syscall.WaitStatus)
	return ok && ws.Signaled() && ws.Signal() == syscall.SIGKILL
}
