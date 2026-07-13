//go:build !windows

package main

import "os"

// stdoutUndeliverable reports whether writes to f would never reach the caller.
//
// A write error cannot detect this on its own: closing fd 1 at exec (`mqlite receive q 1>&-`)
// does NOT make writes fail, because the Go runtime reopens descriptors 0/1/2 to the null
// device before main() runs — so every write "succeeds" into a black hole. The only reliable
// signal is the identity of the file itself, which also covers an explicit `>/dev/null`.
//
// os.SameFile compares (device, inode) here, so a terminal, a pipe and a regular file are all
// correctly distinguished from /dev/null. (On Windows those identifiers are all zero, which is
// why that platform needs a completely different test — see stdout_windows.go.)
func stdoutUndeliverable(f *os.File) bool {
	so, err := f.Stat()
	if err != nil {
		return true // unusable handle
	}
	nul, err := os.Stat(os.DevNull)
	if err != nil {
		return false // no /dev/null to compare against: assume stdout is fine
	}
	return os.SameFile(nul, so)
}
