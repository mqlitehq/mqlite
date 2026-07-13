//go:build windows

package main

import (
	"os"

	"golang.org/x/sys/windows"
)

// stdoutUndeliverable reports whether writes to f would never reach the caller.
//
// Windows needs its own test. os.SameFile is USELESS here: the FileInfo for a console, a pipe
// and `NUL` all carry zero volume/index identifiers, so SameFile(NUL, anything) is true — a
// naive port of the Unix check would declare every ordinary terminal and every piped run
// undeliverable and make `mqlite receive` refuse to work at all.
//
// Classify the handle instead. Disk files and pipes always deliver. A character device
// delivers only if it is a real console — and a console is exactly what has a console mode,
// which `NUL` does not. Anything unclassifiable is assumed deliverable: a genuinely dead
// handle still surfaces as a write error before a message is ever acknowledged, so guessing
// "fine" here costs nothing, whereas guessing "broken" would break normal use.
func stdoutUndeliverable(f *os.File) bool {
	h := windows.Handle(f.Fd())
	if h == windows.InvalidHandle {
		return true
	}
	t, err := windows.GetFileType(h)
	if err != nil || t != windows.FILE_TYPE_CHAR {
		return false // disk file, pipe, or unclassifiable — treat as deliverable
	}
	var mode uint32
	return windows.GetConsoleMode(h, &mode) != nil // a console has a mode; NUL does not
}
