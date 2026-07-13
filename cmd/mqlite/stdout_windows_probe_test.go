//go:build windows

package main

// TEMPORARY probe (MQLITE-96): if the null-device guard is still wrong on this runner, report
// exactly what Windows says about the handle, so the next fix is written against facts rather
// than another guess. Silent when the guard is right. Deleted once CI is green.

import (
	"os"
	"testing"

	"golang.org/x/sys/windows"
)

func TestProbeNulHandle(t *testing.T) {
	f, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if stdoutUndeliverable(f) {
		return // the guard works — nothing to report
	}
	h := windows.Handle(f.Fd())
	ft, ferr := windows.GetFileType(h)
	var mode uint32
	t.Errorf("PROBE the null device was NOT detected: DevNull=%q handle=%v type=%d (CHAR=%d) typeErr=%v consoleModeErr=%v ntObjectName=%q",
		os.DevNull, h, ft, windows.FILE_TYPE_CHAR, ferr, windows.GetConsoleMode(h, &mode), ntObjectName(h))
}
