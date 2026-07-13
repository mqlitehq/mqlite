//go:build windows

package main

// TEMPORARY probe: report what Windows actually says about a handle to the null device, so the
// NUL detection is written against facts instead of a guess. Removed once stdout_windows.go is
// correct.

import (
	"os"
	"testing"
	"unsafe"

	"golang.org/x/sys/windows"
)

func TestProbeNulHandle(t *testing.T) {
	f, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h := windows.Handle(f.Fd())
	t.Logf("PROBE os.DevNull=%q handle=%v invalid=%v", os.DevNull, h, h == windows.InvalidHandle)

	ft, ferr := windows.GetFileType(h)
	t.Logf("PROBE GetFileType=%d err=%v (CHAR=%d DISK=%d PIPE=%d)", ft, ferr,
		windows.FILE_TYPE_CHAR, windows.FILE_TYPE_DISK, windows.FILE_TYPE_PIPE)

	var mode uint32
	t.Logf("PROBE GetConsoleMode err=%v", windows.GetConsoleMode(h, &mode))

	var buf [4 + 2*windows.MAX_PATH]byte
	nerr := windows.GetFileInformationByHandleEx(h, windows.FileNameInfo, &buf[0], uint32(len(buf)))
	if nerr != nil {
		t.Logf("PROBE GetFileInformationByHandleEx(FileNameInfo) err=%v", nerr)
	} else {
		n := *(*uint32)(unsafe.Pointer(&buf[0])) / 2
		name := windows.UTF16ToString(unsafe.Slice((*uint16)(unsafe.Pointer(&buf[4])), n))
		t.Logf("PROBE FileNameInfo len=%d name=%q", n, name)
	}

	// And what the guard currently concludes.
	t.Logf("PROBE stdoutUndeliverable(NUL)=%v", stdoutUndeliverable(f))
}
