//go:build windows

package main

import (
	"os"
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"
)

// stdoutUndeliverable reports whether writes to f would never reach the caller.
//
// Windows needs its own test. os.SameFile is USELESS here: the FileInfo for a console, a pipe
// and `NUL` all carry zero volume/index identifiers, so SameFile(NUL, anything) is true — a
// naive port of the Unix check would declare every ordinary terminal and every piped run
// undeliverable and make `mqlite receive` refuse to work at all.
//
// So classify the handle, and only ever refuse for the null device itself:
//
//	disk file / pipe             -> delivers
//	character device + console   -> delivers (a real terminal)
//	character device, `\Device\Null` -> DOES NOT deliver
//	character device, other      -> delivers; COM1 and LPT1 are console-less character
//	                                devices that really do accept output, so they must not be
//	                                lumped in with NUL just for having no console mode
//	unclassifiable               -> assumed to deliver; a dead handle still surfaces as a write
//	                                error before any message is acknowledged, so guessing
//	                                "fine" costs nothing while guessing "broken" breaks
//	                                ordinary use
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
	if windows.GetConsoleMode(h, &mode) == nil {
		return false // a real console
	}
	return strings.EqualFold(ntObjectName(h), `\Device\Null`)
}

var procNtQueryObject = windows.NewLazySystemDLL("ntdll.dll").NewProc("NtQueryObject")

// ntObjectName returns the NT object name behind a handle (`\Device\Null` for NUL,
// `\Device\Serial0` for COM1), or "" if it can't be read.
//
// The obvious API — GetFileInformationByHandleEx(FileNameInfo) — is a dead end here: it is
// filesystem-only and fails with ERROR_NOACCESS on the null device (confirmed on the Windows
// CI runner), so it can neither name NUL nor tell it apart from a serial port. The object name
// is the one identifier every device handle has.
func ntObjectName(h windows.Handle) string {
	const objectNameInformation = 1
	var buf [2048]byte // UNICODE_STRING header + the name
	var n uint32
	// NtQueryObject returns an NTSTATUS; anything non-zero is a failure (unsure -> deliverable).
	st, _, _ := procNtQueryObject.Call(uintptr(h), objectNameInformation,
		uintptr(unsafe.Pointer(&buf[0])), uintptr(len(buf)), uintptr(unsafe.Pointer(&n)))
	if st != 0 {
		return ""
	}
	us := (*windows.NTUnicodeString)(unsafe.Pointer(&buf[0]))
	if us.Buffer == nil || us.Length == 0 {
		return ""
	}
	return windows.UTF16ToString(unsafe.Slice(us.Buffer, us.Length/2)) // Length is in bytes
}
