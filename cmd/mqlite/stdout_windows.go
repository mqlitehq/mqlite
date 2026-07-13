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
// So classify the handle, and be conservative: only the null device is undeliverable.
//
//	disk file / pipe          -> delivers
//	character device + console-> delivers (a real terminal)
//	character device, named   -> delivers UNLESS it is the null device; COM1 and LPT1 are
//	                             console-less character devices that really do accept output
//	unclassifiable            -> assumed to deliver; a dead handle still surfaces as a write
//	                             error before any message is acknowledged, so guessing "fine"
//	                             costs nothing while guessing "broken" would break normal use
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
	return isNullDevice(h)
}

// isNullDevice asks the handle for its device name and reports whether it is the null device
// (`\Device\Null`). Every other console-less character device — COM1, LPT1 — is a real sink
// that delivers, so it must NOT be lumped in with NUL just because it has no console mode.
// If the name can't be read we answer false: unsure means deliverable (see above).
func isNullDevice(h windows.Handle) bool {
	// FILE_NAME_INFO: a uint32 length in bytes followed by a UTF-16 name (not NUL-terminated).
	var buf [4 + 2*windows.MAX_PATH]byte
	if err := windows.GetFileInformationByHandleEx(h, windows.FileNameInfo, &buf[0], uint32(len(buf))); err != nil {
		return false
	}
	n := *(*uint32)(unsafe.Pointer(&buf[0])) / 2 // bytes -> UTF-16 code units
	if n == 0 || int(n) > (len(buf)-4)/2 {
		return false
	}
	name := windows.UTF16ToString(unsafe.Slice((*uint16)(unsafe.Pointer(&buf[4])), n))
	// The name comes back device-qualified or not depending on how stdout was opened
	// (`\Device\Null`, `\Null`, `NUL`), so match on the last element rather than the whole
	// string. Only character devices reach here, so a disk file called "null" can't collide.
	if i := strings.LastIndexByte(name, '\\'); i >= 0 {
		name = name[i+1:]
	}
	return strings.EqualFold(name, "Null") || strings.EqualFold(name, "NUL")
}
