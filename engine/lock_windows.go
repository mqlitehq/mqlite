//go:build windows

package engine

import (
	"errors"
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/windows"
)

// acquireFileLock takes an exclusive, non-blocking lock on a sidecar lock file
// (MQLITE-6). See lock_unix.go for the rationale; on Windows the lock is dropped
// when the handle is closed or the process dies. A second opener gets ErrDBLocked.
func acquireFileLock(lockPath string) (io.Closer, error) {
	f, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("mqlite: open lock file %s: %w", lockPath, err)
	}
	// LOCKFILE_FAIL_IMMEDIATELY -> return instead of blocking when held elsewhere.
	err = windows.LockFileEx(windows.Handle(f.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, new(windows.Overlapped))
	if err != nil {
		_ = f.Close()
		if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
			return nil, ErrDBLocked
		}
		return nil, fmt.Errorf("mqlite: lock %s: %w", lockPath, err)
	}
	return &fileLock{f: f}, nil
}

type fileLock struct{ f *os.File }

func (l *fileLock) Close() error {
	_ = windows.UnlockFileEx(windows.Handle(l.f.Fd()), 0, 1, 0, new(windows.Overlapped))
	return l.f.Close()
}
