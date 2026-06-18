//go:build !windows

package engine

import (
	"errors"
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// acquireFileLock takes an exclusive, non-blocking advisory lock on a sidecar
// lock file (MQLITE-6). It locks the sidecar — never the DB file itself — so it
// cannot interfere with SQLite's own file locking. The OS drops the lock when
// the process exits or crashes, so a crash never leaves a stale lock behind.
// A second opener of the same DB gets ErrDBLocked.
func acquireFileLock(lockPath string) (io.Closer, error) {
	f, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("mqlite: open lock file %s: %w", lockPath, err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, ErrDBLocked
		}
		return nil, fmt.Errorf("mqlite: lock %s: %w", lockPath, err)
	}
	return &fileLock{f: f}, nil
}

type fileLock struct{ f *os.File }

func (l *fileLock) Close() error {
	_ = unix.Flock(int(l.f.Fd()), unix.LOCK_UN) // explicit release; close would also drop it
	return l.f.Close()
}
