package engine

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"
)

// The remote (Turso/libSQL) retry path turns on exactly when isConnErr says an
// error is a dropped Hrana stream — never on a logical error, where a blind retry
// could double-execute. This hermetic test pins that classification contract so a
// later refactor can't silently widen or narrow it (MQLITE-4); the live round-trip
// lives in the creds-gated TestTursoIntegration / TestTursoExtended.
func TestIsConnErr(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"wrapped ErrBadConn", fmt.Errorf("exec: %w", driver.ErrBadConn), true},
		{"stream is closed", errors.New("driver: bad connection: stream is closed"), true},
		{"stream closed", errors.New("Hrana: stream closed"), true},
		{"connection reset", errors.New("read tcp: connection reset by peer"), true},
		{"plain bad connection", errors.New("write: bad connection"), true},
		{"no such table", errors.New("no such table: messages"), false},
		{"unique constraint", errors.New("UNIQUE constraint failed: dedup.message_id"), false},
		{"context canceled", errors.New("context canceled"), false},
	}
	for _, c := range cases {
		if got := isConnErr(c.err); got != c.want {
			t.Errorf("isConnErr(%s) = %v, want %v", c.name, got, c.want)
		}
	}
}

// retryable gates retries on remote-ness AND a connection error; attempts() caps
// the loop. Local stores must never retry (a single conn IS the single writer, and
// a logical failure there is final).
func TestRetryableAndAttempts(t *testing.T) {
	remote := &db{remote: true}
	local := &db{remote: false}

	connErr := errors.New("stream is closed")
	busyErr := errors.New("SQLite error: database is locked")
	logicalErr := errors.New("UNIQUE constraint failed")

	if !remote.retryable(connErr) {
		t.Error("remote must retry a dropped-stream connection error")
	}
	if !remote.retryable(busyErr) {
		t.Error("remote must retry a contended write (database is locked) — MQLITE-4")
	}
	if remote.retryable(logicalErr) {
		t.Error("remote must NOT retry a logical error (double-execution risk)")
	}
	if local.retryable(connErr) || local.retryable(busyErr) {
		t.Error("local must never retry, even on a connection or busy error")
	}

	if got := remote.attempts(); got != maxConnAttempts {
		t.Errorf("remote attempts = %d, want %d", got, maxConnAttempts)
	}
	if got := local.attempts(); got != 1 {
		t.Errorf("local attempts = %d, want 1", got)
	}
}

// isBusyErr classifies a contended-write failure from the remote primary, which —
// unlike a logical error — is safe to retry because the lock was never acquired
// (MQLITE-4 concurrency hardening).
func TestIsBusyErr(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"database is locked", errors.New("SQLite error: database is locked"), true},
		{"database table is locked", errors.New("database table is locked"), true},
		{"SQLITE_BUSY", errors.New("SQLITE_BUSY: ..."), true},
		{"unique constraint", errors.New("UNIQUE constraint failed: dedup.message_id"), false},
		{"no such table", errors.New("no such table: messages"), false},
	}
	for _, c := range cases {
		if got := isBusyErr(c.err); got != c.want {
			t.Errorf("isBusyErr(%s) = %v, want %v", c.name, got, c.want)
		}
	}
}
