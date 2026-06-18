package engine

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

// MQLITE-6: a local file DB is single-writer — a second opener is rejected with
// ErrDBLocked rather than racing the first on crash recovery / claims.
func TestFileDBSingleWriter(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + filepath.Join(t.TempDir(), "mq.db")

	e1, err := Open(ctx, Options{DB: dsn, DisableBackground: true})
	if err != nil {
		t.Fatalf("first open: %v", err)
	}

	e2, err := Open(ctx, Options{DB: dsn, DisableBackground: true})
	if !errors.Is(err, ErrDBLocked) {
		if e2 != nil {
			_ = e2.Close()
		}
		t.Fatalf("second open of the same file: got err=%v, want ErrDBLocked", err)
	}

	// Releasing the first frees the lock so the file can be reopened.
	if err := e1.Close(); err != nil {
		t.Fatalf("close first: %v", err)
	}
	e3, err := Open(ctx, Options{DB: dsn, DisableBackground: true})
	if err != nil {
		t.Fatalf("reopen after close: %v", err)
	}
	_ = e3.Close()
}

// :memory: DBs are private per handle, so concurrent opens must NOT be locked.
func TestMemoryDBNotLocked(t *testing.T) {
	ctx := context.Background()
	e1, err := Open(ctx, Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("first :memory: open: %v", err)
	}
	defer e1.Close()
	e2, err := Open(ctx, Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("second :memory: open must not be locked: %v", err)
	}
	defer e2.Close()
}
