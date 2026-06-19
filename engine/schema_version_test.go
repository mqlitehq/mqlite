package engine

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

// MQLITE-24/25: a fresh DB records the single init schema version ("1"), and
// reopening a DB stamped with a different version is refused rather than silently
// running new DDL against an incompatible layout.
func TestSchemaVersionGuard(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + filepath.Join(t.TempDir(), "mq.db")

	// A fresh DB records the collapsed init version (MQLITE-25).
	e, err := Open(ctx, Options{DB: dsn, DisableBackground: true})
	if err != nil {
		t.Fatalf("fresh open: %v", err)
	}
	if schemaVersion != "1" {
		t.Fatalf("init schemaVersion = %q, want \"1\" (the v2→v4 ladder is collapsed)", schemaVersion)
	}
	var v string
	if err := e.db.queryRowScan(ctx, []any{&v}, `SELECT value FROM meta WHERE key='schema_version'`); err != nil {
		t.Fatalf("read recorded version: %v", err)
	}
	if v != schemaVersion {
		t.Fatalf("fresh DB recorded version %q, want %q", v, schemaVersion)
	}

	// Stamp an incompatible version, as if the DB were created by another build.
	if _, err := e.db.exec(ctx, `UPDATE meta SET value='legacy' WHERE key='schema_version'`); err != nil {
		t.Fatalf("stamp legacy version: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopening must refuse rather than run new DDL against the old layout (MQLITE-24).
	e2, err := Open(ctx, Options{DB: dsn, DisableBackground: true})
	if e2 != nil {
		_ = e2.Close()
	}
	if !errors.Is(err, ErrSchemaVersionMismatch) {
		t.Fatalf("reopen with mismatched version: got %v, want ErrSchemaVersionMismatch", err)
	}
}
