package engine

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func installLegacyParentGuards(t *testing.T, e *Engine) {
	t.Helper()
	for _, guard := range parentGuardSchemas {
		if _, err := e.db.exec(context.Background(), `DROP TRIGGER `+guard.name); err != nil {
			t.Fatal(err)
		}
		if _, err := e.db.exec(context.Background(), guard.legacy); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCanonicalParentGuardRepairPreservesLiveDelivery(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "guards.db")
	opts := Options{DB: path, DisableBackground: true}
	e, err := Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	mustQueue(t, e, "q", QueueConfig{MaxDeliveryCount: 1})
	if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte("preserved")}); err != nil {
		t.Fatal(err)
	}
	m := recvOne(t, e, "q")
	if m == nil {
		t.Fatal("missing original delivery")
	}
	installLegacyParentGuards(t, e)
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	for _, guard := range parentGuardSchemas {
		var ddl string
		if err := e.db.queryRowScan(ctx, []any{&ddl}, `SELECT sql FROM sqlite_master WHERE name=?`, guard.name); err != nil {
			t.Fatal(err)
		}
		if normalizedSchemaSQL(ddl) != normalizedSchemaSQL(guard.current) {
			t.Fatalf("%s was not repaired", guard.name)
		}
	}
	if err := e.Complete(ctx, "q", m.SeqNumber, m.LockToken); err != nil {
		t.Fatalf("original last-chance token was invalidated: %v", err)
	}
	if _, err := e.db.exec(ctx, `DELETE FROM queues WHERE name='q'`); err != nil {
		t.Fatalf("empty parent remains deletable: %v", err)
	}
}

func TestCanonicalParentGuardConflictRefusesBeforeDDL(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "conflict.db")
	opts := Options{DB: path, DisableBackground: true}
	e, err := Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	guard := parentGuardSchemas[0]
	if _, err := e.db.exec(ctx, `DROP TRIGGER `+guard.name); err != nil {
		t.Fatal(err)
	}
	conflict := strings.Replace(guard.legacy, "'FOREIGN KEY constraint failed'", "'application guard'", 1)
	if _, err := e.db.exec(ctx, conflict); err != nil {
		t.Fatal(err)
	}
	if _, err := e.db.exec(ctx, `DROP INDEX idx_access_keys_created`); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if opened, err := Open(ctx, opts); !errors.Is(err, ErrSchemaVersionMismatch) {
		if opened != nil {
			_ = opened.Close()
		}
		t.Fatalf("unknown guard must refuse Open: %v", err)
	}
	d, err := openDB(ctx, path, "", "")
	if err != nil {
		t.Fatal(err)
	}
	defer d.close()
	var count int
	if err := d.queryRowScan(ctx, []any{&count}, `SELECT count(*) FROM sqlite_master WHERE name='idx_access_keys_created'`); err != nil || count != 0 {
		t.Fatalf("DDL ran before guard validation: count=%d err=%v", count, err)
	}
	var ddl string
	if err := d.queryRowScan(ctx, []any{&ddl}, `SELECT sql FROM sqlite_master WHERE name=?`, guard.name); err != nil || normalizedSchemaSQL(ddl) != normalizedSchemaSQL(conflict) {
		t.Fatalf("application guard changed: %v", err)
	}
}

// Rust serializes SQLite's <> spelling as != in sqlite_master. This fixture
// uses the actual service spelling without generalizing comparison operators.
func TestCanonicalParentGuardRustInequalitySpelling(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "rust-guards.db")
	opts := Options{DB: path, DisableBackground: true}
	e, err := Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, guard := range parentGuardSchemas {
		if _, err := e.db.exec(ctx, `DROP TRIGGER `+guard.name); err != nil {
			t.Fatal(err)
		}
		ddl := strings.Replace(guard.legacy, "NEW.name<>OLD.name", "NEW.name != OLD.name", 1)
		if _, err := e.db.exec(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	for _, guard := range parentGuardSchemas {
		if _, err := e.db.exec(ctx, `DROP TRIGGER `+guard.name); err != nil {
			t.Fatal(err)
		}
		ddl := strings.Replace(guard.current, "NEW.name<>OLD.name", "NEW.name != OLD.name", 1)
		if _, err := e.db.exec(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	if refresh, err := e.db.validateParentGuardSchema(ctx); err != nil || refresh {
		t.Fatalf("current Rust guard refresh=%t error=%v", refresh, err)
	}
}

func TestCanonicalParentGuardRepairRollsBackBothDefinitions(t *testing.T) {
	e, _ := testEngine(t)
	installLegacyParentGuards(t, e)
	old := parentGuardSchemas[1].current
	parentGuardSchemas[1].current = `CREATE TRIGGER malformed`
	defer func() { parentGuardSchemas[1].current = old }()
	if err := e.refreshParentGuardSchema(context.Background()); err == nil {
		t.Fatal("injected second CREATE failure must fail the repair")
	}
	for _, guard := range parentGuardSchemas {
		var ddl string
		if err := e.db.queryRowScan(context.Background(), []any{&ddl}, `SELECT sql FROM sqlite_master WHERE name=?`, guard.name); err != nil || normalizedSchemaSQL(ddl) != normalizedSchemaSQL(guard.legacy) {
			t.Fatalf("%s was not rolled back: %v", guard.name, err)
		}
	}
}

func TestCanonicalParentGuardRejectsChangedInequality(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "changed-operator.db")
	opts := Options{DB: path, DisableBackground: true}
	e, err := Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	guard := parentGuardSchemas[1]
	if _, err := e.db.exec(ctx, `DROP TRIGGER `+guard.name); err != nil {
		t.Fatal(err)
	}
	changed := strings.Replace(guard.legacy, "NEW.name<>OLD.name", "NEW.name>OLD.name", 1)
	if _, err := e.db.exec(ctx, changed); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if opened, err := Open(ctx, opts); !errors.Is(err, ErrSchemaVersionMismatch) {
		if opened != nil {
			_ = opened.Close()
		}
		t.Fatalf("changed inequality must not be adopted: %v", err)
	}
}
