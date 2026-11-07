package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// validateParentGuardSchema refuses unknown application definitions before any
// schema DDL. Only the exact old or current canonical guards belong to MQLite.
func (d *db) validateParentGuardSchema(ctx context.Context) (bool, error) {
	refresh := false
	for _, guard := range parentGuardSchemas {
		var kind, table, ddl string
		err := d.queryRowScan(ctx, []any{&kind, &table, &ddl}, `SELECT type,tbl_name,sql FROM sqlite_master WHERE name=? COLLATE NOCASE`, guard.name)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return false, fmt.Errorf("check parent guard schema: %w", err)
		}
		current := normalizedSchemaSQL(ddl)
		if kind != "trigger" || table != "queues" || (current != normalizedSchemaSQL(guard.current) && current != normalizedSchemaSQL(guard.legacy)) {
			return false, parentGuardConflict(guard.name)
		}
		refresh = refresh || current == normalizedSchemaSQL(guard.legacy)
	}
	return refresh, nil
}

func parentGuardConflict(name string) error {
	return fmt.Errorf("%w: existing %s object does not match the canonical parent guard", ErrSchemaVersionMismatch, name)
}

// refreshParentGuardSchema changes only equivalent, recognized trigger SQL.
// It runs before workers or public calls exist and uses the ordinary transaction
// contract: no visible interval lacks a guard, and an uncertain COMMIT refuses
// Open. The next Open accepts either complete definition, never a partial edit.
func (e *Engine) refreshParentGuardSchema(ctx context.Context) error {
	return e.inTxUnobserved(ctx, func(ctx context.Context, tx *txn) error {
		for _, guard := range parentGuardSchemas {
			var kind, table, ddl string
			if err := tx.QueryRowContext(ctx, `SELECT type,tbl_name,sql FROM sqlite_master WHERE name=? COLLATE NOCASE`, guard.name).Scan(&kind, &table, &ddl); err != nil {
				return fmt.Errorf("refresh parent guard schema: %w", err)
			}
			definition := normalizedSchemaSQL(ddl)
			if kind != "trigger" || table != "queues" || (definition != normalizedSchemaSQL(guard.current) && definition != normalizedSchemaSQL(guard.legacy)) {
				return parentGuardConflict(guard.name)
			}
			if definition == normalizedSchemaSQL(guard.current) {
				continue
			}
			// The identifier is a package-owned constant, never a caller value.
			if _, err := tx.ExecContext(ctx, `DROP TRIGGER `+guard.name); err != nil {
				return fmt.Errorf("refresh parent guard schema: %w", err)
			}
			if _, err := tx.ExecContext(ctx, guard.current); err != nil {
				return fmt.Errorf("refresh parent guard schema: %w", err)
			}
		}
		return nil
	})
}
