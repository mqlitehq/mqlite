//go:build mqlite_perf_diag

package engine

import (
	"context"
	"database/sql"
)

// DiagnosticPoolStats exposes only local database/sql counters to the opt-in
// acceptance executable. It does not install engine tracing or change behavior.
func (e *Engine) DiagnosticPoolStats() sql.DBStats {
	if e == nil || e.db == nil || e.db.sql == nil {
		return sql.DBStats{}
	}
	return e.db.sql.Stats()
}

// DiagnosticSetPoolLimit is an isolated remote-pool comparison. It never changes
// local SQLite's single-writer setting or remote writer admission.
func (e *Engine) DiagnosticSetPoolLimit(n int) error {
	if e == nil || e.db == nil || e.db.sql == nil || !e.db.remote || n < 1 || n > 64 {
		return ErrInvalidArgument
	}
	if e.db.isClosing() {
		return ErrClosed
	}
	e.db.sql.SetMaxOpenConns(n)
	return nil
}

// DiagnosticSelectOne uses the ordinary lifecycle-protected fixed read path.
func (e *Engine) DiagnosticSelectOne(ctx context.Context) error {
	if e == nil || e.db == nil {
		return ErrClosed
	}
	var one int
	return e.db.queryRowScan(ctx, []any{&one}, "SELECT 1")
}
