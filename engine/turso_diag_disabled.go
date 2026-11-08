//go:build !mqlite_perf_diag

package engine

import (
	"context"
	"database/sql"
)

// The production build keeps the original rows type and does not collect data.
type diagnosticRows = sql.Rows
type diagnosticSpan struct{}

func diagnosticStart(context.Context, string, string) diagnosticSpan      { return diagnosticSpan{} }
func (diagnosticSpan) end(error)                                          {}
func diagnosticMark(context.Context, string, string, error)               {}
func diagnosticOperationContext(ctx context.Context) context.Context      { return ctx }
func diagnosticAttemptContext(ctx context.Context, _ int) context.Context { return ctx }
func diagnosticWrapRows(rows *sql.Rows, _ context.Context, _ string, _ diagnosticSpan) *diagnosticRows {
	return rows
}
func diagnosticScanRows(_ context.Context, _ string, rows *sql.Rows, scan func(*sql.Rows) error) error {
	return scan(rows)
}
func diagnosticRollback(_ context.Context, tx *sql.Tx) error { return tx.Rollback() }
