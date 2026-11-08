package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"
)

// These fixtures run in both builds to keep instrumentation out of SQL policy.
type diagnosticScript struct {
	mu           sync.Mutex
	calls        []string
	failExec     int
	commitErr    error
	rowsErr      error
	value        driver.Value
	checkContext func(context.Context)
}

func (s *diagnosticScript) record(call string) {
	s.mu.Lock()
	s.calls = append(s.calls, call)
	s.mu.Unlock()
}
func (s *diagnosticScript) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

type diagnosticConnector struct{ script *diagnosticScript }

func (c diagnosticConnector) Connect(context.Context) (driver.Conn, error) {
	return &diagnosticConn{script: c.script}, nil
}
func (diagnosticConnector) Driver() driver.Driver { return nil }

type diagnosticConn struct{ script *diagnosticScript }

func (*diagnosticConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("fixture does not prepare")
}
func (*diagnosticConn) Close() error { return nil }
func (c *diagnosticConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}
func (c *diagnosticConn) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	c.script.record("BEGIN")
	if c.script.checkContext != nil {
		c.script.checkContext(ctx)
	}
	return &diagnosticTx{script: c.script}, nil
}
func (c *diagnosticConn) ExecContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.script.record("EXEC:" + query)
	if c.script.checkContext != nil {
		c.script.checkContext(ctx)
	}
	c.script.mu.Lock()
	defer c.script.mu.Unlock()
	if c.script.failExec > 0 {
		c.script.failExec--
		return nil, diagnosticBusyError{}
	}
	return driver.RowsAffected(1), nil
}
func (c *diagnosticConn) QueryContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.script.record("QUERY:" + query)
	if c.script.checkContext != nil {
		c.script.checkContext(ctx)
	}
	return &diagnosticDriverRows{script: c.script}, nil
}

type diagnosticTx struct{ script *diagnosticScript }

func (tx *diagnosticTx) Commit() error {
	tx.script.record("COMMIT")
	return tx.script.commitErr
}
func (tx *diagnosticTx) Rollback() error { tx.script.record("ROLLBACK"); return nil }

type diagnosticBusyError struct{}

func (diagnosticBusyError) Error() string { return "database is locked: fixture secret busy body" }
func (diagnosticBusyError) Code() int     { return 5 }

type diagnosticDriverRows struct {
	script *diagnosticScript
	done   bool
}

func (*diagnosticDriverRows) Columns() []string { return []string{"value"} }
func (rows *diagnosticDriverRows) Close() error { rows.script.record("ROWS_CLOSE"); return nil }
func (rows *diagnosticDriverRows) Next(dest []driver.Value) error {
	if rows.script.rowsErr != nil {
		return rows.script.rowsErr
	}
	if rows.done {
		return io.EOF
	}
	rows.done = true
	value := rows.script.value
	if value == nil {
		value = int64(7)
	}
	dest[0] = value
	return nil
}

func newDiagnosticScriptEngine(t *testing.T, script *diagnosticScript) *Engine {
	t.Helper()
	sdb := sql.OpenDB(diagnosticConnector{script: script})
	t.Cleanup(func() { _ = sdb.Close() })
	return fakeRemoteEngine(&db{sql: sdb, remote: true})
}

func TestDiagnosticHooksPreserveSQLSequence(t *testing.T) {
	for _, commitErr := range []error{nil, io.EOF} {
		t.Run("commit-"+diagnosticTestErrorName(commitErr), func(t *testing.T) {
			script := &diagnosticScript{commitErr: commitErr}
			e := newDiagnosticScriptEngine(t, script)
			callbacks := 0
			err := e.inTx(context.Background(), func(ctx context.Context, tx *txn) error {
				callbacks++
				if _, err := tx.ExecContext(ctx, "UPDATE fixture SET value=?", 7); err != nil {
					return err
				}
				var value int
				return tx.QueryRowContext(ctx, "SELECT value FROM fixture").Scan(&value)
			})
			if commitErr == nil && err != nil || commitErr != nil && !errors.Is(err, ErrOutcomeUnknown) {
				t.Fatalf("commit result = %v", err)
			}
			if callbacks != 1 {
				t.Fatalf("callback replayed %d times", callbacks)
			}
			want := []string{"BEGIN", "EXEC:UPDATE fixture SET value=?", "QUERY:SELECT value FROM fixture", "ROWS_CLOSE", "COMMIT"}
			if got := script.snapshot(); !reflect.DeepEqual(got, want) {
				t.Fatalf("SQL sequence = %v, want %v", got, want)
			}
		})
	}
}

func diagnosticTestErrorName(err error) string {
	if err == nil {
		return "success"
	}
	return "unknown"
}

func TestDiagnosticHooksPreservePrecommitRetry(t *testing.T) {
	script := &diagnosticScript{failExec: 2}
	e := newDiagnosticScriptEngine(t, script)
	callbacks := 0
	err := e.inTx(context.Background(), func(ctx context.Context, tx *txn) error {
		callbacks++
		_, err := tx.ExecContext(ctx, "UPDATE fixture SET value=1")
		return err
	})
	if err != nil || callbacks != 3 {
		t.Fatalf("result=%v callbacks=%d", err, callbacks)
	}
	want := []string{"BEGIN", "EXEC:UPDATE fixture SET value=1", "ROLLBACK", "BEGIN", "EXEC:UPDATE fixture SET value=1", "ROLLBACK", "BEGIN", "EXEC:UPDATE fixture SET value=1", "COMMIT"}
	if got := script.snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("SQL sequence=%v, want %v", got, want)
	}
}

func TestDiagnosticHooksPreserveRowsFailure(t *testing.T) {
	script := &diagnosticScript{rowsErr: io.ErrUnexpectedEOF}
	e := newDiagnosticScriptEngine(t, script)
	sentinel := errors.New("application callback failure")
	err := e.inTx(context.Background(), func(ctx context.Context, tx *txn) error {
		rows, err := tx.QueryContext(ctx, "SELECT value FROM fixture")
		if err != nil {
			return err
		}
		defer rows.Close()
		if rows.Next() || !errors.Is(rows.Err(), io.ErrUnexpectedEOF) {
			t.Fatal("row error changed")
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("callback result=%v", err)
	}
	want := []string{"BEGIN", "QUERY:SELECT value FROM fixture", "ROWS_CLOSE", "ROLLBACK"}
	if got := script.snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("SQL sequence=%v, want %v", got, want)
	}
}
