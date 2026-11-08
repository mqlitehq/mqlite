//go:build mqlite_perf_diag

package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

func diagnosticCapture(t *testing.T) func() []DiagnosticEvent {
	t.Helper()
	var mu sync.Mutex
	var events []DiagnosticEvent
	restore := SetDiagnosticObserver(func(event DiagnosticEvent) {
		mu.Lock()
		events = append(events, event)
		mu.Unlock()
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := DiagnosticDrain(ctx); err != nil {
			t.Error(err)
		}
		restore()
	})
	return func() []DiagnosticEvent {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := DiagnosticDrain(ctx); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		defer mu.Unlock()
		return append([]DiagnosticEvent(nil), events...)
	}
}

func TestDiagnosticEventsPreserveContextAndRedact(t *testing.T) {
	read := diagnosticCapture(t)
	type key struct{}
	parent, cancel := context.WithTimeout(context.WithValue(context.Background(), key{}, "original"), 10*time.Second)
	defer cancel()
	ctx := DiagnosticContext(parent, "send-1")
	wantDeadline, _ := parent.Deadline()
	script := &diagnosticScript{checkContext: func(got context.Context) {
		deadline, ok := got.Deadline()
		if !ok || !deadline.Equal(wantDeadline) || got.Done() != parent.Done() || got.Value(key{}) != "original" {
			t.Error("instrumentation changed context cancellation, deadline or values")
		}
	}}
	e := newDiagnosticScriptEngine(t, script)
	err := e.inTx(ctx, func(ctx context.Context, tx *txn) error {
		_, err := tx.ExecContext(ctx, "UPDATE secret_table SET secret='must-not-appear'", "private argument")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	events := read()
	seen := map[string]bool{}
	for _, event := range events {
		if event.OperationID != "send-1" || (event.Attempt != 1 && event.Phase != "transaction_total") || event.ElapsedMS < 0 {
			t.Fatalf("invalid event fields: %+v", event)
		}
		seen[event.Phase] = true
	}
	for _, phase := range []string{"attempt_start", "begin_total", "sql_exec", "transaction_callback", "commit_mutex_wait", "commit"} {
		if !seen[phase] {
			t.Errorf("missing phase %s", phase)
		}
	}
	encoded, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"secret_table", "must-not-appear", "private argument"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("diagnostic leaked %s", secret)
		}
	}
	if got := diagnosticSafeID("https://secret.invalid/token"); strings.Contains(got, "secret") || !strings.HasPrefix(got, "op-") {
		t.Fatalf("unsafe identifier = %q", got)
	}
	if got := diagnosticErrorClass(errors.New("secret provider response")); got != "other" {
		t.Fatalf("error class = %s", got)
	}
}

func TestDiagnosticEventsObserveLateRowsAndScanErrors(t *testing.T) {
	for _, mode := range []string{"rows", "scan", "scan-callback"} {
		t.Run(mode, func(t *testing.T) {
			read := diagnosticCapture(t)
			script := &diagnosticScript{}
			if mode != "scan" {
				script.rowsErr = io.ErrUnexpectedEOF
			} else {
				script.value = "secret-not-an-integer"
			}
			e := newDiagnosticScriptEngine(t, script)
			ctx := DiagnosticContext(context.Background(), "query-1")
			if mode == "scan-callback" {
				// Observe Rows.Err even if a caller's scan callback neglects it;
				// diagnostics must not silently change the existing returned value.
				err := e.db.queryFreshMode(ctx, "SELECT value FROM fixture", func() []any { return nil }, func(rows *sql.Rows) error {
					for rows.Next() {
					}
					return nil
				}, false)
				if err != nil {
					t.Fatalf("instrumentation changed callback result: %v", err)
				}
			} else {
				_ = e.inTx(ctx, func(ctx context.Context, tx *txn) error {
					rows, err := tx.QueryContext(ctx, "SELECT value FROM fixture")
					if err != nil {
						return err
					}
					defer rows.Close()
					if mode == "rows" {
						rows.Next()
						if !errors.Is(rows.Err(), io.ErrUnexpectedEOF) {
							t.Fatal("missing late row error")
						}
						return errors.New("callback stops without retry")
					}
					var value int
					if !rows.Next() {
						t.Fatal("missing fixture row")
					}
					return rows.Scan(&value)
				})
			}
			var matched bool
			for _, event := range read() {
				if event.ErrorClass == "unexpected_eof" && (event.Phase == "sql_rows_lifetime" || event.Phase == "sql_rows_scan") || mode == "scan" && event.Phase == "sql_scan" && event.ErrorClass == "other" {
					matched = true
				}
			}
			if !matched {
				t.Fatal("late Rows/Scan failure was not observed")
			}
		})
	}
}

func TestDiagnosticObserverNeverBlocksSQL(t *testing.T) {
	entered := make(chan struct{})
	unblock := make(chan struct{})
	var first sync.Once
	restore := SetDiagnosticObserver(func(DiagnosticEvent) {
		first.Do(func() { close(entered) })
		<-unblock
	})
	defer restore()
	var release sync.Once
	defer release.Do(func() { close(unblock) })
	ctx := DiagnosticContext(context.Background(), "nonblocking-1")
	diagnosticMark(ctx, "test", "", nil)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("observer did not start")
	}
	before := DiagnosticObserverDropped()
	for i := 0; i < cap(diagnosticQueue)+16; i++ {
		diagnosticMark(ctx, "test", "", nil)
	}
	script := &diagnosticScript{}
	e := newDiagnosticScriptEngine(t, script)
	done := make(chan error, 1)
	go func() {
		done <- e.inTx(ctx, func(ctx context.Context, tx *txn) error {
			_, err := tx.ExecContext(ctx, "UPDATE fixture SET value=1")
			return err
		})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("observer blocked SQL")
	}
	if DiagnosticObserverDropped() <= before {
		t.Fatal("measurement loss was not observable")
	}
	drainCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	if err := DiagnosticDrain(drainCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked drain=%v", err)
	}
	cancel()
	release.Do(func() { close(unblock) })
	drainCtx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := DiagnosticDrain(drainCtx); err != nil {
		t.Fatal(err)
	}
}

func TestDiagnosticObserverConcurrencyAndPanic(t *testing.T) {
	read := diagnosticCapture(t)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 32; n++ {
				diagnosticMark(DiagnosticContext(context.Background(), "parallel-1"), "test", "SELECT 1", nil)
			}
		}()
	}
	wg.Wait()
	if got := len(read()); got != 512 {
		t.Fatalf("delivered=%d", got)
	}
	before := DiagnosticObserverDropped()
	restore := SetDiagnosticObserver(func(DiagnosticEvent) { panic("private observer detail") })
	diagnosticMark(DiagnosticContext(context.Background(), "panic-1"), "test", "", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := DiagnosticDrain(ctx); err != nil {
		t.Fatal(err)
	}
	restore()
	if DiagnosticObserverDropped() != before+1 {
		t.Fatal("observer panic was not counted as measurement loss")
	}
}

func TestDiagnosticPoolAndSelectOneAreScoped(t *testing.T) {
	script := &diagnosticScript{}
	e := newDiagnosticScriptEngine(t, script)
	for _, n := range []int{0, -1, 65} {
		if !errors.Is(e.DiagnosticSetPoolLimit(n), ErrInvalidArgument) {
			t.Fatalf("accepted pool limit %d", n)
		}
	}
	if err := e.DiagnosticSetPoolLimit(2); err != nil || e.DiagnosticPoolStats().MaxOpenConnections != 2 {
		t.Fatalf("pool setting=%v stats=%+v", err, e.DiagnosticPoolStats())
	}
	if len(script.snapshot()) != 0 {
		t.Fatal("pool observation/settings issued SQL")
	}
	if err := e.DiagnosticSelectOne(DiagnosticContext(context.Background(), "baseline-1")); err != nil {
		t.Fatal(err)
	}
	want := []string{"QUERY:SELECT 1", "ROWS_CLOSE"}
	if got := script.snapshot(); strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("baseline SQL=%v", got)
	}
	e.db.remote = false
	if !errors.Is(e.DiagnosticSetPoolLimit(2), ErrInvalidArgument) {
		t.Fatal("local single-writer invariant was adjustable")
	}
}
