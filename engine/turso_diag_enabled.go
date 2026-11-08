//go:build mqlite_perf_diag

package engine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// DiagnosticEvent belongs to an isolated diagnostic build, not the product API.
// begin_total includes database/sql acquisition plus driver BEGIN. sql_rows_lifetime
// includes consumer time and overlaps sql_query/sql_scan; these are not additive.
// DBStats.WaitDuration is aggregate pool wait, not an exact per-operation split.
// sql_query outside a transaction can include acquisition; total phases and scan
// callbacks spanning a logical read may use Attempt=0. No event proves dispatch.
// Raw SQL exposed by EngineTx.SQL bypasses per-statement hooks.
type DiagnosticEvent struct {
	OperationID string
	Attempt     int
	Phase       string
	SQLTag      string
	ElapsedMS   float64
	ErrorClass  string
}

type diagnosticIdentity struct {
	id      string
	attempt int
}
type diagnosticIdentityKey struct{}
type diagnosticObserver struct{ fn func(DiagnosticEvent) }
type diagnosticDelivery struct {
	observer *diagnosticObserver
	event    DiagnosticEvent
	barrier  chan struct{}
}

var diagnosticCurrent atomic.Pointer[diagnosticObserver]
var diagnosticDropped atomic.Uint64
var diagnosticIDs atomic.Uint64
var diagnosticQueueOnce sync.Once
var diagnosticQueue = make(chan diagnosticDelivery, 8192)

// DiagnosticContext preserves the parent's values, cancellation and deadline.
// IDs outside a small opaque alphabet are hashed rather than copied to output.
func DiagnosticContext(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, diagnosticIdentityKey{}, diagnosticIdentity{id: diagnosticSafeID(id)})
}

func diagnosticSafeID(id string) string {
	valid := len(id) > 0 && len(id) <= 96
	for _, c := range id {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("._:-", c)) {
			valid = false
		}
	}
	if valid {
		return id
	}
	hash := sha256.Sum256([]byte(id))
	return "op-" + hex.EncodeToString(hash[:8])
}

func diagnosticStartQueue() {
	diagnosticQueueOnce.Do(func() {
		go func() {
			for delivery := range diagnosticQueue {
				if delivery.barrier != nil {
					close(delivery.barrier)
					continue
				}
				func() {
					defer func() {
						if recover() != nil {
							diagnosticDropped.Add(1)
						}
					}()
					delivery.observer.fn(delivery.event)
				}()
			}
		}()
	})
}

// SetDiagnosticObserver installs a nonblocking-to-SQL observer. Delivery is
// asynchronous and bounded; a blocked/panicking observer drops measurements,
// never blocks a database statement. Call DiagnosticDrain after joining work and
// compare DiagnosticObserverDropped before declaring instrumentation complete.
// Restore only changes the current registration; queued events retain theirs.
func SetDiagnosticObserver(fn func(DiagnosticEvent)) func() {
	diagnosticStartQueue()
	current := &diagnosticObserver{fn: fn}
	previous := diagnosticCurrent.Swap(current)
	var once sync.Once
	return func() { once.Do(func() { diagnosticCurrent.CompareAndSwap(current, previous) }) }
}

// DiagnosticObserverDropped is a process-wide measurement-loss counter.
func DiagnosticObserverDropped() uint64 { return diagnosticDropped.Load() }

// DiagnosticDrain waits for delivery of events queued before its barrier. It
// does not join in-flight SQL, driver background Close calls or future emitters.
func DiagnosticDrain(ctx context.Context) error {
	diagnosticStartQueue()
	done := make(chan struct{})
	select {
	case diagnosticQueue <- diagnosticDelivery{barrier: done}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func diagnosticOperationContext(ctx context.Context) context.Context {
	if observer := diagnosticCurrent.Load(); observer == nil || observer.fn == nil {
		return ctx
	}
	if identity, ok := ctx.Value(diagnosticIdentityKey{}).(diagnosticIdentity); ok && identity.id != "" {
		return ctx
	}
	return DiagnosticContext(ctx, "internal-"+strconv.FormatUint(diagnosticIDs.Add(1), 10))
}

func diagnosticAttemptContext(ctx context.Context, attempt int) context.Context {
	ctx = diagnosticOperationContext(ctx)
	identity, ok := ctx.Value(diagnosticIdentityKey{}).(diagnosticIdentity)
	if !ok {
		return ctx
	}
	identity.attempt = attempt
	return context.WithValue(ctx, diagnosticIdentityKey{}, identity)
}

type diagnosticSpan struct {
	observer *diagnosticObserver
	event    DiagnosticEvent
	started  time.Time
}

func diagnosticStart(ctx context.Context, phase, query string) diagnosticSpan {
	observer := diagnosticCurrent.Load()
	if observer == nil || observer.fn == nil {
		return diagnosticSpan{}
	}
	identity, _ := ctx.Value(diagnosticIdentityKey{}).(diagnosticIdentity)
	return diagnosticSpan{observer: observer, event: DiagnosticEvent{
		OperationID: identity.id, Attempt: identity.attempt, Phase: phase, SQLTag: diagnosticSQLTag(query),
	}, started: time.Now()}
}

func (span diagnosticSpan) end(err error) {
	if span.observer == nil {
		return
	}
	event := span.event
	event.ElapsedMS = float64(time.Since(span.started)) / float64(time.Millisecond)
	event.ErrorClass = diagnosticErrorClass(err)
	select {
	case diagnosticQueue <- diagnosticDelivery{observer: span.observer, event: event}:
	default:
		diagnosticDropped.Add(1)
	}
}

func diagnosticMark(ctx context.Context, phase, query string, err error) {
	diagnosticStart(ctx, phase, query).end(err)
}

func diagnosticErrorClass(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, ErrOutcomeUnknown):
		return "outcome_unknown"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, driver.ErrBadConn):
		return "bad_connection"
	case errors.Is(err, sql.ErrNoRows):
		return "no_rows"
	case errors.Is(err, sql.ErrTxDone):
		return "transaction_done"
	case errors.Is(err, io.EOF):
		return "eof"
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "unexpected_eof"
	case errors.Is(err, ErrClosed):
		return "closed"
	}
	var coded interface{ Code() int }
	if errors.As(err, &coded) {
		if coded.Code()&255 == 5 {
			return "typed_sqlite_busy"
		}
		return "typed_database_error"
	}
	var network net.Error
	if errors.As(err, &network) && network.Timeout() {
		return "network_timeout"
	}
	return "other"
}

func diagnosticSQLTag(query string) string {
	if query == "" {
		return ""
	}
	u := strings.ToUpper(strings.Join(strings.Fields(query), " "))
	for _, item := range []struct{ prefix, contains, tag string }{
		{"SELECT 1", "", "select_one"},
		{"SELECT", "FROM DEDUP", "dedup_read"},
		{"SELECT", "FROM RECEIVE_ATTEMPTS", "attempt_read"},
		{"SELECT", "FROM SETTLEMENT_RECEIPTS", "receipt_read"},
		{"SELECT", "FROM QUEUES", "queue_read"},
		{"INSERT INTO MESSAGES", "", "message_insert"},
		{"INSERT INTO DEDUP", "", "dedup_write"},
		{"INSERT OR REPLACE INTO RECEIVE_ATTEMPTS", "", "attempt_write"},
		{"INSERT OR REPLACE INTO SETTLEMENT_RECEIPTS", "", "receipt_write"},
		{"UPDATE MESSAGES", "", "message_update"},
		{"DELETE FROM MESSAGES", "", "message_delete"},
	} {
		if item.prefix == "SELECT 1" && u != "SELECT 1" {
			continue
		}
		if strings.HasPrefix(u, item.prefix) && strings.Contains(u, item.contains) {
			return item.tag
		}
	}
	hash := sha256.Sum256([]byte(query))
	return "sql-" + hex.EncodeToString(hash[:8])
}

type diagnosticRows struct {
	*sql.Rows
	ctx   context.Context
	query string
	span  diagnosticSpan
	once  sync.Once
}

func diagnosticWrapRows(rows *sql.Rows, ctx context.Context, query string, querySpan diagnosticSpan) *diagnosticRows {
	if rows == nil {
		return nil
	}
	querySpan.event.Phase = "sql_rows_lifetime"
	return &diagnosticRows{Rows: rows, ctx: ctx, query: query, span: querySpan}
}

func (rows *diagnosticRows) finish(err error) { rows.once.Do(func() { rows.span.end(err) }) }
func (rows *diagnosticRows) Next() bool {
	ok := rows.Rows.Next()
	if !ok {
		rows.finish(rows.Rows.Err())
	}
	return ok
}
func (rows *diagnosticRows) NextResultSet() bool {
	ok := rows.Rows.NextResultSet()
	if !ok {
		rows.finish(rows.Rows.Err())
	}
	return ok
}
func (rows *diagnosticRows) Scan(dest ...any) error {
	span := diagnosticStart(rows.ctx, "sql_scan", rows.query)
	err := rows.Rows.Scan(dest...)
	span.end(err)
	if err != nil {
		rows.finish(err)
	}
	return err
}
func (rows *diagnosticRows) Err() error {
	err := rows.Rows.Err()
	if err != nil {
		rows.finish(err)
	}
	return err
}
func (rows *diagnosticRows) Close() error {
	err := rows.Rows.Close()
	observed := err
	if observed == nil {
		observed = rows.Rows.Err()
	}
	rows.finish(observed)
	return err
}

func diagnosticScanRows(ctx context.Context, query string, rows *sql.Rows, scan func(*sql.Rows) error) error {
	span := diagnosticStart(ctx, "sql_rows_scan", query)
	err := scan(rows)
	observed := err
	if observed == nil {
		observed = rows.Err()
	}
	span.end(observed)
	return err
}

func diagnosticRollback(ctx context.Context, tx *sql.Tx) error {
	span := diagnosticStart(ctx, "rollback", "")
	err := tx.Rollback()
	if !errors.Is(err, sql.ErrTxDone) {
		span.end(err) // ErrTxDone is a local no-op, not a provider ROLLBACK.
	}
	return err
}

// DiagnosticPoolStats reads only local database/sql counters.
func (e *Engine) DiagnosticPoolStats() sql.DBStats {
	if e == nil || e.db == nil || e.db.sql == nil {
		return sql.DBStats{}
	}
	return e.db.sql.Stats()
}

// DiagnosticSetPoolLimit is an isolated counterfactual for remote stores only.
// It never changes local SQLite's single-writer invariant.
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

// DiagnosticSelectOne measures only the ordinary fixed SELECT 1 read path.
func (e *Engine) DiagnosticSelectOne(ctx context.Context) error {
	if e == nil || e.db == nil {
		return ErrClosed
	}
	var one int
	return e.db.queryRowScan(ctx, []any{&one}, "SELECT 1")
}
