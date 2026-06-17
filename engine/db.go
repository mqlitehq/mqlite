package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	// Pure-Go drivers, no CGO (design D2 default + L-Turso remote).
	_ "github.com/tursodatabase/libsql-client-go/libsql" // driver "libsql" (remote Turso, Hrana)
	_ "modernc.org/sqlite"                               // driver "sqlite"  (local file/:memory:)
)

// db wraps a single-connection *sql.DB. MaxOpenConns(1) IS the single writer
// (design D3): database/sql serializes every caller onto one connection, so
// there is zero file-level write-lock contention and claims stay atomic.
type db struct {
	sql    *sql.DB
	remote bool
}

// resolveDSN turns the user-facing DB string + optional auth token into a
// (driver, connection-string) pair. The token is injected here from the
// environment and is NEVER part of the compiled source.
func resolveDSN(dsn, token, sync string) (driver, conn string, remote bool) {
	low := strings.ToLower(strings.TrimSpace(dsn))
	switch {
	case strings.HasPrefix(low, "libsql://"),
		strings.HasPrefix(low, "wss://"),
		strings.HasPrefix(low, "ws://"):
		conn = dsn
		if token != "" {
			sep := "?"
			if strings.Contains(conn, "?") {
				sep = "&"
			}
			conn += sep + "authToken=" + url.QueryEscape(token)
		}
		return "libsql", conn, true
	}

	// PRAGMA synchronous: NORMAL is the durable+fast default for WAL.
	syncMode := strings.ToUpper(strings.TrimSpace(sync))
	switch syncMode {
	case "FULL", "OFF", "NORMAL", "EXTRA":
	default:
		syncMode = "NORMAL"
	}

	// local modernc SQLite. Apply pragmas via DSN so they hold on the conn.
	if low == ":memory:" || low == "" {
		return "sqlite", "file::memory:?_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)", false
	}
	path := strings.TrimPrefix(dsn, "file:")
	pragmas := "_pragma=journal_mode(WAL)&_pragma=synchronous(" + syncMode + ")" +
		"&_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)&_pragma=temp_store(MEMORY)"
	return "sqlite", "file:" + path + "?" + pragmas, false
}

func openDB(ctx context.Context, dsn, token, sync string) (*db, error) {
	driver, conn, remote := resolveDSN(dsn, token, sync)
	sdb, err := sql.Open(driver, conn)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", driver, err)
	}
	if remote {
		// Remote Turso/libSQL: the broker is still the single logical writer, but
		// the Turso primary serializes writes for us, so a tiny pool is safe and
		// far more resilient — Turso closes idle Hrana streams, and a stale stream
		// surfaces as a (wrapped) bad-connection error. A short idle timeout makes
		// database/sql recycle idle conns instead of reusing a server-closed stream.
		sdb.SetMaxOpenConns(4)
		sdb.SetMaxIdleConns(2)
		sdb.SetConnMaxIdleTime(3 * time.Second)
		sdb.SetConnMaxLifetime(55 * time.Second)
	} else {
		// Local file/:memory: — one connection IS the single writer (no file-lock
		// contention, atomic claims).
		sdb.SetMaxOpenConns(1)
		sdb.SetMaxIdleConns(1)
		sdb.SetConnMaxLifetime(0)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := sdb.PingContext(pingCtx); err != nil {
		sdb.Close()
		return nil, fmt.Errorf("ping %s: %w", driver, err)
	}
	return &db{sql: sdb, remote: remote}, nil
}

// migrate creates tables/indexes idempotently and records the schema version.
func (d *db) migrate(ctx context.Context) error {
	for _, stmt := range schemaStmts {
		if _, err := d.exec(ctx, stmt); err != nil {
			return fmt.Errorf("schema: %w\n%s", err, firstLine(stmt))
		}
	}
	if _, err := d.exec(ctx,
		`INSERT OR IGNORE INTO meta(key,value) VALUES ('schema_version', ?)`, schemaVersion); err != nil {
		return fmt.Errorf("schema version: %w", err)
	}
	return nil
}

func (d *db) close() error { return d.sql.Close() }

// ── connection-error resilience (remote only) ───────────────────────────────
//
// Turso closes idle Hrana streams; libsql-client-go then returns a *wrapped*
// driver.ErrBadConn ("stream is closed: driver: bad connection"). Because it is
// wrapped, database/sql will not transparently retry it. A closed stream means
// the statement never reached the server, so retrying on a fresh pooled
// connection is safe (no double-execution). Local SQLite never retries.

const maxConnAttempts = 3

func isConnErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, driver.ErrBadConn) {
		return true
	}
	s := err.Error()
	for _, sub := range []string{"bad connection", "stream is closed", "stream closed", "connection reset"} {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

func (d *db) attempts() int {
	if d.remote {
		return maxConnAttempts
	}
	return 1
}

func (d *db) retryable(err error) bool { return d.remote && isConnErr(err) }

func (d *db) exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	var res sql.Result
	var err error
	for i := 0; i < d.attempts(); i++ {
		res, err = d.sql.ExecContext(ctx, query, args...)
		if err == nil || !d.retryable(err) {
			return res, err
		}
	}
	return res, err
}

func (d *db) query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	var rows *sql.Rows
	var err error
	for i := 0; i < d.attempts(); i++ {
		rows, err = d.sql.QueryContext(ctx, query, args...)
		if err == nil || !d.retryable(err) {
			return rows, err
		}
	}
	return rows, err
}

// queryRowScan runs a single-row query and scans it, retrying connection errors.
// Returns sql.ErrNoRows when there is no row.
func (d *db) queryRowScan(ctx context.Context, dest []any, query string, args ...any) error {
	var err error
	for i := 0; i < d.attempts(); i++ {
		var rows *sql.Rows
		rows, err = d.sql.QueryContext(ctx, query, args...)
		if err != nil {
			if d.retryable(err) {
				continue
			}
			return err
		}
		if !rows.Next() {
			cerr := rows.Err()
			rows.Close()
			if cerr != nil {
				err = cerr
				if d.retryable(cerr) {
					continue
				}
				return cerr
			}
			return sql.ErrNoRows
		}
		err = rows.Scan(dest...)
		rows.Close()
		if err != nil && d.retryable(err) {
			continue
		}
		return err
	}
	return err
}

// inTx runs fn inside a transaction, retrying the whole transaction on a
// connection error (the aborted tx leaves nothing committed, so retry is safe).
func (e *Engine) inTx(ctx context.Context, fn func(*sql.Tx) error) error {
	var err error
	for i := 0; i < e.db.attempts(); i++ {
		var tx *sql.Tx
		tx, err = e.db.sql.BeginTx(ctx, nil)
		if err != nil {
			if e.db.retryable(err) {
				continue
			}
			return err
		}
		err = fn(tx)
		if err != nil {
			tx.Rollback()
			if e.db.retryable(err) {
				continue
			}
			return err
		}
		err = tx.Commit()
		if err != nil {
			if e.db.retryable(err) {
				continue
			}
			return err
		}
		return nil
	}
	return err
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
