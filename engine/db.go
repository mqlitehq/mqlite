package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
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
	lock   io.Closer // single-writer advisory lock on a local file DB (MQLITE-6); nil for :memory:/remote
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

// localFilePath returns the on-disk path of a local file DSN, or ok=false for an
// in-memory DB (which needs no single-writer lock — each :memory: is its own DB).
func localFilePath(dsn string) (string, bool) {
	low := strings.ToLower(strings.TrimSpace(dsn))
	if low == "" || strings.Contains(low, ":memory:") {
		return "", false
	}
	return strings.TrimPrefix(dsn, "file:"), true
}

func openDB(ctx context.Context, dsn, token, sync string) (*db, error) {
	driver, conn, remote := resolveDSN(dsn, token, sync)

	// Single-writer guard (MQLITE-6): a local file DB may be opened by only one
	// process at a time — two writers would race on crash recovery and claims.
	// Exempt :memory: (private per handle) and remote Turso (serialized server-side).
	// Lock the sidecar, not the DB file, so SQLite's own locking is untouched; the
	// OS releases it on process exit, so a crash never leaves a stale lock.
	var lock io.Closer
	if !remote {
		if path, ok := localFilePath(dsn); ok {
			l, err := acquireFileLock(path + ".lock")
			if err != nil {
				return nil, err
			}
			lock = l
		}
	}

	sdb, err := sql.Open(driver, conn)
	if err != nil {
		if lock != nil {
			_ = lock.Close()
		}
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
		if lock != nil {
			_ = lock.Close()
		}
		return nil, fmt.Errorf("ping %s: %w", driver, err)
	}
	return &db{sql: sdb, remote: remote, lock: lock}, nil
}

// initSchema creates the tables/indexes idempotently (CREATE IF NOT EXISTS) and
// records the schema token. mqlite keeps a single canonical schema and does not
// migrate: a database whose recorded token differs is refused (never altered in
// place), so a stale experimental DB is simply recreated.
func (d *db) initSchema(ctx context.Context) error {
	if v, ok, err := d.recordedSchemaVersion(ctx); err != nil {
		return err
	} else if ok && v != schemaVersion {
		return fmt.Errorf("%w: database schema is %q but this build expects %q; "+
			"recreate it (delete the file / drop the tables) — mqlite keeps a single schema and does not migrate",
			ErrSchemaVersionMismatch, v, schemaVersion)
	}
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

// recordedSchemaVersion returns the schema_version stored in meta, or ok=false for
// a fresh database (the meta table or its row does not exist yet).
func (d *db) recordedSchemaVersion(ctx context.Context) (string, bool, error) {
	var v string
	err := d.queryRowScan(ctx, []any{&v}, `SELECT value FROM meta WHERE key='schema_version'`)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) || isNoSuchTable(err) {
			return "", false, nil
		}
		return "", false, err
	}
	return v, true, nil
}

func isNoSuchTable(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no such table")
}

func (d *db) close() error {
	err := d.sql.Close()
	if d.lock != nil {
		if e := d.lock.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

// ── remote write resilience (Turso/libSQL only) ─────────────────────────────
//
// Two transient remote failures are retried (a local SQLite single writer never
// retries):
//   - a closed idle Hrana stream surfaces as a wrapped driver.ErrBadConn ("stream
//     is closed") — the statement never reached the server, so a retry on a fresh
//     pooled connection can't double-execute; and
//   - a contended write surfaces as "database is locked" (SQLITE_BUSY) — the lock
//     was never acquired so nothing ran, and a retry after a short backoff is
//     equally safe.
//
// The remote pool is small (4) but the Turso primary serializes writes, so a burst
// of concurrent enqueues races for the write lock; bounded retry + backoff lets
// them through instead of erroring (the MQLITE-4 pool-vs-single-writer tension).

const maxConnAttempts = 6

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

// isBusyErr reports a contended-write error from the remote primary. The lock was
// not acquired, so the statement did not run and a retry is safe (no double-apply).
func isBusyErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	for _, sub := range []string{"database is locked", "database table is locked", "SQLITE_BUSY"} {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// backoff pauses before a retry so a contended remote write doesn't immediately
// re-collide; it escalates a little per attempt and honors ctx cancellation.
func (d *db) backoff(ctx context.Context, attempt int) {
	t := time.NewTimer(time.Duration(attempt) * 40 * time.Millisecond)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func (d *db) attempts() int {
	if d.remote {
		return maxConnAttempts
	}
	return 1
}

func (d *db) retryable(err error) bool {
	return d.remote && (isConnErr(err) || isBusyErr(err))
}

func (d *db) exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	var res sql.Result
	var err error
	for i := 0; i < d.attempts(); i++ {
		if i > 0 {
			d.backoff(ctx, i)
		}
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
		if i > 0 {
			d.backoff(ctx, i)
		}
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
		if i > 0 {
			d.backoff(ctx, i)
		}
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
		if i > 0 {
			e.db.backoff(ctx, i)
		}
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
			_ = tx.Rollback()
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
