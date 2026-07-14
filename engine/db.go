package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"path/filepath"
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
	dsn    string    // the user-facing DB string (no auth token — that's only in the conn string)
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

	// PRAGMA synchronous: NORMAL is the durable+fast default for WAL. The value is
	// validated in openDB (validateSync) before we get here, so an unknown value has
	// already failed startup; "" falls through to the NORMAL default.
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
	// auto_vacuum=INCREMENTAL (set at creation, before any table exists) lets a new DB
	// return free pages to the OS via `PRAGMA incremental_vacuum` (the `vacuum` command /
	// Engine.Compact) without a full rewrite or a global lock — a fit for a queue that
	// churns. Existing DBs created without it keep their mode until a full VACUUM.
	pragmas := "_pragma=journal_mode(WAL)&_pragma=synchronous(" + syncMode + ")" +
		"&_pragma=auto_vacuum(INCREMENTAL)" +
		"&_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)&_pragma=temp_store(MEMORY)"
	return "sqlite", "file:" + path + "?" + pragmas, false
}

// localFilePath returns the on-disk path of a local file DSN, or ok=false for an
// in-memory DB (which needs no single-writer lock — each :memory: is its own DB).
//
// The path is CANONICALIZED (MQLITE-60 / review F5): the lock sidecar is keyed on
// it, so every spelling of the same physical file must derive the same .lock —
// otherwise two processes opening "./mq.db" and "/abs/dir/mq.db" each take their
// own lock and the single-writer guarantee is gone. DSN query options are not
// part of the file identity; Abs+Clean folds relative segments; EvalSymlinks
// resolves symlinked spellings once the target exists (a not-yet-created DB
// keeps the absolute path — the sidecar then lives next to the symlink, which
// still locks consistently for every opener using any non-symlinked spelling).
func localFilePath(dsn string) (string, bool) {
	low := strings.ToLower(strings.TrimSpace(dsn))
	if low == "" || strings.Contains(low, ":memory:") {
		return "", false
	}
	path := strings.TrimPrefix(strings.TrimSpace(dsn), "file:")
	if i := strings.IndexByte(path, '?'); i >= 0 {
		path = path[:i]
	}
	if abs, err := filepath.Abs(path); err == nil {
		path = abs
	}
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		path = resolved
	}
	return path, true
}

// validateSync rejects an unrecognized MQLITE_SYNC / Options.Synchronous value up front
// instead of silently falling back to NORMAL: a durability typo (FULL -> "FULLL") must
// not quietly weaken the guarantee the operator asked for — fail startup loudly, the same
// way a malformed MQLITE_TOKENS/bind does (MQLITE-88). "" means "use the default".
func validateSync(sync string) error {
	switch strings.ToUpper(strings.TrimSpace(sync)) {
	case "", "NORMAL", "FULL", "OFF", "EXTRA":
		return nil
	default:
		return fmt.Errorf("%w: unknown MQLITE_SYNC %q (want NORMAL, FULL, OFF or EXTRA)", ErrInvalidArgument, sync)
	}
}

func openDB(ctx context.Context, dsn, token, sync string) (*db, error) {
	if err := validateSync(sync); err != nil {
		return nil, err
	}
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
	return &db{sql: sdb, remote: remote, dsn: dsn, lock: lock}, nil
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

// isConnErr reports a dropped/half-open remote transport: the request may or may not
// have reached the primary. It gates both read retry (safe — reads are idempotent) and
// the write outcome-unknown signal. It deliberately spans the WHOLE transport-failure
// family, not just "connection reset": the libSQL/Hrana client returns the network error
// un-wrapped from the POST that carries a write, so a lost commit ack most often surfaces
// as a bare io.EOF / "broken pipe" / "i/o timeout" — especially behind a proxy like Fly,
// where an upstream drop reads as EOF, not RST (MQLITE-59). A structured server *response*
// ("error code 500: …") is a definite non-commit and is intentionally NOT matched here.
func isConnErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	s := err.Error()
	for _, sub := range []string{
		"bad connection", "stream is closed", "stream closed",
		"connection reset", "broken pipe", "i/o timeout", "unexpected EOF", "EOF",
	} {
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

// ─── cancellation on a LOCAL store ────────────────────────────────────────────
//
// Interrupting an in-flight statement on a local SQLite store LEAKS THE CONNECTION. The pool then
// reports zero open connections while the file stays locked, and every later statement fails with
// SQLITE_BUSY — permanently, because the leaked handle is no longer reachable from Go. On a
// `:memory:` store the same event destroys the database outright: it lives inside that connection,
// so afterwards every call answers "no such table: messages" (MQLITE-98). One root cause, two
// faces. Measured on a 200-cancellation storm: ~40% of runs wedge; with no interrupt, 0 of 8.
//
// So a local statement, once RUNNING, is not interrupted. That is a deliberate contract, and it is
// narrow:
//
//   - a caller who is ALREADY cancelled never starts a statement at all (checkCtx below);
//   - a caller WAITING for the single writer keeps its own context, so it walks away on cancel and
//     mutates nothing — that wait is the only place a local caller can actually be stuck;
//   - only a statement that is already EXECUTING runs to completion — microseconds on a local store,
//     since it does no network I/O.
//
// What we buy: cancelling a request can never destroy or wedge the database. What we pay: a write
// already in flight when the caller gives up may still commit. The caller must already tolerate
// that — mqlite is at-least-once and hands you `message_id` for exactly this — whereas a wedged or
// erased database is unrecoverable and takes everything with it.
//
// REMOTE stores keep full cancellation: their statements can genuinely block on the network, a
// discarded connection holds no local lock, and the Turso primary is the one enforcing serialization.
func (d *db) stmtCtx(ctx context.Context) context.Context {
	if d.remote {
		return ctx
	}
	return context.WithoutCancel(ctx)
}

// checkCtx refuses to begin work for a caller who has already given up. It is what keeps the
// contract above narrow: nothing is started, so nothing can commit late.
func checkCtx(ctx context.Context) error { return ctx.Err() }

// withConn acquires the connection with the CALLER's context — so waiting for the single writer
// stays cancellable and a queued caller mutates nothing — and then runs fn with the execution
// context from stmtCtx.
func (d *db) withConn(ctx context.Context, fn func(execCtx context.Context, c *sql.Conn) error) error {
	if err := checkCtx(ctx); err != nil {
		return err
	}
	conn, err := d.sql.Conn(ctx) // cancellable WAIT
	if err != nil {
		return err
	}
	defer conn.Close()
	return fn(d.stmtCtx(ctx), conn)
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

// retryableWrite is retryable for a WRITE or COMMIT: safe to replay only when the error
// guarantees the statement never applied — driver.ErrBadConn (database/sql's "retry on a
// fresh connection" signal) or a busy error (the lock was never acquired). A broad transport
// error like "connection reset" on a write is outcome-unknown (the primary may have committed
// before the ack was lost), so it is NOT replayed — the caller gets ErrOutcomeUnknown instead
// of a silent double-apply (MQLITE-59).
func (d *db) retryableWrite(err error) bool {
	return d.remote && (errors.Is(err, driver.ErrBadConn) || isBusyErr(err))
}

func (d *db) exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if !d.remote { // local: no retries; the wait is cancellable, the running statement is not
		var res sql.Result
		err := d.withConn(ctx, func(ec context.Context, c *sql.Conn) error {
			var e error
			res, e = c.ExecContext(ec, query, args...)
			return e
		})
		return res, err
	}
	var res sql.Result
	var err error
	for i := 0; i < d.attempts(); i++ {
		if i > 0 {
			d.backoff(ctx, i)
		}
		res, err = d.sql.ExecContext(ctx, query, args...)
		if err == nil || !d.retryableWrite(err) {
			break
		}
	}
	// A remote write that ended on an outcome-unknown transport error (a broad "connection
	// reset", not driver.ErrBadConn/busy) may have applied server-side before the ack was
	// lost — surface it as ErrOutcomeUnknown so the caller can't blindly retry into a
	// double-apply (MQLITE-59).
	if err != nil && d.remote && isConnErr(err) && !d.retryableWrite(err) {
		// %s (not %w) on purpose: the raw transport error must stay OUT of the
		// errors.Is chain — a caller checks errors.Is(err, ErrOutcomeUnknown), never
		// the underlying EOF/reset (which reads as "safe to retry").
		return res, fmt.Errorf("%w: %s", ErrOutcomeUnknown, err.Error())
	}
	return res, err
}

// execFresh / queryFresh are exec/query for a statement whose ARGUMENTS are time-sensitive:
// buildArgs is called again for every attempt, and — crucially — only once a connection is
// already in hand.
//
// Both halves matter. A remote retry backs off for up to hundreds of milliseconds, so arguments
// computed once, before the loop, are stale by the time a later attempt lands. But `DB.Exec`
// itself can also block waiting for a free connection and will transparently retry a bad
// connection several times, REUSING the arguments it was given — so building them before that
// call is not enough either. Each attempt therefore takes a *sql.Conn first, builds its arguments
// against the clock at that moment, and runs the statement on that specific connection; a bad
// connection surfaces to OUR loop, which retries with a fresh conn and fresh arguments.
//
// For a lock deadline (`locked_until = now + lockDuration`) this is not cosmetic: the write can
// otherwise commit a lease that has ALREADY EXPIRED, while still reporting success, and the
// reaper then reclaims the message immediately (MQLITE-97).
func (d *db) execFresh(ctx context.Context, query string, buildArgs func() []any) (sql.Result, error) {
	if !d.remote { // local: one attempt; the wait is cancellable, the running statement is not
		var res sql.Result
		err := d.withConn(ctx, func(ec context.Context, c *sql.Conn) error {
			var e error
			res, e = c.ExecContext(ec, query, buildArgs()...)
			return e
		})
		return res, err
	}
	var res sql.Result
	var err error
	for i := 0; i < d.attempts(); i++ {
		if i > 0 {
			d.backoff(ctx, i)
		}
		var conn *sql.Conn
		conn, err = d.sql.Conn(ctx) // wait for a connection BEFORE reading the clock
		if err != nil {
			if d.retryableWrite(err) {
				continue
			}
			break
		}
		res, err = conn.ExecContext(ctx, query, buildArgs()...)
		_ = conn.Close()
		if err == nil || !d.retryableWrite(err) {
			break
		}
	}
	if err != nil && d.remote && isConnErr(err) && !d.retryableWrite(err) {
		return res, fmt.Errorf("%w: %s", ErrOutcomeUnknown, err.Error())
	}
	return res, err
}

// queryFresh hands its rows to scan rather than returning them, because the rows are bound to the
// connection this attempt holds: the caller cannot be trusted to consume them before we release
// it.
func (d *db) queryFresh(ctx context.Context, query string, buildArgs func() []any, scan func(*sql.Rows) error) error {
	if !d.remote {
		return d.withConn(ctx, func(ec context.Context, c *sql.Conn) error {
			rows, qerr := c.QueryContext(ec, query, buildArgs()...)
			if qerr != nil {
				return qerr
			}
			serr := scan(rows)
			_ = rows.Close()
			return serr
		})
	}
	var err error
	for i := 0; i < d.attempts(); i++ {
		if i > 0 {
			d.backoff(ctx, i)
		}
		var conn *sql.Conn
		conn, err = d.sql.Conn(ctx) // wait for a connection BEFORE reading the clock
		if err != nil {
			if d.retryable(err) {
				continue
			}
			return err
		}
		var rows *sql.Rows
		rows, err = conn.QueryContext(ctx, query, buildArgs()...)
		if err != nil {
			_ = conn.Close()
			if d.retryable(err) {
				continue
			}
			return err
		}
		err = scan(rows)
		_ = rows.Close()
		_ = conn.Close()
		if err == nil || !d.retryable(err) {
			return err
		}
	}
	return err
}

// queryRows is query for a LOCAL store: the rows are consumed inside scan, which is what lets the
// connection be reserved — a cancellable wait, then an uninterruptible statement (see stmtCtx).
// Returning *sql.Rows cannot do that: the rows would outlive the reservation.
func (d *db) queryRows(ctx context.Context, q string, scan func(*sql.Rows) error, args ...any) error {
	if d.remote {
		rows, err := d.query(ctx, q, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		return scan(rows)
	}
	return d.withConn(ctx, func(ec context.Context, c *sql.Conn) error {
		rows, err := c.QueryContext(ec, q, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		return scan(rows)
	})
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
	if !d.remote {
		return d.withConn(ctx, func(ec context.Context, c *sql.Conn) error {
			rows, qerr := c.QueryContext(ec, query, args...)
			if qerr != nil {
				return qerr
			}
			defer rows.Close()
			if !rows.Next() {
				if cerr := rows.Err(); cerr != nil {
					return cerr
				}
				return sql.ErrNoRows
			}
			return rows.Scan(dest...)
		})
	}
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
// fn is handed the context ITS STATEMENTS must use — not the caller's. On a local store that is an
// uninterruptible one (see stmtCtx): a statement interrupted inside a transaction leaks the
// connection and wedges the database. Taking it as a parameter means the closures cannot get this
// wrong by accident — the `ctx` in scope inside fn is always the right one.
func (e *Engine) inTx(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	if !e.db.remote {
		// Local: one attempt, and the transaction is not interruptible once begun — interrupting it
		// leaks the connection and wedges (or erases) the database. The WAIT for the single writer
		// still honours the caller's context, and an already-cancelled caller never begins at all.
		return e.db.withConn(ctx, func(ec context.Context, c *sql.Conn) error {
			tx, berr := c.BeginTx(ec, nil)
			if berr != nil {
				return berr
			}
			if ferr := fn(ec, tx); ferr != nil {
				_ = tx.Rollback()
				return ferr
			}
			// The statements are protected from interruption; the TRANSACTION is not. A callback
			// that spans several statements can be cancelled BETWEEN them — an Engine.Tx callback
			// that pauses, a large multi-message Send mid-loop — and committing then would persist
			// work the caller had already abandoned. The contract is "a statement already executing
			// finishes", not "a transaction already begun commits" (codex). Check the CALLER's
			// context, not the protected one.
			if cerr := ctx.Err(); cerr != nil {
				_ = tx.Rollback()
				return cerr
			}
			return tx.Commit()
		})
	}
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
		err = fn(e.db.stmtCtx(ctx), tx)
		if err != nil {
			_ = tx.Rollback()
			if e.db.retryable(err) {
				continue
			}
			return err
		}
		err = tx.Commit()
		if err != nil {
			if e.db.retryableWrite(err) {
				continue // ErrBadConn / busy: the commit provably didn't land, safe to replay
			}
			if e.db.remote && isConnErr(err) {
				// Outcome-unknown commit transport error: the primary may have durably
				// committed before the ack was lost, so replaying the whole closure would
				// double-apply (e.g. a second insert). Surface it instead of retrying
				// (MQLITE-59). Transparent retry needs durable per-op idempotency — deferred.
				return fmt.Errorf("%w: %s", ErrOutcomeUnknown, err.Error()) // %s: keep raw err out of the errors.Is chain
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
