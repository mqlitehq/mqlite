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
	"sync"
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

	// The close gate serializes every local operation against Close so Close cannot release the
	// single-writer file lock — nor close the pool — while a write is still in flight. Without it,
	// Close returned while a transaction was still committing and dropped the advisory lock, letting
	// a SECOND process open the same file, run crash recovery, and re-deliver still-held locked rows
	// (round-8 P1).
	//
	// It is a fail-fast admission gate, NOT a lock held for the operation's duration: a new op checks
	// the flag under `closeMu` and, if closing, returns ErrClosed IMMEDIATELY — it never blocks. That
	// keeps a deadline-bound caller from hanging (an RWMutex would give a pending Close priority and
	// make new RLocks wait, uncancellably, behind arbitrary in-flight user SQL — round-8 P2). Close
	// flips the flag, then waits on the WaitGroup for the ops admitted before it to finish. The flag
	// check and the WaitGroup Add happen under the same mutex, so no op can be admitted after Close
	// has begun waiting.
	closeMu  sync.Mutex
	closing  bool // admission gate: enter() refuses new ops once set
	torn     bool // teardown-once guard for close()
	inFlight sync.WaitGroup
}

// enter admits a local operation, or reports that the store is closing so the caller fails fast.
// A successful enter must be paired with exactly one inFlight.Done() (see withConn).
func (d *db) enter() bool {
	d.closeMu.Lock()
	defer d.closeMu.Unlock()
	if d.closing {
		return false
	}
	d.inFlight.Add(1) // under closeMu, so it can never race Close's Wait
	return true
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

// beginClosing shuts the admission gate: from here on enter() refuses new operations, so they fail
// fast with ErrClosed. Engine.Close calls this BEFORE it drains the background workers, so an
// external Send/Tx arriving during that drain (which can be slow — a maintenance pass may be inside
// an uninterruptible vacuum or a large retention delete) is refused rather than admitted (round-8).
// Idempotent; the pool/lock teardown happens later, in close().
func (d *db) beginClosing() {
	d.closeMu.Lock()
	d.closing = true
	d.closeMu.Unlock()
}

func (d *db) close() error {
	// Shut the gate (idempotent) so nothing new is admitted, then wait for the ops already in flight
	// to finish — a statement still executing, a transaction still committing — before closing the
	// pool and releasing the file lock, so it is never dropped out from under a live write. The Add
	// in enter happens under the same mutex as the flag, so every in-flight op is counted before this
	// Wait observes the total.
	d.closeMu.Lock()
	if d.torn {
		d.closeMu.Unlock()
		return nil
	}
	d.torn = true
	d.closing = true
	d.closeMu.Unlock()
	d.inFlight.Wait()

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
//   - only a statement that is already EXECUTING runs to completion. There is NO upper bound on how
//     long that takes: EngineTx.SQL() runs arbitrary user SQL, a full VACUUM rewrites the file, and a
//     large Purge or Redrive is a big DELETE. "Microseconds" described the common case and was quietly
//     read as a guarantee (round-6 §2.3) — it is not one. Cancellation is not a latency knob here,
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

// afterBeginTx runs between BeginTx and the callback — the one window a test cannot reach through
// any public seam. nil in production.
var afterBeginTx func()

// afterConnAcquired runs between acquiring the connection and starting the statement. It exists so
// a test can cancel a caller precisely in that window — the window the recheck below defends, and
// one that is otherwise almost impossible to hit on purpose. nil in production.
var afterConnAcquired func()

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
	// Admit this operation, or fail fast if the store is closing. enter never blocks, so a
	// deadline-bound caller is never stuck behind a pending Close. The op stays "in flight" for its
	// whole duration — the wait for the connection, the statement, and (for inTx, whose entire
	// transaction runs inside this closure) the commit — so Close waits for all of it before
	// releasing the lock. All local access funnels through withConn, and withConn never nests, so
	// this one gate covers every local operation.
	if !d.enter() {
		return ErrClosed
	}
	defer d.inFlight.Done()
	conn, err := d.sql.Conn(ctx) // cancellable WAIT
	if err != nil {
		return err
	}
	defer conn.Close()
	if afterConnAcquired != nil { // test seam: lets a test cancel EXACTLY in the handoff window
		afterConnAcquired()
	}
	// And AGAIN, now that we hold the connection. The caller can give up in the window between the
	// wait returning and the statement starting — a handoff that races the deadline, a descheduled
	// goroutine — and past this point cancellation is deliberately gone. Without this recheck a
	// destructive operation (Purge, Cancel, ReceiveDeferred) could mutate the database on behalf of
	// a caller who had already timed out and never started it (codex). The rule is: nothing BEGINS
	// for a caller who has given up.
	if err := checkCtx(ctx); err != nil {
		return err
	}
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
	// remote path: gate for the whole attempt so Close waits for it and post-Close work is refused.
	if !d.enter() {
		return nil, ErrClosed
	}
	defer d.inFlight.Done()
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
			// buildArgs reads the clock and can be preempted, so the caller may give up WHILE it
			// runs — after withConn's check and before the SQL exists. Materialize the arguments,
			// then look again: a stopped renewer must not extend a lease (codex).
			args := buildArgs()
			if err := checkCtx(ctx); err != nil {
				return err
			}
			var e error
			res, e = c.ExecContext(ec, query, args...)
			return e
		})
		return res, err
	}
	var res sql.Result
	var err error
	// remote path: gate for the whole attempt so Close waits for it and post-Close work is refused.
	if !d.enter() {
		return nil, ErrClosed
	}
	defer d.inFlight.Done()
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
			args := buildArgs() // then look again — see execFresh
			if err := checkCtx(ctx); err != nil {
				return err
			}
			rows, qerr := c.QueryContext(ec, query, args...)
			if qerr != nil {
				return qerr
			}
			serr := scan(rows)
			_ = rows.Close()
			return serr
		})
	}
	var err error
	// remote path: gate for the whole attempt so Close waits for it and post-Close work is refused.
	if !d.enter() {
		return ErrClosed
	}
	defer d.inFlight.Done()
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
	// remote path: gate for the whole attempt so Close waits for it and post-Close work is refused.
	if !d.enter() {
		return nil, ErrClosed
	}
	defer d.inFlight.Done()
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
	// remote path: gate for the whole attempt so Close waits for it and post-Close work is refused.
	if !d.enter() {
		return ErrClosed
	}
	defer d.inFlight.Done()
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

// txn is the ONLY way engine code issues a statement inside a transaction, and it exists because
// three review rounds running found a fresh path that had quietly skipped the cancellation rules.
// A guard you have to remember to apply is a guard that gets forgotten, so this one is structural:
// the closures are handed a *txn instead of a *sql.Tx, and every statement they can issue goes
// through it.
//
// It enforces both halves of the contract at once, per statement:
//
//   - the caller is still waiting → run on the PROTECTED context. A statement already executing
//     is never interrupted: on a local store that leaks the driver connection and wedges the file
//     (SQLITE_BUSY forever) or erases a :memory: database outright.
//   - the caller has given up → hand database/sql the caller's already-cancelled context. It fails
//     the statement before it reaches the driver, so nothing begins and nothing is interrupted —
//     and fn unwinds into the rollback below instead of grinding through the rest of a transaction
//     nobody is waiting for (codex, round-6).
//
// The methods mirror *sql.Tx's signatures — including the ctx parameter they deliberately ignore —
// so the 28 statement sites read exactly as they did, and so a future one cannot pick the wrong
// context even by trying.
type txn struct {
	tx     *sql.Tx
	caller context.Context // may be cancelled
	exec   context.Context // protected on a local store; the caller's on a remote one
}

func (t *txn) ctx() context.Context {
	if t.caller.Err() != nil {
		return t.caller // cancelled: database/sql refuses it before the driver ever sees it
	}
	return t.exec
}

func (t *txn) ExecContext(_ context.Context, q string, args ...any) (sql.Result, error) {
	return t.tx.ExecContext(t.ctx(), q, args...)
}
func (t *txn) QueryContext(_ context.Context, q string, args ...any) (*sql.Rows, error) {
	return t.tx.QueryContext(t.ctx(), q, args...)
}
func (t *txn) QueryRowContext(_ context.Context, q string, args ...any) *sql.Row {
	return t.tx.QueryRowContext(t.ctx(), q, args...)
}

// SQL exposes the raw transaction for the one caller that needs it: Embedded.Tx, whose user
// callback runs its own business writes. Statements issued through it are the USER's, and mqlite
// cannot see their boundaries — so they are not guarded. EngineTx documents this.
func (t *txn) SQL() *sql.Tx { return t.tx }

// inTx runs fn inside a transaction, retrying the whole transaction on a
// connection error (the aborted tx leaves nothing committed, so retry is safe).
func (e *Engine) inTx(ctx context.Context, fn func(context.Context, *txn) error) error {
	if !e.db.remote {
		// Local: one attempt, and the transaction is not interruptible once begun — interrupting it
		// leaks the connection and wedges (or erases) the database. The WAIT for the single writer
		// still honours the caller's context, and an already-cancelled caller never begins at all.
		return e.db.withConn(ctx, func(ec context.Context, c *sql.Conn) error {
			tx, berr := c.BeginTx(ec, nil)
			if berr != nil {
				return berr
			}
			if afterBeginTx != nil { // test seam: cancel with the transaction open, before fn
				afterBeginTx()
			}
			// BeginTx itself runs on the protected context, so the caller can give up WHILE it
			// runs. fn is public code — an Engine.Tx callback — and once it starts it holds the
			// single writer until it returns. Nothing BEGINS for a caller who has given up, and
			// that includes the callback (codex).
			if cerr := ctx.Err(); cerr != nil {
				_ = tx.Rollback()
				return cerr
			}
			// Deferred, not just on the error path: an Engine.Tx callback is USER code and it can
			// PANIC. Unwinding would otherwise reach withConn's `defer conn.Close()` with the
			// transaction still open — and Close blocks on it forever, so the panic never reaches
			// the caller and the process's one writer is wedged for good (codex). Rollback after a
			// successful Commit is a no-op (ErrTxDone).
			defer func() { _ = tx.Rollback() }()
			if ferr := fn(ec, &txn{tx: tx, caller: ctx, exec: ec}); ferr != nil {
				return ferr
			}
			// The statements are protected from interruption; the TRANSACTION is not. A callback
			// that spans several statements can be cancelled BETWEEN them — an Engine.Tx callback
			// that pauses, a large multi-message Send mid-loop — and committing then would persist
			// work the caller had already abandoned. The contract is "a statement already executing
			// finishes", not "a transaction already begun commits" (codex). Check the CALLER's
			// context, not the protected one.
			if cerr := ctx.Err(); cerr != nil {
				return cerr // the deferred rollback undoes it
			}
			return tx.Commit()
		})
	}
	var err error
	// remote path: gate for the whole attempt so Close waits for it and post-Close work is refused.
	if !e.db.enter() {
		return ErrClosed
	}
	defer e.db.inFlight.Done()
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
		// The rollback is deferred across BOTH the callback and the commit — scoped to the
		// callback alone it would fire on the success path too, BEFORE Commit, silently rolling
		// back every remote transaction (TestRemoteTxClosureCanBeReplayed caught exactly that).
		// After a successful Commit it is a no-op (ErrTxDone); after a PANIC in fn it is what
		// stops the transaction from being abandoned open.
		//
		// The two errors stay separate because they are not retryable on the same terms: a failed
		// statement retries on any connection error (retryable), a failed COMMIT only when the
		// error proves it never landed (retryableWrite). Collapsing them would quietly downgrade
		// replayable statement failures into ErrOutcomeUnknown.
		var ferr, cerr error
		func() {
			defer func() { _ = tx.Rollback() }()
			if ferr = fn(e.db.stmtCtx(ctx), &txn{tx: tx, caller: ctx, exec: e.db.stmtCtx(ctx)}); ferr != nil {
				return
			}
			cerr = tx.Commit()
		}()
		if ferr != nil {
			err = ferr
			if e.db.retryable(err) {
				continue
			}
			return err
		}
		if err = cerr; err != nil {
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
