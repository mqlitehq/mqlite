package engine

// Storage-layer tests — how the engine opens, guards, and persists its SQLite/
// libSQL store, independent of queue semantics. Sections:
//
//   - Durability pragma   MQLITE_SYNC → PRAGMA synchronous
//   - Schema version       refuse a DB stamped with an incompatible version
//   - Single-writer lock   local file = one writer (ErrDBLocked); :memory: is private
//   - Remote retry         which errors the Turso/libSQL path retries (and which it must not)
//
// The live remote round-trip lives in turso_test.go (creds-gated).

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// netTimeout is a net.Error whose message contains none of isConnErr's substrings,
// so it exercises the errors.As(net.Error).Timeout() branch specifically (MQLITE-59).
type netTimeout struct{}

func (netTimeout) Error() string   { return "deadline exceeded" }
func (netTimeout) Timeout() bool   { return true }
func (netTimeout) Temporary() bool { return true }

var _ net.Error = netTimeout{}

// ─── Durability pragma ──────────────────────────────────────────────────────

// MQLITE-7: the Synchronous option sets SQLite's PRAGMA synchronous on the local
// connection (0=OFF, 1=NORMAL, 2=FULL). This is the durability vs throughput knob.
func TestSynchronousPragma(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		mode string
		want int64
	}{
		{"", 1}, // default -> NORMAL
		{"NORMAL", 1},
		{"FULL", 2},
		{"OFF", 0},
	} {
		e, err := Open(ctx, Options{
			DB:                "file:" + filepath.Join(t.TempDir(), "mq.db"),
			Synchronous:       tc.mode,
			DisableBackground: true,
		})
		if err != nil {
			t.Fatalf("open synchronous=%q: %v", tc.mode, err)
		}
		var got int64
		if err := e.db.sql.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&got); err != nil {
			t.Fatalf("read pragma (%q): %v", tc.mode, err)
		}
		if got != tc.want {
			t.Errorf("synchronous=%q -> PRAGMA synchronous=%d, want %d", tc.mode, got, tc.want)
		}
		_ = e.Close()
	}
}

// ─── Schema version guard ───────────────────────────────────────────────────

// A fresh DB records the current schema version, and reopening a DB stamped with a
// different version is refused rather than silently running today's DDL against an
// incompatible layout. (Value-agnostic: it asserts the round-trip + the refusal, not
// any particular version string.)
func TestSchemaVersionGuard(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + filepath.Join(t.TempDir(), "mq.db")

	e, err := Open(ctx, Options{DB: dsn, DisableBackground: true})
	if err != nil {
		t.Fatalf("fresh open: %v", err)
	}
	var v string
	if err := e.db.queryRowScan(ctx, []any{&v}, `SELECT value FROM meta WHERE key='schema_version'`); err != nil {
		t.Fatalf("read recorded version: %v", err)
	}
	if v != schemaVersion {
		t.Fatalf("fresh DB recorded version %q, want %q", v, schemaVersion)
	}

	// Stamp an incompatible version, as if the DB were created by another build.
	if _, err := e.db.exec(ctx, `UPDATE meta SET value='legacy' WHERE key='schema_version'`); err != nil {
		t.Fatalf("stamp legacy version: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopening must refuse rather than run new DDL against the old layout (MQLITE-24).
	e2, err := Open(ctx, Options{DB: dsn, DisableBackground: true})
	if e2 != nil {
		_ = e2.Close()
	}
	if !errors.Is(err, ErrSchemaVersionMismatch) {
		t.Fatalf("reopen with mismatched version: got %v, want ErrSchemaVersionMismatch", err)
	}
}

// The schema-content golden guard (review F13): schemaVersion is an opaque token
// that MUST change whenever the DDL changes incompatibly — but nothing tied the
// token to the actual statements, so a schema edit with a forgotten bump would
// let an old DB pass the version guard and run against a layout it doesn't
// match. Pin both: any DDL drift fails here and forces a DELIBERATE update of
// the hash AND a decision about the token.
//
// If this test fails you changed schemaStmts. Update wantHash to the printed
// value, and bump schemaVersion in schema.go unless the change is provably
// compatible with existing DBs (a comment-only edit inside a statement is not —
// the statements are the contract).
func TestSchemaContentPinnedToVersionToken(t *testing.T) {
	const (
		wantVersion = "5"
		wantHash    = "9dcb13ddf1fbf11179a690e70d357dcfa1b87fa6f135a85db02164185d5854e6"
	)
	sum := sha256.Sum256([]byte(strings.Join(schemaStmts, "\n")))
	if got := hex.EncodeToString(sum[:]); got != wantHash {
		t.Fatalf("schemaStmts changed (hash %s, pinned %s):\nbump schemaVersion (schema.go) if incompatible, then update wantHash here", got, wantHash)
	}
	if schemaVersion != wantVersion {
		t.Fatalf("schemaVersion changed to %q without updating this pin (wantVersion %q): update both together", schemaVersion, wantVersion)
	}
}

// ─── Free-page reclamation ──────────────────────────────────────────────────

// Compact(full=false) must actually return freed pages to the OS — a single
// incremental_vacuum step with no checkpoint reclaimed almost nothing and never shrank the
// file (MQLITE-78). After draining a queue, Compact empties the freelist and shrinks the file.
func TestCompactReclaimsFreePages(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "mq.db")
	e, _ := testEngineAt(t, path)
	mustQueue(t, e, "q", QueueConfig{})

	body := make([]byte, 4096)
	for i := 0; i < 500; i++ {
		if _, err := e.SendOne(ctx, "q", OutMessage{Body: body}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 500; i++ {
		m := recvOne(t, e, "q")
		if err := e.Complete(ctx, "q", m.SeqNumber, m.LockToken); err != nil {
			t.Fatal(err)
		}
	}
	// Materialize the deletes onto the main-DB freelist so the "before" state is deterministic.
	if _, err := e.db.exec(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	fi1, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	var before int
	if err := e.db.queryRowScan(ctx, []any{&before}, `PRAGMA freelist_count`); err != nil {
		t.Fatal(err)
	}
	if before < 100 {
		t.Fatalf("expected a sizable freelist to reclaim; got %d free pages", before)
	}

	if err := e.Compact(ctx, false); err != nil {
		t.Fatal(err)
	}

	var after int
	if err := e.db.queryRowScan(ctx, []any{&after}, `PRAGMA freelist_count`); err != nil {
		t.Fatal(err)
	}
	if after > 16 {
		t.Fatalf("Compact left %d free pages (was %d); incremental reclaim should empty the freelist", after, before)
	}
	fi2, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi2.Size() >= fi1.Size() {
		t.Fatalf("Compact did not shrink the file: %d -> %d bytes", fi1.Size(), fi2.Size())
	}
}

// ─── Single-writer lock ─────────────────────────────────────────────────────

// MQLITE-6: a local file DB is single-writer — a second opener is rejected with
// ErrDBLocked rather than racing the first on crash recovery / claims.
func TestFileDBSingleWriter(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + filepath.Join(t.TempDir(), "mq.db")

	e1, err := Open(ctx, Options{DB: dsn, DisableBackground: true})
	if err != nil {
		t.Fatalf("first open: %v", err)
	}

	e2, err := Open(ctx, Options{DB: dsn, DisableBackground: true})
	if !errors.Is(err, ErrDBLocked) {
		if e2 != nil {
			_ = e2.Close()
		}
		t.Fatalf("second open of the same file: got err=%v, want ErrDBLocked", err)
	}

	// Releasing the first frees the lock so the file can be reopened.
	if err := e1.Close(); err != nil {
		t.Fatalf("close first: %v", err)
	}
	e3, err := Open(ctx, Options{DB: dsn, DisableBackground: true})
	if err != nil {
		t.Fatalf("reopen after close: %v", err)
	}
	_ = e3.Close()
}

// MQLITE-60: the lock sidecar is keyed on the CANONICAL path, so every spelling
// of the same physical file must collide on one .lock — dot segments, DSN query
// options, and (where the file exists) symlinked directories included. Before
// the fix each spelling derived its own sidecar and two writers could open one
// file.
func TestFileDBSingleWriterPathSpellings(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	clean := filepath.Join(dir, "mq.db")

	e1, err := Open(ctx, Options{DB: "file:" + clean, DisableBackground: true})
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	defer e1.Close()

	sep := string(os.PathSeparator)
	dotted := dir + sep + "." + sep + "mq.db"                                      // dir/./mq.db
	parent := dir + sep + ".." + sep + filepath.Base(dir) + sep + "mq.db"          // dir/../dir/mq.db
	for _, spelling := range []string{dotted, parent, clean + "?_pragma=foo(1)"} { // + DSN options
		e2, err := Open(ctx, Options{DB: "file:" + spelling, DisableBackground: true})
		if !errors.Is(err, ErrDBLocked) {
			if e2 != nil {
				_ = e2.Close()
			}
			t.Fatalf("spelling %q must collide on the same lock: got err=%v, want ErrDBLocked", spelling, err)
		}
	}

	if runtime.GOOS != "windows" { // symlinks need privileges on Windows runners
		link := filepath.Join(t.TempDir(), "dir-link")
		if err := os.Symlink(dir, link); err != nil {
			t.Skipf("symlink: %v", err)
		}
		e2, err := Open(ctx, Options{DB: "file:" + filepath.Join(link, "mq.db"), DisableBackground: true})
		if !errors.Is(err, ErrDBLocked) {
			if e2 != nil {
				_ = e2.Close()
			}
			t.Fatalf("symlinked spelling must collide on the same lock: got err=%v, want ErrDBLocked", err)
		}
	}
}

// :memory: DBs are private per handle, so concurrent opens must NOT be locked.
func TestMemoryDBNotLocked(t *testing.T) {
	ctx := context.Background()
	e1, err := Open(ctx, Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("first :memory: open: %v", err)
	}
	defer e1.Close()
	e2, err := Open(ctx, Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("second :memory: open must not be locked: %v", err)
	}
	defer e2.Close()
}

// ─── Remote retry classification ────────────────────────────────────────────

// The remote (Turso/libSQL) retry path turns on exactly when isConnErr says an
// error is a dropped Hrana stream — never on a logical error, where a blind retry
// could double-execute. This hermetic test pins that classification contract so a
// later refactor can't silently widen or narrow it (MQLITE-4); the live round-trip
// lives in the creds-gated TestTursoIntegration / TestTursoExtended.
func TestIsConnErr(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"wrapped ErrBadConn", fmt.Errorf("exec: %w", driver.ErrBadConn), true},
		{"stream is closed", errors.New("driver: bad connection: stream is closed"), true},
		{"stream closed", errors.New("Hrana: stream closed"), true},
		{"connection reset", errors.New("read tcp: connection reset by peer"), true},
		{"plain bad connection", errors.New("write: bad connection"), true},
		// Transport siblings of "connection reset": how a lost commit ack actually
		// surfaces from the libSQL POST behind a proxy (Fly upstream drop → EOF, not
		// RST). These MUST classify as conn errors so a write wraps ErrOutcomeUnknown
		// instead of leaking a raw "EOF" that reads as safe-to-retry (MQLITE-59).
		{"wrapped io.EOF", fmt.Errorf("read response: %w", io.EOF), true},
		{"bare EOF string", errors.New("EOF"), true},
		{"unexpected EOF", errors.New("unexpected EOF"), true},
		{"wrapped ErrUnexpectedEOF", fmt.Errorf("body: %w", io.ErrUnexpectedEOF), true},
		{"broken pipe", errors.New("write tcp 10.0.0.1->10.0.0.2: broken pipe"), true},
		{"i/o timeout string", errors.New("read tcp: i/o timeout"), true},
		{"net.Error timeout (no substring)", netTimeout{}, true},
		// A structured server *response* is a definite non-commit — must NOT be
		// treated as outcome-unknown (else a rejected write reads as "maybe applied").
		{"server 500 response", errors.New("error code 500: SQLITE_ERROR"), false},
		{"no such table", errors.New("no such table: messages"), false},
		{"unique constraint", errors.New("UNIQUE constraint failed: dedup.message_id"), false},
		{"context canceled", errors.New("context canceled"), false},
	}
	for _, c := range cases {
		if got := isConnErr(c.err); got != c.want {
			t.Errorf("isConnErr(%s) = %v, want %v", c.name, got, c.want)
		}
	}
}

// retryable gates retries on remote-ness AND a connection error; attempts() caps
// the loop. Local stores must never retry (a single conn IS the single writer, and
// a logical failure there is final).
func TestRetryableAndAttempts(t *testing.T) {
	remote := &db{remote: true}
	local := &db{remote: false}

	connErr := errors.New("stream is closed")
	busyErr := errors.New("SQLite error: database is locked")
	logicalErr := errors.New("UNIQUE constraint failed")

	if !remote.retryable(connErr) {
		t.Error("remote must retry a dropped-stream connection error")
	}
	if !remote.retryable(busyErr) {
		t.Error("remote must retry a contended write (database is locked) — MQLITE-4")
	}
	if remote.retryable(logicalErr) {
		t.Error("remote must NOT retry a logical error (double-execution risk)")
	}
	if local.retryable(connErr) || local.retryable(busyErr) {
		t.Error("local must never retry, even on a connection or busy error")
	}

	if got := remote.attempts(); got != maxConnAttempts {
		t.Errorf("remote attempts = %d, want %d", got, maxConnAttempts)
	}
	if got := local.attempts(); got != 1 {
		t.Errorf("local attempts = %d, want 1", got)
	}
}

// A WRITE/COMMIT may only be replayed when the error guarantees it never applied
// (driver.ErrBadConn or busy). A broad transport error like "connection reset" is
// outcome-unknown — replaying it could double-apply, so it is NOT retryable for a write
// (the caller gets ErrOutcomeUnknown). MQLITE-59.
func TestRetryableWrite(t *testing.T) {
	remote := &db{remote: true}
	if !remote.retryableWrite(fmt.Errorf("exec: %w", driver.ErrBadConn)) {
		t.Error("write on ErrBadConn must be retryable (the statement never ran)")
	}
	if !remote.retryableWrite(errors.New("database is locked")) {
		t.Error("write on a busy error must be retryable (lock never acquired)")
	}
	// Every broad transport error — including the EOF/timeout/broken-pipe siblings
	// isConnErr now spans — is outcome-unknown for a write: it may have applied before
	// the ack was lost, so it must NOT be replayed (MQLITE-59). retryableWrite stays
	// narrow (ErrBadConn/busy only) even though isConnErr(these) is true.
	for _, broad := range []string{
		"stream is closed", "read tcp: connection reset by peer", "bad connection",
		"EOF", "unexpected EOF", "write tcp: broken pipe", "read tcp: i/o timeout",
	} {
		e := errors.New(broad)
		if remote.retryableWrite(e) {
			t.Errorf("write on %q must NOT be retryable — outcome unknown, don't double-apply", broad)
		}
		if !isConnErr(e) {
			t.Errorf("isConnErr(%q) = false; a write on it must still wrap ErrOutcomeUnknown", broad)
		}
	}
	if (&db{}).retryableWrite(fmt.Errorf("x: %w", driver.ErrBadConn)) {
		t.Error("local writes must never retry")
	}
}

// An unrecognized MQLITE_SYNC / Options.Synchronous must fail Open loudly, not silently
// downgrade to NORMAL — a durability typo can't quietly weaken the guarantee (MQLITE-88).
func TestValidateSync(t *testing.T) {
	for _, ok := range []string{"", "NORMAL", "normal", " full ", "OFF", "EXTRA"} {
		if err := validateSync(ok); err != nil {
			t.Errorf("validateSync(%q) = %v, want nil", ok, err)
		}
	}
	for _, bad := range []string{"FULLL", "fastest", "1", "yes"} {
		if err := validateSync(bad); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("validateSync(%q) = %v, want ErrInvalidArgument", bad, err)
		}
	}
	// End to end: a bad value fails Open rather than opening on the NORMAL default.
	if _, err := Open(context.Background(), Options{DB: ":memory:", Synchronous: "FULLL", DisableBackground: true}); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("Open with bad Synchronous = %v, want ErrInvalidArgument", err)
	}
}

// isBusyErr classifies a contended-write failure from the remote primary, which —
// unlike a logical error — is safe to retry because the lock was never acquired
// (MQLITE-4 concurrency hardening).
func TestIsBusyErr(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"database is locked", errors.New("SQLite error: database is locked"), true},
		{"database table is locked", errors.New("database table is locked"), true},
		{"SQLITE_BUSY", errors.New("SQLITE_BUSY: ..."), true},
		{"unique constraint", errors.New("UNIQUE constraint failed: dedup.message_id"), false},
		{"no such table", errors.New("no such table: messages"), false},
	}
	for _, c := range cases {
		if got := isBusyErr(c.err); got != c.want {
			t.Errorf("isBusyErr(%s) = %v, want %v", c.name, got, c.want)
		}
	}
}

// ─── Compaction ─────────────────────────────────────────────────────────────

// New local DBs are created with auto_vacuum=INCREMENTAL, and Compact reclaims free
// pages with either incremental_vacuum (default) or a full VACUUM — neither errors.
func TestCompact(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t) // local file DB

	var av int64
	if err := e.db.queryRowScan(ctx, []any{&av}, "PRAGMA auto_vacuum"); err != nil {
		t.Fatalf("read auto_vacuum: %v", err)
	}
	if av != 2 { // 0=NONE, 1=FULL, 2=INCREMENTAL
		t.Fatalf("fresh local DB auto_vacuum=%d, want 2 (INCREMENTAL)", av)
	}

	// Churn so there are free pages to reclaim, then compact both ways.
	mustQueue(t, e, "q", QueueConfig{})
	for i := 0; i < 300; i++ {
		if _, err := e.SendOne(ctx, "q", OutMessage{Body: make([]byte, 256)}); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	for {
		ms, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 64})
		if err != nil {
			t.Fatalf("receive: %v", err)
		}
		if len(ms) == 0 {
			break
		}
		for _, m := range ms {
			if err := e.Complete(ctx, "q", m.SeqNumber, m.LockToken); err != nil {
				t.Fatalf("complete: %v", err)
			}
		}
	}
	if err := e.Compact(ctx, false); err != nil {
		t.Fatalf("incremental compact: %v", err)
	}
	if err := e.Compact(ctx, true); err != nil {
		t.Fatalf("full compact: %v", err)
	}
}

// reclaimFreePages (the background MQLITE-53 pass) returns freed pages to the OS on a
// local file DB once the freelist has grown past freePageReclaimMin, and is a safe no-op
// for :memory: (no OS pages to return).
func TestBackgroundReclaimFreePages(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t) // local file DB, auto_vacuum=INCREMENTAL
	mustQueue(t, e, "q", QueueConfig{})

	// Churn enough rows to free well over freePageReclaimMin pages.
	body := make([]byte, 1024)
	for i := 0; i < 2000; i++ {
		if _, err := e.SendOne(ctx, "q", OutMessage{Body: body}); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	for {
		ms, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 200})
		if err != nil {
			t.Fatalf("receive: %v", err)
		}
		if len(ms) == 0 {
			break
		}
		items := make([]SettleItem, len(ms))
		for i, m := range ms {
			items[i] = SettleItem{SeqNumber: m.SeqNumber, LockToken: m.LockToken}
		}
		if _, err := e.CompleteBatch(ctx, "q", items); err != nil {
			t.Fatalf("complete batch: %v", err)
		}
	}
	// Flush the WAL so the freed pages land on the main-DB freelist.
	if rows, err := e.db.query(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err == nil {
		rows.Close()
	}

	freelist := func() int {
		var n int
		if err := e.db.queryRowScan(ctx, []any{&n}, "PRAGMA freelist_count"); err != nil {
			t.Fatalf("freelist_count: %v", err)
		}
		return n
	}
	before := freelist()
	if before < freePageReclaimMin {
		t.Fatalf("churn freed only %d pages, need >= %d to exercise reclaim", before, freePageReclaimMin)
	}
	path, _ := localFilePath(e.db.dsn)
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat db: %v", err)
	}
	sizeBefore := fi.Size()

	e.reclaimFreePages(ctx) // checkpoint + incremental_vacuum + checkpoint → file shrinks

	afterFree := freelist()
	fi2, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat db after: %v", err)
	}
	t.Logf("reclaim: freelist %d -> %d pages, file %d -> %d bytes (%.0f%% returned to OS)",
		before, afterFree, sizeBefore, fi2.Size(), 100*float64(sizeBefore-fi2.Size())/float64(sizeBefore))
	// A working reclaim returns essentially the WHOLE freelist to the OS, not a few
	// pages. incremental_vacuum needs the freed pages on the main-DB freelist AND a
	// checkpoint to truncate the file — a weak "shrank at all" check would pass even on
	// a near-total leak, which is exactly what an empty-queue-but-10MB-file looks like.
	if afterFree >= freePageReclaimMin {
		t.Fatalf("reclaim left %d free pages on the freelist (was %d) — not returned to the OS", afterFree, before)
	}
	if fi2.Size() > sizeBefore/2 {
		t.Fatalf("reclaim returned only %d of %d bytes — expected most of the freelist back", sizeBefore-fi2.Size(), sizeBefore)
	}

	// :memory: has no OS pages to return — reclaim must be a harmless no-op.
	mem, err := Open(ctx, Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("open memory: %v", err)
	}
	defer mem.Close()
	mem.reclaimFreePages(ctx)
}

// ─── remote retry: time-sensitive statement arguments (MQLITE-97) ──────────────
//
// execFresh/queryFresh exist because a remote retry BACKS OFF: arguments computed once, before
// the loop, are stale by the time a later attempt lands. For a lock deadline that is not
// cosmetic — the write can commit a lease that has ALREADY EXPIRED, while still reporting
// success, and the reaper reclaims the message immediately.
//
// A fake driver is the only way to see this: retries fire only on a remote store, and a real
// Turso round trip cannot be made to fail on demand. It returns a busy error for the first N
// calls — database/sql passes that straight through (unlike driver.ErrBadConn, which it retries
// on its own), so OUR loop is the one doing the retrying.

// ─── a driver that is busy for the first n calls ───────────────────────────────

type busyConn struct{ st *busyState }

type busyState struct {
	failsLeft int
	args      [][]driver.NamedValue // the args of every attempt, in order
}

func (c *busyConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("prepare unused") }
func (c *busyConn) Close() error                        { return nil }

// Begin always succeeds; the COMMIT is what fails while failsLeft > 0. That is the interesting
// fault: it lands AFTER the closure has run, so replaying the transaction replays the closure —
// which is the whole contract being pinned. A failure at Begin would prove nothing.
func (c *busyConn) Begin() (driver.Tx, error) { return &busyTx{st: c.st}, nil }

type busyTx struct{ st *busyState }

func (t *busyTx) Commit() error {
	if t.st.failsLeft > 0 {
		t.st.failsLeft--
		return errors.New("database is locked") // busy: the commit provably did not land
	}
	return nil
}
func (t *busyTx) Rollback() error { return nil }

func (c *busyConn) record(args []driver.NamedValue) bool {
	c.st.args = append(c.st.args, args)
	if c.st.failsLeft > 0 {
		c.st.failsLeft--
		return false
	}
	return true
}

func (c *busyConn) QueryContext(_ context.Context, _ string, args []driver.NamedValue) (driver.Rows, error) {
	if !c.record(args) {
		return nil, errors.New("database is locked") // isBusyErr => retryable
	}
	return emptyRows{}, nil
}

func (c *busyConn) ExecContext(_ context.Context, _ string, args []driver.NamedValue) (driver.Result, error) {
	if !c.record(args) {
		return nil, errors.New("database is locked")
	}
	return driver.RowsAffected(1), nil
}

type emptyRows struct{}

func (emptyRows) Columns() []string         { return []string{"id"} }
func (emptyRows) Close() error              { return nil }
func (emptyRows) Next([]driver.Value) error { return io.EOF }

type busyConnector struct{ st *busyState }

func (c busyConnector) Connect(context.Context) (driver.Conn, error) { return &busyConn{st: c.st}, nil }
func (c busyConnector) Driver() driver.Driver                        { return nil }

// remoteDBFailingFirst builds a *db that looks remote (so the retry loop is armed) and whose
// first n statements come back busy.
func remoteDBFailingFirst(t *testing.T, n int) (*db, *busyState) {
	t.Helper()
	st := &busyState{failsLeft: n}
	sdb := sql.OpenDB(busyConnector{st: st})
	t.Cleanup(func() { _ = sdb.Close() })
	return &db{sql: sdb, remote: true}, st
}

// ─── the contract ──────────────────────────────────────────────────────────────

// queryFresh/execFresh must rebuild their arguments for EVERY attempt. If the args are built
// once, the retry replays a stale deadline — and after a backoff that deadline can be in the
// past, so the row commits an already-expired lease while reporting success.
func TestFreshHelpersRebuildArgsPerAttempt(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		run  func(d *db, build func() []any) error
	}{
		{"queryFresh", func(d *db, build func() []any) error {
			return d.queryFresh(ctx, "UPDATE t SET x=?", build, func(*sql.Rows) error { return nil })
		}},
		{"execFresh", func(d *db, build func() []any) error {
			_, err := d.execFresh(ctx, "UPDATE t SET x=?", build)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, st := remoteDBFailingFirst(t, 2) // two busy failures, then success

			// A "clock" that ticks on every read: each attempt must see a NEW value.
			tick := int64(0)
			build := func() []any {
				tick++
				return []any{tick}
			}
			if err := tc.run(d, build); err != nil {
				t.Fatalf("after the retries the statement should succeed, got %v", err)
			}
			if len(st.args) != 3 {
				t.Fatalf("attempts = %d, want 3 (two busy + one success)", len(st.args))
			}
			// The third attempt — the one that actually lands — must carry the value built AT
			// THAT ATTEMPT, not the one computed before the loop backed off.
			for i, got := range st.args {
				want := int64(i + 1)
				if len(got) != 1 || got[0].Value != want {
					t.Errorf("attempt %d wrote %v, want %d — the args were not rebuilt for this attempt",
						i+1, got, want)
				}
			}
		})
	}
}

// The plain (non-fresh) helpers keep their existing behavior: args are fixed, and a retry
// replays them verbatim. That is correct for a statement whose arguments are not time-sensitive,
// and it is what makes the distinction between the two worth having.
func TestPlainHelpersReplayTheSameArgs(t *testing.T) {
	d, st := remoteDBFailingFirst(t, 1)
	if _, err := d.exec(context.Background(), "UPDATE t SET x=?", int64(7)); err != nil {
		t.Fatalf("exec: %v", err)
	}
	if len(st.args) != 2 {
		t.Fatalf("attempts = %d, want 2 (one busy + one success)", len(st.args))
	}
	for i, got := range st.args {
		if got[0].Value != int64(7) {
			t.Errorf("attempt %d wrote %v, want the same 7 on every replay", i+1, got)
		}
	}
}

// The arguments must be built only ONCE A CONNECTION IS IN HAND. Building them before the call is
// not enough: DB.Exec/Query can BLOCK waiting for a free connection (and will transparently retry
// a bad connection, reusing the arguments it was handed). A lock deadline computed before that
// wait can therefore be committed already expired — reported as a successful renewal, and
// reclaimed by the reaper at once (codex).
//
// Saturate the pool, and watch: while the helper is stuck waiting for a connection, it must not
// have read the clock yet.
func TestFreshHelpersBuildArgsAfterAcquiringAConn(t *testing.T) {
	d, _ := remoteDBFailingFirst(t, 0)
	d.sql.SetMaxOpenConns(1)

	// Take the only connection and hold it.
	held, err := d.sql.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	var built atomic.Bool
	done := make(chan error, 1)
	go func() {
		done <- d.queryFresh(context.Background(), "UPDATE t SET x=?",
			func() []any { built.Store(true); return []any{int64(1)} },
			func(*sql.Rows) error { return nil })
	}()

	// The helper is now blocked in Conn(). If it had built its arguments up front, the clock would
	// already have been read — and by the time a connection frees up, that deadline is stale.
	time.Sleep(150 * time.Millisecond)
	if built.Load() {
		t.Fatal("the arguments were built while still waiting for a connection — a deadline read there is stale by the time the statement runs")
	}

	_ = held.Close() // release the pool
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("queryFresh: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("queryFresh never completed after the connection was released")
	}
	if !built.Load() {
		t.Error("the arguments were never built")
	}
}

// An in-memory database lives INSIDE its connection: discard the connection and the schema goes
// with it. So the statement helpers must never hand a local connection back to the pool to be
// dropped — a renewal that quietly destroys the database is a spectacular way to fail.
//
// This is not hypothetical: reserving a *sql.Conn per attempt (right for a remote store, where
// retries and backoff live) made a `:memory:` engine intermittently come back with
// "no such table: messages" on CI. Hammer the fresh-argument paths and require the database to
// still be there.
func TestMemoryDBSurvivesRepeatedFreshStatements(t *testing.T) {
	ctx := context.Background()
	e, err := Open(ctx, Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err := e.CreateQueue(ctx, "q", QueueConfig{LockDurationMs: 60_000, MaxDeliveryCount: 10}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte("m")}); err != nil {
		t.Fatal(err)
	}
	msgs, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 1})
	if err != nil || len(msgs) != 1 {
		t.Fatalf("receive: %v n=%d", err, len(msgs))
	}
	item := SettleItem{SeqNumber: msgs[0].SeqNumber, LockToken: msgs[0].LockToken}

	for i := 0; i < 200; i++ {
		if _, err := e.RenewBatch(ctx, "q", []SettleItem{item}); err != nil {
			t.Fatalf("RenewBatch #%d: %v", i, err)
		}
		if err := e.Renew(ctx, "q", item.SeqNumber, item.LockToken); err != nil {
			t.Fatalf("Renew #%d: %v", i, err)
		}
	}
	// The database — and the message — must still be there.
	if m, err := e.Stats(ctx, "q"); err != nil {
		t.Fatalf("stats after 200 renewals: %v (the in-memory database was destroyed)", err)
	} else if m.Locked != 1 {
		t.Errorf("locked=%d after renewals, want 1", m.Locked)
	}
}

// On a remote store, a transaction that fails on a retryable connection/busy error is replayed
// from the start — so the closure handed to inTx (and therefore to the PUBLIC Engine.Tx, which is
// how the transactional outbox is written) MAY RUN MORE THAN ONCE.
//
// The database work of the failed attempt rolled back, so the data stays correct. But anything the
// closure did outside the transaction — an HTTP call, a charge, a counter — happened twice, and no
// rollback undoes it. That is now the documented contract (round-4 §5.2); this pins it, so it
// cannot quietly become untrue in either direction.
func TestRemoteTxClosureCanBeReplayed(t *testing.T) {
	d, _ := remoteDBFailingFirst(t, 2) // two retryable failures, then success
	e := &Engine{db: d}

	runs := 0
	if err := e.inTx(context.Background(), func(ctx context.Context, tx *txn) error {
		runs++
		return nil
	}); err != nil {
		t.Fatalf("inTx: %v", err)
	}
	if runs != 3 {
		t.Fatalf("the closure ran %d time(s); a remote store replays it on a retryable failure (want 3: two busy + one success)", runs)
	}

	// Local stores never retry: exactly once, which is what the embedded outbox relies on.
	local, _ := testEngine(t)
	runs = 0
	if err := local.inTx(context.Background(), func(ctx context.Context, tx *txn) error {
		runs++
		return nil
	}); err != nil {
		t.Fatalf("local inTx: %v", err)
	}
	if runs != 1 {
		t.Errorf("a local transaction ran its closure %d times, want exactly 1 — the outbox depends on it", runs)
	}
}

// TestCloseWaitsForInFlightWritesBeforeReleasingTheLock pins the round-8 P1: Close must not release
// the single-writer file lock — nor let its transaction be abandoned open — while a write is still
// in flight. Before the fix, db.close() called sql.DB.Close() (which does NOT wait for a checked-out
// connection) and then dropped the advisory lock, so a SECOND opener could take the file and run
// crash recovery while the first writer's transaction was still committing.
func TestCloseWaitsForInFlightWritesBeforeReleasingTheLock(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + filepath.Join(t.TempDir(), "mq.db")
	e, err := Open(ctx, Options{DB: dsn, DisableBackground: true})
	if err != nil {
		t.Fatal(err)
	}

	release := make(chan struct{})
	inTx := make(chan struct{})
	txDone := make(chan error, 1)

	// A transaction that has begun and is holding the single writer, parked until we release it.
	go func() {
		txDone <- e.inTx(ctx, func(ec context.Context, tx *txn) error {
			if _, err := tx.ExecContext(ec, `INSERT INTO meta(key,value) VALUES ('k','v')`); err != nil {
				return err
			}
			close(inTx)
			<-release // hold the transaction open
			return nil
		})
	}()

	<-inTx // the transaction is open and holds the writer

	// Close in the background: it MUST block until the transaction finishes.
	closed := make(chan error, 1)
	go func() { closed <- e.Close() }()

	// Deterministically wait until Close has begun (flipped closing, now blocked on the in-flight
	// transaction), then confirm it has NOT returned — no scheduler-timing guess (round-8, codex).
	awaitClosing(t, e)
	select {
	case <-closed:
		t.Fatal("Close returned while a transaction was still open — it released the writer/lock early")
	default:
		// Good: Close has begun but is blocked on the in-flight transaction.
	}

	// While Close is blocked, the file lock is still held, so a second open must be refused.
	if e2, err := Open(ctx, Options{DB: dsn, DisableBackground: true}); !errors.Is(err, ErrDBLocked) {
		if e2 != nil {
			_ = e2.Close()
		}
		t.Fatalf("a second open during an in-flight-then-closing engine must get ErrDBLocked, got %v", err)
	}

	close(release) // let the transaction commit and return

	if err := <-txDone; err != nil {
		t.Fatalf("transaction: %v", err)
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after the transaction finished")
	}

	// Now — and only now — the file can be reopened, and the write is durably there.
	e2, err := Open(ctx, Options{DB: dsn, DisableBackground: true})
	if err != nil {
		t.Fatalf("reopen after a clean close: %v", err)
	}
	defer e2.Close()
	var got string
	if err := e2.db.queryRowScan(ctx, []any{&got}, `SELECT value FROM meta WHERE key='k'`); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if got != "v" {
		t.Fatalf("the committed write is missing after reopen: got %q", got)
	}

	// And once closed, further operations fail fast rather than racing a torn-down store.
	if err := e.Complete(ctx, "q", 1, "tok"); !errors.Is(err, ErrClosed) && !errors.Is(err, ErrLockLost) {
		t.Fatalf("an operation after Close should be refused, got %v", err)
	}
}

// awaitClosing blocks until db.close has flipped the closing flag — a deterministic substitute for
// sleeping, so a slow scheduler cannot make the close-gate tests race (round-8, codex).
func awaitClosing(t *testing.T, e *Engine) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		e.db.closeMu.Lock()
		closing := e.db.closing
		e.db.closeMu.Unlock()
		if closing {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("Close never reached its waiting state (closing flag not set)")
}

// TestOperationsFailFastWhileClosing pins the round-8 P2: a new operation that arrives while Close is
// waiting for an in-flight write must FAIL FAST with ErrClosed, not block. An RWMutex gate would give
// the pending Close writer-priority and make every later operation wait — uncancellably, behind
// arbitrary in-flight user SQL — so a deadline-bound caller could hang and then get the wrong error.
func TestOperationsFailFastWhileClosing(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + filepath.Join(t.TempDir(), "mq.db")
	e, err := Open(ctx, Options{DB: dsn, DisableBackground: true})
	if err != nil {
		t.Fatal(err)
	}
	mustQueue(t, e, "q", QueueConfig{})

	inTx := make(chan struct{})
	release := make(chan struct{})
	txDone := make(chan error, 1)
	go func() {
		txDone <- e.inTx(ctx, func(ec context.Context, tx *txn) error {
			close(inTx)
			<-release // hold the writer (and the in-flight count) open
			return nil
		})
	}()
	<-inTx

	closed := make(chan error, 1)
	go func() { closed <- e.Close() }()
	awaitClosing(t, e) // deterministically wait until Close has flipped the closing flag

	// A new operation must now return promptly with ErrClosed — it must NOT block behind the pending
	// Close (which is itself blocked on the held transaction).
	opDone := make(chan error, 1)
	go func() { _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte("x")}); opDone <- err }()
	select {
	case err := <-opDone:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("an operation arriving while closing must fail fast with ErrClosed, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("an operation blocked behind a pending Close instead of failing fast — the close gate " +
			"is not cancellation-safe")
	}

	close(release)
	if err := <-txDone; err != nil {
		t.Fatalf("tx: %v", err)
	}
	if err := <-closed; err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestCloseGateCoversRemoteAndFailsFastBeforeTeardown pins two round-8 P2 follow-ups:
//   - beginClosing shuts the admission gate BEFORE the pool is torn down, so a new op is refused the
//     moment Close starts (Engine.Close flips it before draining background workers, which can be
//     slow), not only once the pool is closed;
//   - the REMOTE code paths (which bypass withConn) are gated too, so a remote op after close returns
//     the typed ErrClosed and remote work is counted in the in-flight set.
func TestCloseGateCoversRemoteAndFailsFastBeforeTeardown(t *testing.T) {
	// A "remote" db over the fake driver — its exec/query/inTx take the remote branch.
	d, _ := remoteDBFailingFirst(t, 0)

	// Before closing, the remote path works (admitted).
	if _, err := d.exec(context.Background(), "SELECT 1"); err != nil {
		t.Fatalf("remote exec before close: %v", err)
	}

	// beginClosing alone — no pool teardown yet — must already refuse new work.
	d.beginClosing()
	if d.enter() {
		t.Fatal("enter admitted an operation after beginClosing, before teardown")
	}
	if _, err := d.exec(context.Background(), "SELECT 1"); !errors.Is(err, ErrClosed) {
		t.Fatalf("a remote exec after beginClosing must be ErrClosed, got %v", err)
	}
	if err := d.queryRowScan(context.Background(), []any{new(int)}, "SELECT 1"); !errors.Is(err, ErrClosed) {
		t.Fatalf("a remote queryRowScan after beginClosing must be ErrClosed, got %v", err)
	}
}

// TestRemoteQueryRowsHoldsAdmissionThroughScan pins the round-8 P2: on a remote store, queryRows must
// keep its admission registered for the whole read — query, scan, and Rows.Close — not just the query
// call. Otherwise Close's inFlight.Wait could return and tear the pool down while a slow scan is still
// reading rows.
func TestRemoteQueryRowsHoldsAdmissionThroughScan(t *testing.T) {
	d, _ := remoteDBFailingFirst(t, 0)

	inScan := make(chan struct{})
	release := make(chan struct{})
	qDone := make(chan error, 1)
	go func() {
		qDone <- d.queryRows(context.Background(), "SELECT 1", func(*sql.Rows) error {
			close(inScan)
			<-release // hold the scan open
			return nil
		})
	}()
	<-inScan

	// A Close arriving now must wait for the scan: its inFlight.Wait must not return yet.
	d.beginClosing()
	waitDone := make(chan struct{})
	go func() { d.inFlight.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
		t.Fatal("inFlight.Wait returned while a remote scan was still active — Close could tear the pool down mid-scan")
	case <-time.After(150 * time.Millisecond):
		// Good: the scan still holds the admission.
	}

	close(release)
	if err := <-qDone; err != nil {
		t.Fatalf("queryRows: %v", err)
	}
	<-waitDone // now the wait completes
}

// TestCloseWakesLongPollWaiters pins the round-8 follow-up: a Receive long-polling an EMPTY queue
// parks on the notifier, outside the admission gate (deliberately — Close must not wait 20s for it).
// Nothing else ever wakes that waiter, so before the fix Close() returned while the Receive slept
// out its full window — up to 20 seconds — against an engine that was already torn down (the
// reviewer's race probe reproduced 10/10). Close now closes e.closed, and the long-poll select has
// an arm for it: every parked waiter returns ErrClosed promptly.
func TestCloseWakesLongPollWaiters(t *testing.T) {
	ctx := context.Background()
	e, err := Open(ctx, Options{DB: "file:" + filepath.Join(t.TempDir(), "mq.db"), DisableBackground: true})
	if err != nil {
		t.Fatal(err)
	}
	mustQueue(t, e, "q", QueueConfig{})

	const waiters = 4
	done := make(chan error, waiters)
	for i := 0; i < waiters; i++ {
		go func() {
			// A 10s long-poll on an empty queue: with the bug, this outlives Close by ~10s.
			msgs, rerr := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 1, WaitMs: 10_000})
			if rerr == nil && len(msgs) > 0 {
				done <- fmt.Errorf("received %d messages from an empty queue", len(msgs))
				return
			}
			done <- rerr
		}()
	}
	time.Sleep(150 * time.Millisecond) // let the waiters park on the notifier

	start := time.Now()
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	for i := 0; i < waiters; i++ {
		select {
		case rerr := <-done:
			// ErrClosed is the woken-by-Close path. A nil (empty result) is tolerated only if the
			// waiter returned BEFORE Close got to it — but never late.
			if rerr != nil && !errors.Is(rerr, ErrClosed) {
				t.Fatalf("waiter returned %v, want ErrClosed", rerr)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("a long-poll waiter was still parked %.1fs after Close returned — Close must wake "+
				"every waiter, not leave it to sleep out its window against a torn-down engine",
				time.Since(start).Seconds())
		}
	}
}
