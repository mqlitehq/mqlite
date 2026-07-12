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
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

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
		wantVersion = "3"
		wantHash    = "9cde8ede1b6c3a2018ea57491a0a4f8441bf4c0a9ddfc59156bede68624ddf0f"
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
