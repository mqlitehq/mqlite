package engine

// Retry-loop tests for the time-sensitive statement helpers (MQLITE-97).
//
// execFresh/queryFresh exist because a remote retry BACKS OFF: arguments computed once, before
// the loop, are stale by the time a later attempt lands. For a lock deadline that is not
// cosmetic — the write can commit a lease that has already expired, while still reporting
// success, and the reaper reclaims the message immediately.
//
// A fake driver is the only way to see this: retries fire only on a remote store, and a real
// Turso round trip cannot be made to fail on demand. It returns a busy error for the first N
// calls — `database/sql` passes that straight through (unlike driver.ErrBadConn, which it
// retries on its own), so our loop is the one doing the retrying.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"testing"
)

// ─── a driver that is busy for the first n calls ───────────────────────────────

type busyConn struct{ st *busyState }

type busyState struct {
	failsLeft int
	args      [][]driver.NamedValue // the args of every attempt, in order
}

func (c *busyConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("prepare unused") }
func (c *busyConn) Close() error                        { return nil }
func (c *busyConn) Begin() (driver.Tx, error)           { return nil, errors.New("begin unused") }

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
			rows, err := d.queryFresh(ctx, "UPDATE t SET x=?", build)
			if rows != nil {
				_ = rows.Close()
			}
			return err
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
