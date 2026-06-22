package engine

import (
	"database/sql"
	"os"
	"testing"
)

// TestMain gives the remote Turso suite a clean slate. mqlite's schema guard refuses
// a database whose schema token differs from this build's, so when MQLITE_TEST_DB is
// set (the creds-gated path) we drop the mqlite tables and let engine.Open rebuild the
// current schema. With no creds set this is a no-op and the local, hermetic engine
// tests run unchanged.
func TestMain(m *testing.M) {
	if dsn := os.Getenv("MQLITE_TEST_DB"); dsn != "" {
		resetRemoteTestDB(dsn, os.Getenv("MQLITE_TEST_DB_AUTH_TOKEN"))
	}
	os.Exit(m.Run())
}

func resetRemoteTestDB(dsn, token string) {
	driver, conn, remote := resolveDSN(dsn, token, "")
	if !remote {
		return // not a remote DSN — nothing to reset
	}
	db, err := sql.Open(driver, conn)
	if err != nil {
		return // best-effort; the tests will surface the real open error
	}
	defer db.Close()
	// Children (FK → queues) first, then queues, then the version stamp in meta —
	// drop order avoids foreign-key violations. A fresh CREATE in engine.Open then
	// reinitialises the exact current schema, sidestepping any column/index drift a
	// re-stamp alone could miss.
	for _, t := range []string{
		"messages", "subscriptions", "dedup", "settlement_receipts",
		"receive_attempts", "queues", "meta",
	} {
		_, _ = db.Exec("DROP TABLE IF EXISTS " + t)
	}
}
