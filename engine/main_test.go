package engine

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
)

const tursoSoakVerificationHost = "mqlite-verify-jiba.aws-ap-northeast-1.turso.io"
const remoteTestResetConfirmEnv = "MQLITE_TEST_DB_RESET_CONFIRM"

// TestMain gives the remote Turso suite a clean slate. mqlite's schema guard refuses
// a database whose schema token differs from this build's, so when MQLITE_TEST_DB is
// set (the creds-gated path) we drop the mqlite tables and let engine.Open rebuild the
// current schema. With no creds set this is a no-op and the local, hermetic engine
// tests run unchanged.
func TestMain(m *testing.M) {
	if dsn := os.Getenv("MQLITE_TEST_DB"); dsn != "" {
		// This check must precede resetRemoteTestDB: checking inside a test would be
		// too late because TestMain resets the remote schema before m.Run.
		if err := validateRemoteTestReset(dsn, os.Getenv(remoteTestResetConfirmEnv), os.Getenv(tursoSoakDurationEnv)); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		if err := resetRemoteTestDB(dsn, os.Getenv("MQLITE_TEST_DB_AUTH_TOKEN")); err != nil {
			fmt.Fprintln(os.Stderr, "remote Turso test reset failed:", err)
			os.Exit(2)
		}
	}
	os.Exit(m.Run())
}

func validateRemoteTestReset(dsn, confirmation, soakDuration string) error {
	if confirmation != "1" {
		return fmt.Errorf("remote Turso test reset refused: %s=1 is required because the suite drops all MQLite tables", remoteTestResetConfirmEnv)
	}
	if _, _, remote := resolveDSN(dsn, "", ""); !remote {
		return fmt.Errorf("remote Turso test reset refused: MQLITE_TEST_DB must use a remote libSQL DSN")
	}
	if soakDuration != "" && !approvedTursoSoakDSN(dsn) {
		return fmt.Errorf("Turso soak refused: MQLITE_TEST_DB is not the dedicated verification database")
	}
	return nil
}

func TestRemoteTestResetGate(t *testing.T) {
	approved := "libsql://" + tursoSoakVerificationHost
	for _, tc := range []struct {
		name         string
		dsn          string
		confirmation string
		soakDuration string
		wantErr      bool
	}{
		{name: "ordinary suite without confirmation", dsn: approved, wantErr: true},
		{name: "confirmed local file", dsn: "file:must-not-touch.db", confirmation: "1", wantErr: true},
		{name: "ordinary suite explicitly confirmed", dsn: "libsql://owned-test-db.turso.io", confirmation: "1"},
		{name: "soak on wrong host", dsn: "libsql://owned-test-db.turso.io", confirmation: "1", soakDuration: "2m", wantErr: true},
		{name: "soak on verification host", dsn: approved, confirmation: "1", soakDuration: "2m"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRemoteTestReset(tc.dsn, tc.confirmation, tc.soakDuration)
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateRemoteTestReset() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func approvedTursoSoakDSN(dsn string) bool {
	u, err := url.Parse(strings.TrimSpace(dsn))
	return err == nil && u.Scheme == "libsql" && u.User == nil && u.Port() == "" &&
		u.Path == "" && u.RawQuery == "" && u.Fragment == "" && strings.EqualFold(u.Hostname(), tursoSoakVerificationHost)
}

func resetRemoteTestDB(dsn, token string) error {
	driver, conn, remote := resolveDSN(dsn, token, "")
	if !remote {
		return fmt.Errorf("MQLITE_TEST_DB must use a remote libSQL DSN")
	}
	db, err := sql.Open(driver, conn)
	if err != nil {
		return err
	}
	// Children (FK → queues) first, then queues, then the version stamp in meta —
	// drop order avoids foreign-key violations. A fresh CREATE in engine.Open then
	// reinitialises the exact current schema, sidestepping any column/index drift a
	// re-stamp alone could miss.
	for _, t := range []string{
		"messages", "subscriptions", "dedup", "settlement_receipts",
		"receive_attempts", "queues", "access_keys", "meta",
	} {
		if _, err := db.Exec("DROP TABLE IF EXISTS " + t); err != nil {
			_ = db.Close()
			return fmt.Errorf("drop table %s: %w", t, err)
		}
	}
	return db.Close()
}
