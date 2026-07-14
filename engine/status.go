package engine

import (
	"context"
	"net/url"
	"os"
	"strings"
	"time"
)

// Status is a desensitized snapshot of the engine's runtime, safe to surface in an admin
// UI. The location of a local DB is its on-disk path (shown to help operators); a remote
// DB's host is masked and its auth token is NEVER part of any field.
type Status struct {
	Backend       string // "memory" | "local file" | "remote libSQL/Turso"
	Remote        bool
	Location      string // ":memory:", a local path, or libsql://***.host (masked)
	SchemaVersion string
	PingMs        int64 // a SELECT 1 read round-trip (the durable read latency); -1 if it failed
	SizeBytes     int64 // local on-disk footprint (db + -wal + -shm); 0 for memory/remote
}

// Status probes the engine's backend without exposing any secret. The ping is a real
// wall-clock read round-trip (most meaningful for a remote DB), independent of the
// injectable clock.
func (e *Engine) Status(ctx context.Context) Status {
	s := Status{SchemaVersion: schemaVersion, Remote: e.db.remote}
	dsn := e.db.dsn
	low := strings.ToLower(strings.TrimSpace(dsn))
	switch {
	case e.db.remote:
		s.Backend = "remote libSQL/Turso"
		s.Location = redactRemoteDSN(dsn)
	case low == "" || strings.Contains(low, ":memory:"):
		s.Backend = "memory"
		s.Location = ":memory:"
	default:
		s.Backend = "local file"
		path := strings.TrimPrefix(dsn, "file:")
		s.Location = path
		s.SizeBytes = localFootprint(path)
	}

	t0 := time.Now()
	var one int
	// queryRowScan, not e.db.sql: Status is reachable from an HTTP handler, so its ctx carries the
	// request deadline — and the one thing that must never happen on a local store is a statement
	// cut off in flight (round-6 §4).
	if err := e.db.queryRowScan(ctx, []any{&one}, "SELECT 1"); err != nil {
		s.PingMs = -1
	} else {
		s.PingMs = time.Since(t0).Milliseconds()
	}
	return s
}

// redactRemoteDSN keeps the scheme + the host's parent domain (so an operator sees the
// provider, e.g. turso.io) but masks the database-identifying first label. The token is
// never in dsn to begin with — it's injected only into the connection string.
func redactRemoteDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.Host == "" {
		return "remote"
	}
	host := u.Hostname()
	if i := strings.IndexByte(host, '.'); i >= 0 {
		return u.Scheme + "://***" + host[i:]
	}
	return u.Scheme + "://***"
}

// localFootprint sums the on-disk bytes a local DB occupies: the main file plus the WAL
// and shared-memory sidecars when present.
func localFootprint(path string) int64 {
	var total int64
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if fi, err := os.Stat(path + suffix); err == nil {
			total += fi.Size()
		}
	}
	return total
}
