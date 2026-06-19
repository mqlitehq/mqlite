package engine

import (
	"context"
	"path/filepath"
	"testing"
)

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
