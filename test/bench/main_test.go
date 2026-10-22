package main

import (
	"context"
	"math"
	"path/filepath"
	"testing"

	"github.com/mqlitehq/mqlite/engine"
)

func TestBloatScenarioPrefillsExactNonFullTailOnce(t *testing.T) {
	oldDir, oldDSN, oldCap := *dir, *dbDSN, *prefillCap
	*dir, *dbDSN, *prefillCap = t.TempDir(), "", 0
	t.Cleanup(func() {
		*dir, *dbDSN, *prefillCap = oldDir, oldDSN, oldCap
	})

	const messages = 65 // one full batch plus a one-message tail
	result, err := run(scen{
		Name:    "bloat_exact_tail",
		Kind:    "bloat",
		Prefill: messages,
		Batch:   64,
		Msg:     8,
	})
	if err != nil {
		t.Fatalf("run bloat scenario: %v", err)
	}
	if result.Ops != messages {
		t.Fatalf("drained ops=%d, want exactly one %d-message prefill", result.Ops, messages)
	}
	if result.FileBytesPerMsg <= 0 {
		t.Fatalf("file bytes per message=%v, want a measured local-file footprint", result.FileBytesPerMsg)
	}
	wantBytesPerMessage := result.DBPeakMB * (1 << 20) / messages
	if math.Abs(result.FileBytesPerMsg-wantBytesPerMessage) > 0.01 {
		t.Fatalf("file bytes per message=%v, want peak bytes divided by actual prefill=%v", result.FileBytesPerMsg, wantBytesPerMessage)
	}

	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{
		DB:                "file:" + filepath.Join(*dir, "bloat_exact_tail.db"),
		DisableBackground: true,
	})
	if err != nil {
		t.Fatalf("reopen benchmark database: %v", err)
	}
	defer eng.Close()
	stats, err := eng.Stats(ctx, "bench")
	if err != nil {
		t.Fatalf("read drained queue stats: %v", err)
	}
	if stats.Total != 0 {
		t.Fatalf("queue retains %d messages after exact drain", stats.Total)
	}
}
