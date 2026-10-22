package main

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	if !result.OK || result.Error != "" {
		t.Fatalf("result status ok=%t error=%q, want success", result.OK, result.Error)
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

func TestBenchmarkFilesystemHelpersFailClosed(t *testing.T) {
	t.Run("directory success and failure", func(t *testing.T) {
		root := t.TempDir()
		dir := filepath.Join(root, "nested", "bench")
		if err := ensureBenchmarkDir(dir); err != nil {
			t.Fatalf("create benchmark directory: %v", err)
		}
		if info, err := os.Stat(dir); err != nil || !info.IsDir() {
			t.Fatalf("benchmark directory stat: info=%v err=%v", info, err)
		}

		blocker := filepath.Join(root, "blocker")
		if err := os.WriteFile(blocker, []byte("file"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := ensureBenchmarkDir(filepath.Join(blocker, "child")); err == nil {
			t.Fatal("ensureBenchmarkDir succeeded through a file")
		}
	})

	t.Run("stale database cleanup", func(t *testing.T) {
		base := filepath.Join(t.TempDir(), "bench.db")
		for _, ext := range []string{"", "-wal", "-shm"} {
			if err := os.WriteFile(base+ext, []byte("stale"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		if err := resetBenchmarkDBFiles(base); err != nil {
			t.Fatalf("remove stale database files: %v", err)
		}
		for _, ext := range []string{"", "-wal", "-shm"} {
			if _, err := os.Stat(base + ext); !os.IsNotExist(err) {
				t.Fatalf("stale file %q remains: %v", base+ext, err)
			}
		}

		blocked := filepath.Join(t.TempDir(), "non-empty-directory")
		if err := os.Mkdir(blocked, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(blocked, "child"), []byte("keep"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := resetBenchmarkDBFiles(blocked); err == nil {
			t.Fatal("resetBenchmarkDBFiles ignored a non-removable stale path")
		}
	})

	t.Run("JSON success and failures", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "result.json")
		want := map[string]any{"ops": float64(7)}
		if err := writeBenchmarkJSON(path, want); err != nil {
			t.Fatalf("write benchmark JSON: %v", err)
		}
		var got map[string]any
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(b, &got); err != nil {
			t.Fatalf("decode benchmark JSON: %v", err)
		}
		if got["ops"] != want["ops"] {
			t.Fatalf("ops=%v, want %v", got["ops"], want["ops"])
		}
		if err := writeBenchmarkJSON(path, make(chan int)); err == nil || !strings.Contains(err.Error(), "encode benchmark results") {
			t.Fatalf("marshal error=%v, want contextual failure", err)
		}
		missingParent := filepath.Join(t.TempDir(), "missing", "result.json")
		if err := writeBenchmarkJSON(missingParent, want); err == nil || !strings.Contains(err.Error(), "write benchmark results") {
			t.Fatalf("write error=%v, want contextual failure", err)
		}
	})

	t.Run("output path defaults and validation", func(t *testing.T) {
		if got := resolveBenchmarkOutputPath("", "/data", false); got != filepath.Join("/data", "results.json") {
			t.Fatalf("local default output=%q", got)
		}
		if got := resolveBenchmarkOutputPath("", "/data", true); got != "results.json" {
			t.Fatalf("remote default output=%q", got)
		}
		if err := validateBenchmarkOutputPath(filepath.Join(t.TempDir(), "missing", "result.json")); err == nil {
			t.Fatal("validateBenchmarkOutputPath accepted a missing parent")
		}
	})
}

func TestBenchmarkCLIWriteFailureExitsNonzero(t *testing.T) {
	if os.Getenv("MQLITE_BENCH_WRITE_FAILURE_HELPER") == "1" {
		main()
		return
	}

	root := t.TempDir()
	cmd := exec.Command(os.Args[0],
		"-test.run=^TestBenchmarkCLIWriteFailureExitsNonzero$",
		"-dir", filepath.Join(root, "data"),
		"-out", root,
		"-only", "__no_such_scenario__",
	)
	cmd.Env = append(os.Environ(), "MQLITE_BENCH_WRITE_FAILURE_HELPER=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("benchmark CLI succeeded despite output write failure:\n%s", out)
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() == 0 {
		t.Fatalf("benchmark CLI error=%v, want nonzero exit", err)
	}
	text := string(out)
	if !strings.Contains(text, "write benchmark results") {
		t.Fatalf("benchmark CLI output omits write failure:\n%s", text)
	}
	if strings.Contains(text, "results written to") {
		t.Fatalf("benchmark CLI printed false success after write failure:\n%s", text)
	}
}

func TestRunMarksFailureForMachineReaders(t *testing.T) {
	oldDir, oldDSN := *dir, *dbDSN
	*dir, *dbDSN = t.TempDir(), ""
	t.Cleanup(func() { *dir, *dbDSN = oldDir, oldDSN })

	blockedDB := filepath.Join(*dir, "blocked.db")
	if err := os.Mkdir(blockedDB, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(blockedDB, "child"), []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	result, err := run(scen{Name: "blocked", Kind: "produce", P: 1, Msg: 8})
	if err == nil {
		t.Fatal("run succeeded despite stale database cleanup failure")
	}
	if result.OK || result.Error == "" {
		t.Fatalf("failed result status ok=%t error=%q", result.OK, result.Error)
	}
}

func TestWorkloadsFailOnStorageErrors(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: "file:" + filepath.Join(t.TempDir(), "closed.db"), DisableBackground: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.CreateQueue(ctx, "q", engine.QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		run  func() error
	}{
		{"produce", func() error { _, _, err := produce(ctx, eng, "q", time.Second, 1, 8, ""); return err }},
		{"batch", func() error { _, _, err := batchProduce(ctx, eng, "q", time.Second, 1, 2, 8); return err }},
		{"sessions", func() error { _, _, _, err := sessionsRun(ctx, eng, "q", time.Second, 1, 1, 1, 8); return err }},
		{"e2e", func() error { _, _, _, err := e2eRun(ctx, eng, "q", time.Second, 1, 1, 8); return err }},
		{"drain", func() error { _, _, err := drain(ctx, eng, "q", 1); return err }},
		{"properties", func() error { _, _, err := propsRun(ctx, eng, "q", time.Second, 1, 2, 8); return err }},
		{"bloat", func() error { _, _, err := bloatRun(ctx, eng, "q", 1); return err }},
		{"ramp-down", func() error { _, _, _, err := rampDownRun(ctx, eng, "q", time.Second, 1, 1, 8); return err }},
		{"consumer churn", func() error { _, _, _, err := churnRun(ctx, eng, "q", time.Second, 1, 1, 8); return err }},
		{"lifecycle churn", func() error { _, err := lifecycleChurnRun(ctx, eng, "q", 0, 8, nil); return err }},
		{"prefill", func() error { _, err := prefill(ctx, eng, "q", 1, 1, 8); return err }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			started := time.Now()
			if err := tt.run(); err == nil {
				t.Fatal("workload ignored storage failure")
			}
			if elapsed := time.Since(started); elapsed > 2*time.Second {
				t.Fatalf("workload took %s to report storage failure", elapsed)
			}
		})
	}
}

func TestDrainRejectsMismatchedPrefillAccounting(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: "file:" + filepath.Join(t.TempDir(), "drain.db"), DisableBackground: true})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	if err := eng.CreateQueue(ctx, "q", engine.QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	if n, err := prefill(ctx, eng, "q", 65, 64, 8); err != nil || n != 65 {
		t.Fatalf("prefill n=%d err=%v", n, err)
	}
	completed, _, err := drain(ctx, eng, "q", 2)
	if err != nil {
		t.Fatalf("drain messages: %v", err)
	}
	if _, err := verifyQueueAccounting(ctx, eng, "q", 64, completed, true); err == nil || !strings.Contains(err.Error(), "does not reconcile") {
		t.Fatalf("drain completed=%d err=%v, want accounting failure", completed, err)
	}
	if completed != 65 {
		t.Fatalf("drain completed=%d, want actual count 65", completed)
	}
}

func TestMixedWorkloadsReconcileAcceptedCompletedAndRemaining(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: "file:" + filepath.Join(t.TempDir(), "accounting.db"), DisableBackground: true})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	tests := []struct {
		name string
		run  func(string) (int64, int64, error)
	}{
		{"e2e", func(q string) (int64, int64, error) {
			completed, _, accepted, err := e2eRun(ctx, eng, q, 250*time.Millisecond, 1, 1, 8)
			return completed, accepted, err
		}},
		{"ramp-down", func(q string) (int64, int64, error) {
			completed, _, accepted, err := rampDownRun(ctx, eng, q, 250*time.Millisecond, 1, 1, 8)
			return completed, accepted, err
		}},
		{"consumer-churn", func(q string) (int64, int64, error) {
			completed, _, accepted, err := churnRun(ctx, eng, q, 250*time.Millisecond, 1, 1, 8)
			return completed, accepted, err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := eng.CreateQueue(ctx, tt.name, engine.QueueConfig{}); err != nil {
				t.Fatal(err)
			}
			completed, accepted, err := tt.run(tt.name)
			if err != nil {
				t.Fatalf("run workload: %v", err)
			}
			if accepted == 0 {
				t.Fatal("workload accepted no messages")
			}
			if _, err := verifyQueueAccounting(ctx, eng, tt.name, accepted, completed, false); err != nil {
				t.Fatalf("verify accounting: %v", err)
			}
		})
	}
}

func TestCleanupBenchmarkQueuesDeletesOnlyOwnedNamespace(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: "file:" + filepath.Join(t.TempDir(), "cleanup.db"), DisableBackground: true})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	prefix := benchmarkQueueName("cleanup")
	owned := []string{prefix, prefix + "-child"}
	for _, q := range append(append([]string(nil), owned...), "preexisting") {
		if err := eng.CreateQueue(ctx, q, engine.QueueConfig{}); err != nil {
			t.Fatalf("create %q: %v", q, err)
		}
	}
	if _, err := prefill(ctx, eng, owned[0], 1101, 64, 8); err != nil {
		t.Fatalf("prefill owned queue: %v", err)
	}
	if _, err := prefill(ctx, eng, owned[1], 1, 1, 8); err != nil {
		t.Fatalf("prefill owned child queue: %v", err)
	}
	if _, err := prefill(ctx, eng, "preexisting", 1, 1, 8); err != nil {
		t.Fatalf("prefill unrelated queue: %v", err)
	}

	cleanupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := cleanupBenchmarkQueues(cleanupCtx, eng, prefix); err != nil {
		t.Fatalf("cleanup benchmark namespace: %v", err)
	}
	queues, err := eng.ListQueues(ctx)
	if err != nil {
		t.Fatalf("list queues after cleanup: %v", err)
	}
	for _, q := range queues {
		if q.Name == prefix || strings.HasPrefix(q.Name, prefix+"-") {
			t.Fatalf("owned queue %q remains after cleanup", q.Name)
		}
	}
	stats, err := eng.Stats(ctx, "preexisting")
	if err != nil {
		t.Fatalf("unrelated queue removed: %v", err)
	}
	if stats.Total != 1 {
		t.Fatalf("unrelated queue total=%d, want 1", stats.Total)
	}
}
