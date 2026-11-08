//go:build mqlite_perf_diag

package main

import (
	"flag"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// Re-exec the test binary to exercise actual flag parsing and process status
// without compiling or downloading another executable or opening a database.
func TestDiagnosticSelectionCLIProcess(t *testing.T) {
	if os.Getenv("MQLITE_DIAG_HERMETIC_CLI_CHILD") == "1" {
		flag.CommandLine = flag.NewFlagSet("diagnostic", flag.ContinueOnError)
		os.Args = []string{"diagnostic", "--validate-selection-only", "--mode", os.Getenv("MQLITE_DIAG_HERMETIC_MODE"), "--case", os.Getenv("MQLITE_DIAG_HERMETIC_CASE")}
		os.Exit(runMain())
	}
	for _, tc := range []struct {
		mode, selection string
		want            int
	}{
		{"mixed", "mixed_native_lock3", 0}, {"mixed", "mixed_low-floor_lock30", 0},
		{"mixed", "original_native_c32_b1", 2}, {"contention", "mixed_native_lock3", 2},
		{"mixed", "mixed_native_lock3,mixed_native_lock30", 2}, {"mixed", "mixed_native_lock3 ", 2},
	} {
		t.Run(tc.mode+"/"+tc.selection, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestDiagnosticSelectionCLIProcess$")
			for _, v := range os.Environ() {
				if !strings.HasPrefix(v, "MQLITE_") {
					cmd.Env = append(cmd.Env, v)
				}
			}
			cmd.Env = append(cmd.Env, "MQLITE_DIAG_HERMETIC_CLI_CHILD=1", "MQLITE_DIAG_HERMETIC_MODE="+tc.mode, "MQLITE_DIAG_HERMETIC_CASE="+tc.selection, "MQLITE_DIAG_STANDARD_DB=libsql://private.invalid", "MQLITE_DIAG_STANDARD_TOKEN=private-cli-canary")
			cmd.Dir = t.TempDir()
			out, err := cmd.CombinedOutput()
			code := 0
			if err != nil {
				if e, ok := err.(*exec.ExitError); ok {
					code = e.ExitCode()
				} else {
					t.Fatal("CLI child failed to run")
				}
			}
			if code != tc.want {
				t.Fatalf("selection CLI exit=%d want=%d", code, tc.want)
			}
			if strings.Contains(string(out), "private.invalid") || strings.Contains(string(out), "private-cli-canary") {
				t.Fatal("selection CLI disclosed credentials")
			}
			entries, err := os.ReadDir(cmd.Dir)
			if err != nil || len(entries) != 0 {
				t.Fatal("selection-only CLI wrote artifacts")
			}
		})
	}
}
