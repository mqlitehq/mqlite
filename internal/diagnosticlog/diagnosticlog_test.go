package diagnosticlog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
)

func TestRotationRestartAndPermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "diagnostics.jsonl")
	w, err := open(path, 64)
	if err != nil {
		t.Fatal(err)
	}
	for n := 0; n < 10; n++ {
		// Exactly two records fit into each file.
		if _, err := fmt.Fprintf(w, "%031d\n", n); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopening appends; a broker restart does not truncate historical evidence.
	w, err = open(path, 64)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(w, "%031d\n", 10); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("closed")); err == nil {
		t.Fatal("write after close succeeded")
	}
	files, err := filepath.Glob(path + "*")
	if err != nil || len(files) != 4 {
		t.Fatalf("files=%v err=%v", files, err)
	}
	var all string
	for _, file := range files {
		info, err := os.Stat(file)
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() > 64 {
			t.Fatal("rotation exceeded bound")
		}
		if runtime.GOOS != "windows" && info.Mode().Perm() != 0600 {
			t.Fatalf("permissions=%v", info.Mode())
		}
		content, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		all += string(content)
	}
	for n := 4; n <= 10; n++ {
		if !strings.Contains(all, fmt.Sprintf("%031d\n", n)) {
			t.Fatalf("missing retained record %d", n)
		}
	}
	if strings.Contains(all, fmt.Sprintf("%031d\n", 3)) {
		t.Fatal("oldest record not rotated out")
	}
}

func TestHandlerSelectionConcurrencyAndWriteFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "diagnostics.jsonl")
	var console bytes.Buffer
	h, err := New(slog.NewJSONHandler(&console, nil), path)
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(h).With("instance", "test").WithGroup("diagnostic")
	logger.Info("routine request", "secret", "must-not-be-journaled")
	var wg sync.WaitGroup
	for n := 0; n < 20; n++ {
		wg.Add(1)
		go func(n int) { defer wg.Done(); logger.Error("maintenance.failure", "task", "locks", "record", n) }(n)
	}
	wg.Wait()
	logger.Info("maintenance.recovered", "task", "locks")
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(content), []byte("\n"))
	if len(lines) != 21 || bytes.Contains(content, []byte("must-not-be-journaled")) {
		t.Fatal("journal selection failed")
	}
	for _, line := range lines {
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		if record["instance"] != "test" || record["diagnostic"].(map[string]any)["task"] != "locks" {
			t.Fatal("lost attributes/groups")
		}
	}
	// A failed journal must still emit each failure to the operator's stderr logger.
	logger.Error("storage.failure", "record", "after-close-1")
	logger.Error("storage.failure", "record", "after-close-2")
	if !strings.Contains(console.String(), "after-close-1") || !strings.Contains(console.String(), "after-close-2") || strings.Count(console.String(), "diagnostic log write failed") != 1 {
		t.Fatal("journal failure hid errors or flooded warnings")
	}
}

func TestInvalidDestinationAndOversize(t *testing.T) {
	root := t.TempDir()
	for _, path := range []string{root, filepath.Join(root, "missing", "log")} {
		if h, err := New(slog.Default().Handler(), path); err == nil {
			h.Close()
			t.Fatalf("accepted invalid path %s", path)
		}
	}
	target := filepath.Join(root, "target")
	if err := os.WriteFile(target, []byte("existing"), 0644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(target, link); err == nil {
		if h, err := New(slog.Default().Handler(), link); err == nil {
			h.Close()
			t.Fatal("accepted symlink")
		}
	}
	w, err := open(target, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.Write(bytes.Repeat([]byte("x"), 33)); err == nil {
		t.Fatal("accepted oversize record")
	}
	if content, err := os.ReadFile(target); err != nil || string(content) != "existing" {
		t.Fatal("oversize write changed existing file")
	}
}
