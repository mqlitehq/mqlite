package mqlite_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode"
)

// TestRepoEnglishOnly is the anti-rot guard for the repo's English-only rule (CLAUDE.md,
// MQLITE-91): it walks the source tree and fails on any CJK (Han) character outside the
// few files that deliberately embed non-ASCII UNICODE FIXTURES (they assert the queue
// carries arbitrary bytes in bodies/properties). So Chinese creeping back into a comment
// or a doc is loud, not silently committed.
func TestRepoEnglishOnly(t *testing.T) {
	// Files that legitimately contain non-ASCII: they test Unicode round-tripping, not prose.
	fixtureFiles := map[string]bool{
		filepath.FromSlash("engine/functional_test.go"): true,
		filepath.FromSlash("test/api_tests.py"):         true,
		filepath.FromSlash("test/sdkcheck/main.go"):     true,
	}
	// Dirs to skip: VCS, the embedded (minified) web console, and generated bench output.
	skipDirs := map[string]bool{
		".git":           true,
		"server/web":     true,
		"test/bench/out": true,
		"bin":            true,
		"node_modules":   true,
	}
	srcExt := map[string]bool{".go": true, ".md": true, ".sh": true, ".py": true}

	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel := filepath.ToSlash(path)
		if d.IsDir() {
			if skipDirs[rel] {
				return fs.SkipDir
			}
			return nil
		}
		if !srcExt[filepath.Ext(path)] || fixtureFiles[path] {
			return nil
		}
		b, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		for i, line := range strings.Split(string(b), "\n") {
			for _, r := range line {
				if unicode.Is(unicode.Han, r) {
					t.Errorf("%s:%d contains non-English (Han) text — the repo is English-only: %q",
						rel, i+1, strings.TrimSpace(line))
					break
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
}
