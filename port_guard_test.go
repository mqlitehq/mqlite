package mqlite

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestNoStrayLegacyPort guards the 6754 migration (MQLITE-84): the previous default broker
// port must not reappear as a casual default anywhere in the tracked source or docs. The
// only place allowed to name it is deliberate upgrade/compat documentation (CHANGELOG.md).
// Any new occurrence fails loudly so the old port can't quietly creep back as a default.
//
// The needle is assembled at runtime so this guard never matches itself.
func TestNoStrayLegacyPort(t *testing.T) {
	needle := "80" + "80"

	allow := map[string]bool{
		filepath.FromSlash("CHANGELOG.md"): true, // documents the changed default + compat mapping
	}
	skipDir := map[string]bool{
		".git": true, "node_modules": true, "bin": true, "testdata": true,
		"out": true, "web": true, // server/web is a vendored, minified console dist
	}
	textExt := map[string]bool{
		".go": true, ".md": true, ".yml": true, ".yaml": true, ".toml": true,
		".sh": true, ".py": true, ".txt": true, ".example": true, ".mjs": true,
	}

	err := filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if skipDir[info.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		rel := strings.TrimPrefix(filepath.ToSlash(path), "./")
		if allow[filepath.FromSlash(rel)] {
			return nil
		}
		if info.Name() != "Dockerfile" && !textExt[filepath.Ext(info.Name())] {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for i, ln := range strings.Split(string(b), "\n") {
			if strings.Contains(ln, needle) {
				t.Errorf("%s:%d uses the legacy default port (switch to 6754, or allowlist in "+
					"port_guard_test.go if it is deliberate migration docs): %s", rel, i+1, strings.TrimSpace(ln))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
}
