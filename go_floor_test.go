package mqlite_test

import (
	"os"
	"strings"
	"testing"
)

// TestGoModFloorStaysAt121 guards the go.mod floor. MQLite pins it at go 1.21 so
// the SDK stays embeddable in older projects (MQLITE-1). That floor also freezes
// modernc.org/sqlite at v1.36.1 and golang.org/x/sys at v0.30.0 — every later
// release of either requires go >= 1.23 (see docs/dependencies.md). The go 1.21.x
// CI matrix enforces that the code still *builds*; this test enforces *intent*, so
// a floor bump fails with a clear message instead of a cryptic toolchain error and
// the dependency freeze can't be lifted by accident.
func TestGoModFloorStaysAt121(t *testing.T) {
	b, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	const want = "go 1.21"
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "go 1.") {
			if line != want {
				t.Fatalf("go.mod floor = %q, want %q — raising it drops embedding "+
					"compatibility and unfreezes sqlite/x/sys. If intended, update "+
					"docs/dependencies.md and the Dependabot ignore rules deliberately.", line, want)
			}
			return
		}
	}
	t.Fatal("no `go 1.x` directive found in go.mod")
}
