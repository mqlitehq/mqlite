package mqlite_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// docs/conformance.md is the spec, and it cites the test that proves each rule. A citation that no
// longer resolves is worse than no citation: it reads as evidence while proving nothing, and it is
// exactly what happens when a test is renamed or moved (the repo's own conventions warn about this,
// and nothing enforced it).
//
// So the spec's references are pinned. Every file it names must exist, and every TestXxx it names
// must exist in that file. Rename a test and this fails, which forces the spec to be updated with
// it rather than quietly rotting.
func TestConformanceSpecCitesTestsThatExist(t *testing.T) {
	spec, err := os.ReadFile("docs/conformance.md")
	if err != nil {
		t.Fatal(err)
	}
	src := string(spec)

	// Citations look like *(engine/foo_test.go: TestA, TestB)* or *(server/x_test.go, server/y_test.go)*.
	fileRe := regexp.MustCompile(`\b((?:[a-z0-9_]+/)*[a-z0-9_]+_test\.go)\b`)
	files := map[string]bool{}
	for _, m := range fileRe.FindAllStringSubmatch(src, -1) {
		files[m[1]] = true
	}
	if len(files) < 5 {
		t.Fatalf("found only %d test-file citations in the spec — the regex has stopped matching", len(files))
	}

	bodies := map[string]string{}
	for f := range files {
		b, err := os.ReadFile(filepath.Clean(f))
		if err != nil {
			t.Errorf("docs/conformance.md cites %s, which does not exist. A spec that points at a\n"+
				"missing test reads as evidence and proves nothing — fix the citation or restore the test.", f)
			continue
		}
		bodies[f] = string(b)
	}

	// Every TestXxx named anywhere in the spec must exist in SOME cited file. (The spec names tests
	// next to the file they live in, but not always in a parseable one-to-one shape, so this checks
	// the weaker — and still rot-proof — property that the name resolves at all.)
	testRe := regexp.MustCompile(`\bTest[A-Z][A-Za-z0-9_]*`)
	named := map[string]bool{}
	for _, m := range testRe.FindAllString(src, -1) {
		named[m] = true
	}
	for name := range named {
		found := false
		for _, body := range bodies {
			if strings.Contains(body, "func "+name+"(") {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("docs/conformance.md cites %s, but no cited test file defines it.\n"+
				"Renaming a test without updating the spec leaves the rule looking proven when it is not.", name)
		}
	}
}
