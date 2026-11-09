package hrana

import (
	"encoding/base64"
	"testing"
)

func TestRawBase64ValidationMatchesPinnedDecoder(t *testing.T) {
	check := func(encoded string) {
		t.Helper()
		_, err := base64.RawStdEncoding.DecodeString(encoded)
		if got := validRawBase64(encoded); got != (err == nil) {
			t.Fatalf("base64 validation for %q = %t; standard decoder error=%v", encoded, got, err)
		}
	}
	for _, encoded := range []string{
		"", "aGk", "Zg", "Zm8", "Zm9v", "/w", "//", "///", "aG\rk\n", "\r\n",
		"Zg==", "Zm8=", "!!!!", "A", "AAAAA", "AA AA", "AA\tAA", "____", "\xff",
	} {
		check(encoded)
	}
	// Exhaust short inputs over representatives of every accepted alphabet range
	// plus newline, padding and invalid bytes. The standard decoder is the oracle.
	alphabet := []byte("AZaz09+/\r\n=!_\xff")
	var visit func([]byte, int)
	visit = func(prefix []byte, remaining int) {
		check(string(prefix))
		if remaining == 0 {
			return
		}
		for _, c := range alphabet {
			visit(append(prefix, c), remaining-1)
		}
	}
	visit(make([]byte, 0, 4), 4)
	if allocations := testing.AllocsPerRun(100, func() { _ = validRawBase64("aGVsbG8gd29ybGQ\r\n") }); allocations != 0 {
		t.Fatalf("base64 validation allocated %v times", allocations)
	}
}
