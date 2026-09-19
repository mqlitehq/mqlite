package mqlite_test

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/mqlitehq/mqlite"
)

func TestGenerateToken(t *testing.T) {
	a := mqlite.GenerateToken()
	if !strings.HasPrefix(a, mqlite.TokenPrefix) {
		t.Fatalf("token %q missing %q prefix", a, mqlite.TokenPrefix)
	}
	hexPart := strings.TrimPrefix(a, mqlite.TokenPrefix)
	if len(hexPart) != 64 { // 32 bytes = 256 bits
		t.Fatalf("hex part has %d chars, want 64 (256-bit)", len(hexPart))
	}
	if hexPart != strings.ToLower(hexPart) {
		t.Fatal("token body must be lowercase")
	}
	if _, err := hex.DecodeString(hexPart); err != nil {
		t.Fatalf("token body is not hex: %v", err)
	}
	if b := mqlite.GenerateToken(); a == b {
		t.Fatal("two generated tokens must differ (randomness)")
	}
}

func TestGenerateKeyID(t *testing.T) {
	a := mqlite.GenerateKeyID()
	if len(a) != 32 || a != strings.ToLower(a) {
		t.Fatalf("invalid key ID format: %q", a)
	}
	if _, err := hex.DecodeString(a); err != nil {
		t.Fatalf("key ID is not hex: %v", err)
	}
	if b := mqlite.GenerateKeyID(); a == b {
		t.Fatal("two generated key IDs must differ")
	}
}
