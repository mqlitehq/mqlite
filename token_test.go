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
	if len(hexPart) != 32 { // 16 bytes = 128 bits
		t.Fatalf("hex part %q has %d chars, want 32 (128-bit)", hexPart, len(hexPart))
	}
	if _, err := hex.DecodeString(hexPart); err != nil {
		t.Fatalf("token body is not hex: %v", err)
	}
	if b := mqlite.GenerateToken(); a == b {
		t.Fatal("two generated tokens must differ (randomness)")
	}
}
