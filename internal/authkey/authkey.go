// Package authkey defines the common format of generated broker credentials.
package authkey

import (
	"crypto/rand"
	"encoding/hex"
	"io"
	"strings"
)

// Prefix identifies a newly generated MQLite access token.
const Prefix = "mqk_"

// GenerateToken generates a 256-bit secret using the operating system's CSPRNG.
func GenerateToken() (string, error) { return generate(rand.Reader, Prefix, 32) }

// GenerateID generates a public 128-bit identifier, independent of any secret.
func GenerateID() (string, error) { return generate(rand.Reader, "", 16) }

func generate(source io.Reader, prefix string, size int) (string, error) {
	b := make([]byte, size)
	if _, err := io.ReadFull(source, b); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(b), nil
}

// ValidToken accepts only the canonical, case-sensitive generated token format.
func ValidToken(token string) bool {
	return strings.HasPrefix(token, Prefix) && lowerHex(token[len(Prefix):], 64)
}

// ValidID accepts exactly 32 lowercase hexadecimal characters.
func ValidID(id string) bool { return lowerHex(id, 32) }

func lowerHex(s string, size int) bool {
	if len(s) != size {
		return false
	}
	for i := range s {
		if (s[i] < '0' || s[i] > '9') && (s[i] < 'a' || s[i] > 'f') {
			return false
		}
	}
	return true
}
