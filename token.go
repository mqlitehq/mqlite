package mqlite

import (
	"crypto/rand"
	"encoding/hex"
)

// TokenPrefix is the conventional prefix for a broker auth token, so a token is
// recognizable at a glance (and greppable in logs/config). It is a convention, not a
// requirement — any non-empty string in MQLITE_TOKENS is accepted as a token.
const TokenPrefix = "mqk_"

// GenerateToken mints a fresh broker auth token: the "mqk_" prefix plus 128 bits of
// crypto/rand, hex-encoded (e.g. "mqk_" + 32 hex chars). Use it to seed MQLITE_TOKENS /
// WithTokens, or let `mqlite serve` call it automatically when no token is configured
// (secure by default). 128 bits is unguessable; bump the byte count here if a longer
// secret is ever wanted.
func GenerateToken() string {
	var b [16]byte
	// crypto/rand.Read fills the buffer or, on a platform without a working CSPRNG,
	// returns an error — in which case we must not hand back a weak token.
	if _, err := rand.Read(b[:]); err != nil {
		panic("mqlite: secure random source unavailable: " + err.Error())
	}
	return TokenPrefix + hex.EncodeToString(b[:])
}
