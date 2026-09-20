package mqlite

import "github.com/mqlitehq/mqlite/internal/authkey"

// TokenPrefix is the conventional prefix for a broker auth token, so a token is
// recognizable at a glance (and greppable in logs/config). It is a convention, not a
// requirement — any non-empty string in MQLITE_TOKENS is accepted as a token.
const TokenPrefix = authkey.Prefix

// GenerateToken mints a fresh broker auth token: the "mqk_" prefix plus 256 bits of
// crypto/rand, hex-encoded ("mqk_" + 64 lowercase hex characters). Use it to seed MQLITE_TOKENS /
// WithTokens, or let `mqlite serve` call it automatically when no token is configured
// (secure by default). It panics if the secure random source fails.
func GenerateToken() string {
	token, err := authkey.GenerateToken()
	if err != nil {
		panic("mqlite: secure random source unavailable: " + err.Error())
	}
	return token
}

// GenerateKeyID generates a public access key ID (32 lowercase hex characters).
// Retain the ID before calling CreateKey so an uncertain result can be reconciled.
// Like GenerateToken, it panics if the secure random source fails.
func GenerateKeyID() string {
	id, err := authkey.GenerateID()
	if err != nil {
		panic("mqlite: secure random source unavailable: " + err.Error())
	}
	return id
}
