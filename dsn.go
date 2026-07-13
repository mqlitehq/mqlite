package mqlite

import (
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/mqlitehq/mqlite/internal/defaults"
)

// parseDSN turns an mqlite connection string into (endpoint, token).
//
//	mqlite://<token>@host        -> http://host:6754   + token  (product port supplied)
//	mqlites://<token>@host       -> https://host:6754  + token  (needs a TLS terminator)
//	mqlite://<token>@host:9000   -> http://host:9000   + token  (explicit port wins)
//	http://host / https://host   -> used as-is, standard 80/443 (token via WithToken/env)
//
// Only the product-specific schemes get the broker's default port when it is omitted;
// plain HTTP(S) keeps standard Web semantics so the client never guesses a port for a
// generic URL.
func parseDSN(dsn string) (endpoint, token string, err error) {
	if dsn == "" {
		return "", "", fmt.Errorf("mqlite: empty connection string")
	}
	// Plain HTTP endpoints pass through unchanged.
	if strings.HasPrefix(dsn, "http://") || strings.HasPrefix(dsn, "https://") {
		return strings.TrimRight(dsn, "/"), "", nil
	}
	u, err := url.Parse(dsn)
	if err != nil {
		return "", "", fmt.Errorf("mqlite: bad connection string: %w", err)
	}
	switch u.Scheme {
	case "mqlite", "mqlites":
	default:
		return "", "", fmt.Errorf("mqlite: unsupported scheme %q (use mqlite://, mqlites://, http://, https://)", u.Scheme)
	}
	if u.User != nil {
		token = u.User.Username()
	}
	scheme := "http"
	if u.Scheme == "mqlites" || u.Query().Get("tls") == "true" {
		scheme = "https"
	}
	if u.Host == "" {
		return "", "", fmt.Errorf("mqlite: missing host in connection string")
	}
	// Supply the product default port only when the custom scheme omits one; an explicit
	// port always wins. net.JoinHostPort re-brackets IPv6 hosts correctly.
	host := u.Host
	if u.Port() == "" {
		host = net.JoinHostPort(u.Hostname(), defaults.BrokerPort)
	}
	return scheme + "://" + host, token, nil
}

// EndpointIdentity returns the identity of the broker an endpoint string addresses: it is
// literally the base URL the client would dial, i.e. the string Client.post concatenates the
// RPC route onto. Two endpoints reach the same broker exactly when this value is equal.
//
// Use it to answer "are these two endpoints the same broker?" — the question that decides
// whether an ambient token may be reused for an endpoint the caller overrode.
//
// It deliberately does NOT try to canonicalize a URL. Deciding which parts of a URL are
// "insignificant" is a losing game — the path, percent-escaping, a query, a fragment, an IPv6
// zone id and host case all change (or fail to change) where a reverse proxy actually routes
// the request, and every component you normalize away is a chance to call two different
// brokers the same and hand one of them the other's credential. Comparing the dialed string
// itself cannot make that mistake: if the bytes we send differ, we treat it as a different
// broker and withhold the token. The failure mode is a warning and an explicit --token, never
// a leak.
//
// What it does normalize is only what parseDSN already normalizes to build the dial target —
// an insignificant trailing slash (the reported bug: `http://h:6754/` cost the caller their
// token), the product default port for the custom schemes, and a credential embedded in an
// `mqlite://token@host` DSN (which is not part of the target).
func EndpointIdentity(dsn string) (string, error) {
	ep, _, err := parseDSN(dsn) // exactly the base URL the client dials
	return ep, err
}
