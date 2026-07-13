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

// EndpointAuthority returns the target an endpoint string actually resolves to —
// `scheme://host:port`, lower-cased, credentials and path/trailing-slash noise removed.
//
// This is the boundary that matters when deciding whether two endpoints are the same broker:
// `http://h:6754` and `http://h:6754/` are, and so are `mqlite://h` and `http://h:6754` (the
// custom scheme supplies the product port). Comparing raw text instead would call them
// different hosts. It is deliberately derived from parseDSN, so it can never drift from the
// endpoint the client dials.
func EndpointAuthority(dsn string) (string, error) {
	ep, _, err := parseDSN(dsn)
	if err != nil {
		return "", err
	}
	u, err := url.Parse(ep)
	if err != nil {
		return "", fmt.Errorf("mqlite: bad connection string: %w", err)
	}
	scheme := strings.ToLower(u.Scheme)
	port := u.Port()
	if port == "" { // plain http(s) keeps standard Web ports
		port = "80"
		if scheme == "https" {
			port = "443"
		}
	}
	return scheme + "://" + net.JoinHostPort(strings.ToLower(u.Hostname()), port), nil
}
