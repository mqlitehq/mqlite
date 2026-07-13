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

// EndpointIdentity returns the broker an endpoint string actually resolves to, canonicalized:
// `scheme://host:port[/path]`, lower-cased scheme/host, the default port supplied, credentials
// removed, and an insignificant trailing slash dropped.
//
// Use it to answer "are these two endpoints the same broker?" — the question that decides
// whether an ambient token may be reused. Comparing raw text gets it wrong in both directions:
// `http://h:6754` and `http://h:6754/` are one broker (and `mqlite://h` and `http://h:6754`
// are too, since the custom scheme supplies the product port), so treating them as different
// costs the caller their token for no reason.
//
// The PATH is part of the identity, not noise. A reverse proxy routes `https://gw/prod` and
// `https://gw/dev` to different backends, and Client.post appends the RPC route to the
// endpoint verbatim — so those are two brokers and a token must NOT cross between them.
// Anything unrecognized is likewise kept, so an unfamiliar shape can only ever make two
// endpoints look *more* distinct (withhold the token), never less.
//
// It is derived from parseDSN, so it cannot drift from the endpoint the client really dials.
func EndpointIdentity(dsn string) (string, error) {
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
	id := scheme + "://" + net.JoinHostPort(strings.ToLower(u.Hostname()), port)
	id += strings.TrimRight(u.Path, "/") // "/prod/" == "/prod"; root == ""
	if u.RawQuery != "" {
		id += "?" + u.RawQuery
	}
	return id, nil
}
