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
