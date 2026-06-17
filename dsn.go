package mqlite

import (
	"fmt"
	"net/url"
	"strings"
)

// parseDSN turns an mqlite connection string into (endpoint, token).
//
//	mqlite://<token>@host:port?tls=true   -> http(s)://host:port  + token
//	mqlites://<token>@host:port           -> https://host:port    + token
//	http://host:port / https://host:port  -> used as-is (token via WithToken/env)
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
	host := u.Host
	if host == "" {
		return "", "", fmt.Errorf("mqlite: missing host in connection string")
	}
	return scheme + "://" + host, token, nil
}
