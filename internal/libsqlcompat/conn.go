// Package libsqlcompat contains the pinned HTTP v2 driver with bounded detached
// stream cleanup. See NOTICE and UPSTREAM.json for its source and local changes.
package libsqlcompat

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"

	"github.com/mqlitehq/mqlite/internal/libsqlcompat/hrana"
	httpv2 "github.com/mqlitehq/mqlite/internal/libsqlcompat/httpv2"
)

// IsInvalidAcknowledgement distinguishes a received malformed success from a
// definite rejection. An issued write may already have committed in this case.
func IsInvalidAcknowledgement(err error) bool {
	var invalid *hrana.InvalidAcknowledgementError
	return errors.As(err, &invalid)
}

// IsIncompleteAcknowledgement preserves ordinary read retries when a known
// transport error interrupted the body. It never authorizes replay of a write.
func IsIncompleteAcknowledgement(err error) bool {
	var invalid *hrana.InvalidAcknowledgementError
	return errors.As(err, &invalid) && invalid.BodyRead
}

// OpenHTTP preserves the legacy libsql Driver.Open HTTP DSN contract. The caller
// selects this entry point only for libsql://, http:// and https:// transports;
// the upstream driver continues to own the separate WebSocket and file paths.
func OpenHTTP(dbURL string) (driver.Conn, error) {
	return NewOpener().OpenHTTP(dbURL)
}

// HTTPStatus preserves an actual provider response status for the engine's
// stage-aware retry policy. It does not imply that a write was rejected.
func HTTPStatus(err error) int { return httpv2.HTTPStatus(err) }

// Opener shares a bounded detached-cleanup manager across one database's
// connections. Use a separate opener for each database/sql pool.
type Opener struct {
	cleanup *httpv2.CleanupManager
}

func NewOpener() *Opener {
	return &Opener{cleanup: httpv2.NewCleanupManager()}
}

func (o *Opener) CleanupStats() httpv2.CleanupStats { return o.cleanup.Stats() }

func (o *Opener) OpenHTTP(dbURL string) (driver.Conn, error) {
	u, err := url.Parse(dbURL)
	if err != nil {
		return nil, err
	}
	query := u.Query()
	jwt, err := extractJwt(&query)
	if err != nil {
		return nil, err
	}
	tls, err := extractTls(&query, u.Scheme)
	if err != nil {
		return nil, err
	}
	for name := range query {
		return nil, fmt.Errorf("unknown query parameter %#v", name)
	}
	u.RawQuery = ""
	if u.Scheme == "libsql" {
		if tls {
			u.Scheme = "https"
		} else {
			if u.Port() == "" {
				return nil, fmt.Errorf("libsql:// URL with ?tls=0 must specify an explicit port")
			}
			u.Scheme = "http"
		}
	}
	if (u.Scheme == "wss" || u.Scheme == "https") && !tls {
		return nil, fmt.Errorf("%s:// URL cannot opt out of TLS using ?tls=0", u.Scheme)
	}
	if (u.Scheme == "ws" || u.Scheme == "http") && tls {
		return nil, fmt.Errorf("%s:// URL cannot opt in to TLS using ?tls=1", u.Scheme)
	}
	if u.Scheme == "https" || u.Scheme == "http" {
		return httpv2.Connect(u.String(), jwt, u.Host, false, "", o.cleanup), nil
	}
	return nil, fmt.Errorf("unsupported URL scheme: %s\nThis driver supports only URLs that start with libsql://, file://, https://, http://, wss:// and ws://", u.Scheme)
}

// These parsers retain the pinned upstream's alias, empty-value and TLS rules.
func extractJwt(query *url.Values) (string, error) {
	authTokenSnake := query.Get("auth_token")
	authTokenCamel := query.Get("authToken")
	jwt := query.Get("jwt")
	query.Del("auth_token")
	query.Del("authToken")
	query.Del("jwt")

	countNonEmpty := func(slice ...string) int {
		count := 0
		for _, s := range slice {
			if s != "" {
				count++
			}
		}
		return count
	}

	if countNonEmpty(authTokenSnake, authTokenCamel, jwt) > 1 {
		return "", fmt.Errorf("please use at most one of the following query parameters: 'auth_token', 'authToken', 'jwt'")
	}

	if authTokenSnake != "" {
		return authTokenSnake, nil
	} else if authTokenCamel != "" {
		return authTokenCamel, nil
	} else {
		return jwt, nil
	}
}

func extractTls(query *url.Values, scheme string) (bool, error) {
	tls := query.Get("tls")
	query.Del("tls")
	switch tls {
	case "":
		if scheme == "http" || scheme == "ws" {
			return false, nil
		} else {
			return true, nil
		}
	case "0":
		return false, nil

	case "1":

		return true, nil
	default:
		return true, fmt.Errorf("unknown value of tls query parameter. Valid values are 0 and 1")
	}
}
