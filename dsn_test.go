package mqlite

import "testing"

// TestParseDSN pins the connection-string port contract (MQLITE-84): custom mqlite/mqlites
// schemes supply the product default port 6754 when it is omitted, an explicit port always
// wins, and plain http/https keep standard Web semantics (80/443, passthrough). Before this
// the custom schemes fell through to :80/:443 — a real footgun since a broker's port is not 80.
func TestParseDSN(t *testing.T) {
	cases := []struct {
		name, dsn, endpoint, token string
		wantErr                    bool
	}{
		{name: "mqlite supplies product port", dsn: "mqlite://127.0.0.1", endpoint: "http://127.0.0.1:6754"},
		{name: "mqlites supplies port + https", dsn: "mqlites://host", endpoint: "https://host:6754"},
		{name: "explicit port wins", dsn: "mqlite://host:9000", endpoint: "http://host:9000"},
		{name: "explicit port wins on mqlites", dsn: "mqlites://host:8443", endpoint: "https://host:8443"},
		{name: "token extracted + product port", dsn: "mqlite://tok@host", endpoint: "http://host:6754", token: "tok"},
		{name: "token + explicit port both kept", dsn: "mqlite://tok@host:9000", endpoint: "http://host:9000", token: "tok"},
		{name: "plain http passthrough does not extract userinfo as token", dsn: "http://tok@host", endpoint: "http://tok@host", token: ""},
		{name: "tls=true query upgrades scheme", dsn: "mqlite://host?tls=true", endpoint: "https://host:6754"},
		{name: "ipv6 no port", dsn: "mqlite://[::1]", endpoint: "http://[::1]:6754"},
		{name: "ipv6 with port", dsn: "mqlite://[::1]:9000", endpoint: "http://[::1]:9000"},
		{name: "plain http keeps 80 (passthrough)", dsn: "http://host", endpoint: "http://host"},
		{name: "plain https keeps 443, trims slash", dsn: "https://host:8443/", endpoint: "https://host:8443"},
		{name: "empty", dsn: "", wantErr: true},
		{name: "unsupported scheme", dsn: "ftp://host", wantErr: true},
		{name: "missing host", dsn: "mqlite://", wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ep, tok, err := parseDSN(c.dsn)
			if (err != nil) != c.wantErr {
				t.Fatalf("parseDSN(%q) err = %v, wantErr %v", c.dsn, err, c.wantErr)
			}
			if c.wantErr {
				return
			}
			if ep != c.endpoint {
				t.Errorf("endpoint = %q, want %q", ep, c.endpoint)
			}
			if tok != c.token {
				t.Errorf("token = %q, want %q", tok, c.token)
			}
		})
	}
}
