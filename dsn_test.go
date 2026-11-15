package mqlite

import (
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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

func TestParseDSNErrorKeepsCauseWithoutCredentials(t *testing.T) {
	const token = "synthetic-private-marker"
	for _, scheme := range []string{"mqlite", "mqlites"} {
		t.Run(scheme, func(t *testing.T) {
			_, _, err := parseDSN(scheme + "://" + token + "@test.invalid/%zz")
			if err == nil || strings.Contains(err.Error(), token) {
				t.Fatal("malformed connection string did not return a credential-free error")
			}
			var parseError *url.Error
			var escapeError url.EscapeError
			if !errors.As(err, &parseError) || parseError.URL != "" || parseError.Op != "parse" || !errors.As(err, &escapeError) {
				t.Fatal("connection string error lost its safe parse cause")
			}
		})
	}
}

// TestNoStrayLegacyPort guards the 6754 migration (MQLITE-84): the previous default broker
// port must not reappear as a casual default anywhere in the tracked source or docs. The
// only places allowed to name it are deliberate upgrade/compat documents below.
// Any new occurrence fails loudly so the old port can't quietly creep back as a default.
//
// The needle is assembled at runtime so this guard never matches itself.
func TestNoStrayLegacyPort(t *testing.T) {
	needle := "80" + "80"

	allow := map[string]bool{
		filepath.FromSlash("CHANGELOG.md"): true, // documents the changed default + compat mapping
		// The deploy guide must say which port the image you can actually PULL listens on:
		// until v0.3.0 is tagged that is 0.2.x, and it is still the old port. Telling people to
		// publish 6754 for an image that binds the legacy port is how you get a broker that
		// silently answers nothing (review round-3 §3.1). This is migration documentation, the
		// case this guard explicitly allows for — not a default creeping back.
		filepath.FromSlash("docs/deployment.md"): true,
		// The operations runbook explicitly rehearses the old-to-new port/schema cutover.
		filepath.FromSlash("docs/operations.md"): true,
	}
	skipDir := map[string]bool{
		".git": true, "node_modules": true, "bin": true, "testdata": true,
		"out": true, "web": true, // server/web is a vendored, minified console dist
	}
	textExt := map[string]bool{
		".go": true, ".md": true, ".yml": true, ".yaml": true, ".toml": true,
		".sh": true, ".py": true, ".txt": true, ".example": true, ".mjs": true,
	}

	err := filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if skipDir[info.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		rel := strings.TrimPrefix(filepath.ToSlash(path), "./")
		if allow[filepath.FromSlash(rel)] {
			return nil
		}
		if info.Name() != "Dockerfile" && !textExt[filepath.Ext(info.Name())] {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for i, ln := range strings.Split(string(b), "\n") {
			if strings.Contains(ln, needle) {
				t.Errorf("%s:%d uses the legacy default port (switch to 6754, or allowlist in "+
					"dsn_test.go if it is deliberate migration docs): %s", rel, i+1, strings.TrimSpace(ln))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
}
