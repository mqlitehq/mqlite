package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/server"
	"github.com/mqlitehq/mqlite/wire"
)

func consoleServer(t *testing.T, ui bool, tokens []string) *httptest.Server {
	t.Helper()
	eng, err := engine.Open(context.Background(), engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { eng.Close() })
	s := server.New(eng, tokens)
	s.UI = ui
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return ts
}

// With UI on the embedded SPA is served at /ui/, and it's auth-exempt (the static page
// loads without a token; its API calls carry one) — even when the broker requires tokens.
func TestConsoleServedAndAuthExempt(t *testing.T) {
	ts := consoleServer(t, true, []string{"secret"})

	res, err := ts.Client().Get(ts.URL + "/ui/")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK || !strings.HasPrefix(res.Header.Get("Content-Type"), "text/html") {
		t.Fatalf("/ui/ = %d %q, want 200 html (no token)", res.StatusCode, res.Header.Get("Content-Type"))
	}

	// an actual RPC without a token is still 401 — the exemption is only the static UI.
	r2, err := http.Post(ts.URL+wire.PathListQueues, "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	r2.Body.Close()
	if r2.StatusCode != http.StatusUnauthorized {
		t.Fatalf("RPC without token = %d, want 401", r2.StatusCode)
	}
}

// With UI off the broker runs headless: /ui 404s.
func TestConsole404WhenOff(t *testing.T) {
	ts := consoleServer(t, false, nil)
	res, err := ts.Client().Get(ts.URL + "/ui/")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("/ui/ with UI off = %d, want 404", res.StatusCode)
	}
}

// The /ui auth exemption matches exactly "/ui" and "/ui/..." — a loose prefix
// would also exempt /uixyz from Bearer auth (review F11 / MQLITE-64).
func TestUIAuthExemptionIsExact(t *testing.T) {
	ts := consoleServer(t, true, []string{"secret"})

	res, err := http.Post(ts.URL+"/uixyz", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("/uixyz without a token = %d, want 401 (must not ride the /ui exemption)", res.StatusCode)
	}
}
