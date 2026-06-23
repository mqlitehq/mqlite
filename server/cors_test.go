package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/server"
	"github.com/mqlitehq/mqlite/wire"
)

func corsServer(t *testing.T, origin string) *httptest.Server {
	t.Helper()
	eng, err := engine.Open(context.Background(), engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { eng.Close() })
	s := server.New(eng, []string{"secret"})
	s.CORS = origin
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return ts
}

func TestCORSPreflightBypassesAuth(t *testing.T) {
	ts := corsServer(t, "*")
	// A preflight carries no Authorization; it must be answered, not 401'd.
	req, _ := http.NewRequest(http.MethodOptions, ts.URL+wire.PathSend, nil)
	req.Header.Set("Origin", "https://app.example")
	req.Header.Set("Access-Control-Request-Method", "POST")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		t.Fatalf("preflight status = %d, want 204", res.StatusCode)
	}
	if got := res.Header.Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("Allow-Origin = %q, want *", got)
	}
	if got := res.Header.Get("Access-Control-Allow-Headers"); got == "" {
		t.Errorf("Allow-Headers missing")
	}
}

func TestCORSHeaderOnActualResponse(t *testing.T) {
	ts := corsServer(t, "*")
	// An unauthenticated real request is still 401, but must carry the CORS header so the
	// browser lets the page read that 401 (rather than a masked network error).
	res, err := http.Post(ts.URL+wire.PathListQueues, "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", res.StatusCode)
	}
	if got := res.Header.Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("Allow-Origin = %q, want *", got)
	}
}

func TestCORSSpecificOriginSetsVary(t *testing.T) {
	ts := corsServer(t, "https://app.example")
	res, err := http.Post(ts.URL+wire.PathListQueues, "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if got := res.Header.Get("Access-Control-Allow-Origin"); got != "https://app.example" {
		t.Errorf("Allow-Origin = %q", got)
	}
	if got := res.Header.Get("Vary"); got != "Origin" {
		t.Errorf("Vary = %q, want Origin", got)
	}
}

func TestCORSDisabledByDefault(t *testing.T) {
	ts := corsServer(t, "") // library default: off
	res, err := http.Post(ts.URL+wire.PathListQueues, "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if got := res.Header.Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("Allow-Origin = %q, want empty (CORS off)", got)
	}
}
