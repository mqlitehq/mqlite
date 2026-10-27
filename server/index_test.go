package server_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/server"
)

// The open "/" discovery endpoint returns a JSON identity card with NO auth — even
// when Bearer tokens are configured — so hitting the broker root tells you what it is.
func TestIndexDiscovery(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer eng.Close()

	srv := server.New(eng, []string{"secret"}) // auth ON
	srv.Version = "9.9.9"
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/") // no Authorization header
	if err != nil {
		t.Fatalf("get /: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET / status: want 200 (open), got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type: want application/json, got %q", ct)
	}
	var card struct {
		Name, Version, Status, Auth string
	}
	body, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &card); err != nil {
		t.Fatalf("decode: %v (%s)", err, body)
	}
	// auth is a string ("bearer" when tokens are set) so an agent can match it; the
	// exact card shape + full endpoint catalog are pinned in TestDiscoveryCardPinned.
	if card.Name != "mqlite" || card.Status != "ok" || card.Version != "9.9.9" || card.Auth != "bearer" {
		t.Fatalf("unexpected index card: %+v (%s)", card, body)
	}
}

func TestIndexNegotiatesBrowserConsole(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer eng.Close()

	srv := server.New(eng, nil)
	srv.UI = true
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	client := *ts.Client()
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}

	browserAccept := "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
	tests := []struct {
		name         string
		method       string
		path         string
		accept       string
		wantStatus   int
		wantLocation string
		wantJSON     bool
		wantVary     bool
	}{
		{name: "no accept", method: http.MethodGet, path: "/", wantStatus: http.StatusOK, wantJSON: true, wantVary: true},
		{name: "wildcard", method: http.MethodGet, path: "/", accept: "*/*", wantStatus: http.StatusOK, wantJSON: true, wantVary: true},
		{name: "json", method: http.MethodGet, path: "/", accept: "application/json", wantStatus: http.StatusOK, wantJSON: true, wantVary: true},
		{name: "browser", method: http.MethodGet, path: "/", accept: browserAccept, wantStatus: http.StatusTemporaryRedirect, wantLocation: "/ui/", wantVary: true},
		{name: "browser head", method: http.MethodHead, path: "/", accept: browserAccept, wantStatus: http.StatusTemporaryRedirect, wantLocation: "/ui/", wantVary: true},
		{name: "html excluded", method: http.MethodGet, path: "/", accept: "text/html;q=0,*/*;q=1", wantStatus: http.StatusOK, wantJSON: true, wantVary: true},
		{name: "json preferred", method: http.MethodGet, path: "/", accept: "text/html;q=0.5, application/json;q=0.9", wantStatus: http.StatusOK, wantJSON: true, wantVary: true},
		{name: "html preferred", method: http.MethodGet, path: "/", accept: "application/json;q=0.5, text/html", wantStatus: http.StatusTemporaryRedirect, wantLocation: "/ui/", wantVary: true},
		{name: "exact tie favors json", method: http.MethodGet, path: "/", accept: "text/html, application/json", wantStatus: http.StatusOK, wantJSON: true, wantVary: true},
		{name: "text wildcard is not navigation", method: http.MethodGet, path: "/", accept: "text/*", wantStatus: http.StatusOK, wantJSON: true, wantVary: true},
		{name: "invalid quality", method: http.MethodGet, path: "/", accept: "text/html;q=NaN, */*;q=0.8", wantStatus: http.StatusOK, wantJSON: true, wantVary: true},
		{name: "post stays discovery", method: http.MethodPost, path: "/", accept: browserAccept, wantStatus: http.StatusOK, wantJSON: true, wantVary: true},
		{name: "unknown path stays not found", method: http.MethodGet, path: "/unknown", accept: browserAccept, wantStatus: http.StatusNotFound, wantJSON: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(tt.method, ts.URL+tt.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			if tt.accept != "" {
				req.Header.Set("Accept", tt.accept)
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body=%q", resp.StatusCode, tt.wantStatus, body)
			}
			if got := resp.Header.Get("Location"); got != tt.wantLocation {
				t.Errorf("Location = %q, want %q", got, tt.wantLocation)
			}
			if tt.wantJSON {
				if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
					t.Errorf("Content-Type = %q, want application/json", ct)
				}
				if !json.Valid(body) {
					t.Errorf("body is not JSON: %q", body)
				}
			}
			if got := varyContains(resp.Header, "Accept"); got != tt.wantVary {
				t.Errorf("Vary has Accept = %v, want %v; values=%q", got, tt.wantVary, resp.Header.Values("Vary"))
			}
			if tt.method == http.MethodHead && len(body) != 0 {
				t.Errorf("HEAD body = %q, want empty", body)
			}
		})
	}

	headless := server.New(eng, nil)
	headlessTS := httptest.NewServer(headless.Handler())
	defer headlessTS.Close()
	req, _ := http.NewRequest(http.MethodGet, headlessTS.URL+"/", nil)
	req.Header.Set("Accept", browserAccept)
	resp, err := headlessTS.Client().Do(req)
	if err != nil {
		t.Fatalf("headless request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("headless browser request = %d %q, want 200 application/json", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	if varyContains(resp.Header, "Accept") {
		t.Fatalf("headless response unexpectedly varies on Accept: %q", resp.Header.Values("Vary"))
	}
}

func TestIndexBrowserRedirectPreservesCORSVary(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer eng.Close()

	srv := server.New(eng, nil)
	srv.UI = true
	srv.CORS = "https://console.example"
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	client := *ts.Client()
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/", nil)
	req.Header.Set("Origin", "https://console.example")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,*/*;q=0.8")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("status = %d, want 307", resp.StatusCode)
	}
	for _, name := range []string{"Accept", "Origin"} {
		if !varyContains(resp.Header, name) {
			t.Errorf("Vary is missing %s: %q", name, resp.Header.Values("Vary"))
		}
	}
}

func varyContains(header http.Header, name string) bool {
	for _, value := range header.Values("Vary") {
		for _, field := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(field), name) {
				return true
			}
		}
	}
	return false
}

// "/" is the mux catch-all, so an unknown path returns a JSON 404 (not the card),
// reached here with auth off.
func TestIndexUnknownPath404(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer eng.Close()
	ts := httptest.NewServer(server.New(eng, nil).Handler()) // auth off
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/nope")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /nope: want 404, got %d", resp.StatusCode)
	}
}
