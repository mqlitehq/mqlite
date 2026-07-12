package server_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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
