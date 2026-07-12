package server_test

import (
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"sort"
	"testing"

	"github.com/mqlitehq/mqlite/wire"
)

// wantRPCRoutes is the complete RPC catalog the open "/" discovery card must enumerate,
// in server-registration order. It IS the pinned contract (MQLITE-87): the card's
// endpoints come straight from what server.routes() registered, so adding or removing a
// route changes this list and fails the test until it's updated deliberately — an agent
// iterating endpoints[] can trust it lists every route, with none stale.
var wantRPCRoutes = []string{
	wire.PathSend, wire.PathReceive, wire.PathComplete, wire.PathCompleteBatch,
	wire.PathAbandon, wire.PathReject, wire.PathDefer, wire.PathReceiveDeferred,
	wire.PathRenew, wire.PathSchedule, wire.PathCancel, wire.PathPeek, wire.PathStats,
	wire.PathCreateQueue, wire.PathSubscribe, wire.PathListQueues, wire.PathListSubscriptions,
	wire.PathTestFilter, wire.PathRedrive, wire.PathPurge, wire.PathStatus,
}

func getCard(t *testing.T, url string) (wire.DiscoveryCard, []string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	// Key set of the raw object — catches any field added/removed from the shape.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatalf("decode raw %q: %v", body, err)
	}
	keys := make([]string, 0, len(raw))
	for k := range raw {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var card wire.DiscoveryCard
	if err := json.Unmarshal(body, &card); err != nil {
		t.Fatalf("decode card: %v", err)
	}
	return card, keys
}

// TestDiscoveryCardPinned pins the exact "/" JSON contract agents rely on: a string
// auth ("bearer"/"none"), a complete endpoints[] catalog, and a fixed field set — so
// the server can't silently drift from the documented shape (MQLITE-87 / D4).
func TestDiscoveryCardPinned(t *testing.T) {
	// Auth on, console off.
	ts := consoleServer(t, false, []string{"secret"})
	card, keys := getCard(t, ts.URL+"/")

	wantKeys := []string{"auth", "description", "docs", "endpoints", "health", "metrics", "name", "status", "version"}
	if !reflect.DeepEqual(keys, wantKeys) {
		t.Errorf("card field set = %v\n            want %v", keys, wantKeys)
	}
	if card.Name != "mqlite" || card.Status != "ok" || card.Health != "/healthz" || card.Metrics != "/metrics" {
		t.Errorf("card metadata drift: %+v", card)
	}
	if card.Auth != "bearer" {
		t.Errorf("auth (tokens set) = %q, want \"bearer\"", card.Auth)
	}
	if card.UI != "" {
		t.Errorf("ui = %q, want empty when console is off", card.UI)
	}
	if !reflect.DeepEqual(card.Endpoints, wantRPCRoutes) {
		t.Errorf("endpoints catalog drift:\n got  %v\n want %v", card.Endpoints, wantRPCRoutes)
	}

	// Auth off + console on: auth downgrades to "none" and ui appears.
	ts2 := consoleServer(t, true, nil)
	card2, keys2 := getCard(t, ts2.URL+"/")
	if card2.Auth != "none" {
		t.Errorf("auth (no tokens) = %q, want \"none\"", card2.Auth)
	}
	if card2.UI != "/ui" {
		t.Errorf("ui (console on) = %q, want \"/ui\"", card2.UI)
	}
	if !contains(keys2, "ui") {
		t.Errorf("field set with console on missing \"ui\": %v", keys2)
	}
}

func contains(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}
