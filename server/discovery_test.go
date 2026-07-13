package server_test

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
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
	wire.PathRenewBatch,
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

// Round-3 §3.3: the docs named `/mqlite.v1.Queue/Receive`, a route that does not exist (it is
// `/mqlite.v1.QueueService/Receive`). Rather than fix the three occurrences and hope, pin the
// whole surface: EVERY RPC path written in the docs must be a route the broker actually
// registers. Invent one and this fails.
//
// The comparison is against the live catalog the server publishes on "/", so there is no second
// list to keep in sync — the routes are their own source of truth.
func TestDocsCiteOnlyRealRPCPaths(t *testing.T) {
	card, _ := getCard(t, consoleServer(t, false, nil).URL+"/")
	paths := card.Endpoints
	real := make(map[string]bool, len(paths))
	for _, p := range paths {
		real[p] = true
	}

	docs, err := filepath.Glob(filepath.Join("..", "docs", "*.md"))
	if err != nil {
		t.Fatal(err)
	}
	docs = append(docs, filepath.Join("..", "README.md"))
	// Also cover the source comments, which is where the bad path was copied from.
	more, _ := filepath.Glob(filepath.Join("..", "cmd", "mqlite", "*.go"))
	docs = append(docs, more...)

	re := regexp.MustCompile(`/mqlite\.v1\.[A-Za-z]+/[A-Za-z]+`)
	checked := 0
	for _, f := range docs {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		for _, cited := range re.FindAllString(string(b), -1) {
			checked++
			if !real[cited] {
				t.Errorf("%s cites %q, which the broker does not serve; the registered routes are %v",
					filepath.Base(f), cited, paths)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no RPC paths found in the docs — the regex or the paths moved; this guard is not guarding")
	}
}

// The reverse guard: every route the broker SERVES must be documented in the HTTP reference.
//
// TestDocsCiteOnlyRealRPCPaths stops the docs inventing a route that does not exist. This stops
// the opposite drift — shipping a route nobody can find. RenewBatch was added to the wire, the
// server, the SDK and the CLI, and the api-reference simply skipped it: raw-HTTP users could only
// discover it by reading Go source (codex). A guard in one direction is not a guard.
func TestEveryRPCPathIsDocumented(t *testing.T) {
	card, _ := getCard(t, consoleServer(t, false, nil).URL+"/")

	b, err := os.ReadFile(filepath.Join("..", "docs", "api-reference.md"))
	if err != nil {
		t.Fatalf("read api-reference.md: %v", err)
	}
	doc := string(b)

	// The reference documents operations under `###` headings, and related verbs may share one
	// (`### Complete / Abandon / Reject / Defer / Renew`), so collect every name any heading covers.
	documented := map[string]bool{}
	for _, line := range strings.Split(doc, "\n") {
		if !strings.HasPrefix(line, "### ") {
			continue
		}
		for _, part := range strings.Split(strings.TrimPrefix(line, "### "), "/") {
			documented[strings.TrimSpace(part)] = true
		}
	}

	for _, p := range card.Endpoints {
		name := p[strings.LastIndexByte(p, '/')+1:] // the operation is the last element of the route
		if !documented[name] {
			t.Errorf("the broker serves %s but docs/api-reference.md documents no %q operation — a raw-HTTP user cannot find it",
				p, name)
		}
	}
}
