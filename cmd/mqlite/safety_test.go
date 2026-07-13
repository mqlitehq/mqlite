package main

// Regression tests for the release-blocking CLI safety fixes (MQLITE-93 / review
// 2026-07-12 §4): the `--` terminator, token/endpoint isolation, non-negative destructive
// limits, and single-RPC batch settlement. The closed-stdout data-loss case (P1-1) is a
// process-level black box in blackbox_test.go.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mqlitehq/mqlite"
	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/server"
	"github.com/mqlitehq/mqlite/wire"
)

// resetGlobals restores the package-level flag state between tests.
func resetGlobals(t *testing.T) {
	t.Cleanup(func() {
		gEndpoint, gToken, gOutput = "", "", "text"
		gEndpointSet, gTokenSet = false, false
	})
}

// P1-4: after a `--` terminator every remaining token is a literal positional, so a message
// body keeps its flag-looking words and --output is not hijacked.
func TestParseInterspersedDashDash(t *testing.T) {
	resetGlobals(t)
	fs := newFlags("send")
	pos, err := parseInterspersed(fs, []string{"q", "--", "hello", "--output", "json"})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(pos, "|"); got != "q|hello|--output|json" {
		t.Errorf("positionals = %q, want q|hello|--output|json", got)
	}
	if gOutput != "text" {
		t.Errorf("--output after -- must NOT be parsed; gOutput=%q", gOutput)
	}
	// And readBody joins pos[1:] into the literal body.
	if body, _ := readBody("", pos[1:]); string(body) != "hello --output json" {
		t.Errorf("body = %q, want %q", body, "hello --output json")
	}
}

// P1-5: explicit --token wins (empty means "no token"); an ambient MQLITE_TOKEN is reused
// only for the environment's own endpoint, never forwarded to a --endpoint that changed
// the target host.
func TestResolveToken(t *testing.T) {
	t.Setenv("MQLITE_ENDPOINT", "http://env-host")
	t.Setenv("MQLITE_TOKEN", "env-secret")

	cases := []struct {
		name      string
		tokenSet  bool
		token     string
		epSet     bool
		ep        string
		wantToken string
	}{
		{"no flags -> env token", false, "", false, "http://env-host", "env-secret"},
		{"explicit --token wins", true, "explicit", false, "http://env-host", "explicit"},
		{"explicit empty --token= clears it", true, "", false, "http://env-host", ""},
		{"same endpoint reuses env token", false, "", true, "http://env-host", "env-secret"},
		{"changed endpoint withholds env token", false, "", true, "http://other-host", ""},
	}
	for _, c := range cases {
		resetGlobals(t)
		gTokenSet, gToken = c.tokenSet, c.token
		gEndpointSet, gEndpoint = c.epSet, c.ep
		if got := resolveToken(c.ep, "http://env-host"); got != c.wantToken {
			t.Errorf("%s: resolveToken = %q, want %q", c.name, got, c.wantToken)
		}
	}
}

// P1-5 (codex follow-up): --token= must also clear a credential embedded in the endpoint
// DSN, which WithToken("") would otherwise keep.
func TestStripEndpointCredentials(t *testing.T) {
	if got := stripEndpointCredentials("mqlite://secret@host:6754"); got != "mqlite://host:6754" {
		t.Errorf("strip = %q, want mqlite://host:6754", got)
	}
	if got := stripEndpointCredentials("http://host:6754"); got != "http://host:6754" {
		t.Errorf("no-userinfo endpoint must be unchanged, got %q", got)
	}
}

// P1-2 (codex follow-up): a sub-millisecond negative --older-than truncates to 0 in the
// SDK conversion, so it must be rejected at the CLI before it can bypass the --all guard.
func TestCLIRejectsNegativeDuration(t *testing.T) {
	resetGlobals(t)
	t.Setenv("MQLITE_ENDPOINT", "")
	t.Setenv("MQLITE_DB", "file:"+filepath.Join(t.TempDir(), "mq.db"))
	for _, bad := range []string{"-1ns", "1ns", "999999ns"} { // negative + positive sub-ms
		if err := cmdPurgeDLQ(ctx0, []string{"q", "--older-than", bad}); err == nil {
			t.Errorf("purge-dlq --older-than=%s must be rejected (truncates to unbounded)", bad)
		}
		if err := cmdRedrive(ctx0, []string{"q", "--older-than", bad}); err == nil {
			t.Errorf("redrive --older-than=%s must be rejected", bad)
		}
	}
	// A >= 1ms bound is accepted (dials embedded; fails later for a missing queue, not on the bound).
	if err := cmdPurgeDLQ(ctx0, []string{"q", "--older-than", "1ms"}); err != nil && strings.Contains(err.Error(), "older-than") {
		t.Errorf("purge-dlq --older-than=1ms must be accepted, got %v", err)
	}
}

var ctx0 = context.Background()

// P1-2: negative destructive limits are rejected at the engine boundary (so CLI, SDK, and
// raw HTTP are all covered) and delete/move nothing.
func TestNegativeDestructiveLimits(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	if err := eng.CreateQueue(ctx, "q", engine.QueueConfig{MaxDeliveryCount: 1}); err != nil {
		t.Fatal(err)
	}
	// Dead-letter one message.
	if _, err := eng.SendOne(ctx, "q", engine.OutMessage{Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	ms, _ := eng.Receive(ctx, "q", engine.ReceiveOptions{MaxMessages: 1})
	if len(ms) != 1 {
		t.Fatalf("receive n=%d", len(ms))
	}
	_ = eng.Reject(ctx, "q", ms[0].SeqNumber, ms[0].LockToken, engine.ReasonAppRequested, "")

	for _, opts := range []engine.RedriveOptions{
		{Max: -1}, {OlderThanMs: -1}, {RatePerSec: -1},
	} {
		if _, err := eng.Purge(ctx, "q", opts); err == nil {
			t.Errorf("Purge(%+v) must be rejected", opts)
		}
		if _, err := eng.Redrive(ctx, "q", opts); err == nil {
			t.Errorf("Redrive(%+v) must be rejected", opts)
		}
	}
	// Nothing was purged/redriven.
	if m, _ := eng.Stats(ctx, "q"); m.DeadLettered != 1 {
		t.Errorf("dead_lettered = %d after rejected purges, want 1 (no mutation)", m.DeadLettered)
	}
}

// P1-3: an auto-ack receive of a batch settles with exactly ONE CompleteBatch RPC, never N
// individual Complete calls.
func TestReceiveUsesCompleteBatch(t *testing.T) {
	resetGlobals(t)
	ctx := context.Background()
	eng, err := mqlite.OpenEmbedded(ctx, "file:"+filepath.Join(t.TempDir(), "mq.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	if err := eng.CreateQueue(ctx, "q", mqlite.QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := eng.SendOne(ctx, "q", mqlite.OutMessage{Body: []byte("m")}); err != nil {
			t.Fatal(err)
		}
	}

	var completes, batches atomic.Int64
	inner := server.New(eng.Engine(), nil).Handler()
	counting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case wire.PathComplete:
			completes.Add(1)
		case wire.PathCompleteBatch:
			batches.Add(1)
		}
		inner.ServeHTTP(w, r)
	}))
	defer counting.Close()

	t.Setenv("MQLITE_ENDPOINT", counting.URL)
	t.Setenv("MQLITE_TOKEN", "")
	if err := cmdReceive(ctx, []string{"q", "--max", "5"}); err != nil {
		t.Fatalf("receive: %v", err)
	}
	if b, c := batches.Load(), completes.Load(); b != 1 || c != 0 {
		t.Errorf("settlement RPCs: CompleteBatch=%d Complete=%d, want 1/0", b, c)
	}
}
