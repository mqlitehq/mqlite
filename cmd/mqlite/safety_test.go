package main

// Regression tests for the release-blocking CLI safety fixes (MQLITE-93 / review
// 2026-07-12 §4): the `--` terminator, token/endpoint isolation, non-negative destructive
// limits, and single-RPC batch settlement. The closed-stdout data-loss case (P1-1) is a
// process-level black box in blackbox_test.go.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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

// Round-2 B1: the stdout guard decides whether `receive` may acknowledge, so a FALSE POSITIVE
// is as bad as a false negative — it makes receive refuse to run at all. This pins both
// directions on every platform CI builds (notably Windows, where os.SameFile reports a console
// and a pipe as identical to NUL because their file ids are all zero — a naive port of the
// Unix check breaks every Windows terminal).
func TestStdoutUndeliverable(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "out")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if stdoutUndeliverable(f) {
		t.Error("a regular file must be deliverable (`mqlite receive > out.json`)")
	}

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	if stdoutUndeliverable(w) {
		t.Error("a pipe must be deliverable (`mqlite receive | jq`, and every captured CI run)")
	}

	nul, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer nul.Close()
	if !stdoutUndeliverable(nul) {
		t.Error("the null device must be undeliverable — auto-ack there discards the bodies")
	}
}

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

// ─── credential boundary, destructive bounds & lease safety (review round-2 §3) ───

// must.
func TestSameEndpoint(t *testing.T) {
	// Same broker: identical after the ONLY normalization the dial path itself performs.
	same := [][2]string{
		{"http://127.0.0.1:6754", "http://127.0.0.1:6754/"}, // trailing slash (the reported bug)
		{"http://127.0.0.1:6754", "http://127.0.0.1:6754"},  // identical
		{"mqlite://h", "http://h:6754"},                     // custom scheme supplies the product port
		{"mqlites://h", "https://h:6754"},                   // TLS variant
		{"mqlite://tok@h:6754", "http://h:6754"},            // an embedded credential is not part of the target
		{"https://gw/prod", "https://gw/prod/"},             // only the trailing slash is noise
	}
	for _, p := range same {
		if !sameEndpoint(p[0], p[1]) {
			t.Errorf("sameEndpoint(%q, %q) = false, want true (same broker)", p[0], p[1])
		}
	}
	// Different broker — or, when in doubt, TREATED as different. The identity is the exact
	// base URL the client dials, so anything that changes the bytes we send is a different
	// target and the ambient token is withheld. That is fail-closed on purpose: canonicalizing
	// a URL means deciding which components are "insignificant", and every such decision is a
	// chance to hand one broker another's credential. Worst case here is a warning and an
	// explicit --token; the worst case the other way is a leak.
	diff := [][2]string{
		{"http://h:6754", "http://other:6754"}, // different host
		{"http://h:6754", "http://h:9000"},     // different port
		{"http://h:6754", "https://h:6754"},    // different transport — never send a token over a downgrade
		// A reverse proxy routes these to DIFFERENT backends, and Client.post appends the RPC
		// route to the endpoint verbatim — so the path is part of the broker's identity.
		{"https://gw/prod", "https://gw/dev"},
		{"https://gw/prod", "https://gw"},
		// Each of these was a separate hole while the identity was a hand-canonicalized URL:
		// a decoded path collapsed the first pair, an ignored query/fragment the next two, and
		// a lower-cased IPv6 zone id the last. Comparing the dialed string closes all of them
		// at once, and closes the ones nobody has thought of yet.
		{"https://gw/prod%2Fadmin", "https://gw/prod/admin"},
		{"https://gw/prod", "https://gw/prod?"},
		{"https://gw/prod", "https://gw/prod#x"},
		{"http://[fe80::1%25Eth0]:6754", "http://[fe80::1%25eth0]:6754"},
		{"http://Host:6754", "http://host:6754"}, // host case: conservatively distinct
	}
	for _, p := range diff {
		if sameEndpoint(p[0], p[1]) {
			t.Errorf("sameEndpoint(%q, %q) = true, want false (different target)", p[0], p[1])
		}
	}
}

// §3.3: --all means "delete everything" and a bound means "delete some" — the usage presents
// them as alternatives, so accepting both and quietly honoring one is a trap.
func TestPurgeAllRejectsBounds(t *testing.T) {
	resetGlobals(t)
	t.Setenv("MQLITE_ENDPOINT", "")
	t.Setenv("MQLITE_DB", "file:"+filepath.Join(t.TempDir(), "mq.db"))
	for _, args := range [][]string{
		{"q", "--all", "--max", "10"},
		{"q", "--all", "--older-than", "1h"},
	} {
		if err := cmdPurgeDLQ(ctx0, args); err == nil || !strings.Contains(err.Error(), "cannot be combined") {
			t.Errorf("purge-dlq %v must be rejected as ambiguous, got %v", args, err)
		}
	}
}

// lock duration and require the settle to still succeed.
func TestRenewalSpansSettlement(t *testing.T) {
	resetGlobals(t)
	ctx := context.Background()
	// Background loops ON: the reaper is what reclaims an expired lock.
	eng, err := engine.Open(ctx, engine.Options{DB: "file:" + filepath.Join(t.TempDir(), "mq.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	if err := eng.CreateQueue(ctx, "q", engine.QueueConfig{LockDurationMs: 2000}); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.SendOne(ctx, "q", engine.OutMessage{Body: []byte("slow-link")}); err != nil {
		t.Fatal(err)
	}

	inner := server.New(eng, nil).Handler()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == wire.PathCompleteBatch {
			time.Sleep(3 * time.Second) // > the 2s lock: renewal must carry the lease across it
		}
		inner.ServeHTTP(w, r)
	}))
	defer proxy.Close()

	t.Setenv("MQLITE_ENDPOINT", proxy.URL)
	t.Setenv("MQLITE_TOKEN", "")
	out, err := captureStdout(t, func() error { return cmdReceive(ctx, []string{"q"}) })
	if err != nil {
		t.Errorf("receive over a slow link must still settle: %v", err)
	}
	if !strings.Contains(out, "slow-link") {
		t.Errorf("body was not rendered; stdout=%q", out)
	}
	// Settled for real: the message is gone, not redelivered.
	if m, err := eng.Stats(ctx, "q"); err != nil || m.Total != 0 {
		t.Fatalf("message survived a successful settle (total=%d) — the lease was lost mid-CompleteBatch", m.Total)
	}
}

// already succeeded. Stop must cancel the renewal first: by then it is worthless anyway.
func TestRenewerStopDoesNotHangOnStalledRenew(t *testing.T) {
	resetGlobals(t)
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: "file:" + filepath.Join(t.TempDir(), "mq.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	// A 2s lock so the renewer's ticker (half the lease, min 1s) fires while output is blocked.
	if err := eng.CreateQueue(ctx, "q", engine.QueueConfig{LockDurationMs: 2000}); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.SendOne(ctx, "q", engine.OutMessage{Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}

	wedge := make(chan struct{}) // never closed: a Renew that reaches the broker hangs forever
	renewing := make(chan struct{}, 1)
	inner := server.New(eng, nil).Handler()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case wire.PathRenew:
			select {
			case renewing <- struct{}{}: // record that a Renew really is in flight
			default:
			}
			select {
			case <-wedge:
			case <-r.Context().Done(): // the client cancelling is the ONLY thing that frees us
			}
			return
		case wire.PathCompleteBatch:
			// Settle slowly, so the renewer's ticker (1s) fires and wedges a Renew WHILE the
			// command is still running. Without that, Stop() would find nothing in flight and
			// the test would prove nothing.
			time.Sleep(2500 * time.Millisecond)
		}
		inner.ServeHTTP(w, r)
	}))
	defer proxy.Close()
	defer close(wedge) // LIFO: frees the wedged handler so proxy.Close() can drain

	t.Setenv("MQLITE_ENDPOINT", proxy.URL)
	t.Setenv("MQLITE_TOKEN", "")

	done := make(chan error, 1)
	go func() {
		_, err := captureStdout(t, func() error { return cmdReceive(ctx, []string{"q", "--max", "1"}) })
		done <- err
	}()
	select {
	case <-done: // settled (or errored) — either way it RETURNED, which is the point
	case <-time.After(30 * time.Second):
		t.Fatal("receive hung: Stop() waited on a stalled Renew instead of cancelling it")
	}
	select {
	case <-renewing:
	default:
		t.Fatal("no Renew was ever in flight — the test did not exercise the stalled-renew path")
	}
}
