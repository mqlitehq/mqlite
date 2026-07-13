package main

// Regression tests for the release-blocking CLI safety fixes (MQLITE-93 / review
// 2026-07-12 §4): the `--` terminator, token/endpoint isolation, non-negative destructive
// limits, and single-RPC batch settlement. The closed-stdout data-loss case (P1-1) is a
// process-level black box in blackbox_test.go.

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
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
		case wire.PathRenew, wire.PathRenewBatch:
			select {
			case renewing <- struct{}{}: // record that a renewal really is in flight
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

// Round-3 P1: the supported-batch delayed-link requirement. Renewing message-by-message cost N
// round trips, so on a slow link the renewal pass took LONGER than the lease it was saving —
// 64 messages × 50ms = 3.2s against a 2s lock — and the reviewer watched 44 of 64 locks expire
// mid-pass and redeliver. One RenewBatch per tick makes the pass one round trip regardless of
// batch size.
//
// Reproduces the reviewer's setup exactly: short lease, per-Renew latency, a slow settle, and a
// full-size batch. Asserts what they asked for — every message printed, exit 0, ONE batch
// settlement, and an empty queue.
func TestBatchRenewalUnderDelayedLink(t *testing.T) {
	const (
		lockMs      = 6000            // comfortably longer than a slow runner's receive
		settleDelay = 9 * time.Second // > the lease: the batch survives only if renewal works
	)
	for _, n := range []int{64, 256} { // 256 is the engine's supported maximum receive
		t.Run(fmt.Sprintf("%d messages", n), func(t *testing.T) {
			resetGlobals(t)
			ctx := context.Background()
			// Background loops ON: the reaper is what reclaims an expired lock. A file DB with a
			// ONE-transaction seed keeps the fixture cheap — 256 individually-committed sends is
			// 256 fsyncs, which timed this test out on a slow Windows runner and told us nothing
			// about renewal. (Not :memory:, which a cancelled query can destroy — see the note in
			// the handover: that is a separate, pre-existing engine bug.)
			eng, err := engine.Open(ctx, engine.Options{DB: "file:" + filepath.Join(t.TempDir(), "mq.db")})
			if err != nil {
				t.Fatal(err)
			}
			defer eng.Close()
			// The lease must outlast the RECEIVE itself — renewal cannot start before the batch is
			// claimed, so a lock that expires while the messages are still being fetched is lost to
			// physics, not to a bug. A slow CI runner needed several seconds just to deliver 256
			// messages, which is what a 2s lock turned into "all 256 failed to settle". What the
			// test actually needs is settleDelay > lockDuration, so that renewal — and only
			// renewal — can carry the batch across the settle.
			if err := eng.CreateQueue(ctx, "q", engine.QueueConfig{LockDurationMs: lockMs}); err != nil {
				t.Fatal(err)
			}
			seed := make([]engine.OutMessage, n)
			for i := range seed {
				seed[i] = engine.OutMessage{Body: []byte("m")}
			}
			if _, err := eng.Send(ctx, "q", seed...); err != nil {
				t.Fatal(err)
			}

			var renews, settles atomic.Int64
			inner := server.New(eng, nil).Handler()
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case wire.PathRenew, wire.PathRenewBatch:
					renews.Add(1)
					time.Sleep(50 * time.Millisecond) // per-renew link latency
				case wire.PathCompleteBatch:
					settles.Add(1)
					time.Sleep(settleDelay) // deliberately longer than the lease: only renewal saves it
				}
				inner.ServeHTTP(w, r)
			}))
			defer proxy.Close()

			t.Setenv("MQLITE_ENDPOINT", proxy.URL)
			t.Setenv("MQLITE_TOKEN", "")
			out, err := captureStdout(t, func() error {
				return cmdReceive(ctx, []string{"q", "--max", strconv.Itoa(n)})
			})
			if err != nil {
				t.Fatalf("receive of %d messages over a delayed link must settle cleanly: %v", n, err)
			}
			if got := strings.Count(out, "seq="); got != n {
				t.Errorf("printed %d message lines, want %d", got, n)
			}
			if s := settles.Load(); s != 1 {
				t.Errorf("settlement requests = %d, want exactly 1 CompleteBatch", s)
			}
			// The whole point: the batch is settled, not redelivered.
			m, serr := eng.Stats(ctx, "q")
			if serr != nil {
				t.Fatalf("stats: %v", serr)
			}
			if m.Total != 0 {
				t.Fatalf("total=%d active=%d after settle, want 0 — leases were lost mid-renewal",
					m.Total, m.Active)
			}
			// And a renewal pass is ONE request, so it cannot outgrow the lease: with per-message
			// renewal this batch needed n round trips per pass.
			t.Logf("%d messages: %d renewal request(s), %d settlement request(s)", n, renews.Load(), settles.Load())
		})
	}
}

// A NEW CLI must keep working against an OLD broker. Released v0.2.x has no RenewBatch route and
// answers 404; a renewer that discarded that error would silently stop renewing against a broker
// the previous CLI renewed just fine — the locks would expire and the batch redeliver. So the
// first "I don't know that operation" must downgrade to per-message Renew, which that broker does
// serve (codex).
func TestRenewalFallsBackOnOldBroker(t *testing.T) {
	resetGlobals(t)
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: "file:" + filepath.Join(t.TempDir(), "mq.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	if err := eng.CreateQueue(ctx, "q", engine.QueueConfig{LockDurationMs: 6000}); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.SendOne(ctx, "q", engine.OutMessage{Body: []byte("m")}); err != nil {
		t.Fatal(err)
	}

	var batchTries, singleRenews atomic.Int64
	inner := server.New(eng, nil).Handler()
	// A broker that predates RenewBatch. It must answer the way the RELEASED broker really does:
	// its own catch-all, with the structured `{"code":"not_found","message":"no such path: ..."}`
	// body — NOT a bare http.NotFound. Modelling it with a hand-rolled 404 is how the first
	// version of this fallback came to be tested against a server that does not exist, and would
	// have shipped a downgrade path that never fires (codex).
	//
	// So: strip the route by rewriting the request to an unknown path and letting the REAL
	// handler produce the REAL response.
	old := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case wire.PathRenewBatch:
			batchTries.Add(1)
			r.URL.Path = "/mqlite.v1.QueueService/RenewBatch" // unrouted on this broker
			r2 := r.Clone(r.Context())
			r2.URL.Path = "/no-such-route-on-an-old-broker"
			inner.ServeHTTP(w, r2)
			return
		case wire.PathRenew:
			singleRenews.Add(1)
		case wire.PathCompleteBatch:
			time.Sleep(3 * time.Second) // slow settle: renewal has to carry the lease across it
		}
		inner.ServeHTTP(w, r)
	}))
	defer old.Close()

	t.Setenv("MQLITE_ENDPOINT", old.URL)
	t.Setenv("MQLITE_TOKEN", "")
	if _, err := captureStdout(t, func() error { return cmdReceive(ctx, []string{"q"}) }); err != nil {
		t.Fatalf("receive against a pre-RenewBatch broker must still settle: %v", err)
	}
	if batchTries.Load() == 0 {
		t.Error("the CLI should try RenewBatch first")
	}
	if singleRenews.Load() == 0 {
		t.Error("after the 404 it must fall back to per-message Renew, not give up renewing")
	}
	if m, err := eng.Stats(ctx, "q"); err != nil || m.Total != 0 {
		t.Fatalf("total=%d — the batch was not settled against the old broker", m.Total)
	}
}

// A lease that is nearly spent when the batch arrives — a very short lock duration, or a slow
// Receive — must be renewed IMMEDIATELY. Clamping the first tick up to the cadence floor
// schedules it at or after locked_until, so the reaper takes the batch before the CLI ever asks
// (codex).
func TestRenewIntervalSaysRenewNowWhenLeaseNearlySpent(t *testing.T) {
	now := time.Now()
	nearlyGone := []*mqlite.Message{{LockedUntil: now.Add(30 * time.Millisecond)}}
	if d := renewInterval(nearlyGone); d >= minRenewInterval {
		t.Errorf("interval = %s for a 30ms lease; must be below the cadence floor so the caller renews NOW", d)
	}
	spent := []*mqlite.Message{{LockedUntil: now.Add(-time.Second)}} // already expired
	if d := renewInterval(spent); d != 0 {
		t.Errorf("interval = %s for a spent lease, want 0 (renew immediately)", d)
	}
	healthy := []*mqlite.Message{{LockedUntil: now.Add(30 * time.Second)}}
	if d := renewInterval(healthy); d < 9*time.Second || d > 11*time.Second {
		t.Errorf("interval = %s for a 30s lease, want about a third of it", d)
	}
}

// A renewal must TELL the caller the deadline it committed — and must not write it onto the
// message. The command renders those messages concurrently, so a background write to a shared
// time.Time is a data race; and without the reported deadline the renewer has nothing to compute
// a cadence from, so a lease that merely arrived nearly spent (a slow Receive) would pin the
// cadence to the floor and renew twenty times a second for the rest of the batch's life (codex).
func TestRenewalReportsTheDeadlineWithoutTouchingTheMessage(t *testing.T) {
	resetGlobals(t)
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{
		DB: "file:" + filepath.Join(t.TempDir(), "mq.db"), DisableBackground: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	if err := eng.CreateQueue(ctx, "q", engine.QueueConfig{LockDurationMs: 30_000}); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.SendOne(ctx, "q", engine.OutMessage{Body: []byte("m")}); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(server.New(eng, nil).Handler())
	defer ts.Close()

	c, err := mqlite.Open(ctx, ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	msgs, err := c.Receive(ctx, "q", mqlite.RecvOpts{Max: 1})
	if err != nil || len(msgs) != 1 {
		t.Fatalf("receive: %v n=%d", err, len(msgs))
	}
	before := msgs[0].LockedUntil
	// Let the clock move, so a renewal genuinely commits a LATER deadline than the one the message
	// arrived with — otherwise "did it mutate the message?" is unanswerable: both values would be
	// the same millisecond and the check could not fail even if the SDK did write.
	time.Sleep(20 * time.Millisecond)

	res, err := c.RenewBatch(ctx, "q", msgs...)
	if err != nil || !res[0].Ok {
		t.Fatalf("RenewBatch: %v ok=%v", err, res[0].Ok)
	}
	if res[0].LockedUntil.IsZero() {
		t.Fatal("RenewBatch did not report the deadline it committed — the caller cannot pace itself")
	}
	if !msgs[0].LockedUntil.Equal(before) {
		t.Error("RenewBatch mutated the caller's message; the command renders those concurrently, so that is a data race")
	}

	// The renewer takes the reported deadline and paces itself by it: a third of the real 30s
	// lease, not the floor.
	renew := renewFunc(c, "q", msgs, 30*time.Second)
	until := renew(ctx)
	if until.IsZero() {
		t.Fatal("renewFunc did not report a deadline")
	}
	if d := time.Until(until) / 3; d < 8*time.Second || d > 12*time.Second {
		t.Errorf("cadence = %s, want about a third of the 30s lease — otherwise the CLI renews %d times a second forever",
			d, time.Second/minRenewInterval)
	}
}

// The legacy path renews per message, and an old broker reports no deadline — so the renewer must
// INFER one from the lease the batch arrived with. Otherwise it has nothing to pace by, collapses
// to the floor, and pelts a broker that predates RenewBatch with a full per-message renewal pass
// every 50ms for the rest of a slow output (codex).
func TestLegacyFallbackStillPacesItself(t *testing.T) {
	resetGlobals(t)
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{
		DB: "file:" + filepath.Join(t.TempDir(), "mq.db"), DisableBackground: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	if err := eng.CreateQueue(ctx, "q", engine.QueueConfig{LockDurationMs: 30_000}); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.SendOne(ctx, "q", engine.OutMessage{Body: []byte("m")}); err != nil {
		t.Fatal(err)
	}
	inner := server.New(eng, nil).Handler()
	var singleRenews atomic.Int64
	old := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case wire.PathRenewBatch: // a broker that predates it: its catch-all answers
			r2 := r.Clone(r.Context())
			r2.URL.Path = "/no-such-route-on-an-old-broker"
			inner.ServeHTTP(w, r2)
			return
		case wire.PathRenew:
			singleRenews.Add(1)
		}
		inner.ServeHTTP(w, r)
	}))
	defer old.Close()

	c, err := mqlite.Open(ctx, old.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	msgs, err := c.Receive(ctx, "q", mqlite.RecvOpts{Max: 1})
	if err != nil || len(msgs) != 1 {
		t.Fatalf("receive: %v n=%d", err, len(msgs))
	}

	renew := renewFunc(c, "q", msgs, 30*time.Second) // the lease this batch arrived with
	until := renew(ctx)                              // downgrades to per-message Renew
	if singleRenews.Load() == 0 {
		t.Fatal("expected the per-message fallback to be used")
	}
	if until.IsZero() {
		t.Fatal("the fallback reported no deadline — the renewer would collapse to the 50ms floor and hammer an old broker")
	}
	if d := time.Until(until) / 3; d < 8*time.Second || d > 12*time.Second {
		t.Errorf("fallback cadence = %s, want about a third of the inferred 30s lease", d)
	}
}

// pace() is the whole renewal cadence policy, so it is worth pinning exactly. The rule it must
// never break: a renewal is never scheduled AT OR AFTER the deadline it exists to save. A cadence
// floor is a spin guard, not a licence to sleep past the lock (codex).
func TestPaceNeverSchedulesPastTheDeadline(t *testing.T) {
	// No deadline to aim at (a failed renewal, or an old broker that never told us one): fall back
	// to the calm retry — NOT to a value derived from a lease we do not have.
	if d := pace(time.Time{}); d != renewRetryInterval {
		t.Errorf("pace(unknown) = %s, want the retry cadence %s", d, renewRetryInterval)
	}
	// A spent lease: renew now.
	if d := pace(time.Now().Add(-time.Second)); d != 0 {
		t.Errorf("pace(spent) = %s, want 0 (renew immediately)", d)
	}
	// A healthy lease: a third of it.
	if d := pace(time.Now().Add(30 * time.Second)); d < 9*time.Second || d > 11*time.Second {
		t.Errorf("pace(30s) = %s, want about a third", d)
	}
	// Short leases — the case that used to be handed to the 1s failure backoff, letting a freshly
	// renewed lock be reaped while we waited. Whatever the floor says, the tick must land BEFORE
	// the deadline.
	for _, remaining := range []time.Duration{
		149 * time.Millisecond, 100 * time.Millisecond, 60 * time.Millisecond,
		30 * time.Millisecond, 10 * time.Millisecond, 2 * time.Millisecond,
	} {
		d := pace(time.Now().Add(remaining))
		if d <= 0 {
			t.Errorf("pace(%s) = %s: a live lease must still be scheduled, not treated as spent", remaining, d)
			continue
		}
		if d >= remaining {
			t.Errorf("pace(%s) = %s — the renewal would be scheduled at or AFTER the deadline it is meant to save",
				remaining, d)
		}
	}
}

// The legacy path infers a deadline, and it must measure it from BEFORE the renewals — each lock
// is really extended when its UPDATE runs on the server, so a slow response means the lease
// started well before the reply came back. Reading the clock afterwards credits the batch with
// time it never had: a 1s lease whose Renew takes 600ms would be reported as good for another
// full second, and the next tick would fall after it had already died (codex).
func TestLegacyInferredDeadlineIsMeasuredBeforeTheCall(t *testing.T) {
	resetGlobals(t)
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{
		DB: "file:" + filepath.Join(t.TempDir(), "mq.db"), DisableBackground: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	if err := eng.CreateQueue(ctx, "q", engine.QueueConfig{LockDurationMs: 1000}); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.SendOne(ctx, "q", engine.OutMessage{Body: []byte("m")}); err != nil {
		t.Fatal(err)
	}
	inner := server.New(eng, nil).Handler()
	slowOld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case wire.PathRenewBatch: // a broker that predates it
			r2 := r.Clone(r.Context())
			r2.URL.Path = "/no-such-route-on-an-old-broker"
			inner.ServeHTTP(w, r2)
			return
		case wire.PathRenew:
			time.Sleep(600 * time.Millisecond) // a slow link: most of the 1s lease is already gone
		}
		inner.ServeHTTP(w, r)
	}))
	defer slowOld.Close()

	c, err := mqlite.Open(ctx, slowOld.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	msgs, err := c.Receive(ctx, "q", mqlite.RecvOpts{Max: 1})
	if err != nil || len(msgs) != 1 {
		t.Fatalf("receive: %v n=%d", err, len(msgs))
	}

	renew := renewFunc(c, "q", msgs, time.Second) // the 1s lease this batch arrived with
	until := renew(ctx)                           // downgrades, then renews slowly
	if until.IsZero() {
		t.Fatal("the fallback reported no deadline")
	}
	// The renewal took ~600ms of the 1s lease, so at most ~400ms may remain. Measuring the deadline
	// after the call would report a full second.
	if remaining := time.Until(until); remaining > 600*time.Millisecond {
		t.Errorf("inferred %s of lease remaining after a 600ms renewal of a 1s lock — the deadline was measured AFTER the call, crediting time the lease never had",
			remaining)
	}
}
