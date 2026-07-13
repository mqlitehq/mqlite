package main

// Round-2 contract tests (review 2026-07-12 round 2, §3): exact arity, endpoint equivalence,
// lease renewal across settlement, and vacuum reporting. The two data-loss blockers (§B1/B2)
// are covered in blackbox_test.go and safety_test.go.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mqlitehq/mqlite"
	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/server"
	"github.com/mqlitehq/mqlite/wire"
)

// §3.3: a non-body command takes an EXACT number of positionals. A surplus argument is a typo
// (a misplaced flag, an unquoted value the shell split) — silently ignoring it and exiting 0
// hides the mistake, and on a destructive command that is how you purge the wrong queue.
func TestExactArity(t *testing.T) {
	resetGlobals(t)
	t.Setenv("MQLITE_ENDPOINT", "")
	t.Setenv("MQLITE_DB", "file:"+filepath.Join(t.TempDir(), "mq.db"))

	surplus := []struct {
		name string
		fn   func(context.Context, []string) error
		args []string
	}{
		{"create-queue", cmdCreateQueue, []string{"q", "extra"}},
		{"subscribe", cmdCreateSubscription, []string{"topic", "sub", "extra"}},
		{"peek", cmdPeek, []string{"q", "extra"}},
		{"metrics", cmdMetrics, []string{"q", "extra"}},
		{"list", cmdList, []string{"extra"}},
		{"vacuum", cmdVacuum, []string{"extra"}},
		{"redrive", cmdRedrive, []string{"q", "extra"}},
		{"purge-dlq", cmdPurgeDLQ, []string{"q", "--all", "extra"}},
	}
	for _, c := range surplus {
		resetGlobals(t)
		if err := c.fn(ctx0, c.args); err == nil {
			t.Errorf("%s: a surplus positional must be rejected, not ignored", c.name)
		} else if !strings.Contains(err.Error(), "usage:") {
			t.Errorf("%s: want a usage error, got %v", c.name, err)
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

// §3.5: the token boundary is the AUTHORITY (scheme/host/effective port), not raw text. Two
// spellings of the same broker must not cost the caller their token; a different host still
// must.
func TestSameEndpoint(t *testing.T) {
	same := [][2]string{
		{"http://127.0.0.1:6754", "http://127.0.0.1:6754/"}, // trailing slash (the reported bug)
		{"http://127.0.0.1:6754", "http://127.0.0.1:6754"},  // identical
		{"mqlite://h", "http://h:6754"},                     // custom scheme supplies the product port
		{"http://Host:6754", "http://host:6754"},            // host case
		{"mqlites://h", "https://h:6754"},                   // TLS variant
		{"mqlite://tok@h:6754", "http://h:6754"},            // an embedded credential is not part of the target
	}
	for _, p := range same {
		if !sameEndpoint(p[0], p[1]) {
			t.Errorf("sameEndpoint(%q, %q) = false, want true (same broker)", p[0], p[1])
		}
	}
	diff := [][2]string{
		{"http://h:6754", "http://other:6754"}, // different host
		{"http://h:6754", "http://h:9000"},     // different port
		{"http://h:6754", "https://h:6754"},    // different transport — never send a token over a downgrade
	}
	for _, p := range diff {
		if sameEndpoint(p[0], p[1]) {
			t.Errorf("sameEndpoint(%q, %q) = true, want false (different target)", p[0], p[1])
		}
	}
}

// §3.2: renewal must stay alive through the settlement RPC. On a high-latency link the
// CompleteBatch itself can outlast the lock; if renewal stops when output is written (as it
// used to), the reaper reclaims the batch mid-settle and every message redelivers even though
// the caller already saw it. Proxy the broker with a delay on CompleteBatch that exceeds the
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

// §3.6: a brand-new DB materializes its schema pages as it is opened and vacuumed, so it can
// end up LARGER than it started. Reporting that as "freed -0.12 MiB" is nonsense.
func TestVacuumNewDBReportsNoNegativeFreed(t *testing.T) {
	resetGlobals(t)
	t.Setenv("MQLITE_ENDPOINT", "")
	t.Setenv("MQLITE_DB", "file:"+filepath.Join(t.TempDir(), "fresh.db"))
	out, err := captureStdout(t, func() error { return cmdVacuum(ctx0, nil) })
	if err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	if strings.Contains(out, "freed -") {
		t.Errorf("vacuum reported negative freed space: %q", out)
	}

	resetGlobals(t)
	gOutput = "json"
	out, err = captureStdout(t, func() error { return cmdVacuum(ctx0, nil) })
	if err != nil {
		t.Fatalf("vacuum --output json: %v", err)
	}
	if strings.Contains(out, `"freed_bytes": -`) {
		t.Errorf("vacuum JSON reported negative freed_bytes: %q", out)
	}
}

// §3.1: a negative scheduled_enqueue_time_ms is not an instant. Raw HTTP used to fall through
// the `> 0` branch and enqueue the message ACTIVE with a 200, disagreeing with embedded and
// CLI, where the engine rejects it. All three must now reject it.
func TestNegativeScheduleRejectedEverywhere(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	if err := eng.CreateQueue(ctx, "q", engine.QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(server.New(eng, nil).Handler())
	defer ts.Close()

	// Raw HTTP — the hole.
	body := strings.NewReader(`{"queue":"q","messages":[{"body":"eA=="}],"scheduled_enqueue_time_ms":-1}`)
	resp, err := http.Post(ts.URL+wire.PathSend, "application/json", body)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("raw HTTP send with scheduled_enqueue_time_ms=-1: status %d, want 400", resp.StatusCode)
	}
	if m, err := eng.Stats(ctx, "q"); err != nil || m.Total != 0 {
		t.Fatalf("a rejected schedule must enqueue nothing (total=%d)", m.Total)
	}

	// Embedded SDK — the same value, the same verdict.
	emb, err := mqlite.OpenEmbedded(ctx, "file:"+filepath.Join(t.TempDir(), "mq.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer emb.Close()
	if err := emb.CreateQueue(ctx, "q", mqlite.QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	past := mqlite.SendOpts{At: time.Now().Add(-time.Hour)}
	if _, err := emb.SendOne(ctx, "q", mqlite.OutMessage{Body: []byte("x")}, past); err == nil {
		t.Error("embedded schedule in the past must be rejected")
	}
}
