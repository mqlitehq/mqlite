package main

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mqlitehq/mqlite"
	"github.com/mqlitehq/mqlite/internal/defaults"
)

// TestResolveBrokerTokens covers the broker's secure-by-default token policy.
func TestResolveBrokerTokens(t *testing.T) {
	// unset -> a token is generated (broker is never silently wide open).
	if csv, note, err := resolveBrokerTokens(""); err != nil || !strings.HasPrefix(csv, mqlite.TokenPrefix) || !strings.Contains(note, "generated") {
		t.Errorf("unset: csv=%q note=%q err=%v, want a generated mqk_ token", csv, note, err)
	}
	// off (case/space-insensitive) -> auth explicitly disabled.
	for _, off := range []string{"off", "OFF", "  off  "} {
		if csv, note, err := resolveBrokerTokens(off); err != nil || csv != "" || !strings.Contains(note, "DISABLED") {
			t.Errorf("%q: csv=%q note=%q err=%v, want disabled", off, csv, note, err)
		}
	}
	// provided -> cleaned CSV of the valid tokens (blank elements dropped, spaces trimmed).
	if csv, note, err := resolveBrokerTokens("a, b ,c"); err != nil || csv != "a,b,c" || !strings.Contains(note, "MQLITE_TOKENS") {
		t.Errorf("provided: csv=%q note=%q err=%v, want a,b,c", csv, note, err)
	}
	// set-but-empty (only blanks/commas) MUST fail, not silently disable auth (MQLITE-69).
	for _, bad := range []string{",", " , ", ",,", "   ,"} {
		if csv, _, err := resolveBrokerTokens(bad); err == nil {
			t.Errorf("%q: want an error (no usable token), got csv=%q nil err", bad, csv)
		}
	}
}

// TestResolveCORS covers the broker's CORS policy: wildcard by default while auth is ON, but
// OFF by default once auth is disabled (MQLITE-70 / D6); "off" always disables it and an
// explicit origin / explicit "*" is an opt-in honored even with auth off.
func TestResolveCORS(t *testing.T) {
	// auth ON, unset -> wildcard (a token is still required).
	if origin, _ := resolveCORS("", false); origin != "*" {
		t.Errorf("auth-on unset: origin=%q, want *", origin)
	}
	// auth OFF, unset -> off (a wildcard would let any page drive an open broker).
	if origin, note := resolveCORS("", true); origin != "" || !strings.Contains(note, "auth disabled") {
		t.Errorf("auth-off unset: origin=%q note=%q, want off", origin, note)
	}
	// "off" disables regardless of auth.
	for _, off := range []string{"off", "OFF", "  off  "} {
		for _, authOff := range []bool{false, true} {
			if origin, note := resolveCORS(off, authOff); origin != "" || !strings.Contains(note, "off") {
				t.Errorf("%q (authOff=%v): origin=%q note=%q, want disabled", off, authOff, origin, note)
			}
		}
	}
	// explicit origin -> verbatim.
	if origin, _ := resolveCORS("https://app.example", true); origin != "https://app.example" {
		t.Errorf("provided: origin=%q", origin)
	}
	// explicit "*" with auth off -> honored (opt-in) but the note warns.
	if origin, note := resolveCORS("*", true); origin != "*" || !strings.Contains(note, "WARNING") {
		t.Errorf("explicit wildcard auth-off: origin=%q note=%q, want * with warning", origin, note)
	}
}

// TestIsLoopbackListen covers the auth-off bind guard's loopback classification (MQLITE-70).
func TestIsLoopbackListen(t *testing.T) {
	cases := map[string]bool{
		":6754":            false, // all interfaces
		"0.0.0.0:6754":     false,
		"192.168.1.5:6754": false,
		"[::]:6754":        false, // IPv6 unspecified
		"127.0.0.1:6754":   true,
		"127.0.0.5:6754":   true, // 127.0.0.0/8
		"localhost:6754":   true,
		"[::1]:6754":       true,
	}
	for addr, want := range cases {
		if got := isLoopbackListen(addr); got != want {
			t.Errorf("isLoopbackListen(%q) = %v, want %v", addr, got, want)
		}
	}
}

// TestCommandsEndToEnd drives the CLI command handlers against one embedded DB,
// exercising flag parsing, dispatch, and output formatting (MQLITE-26). Each
// command dials and closes its own DB, so calls run sequentially.
// TestResolveListenAddr covers the listen-address precedence (MQLITE-84):
// explicit --addr > non-empty MQLITE_ADDR > :6754, with blank values rejected.
func TestResolveListenAddr(t *testing.T) {
	if got, err := resolveListenAddr("", false, "", false); err != nil || got != defaults.BrokerListenAddr {
		t.Fatalf("default: got %q err %v, want %q", got, err, defaults.BrokerListenAddr)
	}
	if got, err := resolveListenAddr("", false, "127.0.0.1:9000", true); err != nil || got != "127.0.0.1:9000" {
		t.Errorf("env used when flag unset: got %q err %v", got, err)
	}
	if got, err := resolveListenAddr(":9001", true, "127.0.0.1:9000", true); err != nil || got != ":9001" {
		t.Errorf("explicit flag beats env: got %q err %v", got, err)
	}
	if got, err := resolveListenAddr(" :9002 ", true, "", false); err != nil || got != ":9002" {
		t.Errorf("flag value trimmed: got %q err %v", got, err)
	}
	if _, err := resolveListenAddr("   ", true, "", false); err == nil {
		t.Error("blank --addr should be rejected")
	}
	if _, err := resolveListenAddr("  ", true, "127.0.0.1:9000", true); err == nil {
		t.Error("blank --addr must error even when MQLITE_ADDR is valid (explicit wins)")
	}
	if _, err := resolveListenAddr("", false, "  ", true); err == nil {
		t.Error("blank MQLITE_ADDR should be rejected")
	}
}

// TestReadCapped: an over-limit stdin body must error, never be silently truncated (MQLITE-79).
func TestReadCapped(t *testing.T) {
	if b, err := readCapped(strings.NewReader("hello"), 16); err != nil || string(b) != "hello" {
		t.Fatalf("under cap: got %q err %v", b, err)
	}
	if b, err := readCapped(strings.NewReader(strings.Repeat("x", 16)), 16); err != nil || len(b) != 16 {
		t.Fatalf("exactly at cap: len %d err %v", len(b), err)
	}
	if _, err := readCapped(strings.NewReader(strings.Repeat("x", 17)), 16); err == nil {
		t.Fatal("one byte over the cap must error, not truncate")
	}
}

func TestCommandsEndToEnd(t *testing.T) {
	ctx := context.Background()
	t.Setenv("MQLITE_ENDPOINT", "") // force the embedded path in dial()
	t.Setenv("MQLITE_DB", "file:"+filepath.Join(t.TempDir(), "mq.db"))

	ok := func(name string, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}

	ok("create-queue", cmdCreateQueue(ctx, []string{"orders", "--max-delivery", "5", "--lock", "30s", "--ordering", "group_fifo"}))
	ok("subscribe", cmdCreateSubscription(ctx, []string{"events", "subA", "--expr", `subject startsWith "ord."`}))
	ok("send", cmdSend(ctx, []string{"orders", "hello", "--group", "g1", "--subject", "ord.created"}))
	ok("send-id", cmdSend(ctx, []string{"orders", "world", "--group", "g1", "--message-id", "id-1"}))
	ok("list", cmdList(ctx, nil))
	ok("metrics", cmdMetrics(ctx, []string{"orders"}))
	ok("peek", cmdPeek(ctx, []string{"orders", "--max", "10"}))
	ok("peek-state", cmdPeek(ctx, []string{"orders", "--state", "active"}))
	ok("receive", cmdReceive(ctx, []string{"orders", "--max", "5"}))
	ok("receive-empty", cmdReceive(ctx, []string{"orders"})) // the "(no messages)" path

	// redrive: dead-letter one on its own queue (max-delivery 1), then move it back.
	ok("dlq-queue", cmdCreateQueue(ctx, []string{"dlq", "--max-delivery", "1"}))
	deadLetterOne(t, ctx, "dlq")
	ok("redrive", cmdRedrive(ctx, []string{"dlq"}))

	// Usage branches: too-few positional args must error, not panic.
	for name, err := range map[string]error{
		"send/none":     cmdSend(ctx, nil),
		"create/none":   cmdCreateQueue(ctx, nil),
		"peek/none":     cmdPeek(ctx, nil),
		"metrics/none":  cmdMetrics(ctx, nil),
		"redrive/none":  cmdRedrive(ctx, nil),
		"subscribe/one": cmdCreateSubscription(ctx, []string{"only-topic"}),
		"purgedlq/none": cmdPurgeDLQ(ctx, nil),
	} {
		if err == nil {
			t.Errorf("%s: expected a usage error, got nil", name)
		}
	}
}

func deadLetterOne(t *testing.T, ctx context.Context, queue string) {
	t.Helper()
	c, err := dial(ctx)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if _, err := c.SendOne(ctx, queue, mqlite.OutMessage{Body: []byte("x")}); err != nil {
		t.Fatalf("send: %v", err)
	}
	msgs, err := c.Receive(ctx, queue, mqlite.RecvOpts{})
	if err != nil || len(msgs) != 1 {
		t.Fatalf("receive: %v n=%d", err, len(msgs))
	}
	if err := msgs[0].Reject(ctx, mqlite.RejectOpts{Reason: "test"}); err != nil {
		t.Fatalf("reject (dead-letter): %v", err)
	}
}
