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
	if csv, note := resolveBrokerTokens(""); !strings.HasPrefix(csv, mqlite.TokenPrefix) || !strings.Contains(note, "generated") {
		t.Errorf("unset: csv=%q note=%q, want a generated mqk_ token", csv, note)
	}
	// off (case/space-insensitive) -> auth explicitly disabled.
	for _, off := range []string{"off", "OFF", "  off  "} {
		if csv, note := resolveBrokerTokens(off); csv != "" || !strings.Contains(note, "DISABLED") {
			t.Errorf("%q: csv=%q note=%q, want disabled", off, csv, note)
		}
	}
	// provided -> passed through verbatim.
	if csv, note := resolveBrokerTokens("a,b"); csv != "a,b" || !strings.Contains(note, "MQLITE_TOKENS") {
		t.Errorf("provided: csv=%q note=%q", csv, note)
	}
}

// TestResolveCORS covers the broker's CORS policy: open by default (token still required),
// off on request, otherwise a verbatim origin.
func TestResolveCORS(t *testing.T) {
	if origin, _ := resolveCORS(""); origin != "*" {
		t.Errorf("unset: origin=%q, want *", origin)
	}
	for _, off := range []string{"off", "OFF", "  off  "} {
		if origin, note := resolveCORS(off); origin != "" || !strings.Contains(note, "off") {
			t.Errorf("%q: origin=%q note=%q, want disabled", off, origin, note)
		}
	}
	if origin, _ := resolveCORS("https://app.example"); origin != "https://app.example" {
		t.Errorf("provided: origin=%q", origin)
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
	if _, err := resolveListenAddr("", false, "  ", true); err == nil {
		t.Error("blank MQLITE_ADDR should be rejected")
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
