package main

// Contract tests (MQLITE-94 / review 2026-07-12 §5): they CAPTURE and parse stdout — the
// earlier CLI tests only checked for a returned error, which let key/field/empty-array
// regressions through. Covered: JSON schema of receive, exact arity + mutual exclusion,
// empty-collection [], past-schedule rejection, and top-level help completeness.

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mqlitehq/mqlite"
)

// captureStdout redirects os.Stdout for the duration of fn (tests run sequentially, so the
// global swap is safe) and returns what was written.
func captureStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	runErr := fn()
	_ = w.Close()
	os.Stdout = old
	out, _ := io.ReadAll(r)
	return string(out), runErr
}

func embeddedEnv(t *testing.T) {
	t.Helper()
	resetGlobals(t)
	t.Setenv("MQLITE_ENDPOINT", "")
	t.Setenv("MQLITE_DB", "file:"+filepath.Join(t.TempDir(), "mq.db"))
}

// P2-2: receive --output json emits snake_case keys matching the wire, including the
// operationally-important correlation_id / content_type / timestamps and the lock token.
func TestReceiveJSONShape(t *testing.T) {
	embeddedEnv(t)
	ctx := context.Background()
	c, err := dial(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CreateQueue(ctx, "q", mqlite.QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SendOne(ctx, "q", mqlite.OutMessage{
		Body: []byte("hi"), MessageID: "m1", GroupID: "g1", CorrelationID: "c1",
		Subject: "s", ReplyTo: "r", ContentType: "text/plain", Properties: map[string]string{"k": "v"},
	}); err != nil {
		t.Fatal(err)
	}
	_ = c.Close()

	out, err := captureStdout(t, func() error {
		return cmdReceive(ctx, []string{"q", "--no-ack", "--output", "json"})
	})
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	var got []map[string]any
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("output is not a JSON array: %v\n%s", err, out)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 message, got %d", len(got))
	}
	m := got[0]
	for _, k := range []string{
		"seq", "body", "message_id", "group_id", "correlation_id", "subject",
		"reply_to", "content_type", "properties", "enqueued_at_ms", "locked_until_ms", "lock_token",
	} {
		if _, ok := m[k]; !ok {
			t.Errorf("receive JSON missing key %q; got keys %v", k, keysOf(m))
		}
	}
	if m["body"] != "aGk=" { // base64("hi")
		t.Errorf("body = %v, want base64 aGk=", m["body"])
	}
}

func keysOf(m map[string]any) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}

// P2-4: exact arity, mutually exclusive flags, and empty-collection [] (not null).
func TestCLIContractGuards(t *testing.T) {
	embeddedEnv(t)
	ctx := context.Background()
	// receive --no-ack --delete is contradictory.
	if err := cmdReceive(ctx, []string{"q", "--no-ack", "--delete"}); err == nil {
		t.Error("receive --no-ack --delete must be rejected")
	}
	// stray positionals rejected.
	if err := cmdStatus(ctx, []string{"extra"}); err == nil {
		t.Error("status with an argument must be rejected")
	}
	if err := cmdCancel(ctx, []string{"q", "1", "extra"}); err == nil {
		t.Error("cancel with a stray positional must be rejected")
	}
	// a past absolute schedule is rejected.
	if err := cmdSchedule(ctx, []string{"q", "body", "--at", "2000-01-01T00:00:00Z"}); err == nil {
		t.Error("schedule with a past --at must be rejected")
	}
	// empty list in JSON mode is [] not null.
	out, err := captureStdout(t, func() error { return cmdList(ctx, []string{"--output", "json"}) })
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(out) != "[]" {
		t.Errorf("empty list --output json = %q, want []", strings.TrimSpace(out))
	}
}

// P2-5: the top-level help must list every dispatched command, so one can't silently vanish.
func TestUsageListsAllCommands(t *testing.T) {
	out, _ := captureStdout(t, func() error { usage(); return nil })
	for _, cmd := range []string{
		"serve", "create-queue", "subscribe", "send", "schedule", "cancel", "receive",
		"receive-deferred", "complete", "abandon", "reject", "defer", "renew", "peek",
		"metrics", "status", "list", "list-subscriptions", "test-filter", "redrive",
		"purge-dlq", "vacuum",
	} {
		if !strings.Contains(out, cmd) {
			t.Errorf("help is missing the %q command", cmd)
		}
	}
}
