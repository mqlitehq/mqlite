package main

// Contract tests (MQLITE-94 / review 2026-07-12 §5): they CAPTURE and parse stdout — the
// earlier CLI tests only checked for a returned error, which let key/field/empty-array
// regressions through. Covered: JSON schema of receive, exact arity + mutual exclusion,
// empty-collection [], past-schedule rejection, and top-level help completeness.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mqlitehq/mqlite"
	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/server"
	"github.com/mqlitehq/mqlite/wire"
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

	// Drain CONCURRENTLY. Reading only after fn returns deadlocks the moment fn writes more than
	// the pipe's buffer — it blocks in write, forever, waiting for a reader that has not started.
	// A 64-message receive fits; 256 does not, and the Windows buffer is smaller still, which is
	// exactly how this hung the CI runner for ten minutes.
	done := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()

	runErr := fn()
	_ = w.Close()
	os.Stdout = old
	return <-done, runErr
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
		"seq_number", "body", "message_id", "group_id", "correlation_id", "subject",
		"reply_to", "content_type", "properties", "enqueued_at_ms", "locked_until_ms", "lock_token",
	} {
		if _, ok := m[k]; !ok {
			t.Errorf("receive JSON missing key %q; got keys %v", k, keysOf(m))
		}
	}
	if _, stale := m["seq"]; stale {
		t.Error(`receive JSON uses "seq"; the wire key is "seq_number" (round-2 §3.4)`)
	}
	if m["body"] != "aGk=" { // base64("hi")
		t.Errorf("body = %v, want base64 aGk=", m["body"])
	}
	// Every key the CLI emits must be a real wire.Message field — no CLI-invented names.
	wireKeys := jsonTagsOf(reflect.TypeOf(wire.Message{}))
	for k := range m {
		if !wireKeys[k] {
			t.Errorf("receive JSON key %q is not a wire.Message field — the CLI must not invent keys", k)
		}
	}
}

// §3.4: the CLI's JSON views ARE the wire types, not lookalikes that can drift apart. Pinning
// the TYPE (not a hand-listed set of keys) means a field added to the wire is automatically
// carried by the CLI, and reintroducing a hand-rolled view struct fails here immediately —
// which is how `seq` vs `seq_number` and the dropped visible_at_ms/locked_until_ms happened.
func TestCLIJSONIsWireShape(t *testing.T) {
	want := reflect.TypeOf(wire.Message{})
	if got := reflect.TypeOf(viewMsg(&mqlite.Message{}, false)); got != want {
		t.Errorf("the receive/deferred JSON view is %v, want %v", got, want)
	}
	if got := reflect.TypeOf(viewPeeked(nil)).Elem(); got != want {
		t.Errorf("the peek JSON view element is %v, want %v", got, want)
	}
}

// jsonTagsOf returns the set of JSON key names a struct marshals to.
func jsonTagsOf(t reflect.Type) map[string]bool {
	out := make(map[string]bool, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		name, _, _ := strings.Cut(t.Field(i).Tag.Get("json"), ",")
		if name != "" && name != "-" {
			out[name] = true
		}
	}
	return out
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

// ─── arity, validation & reporting contract (review round-2 §3) ───

// A non-body command takes an EXACT number of positionals. A surplus argument is a typo (a
// misplaced flag, an unquoted value the shell split) — silently ignoring it and exiting 0 hides
// the mistake, and on a destructive command that is how you purge the wrong queue.
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

	// --output json must be passed as a FLAG: newFlags() re-registers --output and resets the
	// global to "text", so setting gOutput directly never reached the JSON branch at all — the
	// assertion below was running against text output and could not have failed (round-3 §3.4).
	resetGlobals(t)
	out, err = captureStdout(t, func() error { return cmdVacuum(ctx0, []string{"--output", "json"}) })
	if err != nil {
		t.Fatalf("vacuum --output json: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("vacuum --output json did not emit JSON: %v\n%s", err, out)
	}
	freed, ok := got["freed_bytes"].(float64)
	if !ok {
		t.Fatalf("vacuum JSON has no freed_bytes: %v", got)
	}
	if freed < 0 {
		t.Errorf("vacuum JSON reported negative freed_bytes: %v", freed)
	}
	if _, ok := got["grew_bytes"]; !ok {
		t.Errorf("vacuum JSON must report growth separately: %v", got)
	}
}

// Round-3 §3.4: the remaining contract gaps — arity on the meta commands, flag PRESENCE (not
// value) for the destructive-mode conflict, strictly positive sequence numbers, and a body
// given twice.
func TestRound3ContractGaps(t *testing.T) {
	embeddedEnv(t)
	ctx := context.Background()

	// A seq is a positive number: 0 and negatives are not messages. (A negative POSITIONAL is
	// already rejected earlier, by the flag parser — it looks like a flag. The gap was the
	// value reaching the backend, and the --seq list, where a negative is just a CSV field.)
	if err := cmdSettle(ctx, "complete", []string{"q", "0", "tok"}); err == nil {
		t.Error("complete with seq 0 must be rejected")
	}
	if err := cmdCancel(ctx, []string{"q", "0"}); err == nil {
		t.Error("cancel with seq 0 must be rejected")
	}
	for _, bad := range []string{"0", "-1", "3,0", "3,-1"} {
		if err := cmdReceiveDeferred(ctx, []string{"q", "--seq", bad}); err == nil {
			t.Errorf("receive-deferred --seq %s must be rejected, not silently return []", bad)
		}
	}

	// --all is "delete everything"; an explicit bound contradicts it even when the bound is 0.
	// A value-only check sees 0 and waves it through.
	for _, args := range [][]string{
		{"q", "--all", "--max", "0"},
		{"q", "--all", "--older-than", "0s"},
	} {
		if err := cmdPurgeDLQ(ctx, args); err == nil || !strings.Contains(err.Error(), "cannot be combined") {
			t.Errorf("purge-dlq %v must be rejected as ambiguous, got %v", args, err)
		}
	}

	// A body given twice is ambiguous — letting --file quietly win discards what was typed.
	f := filepath.Join(t.TempDir(), "body.txt")
	if err := os.WriteFile(f, []byte("from-file"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := cmdSend(ctx, []string{"q", "typed-body", "--file", f}); err == nil {
		t.Error("send with BOTH a positional body and --file must be rejected")
	}

	// Meta commands take no arguments.
	for _, cmd := range []string{"version", "help"} {
		if err := exactMeta(cmd, []string{"extra"}); err == nil {
			t.Errorf("%s with a surplus argument must be rejected", cmd)
		}
		if err := exactMeta(cmd, nil); err != nil {
			t.Errorf("%s with no arguments must be accepted, got %v", cmd, err)
		}
	}
	if err := cmdServe(ctx, []string{"extra"}); err == nil {
		t.Error("serve with a surplus positional must be rejected")
	}
}

// captureStdout must drain the pipe while fn runs, not after it returns. Reading afterwards
// deadlocks as soon as fn writes more than the pipe's buffer: the write blocks forever, waiting
// for a reader that has not started. A 64-message receive fits in the buffer and a 256-message
// one does not — and Windows' buffer is smaller still, which is how this hung a CI runner for
// ten minutes and looked like a bug in the code under test.
//
// A payload larger than any plausible pipe buffer makes the guard platform-independent: on the
// old helper this test hangs everywhere, not just on Windows.
func TestCaptureStdoutDoesNotDeadlockOnLargeOutput(t *testing.T) {
	const size = 4 << 20 // 4 MiB — far past every OS pipe buffer
	out, err := captureStdout(t, func() error {
		_, werr := os.Stdout.Write(bytes.Repeat([]byte("x"), size))
		return werr
	})
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if len(out) != size {
		t.Errorf("captured %d bytes, want %d", len(out), size)
	}
}
