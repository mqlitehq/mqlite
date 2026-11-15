package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mqlitehq/mqlite"
	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/internal/defaults"
	"github.com/mqlitehq/mqlite/server"
	"github.com/mqlitehq/mqlite/wire"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestEmbeddedOptsMaxSubscriptions(t *testing.T) {
	resetGlobals(t)
	ctx := context.Background()
	t.Setenv("MQLITE_ENDPOINT", "")
	t.Setenv("MQLITE_DB", "file:"+filepath.Join(t.TempDir(), "bounded.db"))
	t.Setenv("MQLITE_MAX_SUBSCRIPTIONS", "1")
	c, err := dial(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Subscribe(ctx, "topic", "first", nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Subscribe(ctx, "topic", "second", nil); !errors.Is(err, mqlite.ErrSubscriptionLimit) {
		t.Fatalf("second subscription = %v, want ErrSubscriptionLimit", err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	t.Setenv("MQLITE_DB", "file:"+filepath.Join(t.TempDir(), "default.db"))
	t.Setenv("MQLITE_MAX_SUBSCRIPTIONS", "not-a-number")
	oldStderr := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = w
	c, openErr := dial(ctx)
	_ = w.Close()
	os.Stderr = oldStderr
	warning, _ := io.ReadAll(r)
	_ = r.Close()
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer c.Close()
	if !strings.Contains(string(warning), `ignoring unparseable MQLITE_MAX_SUBSCRIPTIONS="not-a-number"`) {
		t.Fatalf("malformed limit warning = %q", warning)
	}
	for _, name := range []string{"first", "second"} {
		if err := c.Subscribe(ctx, "topic", name, nil); err != nil {
			t.Fatalf("default limit subscription %q: %v", name, err)
		}
	}
}

// TestCmdPurgeDLQ exercises the purge-dlq command end to end against an embedded
// DB: dead-letter a message, then confirm the command clears it (MQLITE-9).
func TestCmdPurgeDLQ(t *testing.T) {
	ctx := context.Background()
	t.Setenv("MQLITE_ENDPOINT", "") // force the embedded path in dial()
	t.Setenv("MQLITE_DB", "file:"+filepath.Join(t.TempDir(), "mq.db"))
	t.Setenv("MQLITE_SYNC", "FULL") // exercise the durability knob through embeddedOpts (MQLITE-7)

	// Arrange: one dead-lettered message. Close releases the file lock (MQLITE-6)
	// before the command opens the same DB.
	c, err := dial(ctx)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if err := c.CreateQueue(ctx, "q", mqlite.QueueConfig{}); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if _, err := c.SendOne(ctx, "q", mqlite.OutMessage{Body: []byte("x")}); err != nil {
		t.Fatalf("send: %v", err)
	}
	msgs, err := c.Receive(ctx, "q", mqlite.RecvOpts{})
	if err != nil || len(msgs) != 1 {
		t.Fatalf("receive: %v n=%d", err, len(msgs))
	}
	if err := msgs[0].Reject(ctx, mqlite.RejectOpts{Reason: "test"}); err != nil {
		t.Fatalf("reject (dead-letter): %v", err)
	}
	_ = c.Close()

	// Act: the CLI command purges the DLQ (unbounded purge now needs an explicit --all).
	if err := cmdPurgeDLQ(ctx, []string{"q", "--all"}); err != nil {
		t.Fatalf("purge-dlq: %v", err)
	}
	// An unbounded purge without --all must be refused.
	if err := cmdPurgeDLQ(ctx, []string{"q"}); err == nil {
		t.Error("purge-dlq without a bound or --all should be refused")
	}

	// Assert: nothing dead-lettered remains.
	c2, err := dial(ctx)
	if err != nil {
		t.Fatalf("re-dial: %v", err)
	}
	defer c2.Close()
	dlq, err := c2.Peek(ctx, "q", mqlite.PeekOpts{State: mqlite.DeadLettered})
	if err != nil {
		t.Fatalf("peek: %v", err)
	}
	if len(dlq) != 0 {
		t.Fatalf("DLQ should be empty after purge-dlq, got %d", len(dlq))
	}
}

var mqliteBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "mqcli-bb-*")
	if err != nil {
		panic(err)
	}
	mqliteBin = filepath.Join(dir, "mqlite")
	if runtime.GOOS == "windows" {
		mqliteBin += ".exe" // `go build -o` produces a .exe on Windows
	}
	build := exec.Command("go", "build", "-o", mqliteBin, ".")
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		panic("build test binary: " + err.Error())
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// bbBroker boots an in-process broker (its own engine so the test can inspect state) and
// returns the base URL, the token it requires, and the engine.
func bbBroker(t *testing.T, token string) (string, *engine.Engine) {
	t.Helper()
	eng, err := engine.Open(context.Background(), engine.Options{
		DB: "file:" + filepath.Join(t.TempDir(), "mq.db"), DisableBackground: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	var tokens []string
	if token != "" {
		tokens = []string{token}
	}
	ts := httptest.NewServer(server.New(eng, tokens).Handler())
	t.Cleanup(ts.Close)
	return ts.URL, eng
}

// exitCode runs the binary and returns its exit code + captured stderr.
func exitCode(cmd *exec.Cmd) (int, string) {
	var errb bytes.Buffer
	cmd.Stderr = &errb
	err := cmd.Run()
	if err == nil {
		return 0, errb.String()
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode(), errb.String()
	}
	return -1, errb.String() + err.Error()
}

func TestBlackboxDeleteQueueRejectsUnknownAndMalformedFlags(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{args: []string{"delete-queue", "q", "--discard"}, want: "flag provided but not defined"},
		{args: []string{"delete-queue", "q", "--force=eventually"}, want: "invalid boolean value"},
		{args: []string{"delete-subscription", "topic", "sub", "--discard"}, want: "flag provided but not defined"},
		{args: []string{"delete-subscription", "topic", "sub", "--force=eventually"}, want: "invalid boolean value"},
	} {
		cmd := exec.Command(mqliteBin, tc.args...)
		code, stderr := exitCode(cmd)
		if code == 0 || !strings.Contains(stderr, tc.want) {
			t.Errorf("mqlite %v: code=%d stderr=%q", tc.args, code, stderr)
		}
	}
}

func TestBlackboxSubscriptionDeleteAndPagedInventory(t *testing.T) {
	ctx := context.Background()
	url, eng := bbBroker(t, "tok")
	if err := eng.Subscribe(ctx, "events", "temporary", nil); err != nil {
		t.Fatal(err)
	}
	backlog := make([]engine.OutMessage, 5000)
	for i := range backlog {
		backlog[i].Body = []byte("x")
	}
	if _, err := eng.Send(ctx, "events", backlog...); err != nil {
		t.Fatal(err)
	}
	env := append(os.Environ(), "MQLITE_ENDPOINT="+url, "MQLITE_TOKEN=tok")

	nonForce := exec.Command(mqliteBin, "delete-subscription", "events", "temporary")
	nonForce.Env = env
	if code, stderr := exitCode(nonForce); code == 0 || !strings.Contains(stderr, "subscription still contains messages") {
		t.Fatalf("non-force delete: code=%d stderr=%q", code, stderr)
	}

	force := exec.Command(mqliteBin, "delete-subscription", "events", "temporary", "--force", "--output", "json")
	force.Env = env
	var forceOut, forceErr bytes.Buffer
	force.Stdout, force.Stderr = &forceOut, &forceErr
	if err := force.Run(); err != nil {
		t.Fatalf("force delete: %v stderr=%q", err, forceErr.String())
	}
	var ack struct {
		Topic   string `json:"topic"`
		Name    string `json:"name"`
		Deleted bool   `json:"deleted"`
		State   string `json:"state"`
	}
	if err := json.Unmarshal(forceOut.Bytes(), &ack); err != nil || ack.Topic != "events" || ack.Name != "temporary" || ack.Deleted || ack.State != "deleting" {
		t.Fatalf("force acknowledgement = %q (%v)", forceOut.String(), err)
	}

	legacy := exec.Command(mqliteBin, "list-subscriptions", "--output", "json")
	legacy.Env = env
	legacyOut, err := legacy.Output()
	if err != nil || strings.TrimSpace(string(legacyOut)) != "[]" {
		t.Fatalf("legacy active-only inventory = %q (%v)", legacyOut, err)
	}
	page := exec.Command(mqliteBin, "list-subscriptions", "--topic", "events", "--include-deleting", "--output", "json")
	page.Env = env
	pageOut, err := page.Output()
	if err != nil {
		t.Fatal(err)
	}
	var inventory wire.ListSubscriptionsPageResponse
	if err := json.Unmarshal(pageOut, &inventory); err != nil || inventory.Total != 1 || len(inventory.Subscriptions) != 1 || inventory.Subscriptions[0].State != "deleting" {
		t.Fatalf("paged deleting inventory = %q (%v)", pageOut, err)
	}
}

func TestBlackboxSubscribeTTL(t *testing.T) {
	ctx := context.Background()
	url, eng := bbBroker(t, "tok")
	_, listenToken, err := eng.CreateAccessKey(ctx, engine.CreateAccessKeyOptions{
		ID: strings.Repeat("b", 32), Name: "subscription provisioner", Permissions: engine.KeyListen,
	})
	if err != nil {
		t.Fatal(err)
	}
	env := append(os.Environ(), "MQLITE_ENDPOINT="+url, "MQLITE_TOKEN="+listenToken)

	create := exec.Command(mqliteBin, "subscribe", "requests", "temporary", "--ttl", "1h", "--expr", `subject == "request"`)
	create.Env = env
	if output, err := create.CombinedOutput(); err != nil {
		t.Fatalf("TTL subscribe: %v: %s", err, output)
	}
	page, err := eng.ListSubscriptionsWithOptions(ctx, engine.ListSubscriptionsOptions{Topic: "requests"})
	if err != nil || len(page.Subscriptions) != 1 {
		t.Fatalf("inventory = %+v, %v", page, err)
	}
	item := page.Subscriptions[0]
	if item.ExpiresAtMs-item.CreatedAtMs != 60*60*1000 || item.Expr != `subject == "request"` {
		t.Fatalf("temporary subscription = %+v", item)
	}

	invalid := exec.Command(mqliteBin, "subscribe", "requests", "sub-ms", "--ttl", "1ns")
	invalid.Env = env
	if code, stderr := exitCode(invalid); code == 0 || !strings.Contains(stderr, "at least 1ms") {
		t.Fatalf("sub-ms TTL: code=%d stderr=%q", code, stderr)
	}
	page, err = eng.ListSubscriptionsWithOptions(ctx, engine.ListSubscriptionsOptions{Topic: "requests"})
	if err != nil || page.Total != 1 {
		t.Fatalf("invalid TTL had side effects: %+v, %v", page, err)
	}

	testFilter := exec.Command(mqliteBin, "test-filter", `subject == "request"`, "--subject", "request")
	testFilter.Env = env
	if output, err := testFilter.CombinedOutput(); err != nil {
		t.Fatalf("listen test-filter: %v: %s", err, output)
	}

	for _, args := range [][]string{
		{"list-subscriptions"},
		{"list-subscriptions", "--topic", "requests"},
		{"delete-subscription", "requests", "temporary"},
		{"delete-subscription", "requests", "temporary", "--force"},
		{"create-queue", "forbidden"},
	} {
		cmd := exec.Command(mqliteBin, args...)
		cmd.Env = env
		var stdout bytes.Buffer
		cmd.Stdout = &stdout
		if code, stderr := exitCode(cmd); code == 0 || stdout.Len() != 0 || !strings.Contains(stderr, "permission denied") {
			t.Fatalf("listen command %v: code=%d stdout=%q stderr=%q", args, code, stdout.String(), stderr)
		}
	}
	page, err = eng.ListSubscriptionsWithOptions(ctx, engine.ListSubscriptionsOptions{Topic: "requests", IncludeDeleting: true})
	if err != nil || page.Total != 1 || len(page.Subscriptions) != 1 || page.Subscriptions[0].State != "active" {
		t.Fatalf("denied administration changed subscription: %+v, %v", page, err)
	}
}

// P1-1: if stdout is a broken pipe, `receive` must exit non-zero AND must NOT acknowledge
// the message — otherwise a Peek-Lock consumer silently loses data.
func TestBlackboxReceiveBrokenPipe(t *testing.T) {
	ctx := context.Background()
	url, eng := bbBroker(t, "tok")
	if err := eng.CreateQueue(ctx, "q", engine.QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.SendOne(ctx, "q", engine.OutMessage{Body: []byte("important")}); err != nil {
		t.Fatal(err)
	}

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(mqliteBin, "receive", "q")
	cmd.Env = append(os.Environ(), "MQLITE_ENDPOINT="+url, "MQLITE_TOKEN=tok")
	cmd.Stdout = w
	var errb bytes.Buffer
	cmd.Stderr = &errb
	// Close the only reader before starting the child. Closing it after Start races
	// with the child's small stdout write, which can otherwise succeed and ack the
	// message before the parent closes the pipe.
	if err := r.Close(); err != nil {
		_ = w.Close()
		t.Fatalf("close pipe reader: %v", err)
	}
	if err := cmd.Start(); err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	if err := w.Close(); err != nil { // parent's write end
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("close parent pipe writer: %v", err)
	}
	werr := cmd.Wait()

	if werr == nil {
		t.Fatalf("receive to a broken pipe should exit non-zero; stderr=%q", errb.String())
	}
	// The message must survive — locked and redeliverable, not deleted.
	if m, err := eng.Stats(ctx, "q"); err != nil || m.Total == 0 {
		t.Fatalf("message was lost after a broken-pipe receive (total=%d) — Peek-Lock must not become at-most-once", m.Total)
	}
}

// Round-2 B1: a process started with fd 1 already CLOSED (`1>&-`, distinct from a broken
// pipe) must exit non-zero and must NOT acknowledge/delete the message — the fd-reuse trap
// where a later open acquires descriptor 1 and writes appear to succeed against the DB.
func TestBlackboxReceiveClosedFd(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("1>&- is a POSIX shell redirection")
	}
	ctx := context.Background()
	url, eng := bbBroker(t, "tok")
	if err := eng.CreateQueue(ctx, "q", engine.QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.SendOne(ctx, "q", engine.OutMessage{Body: []byte("important")}); err != nil {
		t.Fatal(err)
	}
	// The shell closes fd 1 for the exec'd binary — exactly the reviewer's `receive q 1>&-`.
	cmd := exec.Command("sh", "-c", fmt.Sprintf("exec %q receive q 1>&-", mqliteBin))
	cmd.Env = append(os.Environ(), "MQLITE_ENDPOINT="+url, "MQLITE_TOKEN=tok")
	code, se := exitCode(cmd)
	if code == 0 {
		t.Fatalf("receive with fd 1 closed must exit non-zero; stderr=%q", se)
	}
	if m, err := eng.Stats(ctx, "q"); err != nil || m.Total == 0 {
		t.Fatalf("message was deleted after a closed-fd receive (total=%d) — data loss", m.Total)
	}
}

// P1-4: `send q -- hello --output json` stores the whole literal body and does NOT switch
// to JSON output.
func TestBlackboxDashDashBody(t *testing.T) {
	ctx := context.Background()
	url, eng := bbBroker(t, "tok")
	if err := eng.CreateQueue(ctx, "q", engine.QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	env := append(os.Environ(), "MQLITE_ENDPOINT="+url, "MQLITE_TOKEN=tok")

	cmd := exec.Command(mqliteBin, "send", "q", "--", "hello", "--output", "json")
	cmd.Env = env
	if code, se := exitCode(cmd); code != 0 {
		t.Fatalf("send exited %d: %s", code, se)
	}
	msgs, err := eng.Peek(ctx, "q", engine.PeekOptions{Max: 10})
	if err != nil || len(msgs) != 1 {
		t.Fatalf("peek: %v n=%d", err, len(msgs))
	}
	if got := string(msgs[0].Body); got != "hello --output json" {
		t.Errorf("stored body = %q, want %q (`--` must keep the literal body intact)", got, "hello --output json")
	}
}

// P1-5: an explicit empty --token sends no Authorization header, so an auth-required broker
// rejects it (proving the ambient MQLITE_TOKEN was not forwarded).
func TestBlackboxEmptyTokenSendsNoAuth(t *testing.T) {
	url, _ := bbBroker(t, "need-a-token")
	env := append(os.Environ(), "MQLITE_ENDPOINT="+url, "MQLITE_TOKEN=need-a-token")

	// With the env token it works.
	ok := exec.Command(mqliteBin, "list")
	ok.Env = env
	if code, se := exitCode(ok); code != 0 {
		t.Fatalf("list with env token should succeed, exited %d: %s", code, se)
	}
	// With an explicit empty --token it must be rejected (no header sent).
	cleared := exec.Command(mqliteBin, "list", "--token=")
	cleared.Env = env
	if code, _ := exitCode(cleared); code == 0 {
		t.Error("list --token= must send no token and be rejected by the authed broker")
	}

	// A credential embedded in the endpoint DSN (mqlite://token@host) authenticates on its
	// own, but --token= must strip it too.
	dsn := "mqlite://need-a-token@" + strings.TrimPrefix(url, "http://")
	dsnEnv := append(os.Environ(), "MQLITE_ENDPOINT="+dsn)
	viaDSN := exec.Command(mqliteBin, "list")
	viaDSN.Env = dsnEnv
	if code, se := exitCode(viaDSN); code != 0 {
		t.Fatalf("DSN-embedded credential should authenticate, exited %d: %s", code, se)
	}
	dsnCleared := exec.Command(mqliteBin, "list", "--token=")
	dsnCleared.Env = dsnEnv
	if code, _ := exitCode(dsnCleared); code == 0 {
		t.Error("--token= must strip a DSN-embedded credential (mqlite://token@host)")
	}
}

func TestBlackboxKeyCommand(t *testing.T) {
	url, eng := bbBroker(t, "administrator")
	id := strings.Repeat("a", 32)
	cmd := exec.Command(mqliteBin, "key", "create", "--name", "automation", "--permissions", "send", "--id", id, "--output", "json")
	cmd.Env = append(os.Environ(), "MQLITE_ENDPOINT="+url, "MQLITE_TOKEN=administrator")
	var out bytes.Buffer
	cmd.Stdout = &out
	if code, stderr := exitCode(cmd); code != 0 {
		t.Fatalf("key create exited %d: %s", code, stderr)
	}
	var created struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(out.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	page, err := eng.ListAccessKeys(context.Background(), "", 0)
	if err != nil || len(page.Keys) != 1 || page.Keys[0].ID != id {
		t.Fatalf("key create did not persist metadata: %+v, %v", page, err)
	}
	for _, args := range [][]string{{"key", "list"}, {"key", "revoke", "--id", id}, {"key", "create", "--name", "escalation", "--permissions", "manage"}} {
		cmd = exec.Command(mqliteBin, args...)
		cmd.Env = append(os.Environ(), "MQLITE_ENDPOINT="+url, "MQLITE_TOKEN="+created.Token)
		out.Reset()
		cmd.Stdout = &out
		code, stderr := exitCode(cmd)
		if code == 0 || !strings.Contains(stderr, "permission denied") || strings.Contains(stderr, created.Token) || out.Len() != 0 {
			t.Fatalf("restricted command result: code=%d stderr=%q stdout bytes=%d", code, stderr, out.Len())
		}
		if args[1] == "create" && !regexp.MustCompile(`create key ID [0-9a-f]{32}:`).MatchString(stderr) {
			t.Fatal("failed create must preserve generated ID")
		}
	}
}

func TestBlackboxKeyListSort(t *testing.T) {
	ctx := context.Background()
	clock := int64(100)
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true, Now: func() int64 { return clock }})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	for _, fixture := range []struct {
		prefix  string
		created int64
	}{{"f", 100}, {"1", 200}, {"a", 200}} {
		clock = fixture.created
		if _, _, err := eng.CreateAccessKey(ctx, engine.CreateAccessKeyOptions{ID: strings.Repeat(fixture.prefix, 32), Name: "worker-" + fixture.prefix, Permissions: engine.KeySend}); err != nil {
			t.Fatal(err)
		}
	}
	ts := httptest.NewServer(server.New(eng, []string{"administrator"}).Handler())
	defer ts.Close()
	run := func(args ...string) (int, string, string) {
		t.Helper()
		cmd := exec.Command(mqliteBin, args...)
		cmd.Env = append(os.Environ(), "MQLITE_ENDPOINT="+ts.URL, "MQLITE_TOKEN=administrator")
		var out bytes.Buffer
		cmd.Stdout = &out
		code, stderr := exitCode(cmd)
		return code, out.String(), stderr
	}
	code, out, stderr := run("key", "list", "--sort", "created_desc", "--limit", "1")
	if code != 0 {
		t.Fatalf("sorted list exited %d: %s", code, stderr)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 2 || !strings.HasPrefix(lines[0], strings.Repeat("a", 32)+"\t") || !strings.Contains(lines[1], "--sort created_desc") {
		t.Fatalf("first sorted page or continuation incorrect: %q", out)
	}
	// Run the actual continuation printed to the operator, including its sort.
	args := strings.Fields(strings.TrimPrefix(lines[1], "Next page: "))
	code, out, stderr = run(append(args, "--output", "json")...)
	var page wire.ListKeysResponse
	if code != 0 || json.Unmarshal([]byte(out), &page) != nil || len(page.Keys) != 1 || page.Keys[0].ID != strings.Repeat("1", 32) {
		t.Fatalf("printed next-page command lost sort: code=%d, stderr=%s", code, stderr)
	}
	for _, order := range []string{"", "id_asc"} {
		args = []string{"key", "list", "--limit", "1", "--output", "json"}
		if order != "" {
			args = append(args, "--sort", order)
		}
		code, out, stderr = run(args...)
		page = wire.ListKeysResponse{}
		if code != 0 || json.Unmarshal([]byte(out), &page) != nil || len(page.Keys) != 1 || page.Keys[0].ID != strings.Repeat("1", 32) {
			t.Fatalf("default ID order changed: code=%d, stderr=%s", code, stderr)
		}
	}
	code, out, stderr = run("key", "list", "--sort", "created_desc", "--after-id", strings.Repeat("0", 32))
	if code == 0 || out != "" || !strings.Contains(stderr, "invalid argument") {
		t.Fatalf("unknown sorted cursor: code=%d stdout=%q stderr=%q", code, out, stderr)
	}
}

func TestRelayServeShutdownRestoresSignalsBeforeCancel(t *testing.T) {
	for _, trigger := range []string{"signal", "parent"} {
		t.Run(trigger, func(t *testing.T) {
			parentDone := make(chan struct{})
			signals := make(chan os.Signal, 1)
			stop := make(chan struct{})
			restored := make(chan struct{})
			canceled := make(chan struct{})
			orderViolation := make(chan struct{}, 1)
			done := make(chan struct{})

			go relayServeShutdown(parentDone, signals, stop, func() {
				close(restored)
			}, func() {
				select {
				case <-restored:
				default:
					orderViolation <- struct{}{}
				}
				close(canceled)
			}, done)

			if trigger == "signal" {
				signals <- os.Interrupt
			} else {
				close(parentDone)
			}
			select {
			case <-canceled:
			case <-time.After(time.Second):
				t.Fatal("shutdown relay did not cancel")
			}
			<-done
			select {
			case <-orderViolation:
				t.Fatal("serve context canceled before signal behavior was restored")
			default:
			}
		})
	}
}

func TestMergeCleanupError(t *testing.T) {
	serveErr := errors.New("serve failed")
	closeErr := errors.New("close failed")
	tests := []struct {
		name       string
		initial    error
		cleanupErr error
		wantServe  bool
		wantClose  bool
	}{
		{name: "success"},
		{name: "serve only", initial: serveErr, wantServe: true},
		{name: "close only", cleanupErr: closeErr, wantClose: true},
		{name: "both", initial: serveErr, cleanupErr: closeErr, wantServe: true, wantClose: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.initial
			calls := 0
			mergeCleanupError(&got, "close broker", func() error {
				calls++
				return tc.cleanupErr
			})
			if calls != 1 {
				t.Fatalf("cleanup calls = %d, want 1", calls)
			}
			if errors.Is(got, serveErr) != tc.wantServe || errors.Is(got, closeErr) != tc.wantClose {
				t.Fatalf("merged error = %v; serve=%v close=%v", got, errors.Is(got, serveErr), errors.Is(got, closeErr))
			}
			if tc.cleanupErr != nil && !strings.Contains(got.Error(), "close broker") {
				t.Fatalf("cleanup error lacks operation context: %v", got)
			}
		})
	}
}

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

func TestResolveMetrics(t *testing.T) {
	for _, value := range []string{"on", "true", "1", " ON ", "True"} {
		if got, err := resolveMetrics(false, false, value); err != nil || !got {
			t.Errorf("enable %q: got %v, err %v", value, got, err)
		}
	}
	for _, value := range []string{"", "off", "false", "0", " OFF ", "False"} {
		if got, err := resolveMetrics(true, false, value); err != nil || got {
			t.Errorf("disable %q: got %v, err %v", value, got, err)
		}
	}
	for _, value := range []string{" ", "enabled", "disabled", "2", ":9091", "on,off"} {
		if _, err := resolveMetrics(false, false, value); err == nil {
			t.Errorf("invalid metrics setting %q accepted", value)
		}
	}
	for _, env := range []string{"", "on", "off", "invalid"} {
		for _, flag := range []bool{false, true} {
			if got, err := resolveMetrics(flag, true, env); err != nil || got != flag {
				t.Errorf("explicit flag %v with env %q: got %v, err %v", flag, env, got, err)
			}
		}
	}
}

func TestServeMetricsRequiresAuth(t *testing.T) {
	t.Setenv("MQLITE_TOKENS", "off")
	t.Setenv("MQLITE_MONITOR_TOKENS", "")
	t.Setenv("MQLITE_METRICS", "on")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := cmdServe(ctx, []string{"--addr", "127.0.0.1:0"}); err == nil ||
		!strings.Contains(err.Error(), "metrics require administrator authentication") {
		t.Fatalf("enabled metrics without auth must fail before opening DB: %v", err)
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

func TestSubscribeTTLCommand(t *testing.T) {
	ctx := context.Background()
	t.Setenv("MQLITE_ENDPOINT", "")
	t.Setenv("MQLITE_DB", "file:"+filepath.Join(t.TempDir(), "mq.db"))
	if err := cmdCreateSubscription(ctx, []string{"events", "temporary", "--ttl", "1h", "--expr", `subject == "request"`}); err != nil {
		t.Fatal(err)
	}
	if err := cmdCreateSubscription(ctx, []string{"events", "negative", "--ttl", "-1s"}); err == nil {
		t.Fatal("negative subscription TTL accepted")
	}
	if err := cmdCreateSubscription(ctx, []string{"events", "sub-ms", "--ttl", "1ns"}); err == nil {
		t.Fatal("sub-millisecond subscription TTL accepted")
	}
	if err := cmdCreateSubscription(ctx, []string{"events", "durable"}); err != nil {
		t.Fatal(err)
	}

	c, err := dial(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	page, err := c.ListSubscriptionsWithOptions(ctx, mqlite.ListSubscriptionsOptions{Topic: "events"})
	if err != nil || page.Total != 2 || len(page.Subscriptions) != 2 {
		t.Fatalf("inventory = %+v, %v", page, err)
	}
	for _, item := range page.Subscriptions {
		switch item.Name {
		case "temporary":
			if item.ExpiresAtMs-item.CreatedAtMs != time.Hour.Milliseconds() || item.Expr != `subject == "request"` {
				t.Fatalf("temporary = %+v", item)
			}
		case "durable":
			if item.ExpiresAtMs != 0 {
				t.Fatalf("durable = %+v", item)
			}
		default:
			t.Fatalf("unexpected subscription = %+v", item)
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

// TestParityCommands exercises the API-parity commands added in MQLITE-92 (schedule,
// cancel, receive-deferred, status, list-subscriptions, test-filter, --output json) on the
// embedded path. Settle-later is covered separately by the local-command live-lease test
// and against a real broker in TestRemoteSettleByToken.
func TestParityCommands(t *testing.T) {
	ctx := context.Background()
	t.Setenv("MQLITE_ENDPOINT", "")
	t.Setenv("MQLITE_DB", "file:"+filepath.Join(t.TempDir(), "mq.db"))
	t.Cleanup(func() { gOutput = "text" }) // package var — don't leak json to other tests

	ok := func(name string, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	ok("create-queue", cmdCreateQueue(ctx, []string{"orders"}))
	ok("subscribe", cmdCreateSubscription(ctx, []string{"events", "subA", "--expr", `subject == "x"`}))

	// Embedded status must exclude subscription backing queues from the queue count, so it
	// agrees with what the broker's /status reports for the same DB (queues=1, subs=1).
	if s := embStatus(t, ctx); s.Queues != 1 || s.Subscriptions != 1 {
		t.Errorf("embedded status counts = queues %d subs %d, want 1/1 (backing queue excluded)", s.Queues, s.Subscriptions)
	}

	// status / list / list-subscriptions / test-filter (both branches).
	ok("status", cmdStatus(ctx, nil))
	ok("list-subs", cmdListSubscriptions(ctx, nil))
	ok("test-filter-valid", cmdTestFilter(ctx, []string{`subject == "x"`}))
	ok("test-filter-sample", cmdTestFilter(ctx, []string{`subject == "x"`, "--subject", "x"}))
	if err := cmdTestFilter(ctx, []string{`subject ==`}); err == nil {
		t.Error("test-filter with a bad expression must error")
	}

	// schedule then cancel by the seq the API hands back. Each api call opens+closes the
	// embedded engine on its own — a second open while one is held would ErrDBLocked.
	sSeq := apiSchedule(t, ctx, "orders")
	ok("schedule", cmdSchedule(ctx, []string{"orders", "later", "--at", "30m"}))
	ok("cancel", cmdCancel(ctx, []string{"orders", strconv.FormatInt(sSeq, 10)}))

	// receive-deferred: send + receive + defer (persists as deferred), then fetch it by seq.
	dSeq := apiDeferOne(t, ctx, "orders")
	ok("receive-deferred", cmdReceiveDeferred(ctx, []string{"orders", "--seq", strconv.FormatInt(dSeq, 10)}))

	// --output json branch — pass the flag so newFlags actually sets gOutput (assigning it
	// directly would be overwritten by StringVar's "text" default on the next Parse).
	ok("list-json", cmdList(ctx, []string{"--output", "json"}))
	ok("metrics-json", cmdMetrics(ctx, []string{"orders", "--output", "json"}))
	ok("status-json", cmdStatus(ctx, []string{"--output", "json"}))
	ok("peek-json", cmdPeek(ctx, []string{"orders", "--output", "json"}))
	// test-filter in json mode must still exit nonzero on an invalid expression.
	if err := cmdTestFilter(ctx, []string{`subject ==`, "--output", "json"}); err == nil {
		t.Error("test-filter --output json with a bad expr should still error")
	}
	// an unsupported --output value is a usage error, not a silent fall-through.
	if err := cmdList(ctx, []string{"--output", "jsno"}); err == nil {
		t.Error("invalid --output should be a usage error")
	}

	// settlement wrapper: a bogus token cannot own the preserved lock, but exercises
	// cmdSettle end to end; usage errors must not panic.
	if err := cmdSettle(ctx, "complete", []string{"orders", "1", "no-such-token"}); err == nil {
		t.Error("complete with a bogus token should error")
	}
	for name, err := range map[string]error{
		"schedule/none":  cmdSchedule(ctx, nil),
		"schedule/no-at": cmdSchedule(ctx, []string{"orders", "body"}),
		"cancel/none":    cmdCancel(ctx, []string{"orders"}),
		"cancel/badseq":  cmdCancel(ctx, []string{"orders", "notnum"}),
		"complete/short": cmdSettle(ctx, "complete", []string{"orders", "1"}),
		"rdeferred/none": cmdReceiveDeferred(ctx, []string{"orders"}),
		"testfilter/non": cmdTestFilter(ctx, nil),
	} {
		if err == nil {
			t.Errorf("%s: expected a usage error, got nil", name)
		}
	}
}

// embStatus reads the embedded backend status (open+close so no lock is held after).
func embStatus(t *testing.T, ctx context.Context) mqlite.StatusInfo {
	t.Helper()
	c, err := dial(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	s, err := c.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// apiSchedule schedules a message and returns its seq, opening+closing the embedded
// engine so no lock is held when the CLI command dials next.
func apiSchedule(t *testing.T, ctx context.Context, queue string) int64 {
	t.Helper()
	c, err := dial(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	seq, err := c.SendOne(ctx, queue, mqlite.OutMessage{Body: []byte("s")}, mqlite.SendOpts{At: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	return seq
}

// apiDeferOne sends a message, receives it, and Defers it (persists as deferred), then
// returns its seq — again opening+closing so the CLI can dial afterward.
func apiDeferOne(t *testing.T, ctx context.Context, queue string) int64 {
	t.Helper()
	c, err := dial(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	seq, err := c.SendOne(ctx, queue, mqlite.OutMessage{Body: []byte("d")})
	if err != nil {
		t.Fatal(err)
	}
	msgs, err := c.Receive(ctx, queue, mqlite.RecvOpts{Max: 10})
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range msgs {
		if m.SequenceNumber == seq {
			if err := m.Defer(ctx); err != nil {
				t.Fatal(err)
			}
			return seq
		}
	}
	t.Fatalf("seq %d not received to defer", seq)
	return 0
}

// TestCLIParseHelpers covers the small argument parsers.
func TestCLIParseHelpers(t *testing.T) {
	// A future RFC3339 or duration is accepted; a past time or garbage is rejected (P2-1).
	future := time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339)
	if _, err := parseWhen(future); err != nil {
		t.Errorf("future RFC3339: %v", err)
	}
	if w, err := parseWhen("30m"); err != nil || w.IsZero() {
		t.Errorf("duration: %v", err)
	}
	if _, err := parseWhen("2000-01-01T00:00:00Z"); err == nil {
		t.Error("a past --at must be rejected")
	}
	if _, err := parseWhen("-30m"); err == nil {
		t.Error("a negative duration --at must be rejected")
	}
	if _, err := parseWhen("nonsense"); err == nil {
		t.Error("garbage --at must error")
	}
	if seqs, err := parseSeqCSV(" 42, 57 ,8"); err != nil || len(seqs) != 3 || seqs[1] != 57 {
		t.Errorf("parseSeqCSV = %v, %v", seqs, err)
	}
	if _, err := parseSeqCSV("42,x"); err == nil {
		t.Error("non-numeric seq must error")
	}
	if props, err := parseProps("a=1,b=2"); err != nil || props["a"] != "1" || props["b"] != "2" {
		t.Errorf("parseProps = %v, %v", props, err)
	}
	if _, err := parseProps("bad"); err == nil {
		t.Error("prop without = must error")
	}
	if props, _ := parseProps(""); props != nil {
		t.Error("empty --prop should be nil")
	}
}

func TestServeDiagnosticLogRejectsInvalidDestinationBeforeOpeningDatabase(t *testing.T) {
	for _, flag := range []bool{false, true} {
		t.Run(strconv.FormatBool(flag), func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "must-not-open.db")
			t.Setenv("MQLITE_DB", "file:"+dbPath)
			t.Setenv("MQLITE_TOKENS", "off")
			t.Setenv("MQLITE_MONITOR_TOKENS", "")
			t.Setenv("MQLITE_METRICS", "off")
			invalid := filepath.Join(t.TempDir(), "missing", "diagnostics.jsonl")
			args := []string{"--addr", "127.0.0.1:0"}
			if flag {
				t.Setenv("MQLITE_DIAGNOSTIC_LOG", "")
				args = append(args, "--diagnostic-log", invalid)
			} else {
				t.Setenv("MQLITE_DIAGNOSTIC_LOG", invalid)
			}
			if err := cmdServe(context.Background(), args); err == nil || !strings.Contains(err.Error(), "open diagnostic log") {
				t.Fatalf("startup error=%v", err)
			}
			if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
				t.Fatalf("database opened before validation: %v", err)
			}
		})
	}
}

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

func TestStatusDisplaysLifecycleClock(t *testing.T) {
	embeddedEnv(t)
	ctx := context.Background()
	human, err := captureStdout(t, func() error { return cmdStatus(ctx, nil) })
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(human, "broker time:   ") || !strings.Contains(human, "Z\n") {
		t.Fatalf("human status omitted UTC broker clock:\n%s", human)
	}

	encoded, err := captureStdout(t, func() error { return cmdStatus(ctx, []string{"--output", "json"}) })
	if err != nil {
		t.Fatal(err)
	}
	var status mqlite.StatusInfo
	if err := json.Unmarshal([]byte(encoded), &status); err != nil {
		t.Fatalf("status JSON: %v\n%s", err, encoded)
	}
	if status.ServerTimeMs <= 0 {
		t.Fatalf("status JSON lifecycle clock=%d, want positive epoch milliseconds", status.ServerTimeMs)
	}
}

func TestDeleteQueueCommandReportsAcceptedState(t *testing.T) {
	resetGlobals(t)
	ctx := context.Background()
	db := "file:" + filepath.Join(t.TempDir(), "mq.db")
	t.Setenv("MQLITE_ENDPOINT", "")
	t.Setenv("MQLITE_DB", db)

	embedded, err := mqlite.OpenEmbedded(ctx, db, mqlite.WithoutBackground())
	if err != nil {
		t.Fatal(err)
	}
	if err := embedded.CreateQueue(ctx, "empty", mqlite.QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	if err := embedded.CreateQueue(ctx, "busy", mqlite.QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	backlog := make([]mqlite.OutMessage, 5000)
	for i := range backlog {
		backlog[i].Body = []byte("x")
	}
	if _, err := embedded.Send(ctx, "busy", backlog...); err != nil {
		t.Fatal(err)
	}
	if err := embedded.Close(); err != nil {
		t.Fatal(err)
	}

	empty, err := captureStdout(t, func() error {
		return cmdDeleteQueue(ctx, []string{"empty", "--output", "json"})
	})
	if err != nil {
		t.Fatal(err)
	}
	var emptyResult map[string]any
	if err := json.Unmarshal([]byte(empty), &emptyResult); err != nil ||
		!reflect.DeepEqual(emptyResult, map[string]any{"name": "empty", "deleted": true, "state": "deleted"}) {
		t.Fatalf("empty delete = %q (%v), want deleted acknowledgement", empty, err)
	}
	if err := cmdDeleteQueue(ctx, []string{"busy"}); !errors.Is(err, mqlite.ErrQueueNotEmpty) {
		t.Fatalf("non-force delete = %v, want ErrQueueNotEmpty", err)
	}
	pending, err := captureStdout(t, func() error {
		return cmdDeleteQueue(ctx, []string{"busy", "--force", "--output", "json"})
	})
	if err != nil {
		t.Fatal(err)
	}
	var pendingResult map[string]any
	if err := json.Unmarshal([]byte(pending), &pendingResult); err != nil ||
		!reflect.DeepEqual(pendingResult, map[string]any{"name": "busy", "deleted": false, "state": "deleting"}) {
		t.Fatalf("force delete = %q (%v), want accepted pending acknowledgement", pending, err)
	}
}

func TestDeleteSubscriptionCommandReportsAcceptedState(t *testing.T) {
	resetGlobals(t)
	ctx := context.Background()
	db := "file:" + filepath.Join(t.TempDir(), "mq.db")
	t.Setenv("MQLITE_ENDPOINT", "")
	t.Setenv("MQLITE_DB", db)

	embedded, err := mqlite.OpenEmbedded(ctx, db, mqlite.WithoutBackground())
	if err != nil {
		t.Fatal(err)
	}
	if err := embedded.Subscribe(ctx, "empty-topic", "empty-sub", nil); err != nil {
		t.Fatal(err)
	}
	if err := embedded.Subscribe(ctx, "busy-topic", "busy-sub", nil); err != nil {
		t.Fatal(err)
	}
	backlog := make([]mqlite.OutMessage, 5000)
	for i := range backlog {
		backlog[i].Body = []byte("x")
	}
	if _, err := embedded.Send(ctx, "busy-topic", backlog...); err != nil {
		t.Fatal(err)
	}
	if err := embedded.Close(); err != nil {
		t.Fatal(err)
	}

	empty, err := captureStdout(t, func() error {
		return cmdDeleteSubscription(ctx, []string{"empty-topic", "empty-sub", "--output", "json"})
	})
	if err != nil {
		t.Fatal(err)
	}
	var emptyResult map[string]any
	if err := json.Unmarshal([]byte(empty), &emptyResult); err != nil || !reflect.DeepEqual(emptyResult, map[string]any{
		"topic": "empty-topic", "name": "empty-sub", "deleted": true, "state": "deleted",
	}) {
		t.Fatalf("empty subscription delete = %q (%v), want deleted acknowledgement", empty, err)
	}
	if err := cmdDeleteSubscription(ctx, []string{"busy-topic", "busy-sub"}); !errors.Is(err, mqlite.ErrSubscriptionNotEmpty) {
		t.Fatalf("non-force subscription delete = %v, want ErrSubscriptionNotEmpty", err)
	}
	pending, err := captureStdout(t, func() error {
		return cmdDeleteSubscription(ctx, []string{"busy-topic", "busy-sub", "--force", "--output", "json"})
	})
	if err != nil {
		t.Fatal(err)
	}
	var pendingResult map[string]any
	if err := json.Unmarshal([]byte(pending), &pendingResult); err != nil || !reflect.DeepEqual(pendingResult, map[string]any{
		"topic": "busy-topic", "name": "busy-sub", "deleted": false, "state": "deleting",
	}) {
		t.Fatalf("force subscription delete = %q (%v), want accepted pending acknowledgement", pending, err)
	}
}

func TestListSubscriptionsLegacyAndPageJSON(t *testing.T) {
	embeddedEnv(t)
	ctx := context.Background()
	c, err := dial(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]string{{"alpha", "a"}, {"alpha", "b"}, {"beta", "c"}} {
		if err := c.Subscribe(ctx, pair[0], pair[1], nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	legacyJSON, err := captureStdout(t, func() error {
		return cmdListSubscriptions(ctx, []string{"--output", "json"})
	})
	if err != nil {
		t.Fatal(err)
	}
	const wantLegacyJSON = `[
  {
    "topic": "alpha",
    "name": "a"
  },
  {
    "topic": "alpha",
    "name": "b"
  },
  {
    "topic": "beta",
    "name": "c"
  }
]` + "\n"
	if legacyJSON != wantLegacyJSON {
		t.Fatalf("legacy output changed shape:\n%s\nwant:\n%s", legacyJSON, wantLegacyJSON)
	}
	var legacy []mqlite.SubscriptionInfo
	if err := json.Unmarshal([]byte(legacyJSON), &legacy); err != nil || len(legacy) != 3 {
		t.Fatalf("legacy output must remain a full JSON array: %q (%v)", legacyJSON, err)
	}

	pageJSON, err := captureStdout(t, func() error {
		return cmdListSubscriptions(ctx, []string{"--limit", "2", "--output", "json"})
	})
	if err != nil {
		t.Fatal(err)
	}
	var first mqlite.SubscriptionPage
	if err := json.Unmarshal([]byte(pageJSON), &first); err != nil || len(first.Subscriptions) != 2 || first.Total != 3 || first.NextAfter == "" {
		t.Fatalf("first page = %q (%v)", pageJSON, err)
	}
	for _, item := range first.Subscriptions {
		if item.CreatedAtMs <= 0 || item.ExpiresAtMs != 0 {
			t.Fatalf("first page omitted lifetime metadata: %+v", item)
		}
	}
	secondJSON, err := captureStdout(t, func() error {
		return cmdListSubscriptions(ctx, []string{"--after", first.NextAfter, "--limit", "2", "--output", "json"})
	})
	if err != nil {
		t.Fatal(err)
	}
	var second mqlite.SubscriptionPage
	if err := json.Unmarshal([]byte(secondJSON), &second); err != nil || len(second.Subscriptions) != 1 || second.Total != 3 || second.NextAfter != "" {
		t.Fatalf("second page = %q (%v)", secondJSON, err)
	}
	topicJSON, err := captureStdout(t, func() error {
		return cmdListSubscriptions(ctx, []string{"--topic", "alpha", "--output", "json"})
	})
	if err != nil {
		t.Fatal(err)
	}
	var topicPage mqlite.SubscriptionPage
	if err := json.Unmarshal([]byte(topicJSON), &topicPage); err != nil || len(topicPage.Subscriptions) != 2 || topicPage.Total != 2 {
		t.Fatalf("topic page = %q (%v)", topicJSON, err)
	}

	for _, args := range [][]string{{"--limit", "0"}, {"--limit", "1001"}, {"extra"}} {
		if err := cmdListSubscriptions(ctx, args); err == nil {
			t.Errorf("list-subscriptions %v must reject invalid arguments", args)
		}
	}
}

// P2-5: the top-level help must list every dispatched command, so one can't silently vanish.
func TestUsageListsAllCommands(t *testing.T) {
	out, _ := captureStdout(t, func() error { usage(); return nil })
	for _, cmd := range []string{
		"serve", "create-queue", "delete-queue", "subscribe", "delete-subscription", "send", "schedule", "cancel", "receive",
		"receive-deferred", "complete", "abandon", "reject", "defer", "renew", "peek",
		"metrics", "status", "observe", "list", "list-subscriptions", "test-filter", "redrive",
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
		{"delete-queue", cmdDeleteQueue, []string{"q", "extra"}},
		{"subscribe", cmdCreateSubscription, []string{"topic", "sub", "extra"}},
		{"delete-subscription", cmdDeleteSubscription, []string{"topic", "sub", "extra"}},
		{"list-subscriptions", cmdListSubscriptions, []string{"extra"}},
		{"peek", cmdPeek, []string{"q", "extra"}},
		{"metrics", cmdMetrics, []string{"q", "extra"}},
		{"observe", cmdObserve, []string{"extra"}},
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

// viewFields records, for EVERY field of wire.Message, what the CLI's hand-written conversions do
// with it. Returning the wire type guarantees canonical NAMES; it does not guarantee that a field
// added to the wire is actually populated — an unconverted field is emitted as a silent zero,
// which is a quieter kind of wrong than a renamed key (round-4 §5.1).
//
// So every field must be a deliberate decision here. Add one to the wire and this list no longer
// covers it: the test fails, and someone has to choose.
var viewFields = map[string]string{
	"seq_number":              "both",
	"body":                    "both",
	"message_id":              "both",
	"group_id":                "both",
	"correlation_id":          "both",
	"reply_to":                "both",
	"subject":                 "both",
	"content_type":            "both",
	"properties":              "both",
	"delivery_count":          "both",
	"enqueued_at_ms":          "both",
	"locked_until_ms":         "both",
	"expires_at_ms":           "peek only: a delivered message's TTL is not part of the lock",
	"visible_at_ms":           "peek only: a delivered message is by definition visible",
	"state":                   "peek only: a delivered message is, necessarily, locked",
	"dead_letter_reason":      "peek only: DLQ triage; a received message is not dead-lettered",
	"dead_letter_description": "peek only: as above",
	"lock_token":              "receive only, and only with --no-ack: settle-later needs it; an auto-acked batch is already gone",
}

func TestCLIViewsCoverEveryWireField(t *testing.T) {
	rt := reflect.TypeOf(wire.Message{})
	for i := 0; i < rt.NumField(); i++ {
		tag, _, _ := strings.Cut(rt.Field(i).Tag.Get("json"), ",")
		if tag == "" || tag == "-" {
			continue
		}
		if _, ok := viewFields[tag]; !ok {
			t.Errorf("wire.Message has a field %q that the CLI's conversions say nothing about — it would be emitted as a silent zero. Copy it in viewMsg/viewPeeked, or record here why it does not apply.", tag)
		}
	}
	// And the list must not rot in the other direction either.
	wireTags := jsonTagsOf(rt)
	for tag := range viewFields {
		if !wireTags[tag] {
			t.Errorf("viewFields mentions %q, which is not a wire.Message field any more", tag)
		}
	}
}

// viewFields records the DECISION for each wire field; this proves the decision was actually
// carried out. Registering a field in that map and forgetting to copy it in viewMsg/viewPeeked
// would leave it a silent zero — the very failure the map exists to prevent (round-5 §4).
//
// Build a peeked message with every field set to a non-zero value, convert it, and require every
// field marked "both" or "peek" to survive.
func TestPeekViewActuallyCopiesTheFieldsItClaims(t *testing.T) {
	src := &mqlite.PeekedMessage{
		SequenceNumber: 7,
		State:          mqlite.State("dead_lettered"),
		Body:           []byte("b"),
		MessageID:      "mid",
		GroupID:        "gid",
		CorrelationID:  "cid",
		ReplyTo:        "rt",
		Subject:        "subj",
		ContentType:    "ct",
		Properties:     map[string]string{"k": "v"},
		DeliveryCount:  3,
		EnqueuedAt:     time.Unix(1, 0),
		VisibleAt:      time.Unix(2, 0),
		ExpiresAt:      time.Unix(3, 0),
		LockedUntil:    time.Unix(4, 0),

		DeadLetterReason:      "why",
		DeadLetterDescription: "detail",
	}
	got := viewPeeked([]*mqlite.PeekedMessage{src})[0]

	b, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	for tag, decision := range viewFields {
		if strings.HasPrefix(decision, "receive only") {
			continue // peek never locks, so it has no lock token
		}
		if _, ok := m[tag]; !ok {
			t.Errorf("viewFields says peek carries %q (%s), but the conversion left it zero — it is absent from the JSON",
				tag, decision)
		}
	}
}

// The key commands retain the wire shape in both connection modes and only the
// successful create response carries a secret. Lists include revoked metadata.
func TestKeyCommandsJSONContract(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"embedded", "http"} {
		t.Run(mode, func(t *testing.T) {
			embeddedEnv(t)
			if mode == "http" {
				url, _ := bbBroker(t, "administrator")
				t.Setenv("MQLITE_ENDPOINT", url)
				t.Setenv("MQLITE_TOKEN", "administrator")
			}
			run := func(args ...string) string {
				t.Helper()
				out, err := captureStdout(t, func() error { return cmdKey(ctx, append(args, "--output", "json")) })
				if err != nil {
					t.Fatal(err)
				}
				return out
			}
			if out := strings.TrimSpace(run("list")); out != "{\n  \"keys\": []\n}" {
				t.Fatalf("empty page = %q", out)
			}
			ids := []string{strings.Repeat("1", 32), strings.Repeat("2", 32)}
			var secret string
			for i, id := range ids {
				out := run("create", "--name", "worker", "--permissions", "send,listen", "--id", id)
				var result wire.CreateKeyResponse
				if err := json.Unmarshal([]byte(out), &result); err != nil {
					t.Fatal(err)
				}
				if result.Key.ID != id || len(result.Token) != 68 || !strings.HasPrefix(result.Token, "mqk_") || strings.Count(out, result.Token) != 1 {
					t.Fatal("creation must show one secret and the retained public ID")
				}
				var fields map[string]json.RawMessage
				if err := json.Unmarshal([]byte(out), &fields); err != nil {
					t.Fatal(err)
				}
				if len(fields) != 2 || fields["key"] == nil || fields["token"] == nil {
					t.Fatalf("unexpected create response fields: %v", fields)
				}
				var metadata map[string]any
				if err := json.Unmarshal(fields["key"], &metadata); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(jsonTagsOf(reflect.TypeOf(wire.AccessKey{})), fieldSet(metadata)) {
					t.Fatalf("key metadata fields = %v", keysOf(metadata))
				}
				if i == 0 {
					secret = result.Token
				}
			}
			out := run("list", "--limit", "1")
			var page wire.ListKeysResponse
			if err := json.Unmarshal([]byte(out), &page); err != nil {
				t.Fatal(err)
			}
			if len(page.Keys) != 1 || page.Keys[0].ID != ids[0] || page.NextAfterID != ids[0] || strings.Contains(out, secret) || strings.Contains(out, "token") || strings.Contains(out, "hash") {
				t.Fatal("first page must contain public metadata and cursor only")
			}
			out = run("list", "--after-id", page.NextAfterID, "--limit", "1")
			page = wire.ListKeysResponse{}
			if err := json.Unmarshal([]byte(out), &page); err != nil {
				t.Fatal(err)
			}
			if len(page.Keys) != 1 || page.Keys[0].ID != ids[1] || page.NextAfterID != "" {
				t.Fatalf("last page = %+v", page)
			}
			for i, after := range []string{"", ids[1]} {
				out = run("list", "--sort", "created_desc", "--after-id", after, "--limit", "1")
				page = wire.ListKeysResponse{}
				if err := json.Unmarshal([]byte(out), &page); err != nil {
					t.Fatal(err)
				}
				wantNext := ids[1]
				if i == 1 {
					wantNext = ""
				}
				if len(page.Keys) != 1 || page.Keys[0].ID != ids[1-i] || page.NextAfterID != wantNext || strings.Contains(out, secret) {
					t.Fatalf("creation-order page %d = %+v", i, page)
				}
			}
			human, err := captureStdout(t, func() error {
				return cmdKey(ctx, []string{"list", "--sort", "created_desc", "--limit", "1", "--output", "text"})
			})
			if err != nil || !strings.Contains(human, "Next page: key list --after-id "+ids[1]+" --limit 1 --sort created_desc\n") {
				t.Fatalf("human continuation lost creation order: %q, %v", human, err)
			}
			for i := 0; i < 2; i++ {
				out = run("revoke", "--id", ids[0])
				if strings.TrimSpace(out) != "{\n  \"ok\": true\n}" {
					t.Fatalf("revoke JSON = %q", out)
				}
			}
			out = run("list")
			if err := json.Unmarshal([]byte(out), &page); err != nil {
				t.Fatal(err)
			}
			if len(page.Keys) != 2 || page.Keys[0].RevokedAtMs == 0 || strings.Contains(out, secret) {
				t.Fatal("revoked key must remain listed without its secret")
			}
		})
	}
}

func fieldSet(fields map[string]any) map[string]bool {
	out := make(map[string]bool, len(fields))
	for name := range fields {
		out[name] = true
	}
	return out
}

func TestKeyCommandsHumanOutputAndErrors(t *testing.T) {
	embeddedEnv(t)
	ctx := context.Background()
	create := []string{"create", "--name", "human", "--permissions", "manage"}
	out, err := captureStdout(t, func() error { return cmdKey(ctx, create) })
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 4 || !strings.HasPrefix(lines[0], "Key ID: ") || !strings.HasPrefix(lines[3], "Token (save now; shown only once): mqk_") {
		t.Fatalf("human create = %q", out)
	}
	id := strings.TrimPrefix(lines[0], "Key ID: ")
	secret := strings.TrimPrefix(lines[3], "Token (save now; shown only once): ")
	if len(id) != 32 || len(secret) != 68 || strings.Count(out, secret) != 1 {
		t.Fatal("generated ID and one-time secret must be distinguishable")
	}
	out, err = captureStdout(t, func() error { return cmdKey(ctx, []string{"list", "--limit", "1"}) })
	if err != nil || !strings.Contains(out, id) || strings.Contains(out, secret) {
		t.Fatalf("human list err = %v", err)
	}
	out, err = captureStdout(t, func() error { return cmdKey(ctx, []string{"revoke", "--id", id}) })
	if err != nil || strings.TrimSpace(out) != "Revoked key ID: "+id {
		t.Fatalf("human revoke = %q, %v", out, err)
	}
	for _, args := range [][]string{
		nil, {"unknown"}, {"create"}, {"create", "--name", "x", "--permissions", "manage", "extra"},
		{"create", "--name", "x", "--permissions", "read"}, {"create", "--name", "x", "--permissions", "send", "--id="},
		{"create", "--name", "x", "--permissions", "send", "--id", secret},
		{"create", "--name", "x", "--permissions", "send", "--expires-at-ms", "1"},
		{"list", "extra"}, {"list", "--after-id", "bad"}, {"list", "--limit", "1001"},
		{"list", "--sort", "newest"}, {"list", "--sort", ""}, {"list", "--sort", secret},
		{"revoke"}, {"revoke", "--id", "bad"}, {"revoke", "--id", id, "extra"},
	} {
		_, err := captureStdout(t, func() error { return cmdKey(ctx, args) })
		if err == nil {
			t.Fatalf("args %v should fail", args)
		}
		if strings.Contains(err.Error(), secret) {
			t.Fatal("a command failure must not disclose an existing secret")
		}
	}
	// A failed request with a caller-supplied ID must preserve that ID in the
	// error, so an operator can list/revoke a key after losing its response.
	t.Setenv("MQLITE_ENDPOINT", "://bad")
	_, err = captureStdout(t, func() error { return cmdKey(ctx, append(create, "--id", id)) })
	if err == nil || !strings.Contains(err.Error(), id) {
		t.Fatalf("failed create did not retain public ID: %v", err)
	}
}

func TestKeyCreateLostOutputRetainsPublicID(t *testing.T) {
	embeddedEnv(t)
	ctx := context.Background()
	id := strings.Repeat("b", 32)
	file, err := os.CreateTemp(t.TempDir(), "closed-output-")
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = file
	defer func() { os.Stdout = old }()
	err = cmdKey(ctx, []string{"create", "--id", id, "--name", "lost-output", "--permissions", "manage", "--output", "json"})
	os.Stdout = old
	if err == nil || !strings.Contains(err.Error(), id) || strings.Contains(err.Error(), "mqk_") {
		t.Fatalf("output failure must retain public ID only: %v", err)
	}
	c, err := dial(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	page, err := c.ListKeys(ctx, "", 0)
	if err != nil || len(page.Keys) != 1 || page.Keys[0].ID != id {
		t.Fatalf("committed key must remain identifiable after output failure: %+v, %v", page, err)
	}
	if err := c.RevokeKey(ctx, id); err != nil {
		t.Fatal(err)
	}
}

func TestKeyCreateLostResponseRetainsGeneratedID(t *testing.T) {
	resetGlobals(t)
	ids := make(chan string, 10)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req wire.CreateKeyRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		ids <- req.ID
		_, _ = w.Write([]byte(`{"key":`))
	}))
	defer ts.Close()
	t.Setenv("MQLITE_ENDPOINT", ts.URL)
	t.Setenv("MQLITE_TOKEN", "admin")
	out, err := captureStdout(t, func() error {
		return cmdKey(context.Background(), []string{"create", "--name", "uncertain", "--permissions", "send", "--output", "json"})
	})
	calls := len(ids)
	if calls != 1 {
		t.Fatalf("create requests = %d, want 1", calls)
	}
	id := <-ids
	if err == nil || len(id) != 32 || !strings.Contains(err.Error(), id) || out != "" {
		t.Fatalf("lost response: ID=%q, calls=%d, output bytes=%d, error=%v", id, calls, len(out), err)
	}
}

// Observe always emits the canonical response, including explicit unavailable domains.
func TestCLIObserveContract(t *testing.T) {
	embeddedEnv(t)
	for _, args := range [][]string{nil, {"--output", "json"}} {
		out, err := captureStdout(t, func() error { return cmdObserve(context.Background(), args) })
		if err != nil {
			t.Fatal(err)
		}
		value, err := wire.DecodeObserveResponse([]byte(out))
		if err != nil {
			t.Fatal(err)
		}
		if value.Access != "embedded" || value.HTTP.State != "not_applicable" || value.Collection.State != "available" {
			t.Fatalf("unexpected canonical response: %s", out)
		}
	}
	for _, tc := range []struct {
		env, admins string
		valid       bool
	}{
		{"", "", true}, {" monitor-a, monitor-b ", "admin", true}, {", ,", "admin", false},
		{"monitor", "", false}, {"admin", "admin", false},
	} {
		tokens, err := resolveMonitorTokens(tc.env, tc.admins)
		if (err == nil) != tc.valid {
			t.Fatalf("monitor configuration validity = %v, want %v", err, tc.valid)
		}
		if err == nil && tc.env != "" && len(tokens) != 2 {
			t.Fatalf("parsed monitors: %d", len(tokens))
		}
	}
}

// resetGlobals restores the package-level flag state between tests.
func resetGlobals(t *testing.T) {
	t.Cleanup(func() {
		gEndpoint, gToken, gOutput = "", "", "text"
		gEndpointSet, gTokenSet = false, false
	})
}

// A stopped broker's database may be opened by local inspection or maintenance
// commands. Those commands must not revoke a still-live remote delivery merely
// because they are the next process to open the file.
func TestLocalCommandsPreserveLiveBrokerLease(t *testing.T) {
	commands := []struct {
		name string
		run  func(context.Context) error
	}{
		{name: "status", run: func(ctx context.Context) error { return cmdStatus(ctx, nil) }},
		{name: "list", run: func(ctx context.Context) error { return cmdList(ctx, nil) }},
		{name: "peek", run: func(ctx context.Context) error { return cmdPeek(ctx, []string{"q"}) }},
		{name: "metrics", run: func(ctx context.Context) error { return cmdMetrics(ctx, []string{"q"}) }},
		{name: "key-list", run: func(ctx context.Context) error { return cmdKey(ctx, []string{"list"}) }},
		{name: "vacuum", run: func(ctx context.Context) error { return cmdVacuum(ctx, nil) }},
	}

	for _, tc := range commands {
		t.Run(tc.name, func(t *testing.T) {
			resetGlobals(t)
			ctx := context.Background()
			dsn := "file:" + filepath.Join(t.TempDir(), "broker.db")
			t.Setenv("MQLITE_DB", dsn)
			t.Setenv("MQLITE_ENDPOINT", "")

			broker, err := mqlite.OpenEmbedded(ctx, dsn, mqlite.WithBrokerMode(), mqlite.WithoutBackground())
			if err != nil {
				t.Fatal(err)
			}
			if err := broker.CreateQueue(ctx, "q", mqlite.QueueConfig{
				LockDuration: time.Minute, MaxDeliveryCount: 1,
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := broker.SendOne(ctx, "q", mqlite.OutMessage{Body: []byte("in-flight")}); err != nil {
				t.Fatal(err)
			}
			claimed, err := broker.Receive(ctx, "q")
			if err != nil || len(claimed) != 1 {
				t.Fatalf("claim = %+v, %v", claimed, err)
			}
			seq, token := claimed[0].SequenceNumber, claimed[0].LockToken()
			if err := broker.Close(); err != nil {
				t.Fatal(err)
			}

			if err := tc.run(ctx); err != nil {
				t.Fatalf("local command: %v", err)
			}

			reopened, err := mqlite.OpenEmbedded(ctx, dsn, mqlite.WithBrokerMode(), mqlite.WithoutBackground())
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if err := reopened.Message("q", seq, token).Complete(ctx); err != nil {
				t.Fatalf("complete preserved delivery: %v", err)
			}
			stats, err := reopened.Stats(ctx, "q")
			if err != nil || stats.Total != 0 || stats.DeadLettered != 0 {
				t.Fatalf("stats after complete = %+v, %v", stats, err)
			}
		})
	}
}

func TestCreateQueueRejectsInvalidLockBeforeDial(t *testing.T) {
	t.Run("local database", func(t *testing.T) {
		resetGlobals(t)
		dbPath := filepath.Join(t.TempDir(), "must-not-exist.db")
		t.Setenv("MQLITE_ENDPOINT", "")
		t.Setenv("MQLITE_DB", "file:"+dbPath)

		for _, value := range []string{"-1s", "500ms", "2h"} {
			err := cmdCreateQueue(context.Background(), []string{"q", "--lock", value})
			if !errors.Is(err, mqlite.ErrInvalidArgument) || !strings.Contains(err.Error(), "between 1s and 1h") {
				t.Errorf("--lock %s error = %v, want clear ErrInvalidArgument", value, err)
			}
		}
		for _, path := range []string{dbPath, dbPath + ".lock", dbPath + "-wal", dbPath + "-shm"} {
			if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("invalid --lock touched %s: %v", path, statErr)
			}
		}
	})

	t.Run("remote broker", func(t *testing.T) {
		resetGlobals(t)
		var calls atomic.Int32
		ts := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			calls.Add(1)
		}))
		defer ts.Close()
		t.Setenv("MQLITE_ENDPOINT", ts.URL)

		err := cmdCreateQueue(context.Background(), []string{"q", "--lock", "2h"})
		if !errors.Is(err, mqlite.ErrInvalidArgument) {
			t.Fatalf("invalid remote --lock error = %v, want ErrInvalidArgument", err)
		}
		if got := calls.Load(); got != 0 {
			t.Fatalf("invalid --lock made %d remote requests, want none", got)
		}
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

// A slow settlement must not outlive its Peek-Lock lease. Synchronize the slow
// CompleteBatch with the first successful renewal so this test exercises renewal
// during settlement rather than racing the renew goroutine's first scheduling on
// a loaded runner.
func TestRenewalSpansSettlement(t *testing.T) {
	resetGlobals(t)
	ctx := context.Background()
	const (
		lockDuration = 5 * time.Second
		settleDelay  = 6 * time.Second
	)
	// Production leases use epoch timestamps, but a host wall-clock correction is
	// unrelated to this delayed-settlement contract. Keep an epoch value while the
	// test advances from Go's monotonic clock, as the larger batch test below does.
	clockOrigin := time.Now()
	stableEpoch := func() int64 {
		return clockOrigin.UnixMilli() + time.Since(clockOrigin).Milliseconds()
	}
	// Background loops ON: the reaper is what reclaims an expired lock.
	eng, err := engine.Open(ctx, engine.Options{
		DB:  "file:" + filepath.Join(t.TempDir(), "mq.db"),
		Now: stableEpoch,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	if err := eng.CreateQueue(ctx, "q", engine.QueueConfig{LockDurationMs: lockDuration.Milliseconds()}); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.SendOne(ctx, "q", engine.OutMessage{Body: []byte("slow-link")}); err != nil {
		t.Fatal(err)
	}

	inner := server.New(eng, nil).Handler()
	firstRenewed := make(chan struct{})
	var firstRenewedOnce sync.Once
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case wire.PathRenewBatch:
			// Observe a committed, successful renewal before allowing the delayed
			// settlement to start its lease-spanning wait. Recording also lets the
			// barrier distinguish HTTP success from a failed renewal request.
			recorded := httptest.NewRecorder()
			inner.ServeHTTP(recorded, r)
			var response wire.RenewBatchResponse
			if recorded.Code == http.StatusOK &&
				json.Unmarshal(recorded.Body.Bytes(), &response) == nil &&
				len(response.Results) == 1 && response.Results[0].Ok {
				firstRenewedOnce.Do(func() { close(firstRenewed) })
			}
			for name, values := range recorded.Header() {
				w.Header()[name] = append([]string(nil), values...)
			}
			w.WriteHeader(recorded.Code)
			_, _ = w.Write(recorded.Body.Bytes())
			return
		case wire.PathCompleteBatch:
			select {
			case <-firstRenewed:
			case <-r.Context().Done():
				return
			case <-time.After(15 * time.Second):
				http.Error(w, "timed out waiting for the first successful lease renewal", http.StatusGatewayTimeout)
				return
			}
			// The first renewal alone expires before this wait ends. Complete can
			// succeed only if renewal continues while its RPC remains in flight.
			time.Sleep(settleDelay)
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
			// Production leases deliberately use absolute epoch timestamps so fencing remains
			// meaningful across processes and restarts. A forward host wall-clock step can
			// therefore expire existing broker locks, but that is not the delayed-link behavior
			// under test here. Derive this engine's epoch from Go's monotonic clock so such a
			// step cannot invalidate the fixture mid-run.
			clockOrigin := time.Now()
			stableEpoch := func() int64 {
				return clockOrigin.UnixMilli() + time.Since(clockOrigin).Milliseconds()
			}
			eng, err := engine.Open(ctx, engine.Options{
				DB:  "file:" + filepath.Join(t.TempDir(), "mq.db"),
				Now: stableEpoch,
			})
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
				t.Fatalf("receive of %d messages over a delayed link must settle cleanly: %v (renew requests=%d, settlement requests=%d)",
					n, err, renews.Load(), settles.Load())
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
	//
	// The assertion is "0, or strictly less than what remains", and that is deliberate: a loaded
	// runner can deschedule this goroutine between constructing the deadline and pace() reading the
	// clock, at which point the lease really IS spent and 0 ("renew now") is the correct answer.
	// Demanding a positive delay would make the test fail for being right (codex).
	for _, remaining := range []time.Duration{
		149 * time.Millisecond, 100 * time.Millisecond, 60 * time.Millisecond,
		30 * time.Millisecond, 10 * time.Millisecond, 2 * time.Millisecond,
		// Sub-millisecond headroom: the spin guard must NOT round the delay up past the deadline.
		900 * time.Microsecond, 500 * time.Microsecond, 100 * time.Microsecond,
	} {
		d := pace(time.Now().Add(remaining))
		if d != 0 && d >= remaining {
			t.Errorf("pace(%s) = %s — the renewal would be scheduled at or AFTER the deadline it is meant to save",
				remaining, d)
		}
	}
	// A lease with real headroom must still be SCHEDULED, not renewed on the spot — otherwise the
	// "0" answer above would be hiding a renewal storm.
	if d := pace(time.Now().Add(5 * time.Second)); d <= 0 || d >= 5*time.Second {
		t.Errorf("pace(5s) = %s, want a positive delay well inside the lease", d)
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

// A renewal that fails TRANSIENTLY must not throw away a lease that is still good. Replacing a
// live deadline with "unknown" drops the renewer onto the unknown-deadline backoff, which can
// easily outlast what the lease had left: a failed attempt a third of the way into a one-second
// lock would put the next one 1.33s in — well after the reaper took it (codex).
func TestFailedRenewalKeepsAStillValidLease(t *testing.T) {
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
	// A broker that fails every renewal with a transient 500 — the route exists, so this is NOT the
	// "unsupported, downgrade" path: it is an ordinary, retryable failure.
	flaky := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == wire.PathRenewBatch || r.URL.Path == wire.PathRenew {
			http.Error(w, `{"code":"internal","message":"transient"}`, http.StatusInternalServerError)
			return
		}
		inner.ServeHTTP(w, r)
	}))
	defer flaky.Close()

	c, err := mqlite.Open(ctx, flaky.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	msgs, err := c.Receive(ctx, "q", mqlite.RecvOpts{Max: 1})
	if err != nil || len(msgs) != 1 {
		t.Fatalf("receive: %v n=%d", err, len(msgs))
	}

	renew := renewFunc(c, "q", msgs, 30*time.Second)
	if got := renew(ctx); !got.IsZero() {
		t.Fatalf("a failing renewal must report no deadline, got %v", got)
	}
	// The renewer keeps what it already knew: the 30s lease is still good, so the cadence must stay
	// paced by IT, not collapse to the unknown-deadline retry.
	held := msgs[0].LockedUntil
	if d := pace(held); d < 8*time.Second || d > 11*time.Second {
		t.Errorf("cadence after a failed renewal = %s, want about a third of the lease we still hold", d)
	}
	if pace(time.Time{}) != renewRetryInterval {
		t.Fatal("sanity: an unknown deadline should use the retry cadence")
	}
}

// A denied batch route is an authorization failure, not evidence of an old
// broker. Neither settlement nor renewal may silently fall back to single RPCs.
func TestBatchPermissionDeniedNeverFallsBack(t *testing.T) {
	resetGlobals(t)
	ctx := context.Background()
	var completeBatch, complete, renewBatch, renew atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case wire.PathCompleteBatch:
			completeBatch.Add(1)
		case wire.PathComplete:
			complete.Add(1)
		case wire.PathRenewBatch:
			renewBatch.Add(1)
		case wire.PathRenew:
			renew.Add(1)
		default:
			t.Errorf("unexpected request path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"code":"permission_denied","message":"listen permission required"}`))
	}))
	defer ts.Close()
	c, err := mqlite.Open(ctx, ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	msg := c.Message("q", 1, "lease-token")
	if _, err := c.CompleteBatch(ctx, "q", msg); !errors.Is(err, mqlite.ErrPermissionDenied) {
		t.Fatalf("batch settlement = %v", err)
	}
	if got := renewFunc(c, "q", []*mqlite.Message{msg}, time.Minute)(ctx); !got.IsZero() {
		t.Fatal("denied renewal must not invent a renewed lease")
	}
	if completeBatch.Load() != 1 || renewBatch.Load() != 1 || complete.Load() != 0 || renew.Load() != 0 {
		t.Fatalf("RPC counts: completeBatch=%d complete=%d renewBatch=%d renew=%d", completeBatch.Load(), complete.Load(), renewBatch.Load(), renew.Load())
	}
}

func TestRedactRemoteDatabaseDSN(t *testing.T) {
	for _, scheme := range []string{"libsql", "turso", "ws", "wss", "LIBSQL", "TURSO"} {
		for _, query := range []string{
			"authToken=synthetic-token-marker",
			"auth_token=synthetic-token-marker",
			"jwt=synthetic-token-marker",
			"tls=0&auth_token=synthetic-token-marker&other=synthetic-query-marker",
			"auth%54oken=synthetic-token-marker",
			"unknown=synthetic-query-marker",
			"",
		} {
			t.Run(scheme+"/"+strings.Split(query, "=")[0], func(t *testing.T) {
				dsn := scheme + "://synthetic-user-marker:synthetic-password-marker@db.example.invalid:443/database/path?" + query + "#synthetic-fragment-marker"
				got := redact(dsn)
				want := strings.ToLower(scheme) + "://db.example.invalid:443/database/path"
				if got != want {
					t.Errorf("redacted remote DSN = %q, want scheme, host and path only", got)
				}
				for _, marker := range []string{"synthetic-user-marker", "synthetic-password-marker", "synthetic-token-marker", "synthetic-query-marker", "synthetic-fragment-marker", "?", "#", "@"} {
					if strings.Contains(got, marker) {
						t.Errorf("remote DSN retained forbidden field %q", marker)
					}
				}
			})
		}
	}
}

func TestRedactInvalidRemoteDatabaseDSN(t *testing.T) {
	for _, dsn := range []string{
		"libsql://db.example.invalid/%zz?jwt=synthetic-token-marker",
		"turso://db.example.invalid/path?auth_token=%zz",
		"ws://synthetic-user-marker:%zz@db.example.invalid/path",
		"wss://db.example.invalid/path#%zz",
		"turso://db.example.invalid/path\n?jwt=synthetic-token-marker",
		"libsql://db.example.invalid/path?jwt=synthetic-token-marker\x00",
		"wss://db.example.invalid:invalid/path?jwt=synthetic-token-marker",
		"libsql:///missing-host?jwt=synthetic-token-marker",
		"turso:opaque?jwt=synthetic-token-marker",
	} {
		if got := redact(dsn); got != "[redacted remote database]" {
			t.Errorf("invalid remote DSN did not fail closed: %q", got)
		}
	}
}

func TestRedactKeepsLocalDatabaseLocation(t *testing.T) {
	for _, dsn := range []string{":memory:", "file:./mq.db", "file:./mq.db?_pragma=journal_mode(WAL)", "/tmp/queue.db", "relative queue.db"} {
		if got := redact(dsn); got != dsn {
			t.Errorf("local DSN = %q, want unchanged %q", got, dsn)
		}
	}
}
