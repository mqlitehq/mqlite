package main

// Process-level black-box tests: build the real `mqlite` binary and run it as a subprocess
// so we can exercise things in-process tests cannot — a broken stdout pipe, exit codes, the
// `--` terminator end to end, and an explicit empty --token (MQLITE-93 / review 2026-07-12).

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/server"
	"github.com/mqlitehq/mqlite/wire"
)

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
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	_ = w.Close() // parent's write end
	_ = r.Close() // no reader → the child's stdout write breaks the pipe
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
