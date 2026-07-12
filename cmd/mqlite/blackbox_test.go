package main

// Process-level black-box tests: build the real `mqlite` binary and run it as a subprocess
// so we can exercise things in-process tests cannot — a broken stdout pipe, exit codes, the
// `--` terminator end to end, and an explicit empty --token (MQLITE-93 / review 2026-07-12).

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/server"
)

var mqliteBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "mqcli-bb-*")
	if err != nil {
		panic(err)
	}
	mqliteBin = filepath.Join(dir, "mqlite")
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
}
