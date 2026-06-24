package server_test

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/server"
	"github.com/mqlitehq/mqlite/wire"
)

type logLine struct {
	level  slog.Level
	msg    string
	status int64
	attrs  map[string]string
}

// capture is a slog.Handler that records lines for assertions.
type capture struct{ lines *[]logLine }

func (c capture) Enabled(context.Context, slog.Level) bool { return true }
func (c capture) Handle(_ context.Context, r slog.Record) error {
	l := logLine{level: r.Level, msg: r.Message, attrs: map[string]string{}}
	r.Attrs(func(a slog.Attr) bool {
		l.attrs[a.Key] = a.Value.String()
		if a.Key == "status" {
			l.status = a.Value.Int64()
		}
		return true
	})
	*c.lines = append(*c.lines, l)
	return nil
}
func (c capture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c capture) WithGroup(string) slog.Handler      { return c }

// The access log emits one line per request, with the level chosen by status and the
// shortened RPC as the message.
func TestRequestLog(t *testing.T) {
	eng, err := engine.Open(context.Background(), engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer eng.Close()

	var lines []logLine
	s := server.New(eng, []string{"secret"})
	s.Logger = slog.New(capture{&lines})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	// authed → 200 info
	req, _ := http.NewRequest(http.MethodPost, ts.URL+wire.PathListQueues, bytes.NewReader([]byte("{}")))
	req.Header.Set("Authorization", "Bearer secret")
	res1, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res1.Body.Close()
	// no token → 401 warn
	res2, err := http.Post(ts.URL+wire.PathListQueues, "application/json", bytes.NewReader([]byte("{}")))
	if err != nil {
		t.Fatal(err)
	}
	res2.Body.Close()

	if len(lines) != 2 {
		t.Fatalf("want 2 access-log lines, got %d: %+v", len(lines), lines)
	}
	if lines[0].level != slog.LevelInfo || lines[0].status != 200 || lines[0].msg != "AdminService/ListQueues" {
		t.Errorf("200 line = %+v", lines[0])
	}
	if lines[1].level != slog.LevelWarn || lines[1].status != 401 {
		t.Errorf("401 line = %+v (want warn/401)", lines[1])
	}
}

// With no Logger set, the middleware is a transparent passthrough (no panic, request works).
func TestRequestLogDisabled(t *testing.T) {
	eng, err := engine.Open(context.Background(), engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer eng.Close()
	ts := httptest.NewServer(server.New(eng, nil).Handler()) // Logger nil
	defer ts.Close()
	res, err := http.Post(ts.URL+wire.PathListQueues, "application/json", bytes.NewReader([]byte("{}")))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", res.StatusCode)
	}
}

// Handlers enrich the access line with per-request context (queue / msgs / seq / code),
// and an empty Receive is demoted to Debug so idle long-polls don't flood the Info stream.
func TestRequestLogEnriched(t *testing.T) {
	eng, err := engine.Open(context.Background(), engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer eng.Close()

	var lines []logLine
	s := server.New(eng, nil) // auth off
	s.Logger = slog.New(capture{&lines})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	post := func(path, body string) {
		res, err := http.Post(ts.URL+path, "application/json", bytes.NewReader([]byte(body)))
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
	}
	post(wire.PathCreateQueue, `{"name":"orders"}`)
	post(wire.PathReceive, `{"queue":"orders"}`)                                  // empty -> msgs=0, Debug
	post(wire.PathComplete, `{"queue":"orders","seq_number":1,"lock_token":"x"}`) // 409 lock_lost

	find := func(msg string) logLine {
		for _, l := range lines {
			if l.msg == msg {
				return l
			}
		}
		t.Fatalf("no access-log line for %q in %+v", msg, lines)
		return logLine{}
	}

	if cq := find("AdminService/CreateQueue"); cq.attrs["queue"] != "orders" {
		t.Errorf("CreateQueue queue=%q, want orders", cq.attrs["queue"])
	}
	rc := find("QueueService/Receive")
	if rc.attrs["msgs"] != "0" {
		t.Errorf("empty Receive msgs=%q, want 0", rc.attrs["msgs"])
	}
	if rc.level != slog.LevelDebug {
		t.Errorf("empty Receive level=%v, want Debug (demoted)", rc.level)
	}
	cp := find("QueueService/Complete")
	if cp.status != 409 || cp.attrs["code"] != "lock_lost" {
		t.Errorf("Complete with bad token: status=%d code=%q, want 409/lock_lost", cp.status, cp.attrs["code"])
	}
	if cp.attrs["seq"] != "1" {
		t.Errorf("Complete seq=%q, want 1", cp.attrs["seq"])
	}
}
