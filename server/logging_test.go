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
}

// capture is a slog.Handler that records lines for assertions.
type capture struct{ lines *[]logLine }

func (c capture) Enabled(context.Context, slog.Level) bool { return true }
func (c capture) Handle(_ context.Context, r slog.Record) error {
	l := logLine{level: r.Level, msg: r.Message}
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "status" {
			l.status = a.Value.Int64()
		}
		return true
	})
	*c.lines = append(*c.lines, l)
	return nil
}
func (c capture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c capture) WithGroup(string) slog.Handler       { return c }

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
	if _, err := http.DefaultClient.Do(req); err != nil {
		t.Fatal(err)
	}
	// no token → 401 warn
	if _, err := http.Post(ts.URL+wire.PathListQueues, "application/json", bytes.NewReader([]byte("{}"))); err != nil {
		t.Fatal(err)
	}

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
