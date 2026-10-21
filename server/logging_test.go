package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
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
	req.Header.Set("X-Request-ID", "caller-secret-must-not-be-logged")
	res1, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	requestID1 := res1.Header.Get("X-Request-ID")
	res1.Body.Close()
	// no token → 401 warn
	res2, err := http.Post(ts.URL+wire.PathListQueues, "application/json", bytes.NewReader([]byte("{}")))
	if err != nil {
		t.Fatal(err)
	}
	requestID2 := res2.Header.Get("X-Request-ID")
	res2.Body.Close()

	if len(lines) != 2 {
		t.Fatalf("want 2 access-log lines, got %d: %+v", len(lines), lines)
	}
	if lines[0].level != slog.LevelInfo || lines[0].status != 200 || lines[0].msg != "AdminService/ListQueues" {
		t.Errorf("200 line = %+v", lines[0])
	}
	if !validLogRequestID(requestID1) || requestID1 == "caller-secret-must-not-be-logged" ||
		lines[0].attrs["request_id"] != requestID1 || lines[0].attrs["actor_kind"] != "static_admin" ||
		strings.Contains(fmt.Sprint(lines[0]), "caller-secret-must-not-be-logged") {
		t.Errorf("200 correlation/actor = header %q line %+v", requestID1, lines[0])
	}
	if lines[1].level != slog.LevelWarn || lines[1].status != 401 {
		t.Errorf("401 line = %+v (want warn/401)", lines[1])
	}
	if !validLogRequestID(requestID2) || requestID2 == requestID1 || lines[1].attrs["request_id"] != requestID2 || lines[1].attrs["actor_kind"] != "unverified" {
		t.Errorf("401 correlation/actor = header %q line %+v", requestID2, lines[1])
	}
}

func validLogRequestID(id string) bool {
	if len(id) != 32 {
		return false
	}
	for _, c := range id {
		if c < '0' || c > '9' && c < 'a' || c > 'f' {
			return false
		}
	}
	return true
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
	} else if cq.attrs["actor_kind"] != "anonymous" {
		t.Errorf("CreateQueue actor_kind=%q, want anonymous", cq.attrs["actor_kind"])
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

func TestDestructiveOperationAuditLog(t *testing.T) {
	ctx := context.Background()
	eng := keyTestEngine(t, func() int64 { return 1_000_000 })
	for _, queue := range []string{"orders", "blocked", "dlq"} {
		if err := eng.CreateQueue(ctx, queue, engine.QueueConfig{}); err != nil {
			t.Fatalf("create %s: %v", queue, err)
		}
	}
	for _, queue := range []string{"orders", "blocked"} {
		if _, err := eng.SendOne(ctx, queue, engine.OutMessage{Body: []byte("retained")}); err != nil {
			t.Fatalf("send %s: %v", queue, err)
		}
	}
	if err := eng.Subscribe(ctx, "events", "worker", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.SendOne(ctx, "events", engine.OutMessage{Body: []byte("fanout")}); err != nil {
		t.Fatal(err)
	}

	var lines []logLine
	srv := server.New(eng, []string{"static-secret"})
	srv.MonitorTokens = []string{"monitor-secret"}
	srv.Logger = slog.New(capture{&lines})
	h := srv.Handler()

	const keyID = "11111111111111111111111111111111"
	created := keyTestRequest(h, http.MethodPost, wire.PathCreateKey, "static-secret", wire.CreateKeyRequest{
		ID: keyID, Name: "operator", Permissions: []string{"manage"},
	})
	keyTestStatus(t, created, http.StatusOK, "")
	var key wire.CreateKeyResponse
	if err := json.Unmarshal(created.Body.Bytes(), &key); err != nil {
		t.Fatal(err)
	}

	rejected := keyTestRequest(h, http.MethodPost, wire.PathDeleteQueue, "static-secret", wire.DeleteQueueRequest{Name: "blocked"})
	keyTestStatus(t, rejected, http.StatusConflict, "queue_not_empty")
	deletedQueue := keyTestRequest(h, http.MethodPost, wire.PathDeleteQueue, "static-secret", wire.DeleteQueueRequest{Name: "orders", Force: true})
	keyTestStatus(t, deletedQueue, http.StatusOK, "")
	deletedSub := keyTestRequest(h, http.MethodPost, wire.PathDeleteSubscription, key.Token, wire.DeleteSubscriptionRequest{Topic: "events", Name: "worker", Force: true})
	keyTestStatus(t, deletedSub, http.StatusOK, "")
	purged := keyTestRequest(h, http.MethodPost, wire.PathPurge, "static-secret", wire.PurgeRequest{Queue: "dlq", Max: 7, OlderThanMs: 123})
	keyTestStatus(t, purged, http.StatusOK, "")
	observed := keyTestRequest(h, http.MethodPost, wire.PathObserve, "monitor-secret", wire.ObserveRequest{})
	keyTestStatus(t, observed, http.StatusOK, "")
	revoked := keyTestRequest(h, http.MethodPost, wire.PathRevokeKey, "static-secret", wire.RevokeKeyRequest{ID: keyID})
	keyTestStatus(t, revoked, http.StatusOK, "")
	unverified := keyTestRequest(h, http.MethodPost, wire.PathListQueues, "wrong-secret", wire.Empty{})
	keyTestStatus(t, unverified, http.StatusUnauthorized, "unauthenticated")

	find := func(msg string, match func(logLine) bool) logLine {
		t.Helper()
		for i := len(lines) - 1; i >= 0; i-- {
			if lines[i].msg == msg && match(lines[i]) {
				return lines[i]
			}
		}
		t.Fatalf("no matching access-log line for %q in %+v", msg, lines)
		return logLine{}
	}
	createLine := find("AuthService/CreateKey", func(logLine) bool { return true })
	if createLine.attrs["actor_kind"] != "static_admin" || createLine.attrs["actor_key_id"] != "" ||
		createLine.attrs["key_id"] != keyID || createLine.attrs["result_state"] != "created" {
		t.Errorf("create-key audit = %+v", createLine)
	}
	rejectedLine := find("AdminService/DeleteQueue", func(l logLine) bool { return l.attrs["queue"] == "blocked" })
	if rejectedLine.attrs["force"] != "false" || rejectedLine.attrs["acceptance"] != "rejected" || rejectedLine.attrs["code"] != "queue_not_empty" {
		t.Errorf("rejected delete audit = %+v", rejectedLine)
	}
	queueLine := find("AdminService/DeleteQueue", func(l logLine) bool { return l.attrs["queue"] == "orders" })
	if queueLine.attrs["force"] != "true" || queueLine.attrs["acceptance"] != "accepted" ||
		queueLine.attrs["result_state"] != "deleted" || queueLine.attrs["deleted"] != "true" {
		t.Errorf("force queue-delete audit = %+v", queueLine)
	}
	subLine := find("AdminService/DeleteSubscription", func(logLine) bool { return true })
	if subLine.attrs["actor_kind"] != "managed_key" || subLine.attrs["actor_key_id"] != keyID ||
		subLine.attrs["topic"] != "events" || subLine.attrs["sub"] != "worker" ||
		subLine.attrs["force"] != "true" || subLine.attrs["acceptance"] != "accepted" ||
		subLine.attrs["result_state"] != "deleted" || subLine.attrs["deleted"] != "true" {
		t.Errorf("subscription-delete audit = %+v", subLine)
	}
	purgeLine := find("AdminService/Purge", func(logLine) bool { return true })
	if purgeLine.attrs["queue"] != "dlq" || purgeLine.attrs["max"] != "7" ||
		purgeLine.attrs["older_than_ms"] != "123" || purgeLine.attrs["purged"] != "0" {
		t.Errorf("purge audit = %+v", purgeLine)
	}
	observeLine := find("AdminService/Observe", func(logLine) bool { return true })
	if observeLine.attrs["actor_kind"] != "static_monitor" || observeLine.attrs["actor_key_id"] != "" {
		t.Errorf("monitor actor audit = %+v", observeLine)
	}
	revokeLine := find("AuthService/RevokeKey", func(logLine) bool { return true })
	if revokeLine.attrs["actor_kind"] != "static_admin" || revokeLine.attrs["key_id"] != keyID || revokeLine.attrs["result_state"] != "revoked" {
		t.Errorf("revoke-key audit = %+v", revokeLine)
	}
	unverifiedLine := find("AdminService/ListQueues", func(logLine) bool { return true })
	if unverifiedLine.attrs["actor_kind"] != "unverified" || unverifiedLine.attrs["actor_key_id"] != "" {
		t.Errorf("unverified actor audit = %+v", unverifiedLine)
	}

	seen := map[string]bool{}
	for i, line := range lines {
		id := line.attrs["request_id"]
		if !validLogRequestID(id) || seen[id] {
			t.Fatalf("line %d request_id=%q is invalid or repeated", i, id)
		}
		seen[id] = true
	}
	for _, rec := range []*httptest.ResponseRecorder{created, rejected, deletedQueue, deletedSub, purged, observed, revoked, unverified} {
		if !seen[rec.Header().Get("X-Request-ID")] {
			t.Errorf("response request ID %q has no access-log line", rec.Header().Get("X-Request-ID"))
		}
	}
	serialized := fmt.Sprint(lines)
	for _, secret := range []string{"static-secret", "monitor-secret", "wrong-secret", key.Token} {
		if strings.Contains(serialized, secret) {
			t.Fatalf("access log exposed credential %q", secret)
		}
	}
}

func TestAuditOmitsConfiguredCredentialShapedIDs(t *testing.T) {
	const (
		admin   = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		monitor = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	eng := keyTestEngine(t, nil)
	var lines []logLine
	srv := server.New(eng, []string{admin})
	srv.MonitorTokens = []string{"  " + monitor + "  "}
	srv.Logger = slog.New(capture{&lines})
	h := srv.Handler()

	created := keyTestRequest(h, http.MethodPost, wire.PathCreateKey, admin, wire.CreateKeyRequest{
		ID: admin, Name: "collision", Permissions: []string{"manage"},
	})
	keyTestStatus(t, created, http.StatusOK, "")
	var key wire.CreateKeyResponse
	if err := json.Unmarshal(created.Body.Bytes(), &key); err != nil {
		t.Fatal(err)
	}
	keyTestStatus(t, keyTestRequest(h, http.MethodPost, wire.PathListQueues, key.Token, wire.Empty{}), http.StatusOK, "")
	keyTestStatus(t, keyTestRequest(h, http.MethodPost, wire.PathRevokeKey, admin, wire.RevokeKeyRequest{ID: monitor}), http.StatusNotFound, "")

	var createdLine, managedLine, revokedLine bool
	for _, line := range lines {
		switch line.msg {
		case "AuthService/CreateKey":
			createdLine = line.attrs["actor_kind"] == "static_admin" && line.attrs["key_id"] == "" && line.attrs["result_state"] == "created"
		case "AdminService/ListQueues":
			managedLine = line.attrs["actor_kind"] == "managed_key" && line.attrs["actor_key_id"] == ""
		case "AuthService/RevokeKey":
			revokedLine = line.attrs["actor_kind"] == "static_admin" && line.attrs["key_id"] == ""
		}
	}
	if !createdLine || !managedLine || !revokedLine {
		t.Fatalf("credential-shaped ID audit lines = %+v", lines)
	}
	serialized := fmt.Sprint(lines)
	for _, secret := range []string{admin, monitor, key.Token} {
		if strings.Contains(serialized, secret) {
			t.Fatalf("access log exposed credential %q", secret)
		}
	}
}
