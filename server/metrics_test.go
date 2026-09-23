package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/server"
	"github.com/mqlitehq/mqlite/wire"
)

// MQLITE-5: /metrics serves per-queue counters in Prometheus text format.
func TestMetricsEndpoint(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer eng.Close()
	if err := eng.CreateQueue(ctx, "orders", engine.QueueConfig{}); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if _, err := eng.SendOne(ctx, "orders", engine.OutMessage{Body: []byte("x")}); err != nil {
		t.Fatalf("send: %v", err)
	}

	s := server.New(eng, []string{"administrator-secret"})
	s.MonitorTokens = []string{"monitor-secret"}
	s.Metrics = true
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/metrics", nil)
	if err != nil {
		t.Fatalf("build /metrics request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer monitor-secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	out := string(body)
	for _, want := range []string{
		`mqlite_queue_messages{queue="orders",state="active"} 1`,
		`mqlite_queue_messages{queue="orders",state="locked"} 0`,
		`mqlite_queue_total{queue="orders"} 1`,
		"# TYPE mqlite_queue_messages gauge",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("/metrics output missing %q:\n%s", want, out)
		}
	}
}

// The exporter is off by default, including anonymous development mode.
// Observe also remains protected even while data-plane RPCs are anonymous.
func TestMetricsDefaultOffAndAnonymousObserveDenied(t *testing.T) {
	eng := keyTestEngine(t, nil)
	s := server.New(eng, nil)
	s.CORS = "*"
	h := s.Handler()
	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPost, http.MethodOptions, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodTrace, http.MethodConnect} {
		r := keyTestRequest(h, method, "/metrics", "", nil)
		keyTestStatus(t, r, http.StatusNotFound, "")
		if r.Header().Get("Access-Control-Allow-Origin") != "" {
			t.Fatal("disabled exporter passed through CORS middleware")
		}
	}
	keyTestStatus(t, keyTestRequest(h, http.MethodPost, wire.PathObserve, "", wire.ObserveRequest{}), http.StatusForbidden, "permission_denied")
}

func TestMetricsEnabledRequiresAdministratorAuthentication(t *testing.T) {
	eng := keyTestEngine(t, nil)
	for _, admins := range [][]string{nil, {}, {""}, {" "}} {
		s := server.New(eng, admins)
		s.Metrics = true
		s.CORS = "*"
		h := s.Handler()
		for _, path := range []string{"/", "/healthz", "/ui/", "/metrics", wire.PathObserve, wire.PathSend} {
			keyTestStatus(t, keyTestRequest(h, http.MethodGet, path, "", nil), http.StatusInternalServerError, "internal")
		}
	}
}

func TestMetricsDisabledSkipsDatabaseAuthentication(t *testing.T) {
	eng := keyTestEngine(t, nil)
	_, token := keyTestCreate(t, eng, 1, engine.KeyManage, 0)
	s := server.New(eng, []string{"admin"})
	h := s.Handler()
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
	// Managed-key authentication would fail with a backend error here. Disabled
	// metrics must reject the request before authentication or collection work.
	keyTestStatus(t, keyTestRequest(h, http.MethodGet, "/metrics", token, nil), http.StatusNotFound, "")
}

// Pin the complete credential/method domain in both modes. A disabled exporter
// stays absent even to administrators, and denied callers never get a snapshot.
func TestMetricsPermissionMatrix(t *testing.T) {
	for i, tc := range []struct {
		name       string
		permission engine.KeyPermissions
		expired    bool
		revoked    bool
		expected   int
		code       string
	}{
		{name: "anonymous", expected: http.StatusUnauthorized, code: "unauthenticated"},
		{name: "invalid", expected: http.StatusUnauthorized, code: "unauthenticated"},
		{name: "send", permission: engine.KeySend, expected: http.StatusForbidden, code: "permission_denied"},
		{name: "listen", permission: engine.KeyListen, expected: http.StatusForbidden, code: "permission_denied"},
		{name: "send+listen", permission: engine.KeySend | engine.KeyListen, expected: http.StatusForbidden, code: "permission_denied"},
		{name: "manage", permission: engine.KeyManage, expected: http.StatusOK},
		{name: "static-admin", expected: http.StatusOK},
		{name: "monitor", expected: http.StatusOK},
		{name: "expired", permission: engine.KeyManage, expired: true, expected: http.StatusUnauthorized, code: "unauthenticated"},
		{name: "revoked", permission: engine.KeyManage, revoked: true, expected: http.StatusUnauthorized, code: "unauthenticated"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := int64(1000)
			eng := keyTestEngine(t, func() int64 { return now })
			token := tc.name
			switch tc.name {
			case "anonymous":
				token = ""
			case "invalid":
				token = "mqk_" + strings.Repeat("0", 64)
			case "static-admin":
				token = "admin"
			}
			if tc.permission != 0 {
				expiry := int64(0)
				if tc.expired {
					expiry = now + 1
				}
				key, secret := keyTestCreate(t, eng, i+1, tc.permission, expiry)
				token = secret
				if tc.revoked {
					if err := eng.RevokeAccessKey(context.Background(), key.ID); err != nil {
						t.Fatal(err)
					}
				}
				if tc.expired {
					now++
				}
			}
			for _, enabled := range []bool{false, true} {
				t.Run(fmt.Sprintf("enabled=%t", enabled), func(t *testing.T) {
					s := server.New(eng, []string{"admin"})
					s.MonitorTokens = []string{"monitor"}
					s.Metrics = enabled
					if !enabled {
						s.CORS = "*" // off must win over CORS preflight too
					}
					h := s.Handler()
					for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPost, http.MethodOptions, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodTrace, http.MethodConnect} {
						t.Run(method, func(t *testing.T) {
							want, code := tc.expected, tc.code
							if !enabled {
								want, code = http.StatusNotFound, ""
							} else if want == http.StatusOK && method != http.MethodGet {
								want, code = http.StatusMethodNotAllowed, "unimplemented"
							}
							rec := keyTestRequest(h, method, "/metrics", token, nil)
							keyTestStatus(t, rec, want, code)
							if !enabled && rec.Header().Get("Access-Control-Allow-Origin") != "" {
								t.Fatal("disabled exporter passed through CORS middleware")
							}
							if want == http.StatusOK {
								if rec.Header().Get("Cache-Control") != "no-store" || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/plain; version=0.0.4") {
									t.Fatalf("invalid scrape headers: %v", rec.Header())
								}
								if !strings.Contains(rec.Body.String(), "mqlite_collection_success 1") {
									t.Fatal("successful scrape has no collection snapshot")
								}
							} else if strings.Contains(rec.Body.String(), "mqlite_collection_success") {
								t.Fatal("denied scrape exposed a collection snapshot")
							}
							if want == http.StatusMethodNotAllowed && rec.Header().Get("Allow") != http.MethodGet {
								t.Fatal("method rejection must advertise GET")
							}
						})
					}
				})
			}
		})
	}
}

func TestMetricsCORSPreflightAndAuthentication(t *testing.T) {
	eng := keyTestEngine(t, nil)
	s := server.New(eng, []string{"admin"})
	s.Metrics = true
	s.CORS = "https://console.example.test"
	h := s.Handler()
	for _, token := range []string{"", "invalid", "admin"} {
		req := httptest.NewRequest(http.MethodOptions, "/metrics", nil)
		req.Header.Set("Origin", s.CORS)
		req.Header.Set("Access-Control-Request-Method", http.MethodGet)
		req.Header.Set("Access-Control-Request-Headers", "Authorization")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		keyTestStatus(t, rec, http.StatusNoContent, "")
		if rec.Body.Len() != 0 || rec.Header().Get("Access-Control-Allow-Origin") != s.CORS {
			t.Fatalf("invalid metrics preflight: %s %v", rec.Body, rec.Header())
		}
		want := http.StatusUnauthorized
		if token == "admin" {
			want = http.StatusOK
		}
		keyTestStatus(t, keyTestRequest(h, http.MethodGet, "/metrics", token, nil), want, "")
	}
	// Credentials in a discovery link or query string must not grant access.
	keyTestStatus(t, keyTestRequest(h, http.MethodGet, "/metrics?token=admin", "", nil), http.StatusUnauthorized, "unauthenticated")
}

func TestMetricsAccessKeyExpiryAndRevocationTakeEffectOnNextScrape(t *testing.T) {
	now := int64(1000)
	eng := keyTestEngine(t, func() int64 { return now })
	_, expiring := keyTestCreate(t, eng, 1, engine.KeyManage, now+1)
	key, revocable := keyTestCreate(t, eng, 2, engine.KeyManage, 0)
	s := server.New(eng, []string{"admin"})
	s.Metrics = true
	h := s.Handler()
	for _, token := range []string{expiring, revocable} {
		keyTestStatus(t, keyTestRequest(h, http.MethodGet, "/metrics", token, nil), http.StatusOK, "")
	}
	if err := eng.RevokeAccessKey(context.Background(), key.ID); err != nil {
		t.Fatal(err)
	}
	keyTestStatus(t, keyTestRequest(h, http.MethodGet, "/metrics", revocable, nil), http.StatusUnauthorized, "unauthenticated")
	keyTestStatus(t, keyTestRequest(h, http.MethodGet, "/metrics", expiring, nil), http.StatusOK, "")
	now++
	keyTestStatus(t, keyTestRequest(h, http.MethodGet, "/metrics", expiring, nil), http.StatusUnauthorized, "unauthenticated")
	keyTestStatus(t, keyTestRequest(h, http.MethodGet, "/metrics", "admin", nil), http.StatusOK, "")
}

// The read-only credential remains usable when storage cannot authenticate a
// managed key. Failed collection is unavailable, never a healthy empty queue.
func TestObserveStorageFailureAvailability(t *testing.T) {
	ctx := context.Background()
	eng := keyTestEngine(t, nil)
	_, managed := keyTestCreate(t, eng, 1, engine.KeyManage, 0)
	if err := eng.CreateQueue(ctx, "q", engine.QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.SendOne(ctx, "q", engine.OutMessage{Body: []byte("secret-body")}); err != nil {
		t.Fatal(err)
	}
	s := server.New(eng, []string{"administrator-secret"})
	s.MonitorTokens = []string{"monitor-secret"}
	s.Metrics = true
	h := s.Handler()
	read := func() wire.ObserveResponse {
		t.Helper()
		r := keyTestRequest(h, http.MethodPost, wire.PathObserve, "monitor-secret", wire.ObserveRequest{})
		keyTestStatus(t, r, http.StatusOK, "")
		if r.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("snapshot must not be cached")
		}
		for _, secret := range []string{"administrator-secret", "monitor-secret", managed, "secret-body", "token_hash", "location"} {
			if strings.Contains(r.Body.String(), secret) {
				t.Fatalf("observation exposed protected data category")
			}
		}
		out, err := wire.DecodeObserveResponse(r.Body.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	before := read()
	if before.Access != "monitor" || before.Collection.State != "available" || len(before.Queues) != 1 || before.Queues[0].Total != 1 {
		t.Fatalf("bad healthy observation: %+v", before)
	}
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
	after := read()
	if after.Collection.State != "unavailable" || after.Queues != nil || after.Collection.LastSuccessAtMs != before.Collection.LastSuccessAtMs || after.Runtime.ReadAvailable {
		t.Fatalf("failure replaced by healthy zero: %+v", after)
	}
	r := keyTestRequest(h, http.MethodGet, "/metrics", "monitor-secret", nil)
	keyTestStatus(t, r, http.StatusOK, "")
	for _, want := range []string{"mqlite_collection_success 0", "mqlite_storage_read_available 0", `mqlite_message_events_total{queue="q",event="enqueued"} 1`} {
		if !strings.Contains(r.Body.String(), want) {
			t.Errorf("missing failure fact %q", want)
		}
	}
	if strings.Contains(r.Body.String(), `mqlite_queue_messages{`) || strings.Contains(r.Body.String(), "mqlite_storage_ping_seconds ") && !strings.Contains(r.Body.String(), "mqlite_storage_ping_seconds gauge") {
		t.Fatal("failed gauge serialized")
	}
	keyTestStatus(t, keyTestRequest(h, http.MethodPost, wire.PathObserve, managed, wire.ObserveRequest{}), http.StatusInternalServerError, "internal")
	keyTestStatus(t, keyTestRequest(h, http.MethodGet, "/metrics", managed, nil), http.StatusInternalServerError, "internal")
	keyTestStatus(t, keyTestRequest(h, http.MethodGet, "/metrics", "administrator-secret", nil), http.StatusOK, "")
}

func TestObserveAuthOutcomesAndWholeRequestCoverage(t *testing.T) {
	ctx := context.Background()
	now := int64(10000)
	eng := keyTestEngine(t, func() int64 { return now })
	_, expired := keyTestCreate(t, eng, 1, engine.KeyManage, now+1)
	revokedKey, revoked := keyTestCreate(t, eng, 2, engine.KeyManage, 0)
	_, send := keyTestCreate(t, eng, 3, engine.KeySend, 0)
	if err := eng.RevokeAccessKey(ctx, revokedKey.ID); err != nil {
		t.Fatal(err)
	}
	now++
	s := server.New(eng, []string{"admin"})
	s.MonitorTokens = []string{"monitor"}
	h := s.Handler()
	for _, tt := range []struct {
		token  string
		status int
		code   string
	}{
		{"", 401, "unauthenticated"}, {"invalid", 401, "unauthenticated"}, {expired, 401, "unauthenticated"}, {revoked, 401, "unauthenticated"}, {send, 403, "permission_denied"}, {"admin", 200, ""},
	} {
		keyTestStatus(t, keyTestRequest(h, http.MethodPost, wire.PathObserve, tt.token, wire.ObserveRequest{}), tt.status, tt.code)
	}
	keyTestStatus(t, keyTestRequest(h, http.MethodGet, wire.PathObserve, "monitor", nil), 405, "unimplemented")
	keyTestStatus(t, keyTestRequest(h, http.MethodPost, wire.PathObserve, "monitor", map[string]bool{"unknown": true}), 400, "invalid_argument")
	r := keyTestRequest(h, http.MethodPost, wire.PathObserve, "monitor", wire.ObserveRequest{})
	out, err := wire.DecodeObserveResponse(r.Body.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	wantAuth := map[string]uint64{"success": 3, "missing": 1, "invalid": 1, "expired": 1, "revoked": 1, "permission_denied": 1, "backend_error": 0}
	for _, a := range out.HTTP.Authentication {
		if a.Count != wantAuth[a.Outcome] {
			t.Errorf("auth %s=%d want %d", a.Outcome, a.Count, wantAuth[a.Outcome])
		}
		delete(wantAuth, a.Outcome)
	}
	if len(wantAuth) != 0 {
		t.Fatalf("auth domain incomplete: %v", wantAuth)
	}
	wantCode := map[string]uint64{"unauthenticated": 4, "permission_denied": 1, "ok": 1, "unimplemented": 1, "invalid_argument": 1}
	for _, rq := range out.HTTP.Requests {
		if rq.Count == 0 {
			continue
		}
		if rq.RPC != "AdminService/Observe" || rq.Count != wantCode[rq.Code] || rq.DurationSeconds < 0 {
			t.Errorf("unexpected request counter %+v", rq)
		}
		delete(wantCode, rq.Code)
	}
	if len(wantCode) != 0 {
		t.Fatalf("request domain incomplete: %v", wantCode)
	}
	if len(out.HTTP.HandlerLatency) != 1 || out.HTTP.HandlerLatency[0].Count != 4 {
		t.Fatalf("legacy handler timing must exclude failed authentication, include403: %+v", out.HTTP.HandlerLatency)
	}
}

func TestObserveConcurrentHistogramAndSpecialLabels(t *testing.T) {
	eng := keyTestEngine(t, nil)
	name := "orders\tline\n\"quote\\\u96ea"
	if err := eng.CreateQueue(context.Background(), name, engine.QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	s := server.New(eng, []string{"administrator-secret"})
	s.MonitorTokens = []string{"monitor-secret"}
	s.Metrics = true
	h := s.Handler()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				keyTestRequest(h, http.MethodPost, wire.PathListQueues, "administrator-secret", wire.Empty{})
			}
		}()
	}
	for i := 0; i < 30; i++ {
		r := keyTestRequest(h, http.MethodPost, wire.PathObserve, "monitor-secret", wire.ObserveRequest{})
		out, err := wire.DecodeObserveResponse(r.Body.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		for _, lat := range out.HTTP.HandlerLatency {
			var last uint64
			for _, b := range lat.Buckets {
				if b.Count < last || b.Count > lat.Count {
					t.Fatalf("inconsistent buckets: %+v", lat)
				}
				last = b.Count
			}
		}
		prom := keyTestRequest(h, http.MethodGet, "/metrics", "monitor-secret", nil).Body.String()
		if !strings.Contains(prom, "queue=\"orders\tline\\n\\\"quote\\\\\u96ea\"") {
			t.Fatal("Prometheus label escaping drift")
		}
		inf, count := map[string]string{}, map[string]string{}
		for _, line := range strings.Split(prom, "\n") {
			if strings.HasPrefix(line, "mqlite_rpc_duration_seconds_bucket{") && strings.Contains(line, `,le="+Inf"}`) {
				parts := strings.SplitN(line, `,le="+Inf"} `, 2)
				inf[strings.TrimPrefix(parts[0], "mqlite_rpc_duration_seconds_bucket{")] = parts[1]
			}
			if strings.HasPrefix(line, "mqlite_rpc_duration_seconds_count{") {
				parts := strings.SplitN(line, "} ", 2)
				count[strings.TrimPrefix(parts[0], "mqlite_rpc_duration_seconds_count{")] = parts[1]
			}
		}
		a, _ := json.Marshal(inf)
		b, _ := json.Marshal(count)
		if string(a) != string(b) {
			t.Fatalf("+Inf differs from count: %s vs %s", a, b)
		}
	}
	wg.Wait()
}

func TestMonitorConfigurationFailsClosed(t *testing.T) {
	eng := keyTestEngine(t, nil)
	for _, tt := range []struct{ admins, monitors []string }{{nil, []string{"monitor"}}, {[]string{"same"}, []string{"same"}}, {[]string{"admin"}, []string{""}}, {[]string{"admin"}, []string{"bad token"}}, {[]string{"admin"}, []string{"bad,token"}}} {
		if err := server.ValidateMonitorTokens(tt.admins, tt.monitors); err == nil {
			t.Fatal("bad monitoring configuration accepted")
		}
		s := server.New(eng, tt.admins)
		s.MonitorTokens = tt.monitors
		for _, path := range []string{"/", wire.PathObserve, wire.PathSend, "/metrics"} {
			r := keyTestRequest(s.Handler(), http.MethodPost, path, "admin", wire.Empty{})
			keyTestStatus(t, r, 500, "internal")
			if strings.Contains(r.Body.String(), "bad token") {
				t.Fatal("configuration error exposed token")
			}
		}
	}
	if err := server.ValidateMonitorTokens([]string{"admin"}, []string{"monitor", "monitor-rotated"}); err != nil {
		t.Fatal(err)
	}
}

// /metrics exposes a lifetime completed-message counter that persists past the row
// being deleted on Complete — so "how many were processed" is readable even on an
// empty queue (MQLITE-54).
func TestMetricsCompletedCounter(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer eng.Close()
	if err := eng.CreateQueue(ctx, "orders", engine.QueueConfig{LockDurationMs: 600_000, MaxDeliveryCount: 10}); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := eng.SendOne(ctx, "orders", engine.OutMessage{Body: []byte("x")}); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	msgs, err := eng.Receive(ctx, "orders", engine.ReceiveOptions{MaxMessages: 3})
	if err != nil || len(msgs) != 3 {
		t.Fatalf("receive: got %d (err %v)", len(msgs), err)
	}
	for _, m := range msgs {
		if err := eng.Complete(ctx, "orders", m.SeqNumber, m.LockToken); err != nil {
			t.Fatalf("complete: %v", err)
		}
	}

	s := server.New(eng, []string{"administrator-secret"})
	s.MonitorTokens = []string{"monitor-secret"}
	s.Metrics = true
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/metrics", nil)
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	req.Header.Set("Authorization", "Bearer monitor-secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	out := string(body)
	for _, want := range []string{
		"# TYPE mqlite_messages_completed_total counter",
		`mqlite_messages_completed_total{queue="orders"} 3`, // the queue is empty, yet the count survives
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("/metrics missing %q:\n%s", want, out)
		}
	}
}

// /metrics also exposes a per-RPC latency histogram, fed by every RPC call — so a slow
// dequeue is visible in monitoring, not just in tests.
func TestMetricsRPCLatencyHistogram(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer eng.Close()
	s := server.New(eng, []string{"administrator-secret"})
	s.MonitorTokens = []string{"monitor-secret"}
	s.Metrics = true
	api := httptest.NewServer(s.Handler())
	defer api.Close()

	const n = 5
	for i := 0; i < n; i++ {
		req, err := http.NewRequest(http.MethodPost, api.URL+"/mqlite.v1.AdminService/ListQueues", strings.NewReader("{}"))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer administrator-secret")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
	}

	// observe() runs just after the handler returns, so a read can race the last call —
	// poll until the histogram has counted all n (it settles in microseconds).
	want := `mqlite_rpc_duration_seconds_count{rpc="AdminService/ListQueues"} 5`
	var out string
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		req, err := http.NewRequest(http.MethodGet, api.URL+"/metrics", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer monitor-secret")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if out = string(body); strings.Contains(out, want) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	for _, w := range []string{
		want,
		`mqlite_rpc_duration_seconds_bucket{rpc="AdminService/ListQueues",le="+Inf"} 5`,
		`mqlite_rpc_duration_seconds_sum{rpc="AdminService/ListQueues"}`,
		"# TYPE mqlite_rpc_duration_seconds histogram",
	} {
		if !strings.Contains(out, w) {
			t.Errorf("histogram missing %q:\n%s", w, out)
		}
	}
	// the observer covers only RPCs — not /metrics itself, /healthz, or static paths.
	if strings.Contains(out, `rpc="metrics"`) || strings.Contains(out, `rpc="healthz"`) {
		t.Errorf("histogram should not include non-RPC paths:\n%s", out)
	}
}

// MQLITE-62: only REGISTERED routes are ever labeled. An unregistered
// /mqlite.v1.* path (the 404 catch-all) must not create histogram series —
// otherwise any client can grow the label map without bound (one counter set
// per invented path, never evicted): a memory / scrape-size DoS.
func TestMetricsRPCLabelsOnlyForRegisteredRoutes(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer eng.Close()
	s := server.New(eng, []string{"administrator-secret"})
	s.MonitorTokens = []string{"monitor-secret"}
	s.Metrics = true
	api := httptest.NewServer(s.Handler())
	defer api.Close()

	// A burst of distinct invented RPC names — none may become a label.
	for i := 0; i < 20; i++ {
		req, err := http.NewRequest(http.MethodPost, api.URL+fmt.Sprintf("/mqlite.v1.QueueService/Nope%d", i), strings.NewReader("{}"))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer administrator-secret")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound {
			t.Fatalf("invented RPC path should 404, got %d", res.StatusCode)
		}
	}
	// One real RPC so the histogram section exists at all.
	req, err := http.NewRequest(http.MethodPost, api.URL+"/mqlite.v1.AdminService/ListQueues", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer administrator-secret")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()

	var out string
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		req, err := http.NewRequest(http.MethodGet, api.URL+"/metrics", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer monitor-secret")
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(r.Body)
		r.Body.Close()
		if out = string(body); strings.Contains(out, `rpc="AdminService/ListQueues"`) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !strings.Contains(out, `rpc="AdminService/ListQueues"`) {
		t.Fatalf("registered route must be labeled:\n%s", out)
	}
	if strings.Contains(out, "Nope") {
		t.Fatalf("unregistered RPC paths must never become labels (cardinality DoS):\n%s", out)
	}
}
