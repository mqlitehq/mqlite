package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	mq "github.com/mqlitehq/mqlite"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type rtFunc func(*http.Request) (*http.Response, error)

func (f rtFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type brokenRead struct{}

func (brokenRead) Read([]byte) (int, error) { return 0, errors.New("private-reader-error") }
func (brokenRead) Close() error             { return nil }

type observedFixtureBody struct {
	io.Reader
	closes   int
	closeErr error
}

func (b *observedFixtureBody) Close() error { b.closes++; return b.closeErr }
func traceRequest() *http.Request {
	r, _ := http.NewRequest("POST", "https://provider.invalid/v2/pipeline", strings.NewReader(`{"requests":[{"type":"execute","stmt":{"sql":"CREATE TABLE x(a)"}}]}`))
	return r
}

func TestProviderObserverFiniteBoundaries(t *testing.T) {
	for _, name := range []string{"valid", "wrong-host", "allowed-broker", "no-get-body", "get-body-error", "read-error", "too-large", "response-and-error", "error-only", "nil-response", "nil-body", "http-503", "missing-eof", "close-error"} {
		t.Run(name, func(t *testing.T) {
			r := traceRequest()
			body := &observedFixtureBody{Reader: strings.NewReader("{}")}
			calls := 0
			p := &providerObserver{host: "provider.invalid", next: rtFunc(func(*http.Request) (*http.Response, error) {
				calls++
				resp := &http.Response{StatusCode: 200, Body: body}
				switch name {
				case "response-and-error":
					return resp, errors.New("private-provider-error")
				case "error-only":
					return nil, errors.New("private-provider-error")
				case "nil-response":
					return nil, nil
				case "nil-body":
					resp.Body = nil
				case "http-503":
					resp.StatusCode = 503
				}
				return resp, nil
			})}
			switch name {
			case "wrong-host", "allowed-broker":
				if name == "allowed-broker" {
					p.allowBroker("unrelated.invalid")
				}
				r.URL.Host = "unrelated.invalid"
				r.GetBody = nil
			case "no-get-body":
				r.GetBody = nil
			case "get-body-error":
				r.GetBody = func() (io.ReadCloser, error) { return nil, errors.New("private-body-error") }
			case "read-error":
				r.GetBody = func() (io.ReadCloser, error) { return brokenRead{}, nil }
			case "too-large":
				r.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(make([]byte, 128*1024+1))), nil }
			case "close-error":
				body.closeErr = errors.New("private-close-error")
			}
			resp, e := p.RoundTrip(r)
			if resp != nil && resp.Body != nil {
				if name != "missing-eof" {
					_, _ = io.Copy(io.Discard, resp.Body)
				}
				_ = resp.Body.Close()
				_ = resp.Body.Close()
			}
			got := p.snapshot()
			wantCalls := 1
			if name == "wrong-host" {
				wantCalls = 0
			}
			if calls != wantCalls {
				t.Fatalf("delegate calls=%d", calls)
			}
			if name == "response-and-error" && body.closes != 1 {
				t.Fatal("error response body not closed exactly once")
			}
			switch name {
			case "valid":
				if got.Requests != 1 || got.SchemaDDL != 1 || got.EOFAndClose != 1 || got.HTTP200 != 1 || got.Active != 0 || got.InvalidCapture != 0 {
					t.Fatalf("valid: %+v", got)
				}
			case "allowed-broker":
				if got != (providerCounts{}) {
					t.Fatalf("wrong-host captured: %+v", got)
				}
			case "error-only":
				if e == nil || got.Active != 0 || got.InvalidCapture != 0 {
					t.Fatalf("error: %+v", got)
				}
			case "http-503":
				if got.HTTP200 != 0 || got.EOFAndClose != 1 || got.InvalidCapture != 0 {
					t.Fatalf("HTTP status altered: %+v", got)
				}
			default:
				if got.InvalidCapture == 0 || got.Active != 0 {
					t.Fatalf("bad capture accepted: %+v", got)
				}
			}
		})
	}
}
func TestProviderObserverActiveJoin(t *testing.T) {
	p := &providerObserver{counts: providerCounts{Active: 1}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if p.join(ctx.Done()) {
		t.Fatal("unclosed response joined")
	}
	p.mu.Lock()
	p.counts.Active = 0
	p.mu.Unlock()
	fresh, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	if !p.join(fresh.Done()) {
		t.Fatal("completed response did not join")
	}
}
func TestOwnerCannotCloseBeforeServeJoin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	o := &owner{cancel: func() {}, done: make(chan error)}
	if o.stop(ctx) == nil || o.stopDone {
		t.Fatal("unfinished serve counted as joined")
	}
	if safeClass(o.close(context.Background())) != "close_before_serve_join" {
		t.Fatal("unjoined owner was closed/reopened")
	}
}

func TestSettlementWitnessRequiresExactIdentityAndReceipt(t *testing.T) {
	for _, name := range []string{"complete", "renew", "wrong-queue", "wrong-seq", "wrong-token", "http-201", "false", "missing-ok", "bad-json", "missing-eof", "close-error"} {
		t.Run(name, func(t *testing.T) {
			path := "Complete"
			if name == "renew" {
				path = "Renew"
			}
			in := map[string]any{"queue": "owned", "seq_number": int64(3), "lock_token": "private-token"}
			switch name {
			case "wrong-queue":
				in["queue"] = "other"
			case "wrong-seq":
				in["seq_number"] = 4
			case "wrong-token":
				in["lock_token"] = "other"
			}
			data, _ := json.Marshal(in)
			req, _ := http.NewRequest("POST", "http://broker.invalid/mqlite.v1.QueueService/"+path, bytes.NewReader(data))
			response := `{"ok":true}`
			status := 200
			switch name {
			case "http-201":
				status = 201
			case "false":
				response = `{"ok":false}`
			case "missing-ok":
				response = `{}`
			case "bad-json":
				response = `{"ok":true`
			}
			body := &observedFixtureBody{Reader: strings.NewReader(response)}
			if name == "close-error" {
				body.closeErr = errors.New("private-close-error")
			}
			a := &ackObserver{queue: "owned", next: rtFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: status, Body: body}, nil
			})}
			a.expect(3, "private-token")
			resp, err := a.RoundTrip(req)
			if err != nil {
				t.Fatal(err)
			}
			if name != "missing-eof" {
				_, _ = io.Copy(io.Discard, resp.Body)
			}
			_ = resp.Body.Close()
			_ = resp.Body.Close()
			complete, _ := a.snapshot()
			renew, _ := a.renewSnapshot()
			if complete != (name == "complete") || renew != (name == "renew") {
				t.Fatalf("witness complete=%t renew=%t", complete, renew)
			}
			if body.closes != 2 {
				t.Fatal("Close delegation changed")
			}
		})
	}
}

func TestLocalFiniteReopenAndNegativeWitness(t *testing.T) {
	for _, mode := range []string{"native", "lowfloor", "suppressed-complete", "capture-missing"} {
		t.Run(mode, func(t *testing.T) {
			f := newSQLiteHranaFixture(t)
			p, err := newProviderObserver(f.dsn)
			if err != nil {
				t.Fatal(err)
			}
			restore := p.install()
			defer restore()
			cfg := caseConfig{dsn: f.dsn, trace: p, afterFirstClose: func() { f.setDelay(10 * time.Millisecond) }}
			sdk := mode
			if mode == "suppressed-complete" {
				sdk = "native"
				cfg.budget = 3 * time.Second
				cfg.wrapBroker = func(next http.RoundTripper) http.RoundTripper {
					return rtFunc(func(r *http.Request) (*http.Response, error) {
						if strings.HasSuffix(r.URL.Path, "/Complete") {
							return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"ok":false}`)), Request: r}, nil
						}
						return next.RoundTrip(r)
					})
				}
			}
			if mode == "capture-missing" {
				sdk = "native"
				cfg.afterFirstClose = func() {
					http.DefaultTransport = rtFunc(func(r *http.Request) (*http.Response, error) {
						q := r.Clone(r.Context())
						q.GetBody = nil
						return p.RoundTrip(q)
					})
				}
			}
			result := runCase(context.Background(), cfg, sdk)
			data, _ := json.Marshal(result)
			t.Log(string(data))
			if result.Passed != (mode == "native" || mode == "lowfloor") {
				t.Fatalf("unexpected pass=%t", result.Passed)
			}
			if !result.OwnersJoined || !result.ReceiverJoined || !result.CleanupAbsent || !result.ProviderJoined {
				t.Fatalf("failed bounded cleanup: %s", data)
			}
			if result.Passed && (result.FullWindow.Requests <= result.Reopen.Requests || !result.CompleteAcknowledged || !result.RenewAcknowledged) {
				t.Fatal("window did not extend through actual settlement ACK")
			}
			if mode == "suppressed-complete" && result.CompleteAcknowledged {
				t.Fatal("missing Complete invented")
			}
			if mode == "capture-missing" && p.snapshot().InvalidCapture == 0 {
				t.Fatal("capture negative not exercised")
			}
		})
	}
}

func TestOutputAndOwnershipPrecedeAnyDatabaseCall(t *testing.T) {
	out := t.TempDir() + "/fresh"
	if err := reserveOutput(out); err != nil {
		t.Fatal(err)
	}
	if err := reserveOutput(out); err == nil {
		t.Fatal("existing evidence directory accepted")
	}
	if err := writeJSON(out, "ownership.json", map[string]any{"queues": []string{}}); err != nil {
		t.Fatal(err)
	}
	p := &providerObserver{}
	r := runCase(context.Background(), caseConfig{trace: p, register: func(string, string) error { return errors.New("disk unavailable") }}, "native")
	if r.Failure != "ownership_manifest_write" || p.snapshot().Requests != 0 {
		t.Fatalf("ownership failure touched database: %+v", r)
	}
}

func TestDiagnosticAllowlistBoundsAndPrivateFields(t *testing.T) {
	log := &safeLog{}
	h := safeHandler{log: log}
	record := slog.NewRecord(time.Now(), slog.LevelError, "storage.failure", 0)
	record.AddAttrs(slog.String("operation", "transaction"), slog.String("transaction_phase", "commit_call"), slog.String("category", "outcome_unknown"), slog.String("fingerprint", "0123456789abcdef01234567"), slog.Int("http_status", 503), slog.String("error", "token=private SQL secret"), slog.String("cause", "private-provider.invalid"), slog.String("url", "https://private.invalid"))
	for i := 0; i < 70; i++ {
		if err := h.Handle(context.Background(), record); err != nil {
			t.Fatal(err)
		}
	}
	events, dropped := log.snapshot()
	if len(events) != 64 || dropped != 6 || events[0].Phase != "commit_call" || events[0].Category != "outcome_unknown" {
		t.Fatalf("invalid bounded diagnostics: %d/%d", len(events), dropped)
	}
	b, _ := json.Marshal(events)
	if bytes.Contains(b, []byte("private")) || bytes.Contains(b, []byte("secret")) || bytes.Contains(b, []byte("SQL")) {
		t.Fatal("private log field escaped")
	}
}

func TestSupplementalCleanupRefusesLiveOrUnboundOwner(t *testing.T) {
	savedR, savedS, savedT := runtimeSHA, sdkSHA, runnerSHA
	defer func() { runtimeSHA, sdkSHA, runnerSHA = savedR, savedS, savedT }()
	runtimeSHA = strings.Repeat("1", 40)
	sdkSHA = strings.Repeat("2", 40)
	runnerSHA = strings.Repeat("3", 40)
	out := t.TempDir()
	m := ownershipRecord{Schema: "m162-owned-queues-v1", Backend: "standard", RuntimeSHA: runtimeSHA, SDK_SHA: sdkSHA, RunnerSHA: runnerSHA, HostHash: localHostHash(), PID: os.Getpid(), Queues: []ownedQueue{{Queue: "m162-budget-" + strings.Repeat("a", 24), SDK: "native"}}}
	if err := writeJSON(out, "ownership.json", m); err != nil {
		t.Fatal(err)
	}
	if _, err := loadOwnership(out, "standard", false); safeClass(err) != "prior_owner_exit_confirmation_required" {
		t.Fatal("missing owner confirmation accepted")
	}
	if _, err := loadOwnership(out, "standard", true); safeClass(err) != "prior_owner_still_present" {
		t.Fatal("live owner accepted")
	}
	m.PID = 1 << 30
	m.RuntimeSHA = strings.Repeat("4", 40)
	_ = writeJSON(out, "ownership.json", m)
	if _, err := loadOwnership(out, "standard", true); safeClass(err) != "ownership_manifest_mismatch" {
		t.Fatal("wrong source accepted")
	}
	m.RuntimeSHA = runtimeSHA
	m.Queues[0].Queue = "foreign-owned"
	_ = writeJSON(out, "ownership.json", m)
	if _, err := loadOwnership(out, "standard", true); safeClass(err) != "ownership_scope_invalid" {
		t.Fatal("foreign scope accepted")
	}
}
func TestSupplementalCleanupOnlyOwnedQueueAndPreservesFailure(t *testing.T) {
	savedR, savedS, savedT := runtimeSHA, sdkSHA, runnerSHA
	defer func() { runtimeSHA, sdkSHA, runnerSHA = savedR, savedS, savedT }()
	runtimeSHA = strings.Repeat("1", 40)
	sdkSHA = strings.Repeat("2", 40)
	runnerSHA = strings.Repeat("3", 40)
	f := newSQLiteHranaFixture(t)
	e, err := mq.OpenEmbedded(context.Background(), f.dsn, mq.WithBrokerMode())
	if err != nil {
		t.Fatal(err)
	}
	owned := "m162-budget-" + strings.Repeat("b", 24)
	foreign := "must-remain"
	for _, q := range []string{owned, foreign} {
		if err := e.CreateQueue(context.Background(), q, mq.QueueConfig{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	m := ownershipRecord{Schema: "m162-owned-queues-v1", Backend: "standard", RuntimeSHA: runtimeSHA, SDK_SHA: sdkSHA, RunnerSHA: runnerSHA, HostHash: localHostHash(), PID: 1 << 30, Queues: []ownedQueue{{Queue: owned, SDK: "native"}}}
	if err := writeJSON(out, "ownership.json", m); err != nil {
		t.Fatal(err)
	}
	original := []byte(`{"passed":false,"failure":"original-control-failure"}`)
	if err := os.WriteFile(filepath.Join(out, "result.json"), original, 0600); err != nil {
		t.Fatal(err)
	}
	if code := runCleanup(out, "standard", f.dsn, "", true); code != 0 {
		t.Fatalf("cleanup exit=%d", code)
	}
	after, err := os.ReadFile(filepath.Join(out, "result.json"))
	if err != nil || !bytes.Equal(original, after) {
		t.Fatal("cleanup overwrote original failure")
	}
	verify, err := mq.OpenEmbedded(context.Background(), f.dsn, mq.WithBrokerMode())
	if err != nil {
		t.Fatal(err)
	}
	defer verify.Close()
	qs, err := verify.ListQueues(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	foundForeign := false
	for _, q := range qs {
		if q.Name == owned {
			t.Fatal("owned queue remains")
		}
		foundForeign = foundForeign || q.Name == foreign
	}
	if !foundForeign {
		t.Fatal("cleanup removed unrelated queue")
	}
}

func TestProviderRejectsSchemeDowngrade(t *testing.T) {
	calls := 0
	p := &providerObserver{host: "provider.invalid", scheme: "https", next: rtFunc(func(*http.Request) (*http.Response, error) { calls++; return nil, nil })}
	req := traceRequest()
	req.URL.Scheme = "http"
	if _, err := p.RoundTrip(req); safeClass(err) != "unapproved_provider_scheme" || calls != 0 || p.snapshot().InvalidCapture != 1 {
		t.Fatal("HTTP downgrade was forwarded")
	}
}
