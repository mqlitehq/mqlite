//go:build mqlite_perf_diag

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type providerRoundTripFunc func(*http.Request) (*http.Response, error)

func (f providerRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func providerTestRequest(t *testing.T, target, body string) *http.Request {
	t.Helper()
	r, err := http.NewRequest(http.MethodPost, target+"/v2/pipeline", strings.NewReader(body))
	if err != nil {
		t.Fatal("construct request")
	}
	return r
}

func TestProviderTracePrivacyAndSQLBusy(t *testing.T) {
	const target = "https://private-provider.invalid"
	const sentinel = "not-a-real-credential-SENTINEL-7cgx"
	request := `{"baton":"` + sentinel + `","requests":[{"type":"execute","stmt":{"sql":"INSERT INTO messages(body) VALUES ('private SQL payload ` + sentinel + `')","args":[{"type":"text","value":"` + sentinel + `"}]}},{"type":"close"}]}`
	response := `{"baton":"` + sentinel + `","results":[{"type":"error","error":{"code":"SQLITE_BUSY","message":"` + target + ` ` + sentinel + `"}}]}`
	p, err := newProviderTrace(target, providerRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		data, e := io.ReadAll(r.Body)
		if e != nil || string(data) != request {
			t.Error("request body changed")
		}
		if r.Header.Get("Authorization") != "Bearer "+sentinel {
			t.Error("request authorization changed")
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Secret-Header": []string{sentinel}}, Body: io.NopCloser(strings.NewReader(response))}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	p.SetPhase("C1-B1/native/send")
	r := providerTestRequest(t, target, request)
	r.Header.Set("Authorization", "Bearer "+sentinel)
	resp, err := p.RoundTrip(r)
	if err != nil {
		t.Fatal("RoundTrip failed")
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil || string(data) != response {
		t.Fatal("response body changed")
	}
	if err = resp.Body.Close(); err != nil {
		t.Fatal("close failed")
	}
	s := p.Snapshot()
	if len(s.Events) != 1 {
		t.Fatal("missing event")
	}
	e := s.Events[0]
	if e.StatementCount != 1 || e.CloseCount != 1 || e.HTTPStatus != 200 || !e.BodyEOF || !e.BodyClosed || e.ErrorClass != "none" || !e.Finished {
		t.Fatalf("wrong safe counts: %+v", e)
	}
	if len(e.SQLiteCodes) != 1 || e.SQLiteCodes[0] != "SQLITE_BUSY" {
		t.Fatal("missing SQLite code")
	}
	if len(e.SQLKinds) != 2 || e.SQLKinds[0] != "insert" || e.SQLTags[0] != "message_insert" {
		t.Fatal("wrong SQL classification")
	}
	b, _ := json.Marshal(s)
	for _, private := range []string{target, "private-provider.invalid", sentinel, "private SQL payload", "Bearer", "Secret-Header", "baton"} {
		if bytes.Contains(b, []byte(private)) {
			t.Fatalf("private data appeared in snapshot: field %q", private)
		}
	}
	if s.Observed != 1 || s.Sent != 1 || s.Finished != 1 || s.Active != 0 || s.OpenBodies != 0 || s.Dropped != 0 {
		t.Fatal("unbalanced totals")
	}
}

type providerErrorBody struct {
	closeCalls        int
	readErr, closeErr error
	read              bool
}

func (b *providerErrorBody) Read(dst []byte) (int, error) {
	if b.read {
		return 0, b.readErr
	}
	b.read = true
	return copy(dst, "part"), b.readErr
}
func (b *providerErrorBody) Close() error { b.closeCalls++; return b.closeErr }

func TestProviderTracePreservesReadAndCloseErrors(t *testing.T) {
	const target = "https://errors.invalid"
	closeErr := errors.New("private close diagnostic must not be serialized")
	body := &providerErrorBody{readErr: io.ErrUnexpectedEOF, closeErr: closeErr}
	p, _ := newProviderTrace(target, providerRoundTripFunc(func(*http.Request) (*http.Response, error) { return &http.Response{StatusCode: 503, Body: body}, nil }))
	resp, err := p.RoundTrip(providerTestRequest(t, target, `{"requests":[{"type":"execute","stmt":{"sql":"COMMIT"}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	dst := make([]byte, 8)
	n, err := resp.Body.Read(dst)
	if n != 4 || string(dst[:n]) != "part" || err != io.ErrUnexpectedEOF {
		t.Fatal("read result changed")
	}
	for i := 0; i < 2; i++ {
		if resp.Body.Close() != closeErr {
			t.Fatal("close error changed")
		}
	}
	s := p.Snapshot()
	e := s.Events[0]
	if body.closeCalls != 2 || e.CloseCalls != 2 || e.ErrorClass != "unexpected_eof" || e.CloseErrorClass != "other" || e.BodyEOF || e.HTTPStatus != 503 || s.OpenBodies != 0 {
		t.Fatal("body/error evidence incorrect")
	}
	b, _ := json.Marshal(s)
	if bytes.Contains(b, []byte("private close diagnostic")) {
		t.Fatal("close diagnostic leaked")
	}
}

func TestProviderTraceRoundTripEOFIsNotBodyEOF(t *testing.T) {
	const target = "https://drop.invalid"
	p, _ := newProviderTrace(target, providerRoundTripFunc(func(*http.Request) (*http.Response, error) { return nil, io.EOF }))
	_, err := p.RoundTrip(providerTestRequest(t, target, `{"requests":[{"type":"execute","stmt":{"sql":"COMMIT"}}]}`))
	if err != io.EOF {
		t.Fatal("transport error identity changed")
	}
	e := p.Snapshot().Events[0]
	if e.ErrorClass != "eof" || e.BodyEOF || e.NotSent || !e.Finished {
		t.Fatal("transport EOF was hidden as normal body EOF")
	}
}

func TestProviderTraceGetBodyDoesNotConsumeOriginal(t *testing.T) {
	const target = "http://body.invalid"
	for _, mode := range []string{"unavailable", "copy_error", "too_large", "invalid_json"} {
		t.Run(mode, func(t *testing.T) {
			original := `{"requests":[{"type":"close"}]}`
			p, _ := newProviderTrace(target, providerRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				b, err := io.ReadAll(r.Body)
				if err != nil || string(b) != original {
					t.Error("original request was consumed or changed")
				}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}"))}, nil
			}))
			r := providerTestRequest(t, target, original)
			switch mode {
			case "unavailable":
				r.GetBody = nil
			case "copy_error":
				r.GetBody = func() (io.ReadCloser, error) { return nil, errors.New("private-copy-error") }
			case "too_large":
				r.GetBody = func() (io.ReadCloser, error) {
					return io.NopCloser(strings.NewReader(strings.Repeat("x", maxProviderInspectBytes+1))), nil
				}
			case "invalid_json":
				r.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader("private invalid JSON")), nil }
			}
			resp, err := p.RoundTrip(r)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			want := mode
			if mode == "unavailable" {
				want = "get_body_unavailable"
			}
			e := p.Snapshot().Events[0]
			if e.RequestInspection != want || e.StatementCount != 0 || e.CloseCount != 0 {
				t.Fatal("unknown request classification incorrect")
			}
		})
	}
}

func TestProviderTraceDelayCancellationIsNotSent(t *testing.T) {
	const target = "https://delay.invalid"
	var calls atomic.Int64
	p, _ := newProviderTrace(target, providerRoundTripFunc(func(*http.Request) (*http.Response, error) { calls.Add(1); return nil, errors.New("must not send") }))
	p.SetDelay(time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	r := providerTestRequest(t, target, `{"requests":[{"type":"close"}]}`).WithContext(ctx)
	_, err := p.RoundTrip(r)
	if !errors.Is(err, context.DeadlineExceeded) || calls.Load() != 0 {
		t.Fatal("delay cancellation sent a request")
	}
	s := p.Snapshot()
	e := s.Events[0]
	if !e.NotSent || e.ErrorClass != "deadline" || e.HTTPMS != 0 || e.SyntheticDelayMS != 1000 || e.SyntheticDelayElapsedMS <= 0 || !e.ContextHasDeadline || s.NotSent != 1 || s.Sent != 0 || s.Active != 0 {
		t.Fatal("delay and real HTTP time not separated")
	}
}

func TestProviderTraceDelayIsSeparateAndResettable(t *testing.T) {
	const target = "https://delay-reset.invalid"
	p, _ := newProviderTrace(target, providerRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}"))}, nil
	}))
	p.SetDelay(15 * time.Millisecond)
	for i := 0; i < 2; i++ {
		resp, err := p.RoundTrip(providerTestRequest(t, target, "{}"))
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		p.SetDelay(0)
	}
	s := p.Snapshot()
	if s.Events[0].SyntheticDelayMS != 15 || s.Events[0].SyntheticDelayElapsedMS < 15 || s.Events[1].SyntheticDelayMS != 0 || s.Events[1].SyntheticDelayElapsedMS != 0 {
		t.Fatal("synthetic delay was mixed or retained")
	}
	if s.Events[0].HTTPMS < 0 || s.Events[0].HeadersMS < 0 || s.Events[0].BodyMS < 0 {
		t.Fatal("negative timing")
	}
}

func TestProviderTraceNonTargetRequestsAreNotCountedOrDelayed(t *testing.T) {
	const target = "https://provider.invalid"
	var calls int
	p, _ := newProviderTrace(target, providerRoundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}"))}, nil
	}))
	p.SetDelay(time.Hour)
	for _, other := range []string{"http://127.0.0.1:6754", "https://provider.invalid:8443"} {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		resp, err := p.RoundTrip(providerTestRequest(t, other, "{}").WithContext(ctx))
		if err != nil {
			t.Fatal("non-target behavior changed")
		}
		_ = resp.Body.Close()
	}
	if calls != 2 || p.Snapshot().Observed != 0 {
		t.Fatal("non-provider request counted")
	}
}

func TestProviderTraceLateEOFAndClose(t *testing.T) {
	const target = "https://late.invalid"
	p, _ := newProviderTrace(target, providerRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}"))}, nil
	}))
	p.SetPhase("case/send")
	resp, err := p.RoundTrip(providerTestRequest(t, target, "{}"))
	if err != nil {
		t.Fatal(err)
	}
	p.SetPhase("cleanup")
	_, _ = io.Copy(io.Discard, resp.Body)
	before := p.Snapshot()
	if before.Active != 0 || before.OpenBodies != 1 || before.LateEOF != 1 {
		t.Fatal("EOF concealed an unclosed body")
	}
	p.SetPhase("tail")
	_ = resp.Body.Close()
	_ = resp.Body.Close()
	s := p.Snapshot()
	e := s.Events[0]
	if s.LateEOF != 1 || s.LateClose != 1 || s.OpenBodies != 0 || !e.LateEOF || !e.LateClose || e.Phase != "case/send" || e.FinishPhase != "cleanup" || e.EOFPhase != "cleanup" || e.ClosePhase != "tail" {
		t.Fatal("late body activity was not distinct")
	}
}

func TestProviderTraceTLSAndConnectionReuse(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, `{"results":[]}`) }))
	defer srv.Close()
	base := srv.Client().Transport.(*http.Transport).Clone()
	defer base.CloseIdleConnections()
	p, err := newProviderTrace(srv.URL, base)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: p}
	for i := 0; i < 2; i++ {
		resp, err := client.Do(providerTestRequest(t, srv.URL, `{"requests":[{"type":"execute","stmt":{"sql":"SELECT 1"}}]}`))
		if err != nil {
			t.Fatal("local TLS request failed")
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	s := p.Snapshot()
	a, b := s.Events[0], s.Events[1]
	if !a.ConnectionObserved || a.ConnectionReused || !a.ConnectObserved || !a.TLSObserved || !a.TTFBObserved || !b.ConnectionObserved || !b.ConnectionReused || b.TLSObserved {
		t.Fatal("TLS/reuse observations incorrect")
	}
	if a.HTTPMS < a.HeadersMS || a.HeadersMS < a.TTFBMS || a.ConnWaitMS < a.TLSMS {
		t.Fatal("timing spans are inconsistent")
	}
}

func TestProviderTraceBoundedEventsAndConcurrentSnapshots(t *testing.T) {
	const target = "https://bounded.invalid"
	p, _ := newProviderTrace(target, providerRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{}"))}, nil
	}))
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 32; i++ {
				resp, err := p.RoundTrip(providerTestRequest(t, target, `{"requests":[{"type":"execute","stmt":{"sql":"SELECT 1"}}]}`))
				if err != nil {
					t.Error(err)
					return
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				_ = p.Snapshot()
			}
		}()
	}
	wg.Wait()
	for i := 256; i < maxProviderTraceEvents+3; i++ {
		resp, _ := p.RoundTrip(providerTestRequest(t, target, "{}"))
		_ = resp.Body.Close()
	}
	s := p.Snapshot()
	if len(s.Events) != maxProviderTraceEvents || s.Dropped != 3 || s.Observed != maxProviderTraceEvents+3 || s.Sent != s.Observed || s.Finished != s.Observed || s.Active != 0 || s.OpenBodies != 0 {
		t.Fatal("events or dropped counters unbalanced")
	}
	s.Events[0].Phase = "changed"
	s.Events[0].SQLKinds[0] = "changed"
	again := p.Snapshot()
	if again.Events[0].Phase == "changed" || again.Events[0].SQLKinds[0] == "changed" {
		t.Fatal("snapshot aliases mutable state")
	}
}

func TestProviderTraceSQLiteCodeAllowlist(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		want       []string
	}{
		{"numeric", `{"results":[{"type":"error","error":{"code":5,"extended_code":517,"message":"private"}}]}`, 200, []string{"SQLITE_BUSY", "SQLITE_BUSY_SNAPSHOT"}},
		{"batch", `{"results":[{"type":"ok","response":{"type":"batch","result":{"step_errors":[null,{"code":"SQLITE_CONSTRAINT_UNIQUE","message":"private"}]}}}]}`, 200, []string{"SQLITE_CONSTRAINT_UNIQUE"}},
		{"http-error", `{"code":"SQLITE_LOCKED","message":"private"}`, 503, []string{"SQLITE_LOCKED"}},
		{"payload-not-error", `{"results":[{"type":"ok","response":{"type":"execute","result":{"rows":[[{"type":"text","value":"SQLITE_BUSY"}]]}}}]}`, 200, nil},
		{"untrusted-code", `{"results":[{"type":"error","error":{"code":"SQLITE_BUSY private-secret"}}]}`, 200, nil},
		{"invalid", `private-invalid-body`, 503, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := inspectProviderCodes([]byte(tc.body), tc.status)
			a, _ := json.Marshal(got)
			b, _ := json.Marshal(tc.want)
			if !bytes.Equal(a, b) {
				t.Fatalf("safe code mismatch got=%s want=%s", a, b)
			}
		})
	}
}

func TestProviderTraceLargeResponseAndUnsafePhase(t *testing.T) {
	const target = "https://large.invalid"
	body := strings.Repeat("x", maxProviderInspectBytes+32)
	p, _ := newProviderTrace(target, providerRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body))}, nil
	}))
	p.SetPhase("https://private-phase.invalid/token-secret")
	resp, err := p.RoundTrip(providerTestRequest(t, target, "{}"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil || string(got) != body {
		t.Fatal("inspection truncated actual response")
	}
	_ = resp.Body.Close()
	s := p.Snapshot()
	if !s.Events[0].ResponseInspectionTruncated || s.Events[0].Phase != "invalid_phase" || len(s.Events[0].SQLiteCodes) != 0 {
		t.Fatal("large response or phase evidence wrong")
	}
	b, _ := json.Marshal(s)
	if bytes.Contains(b, []byte("private-phase")) {
		t.Fatal("unsafe phase escaped validation")
	}
}

func TestProviderTraceInvalidTargetDoesNotEchoInput(t *testing.T) {
	for _, dsn := range []string{"https://user:private@host.invalid", "not-a-dsn-private", "file:/private/db", "https://host.invalid:invalid-private-port"} {
		_, err := newProviderTrace(dsn, http.DefaultTransport)
		if err == nil {
			t.Fatal("invalid target accepted")
		}
		if strings.Contains(err.Error(), "private") || strings.Contains(err.Error(), dsn) {
			t.Fatal("target parse error leaked input")
		}
	}
	for _, scheme := range []string{"libsql", "turso"} {
		p, err := newProviderTrace(scheme+"://target.invalid", http.DefaultTransport)
		if err != nil {
			t.Fatal(err)
		}
		r := providerTestRequest(t, "https://target.invalid", "{}")
		if !p.target(r.URL) {
			t.Fatal("native scheme did not match HTTPS target")
		}
	}
}

func TestProviderSQLClassesStayFixed(t *testing.T) {
	for _, tc := range []struct{ sql, kind, tag string }{
		{"-- private comment\nSELECT seq_number FROM dedup WHERE id=?", "select", "dedup_lookup"},
		{"/* private comment */ INSERT INTO messages(body) VALUES (?)", "insert", "message_insert"},
		{"COMMIT", "commit", "transaction_commit"},
		{"ROLLBACK", "rollback", "transaction_rollback"},
		{"CREATE TABLE private_identifier(x TEXT)", "ddl", "schema_ddl"},
		{"unrecognized-private-SQL", "unknown", "unknown"},
	} {
		kind, tag := providerSQLClass(tc.sql)
		if kind != tc.kind || tag != tc.tag {
			t.Fatal(fmt.Sprintf("fixed category mismatch got %s/%s", kind, tag))
		}
	}
}
