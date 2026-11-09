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
	"os"
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

// MQLITE-190: physical evidence for anonymous pending renewal groups.
func membershipFixture() (brokerEvent, providerHTTPEvent) {
	pair := renewalPairHash("owned", 7, "private-token")
	logical := brokerEvent{BrokerRequestID: 1, Method: "RenewBatch", Status: 200, ServerReceived: true, ServerReceivedMS: 1,
		Renewal: &logicalRenewal{RequestValid: true, Queue: shaString("owned"), Request: []renewalPosition{{7, pair}}, ResponseValid: true,
			Response: []renewalACK{{7, pair, true, 250}}, EOF: true, Closed: true, EOFMS: 5, CloseMS: 6, CloseError: "none"}}
	physical := providerHTTPEvent{ID: 10, HTTPStatus: 200, StartedMS: 2, FinishedMS: 3, Finished: true, BodyEOF: true, BodyClosed: true,
		BodyEOFMS: 3, BodyCloseMS: 4, ProtocolSuccess: true, ErrorClass: "none", CloseErrorClass: "none", StatementCount: 1,
		Renewal: &physicalRenewal{RequestValid: true, Kind: renewalMembershipKind, Queue: shaString("owned"), Pairs: []string{pair}, RowsValid: true, Rows: []renewalRow{{pair, 250}}}}
	return logical, physical
}
func cloneMembershipFixture(l brokerEvent, p providerHTTPEvent) (brokerEvent, providerHTTPEvent) {
	data, _ := json.Marshal(l)
	var a brokerEvent
	_ = json.Unmarshal(data, &a)
	data, _ = json.Marshal(p)
	var b providerHTTPEvent
	_ = json.Unmarshal(data, &b)
	return a, b
}
func TestRenewalMembershipAnonymousAndDirectPositive(t *testing.T) {
	for _, name := range []string{"anonymous-single-pair", "direct-verified-id", "same-pair-distinct-rpcs", "different-pairs-distinct-rpcs", "duplicate-input-positions"} {
		t.Run(name, func(t *testing.T) {
			l, p := membershipFixture()
			requests := []brokerEvent{l}
			wantMany := 0
			switch name {
			case "direct-verified-id":
				p.BrokerRequestID = l.BrokerRequestID
			case "same-pair-distinct-rpcs":
				other, _ := cloneMembershipFixture(l, p)
				other.BrokerRequestID = 2
				requests = append(requests, other)
				wantMany = 1
			case "different-pairs-distinct-rpcs":
				other, _ := cloneMembershipFixture(l, p)
				other.BrokerRequestID = 2
				pair := renewalPairHash("owned", 8, "other")
				other.Renewal.Request = []renewalPosition{{8, pair}}
				other.Renewal.Response = []renewalACK{{8, pair, true, 400}}
				requests = append(requests, other)
				p.Renewal.Pairs = append(p.Renewal.Pairs, pair)
				p.Renewal.Rows = append(p.Renewal.Rows, renewalRow{pair, 400})
				wantMany = 1
			case "duplicate-input-positions":
				l.Renewal.Request = append(l.Renewal.Request, l.Renewal.Request[0])
				l.Renewal.Response = append(l.Renewal.Response, l.Renewal.Response[0])
				requests[0] = l
			}
			got := verifyRenewalMembership(true, len(requests), requests, []providerHTTPEvent{p}, 0, 0)
			if !got.Passed || got.ManyToOneEvents != wantMany || got.MatchedRPCs != len(requests) {
				t.Fatalf("membership=%+v", got)
			}
		})
	}
}
func TestRenewalMembershipRejectsFalsePhysicalProof(t *testing.T) {
	names := []string{"wrong-queue", "wrong-pair", "missing-row", "wrong-deadline", "duplicate-row", "extra-row", "missing-eof", "missing-close", "close-error", "http201", "protocol-failed", "sqlite-error", "truncated", "not-sent", "not-finished", "outside-rpc", "close-after-ack", "borrowed-leader-id", "duplicate-event-id", "ambiguous-events", "duplicate-rpc-id", "missing-request", "missing-response", "wrong-ack-sequence", "logical-http201", "logical-close-error", "trace-drop", "broker-drop"}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			l, p := membershipFixture()
			requests := []brokerEvent{l}
			events := []providerHTTPEvent{p}
			providerDrop := uint64(0)
			brokerDrop := 0
			switch name {
			case "wrong-queue":
				p.Renewal.Queue = shaString("other")
			case "wrong-pair":
				p.Renewal.Pairs = []string{shaString("wrong")}
			case "missing-row":
				p.Renewal.Rows = nil
			case "wrong-deadline":
				p.Renewal.Rows[0].Deadline++
			case "duplicate-row":
				p.Renewal.Rows = append(p.Renewal.Rows, p.Renewal.Rows[0])
			case "extra-row":
				p.Renewal.Rows = append(p.Renewal.Rows, renewalRow{shaString("unrequested"), 250})
			case "missing-eof":
				p.BodyEOF = false
			case "missing-close":
				p.BodyClosed = false
			case "close-error":
				p.CloseErrorClass = "io"
			case "http201":
				p.HTTPStatus = 201
			case "protocol-failed":
				p.ProtocolSuccess = false
			case "sqlite-error":
				p.SQLiteCodes = []string{"SQLITE_BUSY"}
			case "truncated":
				p.ResponseInspectionTruncated = true
			case "not-sent":
				p.NotSent = true
			case "not-finished":
				p.Finished = false
			case "outside-rpc":
				p.StartedMS = 0
			case "close-after-ack":
				p.BodyCloseMS = 5.1
			case "borrowed-leader-id":
				p.BrokerRequestID = 2
			case "duplicate-event-id":
				events = append(events, p)
			case "ambiguous-events":
				other := p
				other.ID = 11
				events = append(events, other)
			case "duplicate-rpc-id":
				requests = append(requests, l)
			case "missing-request":
				l.Renewal.RequestValid = false
			case "missing-response":
				l.Renewal.ResponseValid = false
			case "wrong-ack-sequence":
				l.Renewal.Response[0].Sequence++
			case "logical-http201":
				l.Status = 201
			case "logical-close-error":
				l.Renewal.CloseError = "io"
			case "trace-drop":
				providerDrop = 1
			case "broker-drop":
				brokerDrop = 1
			}
			requests[0] = l
			events[0] = p
			if got := verifyRenewalMembership(true, 1, requests, events, brokerDrop, providerDrop); got.Passed {
				t.Fatalf("false physical proof accepted: %+v", got)
			}
		})
	}
}
func TestRenewalMembershipCannotStitchOneRPCFromTwoSQLStatements(t *testing.T) {
	l, p := membershipFixture()
	pair := renewalPairHash("owned", 8, "other")
	l.Renewal.Request = append(l.Renewal.Request, renewalPosition{8, pair})
	l.Renewal.Response = append(l.Renewal.Response, renewalACK{8, pair, true, 400})
	_, other := cloneMembershipFixture(l, p)
	other.ID = 11
	other.Renewal.Pairs = []string{pair}
	other.Renewal.Rows = []renewalRow{{pair, 400}}
	if got := verifyRenewalMembership(true, 1, []brokerEvent{l}, []providerHTTPEvent{p, other}, 0, 0); got.Passed {
		t.Fatal("one-item-at-a-time evidence was stitched into a batch")
	}
}
func TestRenewalMembershipPartialDoesNotInventAcknowledgementsOrCoalescing(t *testing.T) {
	l, p := membershipFixture()
	other, _ := cloneMembershipFixture(l, p)
	other.BrokerRequestID = 2
	pair := renewalPairHash("owned", 8, "other")
	other.Renewal.Request = append(other.Renewal.Request, renewalPosition{8, pair})
	other.Renewal.Response = append(other.Renewal.Response, renewalACK{8, pair, false, 0})
	p.Renewal.Pairs = append(p.Renewal.Pairs, pair)
	got := verifyRenewalMembership(true, 1, []brokerEvent{l, other}, []providerHTTPEvent{p}, 0, 0)
	if !got.Passed || got.SuccessfulRPCs != 1 || got.AcknowledgedRPCs != 2 || got.ManyToOneEvents != 0 || len(got.Uncovered) != 1 {
		t.Fatalf("partial/canceled pairs were made successful: %+v", got)
	}
}
func TestRenewalMembershipLocalIsExplicitlyNotApplicable(t *testing.T) {
	got := verifyRenewalMembership(false, 0, nil, nil, 0, 0)
	if !got.Passed || got.Required || got.Status != "not_applicable_local" || got.ManyToOneEvents != 0 {
		t.Fatalf("local applicability=%+v", got)
	}
}
func membershipRequest(t *testing.T, sqlText string) *http.Request {
	t.Helper()
	args := []map[string]any{{"type": "integer", "value": "100"}, {"type": "integer", "value": "10"}, {"type": "integer", "value": "10"}, {"type": "text", "value": "owned"}, {"type": "integer", "value": "7"}, {"type": "text", "value": "PRIVATE_MEMBERSHIP_CANARY"}}
	data, _ := json.Marshal(map[string]any{"requests": []any{map[string]any{"type": "execute", "stmt": map[string]any{"sql": sqlText, "args": args, "want_rows": true}}}})
	return providerTestRequest(t, "https://provider.invalid", string(data))
}
func TestRenewalMembershipPhysicalParserUsesExactStatementAndActualReturnedDeadline(t *testing.T) {
	req := membershipRequest(t, membershipSQL(1))
	v, q := inspectPhysicalRenewal(req)
	if v == nil || !v.RequestValid || q != "owned" {
		t.Fatal("exact request not recognized")
	}
	response := `{"results":[{"type":"ok","response":{"type":"execute","result":{"cols":[{"name":"id"},{"name":"lock_token"},{"name":"locked_until"}],"rows":[[{"type":"integer","value":"7"},{"type":"text","value":"PRIVATE_MEMBERSHIP_CANARY"},{"type":"integer","value":"250"}]],"affected_row_count":1}}}]}`
	inspectPhysicalRenewalRows(q, []byte(response), v)
	if !v.RowsValid || len(v.Rows) != 1 || v.Rows[0].Deadline != 250 {
		t.Fatalf("actual MAX deadline not retained: %+v", v)
	}
	encoded, _ := json.Marshal(v)
	if bytes.Contains(encoded, []byte("PRIVATE_MEMBERSHIP_CANARY")) || bytes.Contains(encoded, []byte("UPDATE messages")) {
		t.Fatal("private SQL/token escaped observer")
	}
	bad := membershipRequest(t, membershipSQL(1)+"; UPDATE unrelated SET value=1")
	if observed, _ := inspectPhysicalRenewal(bad); observed != nil && observed.RequestValid {
		t.Fatal("extra SQL was treated as one renewal")
	}
	for _, name := range []string{"wrong-columns", "duplicate-returned-pair", "wrong-token", "extra-result-slot"} {
		t.Run(name, func(t *testing.T) {
			copy, _ := inspectPhysicalRenewal(req)
			data := response
			switch name {
			case "wrong-columns":
				data = strings.ReplaceAll(data, `"locked_until"`, `"wrong"`)
			case "duplicate-returned-pair":
				data = strings.Replace(data, `]],"affected_row_count"`, `],[{"type":"integer","value":"7"},{"type":"text","value":"PRIVATE_MEMBERSHIP_CANARY"},{"type":"integer","value":"250"}]],"affected_row_count"`, 1)
			case "wrong-token":
				data = strings.ReplaceAll(data, "PRIVATE_MEMBERSHIP_CANARY", "WRONG")
			case "extra-result-slot":
				data = strings.TrimSuffix(data, "]}") + `,{"type":"ok","response":{"type":"close"}}]}`
			}
			inspectPhysicalRenewalRows(q, []byte(data), copy)
			if copy.RowsValid {
				t.Fatal("unproven returned rows accepted")
			}
		})
	}
}

func TestRenewalMembershipDoesNotSilentlySkipDamagedPartialACK(t *testing.T) {
	for _, kind := range []string{"missing-close", "wrong-sequence", "unverified"} {
		t.Run(kind, func(t *testing.T) {
			l, p := membershipFixture()
			other, _ := cloneMembershipFixture(l, p)
			other.BrokerRequestID = 2
			pair := renewalPairHash("owned", 8, "other")
			other.Renewal.Request = append(other.Renewal.Request, renewalPosition{8, pair})
			other.Renewal.Response = append(other.Renewal.Response, renewalACK{8, pair, false, 0})
			p.Renewal.Pairs = append(p.Renewal.Pairs, pair)
			switch kind {
			case "missing-close":
				other.Renewal.Closed = false
			case "wrong-sequence":
				other.Renewal.Response[0].Sequence++
			case "unverified":
				other.ServerReceived = false
			}
			if got := verifyRenewalMembership(true, 1, []brokerEvent{l, other}, []providerHTTPEvent{p}, 0, 0); got.Passed {
				t.Fatal("damaged partial true ACK bypassed proof through all-true counter")
			}
		})
	}
}
func TestRenewalMembershipProviderObserverCapturesEOFAndDelegatedClose(t *testing.T) {
	response := `{"results":[{"type":"ok","response":{"type":"execute","result":{"cols":[{"name":"id"},{"name":"lock_token"},{"name":"locked_until"}],"rows":[[{"type":"integer","value":"7"},{"type":"text","value":"PRIVATE_MEMBERSHIP_CANARY"},{"type":"integer","value":"250"}]],"affected_row_count":1}}}]}`
	trace, err := newProviderTrace("https://provider.invalid", providerRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(response))}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := trace.RoundTrip(membershipRequest(t, membershipSQL(1)))
	if err != nil {
		t.Fatal(err)
	}
	before := trace.Snapshot()
	if before.Events[0].Renewal.RowsValid || before.Events[0].BodyEOF {
		t.Fatal("observer read the live body eagerly")
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil || string(data) != response {
		t.Fatal("body changed")
	}
	if err = resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	after := trace.Snapshot()
	e := after.Events[0]
	if !e.ProtocolSuccess || !e.BodyEOF || !e.BodyClosed || !e.Renewal.RowsValid || e.Renewal.Rows[0].Deadline != 250 || e.BodyCloseMS < e.BodyEOFMS || after.Active != 0 || after.OpenBodies != 0 {
		t.Fatalf("incomplete actual body evidence: %+v", e)
	}
	encoded, _ := json.Marshal(e)
	if bytes.Contains(encoded, []byte("PRIVATE_MEMBERSHIP_CANARY")) {
		t.Fatal("token leaked")
	}
	trace.mu.Lock()
	trace.events[0].e.Renewal.Rows[0].Deadline = 999
	trace.mu.Unlock()
	if after.Events[0].Renewal.Rows[0].Deadline != 250 {
		t.Fatal("snapshot aliases mutable RETURNING evidence")
	}
}

func TestRenewalMembershipSharedCrossLanguageVectors(t *testing.T) {
	data, err := os.ReadFile("renewal-membership-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors []struct {
		Name    string     `json:"name"`
		Backend string     `json:"backend"`
		Case    caseResult `json:"case"`
		Passed  bool       `json:"expected_membership_passed"`
		Many    *int       `json:"expected_many_to_one_events"`
	}
	if json.Unmarshal(data, &vectors) != nil || len(vectors) != 49 {
		t.Fatal("shared vector inventory changed")
	}
	for _, v := range vectors {
		t.Run(v.Name, func(t *testing.T) {
			expected := 0
			if v.Case.Mixed != nil {
				expected = v.Case.Mixed.RenewalResponses
			}
			got := verifyRenewalMembership(v.Backend != "sqlite", expected, v.Case.BrokerHTTP, v.Case.Provider.Events, v.Case.BrokerHTTPDropped, v.Case.Provider.Dropped)
			encoded, err := json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("membership-result %s", encoded)
			if got.Passed != v.Passed || v.Many != nil && got.ManyToOneEvents != *v.Many {
				t.Fatalf("shared vector %s: got=%+v expected_pass=%v many=%v", v.Name, got, v.Passed, v.Many)
			}
		})
	}
}

func TestRenewalMembershipCompleteEmptyRowsAreObservedNotMissing(t *testing.T) {
	response := `{"results":[{"type":"ok","response":{"type":"execute","result":{"cols":[{"name":"id"},{"name":"lock_token"},{"name":"locked_until"}],"rows":[],"affected_row_count":0}}}]}`
	trace, err := newProviderTrace("https://provider.invalid", providerRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(response))}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := trace.RoundTrip(membershipRequest(t, membershipSQL(1)))
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if err = resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	observed := trace.Snapshot().Events[0].Renewal
	if observed == nil || !observed.RowsValid || observed.Rows == nil || len(observed.Rows) != 0 {
		t.Fatalf("actual empty RETURNING lost: %+v", observed)
	}
	l, p := membershipFixture()
	other := p
	other.ID = 11
	copy := *observed
	other.Renewal = &copy
	// The extra complete statement has no successful logical ACK. Its empty
	// result is not missing evidence, and cannot count as another merged caller.
	got := verifyRenewalMembership(true, 1, []brokerEvent{l}, []providerHTTPEvent{p, other}, 0, 0)
	if !got.Passed || got.ManyToOneEvents != 0 || len(got.Uncovered) != 1 {
		t.Fatalf("empty/false result was mistaken for corruption or success: %+v", got)
	}
}

func TestRenewalMembershipIncomplete200CannotManufactureUniqueness(t *testing.T) {
	for _, kind := range []string{"truncated", "not-eof", "not-closed", "bad-rows"} {
		t.Run(kind, func(t *testing.T) {
			l, p := membershipFixture()
			other, _ := cloneMembershipFixture(l, p)
			other.BrokerRequestID = 2
			_, unknown := cloneMembershipFixture(l, p)
			unknown.ID = 11
			unknown.Renewal.RowsValid = false
			unknown.Renewal.Rows = nil
			unknown.ProtocolSuccess = false
			switch kind {
			case "truncated":
				unknown.ResponseInspectionTruncated = true
			case "not-eof":
				unknown.BodyEOF = false
			case "not-closed":
				unknown.BodyClosed = false
			case "bad-rows":
				unknown.ProtocolSuccess = true
			}
			if got := verifyRenewalMembership(true, 2, []brokerEvent{l, other}, []providerHTTPEvent{p, unknown}, 0, 0); got.Passed {
				t.Fatal("incomplete200 was treated as definite failed SQL to manufacture unique origin")
			}
		})
	}
}
func TestRenewalMembershipExplicitProtocolRejectionIsNotUnknownSuccess(t *testing.T) {
	if !inspectRenewalRejection([]byte(`{"results":[{"type":"error","error":{"code":"SQLITE_BUSY","message":"PRIVATE_ERROR_CANARY"}}]}`)) {
		t.Fatal("complete rejection not recognized")
	}
	for _, data := range []string{`{`, `{"results":[]}`, `{"results":[{"type":"ok","response":{}}]}`, `{"results":[{"type":"error","error":null}]}`} {
		if inspectRenewalRejection([]byte(data)) {
			t.Fatal("missing or unknown success was called a rejection")
		}
	}
}
