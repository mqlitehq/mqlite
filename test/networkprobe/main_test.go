package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type testBody struct {
	r      io.Reader
	closed int
	err    error
}

func (b *testBody) Read(p []byte) (int, error) {
	n, e := b.r.Read(p)
	if e == io.EOF && b.err != nil {
		return n, b.err
	}
	return n, e
}
func (b *testBody) Close() error { b.closed++; return nil }

const commitOK = `{"results":[{"type":"ok","response":{"type":"execute","result":{"cols":[],"rows":[],"affected_row_count":0}}}]}`

func TestRequestClassificationPrivacyAndPreservation(t *testing.T) {
	secret := "customer-body-token-secret.invalid"
	payload := `{"baton":"` + secret + `","requests":[{"type":"execute","stmt":{"sql":"CREATE TABLE IF NOT EXISTS private_name (body TEXT)","args":[{"type":"text","value":"` + secret + `"}]}},{"type":"execute","stmt":{"sql":"COMMIT"}}]}`
	req, err := http.NewRequest("POST", "https://private.invalid/v2/pipeline", strings.NewReader(payload))
	if err != nil {
		t.Fatal("request setup failed")
	}
	req.Header.Set("Authorization", "Bearer "+secret)
	got := requestCategories(req)
	if !hasCategory(got, "ddl_table") || !hasCategory(got, "commit") {
		t.Fatalf("unexpected fixed categories: %v", got)
	}
	actual, err := io.ReadAll(req.Body)
	if err != nil || string(actual) != payload {
		t.Fatal("classification changed original request")
	}
	b, err := req.GetBody()
	if err != nil {
		t.Fatal("GetBody unavailable after observation")
	}
	defer b.Close()
	copy, _ := io.ReadAll(b)
	if !bytes.Equal(actual, copy) {
		t.Fatal("GetBody changed")
	}
	encoded, _ := json.Marshal(got)
	for _, private := range []string{secret, "private_name", "private.invalid", "CREATE TABLE", "Authorization", "baton"} {
		if bytes.Contains(encoded, []byte(private)) {
			t.Fatal("classification leaked private data")
		}
	}
	large, _ := http.NewRequest("POST", "https://private.invalid", strings.NewReader(strings.Repeat("x", maxClassifyBytes+1)))
	if c := requestCategories(large); len(c) != 1 || c[0] != "unclassified" {
		t.Fatal("oversize classification not bounded")
	}
}

func TestResponseReadAndCloseIdentity(t *testing.T) {
	originalErr := errors.New("private response contains SQL token customer")
	body := &testBody{r: strings.NewReader("customer-response"), err: originalErr}
	o := &observer{allow: func(*url.URL) bool { return true }, base: roundTripFunc(func(*http.Request) (*http.Response, error) { return &http.Response{StatusCode: 200, Body: body}, nil })}
	req, _ := http.NewRequest("POST", "https://private.invalid", strings.NewReader(`{"requests":[]}`))
	response, err := o.RoundTrip(req)
	if err != nil {
		t.Fatal("roundtrip failed")
	}
	data, err := io.ReadAll(response.Body)
	if err != originalErr || string(data) != "customer-response" {
		t.Fatal("observer changed original bytes/read error")
	}
	_ = response.Body.Close()
	_ = response.Body.Close()
	events, dropped := o.snapshot()
	if body.closed != 1 || len(events) != 1 || dropped != 0 || events[0].BodyEOF {
		t.Fatal("incorrect completion/close accounting")
	}
	encoded, _ := json.Marshal(events)
	for _, s := range []string{"private.invalid", "customer-response", "SQL token customer"} {
		if bytes.Contains(encoded, []byte(s)) {
			t.Fatal("response observation leaked private text")
		}
	}
}

func TestCommitFaultOnlyAfterSuccessfulCompleteEnvelope(t *testing.T) {
	for _, tc := range []struct {
		name, payload string
		status        int
		want          bool
	}{
		{"success", commitOK, 200, true},
		{"wrong_status", commitOK, 503, false},
		{"empty_ok", `{"results":[{"type":"ok"}]}`, 200, false},
		{"empty_results", `{"results":[]}`, 200, false},
		{"error", `{"results":[{"type":"error","error":{"code":"SECRET","message":"private"}}]}`, 200, false},
		{"malformed", `{"results":[`, 200, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := &testBody{r: strings.NewReader(tc.payload)}
			o := &observer{armed: true, allow: func(*url.URL) bool { return true }, base: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: tc.status, Body: body}, nil
			})}
			req, _ := http.NewRequest("POST", "https://private.invalid", strings.NewReader(`{"requests":[{"type":"execute","stmt":{"sql":"COMMIT"}}]}`))
			resp, err := o.RoundTrip(req)
			if err != nil {
				t.Fatal("roundtrip failed")
			}
			got, err := io.ReadAll(resp.Body)
			if string(got) != tc.payload {
				t.Fatal("response bytes changed")
			}
			if (err == io.ErrUnexpectedEOF) != tc.want {
				t.Fatal("incorrect commit fault decision")
			}
			if !tc.want && err != nil {
				t.Fatal("observer changed normal EOF")
			}
			_ = resp.Body.Close()
			events, _ := o.snapshot()
			if len(events) != 1 || events[0].InjectedEOF != tc.want || o.injected != boolInt(tc.want) {
				t.Fatal("fault accounting differs")
			}
		})
	}
}
func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func TestFaultNotInjectedBeforeTruncatedResponse(t *testing.T) {
	body := &testBody{r: strings.NewReader(commitOK), err: io.ErrUnexpectedEOF}
	o := &observer{armed: true, allow: func(*url.URL) bool { return true }, base: roundTripFunc(func(*http.Request) (*http.Response, error) { return &http.Response{StatusCode: 200, Body: body}, nil })}
	req, _ := http.NewRequest("POST", "https://private.invalid", strings.NewReader(`{"requests":[{"stmt":{"sql":"COMMIT"}}]}`))
	resp, err := o.RoundTrip(req)
	if err != nil {
		t.Fatal("roundtrip failed")
	}
	_, err = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != io.ErrUnexpectedEOF || o.injected != 0 {
		t.Fatal("truncated original response counted as injected post-commit fault")
	}
}

func TestRealHTTPTraceAndReuse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Error("request method changed")
		}
		_, _ = io.Copy(io.Discard, r.Body)
		_, _ = io.WriteString(w, commitOK)
	}))
	defer server.Close()
	base := http.DefaultTransport.(*http.Transport).Clone()
	defer base.CloseIdleConnections()
	o := &observer{base: base, allow: func(u *url.URL) bool {
		return u.Scheme == "http" && u.Host == strings.TrimPrefix(server.URL, "http://")
	}}
	client := &http.Client{Transport: o}
	for i := 0; i < 2; i++ {
		req, _ := http.NewRequestWithContext(context.WithValue(context.Background(), operationKey{}, "open"), "POST", server.URL, strings.NewReader(`{"requests":[{"stmt":{"sql":"SELECT value FROM meta WHERE key='schema_version'"}}]}`))
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal("real HTTP request failed")
		}
		data, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil || string(data) != commitOK {
			t.Fatal("observer changed normal body")
		}
	}
	events, _ := o.snapshot()
	if len(events) != 2 || !events[1].Reused || !events[0].BodyEOF || events[0].Error.Kind != "none" || events[0].FirstByteMs <= 0 || events[0].Operation != "open" {
		t.Fatalf("unexpected safe trace: %+v", events)
	}
}

func TestBoundsAllowlistAndTransportErrors(t *testing.T) {
	for _, raw := range []string{"libsql://unexpected.invalid", "https://" + verificationHost, "libsql://user@" + verificationHost, "libsql://" + verificationHost + "?token=private", "libsql://" + verificationHost + "/path"} {
		if approvedDSN(raw) {
			t.Fatal("unapproved DSN accepted")
		}
	}
	if !approvedDSN("libsql://" + verificationHost) {
		t.Fatal("verification allowlist rejected")
	}
	called := false
	o := &observer{allow: func(*url.URL) bool { return false }, base: roundTripFunc(func(*http.Request) (*http.Response, error) { called = true; return nil, nil })}
	req, _ := http.NewRequest("POST", "https://private.invalid", nil)
	if _, err := o.RoundTrip(req); err == nil || called {
		t.Fatal("unapproved request reached network")
	}
	for i := 0; i < maxProbeEvents+50; i++ {
		o.add(event{Operation: "open"})
	}
	events, dropped := o.snapshot()
	if len(events) != maxProbeEvents || dropped != 50 {
		t.Fatal("event buffer is unbounded")
	}
	original := errors.New("secret provider hostname and body")
	o = &observer{allow: func(*url.URL) bool { return true }, base: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, original })}
	if _, err := o.RoundTrip(req); err != original {
		t.Fatal("observer changed transport error identity")
	}
}
