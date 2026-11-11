package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"unicode"
	"unicode/utf8"
)

const providerObservationLimit = 256
const providerBodyLimit = 16 << 10

type providerEvent struct {
	Stage             string   `json:"stage"`
	RequestKind       string   `json:"request_kind"`
	HTTPStatus        int      `json:"http_status"`
	Code              string   `json:"code,omitempty"`
	CodeType          string   `json:"code_type,omitempty"`
	Message           string   `json:"message,omitempty"`
	SQLMessageOmitted bool     `json:"sql_message_omitted,omitempty"`
	BodyPrefixSHA256  string   `json:"body_prefix_sha256"`
	BodyObservedBytes int      `json:"body_observed_bytes"`
	BodyLimitReached  bool     `json:"body_limit_reached"`
	TypeChain         []string `json:"type_chain"`
}

type providerSnapshot struct {
	Events  []providerEvent `json:"events"`
	Dropped int             `json:"dropped"`
}

type providerObserver struct {
	mu      sync.Mutex
	stage   string
	events  []providerEvent
	dropped int
	host    string
	port    string
	secrets []string
}

var providerURLPattern = regexp.MustCompile(`(?i)\b(?:https?|libsql|wss?)(?:://|%3a%2f%2f)[^\s<>"']+`)
var providerBearerPattern = regexp.MustCompile(`(?i)\bbearer\s+[A-Za-z0-9._~+/=-]+`)
var providerJWTPattern = regexp.MustCompile(`\b[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b`)
var providerSQLPattern = regexp.MustCompile(`(?i)\b(?:select|insert|update|delete|create|alter|drop|pragma|with|replace|returning|where)\b|--|/\*|\*/`)
var providerCodePattern = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,96}$`)

func providerPort(u *url.URL) string {
	if port := u.Port(); port != "" {
		return port
	}
	if u.Scheme == "http" || u.Scheme == "ws" {
		return "80"
	}
	return "443"
}

// The observer adds no request and never stores request SQL or raw response data.
// Install and restore it outside concurrent mutations of http.DefaultTransport.
func observeProvider(dsn, token string) (*providerObserver, func()) {
	o := &providerObserver{}
	for _, secret := range []string{dsn, token} {
		if secret == "" {
			continue
		}
		o.secrets = append(o.secrets, secret, url.QueryEscape(secret), url.PathEscape(secret),
			url.QueryEscape(url.PathEscape(secret)), url.PathEscape(url.QueryEscape(secret)))
	}
	sort.Slice(o.secrets, func(i, j int) bool { return len(o.secrets[i]) > len(o.secrets[j]) })
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() == "" || (u.Scheme != "libsql" && u.Scheme != "https" && u.Scheme != "http") || http.DefaultTransport == nil {
		return o, func() {}
	}
	o.host, o.port = strings.ToLower(u.Hostname()), providerPort(u)
	previous := http.DefaultTransport
	wrapper := &providerTransport{base: previous, observer: o}
	http.DefaultTransport = wrapper
	var once sync.Once
	return o, func() {
		once.Do(func() {
			if http.DefaultTransport == wrapper {
				http.DefaultTransport = previous
			}
		})
	}
}

func providerBounded(text string, maximum int) string {
	text = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, text)
	if len(text) > maximum {
		text = text[:maximum]
		for !utf8.ValidString(text) {
			text = text[:len(text)-1]
		}
	}
	return text
}

func (o *providerObserver) sanitize(text string, maximum int) string {
	for _, secret := range o.secrets {
		text = strings.ReplaceAll(text, secret, "[redacted]")
	}
	text = providerURLPattern.ReplaceAllString(text, "[redacted-url]")
	text = providerBearerPattern.ReplaceAllString(text, "[redacted-bearer]")
	text = providerJWTPattern.ReplaceAllString(text, "[redacted-jwt]")
	return providerBounded(text, maximum)
}

func (o *providerObserver) setStage(stage string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.stage = o.sanitize(stage, 96)
}

func (o *providerObserver) snapshot() providerSnapshot {
	o.mu.Lock()
	defer o.mu.Unlock()
	x := providerSnapshot{Events: make([]providerEvent, len(o.events)), Dropped: o.dropped}
	copy(x.Events, o.events)
	for i := range x.Events {
		x.Events[i].TypeChain = append([]string(nil), x.Events[i].TypeChain...)
	}
	return x
}

func (o *providerObserver) add(event providerEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.events) == providerObservationLimit {
		o.dropped++
		return
	}
	event.Stage = o.stage
	o.events = append(o.events, event)
}

type providerCapture struct {
	io.ReadCloser
	mu     sync.Mutex
	prefix []byte
}

func (r *providerCapture) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.mu.Lock()
	left := providerBodyLimit - len(r.prefix)
	if left > n {
		left = n
	}
	if left > 0 {
		r.prefix = append(r.prefix, p[:left]...)
	}
	r.mu.Unlock()
	return n, err
}

func (r *providerCapture) kind() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var body struct {
		Requests []struct {
			Type string `json:"type"`
		} `json:"requests"`
	}
	if json.Unmarshal(r.prefix, &body) != nil || len(body.Requests) == 0 {
		return "unknown"
	}
	closeRequest, sqlRequest := false, false
	for _, request := range body.Requests {
		switch request.Type {
		case "close":
			closeRequest = true
		case "execute", "batch", "sequence", "describe":
			sqlRequest = true
		}
	}
	if closeRequest && sqlRequest {
		return "sql+close"
	}
	if closeRequest {
		return "close"
	}
	if sqlRequest {
		return "sql"
	}
	return "unknown"
}

type providerReplay struct {
	original io.ReadCloser
	prefix   *bytes.Reader
	err      error
}

func (r *providerReplay) Read(p []byte) (int, error) {
	if r.prefix.Len() > 0 {
		return r.prefix.Read(p)
	}
	if r.err != nil {
		err := r.err
		r.err = nil
		return 0, err
	}
	return r.original.Read(p)
}

func (r *providerReplay) Close() error { return r.original.Close() }

type providerTransport struct {
	base     http.RoundTripper
	observer *providerObserver
}

func (t *providerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	o := t.observer
	if request.URL == nil || strings.ToLower(request.URL.Hostname()) != o.host || providerPort(request.URL) != o.port {
		return t.base.RoundTrip(request)
	}
	copyRequest := request.Clone(request.Context())
	var captured *providerCapture
	if request.Body != nil {
		captured = &providerCapture{ReadCloser: request.Body}
		copyRequest.Body = captured
	}
	response, err := t.base.RoundTrip(copyRequest)
	if response == nil || response.StatusCode < 400 {
		return response, err
	}
	event := providerEvent{HTTPStatus: response.StatusCode, RequestKind: "unknown", TypeChain: []string{"http.Response"}}
	if captured != nil {
		event.RequestKind = captured.kind()
	}
	for cause, count := err, 0; cause != nil && count < 8; cause, count = errors.Unwrap(cause), count+1 {
		event.TypeChain = append(event.TypeChain, providerBounded(fmt.Sprintf("%T", cause), 96))
	}
	var raw []byte
	if response.Body != nil {
		var readErr error
		raw, readErr = io.ReadAll(io.LimitReader(response.Body, providerBodyLimit))
		if errors.Is(readErr, io.EOF) {
			readErr = nil
		}
		response.Body = &providerReplay{original: response.Body, prefix: bytes.NewReader(raw), err: readErr}
	}
	sum := sha256.Sum256(raw)
	event.BodyPrefixSHA256 = hex.EncodeToString(sum[:])
	event.BodyObservedBytes, event.BodyLimitReached = len(raw), len(raw) == providerBodyLimit
	var envelope map[string]json.RawMessage
	if json.Unmarshal(raw, &envelope) == nil {
		event.TypeChain = append(event.TypeChain, "json.object")
		detail := envelope
		if json.Unmarshal(envelope["error"], &detail) != nil || detail == nil {
			detail = envelope
		}
		if results := envelope["results"]; results != nil {
			var rows []struct {
				Type  string                     `json:"type"`
				Error map[string]json.RawMessage `json:"error"`
			}
			if json.Unmarshal(results, &rows) == nil {
				for _, row := range rows {
					if row.Type == "error" && row.Error != nil {
						detail = row.Error
						break
					}
				}
			}
		}
		var code string
		if json.Unmarshal(detail["code"], &code) == nil && code != "" {
			event.CodeType = "string"
			if safe := o.sanitize(code, 96); providerCodePattern.MatchString(safe) {
				event.Code = safe
			}
		} else if number := detail["code"]; len(number) > 0 && len(number) < 32 {
			var value json.Number
			if json.Unmarshal(number, &value) == nil {
				if _, parseErr := value.Int64(); parseErr == nil {
					event.CodeType, event.Code = "number", value.String()
				}
			}
		}
		var message string
		if json.Unmarshal(detail["message"], &message) != nil {
			_ = json.Unmarshal(envelope["error"], &message)
		}
		if providerSQLPattern.MatchString(message) {
			event.SQLMessageOmitted = true
		} else {
			event.Message = o.sanitize(message, 512)
		}
	} else {
		event.TypeChain = append(event.TypeChain, "unparsed.body")
	}
	o.add(event)
	return response, err
}

type providerTestTransport func(*http.Request) (*http.Response, error)

func (f providerTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestProviderObserverTransparentBoundedAndRedacted(t *testing.T) {
	dsn, token := "libsql://approved.invalid?auth=dsn-secret", "opaque+/token=secret"
	message := strings.Join([]string{dsn, token, url.QueryEscape(dsn), url.PathEscape(token), "Bearer unknown-secret", "eyJheader.eyJpayload.signature", "https://other.invalid?secret=another"}, " ")
	raw, _ := json.Marshal(map[string]any{"error": map[string]any{"code": "SQLITE_ERROR", "message": message}})
	previous := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = previous })
	requestBytes := []byte(`{"requests":[{"type":"close"}]}`)
	http.DefaultTransport = providerTestTransport(func(r *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Equal(body, requestBytes) {
			t.Fatal("request bytes changed")
		}
		return &http.Response{StatusCode: 400, Body: io.NopCloser(bytes.NewReader(raw)), Header: http.Header{"X-Test": []string{"preserved"}}}, nil
	})
	o, restore := observeProvider(dsn, token)
	defer restore()
	o.setStage("close-owned")
	request, _ := http.NewRequest(http.MethodPost, "https://approved.invalid/v2/pipeline", bytes.NewReader(requestBytes))
	response, err := http.DefaultTransport.RoundTrip(request)
	if err != nil || response.Header.Get("X-Test") != "preserved" {
		t.Fatal("response envelope changed")
	}
	actual, err := io.ReadAll(response.Body)
	if err != nil || !bytes.Equal(actual, raw) || response.Body.Close() != nil {
		t.Fatal("response bytes changed")
	}
	snapshot := o.snapshot()
	if len(snapshot.Events) != 1 || snapshot.Events[0].RequestKind != "close" || snapshot.Events[0].Code != "SQLITE_ERROR" || snapshot.Events[0].Stage != "close-owned" {
		t.Fatal("safe diagnostic identity missing")
	}
	encoded, _ := json.Marshal(snapshot)
	for _, secret := range []string{dsn, token, url.QueryEscape(dsn), url.PathEscape(token), "unknown-secret", "eyJheader", "other.invalid", "dsn-secret"} {
		if bytes.Contains(encoded, []byte(secret)) {
			t.Fatal("diagnostic leaked a secret")
		}
	}
	// Snapshot copies must not expose the observer's internal slices.
	snapshot.Events[0].TypeChain[0] = "changed"
	if o.snapshot().Events[0].TypeChain[0] == "changed" {
		t.Fatal("snapshot aliases internal state")
	}
}

func TestProviderObserverScopeSQLAndLargeBody(t *testing.T) {
	previous := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = previous })
	var raw []byte
	status := 400
	http.DefaultTransport = providerTestTransport(func(r *http.Request) (*http.Response, error) {
		if r.Body != nil {
			_, _ = io.Copy(io.Discard, r.Body)
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(bytes.NewReader(raw))}, nil
	})
	o, restore := observeProvider("libsql://approved.invalid", "token-secret")
	defer restore()
	call := func(target string) {
		r, _ := http.NewRequest(http.MethodPost, target, strings.NewReader(`{"requests":[{"type":"execute","stmt":{"sql":"SELECT never_record_me"}}]}`))
		response, err := http.DefaultTransport.RoundTrip(r)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		if err != nil || !bytes.Equal(body, raw) {
			t.Fatal("body truncation changed downstream bytes")
		}
		_ = response.Body.Close()
	}
	raw = []byte(`{"error":{"code":"SQLITE_ERROR","message":"failed SELECT never_record_me FROM messages"}}`)
	call("https://unapproved.invalid/v2/pipeline")
	call("https://approved.invalid:444/v2/pipeline")
	status = 200
	call("https://approved.invalid/v2/pipeline")
	if len(o.snapshot().Events) != 0 {
		t.Fatal("recorded an unapproved host/port or successful response")
	}
	status = 400
	call("https://approved.invalid/v2/pipeline")
	first := o.snapshot().Events[0]
	if first.RequestKind != "sql" || !first.SQLMessageOmitted || first.Message != "" {
		t.Fatal("SQL-like error message was recorded")
	}
	raw = bytes.Repeat([]byte("x"), providerBodyLimit*3)
	call("https://approved.invalid/v2/pipeline")
	last := o.snapshot().Events[1]
	if !last.BodyLimitReached || last.BodyObservedBytes != providerBodyLimit || last.Message != "" {
		t.Fatal("response observation was not bounded")
	}
}

func TestProviderObserverConcurrentBoundAndRestore(t *testing.T) {
	previous := http.DefaultTransport
	o, restore := observeProvider("libsql://approved.invalid", "")
	var workers sync.WaitGroup
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 100; j++ {
				o.setStage("bounded")
				o.add(providerEvent{HTTPStatus: 400})
				_ = o.snapshot()
			}
		}()
	}
	workers.Wait()
	x := o.snapshot()
	if len(x.Events) != providerObservationLimit || x.Dropped != 800-providerObservationLimit {
		t.Fatal("event cap or dropped accounting is incorrect")
	}
	restore()
	restore()
	if http.DefaultTransport != previous {
		t.Fatal("transport restore is not idempotent")
	}
}
