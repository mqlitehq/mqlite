//go:build mqlite_perf_diag

package main

// This observer is diagnostic-only. It preserves the HTTP body and errors and
// does not retry requests, change deadlines, or infer a transaction outcome.

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	maxProviderTraceEvents  = 65536
	maxProviderInspectBytes = 1 << 20
)

type providerHTTPEvent struct {
	BrokerRequestID             uint64           `json:"broker_request_id"`
	ID                          uint64           `json:"id"`
	Phase                       string           `json:"phase"`
	FinishPhase                 string           `json:"finish_phase,omitempty"`
	EOFPhase                    string           `json:"eof_phase,omitempty"`
	ClosePhase                  string           `json:"close_phase,omitempty"`
	HTTPStatus                  int              `json:"http_status"`
	Finished                    bool             `json:"finished"`
	StartedMS                   float64          `json:"started_ms"`
	FinishedMS                  float64          `json:"finished_ms"`
	HTTPMS                      float64          `json:"http_ms"`
	HeadersMS                   float64          `json:"headers_ms"`
	ConnWaitMS                  float64          `json:"conn_wait_ms"`
	DNSMS                       float64          `json:"dns_ms"`
	ConnectMS                   float64          `json:"connect_ms"`
	TLSMS                       float64          `json:"tls_ms"`
	TTFBMS                      float64          `json:"ttfb_ms"`
	BodyMS                      float64          `json:"body_ms"`
	ConnectionObserved          bool             `json:"connection_observed"`
	ConnectionReused            bool             `json:"connection_reused"`
	DNSObserved                 bool             `json:"dns_observed"`
	ConnectObserved             bool             `json:"connect_observed"`
	TLSObserved                 bool             `json:"tls_observed"`
	TTFBObserved                bool             `json:"ttfb_observed"`
	ContextHasDeadline          bool             `json:"context_has_deadline"`
	SyntheticDelayMS            float64          `json:"synthetic_delay_ms"`
	SyntheticDelayElapsedMS     float64          `json:"synthetic_delay_elapsed_ms"`
	NotSent                     bool             `json:"not_sent"`
	StatementCount              int              `json:"statement_count"`
	CloseCount                  int              `json:"close_count"`
	SQLKinds                    []string         `json:"sql_kinds"`
	SQLTags                     []string         `json:"sql_tags"`
	SQLiteCodes                 []string         `json:"sqlite_codes"`
	RequestInspection           string           `json:"request_inspection"`
	ProtocolSuccess             bool             `json:"strict_protocol_success"`
	ResponseInspectionTruncated bool             `json:"response_inspection_truncated"`
	ErrorClass                  string           `json:"error_class"`
	CloseErrorClass             string           `json:"close_error_class"`
	BodyEOF                     bool             `json:"body_eof"`
	BodyClosed                  bool             `json:"body_closed"`
	BodyEOFMS                   float64          `json:"body_eof_ms"`
	BodyCloseMS                 float64          `json:"body_close_ms"`
	Renewal                     *physicalRenewal `json:"renewal,omitempty"`
	CloseCalls                  uint64           `json:"close_calls"`
	LateEOF                     bool             `json:"late_eof"`
	LateClose                   bool             `json:"late_close"`
	ServerTiming                string           `json:"server_timing"`
}

type providerTraceSnapshot struct {
	Events                []providerHTTPEvent `json:"events"`
	Dropped               uint64              `json:"dropped"`
	Observed              uint64              `json:"observed"`
	Sent                  uint64              `json:"sent"`
	NotSent               uint64              `json:"not_sent"`
	Finished              uint64              `json:"finished"`
	Active                int64               `json:"active"`
	OpenBodies            int64               `json:"open_bodies"`
	LateEOF               uint64              `json:"late_eof"`
	LateClose             uint64              `json:"late_close"`
	ConnectionWaitMeaning string              `json:"connection_wait_meaning"`
	CorrelationMeaning    string              `json:"correlation_meaning"`
	SentMeaning           string              `json:"sent_meaning"`
	BodyTimingMeaning     string              `json:"body_timing_meaning"`
	StatementCountMeaning string              `json:"statement_count_meaning"`
}

type providerEventState struct {
	renewalQueue string
	expected     []providerResponseShape
	e            providerHTTPEvent
	started      time.Time
	headers      time.Time
	finished     bool
	eofSeen      bool
	closeSeen    bool
}

type providerTrace struct {
	origin     time.Time
	next       http.RoundTripper
	host       string
	port       string
	mu         sync.Mutex
	phase      string
	delay      time.Duration
	events     []*providerEventState
	dropped    uint64
	observed   uint64
	sent       uint64
	notSent    uint64
	finished   uint64
	active     int64
	openBodies int64
	lateEOF    uint64
	lateClose  uint64
}

func newProviderTrace(targetDSN string, next http.RoundTripper) (*providerTrace, error) {
	u, err := url.Parse(targetDSN)
	if err != nil || u.Hostname() == "" || u.User != nil || u.Opaque != "" {
		return nil, errors.New("provider trace requires a valid target")
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "libsql" && scheme != "turso" && scheme != "http" && scheme != "https" {
		return nil, errors.New("provider trace requires an HTTP-compatible target")
	}
	port := u.Port()
	if port == "" {
		port = "443"
		if scheme == "http" {
			port = "80"
		}
	}
	if next == nil {
		next = http.DefaultTransport
	}
	return &providerTrace{origin: time.Now(), next: next, host: strings.ToLower(u.Hostname()), port: port, phase: "unassigned"}, nil
}

// Phase is a diagnostic case label, not a request/transaction correlation ID.
// Refuse URL-like and arbitrary prose values instead of copying them to output.
func (p *providerTrace) SetPhase(label string) {
	if !safeProviderPhase(label) {
		label = "invalid_phase"
	}
	p.mu.Lock()
	p.phase = label
	p.mu.Unlock()
}

func safeProviderPhase(s string) bool {
	if len(s) == 0 || len(s) > 96 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '/') {
			return false
		}
	}
	return true
}

// SetDelay is an explicit test control. Normal measurements use zero delay.
func (p *providerTrace) SetDelay(delay time.Duration) {
	if delay < 0 {
		delay = 0
	}
	p.mu.Lock()
	p.delay = delay
	p.mu.Unlock()
}

func (p *providerTrace) target(u *url.URL) bool {
	if u == nil || !strings.EqualFold(u.Hostname(), p.host) {
		return false
	}
	port := u.Port()
	if port == "" {
		if u.Scheme == "https" {
			port = "443"
		} else if u.Scheme == "http" {
			port = "80"
		}
	}
	return port == p.port && (u.Scheme == "http" || u.Scheme == "https")
}

func (p *providerTrace) Snapshot() providerTraceSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	r := providerTraceSnapshot{Dropped: p.dropped, Observed: p.observed, Sent: p.sent, NotSent: p.notSent, Finished: p.finished, Active: p.active, OpenBodies: p.openBodies, LateEOF: p.lateEOF, LateClose: p.lateClose,
		ConnectionWaitMeaning: "GetConn-to-GotConn HTTP acquisition, including connection setup; not database/sql pool wait. Timing spans can overlap.",
		CorrelationMeaning:    "A nonzero broker_request_id is inherited only from a verified loopback request context. Missing context remains zero with case/phase attribution; transaction identity and private queue order are not inferred.",
		SentMeaning:           "Sent counts calls to the underlying RoundTripper, not proof that bytes reached the provider.",
		BodyTimingMeaning:     "Body time includes client read scheduling and ends at EOF, read error or Close; HTTP time excludes synthetic delay.",
		StatementCountMeaning: "Declared protocol statement slots, not proof of server execution or commit."}
	r.Events = make([]providerHTTPEvent, len(p.events))
	for i, row := range p.events {
		r.Events[i] = row.e
		r.Events[i].SQLKinds = append([]string(nil), row.e.SQLKinds...)
		r.Events[i].SQLTags = append([]string(nil), row.e.SQLTags...)
		r.Events[i].SQLiteCodes = append([]string(nil), row.e.SQLiteCodes...)
		if row.e.Renewal != nil {
			copy := *row.e.Renewal
			copy.Pairs = append([]string(nil), copy.Pairs...)
			if copy.Rows != nil {
				copy.Rows = append([]renewalRow{}, copy.Rows...)
			}
			r.Events[i].Renewal = &copy
		}
	}
	return r
}

func providerMillis(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func (p *providerTrace) finishLocked(s *providerEventState, err error) {
	if s.finished {
		return
	}
	s.finished = true
	s.e.Finished = true
	s.e.FinishedMS = providerMillis(time.Since(p.origin))
	s.e.FinishPhase = p.phase
	s.e.ErrorClass = providerErrorClass(err)
	if !s.started.IsZero() {
		s.e.HTTPMS = providerMillis(time.Since(s.started))
	}
	if !s.headers.IsZero() {
		s.e.BodyMS = providerMillis(time.Since(s.headers))
	}
	p.active--
	p.finished++
}

func (p *providerTrace) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil || !p.target(req.URL) {
		return p.next.RoundTrip(req)
	}
	inspection := inspectProviderRequest(req)
	var renewal *physicalRenewal
	var renewalQueue string
	if inspection.state == "ok" && inspection.statements == 1 && len(inspection.kinds) == 1 && inspection.kinds[0] == "update" && len(inspection.tags) == 1 && inspection.tags[0] == "message_write" {
		renewal, renewalQueue = inspectPhysicalRenewal(req)
	}
	p.mu.Lock()
	p.observed++
	s := &providerEventState{expected: inspection.expected, e: providerHTTPEvent{BrokerRequestID: verifiedBrokerRequestID(req.Context()), ID: p.observed, Phase: p.phase, ErrorClass: "none", CloseErrorClass: "none", ServerTiming: "not_observed", RequestInspection: inspection.state, StatementCount: inspection.statements, CloseCount: inspection.closes, SQLKinds: inspection.kinds, SQLTags: inspection.tags}}
	s.e.Renewal, s.renewalQueue = renewal, renewalQueue
	_, s.e.ContextHasDeadline = req.Context().Deadline()
	delay := p.delay
	s.e.SyntheticDelayMS = providerMillis(delay)
	if len(p.events) < maxProviderTraceEvents {
		p.events = append(p.events, s)
	} else {
		p.dropped++
	}
	p.active++
	p.mu.Unlock()
	beforeDelay := time.Now()
	err := req.Context().Err()
	if err == nil && delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-req.Context().Done():
			err = req.Context().Err()
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
	if err == nil {
		err = req.Context().Err()
	}
	p.mu.Lock()
	if delay > 0 {
		s.e.SyntheticDelayElapsedMS = providerMillis(time.Since(beforeDelay))
	}
	if err != nil {
		s.e.NotSent = true
		p.notSent++
		p.finishLocked(s, err)
		p.mu.Unlock()
		return nil, err
	}
	s.started = time.Now()
	s.e.StartedMS = providerMillis(s.started.Sub(p.origin))
	p.sent++
	p.mu.Unlock()
	var connStart, dnsStart, tlsStart time.Time
	connectStarts := make(map[string]time.Time)
	trace := &httptrace.ClientTrace{
		GetConn: func(string) { p.mu.Lock(); connStart = time.Now(); p.mu.Unlock() },
		GotConn: func(info httptrace.GotConnInfo) {
			p.mu.Lock()
			if !connStart.IsZero() {
				s.e.ConnWaitMS += providerMillis(time.Since(connStart))
			}
			s.e.ConnectionObserved = true
			s.e.ConnectionReused = info.Reused
			p.mu.Unlock()
		},
		DNSStart: func(httptrace.DNSStartInfo) {
			p.mu.Lock()
			dnsStart = time.Now()
			s.e.DNSObserved = true
			p.mu.Unlock()
		},
		DNSDone: func(httptrace.DNSDoneInfo) {
			p.mu.Lock()
			if !dnsStart.IsZero() {
				s.e.DNSMS += providerMillis(time.Since(dnsStart))
				dnsStart = time.Time{}
			}
			p.mu.Unlock()
		},
		ConnectStart: func(network, address string) {
			p.mu.Lock()
			connectStarts[network+"\x00"+address] = time.Now()
			s.e.ConnectObserved = true
			p.mu.Unlock()
		},
		ConnectDone: func(network, address string, _ error) {
			p.mu.Lock()
			key := network + "\x00" + address
			if at, ok := connectStarts[key]; ok {
				s.e.ConnectMS += providerMillis(time.Since(at))
				delete(connectStarts, key)
			}
			p.mu.Unlock()
		},
		TLSHandshakeStart: func() { p.mu.Lock(); tlsStart = time.Now(); s.e.TLSObserved = true; p.mu.Unlock() },
		TLSHandshakeDone: func(tls.ConnectionState, error) {
			p.mu.Lock()
			if !tlsStart.IsZero() {
				s.e.TLSMS += providerMillis(time.Since(tlsStart))
				tlsStart = time.Time{}
			}
			p.mu.Unlock()
		},
		GotFirstResponseByte: func() {
			p.mu.Lock()
			if !s.e.TTFBObserved {
				s.e.TTFBMS = providerMillis(time.Since(s.started))
				s.e.TTFBObserved = true
			}
			p.mu.Unlock()
		},
	}
	clone := req.Clone(httptrace.WithClientTrace(req.Context(), trace))
	resp, err := p.next.RoundTrip(clone)
	p.mu.Lock()
	s.headers = time.Now()
	s.e.HeadersMS = providerMillis(time.Since(s.started))
	if resp != nil {
		s.e.HTTPStatus = resp.StatusCode
	}
	if err != nil || resp == nil || resp.Body == nil {
		p.finishLocked(s, err)
		p.mu.Unlock()
		return resp, err
	}
	p.openBodies++
	p.mu.Unlock()
	resp.Body = &providerObservedBody{body: resp.Body, owner: p, state: s}
	return resp, nil
}

type providerObservedBody struct {
	body      io.ReadCloser
	owner     *providerTrace
	state     *providerEventState
	prefix    []byte // Internal bounded inspection buffer; never exported.
	truncated bool
}

func (b *providerObservedBody) Read(dst []byte) (int, error) {
	n, err := b.body.Read(dst)
	p := b.owner
	p.mu.Lock()
	if !b.state.finished && !b.truncated && n > 0 {
		if len(b.prefix)+n > maxProviderInspectBytes {
			b.prefix = nil
			b.truncated = true
			b.state.e.ResponseInspectionTruncated = true
		} else {
			b.prefix = append(b.prefix, dst[:n]...)
		}
	}
	if err == io.EOF && !b.state.eofSeen {
		b.state.eofSeen = true
		b.state.e.BodyEOF = true
		b.state.e.BodyEOFMS = providerMillis(time.Since(p.origin))
		b.state.e.EOFPhase = p.phase
		if p.phase != b.state.e.Phase {
			b.state.e.LateEOF = true
			p.lateEOF++
		}
		if !b.truncated {
			if b.state.e.Renewal != nil {
				b.state.e.Renewal.Rejected = inspectRenewalRejection(b.prefix)
			}
			b.state.e.SQLiteCodes = inspectProviderCodes(b.prefix, b.state.e.HTTPStatus)
			b.state.e.ProtocolSuccess = b.state.e.RequestInspection == "ok" && inspectProviderSuccess(b.prefix, b.state.e.HTTPStatus, b.state.expected)
			if b.state.e.ProtocolSuccess && b.state.e.Renewal != nil {
				inspectPhysicalRenewalRows(b.state.renewalQueue, b.prefix, b.state.e.Renewal)
			}
		}
		b.state.renewalQueue = ""
	}
	if err != nil {
		terminalErr := err
		if err == io.EOF {
			terminalErr = nil
		}
		p.finishLocked(b.state, terminalErr)
		b.prefix = nil
	}
	p.mu.Unlock()
	return n, err
}

func (b *providerObservedBody) Close() error {
	err := b.body.Close() // Preserve delegation and the exact returned error.
	p := b.owner
	p.mu.Lock()
	b.state.e.CloseCalls++
	if !b.state.closeSeen {
		b.state.closeSeen = true
		b.state.e.BodyClosed = true
		b.state.e.BodyCloseMS = providerMillis(time.Since(p.origin))
		b.state.e.ClosePhase = p.phase
		p.openBodies--
		if p.phase != b.state.e.Phase {
			b.state.e.LateClose = true
			p.lateClose++
		}
	}
	if err != nil {
		b.state.e.CloseErrorClass = providerErrorClass(err)
	}
	p.finishLocked(b.state, err)
	b.prefix = nil
	p.mu.Unlock()
	return err
}

func providerErrorClass(err error) string {
	if err == nil {
		return "none"
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "unexpected_eof"
	case errors.Is(err, io.EOF):
		return "eof"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "connection_refused"
	}
	var dns *net.DNSError
	if errors.As(err, &dns) {
		if dns.IsNotFound {
			return "dns_not_found"
		}
		if dns.Timeout() {
			return "dns_timeout"
		}
		return "dns"
	}
	var network net.Error
	if errors.As(err, &network) {
		if network.Timeout() {
			return "network_timeout"
		}
		return "network"
	}
	return "other"
}

type providerResponseShape struct {
	kind         string
	allowSkipped []bool
}
type providerRequestInspection struct {
	expected           []providerResponseShape
	state              string
	statements, closes int
	kinds, tags        []string
}

func providerUnique(dst []string, value string) []string {
	for _, x := range dst {
		if x == value {
			return dst
		}
	}
	return append(dst, value)
}

func inspectProviderRequest(req *http.Request) providerRequestInspection {
	out := providerRequestInspection{state: "get_body_unavailable", kinds: []string{"unknown"}, tags: []string{"unknown"}}
	if req.GetBody == nil {
		return out
	}
	b, err := req.GetBody()
	if err != nil || b == nil {
		out.state = "copy_error"
		return out
	}
	data, err := io.ReadAll(io.LimitReader(b, maxProviderInspectBytes+1))
	closeErr := b.Close()
	if err != nil || closeErr != nil {
		out.state = "copy_error"
		return out
	}
	if len(data) > maxProviderInspectBytes {
		out.state = "too_large"
		return out
	}
	type statement struct {
		SQL string `json:"sql"`
	}
	var envelope struct {
		Requests []struct {
			Type  string    `json:"type"`
			Stmt  statement `json:"stmt"`
			Batch struct {
				Steps []struct {
					Stmt      statement       `json:"stmt"`
					Condition json.RawMessage `json:"condition"`
				} `json:"steps"`
			} `json:"batch"`
		} `json:"requests"`
	}
	if json.Unmarshal(data, &envelope) != nil || len(envelope.Requests) == 0 {
		out.state = "invalid_json"
		return out
	}
	out = providerRequestInspection{state: "ok"}
	add := func(sql string) {
		out.statements++
		kind, tag := providerSQLClass(sql)
		out.kinds = providerUnique(out.kinds, kind)
		out.tags = providerUnique(out.tags, tag)
	}
	for _, r := range envelope.Requests {
		shape := providerResponseShape{kind: r.Type}
		if r.Type == "batch" {
			for _, step := range r.Batch.Steps {
				kind, _ := providerSQLClass(step.Stmt.SQL)
				shape.allowSkipped = append(shape.allowSkipped, kind == "rollback" && len(step.Condition) > 0 && !bytes.Equal(bytes.TrimSpace(step.Condition), []byte("null")))
			}
		}
		out.expected = append(out.expected, shape)
		switch r.Type {
		case "execute":
			add(r.Stmt.SQL)
		case "batch":
			for _, step := range r.Batch.Steps {
				add(step.Stmt.SQL)
			}
		case "close":
			out.closes++
			out.kinds = providerUnique(out.kinds, "close")
			out.tags = providerUnique(out.tags, "stream_close")
		default:
			out.kinds = providerUnique(out.kinds, "unknown")
			out.tags = providerUnique(out.tags, "unknown")
		}
	}
	return out
}

func providerSQLClass(raw string) (string, string) {
	s := strings.TrimSpace(raw)
	for {
		if strings.HasPrefix(s, "--") {
			i := strings.IndexByte(s, '\n')
			if i < 0 {
				return "unknown", "unknown"
			}
			s = strings.TrimSpace(s[i+1:])
			continue
		}
		if strings.HasPrefix(s, "/*") {
			i := strings.Index(s[2:], "*/")
			if i < 0 {
				return "unknown", "unknown"
			}
			s = strings.TrimSpace(s[i+4:])
			continue
		}
		break
	}
	s = strings.ToUpper(s)
	words := strings.Fields(s)
	if len(words) == 0 {
		return "unknown", "unknown"
	}
	word := strings.TrimSuffix(words[0], ";")
	switch word {
	case "BEGIN":
		return "begin", "transaction_begin"
	case "COMMIT", "END":
		return "commit", "transaction_commit"
	case "ROLLBACK":
		return "rollback", "transaction_rollback"
	case "CREATE", "ALTER", "DROP":
		return "ddl", "schema_ddl"
	case "PRAGMA":
		return "pragma", "pragma"
	case "SELECT":
		if strings.Contains(s, "FROM DEDUP") {
			return "select", "dedup_lookup"
		}
		if strings.Contains(s, "FROM QUEUES") {
			return "select", "queue_route"
		}
		if strings.Contains(s, "SETTLEMENT_RECEIPTS") {
			return "select", "settlement_receipt_read"
		}
		if strings.Contains(s, "RECEIVE_ATTEMPTS") {
			return "select", "receive_attempt_read"
		}
		if strings.Contains(s, "SQLITE_MASTER") || strings.Contains(s, "SQLITE_SCHEMA") {
			return "select", "schema_read"
		}
		return "select", "other_read"
	case "INSERT", "REPLACE", "UPDATE", "DELETE":
		kind := strings.ToLower(word)
		if strings.Contains(s, "SETTLEMENT_RECEIPTS") {
			return kind, "settlement_receipt_write"
		}
		if strings.Contains(s, "RECEIVE_ATTEMPTS") {
			return kind, "receive_attempt_write"
		}
		if strings.Contains(s, "DEDUP") {
			return kind, "dedup_write"
		}
		if strings.Contains(s, "MESSAGES") {
			if word == "INSERT" {
				return kind, "message_insert"
			}
			return kind, "message_write"
		}
		return kind, "other_write"
	default:
		return "unknown", "unknown"
	}
}

func inspectProviderCodes(data []byte, status int) []string {
	var top map[string]json.RawMessage
	if json.Unmarshal(data, &top) != nil {
		return nil
	}
	var out []string
	addError := func(raw json.RawMessage) {
		var e struct {
			Code         json.RawMessage `json:"code"`
			ExtendedCode json.RawMessage `json:"extended_code"`
		}
		if json.Unmarshal(raw, &e) != nil {
			return
		}
		for _, v := range []json.RawMessage{e.Code, e.ExtendedCode} {
			if code := providerSQLiteCode(v); code != "" {
				out = providerUnique(out, code)
			}
		}
	}
	if status >= 400 {
		addError(data)
	}
	addError(top["error"])
	var results []struct {
		Error    json.RawMessage `json:"error"`
		Response struct {
			Result struct {
				StepErrors []json.RawMessage `json:"step_errors"`
			} `json:"result"`
		} `json:"response"`
	}
	if json.Unmarshal(top["results"], &results) == nil {
		for _, r := range results {
			addError(r.Error)
			for _, raw := range r.Response.Result.StepErrors {
				addError(raw)
			}
		}
	}
	return out
}

func providerSQLiteCode(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) != nil {
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		var n json.Number
		if decoder.Decode(&n) != nil {
			return ""
		}
		value, err := strconv.Atoi(n.String())
		if err != nil {
			return ""
		}
		if code, ok := map[int]string{1: "SQLITE_ERROR", 4: "SQLITE_ABORT", 5: "SQLITE_BUSY", 6: "SQLITE_LOCKED", 7: "SQLITE_NOMEM", 8: "SQLITE_READONLY", 9: "SQLITE_INTERRUPT", 10: "SQLITE_IOERR", 11: "SQLITE_CORRUPT", 13: "SQLITE_FULL", 19: "SQLITE_CONSTRAINT", 26: "SQLITE_NOTADB", 261: "SQLITE_BUSY_RECOVERY", 517: "SQLITE_BUSY_SNAPSHOT", 773: "SQLITE_BUSY_TIMEOUT"}[value]; ok {
			return code
		}
		return ""
	}
	switch s {
	case "SQLITE_ERROR", "SQLITE_ABORT", "SQLITE_BUSY", "SQLITE_LOCKED", "SQLITE_NOMEM", "SQLITE_READONLY", "SQLITE_INTERRUPT", "SQLITE_IOERR", "SQLITE_CORRUPT", "SQLITE_FULL", "SQLITE_CONSTRAINT", "SQLITE_NOTADB", "SQLITE_BUSY_RECOVERY", "SQLITE_BUSY_SNAPSHOT", "SQLITE_BUSY_TIMEOUT", "SQLITE_LOCKED_SHAREDCACHE", "SQLITE_CONSTRAINT_CHECK", "SQLITE_CONSTRAINT_FOREIGNKEY", "SQLITE_CONSTRAINT_NOTNULL", "SQLITE_CONSTRAINT_PRIMARYKEY", "SQLITE_CONSTRAINT_TRIGGER", "SQLITE_CONSTRAINT_UNIQUE":
		return s
	default:
		return ""
	}
}
