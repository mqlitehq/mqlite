package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Counts calls into the actual transport, not proof that request bytes reached
// the provider. HTTP completion additionally requires real EOF and Close.
type providerCounts struct {
	Requests       int `json:"requests"`
	SchemaDDL      int `json:"schema_ddl"`
	VersionWrites  int `json:"version_writes"`
	HTTP200        int `json:"http_200"`
	EOFAndClose    int `json:"eof_and_close"`
	Active         int `json:"active"`
	InvalidCapture int `json:"invalid_capture"`
}
type providerObserver struct {
	host    string
	scheme  string
	next    http.RoundTripper
	mu      sync.Mutex
	allowed map[string]bool
	counts  providerCounts
}

func newProviderObserver(dsn string) (*providerObserver, error) {
	u, e := url.Parse(dsn)
	if e != nil || u.Host == "" {
		return nil, failure("observer_target_invalid")
	}
	scheme := "https"
	if u.Scheme == "http" || u.Query().Get("tls") == "0" {
		scheme = "http"
	}
	return &providerObserver{host: u.Host, scheme: scheme, next: http.DefaultTransport, allowed: map[string]bool{}}, nil
}
func (p *providerObserver) install() func() {
	old := http.DefaultTransport
	http.DefaultTransport = p
	return func() { http.DefaultTransport = old }
}
func (p *providerObserver) snapshot() providerCounts {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.counts
}
func delta(a, b providerCounts) providerCounts {
	return providerCounts{Requests: b.Requests - a.Requests, SchemaDDL: b.SchemaDDL - a.SchemaDDL, VersionWrites: b.VersionWrites - a.VersionWrites, HTTP200: b.HTTP200 - a.HTTP200, EOFAndClose: b.EOFAndClose - a.EOFAndClose, Active: b.Active, InvalidCapture: b.InvalidCapture - a.InvalidCapture}
}
func (p *providerObserver) allowBroker(host string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.allowed == nil {
		p.allowed = map[string]bool{}
	}
	p.allowed[strings.ToLower(host)] = true
}
func (p *providerObserver) RoundTrip(r *http.Request) (*http.Response, error) {
	if !strings.EqualFold(r.URL.Host, p.host) {
		p.mu.Lock()
		allowed := p.allowed[strings.ToLower(r.URL.Host)]
		if !allowed {
			p.counts.InvalidCapture++
		}
		p.mu.Unlock()
		if !allowed {
			return nil, failure("unapproved_provider_route")
		}
		return p.next.RoundTrip(r)
	}
	if p.scheme != "" && r.URL.Scheme != p.scheme {
		p.mu.Lock()
		p.counts.InvalidCapture++
		p.mu.Unlock()
		return nil, failure("unapproved_provider_scheme")
	}
	ddl, version, valid := inspectSQLKinds(r)
	p.mu.Lock()
	p.counts.Requests++
	p.counts.Active++
	p.counts.SchemaDDL += ddl
	p.counts.VersionWrites += version
	if !valid {
		p.counts.InvalidCapture++
	}
	p.mu.Unlock()
	resp, err := p.next.RoundTrip(r)
	if err != nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		p.mu.Lock()
		p.counts.Active--
		if resp != nil {
			p.counts.InvalidCapture++
		}
		p.mu.Unlock()
		return nil, err
	}
	if resp == nil || resp.Body == nil {
		p.mu.Lock()
		p.counts.Active--
		p.counts.InvalidCapture++
		p.mu.Unlock()
		return resp, err
	}
	p.mu.Lock()
	if resp.StatusCode == 200 {
		p.counts.HTTP200++
	}
	p.mu.Unlock()
	resp.Body = &observedBody{ReadCloser: resp.Body, done: func(eof, closed bool) {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.counts.Active--
		if eof && closed {
			p.counts.EOFAndClose++
		} else {
			p.counts.InvalidCapture++
		}
	}}
	return resp, nil
}
func inspectSQLKinds(r *http.Request) (int, int, bool) {
	if r.Method != "POST" || r.GetBody == nil {
		return 0, 0, false
	}
	b, err := r.GetBody()
	if err != nil {
		return 0, 0, false
	}
	defer b.Close()
	data, err := io.ReadAll(io.LimitReader(b, 128*1024+1))
	if err != nil || len(data) > 128*1024 {
		return 0, 0, false
	}
	var envelope struct {
		Requests []struct {
			Type string `json:"type"`
			Stmt struct {
				SQL string `json:"sql"`
			} `json:"stmt"`
			Batch struct {
				Steps []struct {
					Stmt struct {
						SQL string `json:"sql"`
					} `json:"stmt"`
				} `json:"steps"`
			} `json:"batch"`
		} `json:"requests"`
	}
	if json.Unmarshal(data, &envelope) != nil || len(envelope.Requests) == 0 {
		return 0, 0, false
	}
	ddl, version := 0, 0
	classify := func(sql string) {
		s := strings.ToUpper(strings.TrimSpace(sql))
		if strings.HasPrefix(s, "CREATE ") {
			ddl++
		}
		if strings.HasPrefix(s, "INSERT OR IGNORE INTO META") {
			version++
		}
	}
	for _, r := range envelope.Requests {
		switch r.Type {
		case "execute":
			if r.Stmt.SQL == "" {
				return 0, 0, false
			}
			classify(r.Stmt.SQL)
		case "batch":
			for _, s := range r.Batch.Steps {
				if s.Stmt.SQL == "" {
					return 0, 0, false
				}
				classify(s.Stmt.SQL)
			}
		case "close":
		default:
			return 0, 0, false
		}
	}
	return ddl, version, true
}

type observedBody struct {
	io.ReadCloser
	mu   sync.Mutex
	eof  bool
	once sync.Once
	done func(bool, bool)
}

func (b *observedBody) Read(p []byte) (int, error) {
	n, e := b.ReadCloser.Read(p)
	if e == io.EOF {
		b.mu.Lock()
		b.eof = true
		b.mu.Unlock()
	}
	return n, e
}
func (b *observedBody) Close() error {
	e := b.ReadCloser.Close()
	b.once.Do(func() { b.mu.Lock(); eof := b.eof; b.mu.Unlock(); b.done(eof, e == nil) })
	return e
}
func (p *providerObserver) join(ctxDone <-chan struct{}) bool {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	var quiet time.Time
	for {
		if p.snapshot().Active == 0 {
			if quiet.IsZero() {
				quiet = time.Now()
			}
			if time.Since(quiet) >= 200*time.Millisecond {
				return true
			}
		} else {
			quiet = time.Time{}
		}
		select {
		case <-ticker.C:
		case <-ctxDone:
			return false
		}
	}
}
