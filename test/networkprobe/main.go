// Command networkprobe is a bounded diagnostic for the dedicated verification DB.
// It never emits SQL, arguments, URLs, headers, credentials or response bodies.
package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/mqlitehq/mqlite/engine"
)

const maxProbeEvents = 512
const maxClassifyBytes = 64 << 10

type safeError struct {
	Kind string `json:"kind"`
	Type string `json:"type,omitempty"`
}

func classifyError(err error) safeError {
	if err == nil || errors.Is(err, io.EOF) {
		return safeError{Kind: "none"}
	}
	kind := "other"
	switch {
	case errors.Is(err, engine.ErrOutcomeUnknown):
		kind = "outcome_unknown"
	case errors.Is(err, engine.ErrLockLost):
		kind = "lock_lost"
	case errors.Is(err, context.Canceled):
		kind = "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		kind = "deadline"
	case errors.Is(err, driver.ErrBadConn):
		kind = "bad_conn"
	case errors.Is(err, io.ErrUnexpectedEOF):
		kind = "unexpected_eof"
	case errors.Is(err, engine.ErrClosed):
		kind = "closed"
	default:
		var e net.Error
		if errors.As(err, &e) {
			kind = "network"
			if e.Timeout() {
				kind = "network_timeout"
			}
		}
	}
	return safeError{Kind: kind, Type: fmt.Sprintf("%T", err)}
}

type event struct {
	Operation   string    `json:"operation"`
	Categories  []string  `json:"categories"`
	Status      int       `json:"http_status"`
	TotalMs     float64   `json:"total_ms"`
	HeadersMs   float64   `json:"headers_ms"`
	ConnWaitMs  float64   `json:"http_connection_wait_ms"`
	DNSMs       float64   `json:"dns_ms"`
	ConnectMs   float64   `json:"connect_ms"`
	TLSMs       float64   `json:"tls_ms"`
	FirstByteMs float64   `json:"first_byte_ms"`
	Reused      bool      `json:"connection_reused"`
	BodyEOF     bool      `json:"body_eof"`
	InjectedEOF bool      `json:"injected_commit_eof"`
	Error       safeError `json:"error"`
}

type operationKey struct{}
type observer struct {
	base     http.RoundTripper
	allow    func(*url.URL) bool
	mu       sync.Mutex
	events   []event
	dropped  int
	armed    bool
	injected int
}

func (o *observer) add(e event) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.events) >= maxProbeEvents {
		o.dropped++
		return
	}
	o.events = append(o.events, e)
}

func (o *observer) snapshot() ([]event, int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]event(nil), o.events...), o.dropped
}

func requestCategories(r *http.Request) []string {
	if r.GetBody == nil {
		return []string{"unclassified"}
	}
	b, err := r.GetBody()
	if err != nil {
		return []string{"unclassified"}
	}
	data, err := io.ReadAll(io.LimitReader(b, maxClassifyBytes+1))
	_ = b.Close()
	if err != nil || len(data) > maxClassifyBytes {
		return []string{"unclassified"}
	}
	var envelope struct {
		Requests []struct {
			Type string `json:"type"`
			SQL  string `json:"sql"`
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
	if json.Unmarshal(data, &envelope) != nil {
		return []string{"unclassified"}
	}
	var out []string
	add := func(s string) {
		for _, x := range out {
			if x == s {
				return
			}
		}
		if len(out) < 16 {
			out = append(out, s)
		}
	}
	for _, req := range envelope.Requests {
		if req.SQL != "" {
			add(sqlCategory(req.SQL))
		}
		if req.Stmt.SQL != "" {
			add(sqlCategory(req.Stmt.SQL))
		}
		for _, step := range req.Batch.Steps {
			add(sqlCategory(step.Stmt.SQL))
		}
		if req.Type == "close" {
			add("stream_close")
		}
	}
	if len(out) == 0 {
		return []string{"unclassified"}
	}
	return out
}

func sqlCategory(raw string) string {
	s := strings.ToUpper(strings.TrimSpace(raw))
	switch {
	case strings.HasPrefix(s, "COMMIT"), strings.HasPrefix(s, "END"):
		return "commit"
	case strings.HasPrefix(s, "BEGIN"):
		return "begin"
	case strings.HasPrefix(s, "ROLLBACK"):
		return "rollback"
	case strings.HasPrefix(s, "CREATE TABLE"):
		return "ddl_table"
	case strings.HasPrefix(s, "CREATE INDEX"), strings.HasPrefix(s, "CREATE UNIQUE INDEX"):
		return "ddl_index"
	case strings.HasPrefix(s, "CREATE TRIGGER"):
		return "ddl_trigger"
	case strings.Contains(s, "SQLITE_MASTER"):
		return "access_schema_check"
	case strings.Contains(s, "SCHEMA_VERSION") && strings.HasPrefix(s, "SELECT"):
		return "schema_token_read"
	case strings.HasPrefix(s, "INSERT OR IGNORE INTO META"):
		return "schema_token_write"
	case strings.HasPrefix(s, "UPDATE MESSAGES SET") && strings.Contains(s, "WHERE STATE='LOCKED'"):
		return "lease_recovery"
	case strings.HasPrefix(s, "UPDATE QUEUES") && strings.Contains(s, "EXPIRES_AT"):
		return "subscription_expiry"
	case strings.HasPrefix(s, "SELECT"):
		return "read"
	case strings.HasPrefix(s, "INSERT"), strings.HasPrefix(s, "UPDATE"), strings.HasPrefix(s, "DELETE"):
		return "write"
	case strings.HasPrefix(s, "PRAGMA"):
		return "pragma"
	default:
		return "other"
	}
}

func hasCategory(categories []string, s string) bool {
	for _, x := range categories {
		if x == s {
			return true
		}
	}
	return false
}
func millis(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func (o *observer) RoundTrip(req *http.Request) (*http.Response, error) {
	if !o.allow(req.URL) {
		return nil, errors.New("probe refused unapproved request")
	}
	started := time.Now()
	e := event{Operation: "unattributed", Categories: requestCategories(req), Error: safeError{Kind: "none"}}
	if op, ok := req.Context().Value(operationKey{}).(string); ok {
		e.Operation = op
	}
	var mu sync.Mutex
	var connAt, dnsAt, connectAt, tlsAt time.Time
	trace := &httptrace.ClientTrace{
		GetConn: func(string) { mu.Lock(); connAt = time.Now(); mu.Unlock() },
		GotConn: func(info httptrace.GotConnInfo) {
			mu.Lock()
			if !connAt.IsZero() {
				e.ConnWaitMs = millis(time.Since(connAt))
			}
			e.Reused = info.Reused
			mu.Unlock()
		},
		DNSStart: func(httptrace.DNSStartInfo) { mu.Lock(); dnsAt = time.Now(); mu.Unlock() },
		DNSDone: func(httptrace.DNSDoneInfo) {
			mu.Lock()
			if !dnsAt.IsZero() {
				e.DNSMs += millis(time.Since(dnsAt))
			}
			mu.Unlock()
		},
		ConnectStart: func(string, string) { mu.Lock(); connectAt = time.Now(); mu.Unlock() },
		ConnectDone: func(string, string, error) {
			mu.Lock()
			if !connectAt.IsZero() {
				e.ConnectMs += millis(time.Since(connectAt))
			}
			mu.Unlock()
		},
		TLSHandshakeStart: func() { mu.Lock(); tlsAt = time.Now(); mu.Unlock() },
		TLSHandshakeDone: func(tls.ConnectionState, error) {
			mu.Lock()
			if !tlsAt.IsZero() {
				e.TLSMs += millis(time.Since(tlsAt))
			}
			mu.Unlock()
		},
		GotFirstResponseByte: func() { mu.Lock(); e.FirstByteMs = millis(time.Since(started)); mu.Unlock() },
	}
	clone := req.Clone(httptrace.WithClientTrace(req.Context(), trace))
	resp, err := o.base.RoundTrip(clone)
	mu.Lock()
	e.HeadersMs = millis(time.Since(started))
	if resp != nil {
		e.Status = resp.StatusCode
	}
	mu.Unlock()
	finish := func(readErr error, eof, injected bool) {
		mu.Lock()
		e.TotalMs = millis(time.Since(started))
		e.BodyEOF = eof
		e.InjectedEOF = injected
		e.Error = classifyError(readErr)
		copy := e
		mu.Unlock()
		o.add(copy)
	}
	if err != nil || resp == nil || resp.Body == nil {
		finish(err, false, false)
		return resp, err
	}
	resp.Body = &observedBody{ReadCloser: resp.Body, finish: finish, canInject: hasCategory(e.Categories, "commit") && resp.StatusCode == 200, owner: o}
	return resp, nil
}

type observedBody struct {
	io.ReadCloser
	finish    func(error, bool, bool)
	once      sync.Once
	closeOnce sync.Once
	closeErr  error
	canInject bool
	prefix    []byte
	tooLarge  bool
	owner     *observer
}

func successfulCommitEnvelope(data []byte) bool {
	var p struct {
		Results []struct {
			Type     string          `json:"type"`
			Error    json.RawMessage `json:"error"`
			Response struct {
				Type   string          `json:"type"`
				Result json.RawMessage `json:"result"`
			} `json:"response"`
		} `json:"results"`
	}
	if json.Unmarshal(data, &p) != nil || len(p.Results) == 0 {
		return false
	}
	execute := false
	for _, r := range p.Results {
		if r.Type != "ok" || len(r.Error) > 0 && string(r.Error) != "null" {
			return false
		}
		if r.Response.Type == "execute" && len(r.Response.Result) > 0 && string(r.Response.Result) != "null" {
			execute = true
		}
	}
	return execute
}

func (b *observedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if b.canInject && !b.tooLarge {
		if len(b.prefix)+n > maxClassifyBytes {
			b.prefix = nil
			b.tooLarge = true
		} else {
			b.prefix = append(b.prefix, p[:n]...)
		}
	}
	injected := false
	if err == io.EOF && b.canInject && !b.tooLarge && successfulCommitEnvelope(b.prefix) {
		b.owner.mu.Lock()
		if b.owner.armed {
			b.owner.armed = false
			b.owner.injected++
			injected = true
			err = io.ErrUnexpectedEOF
		}
		b.owner.mu.Unlock()
	}
	if err != nil {
		b.once.Do(func() { b.finish(err, err == io.EOF || injected, injected) })
	}
	return n, err
}
func (b *observedBody) Close() error {
	b.closeOnce.Do(func() { b.closeErr = b.ReadCloser.Close(); b.once.Do(func() { b.finish(b.closeErr, false, false) }) })
	return b.closeErr
}

type openResult struct {
	Index      int       `json:"index"`
	DurationMs float64   `json:"duration_ms"`
	CloseMs    float64   `json:"close_ms"`
	Error      safeError `json:"error"`
	HTTPCount  int       `json:"http_count"`
}
type settlementResult struct {
	StatsObserved       bool      `json:"stats_observed"`
	FallbackCleanup     safeError `json:"fallback_cleanup"`
	Accepted            int       `json:"accepted"`
	Received            int       `json:"received"`
	First               safeError `json:"first_complete"`
	FirstOutcomeUnknown bool      `json:"first_outcome_unknown"`
	Replay              safeError `json:"exact_replay"`
	Injected            int       `json:"injected_commit_responses"`
	FinalTotal          int64     `json:"final_total"`
	FinalDLQ            int64     `json:"final_dlq"`
	Cleanup             safeError `json:"cleanup"`
	CleanupDeleted      bool      `json:"cleanup_deleted"`
	StatsError          safeError `json:"stats_error"`
	Passed              bool      `json:"passed"`
}
type report struct {
	FinalClose   safeError        `json:"final_close"`
	FinalCloseMs float64          `json:"final_close_ms"`
	Schema       int              `json:"schema"`
	StartedUTC   string           `json:"started_utc"`
	Runtime      string           `json:"runtime"`
	Opens        []openResult     `json:"opens"`
	Settlement   settlementResult `json:"settlement"`
	Events       []event          `json:"http_events"`
	Dropped      int              `json:"dropped_events"`
	Passed       bool             `json:"passed"`
	Error        safeError        `json:"error"`
}

func approvedDSN(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "libsql" && u.Host == verificationHost && u.User == nil && u.Path == "" && u.RawQuery == "" && u.Fragment == ""
}

func opContext(name string, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithValue(context.Background(), operationKey{}, name), timeout)
}

func run(opens int, timeout time.Duration) (r report) {
	r = report{Schema: 1, StartedUTC: time.Now().UTC().Format(time.RFC3339Nano), Runtime: "direct-engine Hrana HTTP diagnostic; not SDK-to-broker", Error: safeError{Kind: "none"}}
	dsn, token := os.Getenv("MQLITE_TEST_DB"), os.Getenv("MQLITE_TEST_DB_AUTH_TOKEN")
	if !approvedDSN(dsn) || token == "" {
		r.Error = safeError{Kind: "configuration_refused"}
		return r
	}
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		r.Error = safeError{Kind: "transport_refused"}
		return r
	}
	o := &observer{base: base.Clone(), allow: func(u *url.URL) bool {
		return u != nil && u.Scheme == "https" && u.Host == verificationHost && u.User == nil
	}}
	previous := http.DefaultTransport
	http.DefaultTransport = o
	defer func() { http.DefaultTransport = previous; o.base.(*http.Transport).CloseIdleConnections() }()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	var e *engine.Engine
	defer func() {
		if e != nil {
			started := time.Now()
			err := e.Close()
			r.FinalClose = classifyError(err)
			r.FinalCloseMs = millis(time.Since(started))
			if err != nil {
				r.Passed = false
			}
		}
		r.Events, r.Dropped = o.snapshot()
		if r.Dropped != 0 {
			r.Passed = false
		}
	}()
	for i := 0; i < opens; i++ {
		ctx, cancel := opContext("open", timeout)
		before, _ := o.snapshot()
		started := time.Now()
		var err error
		e, err = engine.Open(ctx, engine.Options{DB: dsn, AuthToken: token, DisableBackground: true, Logger: logger})
		cancel()
		after, _ := o.snapshot()
		row := openResult{Index: i + 1, DurationMs: millis(time.Since(started)), Error: classifyError(err), HTTPCount: len(after) - len(before)}
		if err != nil {
			r.Opens = append(r.Opens, row)
			r.Error = classifyError(err)
			r.Events, r.Dropped = o.snapshot()
			return r
		}
		if i+1 < opens {
			started = time.Now()
			err = e.Close()
			e = nil
			row.CloseMs = millis(time.Since(started))
			if err != nil {
				row.Error = classifyError(err)
			}
		}
		r.Opens = append(r.Opens, row)
	}
	r.Settlement = settlementProbe(e, o, timeout)
	r.Events, r.Dropped = o.snapshot()
	r.Passed = r.Settlement.Passed && r.Dropped == 0
	for _, row := range r.Opens {
		if row.Error.Kind != "none" {
			r.Passed = false
		}
	}
	return r
}

func settlementProbe(e *engine.Engine, o *observer, timeout time.Duration) (r settlementResult) {
	r = settlementResult{First: safeError{Kind: "not_run"}, Replay: safeError{Kind: "not_run"}, Cleanup: safeError{Kind: "not_run"}}
	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return r
	}
	queue := "network-probe-" + hex.EncodeToString(suffix[:])
	queueDeleted := false
	r.FallbackCleanup = safeError{Kind: "not_needed"}
	defer func() {
		if !queueDeleted {
			ctx, cancel := opContext("fallback_cleanup", timeout)
			defer cancel()
			_, err := e.DeleteQueueWithOptions(ctx, queue, engine.DeleteOptions{Force: true})
			r.FallbackCleanup = classifyError(err)
		}
	}()
	ctx, cancel := opContext("create_queue", timeout)
	err := e.CreateQueue(ctx, queue, engine.QueueConfig{LockDurationMs: 120000, MaxDeliveryCount: 1})
	cancel()
	if err != nil {
		r.First = classifyError(err)
		return r
	}
	ctx, cancel = opContext("send", timeout)
	seq, err := e.SendOne(ctx, queue, engine.OutMessage{Body: []byte("network-probe-fixed-payload")})
	cancel()
	if err != nil {
		r.First = classifyError(err)
		return r
	}
	r.Accepted = 1
	ctx, cancel = opContext("receive", timeout)
	messages, err := e.Receive(ctx, queue, engine.ReceiveOptions{MaxMessages: 1, AttemptID: "probe-" + hex.EncodeToString(suffix[:])})
	cancel()
	if err != nil || len(messages) != 1 || messages[0].SeqNumber != seq {
		r.First = classifyError(err)
		return r
	}
	r.Received = 1
	o.mu.Lock()
	o.armed = true
	o.mu.Unlock()
	ctx, cancel = opContext("complete_drop_commit", timeout)
	err = e.Complete(ctx, queue, seq, messages[0].LockToken)
	cancel()
	r.First = classifyError(err)
	r.FirstOutcomeUnknown = errors.Is(err, engine.ErrOutcomeUnknown)
	o.mu.Lock()
	r.Injected = o.injected
	o.armed = false
	o.mu.Unlock()
	ctx, cancel = opContext("complete_exact_replay", timeout)
	err = e.Complete(ctx, queue, seq, messages[0].LockToken)
	cancel()
	r.Replay = classifyError(err)
	ctx, cancel = opContext("final_stats", timeout)
	stats, statsErr := e.Stats(ctx, queue)
	cancel()
	r.StatsObserved = statsErr == nil
	r.StatsError = classifyError(statsErr)
	r.FinalTotal = stats.Total
	r.FinalDLQ = stats.DeadLettered
	ctx, cancel = opContext("delete_queue", timeout)
	deleted, err := e.DeleteQueueWithOptions(ctx, queue, engine.DeleteOptions{})
	cancel()
	r.Cleanup = classifyError(err)
	r.CleanupDeleted = err == nil && deleted.Deleted && deleted.State == "deleted"
	queueDeleted = r.CleanupDeleted
	r.Passed = r.FirstOutcomeUnknown && r.Injected == 1 && r.Replay.Kind == "none" && statsErr == nil && r.FinalTotal == 0 && r.FinalDLQ == 0 && r.CleanupDeleted
	return r
}

func writeReport(path string, r report) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	if path == "" {
		return errors.New("output path required")
	}
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".network-probe-")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(append(data, '\n'))
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func main() {
	opens := flag.Int("opens", 2, "number of sequential opens (1..3)")
	timeout := flag.Duration("call-timeout", 90*time.Second, "bounded timeout for each operation (1s..2m)")
	flag.Parse()
	if *opens < 1 || *opens > 3 || *timeout < time.Second || *timeout > 2*time.Minute {
		fmt.Fprintln(os.Stderr, "probe refused invalid bounds")
		os.Exit(2)
	}
	r := run(*opens, *timeout)
	if err := writeReport(os.Getenv("MQLITE_NETWORK_PROBE_OUTPUT"), r); err != nil {
		fmt.Fprintln(os.Stderr, "probe report unavailable")
		os.Exit(2)
	}
	fmt.Printf("network_probe passed=%t opens=%d http_events=%d outcome_unknown=%t exact_replay=%s\n", r.Passed, len(r.Opens), len(r.Events), r.Settlement.FirstOutcomeUnknown, r.Settlement.Replay.Kind)
	if !r.Passed {
		os.Exit(1)
	}
}
