package engine

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mqlitehq/mqlite/internal/libsqlcompat"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPHranaEmptyAcknowledgementDoesNotPanic(t *testing.T) {
	for _, body := range []string{`{}`, `{"results":[]}`} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				_, _ = io.Copy(io.Discard, r.Body)
				_, _ = io.WriteString(w, body)
			}))
			defer server.Close()
			conn, err := libsqlcompat.OpenHTTP("libsql://" + strings.TrimPrefix(server.URL, "http://") + "?tls=0")
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			defer func() {
				if value := recover(); value != nil {
					t.Fatalf("empty HTTP 200 acknowledgement panicked: %v", value)
				}
			}()
			_, err = conn.(driver.QueryerContext).QueryContext(context.Background(), "SELECT 1", nil)
			if err == nil {
				t.Fatal("empty HTTP 200 acknowledgement was accepted")
			}
		})
	}
}

func TestHTTPHranaCommittedMalformedAcknowledgementIsUnknown(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"malformed-json", `{`},
		{"empty-results", `{"results":[]}`},
		{"missing-statement", `{"results":[{"type":"ok","response":{"type":"execute","result":{}}}]}`},
		{"wrong-response-type", `{"results":[{"type":"ok","response":{"type":"close"}}]}`},
		{"negative-count", `{"results":[{"type":"ok","response":{"type":"execute","result":{"cols":[],"rows":[],"affected_row_count":-1}}}]}`},
		{"null-row", `{"results":[{"type":"ok","response":{"type":"execute","result":{"cols":[],"rows":[null],"affected_row_count":0}}}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testHTTPHranaCommittedInvalidAcknowledgement(t, tc.body)
		})
	}
}

func testHTTPHranaCommittedInvalidAcknowledgement(t *testing.T, acknowledgement string) {
	t.Helper()
	f := newSQLiteHranaFixture(t)
	var armed atomic.Bool
	var changed atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		_ = r.Body.Close()
		if err != nil {
			http.Error(w, "fixture request read failed", http.StatusBadRequest)
			return
		}
		var pipeline struct {
			Requests []sqliteHranaRequest `json:"requests"`
		}
		if err := json.Unmarshal(body, &pipeline); err != nil {
			http.Error(w, "fixture request decode failed", http.StatusBadRequest)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		recorded := httptest.NewRecorder()
		f.serveHTTP(recorded, r)
		commit := len(pipeline.Requests) == 1 && pipeline.Requests[0].Type == "execute" &&
			strings.EqualFold(strings.TrimSpace(pipeline.Requests[0].Stmt.SQL), "COMMIT")
		if commit && armed.CompareAndSwap(true, false) {
			changed.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, acknowledgement)
			return
		}
		for key, values := range recorded.Header() {
			w.Header()[key] = values
		}
		w.WriteHeader(recorded.Code)
		_, _ = w.Write(recorded.Body.Bytes())
	}))
	defer proxy.Close()
	f.dsn = "libsql://" + strings.TrimPrefix(proxy.URL, "http://") + "?tls=0"
	e := f.openEngine(t, true)
	ctx := context.Background()
	if err := e.CreateQueue(ctx, "commit-ack", QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	before := f.snapshot()
	armed.Store(true)
	_, err := e.Send(ctx, "commit-ack", OutMessage{Body: []byte("once")})
	var rows int
	if readErr := f.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages WHERE queue=?`, "commit-ack").Scan(&rows); readErr != nil {
		t.Fatal(readErr)
	}
	after := f.snapshot()
	if changed.Load() != 1 || rows != 1 || after.MessageInserts-before.MessageInserts != 1 || after.Commits-before.Commits != 1 {
		t.Fatalf("committed acknowledgement control: changed=%d rows=%d inserts=%d commits=%d", changed.Load(), rows, after.MessageInserts-before.MessageInserts, after.Commits-before.Commits)
	}
	if !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("committed malformed acknowledgement = %v; want ErrOutcomeUnknown", err)
	}
}

func TestHTTPHranaMalformedReadAcknowledgementIsNotUnknown(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		_, _ = io.Copy(io.Discard, r.Body)
		calls.Add(1)
		_, _ = io.WriteString(w, `{`)
	}))
	defer server.Close()
	d, err := openDB(context.Background(), "libsql://"+strings.TrimPrefix(server.URL, "http://")+"?tls=0", "", "")
	if err != nil {
		t.Fatal(err)
	}
	defer d.close()
	var value int64
	err = d.queryRowScan(context.Background(), []any{&value}, "SELECT 1")
	if err == nil || errors.Is(err, ErrOutcomeUnknown) || !libsqlcompat.IsInvalidAcknowledgement(err) || calls.Load() != 1 {
		t.Fatalf("read acknowledgement: error=%v calls=%d", err, calls.Load())
	}
}

type hranaAcknowledgementTransport func(*http.Request) (*http.Response, error)

func (f hranaAcknowledgementTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type hranaAcknowledgementReadFailure struct {
	io.ReadCloser
	err error
}

func (b hranaAcknowledgementReadFailure) Read([]byte) (int, error) { return 0, b.err }

type hranaAcknowledgementCloseNotice struct {
	io.ReadCloser
	once *sync.Once
	done chan struct{}
}

func (b hranaAcknowledgementCloseNotice) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(func() { close(b.done) })
	return err
}

// Delegate the real HTTP request first. Only the response reader changes, after
// SQLite has performed the selected operation and returned its HTTP headers.
func failHTTPHranaAcknowledgementRead(t *testing.T, sql string, failure error) (*atomic.Int32, <-chan struct{}) {
	t.Helper()
	previous := http.DefaultClient
	client := *previous
	base := client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	var changed atomic.Int32
	var closed sync.Once
	cleanupClosed := make(chan struct{})
	client.Transport = hranaAcknowledgementTransport(func(request *http.Request) (*http.Response, error) {
		response, err := base.RoundTrip(request)
		if err != nil || request.GetBody == nil {
			return response, err
		}
		body, err := request.GetBody()
		if err != nil {
			_ = response.Body.Close()
			return nil, fmt.Errorf("test fixture could not inspect the request body: %w", err)
		}
		var pipeline struct {
			Requests []sqliteHranaRequest `json:"requests"`
		}
		decodeErr := json.NewDecoder(body).Decode(&pipeline)
		_ = body.Close()
		if decodeErr == nil && len(pipeline.Requests) == 1 && pipeline.Requests[0].Type == "close" {
			response.Body = hranaAcknowledgementCloseNotice{ReadCloser: response.Body, once: &closed, done: cleanupClosed}
		}
		matches := decodeErr == nil && len(pipeline.Requests) == 1 && pipeline.Requests[0].Type == "execute" &&
			strings.EqualFold(strings.TrimSpace(pipeline.Requests[0].Stmt.SQL), sql)
		if matches && changed.CompareAndSwap(0, 1) {
			response.Body = hranaAcknowledgementReadFailure{ReadCloser: response.Body, err: failure}
		}
		return response, nil
	})
	http.DefaultClient = &client
	t.Cleanup(func() { http.DefaultClient = previous })
	return &changed, cleanupClosed
}

func TestHTTPHranaCommittedBodyReadFailureIsUnknown(t *testing.T) {
	f := newSQLiteHranaFixture(t)
	e := f.openEngine(t, true)
	ctx := context.Background()
	if err := e.CreateQueue(ctx, "commit-read-ack", QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	before := f.snapshot()
	changed, cleanupClosed := failHTTPHranaAcknowledgementRead(t, "COMMIT", errors.New("fixture acknowledgement reader failed"))
	_, err := e.Send(ctx, "commit-read-ack", OutMessage{Body: []byte("once")})
	// The lost COMMIT baton is cleaned asynchronously. Before restoring the
	// process HTTP client, observe that worker's delegated response close.
	select {
	case <-cleanupClosed:
	case <-time.After(10 * time.Second):
		t.Fatal("detached cleanup did not finish its HTTP response")
	}
	var rows int
	if readErr := f.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages WHERE queue=?`, "commit-read-ack").Scan(&rows); readErr != nil {
		t.Fatal(readErr)
	}
	after := f.snapshot()
	if changed.Load() != 1 || rows != 1 || after.MessageInserts-before.MessageInserts != 1 || after.Commits-before.Commits != 1 {
		t.Fatalf("committed reader failure control: changed=%d rows=%d inserts=%d commits=%d", changed.Load(), rows, after.MessageInserts-before.MessageInserts, after.Commits-before.Commits)
	}
	if !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("committed body read failure = %v; want ErrOutcomeUnknown", err)
	}
}

func TestHTTPHranaReadBodyFailurePreservesRetryClassification(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		failure   error
		retry     bool
	}{
		{"network", "SELECT 42", io.ErrUnexpectedEOF, true},
		{"other", "SELECT 43", errors.New("fixture acknowledgement reader failed"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSQLiteHranaFixture(t)
			e := f.openEngine(t, true)
			changed, _ := failHTTPHranaAcknowledgementRead(t, tc.sql, tc.failure)
			before := f.snapshot()
			var value int64
			err := e.db.queryRowScan(context.Background(), []any{&value}, tc.sql)
			attempts := f.snapshot().Requests - before.Requests
			if tc.retry {
				if err != nil || value != 42 || attempts != 2 {
					t.Fatalf("read network retry: error=%v value=%d attempts=%d", err, value, attempts)
				}
			} else if err == nil || !libsqlcompat.IsIncompleteAcknowledgement(err) || attempts != 1 {
				t.Fatalf("read non-network failure: error=%v attempts=%d", err, attempts)
			}
			if changed.Load() != 1 || errors.Is(err, ErrOutcomeUnknown) {
				t.Fatalf("read response classification: error=%v changed=%d", err, changed.Load())
			}
		})
	}
}

func TestHTTPHranaPreResponseFailureKeepsIdentity(t *testing.T) {
	failure := errors.New("fixture transport did not receive response headers")
	previous := http.DefaultClient
	client := *previous
	var calls atomic.Int32
	client.Transport = hranaAcknowledgementTransport(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, failure
	})
	http.DefaultClient = &client
	t.Cleanup(func() { http.DefaultClient = previous })
	conn, err := libsqlcompat.OpenHTTP("libsql://fixture.invalid")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, err = conn.(driver.QueryerContext).QueryContext(context.Background(), "SELECT 1", nil)
	if !errors.Is(err, failure) || libsqlcompat.IsInvalidAcknowledgement(err) || calls.Load() != 1 {
		t.Fatalf("pre-response failure: error=%v calls=%d", err, calls.Load())
	}
}

func TestHTTPHranaIssuedRenewalMalformedAcknowledgementIsUnknown(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(map[bool]string{false: "renew", true: "renew-batch"}[batch], func(t *testing.T) {
			f := newSQLiteHranaFixture(t)
			var armed atomic.Bool
			var changed atomic.Int32
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				_ = r.Body.Close()
				if err != nil {
					http.Error(w, "fixture request read failed", http.StatusBadRequest)
					return
				}
				var pipeline struct {
					Requests []sqliteHranaRequest `json:"requests"`
				}
				if err := json.Unmarshal(body, &pipeline); err != nil {
					http.Error(w, "fixture request decode failed", http.StatusBadRequest)
					return
				}
				r.Body = io.NopCloser(bytes.NewReader(body))
				recorded := httptest.NewRecorder()
				f.serveHTTP(recorded, r)
				renewal := len(pipeline.Requests) == 1 && pipeline.Requests[0].Type == "execute" &&
					strings.HasPrefix(strings.ToUpper(strings.Join(strings.Fields(pipeline.Requests[0].Stmt.SQL), " ")), "UPDATE MESSAGES SET LOCKED_UNTIL=MAX")
				if renewal && armed.CompareAndSwap(true, false) {
					changed.Add(1)
					_, _ = io.WriteString(w, `{`)
					return
				}
				w.WriteHeader(recorded.Code)
				_, _ = w.Write(recorded.Body.Bytes())
			}))
			defer proxy.Close()
			f.dsn = "libsql://" + strings.TrimPrefix(proxy.URL, "http://") + "?tls=0"
			e := f.openEngine(t, true)
			ctx := context.Background()
			if err := e.CreateQueue(ctx, "renew-ack", QueueConfig{}); err != nil {
				t.Fatal(err)
			}
			if _, err := e.SendOne(ctx, "renew-ack", OutMessage{Body: []byte("once")}); err != nil {
				t.Fatal(err)
			}
			messages, err := e.Receive(ctx, "renew-ack", ReceiveOptions{})
			if err != nil || len(messages) != 1 {
				t.Fatalf("receive: %v", err)
			}
			message := messages[0]
			now := time.Now().Add(time.Second).UnixMilli()
			e.now = func() int64 { return now }
			armed.Store(true)
			if batch {
				_, err = e.RenewBatch(ctx, "renew-ack", []SettleItem{{SeqNumber: message.SeqNumber, LockToken: message.LockToken}})
			} else {
				err = e.Renew(ctx, "renew-ack", message.SeqNumber, message.LockToken)
			}
			var deadline int64
			if readErr := f.db.QueryRowContext(ctx, `SELECT locked_until FROM messages WHERE id=?`, message.SeqNumber).Scan(&deadline); readErr != nil {
				t.Fatal(readErr)
			}
			if !errors.Is(err, ErrOutcomeUnknown) || changed.Load() != 1 || deadline <= message.LockedUntilMs {
				t.Fatalf("renewal acknowledgement: error=%v changed=%d deadline=%d original=%d", err, changed.Load(), deadline, message.LockedUntilMs)
			}
		})
	}
}
