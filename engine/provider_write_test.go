package engine

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestProviderIssuedWriteKeepsOneDurableEffect(t *testing.T) {
	for _, operation := range []string{"commit", "exec", "exec-fresh", "renew-returning"} {
		for _, body := range []string{"", `{"error":"SQLITE_BUSY database is locked"}`, `{"error":{},"code":"SQLITE_BUSY","message":"database is locked"}`, `{"error":{},"code":"STREAM_EXPIRED","message":"database is locked"}`, `{"error":{},"message":"database is locked"}`, `{"error":{},"message":"gateway overload without SQL evidence"}`} {
			t.Run(fmt.Sprintf("%s/%d", operation, len(body)), func(t *testing.T) {
				f := newSQLiteHranaFixture(t)
				var armed atomic.Bool
				var injected, dispatches atomic.Int32
				proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					raw, er := io.ReadAll(r.Body)
					_ = r.Body.Close()
					if er != nil {
						t.Error(er)
						return
					}
					var p struct {
						Requests []sqliteHranaRequest `json:"requests"`
					}
					if er = json.Unmarshal(raw, &p); er != nil {
						t.Error(er)
						return
					}
					target := false
					for _, q := range p.Requests {
						text := strings.ToUpper(strings.TrimSpace(q.Stmt.SQL))
						switch operation {
						case "commit":
							target = target || text == "COMMIT"
						case "renew-returning":
							target = target || (strings.HasPrefix(text, "UPDATE MESSAGES") && strings.Contains(text, "RETURNING"))
						default:
							target = target || strings.HasPrefix(text, "UPDATE APP_COUNTER")
						}
					}
					if target {
						dispatches.Add(1)
					}
					r.Body = io.NopCloser(bytes.NewReader(raw))
					rr := httptest.NewRecorder()
					f.serveHTTP(rr, r)
					if target && armed.CompareAndSwap(true, false) {
						injected.Add(1)
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(503)
						_, _ = io.WriteString(w, body)
						return
					}
					for k, v := range rr.Header() {
						w.Header()[k] = v
					}
					w.WriteHeader(rr.Code)
					_, _ = w.Write(rr.Body.Bytes())
				}))
				defer proxy.Close()
				f.dsn = "libsql://" + strings.TrimPrefix(proxy.URL, "http://") + "?tls=0"
				e := f.openEngine(t, true)
				ctx := context.Background()
				if _, er := f.db.Exec("CREATE TABLE app_counter(value INTEGER NOT NULL)"); er != nil {
					t.Fatal(er)
				}
				if _, er := f.db.Exec("INSERT INTO app_counter VALUES(0)"); er != nil {
					t.Fatal(er)
				}
				var message *Message
				if operation == "renew-returning" {
					if er := e.CreateQueue(ctx, "renew-proof", QueueConfig{}); er != nil {
						t.Fatal(er)
					}
					if _, er := e.Send(ctx, "renew-proof", OutMessage{Body: []byte("one")}); er != nil {
						t.Fatal(er)
					}
					ms, er := e.Receive(ctx, "renew-proof", ReceiveOptions{MaxMessages: 1})
					if er != nil || len(ms) != 1 {
						t.Fatalf("receive=%v err=%v", ms, er)
					}
					message = ms[0]
				}
				beforeStats := f.snapshot()
				dispatches.Store(0)
				callbacks := 0
				if body != "" {
					armed.Store(true)
				}
				var er error
				switch operation {
				case "commit":
					er = e.Tx(ctx, func(tx *EngineTx) error {
						callbacks++
						_, x := tx.SQL().ExecContext(tx.Context(), "UPDATE app_counter SET value=value+1")
						return x
					})
				case "exec":
					callbacks++
					_, er = e.db.exec(ctx, "UPDATE app_counter SET value=value+1")
				case "exec-fresh":
					_, er = e.db.execFresh(ctx, "UPDATE app_counter SET value=value+1", func() []any { callbacks++; return nil })
				case "renew-returning":
					callbacks++
					_, er = e.RenewBatch(ctx, "renew-proof", []SettleItem{{SeqNumber: message.SeqNumber, LockToken: message.LockToken}})
				}
				var effects int
				if x := f.db.QueryRow("SELECT value FROM app_counter").Scan(&effects); x != nil {
					t.Fatal(x)
				}
				if operation == "commit" && f.snapshot().Commits-beforeStats.Commits != 1 {
					t.Errorf("COMMIT count=%d, want exactly one", f.snapshot().Commits-beforeStats.Commits)
				}
				wantEffects := 1
				if operation == "renew-returning" {
					wantEffects = 0
				}
				if effects != wantEffects || dispatches.Load() != 1 || callbacks != 1 {
					t.Errorf("durable boundary: operation=%s callbacks=%d dispatches=%d effects=%d", operation, callbacks, dispatches.Load(), effects)
				}
				if body == "" {
					if er != nil || injected.Load() != 0 {
						t.Fatalf("healthy control err=%v injected=%d", er, injected.Load())
					}
				} else if !errors.Is(er, ErrOutcomeUnknown) || errors.Is(er, driver.ErrBadConn) || injected.Load() != 1 {
					t.Fatalf("issued write acknowledgement err=%v injected=%d; want Unknown without safe retry", er, injected.Load())
				}
			})
		}
	}
}

func TestProviderHTTPStatusRequiresOneExactLeaf(t *testing.T) {
	leaf := errors.New("error code 502: upstream forward failed")
	for _, tc := range []struct {
		name   string
		err    error
		status int
	}{
		{"leaf", leaf, 502},
		{"driver newline", fmt.Errorf("failed to execute SQL: UPDATE example\n%w", leaf), 502},
		{"SQL text only", errors.New("failed to execute SQL: SELECT 'error code 503: private SQL'"), 0},
		{"outer text only", fmt.Errorf("error code 503: %w", errors.New("logical SQL error")), 0},
		{"non-three-digit", errors.New("error code 5020: unrelated"), 0},
		{"joined", errors.Join(leaf, errors.New("another failure")), 0},
		{"joined singleton", errors.Join(leaf), 0},
		{"network wrapper", &net.OpError{Op: "read", Net: "tcp", Err: leaf}, 0},
		{"URL wrapper", &url.Error{Op: "Post", Err: leaf}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := remoteProviderHTTPStatus(tc.err); got != tc.status {
				t.Fatalf("status=%d, want %d", got, tc.status)
			}
			if (&db{remote: true}).retryableWrite(tc.err) {
				t.Fatal("HTTP/ambiguous explanation became safe write replay")
			}
		})
	}
	for _, status := range []int{408, 429, 499, 500, 502, 503, 599} {
		err := fmt.Errorf("failed to execute SQL:\n%w", fmt.Errorf("error code %d: SQLITE_BUSY no such table EOF", status))
		if !(&db{remote: true}).retryable(err) || (&db{remote: true}).retryableWrite(err) || isBusyErr(err) || storageErrorCode(err) != "connection" {
			t.Fatalf("HTTP %d body overrode retry boundaries", status)
		}
	}
	for _, status := range []int{400, 401, 403, 404, 409, 422} {
		err := fmt.Errorf("failed to execute SQL:\n%w", fmt.Errorf("error code %d: SQLITE_BUSY EOF", status))
		if (&db{remote: true}).retryable(err) || (&db{remote: true}).retryableWrite(err) || isConnErr(err) || isBusyErr(err) {
			t.Fatalf("nontransient HTTP %d was retried", status)
		}
	}
	unknown := unknownRemoteWrite(leaf)
	if !errors.Is(unknown, ErrOutcomeUnknown) || !errors.Is(unknown, leaf) || (&db{remote: true}).retryable(unknown) || (&db{remote: true}).retryableWrite(unknown) {
		t.Fatal("unknown outcome lost its cause or became replayable")
	}
	var statusError interface{ HTTPStatus() int }
	if !errors.As(unknown, &statusError) || statusError.HTTPStatus() != 502 {
		t.Fatal("unknown outcome lost provider HTTP status")
	}
	detail := describeError(fmt.Errorf("failed to execute SQL:\n%w", leaf), 1)
	if detail.HTTPStatus != 502 || detail.Category != "provider_unavailable" || strings.Contains(detail.Message, "upstream") {
		t.Fatalf("provider failure diagnostics=%+v", detail)
	}
	detail = describeError(unknown, 1)
	if detail.HTTPStatus != 502 || detail.Category != "outcome_unknown" {
		t.Fatalf("unknown provider diagnostics=%+v", detail)
	}
}

func TestQueryFreshWriteScanAndWaitingBoundaries(t *testing.T) {
	for _, stage := range []string{"write-scan", "write-rows", "read-scan"} {
		t.Run(stage, func(t *testing.T) {
			d, state := remoteDBFailingFirst(t, 0)
			calls := 0
			scan := func(*sql.Rows) error {
				calls++
				if calls == 1 {
					return io.ErrUnexpectedEOF
				}
				return nil
			}
			var err error
			if stage == "read-scan" {
				err = d.queryFresh(context.Background(), "SELECT value", func() []any { return nil }, scan)
			} else {
				if stage == "write-rows" {
					state.queryResultRows = terminalErrorRows{err: io.ErrUnexpectedEOF}
					scan = func(rows *sql.Rows) error {
						calls++
						for rows.Next() {
						}
						return nil
					}
				}
				err = d.queryFreshWrite(context.Background(), "UPDATE t SET x=? RETURNING x", func() []any { return []any{1} }, scan)
			}
			if stage == "read-scan" {
				if err != nil || calls != 2 || len(state.args) != 2 {
					t.Fatalf("read calls=%d queries=%d err=%v", calls, len(state.args), err)
				}
			} else if !errors.Is(err, ErrOutcomeUnknown) || calls != 1 || len(state.args) != 1 {
				t.Fatalf("write calls=%d queries=%d err=%v", calls, len(state.args), err)
			}
		})
	}
	d, state := remoteDBFailingFirst(t, 0)
	d.sql.SetMaxOpenConns(1)
	held, err := d.sql.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	built := false
	err = d.queryFreshWrite(ctx, "UPDATE t SET x=? RETURNING x", func() []any { built = true; return nil }, func(*sql.Rows) error { t.Fatal("scan ran while waiting for a pool lease"); return nil })
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrOutcomeUnknown) || built || len(state.args) != 0 {
		t.Fatalf("pre-dispatch err=%v built=%t queries=%d", err, built, len(state.args))
	}
}

// Cancel only after the provider actually executed the selected write. A caller
// cancellation at the pool/admission boundary is covered independently.
func TestProviderAppliedWriteCancellationIsUnknown(t *testing.T) {
	for _, operation := range []string{"exec", "exec-fresh", "renew-returning"} {
		t.Run(operation, func(t *testing.T) {
			f := newSQLiteHranaFixture(t)
			var armed atomic.Bool
			var dispatches atomic.Int32
			applied := make(chan struct{})
			release := make(chan struct{})
			defer close(release)
			proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, er := io.ReadAll(r.Body)
				_ = r.Body.Close()
				if er != nil {
					return
				}
				var p struct {
					Requests []sqliteHranaRequest `json:"requests"`
				}
				if json.Unmarshal(raw, &p) != nil {
					return
				}
				target := false
				for _, q := range p.Requests {
					text := strings.ToUpper(strings.TrimSpace(q.Stmt.SQL))
					if operation == "renew-returning" {
						target = target || (strings.HasPrefix(text, "UPDATE MESSAGES") && strings.Contains(text, "RETURNING"))
					} else {
						target = target || strings.HasPrefix(text, "UPDATE APP_COUNTER")
					}
				}
				if target {
					dispatches.Add(1)
				}
				r.Body = io.NopCloser(bytes.NewReader(raw))
				rr := httptest.NewRecorder()
				f.serveHTTP(rr, r)
				if target && armed.CompareAndSwap(true, false) {
					close(applied)
					select {
					case <-r.Context().Done():
					case <-release:
					case <-time.After(3 * time.Second):
					}
					return
				}
				for k, v := range rr.Header() {
					w.Header()[k] = v
				}
				w.WriteHeader(rr.Code)
				_, _ = w.Write(rr.Body.Bytes())
			}))
			defer proxy.Close()
			f.dsn = "libsql://" + strings.TrimPrefix(proxy.URL, "http://") + "?tls=0"
			e := f.openEngine(t, true)
			ctx := context.Background()
			if _, er := f.db.Exec("CREATE TABLE app_counter(value INTEGER NOT NULL)"); er != nil {
				t.Fatal(er)
			}
			if _, er := f.db.Exec("INSERT INTO app_counter VALUES(0)"); er != nil {
				t.Fatal(er)
			}
			var message *Message
			if operation == "renew-returning" {
				if er := e.CreateQueue(ctx, "renew-cancel", QueueConfig{}); er != nil {
					t.Fatal(er)
				}
				if _, er := e.Send(ctx, "renew-cancel", OutMessage{Body: []byte("one")}); er != nil {
					t.Fatal(er)
				}
				ms, er := e.Receive(ctx, "renew-cancel", ReceiveOptions{MaxMessages: 1})
				if er != nil || len(ms) != 1 {
					t.Fatal(er)
				}
				message = ms[0]
			}
			dispatches.Store(0)
			armed.Store(true)
			caller, cancel := context.WithCancel(ctx)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				var er error
				switch operation {
				case "exec":
					_, er = e.db.exec(caller, "UPDATE app_counter SET value=value+1")
				case "exec-fresh":
					_, er = e.db.execFresh(caller, "UPDATE app_counter SET value=value+1", func() []any { return nil })
				case "renew-returning":
					_, er = e.RenewBatch(caller, "renew-cancel", []SettleItem{{SeqNumber: message.SeqNumber, LockToken: message.LockToken}})
				}
				done <- er
			}()
			select {
			case <-applied:
			case <-time.After(3 * time.Second):
				t.Fatal("provider did not apply")
			}
			cancel()
			select {
			case er := <-done:
				if !errors.Is(er, ErrOutcomeUnknown) || errors.Is(er, context.Canceled) || errors.Is(er, driver.ErrBadConn) {
					t.Fatalf("issued canceled write=%v", er)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("issued canceled write did not join")
			}
			var effects int
			if er := f.db.QueryRow("SELECT value FROM app_counter").Scan(&effects); er != nil {
				t.Fatal(er)
			}
			want := 1
			if operation == "renew-returning" {
				want = 0
			}
			if effects != want || dispatches.Load() != 1 {
				t.Fatalf("effects=%d requests=%d", effects, dispatches.Load())
			}
		})
	}
}
