package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestRustTursoBusyClassificationIsExactAndTransportSafe(t *testing.T) {
	provider := errors.New("Tursodb error: Database is busy")
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"provider leaf", provider, true},
		{"pinned driver wrapper", fmt.Errorf("failed to execute SQL:\n%w", provider), true},
		{"bare phrase", errors.New("Database is busy"), false},
		{"logical prefix", errors.New("constraint: Tursodb error: Database is busy"), false},
		{"post-commit checkpoint", errors.New("Tursodb error: Checkpoint failed: Database is busy"), false},
		{"provider suffix", errors.New("Tursodb error: Database is busy after commit"), false},
		{"snapshot", errors.New("Tursodb error: Database snapshot is stale. You must rollback and retry the whole transaction."), false},
		{"unknown outcome", fmt.Errorf("%w: %w", ErrOutcomeUnknown, provider), false},
		{"joined EOF", errors.Join(io.EOF, provider), false},
		{"joined arbitrary", errors.Join(errors.New("untrusted transport result"), provider), false},
		{"network wrapper", &net.OpError{Op: "read", Net: "tcp", Err: provider}, false},
		{"URL wrapper", &url.Error{Op: "Post", Err: provider}, false},
		{"EOF outer text", fmt.Errorf("unexpected EOF while reading response: %w", provider), false},
		{"caller canceled", errors.Join(context.Canceled, provider), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isBusyErr(tc.err); got != tc.want {
				t.Fatalf("busy classification=%t, want %t", got, tc.want)
			}
			if (&db{remote: true}).retryableWrite(tc.err) {
				t.Fatal("untyped provider text authorized issued write replay")
			}
			if (&db{}).retryableWrite(tc.err) {
				t.Fatal("local store started retrying")
			}
		})
	}
	detail := describeError(fmt.Errorf("failed to execute SQL:\n%w", provider), 1)
	if detail.Category != "busy" || detail.SQLiteCode != 5 || detail.Message == "" {
		t.Fatalf("definite provider contention not diagnosed: %+v", detail)
	}
}

// This small provider model runs through the pinned HTTP driver and database/sql.
// Its protocol-level busy response has no committed effect. HTTP failure controls
// can instead commit before returning a proxy error, preserving that ambiguity.
type rustBusyProvider struct {
	failureMessage                                                string
	failureCode                                                   string
	rollbackOnFailure                                             bool
	httpStatus                                                    int
	httpBody                                                      string
	httpApply                                                     bool
	onFailure                                                     func()
	skipAtStage                                                   int
	expiredRollbacks                                              int
	reads                                                         int
	mu                                                            sync.Mutex
	failStage                                                     string
	remaining                                                     int
	next                                                          int
	pending                                                       map[string]int
	starts, writes, commits, applied, failures, closedUncommitted int
}

func (p *rustBusyProvider) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var q struct {
		Baton    string `json:"baton"`
		Requests []struct {
			Type string `json:"type"`
			Stmt struct {
				SQL string `json:"sql"`
			} `json:"stmt"`
		} `json:"requests"`
	}
	if json.NewDecoder(r.Body).Decode(&q) != nil || len(q.Requests) != 1 {
		http.Error(w, "invalid test request", 400)
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	req := q.Requests[0]
	baton := q.Baton
	var result any
	if req.Type == "close" {
		if _, ok := p.pending[baton]; ok {
			p.closedUncommitted++
			delete(p.pending, baton)
		}
		baton = ""
		result = map[string]any{"type": "ok", "response": map[string]any{"type": "close"}}
	} else {
		stage := ""
		switch req.Stmt.SQL {
		case "BEGIN":
			p.starts++
			stage = "begin"
			p.next++
			baton = "tx-" + strconv.Itoa(p.next)
			p.pending[baton] = 0
		case "UPDATE validation_counter SET value=value+1":
			p.writes++
			stage = "statement"
		case "SELECT value FROM validation_counter":
			p.reads++
			stage = "read"
		case "COMMIT":
			p.commits++
			stage = "commit"
		case "ROLLBACK":
			delete(p.pending, baton)
			baton = ""
		default:
			if strings.HasPrefix(strings.Join(strings.Fields(req.Stmt.SQL), " "), "UPDATE messages SET locked_until=MAX(") {
				p.writes++
				stage = "statement"
			} else {
				http.Error(w, "unexpected test SQL", 400)
				return
			}
		}
		shouldFail := stage == p.failStage && p.remaining > 0
		if shouldFail && p.skipAtStage > 0 {
			p.skipAtStage--
			shouldFail = false
		}
		if shouldFail {
			p.remaining--
			p.failures++
			if p.httpStatus != 0 {
				if p.httpApply {
					if stage == "commit" {
						p.applied += p.pending[baton]
						delete(p.pending, baton)
					} else if stage == "statement" {
						if baton == "" {
							p.applied++
						} else {
							p.pending[baton]++
						}
					}
				}
				w.WriteHeader(p.httpStatus)
				body := p.httpBody
				if body == "" {
					body = `{"error":"upstream forward failed"}`
				}
				_, _ = io.WriteString(w, body)
				if p.onFailure != nil {
					p.onFailure()
				}
				return
			}
			message, code := p.failureMessage, p.failureCode
			if message == "" {
				message = "Tursodb error: Database is busy"
			}
			if code == "" {
				code = "SQLITE_BUSY"
			}
			if p.rollbackOnFailure {
				delete(p.pending, baton)
				baton = ""
				p.expiredRollbacks++
			}
			result = map[string]any{"type": "error", "error": map[string]any{"code": code, "message": message}}
		} else {
			if stage == "statement" {
				if baton == "" {
					p.applied++
				} else {
					p.pending[baton]++
				}
			}
			if stage == "commit" {
				p.applied += p.pending[baton]
				delete(p.pending, baton)
				baton = ""
			}
			cols, rows := []any{}, []any{}
			if stage == "read" {
				cols = []any{map[string]any{"name": "value", "decltype": "INTEGER"}}
				rows = []any{[]any{map[string]any{"type": "integer", "value": strconv.Itoa(p.applied)}}}
			}
			result = map[string]any{"type": "ok", "response": map[string]any{"type": "execute", "result": map[string]any{"cols": cols, "rows": rows, "affected_row_count": 1, "last_insert_rowid": "1"}}}
		}
	}
	var batonValue any
	if baton != "" {
		batonValue = baton
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"baton": batonValue, "base_url": nil, "results": []any{result}})
}
func newRustBusyEngine(t *testing.T, p *rustBusyProvider) *Engine {
	t.Helper()
	p.pending = map[string]int{}
	server := httptest.NewServer(p)
	t.Cleanup(server.Close)
	pool, e := openHTTPHranaDB("libsql://" + strings.TrimPrefix(server.URL, "http://") + "?tls=0")
	if e != nil {
		t.Fatal(e)
	}
	pool.SetMaxOpenConns(1)
	pool.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = pool.Close() })
	return &Engine{db: &db{sql: pool, remote: true}}
}
func TestRustTursoStatementAndCommitBusyReplayOnlyOneCommittedEffect(t *testing.T) {
	for _, stage := range []string{"statement", "commit"} {
		t.Run(stage, func(t *testing.T) {
			p := &rustBusyProvider{failStage: stage, remaining: 1}
			e := newRustBusyEngine(t, p)
			runs := 0
			err := e.inTx(context.Background(), func(ctx context.Context, tx *txn) error {
				runs++
				_, err := tx.ExecContext(ctx, "UPDATE validation_counter SET value=value+1")
				if err == nil {
					tx.record("q", "enqueued", 1)
				}
				return err
			})
			if stage == "commit" {
				if !errors.Is(err, ErrOutcomeUnknown) || runs != 1 {
					t.Fatalf("untyped commit response runs=%d err=%v", runs, err)
				}
				p.mu.Lock()
				defer p.mu.Unlock()
				if p.applied != 0 || p.commits != 1 {
					t.Fatalf("effects=%d commits=%d", p.applied, p.commits)
				}
				return
			}
			if err != nil {
				t.Fatalf("definite provider busy was not replayed: %v", err)
			}
			p.mu.Lock()
			defer p.mu.Unlock()
			if runs != 2 || p.starts != 2 || p.writes != 2 || p.failures != 1 || p.applied != 1 || e.observation.events[eventKey{"q", "enqueued"}] != 1 {
				t.Fatalf("runs=%d starts=%d writes=%d failures=%d committed effects=%d", runs, p.starts, p.writes, p.failures, p.applied)
			}
		})
	}
}
func TestRustTursoBusyExhaustionIsBoundedAndObservable(t *testing.T) {
	p := &rustBusyProvider{failStage: "statement", remaining: maxConnAttempts + 1}
	e := newRustBusyEngine(t, p)
	runs := 0
	err := e.inTx(context.Background(), func(ctx context.Context, tx *txn) error {
		runs++
		_, err := tx.ExecContext(ctx, "UPDATE validation_counter SET value=value+1")
		return err
	})
	if err == nil || errors.Is(err, ErrOutcomeUnknown) || storageErrorCode(err) != "busy" {
		t.Fatalf("expected definite exhausted contention, got %v", err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if runs != maxConnAttempts || p.commits != 0 || p.applied != 0 {
		t.Fatalf("retry bound/effect mismatch runs=%d commits=%d effects=%d", runs, p.commits, p.applied)
	}
}
func TestUnknownCommitWithRustBusyTextIsNotReplayed(t *testing.T) {
	d, state := remoteDBFailingFirst(t, 0)
	state.commitErr = errors.Join(io.EOF, errors.New("Tursodb error: Database is busy"))
	e := &Engine{db: d}
	runs := 0
	err := e.inTx(context.Background(), func(context.Context, *txn) error { runs++; return nil })
	if !errors.Is(err, ErrOutcomeUnknown) || runs != 1 {
		t.Fatalf("ambiguous commit replayed or lost outcome semantics: runs=%d err=%v", runs, err)
	}
}
