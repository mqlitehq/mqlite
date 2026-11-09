//go:build mqlite_perf_diag

package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// sqliteHranaFixture exercises the pinned HTTP driver against real SQLite SQL.
// It is a finite loopback protocol fixture, not a model of Turso scheduling,
// replication, durability or capacity. No credentials or external hosts are used.
type sqliteHranaFixture struct {
	db         *sql.DB
	server     *httptest.Server
	dsn        string
	delay      atomic.Int64
	failInsert atomic.Bool
	mu         sync.Mutex
	next       int
	sessions   map[string]*sqliteHranaSession
	stats      sqliteHranaStats
	insertHook func()
}

type sqliteHranaSession struct {
	mu   sync.Mutex
	conn *sql.Conn
	inTx bool
}

type sqliteHranaStats struct {
	Requests, Statements, Begins, Commits, Rollbacks, MessageInserts, Batches, Busy int
}

type sqliteHranaValue struct {
	Type   string  `json:"type"`
	Value  any     `json:"value,omitempty"`
	Base64 *string `json:"base64,omitempty"`
}

type sqliteHranaStatement struct {
	SQL       string             `json:"sql"`
	Args      []sqliteHranaValue `json:"args"`
	NamedArgs []struct {
		Name  string           `json:"name"`
		Value sqliteHranaValue `json:"value"`
	} `json:"named_args"`
	WantRows bool `json:"want_rows"`
}

type sqliteHranaCondition struct {
	Type  string                 `json:"type"`
	Step  int                    `json:"step"`
	Cond  *sqliteHranaCondition  `json:"cond"`
	Conds []sqliteHranaCondition `json:"conds"`
}

type sqliteHranaRequest struct {
	Type  string               `json:"type"`
	Stmt  sqliteHranaStatement `json:"stmt"`
	Batch struct {
		Steps []struct {
			Stmt      sqliteHranaStatement  `json:"stmt"`
			Condition *sqliteHranaCondition `json:"condition"`
		} `json:"steps"`
	} `json:"batch"`
}

func newSQLiteHranaFixture(t *testing.T) *sqliteHranaFixture {
	t.Helper()
	path := filepath.ToSlash(filepath.Join(t.TempDir(), "provider.db"))
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5)&_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(16)
	if _, err = db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	f := &sqliteHranaFixture{db: db, sessions: make(map[string]*sqliteHranaSession)}
	f.server = httptest.NewServer(http.HandlerFunc(f.serveHTTP))
	f.dsn = "libsql://" + strings.TrimPrefix(f.server.URL, "http://") + "?tls=0"
	t.Cleanup(func() {
		f.server.Close()
		f.mu.Lock()
		sessions := f.sessions
		f.sessions = make(map[string]*sqliteHranaSession)
		f.mu.Unlock()
		for _, session := range sessions {
			session.mu.Lock()
			if session.inTx {
				_, _ = session.conn.ExecContext(context.Background(), "ROLLBACK")
			}
			_ = session.conn.Close()
			session.mu.Unlock()
		}
		_ = db.Close()
	})
	return f
}

func (f *sqliteHranaFixture) setDelay(delay time.Duration) { f.delay.Store(int64(delay)) }

// failNextMessageInsert injects one definite pre-execution busy rejection.
func (f *sqliteHranaFixture) failNextMessageInsert() { f.failInsert.Store(true) }

func (f *sqliteHranaFixture) afterMessageInsert(hook func()) {
	f.mu.Lock()
	f.insertHook = hook
	f.mu.Unlock()
}

type sqliteHranaBusyFailure struct{}

func (sqliteHranaBusyFailure) Error() string { return "database is locked (5)" }
func (sqliteHranaBusyFailure) Code() int     { return 5 }

func (f *sqliteHranaFixture) snapshot() sqliteHranaStats {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stats
}

func (f *sqliteHranaFixture) session(baton string) (string, *sqliteHranaSession, error) {
	f.mu.Lock()
	if baton != "" {
		session := f.sessions[baton]
		f.mu.Unlock()
		if session == nil {
			return "", nil, errors.New("unknown loopback session")
		}
		return baton, session, nil
	}
	f.next++
	baton = "fixture-" + strconv.Itoa(f.next)
	f.mu.Unlock()
	conn, err := f.db.Conn(context.Background())
	if err != nil {
		return "", nil, err
	}
	session := &sqliteHranaSession{conn: conn}
	f.mu.Lock()
	f.sessions[baton] = session
	f.mu.Unlock()
	return baton, session, nil
}

func (f *sqliteHranaFixture) retire(baton string, session *sqliteHranaSession) {
	f.mu.Lock()
	delete(f.sessions, baton)
	f.mu.Unlock()
	if session.inTx {
		_, _ = session.conn.ExecContext(context.Background(), "ROLLBACK")
		session.inTx = false
	}
	_ = session.conn.Close()
}

func (f *sqliteHranaFixture) serveHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != http.MethodPost || r.URL.Path != "/v2/pipeline" {
		http.NotFound(w, r)
		return
	}
	var pipeline struct {
		Baton    string               `json:"baton"`
		Requests []sqliteHranaRequest `json:"requests"`
	}
	if json.NewDecoder(io.LimitReader(r.Body, 8<<20)).Decode(&pipeline) != nil || len(pipeline.Requests) == 0 {
		http.Error(w, "invalid loopback protocol request", http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	f.stats.Requests++
	f.mu.Unlock()
	if delay := time.Duration(f.delay.Load()); delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-r.Context().Done():
			return
		}
	}
	baton, session, err := f.session(pipeline.Baton)
	if err != nil {
		http.Error(w, "invalid loopback session", http.StatusBadRequest)
		return
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	results := make([]any, 0, len(pipeline.Requests))
	closed := false
	for _, request := range pipeline.Requests {
		switch request.Type {
		case "close":
			f.retire(baton, session)
			closed = true
			results = append(results, map[string]any{"type": "ok", "response": map[string]any{"type": "close"}})
		case "execute":
			result, err := f.execute(session, request.Stmt)
			if err != nil {
				results = append(results, map[string]any{"type": "error", "error": f.protocolError(err)})
			} else {
				results = append(results, map[string]any{"type": "ok", "response": map[string]any{"type": "execute", "result": result}})
			}
		case "batch":
			f.mu.Lock()
			f.stats.Batches++
			f.mu.Unlock()
			stepResults := make([]any, len(request.Batch.Steps))
			stepErrors := make([]any, len(request.Batch.Steps))
			for i, step := range request.Batch.Steps {
				if !sqliteHranaConditionOK(step.Condition, stepResults, stepErrors) {
					continue
				}
				result, err := f.execute(session, step.Stmt)
				if err != nil {
					stepErrors[i] = f.protocolError(err)
				} else {
					stepResults[i] = result
				}
			}
			results = append(results, map[string]any{"type": "ok", "response": map[string]any{"type": "batch", "result": map[string]any{"step_results": stepResults, "step_errors": stepErrors}}})
		default:
			results = append(results, map[string]any{"type": "error", "error": map[string]any{"message": "unsupported loopback request", "code": "SQLITE_ERROR"}})
		}
	}
	if !closed && !session.inTx {
		f.retire(baton, session)
		closed = true
	}
	var responseBaton any
	if !closed {
		responseBaton = baton
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"baton": responseBaton, "base_url": nil, "results": results})
}

func sqliteHranaConditionOK(condition *sqliteHranaCondition, results, failures []any) bool {
	if condition == nil {
		return true
	}
	switch condition.Type {
	case "ok":
		return condition.Step >= 0 && condition.Step < len(results) && results[condition.Step] != nil
	case "error":
		return condition.Step >= 0 && condition.Step < len(failures) && failures[condition.Step] != nil
	case "not":
		return !sqliteHranaConditionOK(condition.Cond, results, failures)
	case "and":
		for i := range condition.Conds {
			if !sqliteHranaConditionOK(&condition.Conds[i], results, failures) {
				return false
			}
		}
		return true
	case "or":
		for i := range condition.Conds {
			if sqliteHranaConditionOK(&condition.Conds[i], results, failures) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func (f *sqliteHranaFixture) protocolError(err error) map[string]any {
	code := "SQLITE_ERROR"
	var coded interface{ Code() int }
	if errors.As(err, &coded) {
		switch coded.Code() & 255 {
		case 5:
			code = "SQLITE_BUSY"
		case 19:
			code = "SQLITE_CONSTRAINT"
		}
	}
	if code == "SQLITE_BUSY" {
		f.mu.Lock()
		f.stats.Busy++
		f.mu.Unlock()
	}
	// SQLite messages here refer only to synthetic local test data.
	return map[string]any{"message": err.Error(), "code": code}
}

func (f *sqliteHranaFixture) execute(session *sqliteHranaSession, statement sqliteHranaStatement) (map[string]any, error) {
	args := make([]any, 0, len(statement.Args)+len(statement.NamedArgs))
	for _, value := range statement.Args {
		arg, err := sqliteHranaDecode(value)
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	for _, value := range statement.NamedArgs {
		arg, err := sqliteHranaDecode(value.Value)
		if err != nil {
			return nil, err
		}
		args = append(args, sql.Named(value.Name, arg))
	}
	normalized := strings.ToUpper(strings.Join(strings.Fields(statement.SQL), " "))
	f.mu.Lock()
	f.stats.Statements++
	if normalized == "BEGIN" {
		f.stats.Begins++
	}
	if normalized == "COMMIT" {
		f.stats.Commits++
	}
	if normalized == "ROLLBACK" {
		f.stats.Rollbacks++
	}
	if strings.HasPrefix(normalized, "INSERT INTO MESSAGES ") {
		f.stats.MessageInserts++
	}
	hook := f.insertHook
	f.mu.Unlock()
	isMessageInsert := strings.HasPrefix(normalized, "INSERT INTO MESSAGES ")
	if isMessageInsert && f.failInsert.CompareAndSwap(true, false) {
		return nil, sqliteHranaBusyFailure{}
	}
	var columns []map[string]any
	values := make([][]sqliteHranaValue, 0)
	var affected int64
	var lastID int64
	if statement.WantRows {
		rows, err := session.conn.QueryContext(context.Background(), statement.SQL, args...)
		if err != nil {
			return nil, err
		}
		names, err := rows.Columns()
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		for _, name := range names {
			columns = append(columns, map[string]any{"name": name, "decltype": nil})
		}
		for rows.Next() {
			row := make([]any, len(names))
			destinations := make([]any, len(names))
			for i := range row {
				destinations[i] = &row[i]
			}
			if err := rows.Scan(destinations...); err != nil {
				_ = rows.Close()
				return nil, err
			}
			encoded := make([]sqliteHranaValue, len(row))
			for i, value := range row {
				encoded[i], err = sqliteHranaEncode(value)
				if err != nil {
					_ = rows.Close()
					return nil, err
				}
			}
			values = append(values, encoded)
		}
		err = rows.Err()
		closeErr := rows.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
	} else {
		result, err := session.conn.ExecContext(context.Background(), statement.SQL, args...)
		if err != nil {
			return nil, err
		}
		affected, _ = result.RowsAffected()
		lastID, _ = result.LastInsertId()
	}
	if normalized == "BEGIN" {
		session.inTx = true
	}
	if normalized == "COMMIT" || normalized == "ROLLBACK" {
		session.inTx = false
	}
	if isMessageInsert && hook != nil {
		hook()
	}
	if columns == nil {
		columns = make([]map[string]any, 0)
	}
	return map[string]any{"cols": columns, "rows": values, "affected_row_count": affected, "last_insert_rowid": strconv.FormatInt(lastID, 10)}, nil
}

func sqliteHranaDecode(value sqliteHranaValue) (any, error) {
	switch value.Type {
	case "null":
		return nil, nil
	case "text":
		return value.Value, nil
	case "integer":
		s, ok := value.Value.(string)
		if !ok {
			return nil, errors.New("invalid integer fixture value")
		}
		return strconv.ParseInt(s, 10, 64)
	case "float":
		return value.Value, nil
	case "blob":
		if value.Base64 == nil {
			return nil, errors.New("missing fixture blob data")
		}
		return base64.RawStdEncoding.DecodeString(*value.Base64)
	default:
		return nil, fmt.Errorf("unsupported fixture value type %q", value.Type)
	}
}

func sqliteHranaEncode(value any) (sqliteHranaValue, error) {
	switch v := value.(type) {
	case nil:
		return sqliteHranaValue{Type: "null"}, nil
	case int64:
		return sqliteHranaValue{Type: "integer", Value: strconv.FormatInt(v, 10)}, nil
	case float64:
		return sqliteHranaValue{Type: "float", Value: v}, nil
	case string:
		return sqliteHranaValue{Type: "text", Value: v}, nil
	case []byte:
		encoded := base64.RawStdEncoding.EncodeToString(v)
		return sqliteHranaValue{Type: "blob", Base64: &encoded}, nil
	default:
		return sqliteHranaValue{}, fmt.Errorf("unsupported SQLite fixture value type %T", value)
	}
}
