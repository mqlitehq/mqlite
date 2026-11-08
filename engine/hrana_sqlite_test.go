package engine

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
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

func (f *sqliteHranaFixture) openEngine(t *testing.T, disableBackground bool) *Engine {
	t.Helper()
	e, err := Open(context.Background(), Options{DB: f.dsn, DisableBackground: disableBackground,
		PreserveUnexpiredLeases: true, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	})
	return e
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

func TestSQLiteHranaFixturePinnedDriverBatchResultSetsAndRollback(t *testing.T) {
	for _, failAt := range []int{-1, 0, 1} {
		t.Run(strconv.Itoa(failAt), func(t *testing.T) {
			f := newSQLiteHranaFixture(t)
			if _, err := f.db.Exec("CREATE TABLE probe(id INTEGER PRIMARY KEY, value TEXT UNIQUE)"); err != nil {
				t.Fatal(err)
			}
			pool, err := openHTTPHranaDB(f.dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			tx, err := pool.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			query := "SELECT 1; INSERT INTO probe(value) VALUES (?) RETURNING id; INSERT INTO probe(value) VALUES (?) RETURNING id; INSERT INTO probe(value) VALUES (?) RETURNING id"
			args := []any{"first", "second", "third"}
			if failAt == 0 {
				if _, err := f.db.Exec("INSERT INTO probe(value) VALUES ('first')"); err != nil {
					t.Fatal(err)
				}
			}
			if failAt == 1 {
				args[1] = "first"
			}
			rows, err := tx.QueryContext(context.Background(), query, args...)
			if failAt >= 0 {
				// The pinned driver checks every StepError before exposing rows.
				if err == nil {
					if rows != nil {
						_ = rows.Close()
					}
					t.Fatal("batch SQL error was hidden")
				}
				var count int
				if err := f.db.QueryRow("SELECT count(*) FROM probe").Scan(&count); err != nil {
					t.Fatal(err)
				}
				want := 0
				if failAt == 0 {
					want = 1
				}
				stats := f.snapshot()
				if count != want || stats.Rollbacks != 1 || stats.Statements != 4+failAt {
					t.Fatalf("failed batch count=%d want=%d stats=%+v", count, want, stats)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var sentinel int
			if !rows.Next() || rows.Scan(&sentinel) != nil || sentinel != 1 {
				t.Fatal("missing leading sentinel")
			}
			if rows.Next() {
				t.Fatal("extra sentinel row")
			}
			var got []int64
			for rows.NextResultSet() {
				var id int64
				if !rows.Next() {
					t.Fatal("missing insertion result")
				}
				if err := rows.Scan(&id); err != nil {
					t.Fatal(err)
				}
				got = append(got, id)
				if rows.Next() {
					t.Fatal("extra insertion row")
				}
			}
			rowErr := rows.Err()
			_ = rows.Close()
			if rowErr != nil || len(got) != 3 || got[0] >= got[1] || got[1] >= got[2] {
				t.Fatalf("results=%v err=%v", got, rowErr)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			var count int
			if err := f.db.QueryRow("SELECT count(*) FROM probe").Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 3 {
				t.Fatalf("committed rows=%d want=3", count)
			}
		})
	}
}

func TestSQLiteHranaFixtureEngineRoundTrip(t *testing.T) {
	f := newSQLiteHranaFixture(t)
	e := f.openEngine(t, true)
	ctx := context.Background()
	if err := e.CreateQueue(ctx, "q", QueueConfig{LockDurationMs: 2000, DedupWindowMs: 60000}); err != nil {
		t.Fatal(err)
	}
	seq, err := e.SendOne(ctx, "q", OutMessage{MessageID: "synthetic", Body: []byte("body"), Properties: map[string]string{"key": "value"}})
	if err != nil {
		t.Fatal(err)
	}
	messages, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 1})
	if err != nil || len(messages) != 1 {
		t.Fatalf("receive=%d err=%v", len(messages), err)
	}
	if messages[0].SeqNumber != seq || string(messages[0].Body) != "body" {
		t.Fatal("message changed")
	}
	if err := e.Complete(ctx, "q", seq, messages[0].LockToken); err != nil {
		t.Fatal(err)
	}
	metrics, err := e.Stats(ctx, "q")
	if err != nil || metrics.Total != 0 {
		t.Fatalf("metrics=%+v err=%v", metrics, err)
	}
}

func TestSQLiteHranaSendBatchBoundsRemoteRequests(t *testing.T) {
	f := newSQLiteHranaFixture(t)
	e := f.openEngine(t, true)
	ctx := context.Background()
	if err := e.CreateQueue(ctx, "batch", QueueConfig{LockDurationMs: 30000, DedupWindowMs: 3600000}); err != nil {
		t.Fatal(err)
	}
	inputs := make([]OutMessage, 64)
	for i := range inputs {
		inputs[i] = OutMessage{MessageID: fmt.Sprintf("synthetic-%02d", i), Body: []byte(fmt.Sprintf("body-%02d", i))}
	}
	before := f.snapshot()
	seqs, err := e.Send(ctx, "batch", inputs...)
	if err != nil {
		t.Fatal(err)
	}
	after := f.snapshot()
	if len(seqs) != len(inputs) {
		t.Fatalf("sequences=%d want=%d", len(seqs), len(inputs))
	}
	for i, seq := range seqs {
		if seq <= 0 || i > 0 && seq <= seqs[i-1] {
			t.Fatalf("sequence mapping is not ordered: %v", seqs)
		}
	}
	if after.MessageInserts-before.MessageInserts != 64 || after.Commits-before.Commits != 1 {
		t.Fatalf("batch effects not one atomic commit: before=%+v after=%+v", before, after)
	}
	// A tiny deduplicated batch fits one lookup, protocol INSERT batch and upsert.
	// This counts storage HTTP, not SQL steps or elapsed-time estimates.
	if requests := after.Requests - before.Requests; requests > 6 {
		t.Errorf("Send storage HTTP requests=%d want<=6", requests)
	}
	messages, err := e.Receive(ctx, "batch", ReceiveOptions{MaxMessages: 64})
	if err != nil || len(messages) != 64 {
		t.Fatalf("received=%d err=%v", len(messages), err)
	}
	for i, message := range messages {
		if message.SeqNumber != seqs[i] || message.MessageID != inputs[i].MessageID || string(message.Body) != string(inputs[i].Body) {
			t.Fatalf("batch input/result mapping changed at slot %d", i)
		}
	}
	items := make([]SettleItem, len(messages))
	for i, message := range messages {
		items[i] = SettleItem{SeqNumber: message.SeqNumber, LockToken: message.LockToken}
	}
	results, err := e.CompleteBatch(ctx, "batch", items)
	if err != nil || len(results) != 64 {
		t.Fatalf("settlements=%d err=%v", len(results), err)
	}
	for _, result := range results {
		if !result.Ok {
			t.Fatalf("settlement was not acknowledged: %+v", result)
		}
	}
	metrics, err := e.Stats(ctx, "batch")
	if err != nil || metrics.Total != 0 {
		t.Fatalf("final metrics=%+v err=%v", metrics, err)
	}
}

func TestSQLiteHranaMixedShortLeaseRenewalAndObservation(t *testing.T) {
	f := newSQLiteHranaFixture(t)
	e := f.openEngine(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, q := range []string{"consume", "produce"} {
		if err := e.CreateQueue(ctx, q, QueueConfig{LockDurationMs: 3000, MaxDeliveryCount: 1, DedupWindowMs: 3600000}); err != nil {
			t.Fatal(err)
		}
	}
	seq, err := e.SendOne(ctx, "consume", OutMessage{MessageID: "held", Body: []byte("held-body")})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := e.Receive(ctx, "consume", ReceiveOptions{MaxMessages: 1})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim=%d err=%v", len(claimed), err)
	}
	message := claimed[0]
	f.setDelay(12 * time.Millisecond)
	inserted := make(chan struct{})
	release := make(chan struct{})
	var holdOnce, releaseOnce sync.Once
	releaseHeld := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseHeld()
	f.afterMessageInsert(func() { holdOnce.Do(func() { close(inserted); <-release }) })
	type producerResult struct {
		worker    int
		sequences []int64
		err       error
	}
	produced := make(chan producerResult, 8)
	var producers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		producers.Add(1)
		go func(worker int) {
			defer producers.Done()
			inputs := make([]OutMessage, 64)
			for i := range inputs {
				id := fmt.Sprintf("mixed-%d-%d", worker, i)
				inputs[i] = OutMessage{MessageID: id, Body: []byte(id)}
			}
			sequences, err := e.Send(ctx, "produce", inputs...)
			produced <- producerResult{worker, sequences, err}
		}(worker)
	}
	select {
	case <-inserted:
	case <-ctx.Done():
		t.Fatal("producer never reached an insertion")
	}
	// The producer holds a real SQLite write transaction and its HTTP reply.
	// A whole-pool=1 workaround would queue this read until releaseHeld.
	readCtx, readCancel := context.WithTimeout(ctx, time.Second)
	metrics, readErr := e.Stats(readCtx, "consume")
	readCancel()
	releaseHeld()
	if readErr != nil || metrics.Locked != 1 {
		t.Fatalf("observation blocked by producer: metrics=%+v err=%v", metrics, readErr)
	}
	renewCtx, stopRenew := context.WithCancel(ctx)
	defer stopRenew()
	renewDone := make(chan error, 1)
	var renewals atomic.Int64
	go func() {
		ticker := time.NewTicker(300 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				renewDone <- nil
				return
			case <-ticker.C:
				call, stop := context.WithTimeout(renewCtx, 2*time.Second)
				results, err := e.RenewBatch(call, "consume", []SettleItem{{SeqNumber: seq, LockToken: message.LockToken}})
				stop()
				if renewCtx.Err() != nil {
					renewDone <- nil
					return
				}
				if err != nil {
					renewDone <- err
					return
				}
				if len(results) != 1 || !results[0].Ok || results[0].LockedUntilMs <= message.LockedUntilMs {
					renewDone <- errors.New("healthy lease did not renew")
					return
				}
				renewals.Add(1)
			}
		}
	}()
	handlerTimer := time.NewTimer(4 * time.Second)
	defer handlerTimer.Stop()
	select {
	case err := <-renewDone:
		t.Fatalf("renewal ended before handler finished: %v", err)
	case <-ctx.Done():
		t.Fatal("mixed handler budget expired")
	case <-handlerTimer.C:
	}
	stopRenew()
	if err := <-renewDone; err != nil {
		t.Fatal(err)
	}
	if renewals.Load() == 0 {
		t.Fatal("test never renewed the initial three-second lease")
	}
	if err := e.Complete(ctx, "consume", seq, message.LockToken); err != nil {
		t.Fatalf("healthy handler could not settle: %v", err)
	}
	joined := make(chan struct{})
	go func() { producers.Wait(); close(produced); close(joined) }()
	select {
	case <-joined:
	case <-ctx.Done():
		t.Fatal("producers did not join")
	}
	acked := make(map[int64]string, 8*64)
	for result := range produced {
		if result.err != nil || len(result.sequences) != 64 {
			t.Fatalf("producer=%d ack=%d err=%v", result.worker, len(result.sequences), result.err)
		}
		for i, id := range result.sequences {
			if id <= 0 || acked[id] != "" {
				t.Fatal("missing or duplicate producer acknowledgement")
			}
			acked[id] = fmt.Sprintf("mixed-%d-%d", result.worker, i)
		}
	}
	rows, err := f.db.QueryContext(ctx, "SELECT id,message_id,body FROM messages WHERE queue='produce'")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for rows.Next() {
		var id int64
		var key string
		var body []byte
		if err := rows.Scan(&id, &key, &body); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		if acked[id] != key || string(body) != key {
			_ = rows.Close()
			t.Fatal("acknowledged payload changed")
		}
		count++
	}
	rowsErr := rows.Err()
	_ = rows.Close()
	if rowsErr != nil || count != len(acked) || count != 512 {
		t.Fatalf("retained ACK ledger=%d/%d err=%v", count, len(acked), rowsErr)
	}
	metrics, err = e.Stats(ctx, "consume")
	if err != nil || metrics.Total != 0 || metrics.DeadLettered != 0 {
		t.Fatalf("consumer final metrics=%+v err=%v", metrics, err)
	}
	if busy := f.snapshot().Busy; busy != 0 {
		t.Fatalf("single-owner mixed workload had %d busy responses", busy)
	}
}
