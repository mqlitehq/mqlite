package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestSQLiteHranaBatchCommittedResponseLossNeverReplays(t *testing.T) {
	f := newSQLiteHranaFixture(t)
	var armed atomic.Bool
	var disconnected atomic.Int64
	fault := make(chan error, 1)
	// The pinned HTTP driver receives a real socket EOF only after the fixture
	// has run COMMIT against SQLite. Recording the response prevents net/http
	// from sending an acknowledgement before we close the client connection.
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
		_ = r.Body.Close()
		if err != nil {
			http.Error(w, "read synthetic request", http.StatusBadRequest)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		var pipeline struct {
			Requests []sqliteHranaRequest `json:"requests"`
		}
		isCommit := false
		if json.Unmarshal(body, &pipeline) == nil {
			for _, request := range pipeline.Requests {
				if request.Type == "execute" && strings.EqualFold(strings.TrimSpace(request.Stmt.SQL), "COMMIT") {
					isCommit = true
				}
			}
		}
		if !isCommit || !armed.CompareAndSwap(true, false) {
			f.serveHTTP(w, r)
			return
		}
		response := httptest.NewRecorder()
		f.serveHTTP(response, r)
		var committed int
		if err := f.db.QueryRow(`SELECT count(*) FROM messages WHERE queue='batch'`).Scan(&committed); err != nil || committed != 64 {
			fault <- fmt.Errorf("disconnect was not after durable synthetic commit: rows=%d error=%v", committed, err)
			http.Error(w, "invalid fault point", http.StatusInternalServerError)
			return
		}
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			fault <- err
			return
		}
		disconnected.Add(1)
		fault <- nil
		_ = conn.Close()
	}))
	t.Cleanup(endpoint.Close)
	dsn := "libsql://" + strings.TrimPrefix(endpoint.URL, "http://") + "?tls=0"
	e, err := Open(context.Background(), Options{DB: dsn, DisableBackground: true,
		PreserveUnexpiredLeases: true, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	ctx := context.Background()
	if err := e.CreateQueue(ctx, "batch", QueueConfig{DedupWindowMs: 60000}); err != nil {
		t.Fatal(err)
	}
	inputs := make([]OutMessage, 64)
	for i := range inputs {
		inputs[i] = OutMessage{MessageID: fmt.Sprintf("synthetic-%02d", i), Body: []byte(fmt.Sprintf("body-%02d", i))}
	}
	e.db.storage.mu.Lock()
	e.db.storage.operations = nil
	e.db.storage.mu.Unlock()
	before := f.snapshot()
	armed.Store(true)
	seqs, err := e.Send(ctx, "batch", inputs...)
	after := f.snapshot()
	if !errors.Is(err, ErrOutcomeUnknown) || seqs != nil {
		t.Fatalf("lost committed acknowledgement returned sequences=%v error=%v", seqs, err)
	}
	if disconnected.Load() != 1 {
		t.Fatalf("committed-response disconnections=%d, want one", disconnected.Load())
	}
	if err := <-fault; err != nil {
		t.Fatal(err)
	}
	if after.Begins-before.Begins != 1 || after.Commits-before.Commits != 1 || after.MessageInserts-before.MessageInserts != 64 {
		t.Fatalf("unknown COMMIT was replayed: before=%+v after=%+v", before, after)
	}
	for _, table := range []string{"messages", "dedup"} {
		var count int
		if err := f.db.QueryRow("SELECT count(*) FROM " + table + " WHERE queue='batch'").Scan(&count); err != nil || count != 64 {
			t.Fatalf("committed %s rows=%d, want 64: %v", table, count, err)
		}
	}
	rows, err := f.db.Query(`SELECT m.message_id,m.body,d.request_hash FROM messages m
		JOIN dedup d ON d.queue=m.queue AND d.message_id=m.message_id AND d.seq_number=m.id
		WHERE m.queue='batch' ORDER BY m.id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	index := 0
	for rows.Next() {
		var id, hash string
		var body []byte
		if err := rows.Scan(&id, &body, &hash); err != nil {
			t.Fatal(err)
		}
		if index >= len(inputs) || id != inputs[index].MessageID || !bytes.Equal(body, inputs[index].Body) || hash != sha256hex(body) {
			t.Fatalf("committed batch mapping changed at input %d", index)
		}
		index++
	}
	if err := rows.Err(); err != nil || index != 64 {
		t.Fatalf("committed message/dedup mappings=%d, want 64: %v", index, err)
	}
	ops := nonzeroStorageOperations(e.db.storage.snapshot(e.db.sql.Stats()))
	if len(ops) != 1 || ops[0].Operation != "transaction" || ops[0].Outcome != "outcome_unknown" || ops[0].Retries != 0 || ops[0].Duration.Count != 1 {
		t.Fatalf("unknown batch commit was counted as acknowledged success or retry: %+v", ops)
	}
	requireEvents(t, e, "batch", map[string]uint64{})
	t.Logf("committed response lost: begins=1 commits=1 messages=64 dedup=64 retries=0; Send returned outcome unknown")
}
