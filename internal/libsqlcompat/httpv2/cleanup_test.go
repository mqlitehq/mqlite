package hranaV2

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func waitCleanupIdle(t *testing.T, manager *CleanupManager) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for manager.Stats().Active != 0 {
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatalf("cleanup slots did not return: %+v", manager.Stats())
		}
	}
}

func TestCleanupConcurrencyDeadlinesAndCapacityReuse(t *testing.T) {
	var requests, warnings atomic.Int64
	warningObserved := make(chan struct{})
	started := make(chan string, streamCleanupLimit+1)
	finished := make(chan string, streamCleanupLimit)
	abort := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var request struct {
			Baton    string                  `json:"baton"`
			Requests []struct{ Type string } `json:"requests"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode cleanup: %v", err)
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/v2/pipeline" || len(request.Requests) != 1 || request.Requests[0].Type != "close" {
			t.Errorf("unexpected cleanup request shape")
			return
		}
		requests.Add(1)
		started <- request.Baton
		if request.Baton == "reuse" {
			_, _ = io.WriteString(w, `{"baton":null,"results":[]}`)
			return
		}
		if strings.HasPrefix(request.Baton, "body-") {
			_, _ = io.WriteString(w, `{"baton":`)
			w.(http.Flusher).Flush()
		}
		select {
		case <-r.Context().Done():
		case <-abort:
		}
		finished <- request.Baton
	}))
	defer server.Close()
	defer close(abort)
	manager := NewCleanupManager()
	manager.warning = func() {
		if warnings.Add(1) == 1 {
			close(warningObserved)
		}
	}
	before := time.Now()
	for i := 0; i < streamCleanupLimit; i++ {
		prefix := "header"
		if i%2 != 0 {
			prefix = "body"
		}
		conn := Connect(server.URL, "", "", false, "", manager).(*hranaV2Conn)
		conn.baton = fmt.Sprintf("%s-%d", prefix, i)
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
		if conn.baton != "" {
			t.Fatal("Close retained the captured baton")
		}
		_ = conn.Close() // Serial repeated Close must not issue another request.
	}
	ready := time.NewTimer(2 * time.Second)
	defer ready.Stop()
	for i := 0; i < streamCleanupLimit; i++ {
		select {
		case <-started:
		case <-ready.C:
			t.Fatal("not all admitted header/body cleanups reached the server")
		}
	}
	if stats := manager.Stats(); stats.Active != streamCleanupLimit || stats.Skipped != 0 {
		t.Fatalf("admitted cleanup stats = %+v", stats)
	}
	const overflow = 64
	var workers sync.WaitGroup
	workers.Add(overflow)
	for i := 0; i < overflow; i++ {
		go func() {
			defer workers.Done()
			conn := Connect(server.URL, "", "", false, "", manager).(*hranaV2Conn)
			conn.baton = "skipped"
			_ = conn.Close()
			_ = conn.Close()
		}()
	}
	returned := make(chan struct{})
	go func() { workers.Wait(); close(returned) }()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("overflow Close calls queued behind cleanup capacity")
	}
	if stats := manager.Stats(); stats.Active != streamCleanupLimit || stats.Skipped != overflow || warnings.Load() != 0 {
		t.Fatalf("overload stats = %+v, warnings=%d", stats, warnings.Load())
	}
	if requests.Load() != streamCleanupLimit {
		t.Fatalf("skipped requests reached HTTP: %d", requests.Load())
	}
	deadline := time.NewTimer(time.Until(before.Add(streamCleanupTimeout + 2*time.Second)))
	defer deadline.Stop()
	for i := 0; i < streamCleanupLimit; i++ {
		select {
		case <-finished:
		case <-deadline.C:
			t.Fatal("header/body cleanup did not cancel within the five-second budget")
		}
	}
	waitCleanupIdle(t, manager)
	select {
	case <-warningObserved:
	case <-time.After(time.Second):
		t.Fatal("completed cleanup did not report skipped work")
	}
	conn := Connect(server.URL, "", "", false, "", manager).(*hranaV2Conn)
	conn.baton = "reuse"
	_ = conn.Close()
	select {
	case baton := <-started:
		if baton != "reuse" {
			t.Fatalf("unexpected post-deadline cleanup %q", baton)
		}
	case <-time.After(time.Second):
		t.Fatal("returned cleanup capacity could not be reused")
	}
	waitCleanupIdle(t, manager)
	if stats := manager.Stats(); stats.Skipped != overflow || requests.Load() != streamCleanupLimit+1 || warnings.Load() != 1 {
		t.Fatalf("reused stats = %+v, requests=%d, warnings=%d", stats, requests.Load(), warnings.Load())
	}
}

func TestCleanupDoesNotChangeConfirmedTransactionOrConnectionReuse(t *testing.T) {
	var mu sync.Mutex
	var sqls, batons, peers []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var request struct {
			Baton    string `json:"baton"`
			Requests []struct {
				Type string `json:"type"`
				Stmt struct {
					SQL string `json:"sql"`
				} `json:"stmt"`
			} `json:"requests"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || len(request.Requests) != 1 {
			t.Errorf("invalid transaction request: %v", err)
			return
		}
		mu.Lock()
		peers = append(peers, r.RemoteAddr)
		batons = append(batons, request.Baton)
		if request.Requests[0].Type == "close" {
			sqls = append(sqls, "close")
		} else {
			sqls = append(sqls, request.Requests[0].Stmt.SQL)
		}
		mu.Unlock()
		if request.Requests[0].Type == "close" {
			_, _ = io.WriteString(w, `{"baton":null,"results":[]}`)
		} else {
			_, _ = io.WriteString(w, `{"baton":"same-stream","results":[{"type":"ok","response":{"type":"execute","result":{"cols":[],"rows":[],"affected_row_count":1,"last_insert_rowid":"7"}}}]}`)
		}
	}))
	defer server.Close()
	manager := NewCleanupManager()
	conn := Connect(server.URL, "", "", false, "", manager).(*hranaV2Conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	tx, err := conn.BeginTx(ctx, driver.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if stats := manager.Stats(); stats.Active != 0 {
		t.Fatalf("cleanup began before transaction ended: %+v", stats)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	waitCleanupIdle(t, manager)
	if err != nil {
		t.Fatalf("cleanup changed the confirmed commit result: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(sqls, ",") != "BEGIN,COMMIT,close" || strings.Join(batons, ",") != ",same-stream,same-stream" {
		t.Fatalf("transaction/cleanup sequence = %q, batons=%q", sqls, batons)
	}
	for _, peer := range peers {
		if peer != peers[0] {
			t.Fatalf("HTTP keep-alive was not retained: %v", peers)
		}
	}
}

func TestCleanupCapacityIsPerManagerAndEmptyBatonIsANoop(t *testing.T) {
	var warnings atomic.Int64
	first, second := NewCleanupManager(), NewCleanupManager()
	first.warning = func() { warnings.Add(1) }
	for i := 0; i < streamCleanupLimit; i++ {
		first.slots <- struct{}{}
	}
	first.closeStreamAsync("", "http://127.0.0.1:1", "", "", "")
	first.closeStreamAsync("discarded", "http://127.0.0.1:1", "", "", "")
	if first.Stats().Skipped != 1 || warnings.Load() != 0 || second.Stats().Active != 0 || second.Stats().Skipped != 0 {
		t.Fatalf("per-database/empty-token accounting = %+v / %+v", first.Stats(), second.Stats())
	}
	for i := 0; i < streamCleanupLimit; i++ {
		<-first.slots
	}
}

func TestConfirmedCommitSurvivesFailedCleanup(t *testing.T) {
	var committed atomic.Bool
	var closes atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var request struct {
			Requests []struct {
				Type string `json:"type"`
				Stmt struct {
					SQL string `json:"sql"`
				} `json:"stmt"`
			} `json:"requests"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || len(request.Requests) != 1 {
			t.Errorf("decode transaction: %v", err)
			return
		}
		if request.Requests[0].Type == "close" {
			closes.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `{"error":"fixture cleanup unavailable"}`)
			return
		}
		if request.Requests[0].Stmt.SQL == "COMMIT" {
			committed.Store(true)
		}
		_, _ = io.WriteString(w, `{"baton":"committed-stream","results":[{"type":"ok","response":{"type":"execute","result":{"cols":[],"rows":[],"affected_row_count":1}}}]}`)
	}))
	defer server.Close()
	manager := NewCleanupManager()
	conn := Connect(server.URL, "", "", false, "", manager).(*hranaV2Conn)
	tx, err := conn.BeginTx(context.Background(), driver.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil || !committed.Load() {
		t.Fatalf("commit was not acknowledged: err=%v, applied=%v", err, committed.Load())
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("best-effort cleanup changed the confirmed transaction outcome: %v", err)
	}
	waitCleanupIdle(t, manager)
	if closes.Load() != 1 || !committed.Load() || manager.Stats().Skipped != 0 {
		t.Fatalf("cleanup retried or changed committed state: closes=%d, committed=%v, stats=%+v", closes.Load(), committed.Load(), manager.Stats())
	}
}

func TestResetSessionCapturesOldBatonAndLeavesANewStream(t *testing.T) {
	var mu sync.Mutex
	var closeBatons []string
	var nextBaton string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var request struct {
			Baton    string `json:"baton"`
			Requests []struct {
				Type string `json:"type"`
				Stmt struct {
					SQL string `json:"sql"`
				} `json:"stmt"`
			} `json:"requests"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || len(request.Requests) != 1 {
			t.Errorf("decode reset request: %v", err)
			return
		}
		if request.Requests[0].Type == "close" {
			mu.Lock()
			closeBatons = append(closeBatons, request.Baton)
			mu.Unlock()
			_, _ = io.WriteString(w, `{"baton":null,"results":[]}`)
			return
		}
		baton := "old-stream"
		if request.Requests[0].Stmt.SQL == "SELECT 2" {
			mu.Lock()
			nextBaton = request.Baton
			mu.Unlock()
			baton = "new-stream"
		}
		_, _ = fmt.Fprintf(w, `{"baton":%q,"results":[{"type":"ok","response":{"type":"execute","result":{"cols":[],"rows":[],"affected_row_count":0}}}]}`, baton)
	}))
	defer server.Close()
	manager := NewCleanupManager()
	conn := Connect(server.URL, "", "", false, "", manager).(*hranaV2Conn)
	if _, err := conn.ExecContext(context.Background(), "SELECT 1", nil); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := conn.ResetSession(canceled); err != nil || conn.baton != "" {
		t.Fatalf("ResetSession changed its nonblocking contract: err=%v, baton=%q", err, conn.baton)
	}
	waitCleanupIdle(t, manager)
	if _, err := conn.ExecContext(context.Background(), "SELECT 2", nil); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	_ = conn.Close()
	waitCleanupIdle(t, manager)
	mu.Lock()
	defer mu.Unlock()
	if nextBaton != "" || strings.Join(closeBatons, ",") != "old-stream,new-stream" {
		t.Fatalf("captured cleanup/new-stream contract changed: next=%q, closes=%q", nextBaton, closeBatons)
	}
}

func TestBlockedWarningNeverBlocksForegroundOrHoldsHTTPResources(t *testing.T) {
	manager := NewCleanupManager()
	warningEntered, warningReturned, releaseWarning := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var warningCalls atomic.Int64
	manager.warning = func() {
		if warningCalls.Add(1) == 1 {
			close(warningEntered)
		}
		<-releaseWarning
		close(warningReturned)
	}
	phase1, phase2 := make(chan struct{}), make(chan struct{})
	started := make(chan string, 2*streamCleanupLimit)
	var activeHTTP atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var request struct {
			Baton    string                  `json:"baton"`
			Requests []struct{ Type string } `json:"requests"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || len(request.Requests) != 1 {
			t.Errorf("decode blocked-warning request: %v", err)
			return
		}
		if request.Requests[0].Type != "close" {
			_, _ = io.WriteString(w, `{"baton":null,"results":[{"type":"ok","response":{"type":"execute","result":{"cols":[],"rows":[],"affected_row_count":0}}}]}`)
			return
		}
		activeHTTP.Add(1)
		defer activeHTTP.Add(-1)
		started <- request.Baton
		gate := phase1
		if strings.HasPrefix(request.Baton, "phase2") {
			gate = phase2
		}
		select {
		case <-gate:
		case <-r.Context().Done():
		}
		_, _ = io.WriteString(w, `{"baton":null,"results":[]}`)
	}))
	defer server.Close()
	var release1, release2, releaseLog sync.Once
	var firstDone, secondDone chan struct{}
	cleanup := func() {
		releaseLog.Do(func() { close(releaseWarning) })
		release1.Do(func() { close(phase1) })
		release2.Do(func() { close(phase2) })
		for _, done := range []chan struct{}{firstDone, secondDone} {
			if done == nil {
				continue
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("foreground Close did not join after releasing the logger")
			}
		}
		waitCleanupIdle(t, manager)
		if warningCalls.Load() > 0 {
			select {
			case <-warningReturned:
			case <-time.After(time.Second):
				t.Error("warning callback did not return after release")
			}
		}
	}
	defer cleanup()
	fill := func(phase string) {
		t.Helper()
		for i := 0; i < streamCleanupLimit; i++ {
			conn := Connect(server.URL, "", "", false, "", manager).(*hranaV2Conn)
			conn.baton = fmt.Sprintf("%s-%d", phase, i)
			_ = conn.Close()
		}
		deadline := time.NewTimer(2 * time.Second)
		defer deadline.Stop()
		for i := 0; i < streamCleanupLimit; i++ {
			select {
			case <-started:
			case <-deadline.C:
				t.Fatal("cleanup admission barrier was not reached")
			}
		}
	}
	overflow := func() chan struct{} {
		done := make(chan struct{})
		go func() {
			conn := Connect(server.URL, "", "", false, "", manager).(*hranaV2Conn)
			conn.baton = "overflow"
			_ = conn.Close()
			close(done)
		}()
		return done
	}
	fill("phase1")
	firstDone = overflow()
	firstReturned := false
	select {
	case <-firstDone:
		firstReturned = true
	case <-time.After(time.Second):
	}
	// Do not fail while a callback is parked: release/join in the deferred
	// cleanup first, then report whether foreground Close hit the logger gate.
	release1.Do(func() { close(phase1) })
	select {
	case <-warningEntered:
	case <-time.After(time.Second):
		t.Fatal("overflow was not reported by a completed cleanup worker")
	}
	waitCleanupIdle(t, manager)
	if activeHTTP.Load() != 0 {
		t.Errorf("warning retained HTTP resources: %d", activeHTTP.Load())
	}
	conn := Connect(server.URL, "", "", false, "", manager).(*hranaV2Conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, queryErr := conn.ExecContext(ctx, "SELECT 1", nil)
	cancel()
	_ = conn.Close()
	if queryErr != nil {
		t.Errorf("SQL was blocked by the parked warning: %v", queryErr)
	}
	fill("phase2")
	secondDone = overflow()
	secondReturned := false
	select {
	case <-secondDone:
		secondReturned = true
	case <-time.After(time.Second):
	}
	release2.Do(func() { close(phase2) })
	waitCleanupIdle(t, manager)
	if activeHTTP.Load() != 0 || manager.Stats().Skipped != 2 || warningCalls.Load() != 1 {
		t.Errorf("blocked-warning accounting: HTTP=%d, stats=%+v, warningCalls=%d", activeHTTP.Load(), manager.Stats(), warningCalls.Load())
	}
	cleanup()
	if !firstReturned || !secondReturned {
		t.Errorf("foreground Close waited for blocked logger: firstReturned=%v secondReturned=%v", firstReturned, secondReturned)
	}
}
