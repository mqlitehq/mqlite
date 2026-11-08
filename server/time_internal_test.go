package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/wire"
)

const elapsedHeaderUnderTest = "X-Mqlite-Server-Elapsed-Ms"

func elapsedMilliseconds(t *testing.T, header http.Header) int64 {
	t.Helper()
	value := header.Get(elapsedHeaderUnderTest)
	ms, err := strconv.ParseInt(value, 10, 64)
	if err != nil || ms < 0 {
		t.Fatalf("invalid server elapsed header %q: %v", value, err)
	}
	return ms
}

func elapsedTestEngine(t *testing.T, now func() int64) *engine.Engine {
	t.Helper()
	e, err := engine.Open(context.Background(), engine.Options{DB: ":memory:", DisableBackground: true, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	if err := e.CreateQueue(context.Background(), "q", engine.QueueConfig{}); err != nil {
		t.Fatal(err)
	}
	return e
}

func TestServerElapsedHeaderIncludesLongPollAndStorageWait(t *testing.T) {
	t.Run("long_poll", func(t *testing.T) {
		e := elapsedTestEngine(t, nil)
		s := New(e, nil)
		body, _ := json.Marshal(wire.ReceiveRequest{Queue: "q", MaxMessages: 1, WaitTimeMs: 40})
		rec := httptest.NewRecorder()
		started := time.Now()
		s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, wire.PathReceive, bytes.NewReader(body)))
		total := time.Since(started).Milliseconds()
		elapsed := elapsedMilliseconds(t, rec.Result().Header)
		if rec.Code != http.StatusOK || elapsed < 20 || elapsed > total {
			t.Fatalf("long-poll status=%d elapsed=%dms total=%dms", rec.Code, elapsed, total)
		}
	})
	t.Run("storage_writer_wait", func(t *testing.T) {
		e := elapsedTestEngine(t, nil)
		entered := make(chan struct{})
		release := make(chan struct{})
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		t.Cleanup(unblock)
		txDone := make(chan error, 1)
		go func() {
			txDone <- e.Tx(context.Background(), func(*engine.EngineTx) error {
				close(entered)
				<-release
				return nil
			})
		}()
		<-entered
		s := New(e, nil)
		handlerEntered := make(chan struct{})
		h := s.withServerTime(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(handlerEntered)
			s.mux.ServeHTTP(w, r)
		}))
		body, _ := json.Marshal(wire.ReceiveRequest{Queue: "q", MaxMessages: 1})
		rec := httptest.NewRecorder()
		done := make(chan struct{})
		started := time.Now()
		go func() {
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, wire.PathReceive, bytes.NewReader(body)))
			close(done)
		}()
		<-handlerEntered
		<-time.After(25 * time.Millisecond)
		unblock()
		<-done
		total := time.Since(started).Milliseconds()
		if err := <-txDone; err != nil {
			t.Fatal(err)
		}
		elapsed := elapsedMilliseconds(t, rec.Result().Header)
		if rec.Code != http.StatusOK || elapsed < 20 || elapsed > total {
			t.Fatalf("storage-wait status=%d elapsed=%dms total=%dms", rec.Code, elapsed, total)
		}
	})
}

func TestServerElapsedHeaderUsesMonotonicClock(t *testing.T) {
	var clock atomic.Int64
	clock.Store(1_800_000_000_000)
	e := elapsedTestEngine(t, clock.Load)
	s := New(e, nil)
	h := s.withServerTime(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-time.After(25 * time.Millisecond)
		clock.Add(-30 * 24 * 60 * 60 * 1000)
		w.WriteHeader(http.StatusOK)
	}))
	rec := httptest.NewRecorder()
	started := time.Now()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	total := time.Since(started).Milliseconds()
	elapsed := elapsedMilliseconds(t, rec.Result().Header)
	if elapsed < 20 || elapsed > total {
		t.Fatalf("wall-clock correction affected elapsed: %dms, total=%dms", elapsed, total)
	}
	if got := rec.Header().Get(wire.ServerTimeHeader); got != strconv.FormatInt(clock.Load(), 10) {
		t.Fatalf("server wall-clock observation=%q, want corrected lifecycle clock", got)
	}
}

func TestServerElapsedHeaderIsLowerBoundBeforeWallSampleAndStaysFixed(t *testing.T) {
	s := &Server{}
	var wallSamples int
	started := time.Now()
	h := s.withServerTime(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writer := w.(*serverTimeResponseWriter)
		writer.now = func() int64 {
			wallSamples++
			// Elapsed must be floored and stored BEFORE sampling the lifecycle
			// wall clock: subtracting later server work could extend a lease.
			bound := time.Since(started).Milliseconds()
			if elapsed := elapsedMilliseconds(t, writer.Header()); elapsed > bound {
				t.Fatalf("elapsed=%dms exceeds pre-wall-sample bound=%dms", elapsed, bound)
			}
			return int64(1_800_000_000_000 + wallSamples)
		}
		w.WriteHeader(http.StatusAccepted)
		elapsed, wall := writer.Header().Get(elapsedHeaderUnderTest), writer.Header().Get(wire.ServerTimeHeader)
		_, _ = w.Write([]byte("first"))
		_, _ = w.Write([]byte("second"))
		if writer.Header().Get(elapsedHeaderUnderTest) != elapsed || writer.Header().Get(wire.ServerTimeHeader) != wall {
			t.Fatal("later response writes changed the first-header clock observation")
		}
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	if wallSamples != 1 {
		t.Fatalf("response sampled wall clock %d times, want once", wallSamples)
	}
}

func TestServerElapsedHeaderOnEveryResponseCategory(t *testing.T) {
	e := elapsedTestEngine(t, nil)
	s := New(e, []string{"secret"})
	s.CORS = "*"
	for _, tc := range []struct {
		name, method, path, body string
		status                   int
	}{
		{"success", http.MethodGet, "/", "", http.StatusOK},
		{"unauthorized", http.MethodPost, wire.PathListQueues, "{}", http.StatusUnauthorized},
		{"invalid_json", http.MethodPost, wire.PathSend, "{", http.StatusBadRequest},
		{"preflight", http.MethodOptions, wire.PathReceive, "", http.StatusNoContent},
		{"not_found", http.MethodGet, "/missing", "", http.StatusNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, tc.path, bytes.NewBufferString(tc.body))
			if tc.name != "unauthorized" && tc.method != http.MethodOptions {
				req.Header.Set("Authorization", "Bearer secret")
			}
			req.Header.Set("Content-Type", "application/json")
			s.Handler().ServeHTTP(rec, req)
			if rec.Code != tc.status {
				t.Fatalf("status=%d, want %d", rec.Code, tc.status)
			}
			_ = elapsedMilliseconds(t, rec.Result().Header)
			if rec.Result().Header.Get(wire.ServerTimeHeader) == "" {
				t.Fatal("missing matching server-time observation")
			}
		})
	}
	for _, tc := range []struct {
		name string
		s    *Server
	}{
		{"invalid_monitor", New(e, nil)},
		{"metrics_without_auth", New(e, nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "invalid_monitor" {
				tc.s.MonitorTokens = []string{"monitor"}
			} else {
				tc.s.Metrics = true
			}
			rec := httptest.NewRecorder()
			tc.s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
			if rec.Code != http.StatusInternalServerError {
				t.Fatalf("misconfiguration status=%d", rec.Code)
			}
			_ = elapsedMilliseconds(t, rec.Result().Header)
		})
	}
	// net/http sends an implicit 200 when a handler returns without a write.
	rec := httptest.NewRecorder()
	s.withServerTime(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	_ = elapsedMilliseconds(t, rec.Result().Header)
}

type elapsedFlushRecorder struct {
	*httptest.ResponseRecorder
	err error
}

func (r *elapsedFlushRecorder) FlushError() error { r.Flush(); return r.err }

func TestServerElapsedHeaderResponseControllerFlushPreservesWriter(t *testing.T) {
	sentinel := errors.New("controlled flush failure")
	underlying := &elapsedFlushRecorder{ResponseRecorder: httptest.NewRecorder(), err: sentinel}
	s := &Server{}
	h := s.withServerTime(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if w.(*serverTimeResponseWriter).Unwrap() != underlying {
			t.Fatal("timing wrapper changed ResponseController's underlying writer")
		}
		if err := http.NewResponseController(w).Flush(); !errors.Is(err, sentinel) {
			t.Fatalf("flush error=%v, want underlying error", err)
		}
	}))
	h.ServeHTTP(underlying, httptest.NewRequest(http.MethodGet, "/", nil))
	if !underlying.Flushed {
		t.Fatal("timing wrapper did not flush the underlying writer")
	}
	_ = elapsedMilliseconds(t, underlying.Result().Header)
}

type elapsedUnflushableWriter struct{ header http.Header }

func (w *elapsedUnflushableWriter) Header() http.Header       { return w.header }
func (*elapsedUnflushableWriter) WriteHeader(int)             {}
func (*elapsedUnflushableWriter) Write(p []byte) (int, error) { return len(p), nil }

func TestServerElapsedHeaderUnsupportedFlushStaysUnsupported(t *testing.T) {
	underlying := &elapsedUnflushableWriter{header: make(http.Header)}
	s := &Server{}
	s.withServerTime(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if _, exposed := w.(http.Flusher); exposed {
			t.Fatal("timing wrapper unexpectedly advertises http.Flusher")
		}
		if err := http.NewResponseController(w).Flush(); !errors.Is(err, http.ErrNotSupported) {
			t.Fatalf("unsupported flush error=%v", err)
		}
	})).ServeHTTP(underlying, httptest.NewRequest(http.MethodGet, "/", nil))
	_ = elapsedMilliseconds(t, underlying.Header())
}
