//go:build mqlite_perf_diag

package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/server"
)

type casePlan struct {
	ID           string `json:"id"`
	SDK          string `json:"sdk"`
	Family       string `json:"family"`
	Concurrency  int    `json:"concurrency"`
	Batch        int    `json:"batch"`
	Count        int    `json:"count"`
	Payload      int    `json:"payload_bytes"`
	LockSeconds  int    `json:"lock_seconds"`
	DedupSeconds int    `json:"dedup_seconds"`
	Background   bool   `json:"background"`
	DelayMS      int    `json:"synthetic_provider_delay_ms"`
	PoolLimit    int    `json:"pool_limit_control,omitempty"`
	Admission    int    `json:"admission_control,omitempty"`
}

func plans(mode string) []casePlan {
	var ps []casePlan
	if mode == "mixed" {
		for _, sdk := range []string{"native", "low-floor"} {
			for _, lock := range []int{3, 30} {
				ps = append(ps, casePlan{ID: fmt.Sprintf("mixed_%s_lock%d", sdk, lock), SDK: sdk, Family: "mixed_receiver", Concurrency: 4, Batch: 16, Count: 68, Payload: 256, LockSeconds: lock, DedupSeconds: 3600, Background: true})
			}
		}
		return ps
	}
	if mode == "quick" {
		for _, p := range []struct {
			id                 string
			b, n, dedup, delay int
		}{{"isolated_c1_b1", 1, 16, 3600, 0}, {"isolated_c1_b16", 16, 16, 3600, 0}, {"isolated_c1_b64", 64, 64, 3600, 0}, {"control_dedup_off_b16", 16, 16, 0, 0}, {"control_delay_75ms_b16", 16, 16, 3600, 75}} {
			ps = append(ps, casePlan{ID: p.id, SDK: "native", Family: "isolated_diagnostic", Concurrency: 1, Batch: p.b, Count: p.n, Payload: 256, DedupSeconds: p.dedup, DelayMS: p.delay})
		}
		for i := range ps {
			ps[i].LockSeconds = 600
		}
		return ps
	}
	for _, sdk := range []string{"native", "low-floor"} {
		for _, p := range []struct{ c, b, n int }{{1, 1, 16}, {1, 64, 64}, {8, 1, 32}, {32, 1, 32}} {
			ps = append(ps, casePlan{ID: fmt.Sprintf("original_%s_c%d_b%d", sdk, p.c, p.b), SDK: sdk, Family: "original_v11_contention", Concurrency: p.c, Batch: p.b, Count: p.n, Payload: 256, DedupSeconds: 3600, Background: true})
		}
	}
	// These never replace or rename an original failure. Local SQLite controls
	// retain its invariant: remote pool changes are explicitly not applicable.
	for _, p := range []struct {
		id              string
		pool, admission int
	}{{"control_pool1_c32_b1", 1, 0}, {"control_pool4_c32_b1", 4, 0}, {"control_admission1_c32_b1", 0, 1}} {
		ps = append(ps, casePlan{ID: p.id, SDK: "native", Family: "separate_counterfactual", Concurrency: 32, Batch: 1, Count: 32, Payload: 256, DedupSeconds: 3600, Background: true, PoolLimit: p.pool, Admission: p.admission})
	}
	for i := range ps {
		ps[i].LockSeconds = 600
	}
	return ps
}

func selectedPlans(mode, selection string) ([]casePlan, error) {
	if mode != "quick" && mode != "contention" && mode != "mixed" {
		return nil, oracle("invalid_mode")
	}
	ps := plans(mode)
	if selection == "all" {
		return ps, nil
	}
	for _, p := range ps {
		if selection == p.ID {
			return []casePlan{p}, nil
		}
	}
	return nil, oracle("invalid_case_selection")
}

func queueName(run string, i int, sdk string) string {
	return fmt.Sprintf("diag_%s_%02d_%s", run, i, strings.ReplaceAll(sdk, "-", "_"))
}

type operationSample struct {
	ID              string   `json:"id"`
	Phase           string   `json:"phase"`
	ElapsedMS       float64  `json:"elapsed_ms"`
	AdmissionWaitMS float64  `json:"admission_wait_ms"`
	Failure         *failure `json:"failure,omitempty"`
}
type poolSnapshot struct {
	MaxOpen   int     `json:"max_open"`
	Open      int     `json:"open"`
	InUse     int     `json:"in_use"`
	Idle      int     `json:"idle"`
	WaitCount int64   `json:"wait_count"`
	WaitMS    float64 `json:"wait_ms"`
}

func poolView(p sql.DBStats) poolSnapshot {
	return poolSnapshot{p.MaxOpenConnections, p.OpenConnections, p.InUse, p.Idle, p.WaitCount, float64(p.WaitDuration) / float64(time.Millisecond)}
}

type brokerEvent struct {
	OperationID     string   `json:"operation_id"`
	Method          string   `json:"method"`
	Status          int      `json:"status"`
	HeaderElapsedMS float64  `json:"headers_elapsed_ms"`
	Failure         *failure `json:"failure,omitempty"`
}
type operationKey struct{}

const diagnosticHeader = "X-Mqlite-Diag-Operation"

type brokerTrace struct {
	next    http.RoundTripper
	mu      sync.Mutex
	events  []brokerEvent
	dropped int
	mixed   *mixedState
}

func (t *brokerTrace) RoundTrip(r *http.Request) (*http.Response, error) {
	id, _ := r.Context().Value(operationKey{}).(string)
	if !safeID(id) {
		id = "unknown_operation"
	}
	copy := r.Clone(r.Context())
	copy.Header = r.Header.Clone()
	copy.Header.Set(diagnosticHeader, id)
	start := time.Now()
	resp, err := t.next.RoundTrip(copy)
	s := 0
	if resp != nil {
		s = resp.StatusCode
	}
	t.mu.Lock()
	if len(t.events) < 65536 {
		t.events = append(t.events, brokerEvent{OperationID: id, Method: brokerMethod(r.URL.Path), Status: s, HeaderElapsedMS: float64(time.Since(start)) / float64(time.Millisecond), Failure: safeFailure(err)})
	} else {
		t.dropped++
	}
	t.mu.Unlock()
	if t.mixed != nil && err == nil {
		t.mixed.observe(r, resp, start)
	}
	return resp, err
}
func brokerMethod(path string) string {
	for _, method := range []string{"CreateQueue", "DeleteQueue", "ListQueues", "Send", "Receive", "Complete", "CompleteBatch", "Renew", "RenewBatch", "Peek", "Stats"} {
		if path == "/mqlite.v1.QueueService/"+method {
			return method
		}
	}
	return "other"
}
func (t *brokerTrace) snapshot() ([]brokerEvent, int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]brokerEvent(nil), t.events...), t.dropped
}

type logCollector struct {
	mu     sync.Mutex
	counts map[string]int
	errors int
}

func (l *logCollector) Enabled(context.Context, slog.Level) bool { return true }
func (l *logCollector) Handle(_ context.Context, r slog.Record) error {
	event := "other"
	switch r.Message {
	case "maintenance.failure", "storage.failure", "maintenance.interrupted", "storage.interrupted", "maintenance.recovered":
		event = r.Message
	}
	l.mu.Lock()
	l.counts[event]++
	if r.Level >= slog.LevelError {
		l.errors++
	}
	l.mu.Unlock()
	return nil
}
func (l *logCollector) WithAttrs([]slog.Attr) slog.Handler { return l }
func (l *logCollector) WithGroup(string) slog.Handler      { return l }
func (l *logCollector) snapshot() (map[string]int, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	m := map[string]int{}
	for k, v := range l.counts {
		m[k] = v
	}
	return m, l.errors
}

type cleanupResult struct {
	Attempted       bool     `json:"attempted"`
	ExactOwnedQueue bool     `json:"exact_owned_queue"`
	Deleted         bool     `json:"deleted"`
	State           string   `json:"state"`
	AbsentReadback  bool     `json:"absent_readback"`
	Failure         *failure `json:"failure,omitempty"`
}
type quietResult struct {
	Required                     bool    `json:"required"`
	ObservedQuiet                bool    `json:"observed_quiet"`
	QuietWindowMS                int     `json:"quiet_window_ms"`
	WaitedMS                     float64 `json:"waited_ms"`
	HiddenDriverGoroutinesJoined bool    `json:"hidden_driver_goroutines_joined"`
}

// The pinned driver launches close-only HTTP asynchronously. An observed quiet
// window catches late HTTP without inventing a way to join its private goroutines.
func waitProviderQuiet(ctx context.Context, snapshot func() providerTraceSnapshot, quietWindow, poll time.Duration) quietResult {
	r := quietResult{Required: true, QuietWindowMS: int(quietWindow / time.Millisecond)}
	started := time.Now()
	stable := started
	last := snapshot()
	for {
		now := time.Now()
		current := snapshot()
		changed := current.Observed != last.Observed || current.Finished != last.Finished || current.Active != last.Active || current.OpenBodies != last.OpenBodies || current.LateEOF != last.LateEOF || current.LateClose != last.LateClose
		if changed || current.Active != 0 || current.OpenBodies != 0 {
			stable = now
		}
		last = current
		if current.Active == 0 && current.OpenBodies == 0 && now.Sub(stable) >= quietWindow {
			r.ObservedQuiet = true
			r.WaitedMS = float64(time.Since(started)) / float64(time.Millisecond)
			return r
		}
		select {
		case <-ctx.Done():
			r.WaitedMS = float64(time.Since(started)) / float64(time.Millisecond)
			return r
		case <-time.After(poll):
		}
	}
}

type caseResult struct {
	Plan                     casePlan              `json:"plan"`
	Queue                    string                `json:"queue"`
	Status                   string                `json:"status"`
	Passed                   bool                  `json:"passed"`
	Blocked                  bool                  `json:"blocked"`
	OwnerReleased            bool                  `json:"owner_released"`
	Failure                  *failure              `json:"failure,omitempty"`
	OpenMS                   float64               `json:"open_ms"`
	SetupMS                  float64               `json:"setup_ms"`
	SendMS                   float64               `json:"send_ms"`
	DrainMS                  float64               `json:"drain_ms"`
	CallerBudgetSeconds      int                   `json:"caller_budget_seconds"`
	ConfirmedSendPerSecond   float64               `json:"confirmed_send_messages_per_second"`
	ConfirmedSettlePerSecond float64               `json:"confirmed_client_settlement_items_per_second"`
	CloseMS                  float64               `json:"close_ms"`
	Workload                 accounting            `json:"workload"`
	FinalStatsValid          bool                  `json:"final_stats_valid"`
	FinalStats               engine.Metrics        `json:"final_stats"`
	FinalPeekEmpty           bool                  `json:"final_peek_empty"`
	Cleanup                  cleanupResult         `json:"cleanup"`
	PoolBefore               poolSnapshot          `json:"pool_before_workload"`
	PoolAfter                poolSnapshot          `json:"pool_after_workload"`
	PoolWaitDeltaMS          float64               `json:"pool_wait_delta_ms"`
	PoolWaitCountDelta       int64                 `json:"pool_wait_count_delta"`
	Operations               []operationSample     `json:"operations"`
	Latency                  map[string]latency    `json:"latency"`
	EnginePhaseTracing       bool                  `json:"engine_phase_tracing"`
	Mixed                    *mixedResult          `json:"mixed,omitempty"`
	BrokerHTTP               []brokerEvent         `json:"broker_http"`
	BrokerHTTPDropped        int                   `json:"broker_http_dropped"`
	Provider                 providerTraceSnapshot `json:"provider_http"`
	ProviderQuiet            quietResult           `json:"provider_quiet_barrier"`
	BackgroundLogs           map[string]int        `json:"background_log_event_counts"`
	BackgroundErrors         int                   `json:"background_error_records"`
	InstrumentationComplete  bool                  `json:"provider_broker_trace_complete"`
}
type backendResult struct {
	Alias         string                `json:"backend"`
	Passed        bool                  `json:"passed"`
	Blocked       bool                  `json:"blocked"`
	BudgetSeconds int                   `json:"outer_budget_seconds"`
	Cases         []caseResult          `json:"cases"`
	Provider      providerTraceSnapshot `json:"provider_http"`
}
type caseProgress struct {
	mu      sync.Mutex
	samples []operationSample
	next    atomic.Uint64
	book    *identityBook
}

func (p *caseProgress) operations() []operationSample {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]operationSample(nil), p.samples...)
}
func (p *caseProgress) call(parent context.Context, id, phase string, admission chan struct{}, fn func(context.Context) error) error {
	op := fmt.Sprintf("%s-%s-%d", id, phase, p.next.Add(1))
	ctx, cancel := context.WithTimeout(parent, callerBudget)
	defer cancel()
	ctx = context.WithValue(ctx, operationKey{}, op)
	start := time.Now()
	var waited time.Duration
	if admission != nil {
		select {
		case admission <- struct{}{}:
			waited = time.Since(start)
			defer func() { <-admission }()
		case <-ctx.Done():
			waited = time.Since(start)
			p.record(operationSample{op, phase, float64(time.Since(start)) / float64(time.Millisecond), float64(waited) / float64(time.Millisecond), safeFailure(ctx.Err())})
			return ctx.Err()
		}
	}
	err := fn(ctx)
	p.record(operationSample{op, phase, float64(time.Since(start)) / float64(time.Millisecond), float64(waited) / float64(time.Millisecond), safeFailure(err)})
	return err
}
func (p *caseProgress) record(s operationSample) {
	p.mu.Lock()
	p.samples = append(p.samples, s)
	p.mu.Unlock()
}
func runBackend(parent context.Context, b backend, mode, run string, owned []ownedQueue) backendResult {
	return runBackendSelected(parent, b, mode, "all", run, owned)
}

func runBackendSelected(parent context.Context, b backend, mode, selection, run string, owned []ownedQueue) backendResult {
	r := backendResult{Alias: b.Alias, BudgetSeconds: int(backendBudget / time.Second)}
	ps, err := selectedPlans(mode, selection)
	if err != nil {
		r.Cases = []caseResult{{Status: "refused", Failure: safeFailure(err)}}
		return r
	}
	ctx, cancel := context.WithTimeout(parent, backendBudget)
	defer cancel()
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		r.Cases = []caseResult{{Status: "refused", Failure: &failure{Class: "unexpected_default_transport"}}}
		return r
	}
	target := b.dsn
	if target == "" {
		target = "libsql://local-diagnostic.invalid"
	}
	provider, e := newProviderTrace(target, base)
	if e != nil {
		r.Cases = []caseResult{{Status: "refused", Failure: &failure{Class: "provider_trace_configuration"}}}
		return r
	}
	previous := http.DefaultTransport
	http.DefaultTransport = provider
	defer func() { http.DefaultTransport = previous }()
	for i, p := range ps {
		if ctx.Err() != nil {
			r.Cases = append(r.Cases, caseResult{Plan: p, Status: "not_run_after_backend_budget", Blocked: true, Failure: &failure{Class: "backend_budget"}})
			r.Blocked = true
			break
		}
		if b.Alias == "sqlite" && p.PoolLimit > 0 {
			r.Cases = append(r.Cases, caseResult{Plan: p, Status: "not_applicable_local_single_writer", OwnerReleased: true})
			continue
		}
		q := queueName(run, i, p.SDK)
		progress := &caseProgress{book: newBook()}
		caseCtx, cancelCase := context.WithTimeout(ctx, caseBudget)
		done := make(chan caseResult, 1)
		go func() { done <- runCase(caseCtx, b, p, q, provider, progress, base, owned, i == 0) }()
		cr := awaitCase(caseCtx, done, progress, p, q, joinBudget)
		cancelCase()
		r.Cases = append(r.Cases, cr)
		if cr.Blocked {
			r.Blocked = true
			break
		}
	}
	r.Provider = provider.Snapshot()
	r.Passed = !r.Blocked && len(r.Cases) == len(ps)
	for _, c := range r.Cases {
		if !c.Passed && c.Status != "not_applicable_local_single_writer" {
			r.Passed = false
		}
	}
	return r
}

func awaitCase(ctx context.Context, done <-chan caseResult, progress *caseProgress, p casePlan, q string, grace time.Duration) caseResult {
	select {
	case cr := <-done:
		if ctx.Err() != nil {
			cr.Passed = false
			cr.Blocked = true
			cr.Status = "blocked_outer_budget"
			cr.Failure = &failure{Class: "outer_budget"}
		}
		return cr
	case <-ctx.Done():
		select {
		case cr := <-done:
			cr.Passed = false
			cr.Blocked = true
			cr.Status = "blocked_outer_budget"
			cr.Failure = &failure{Class: "outer_budget"}
			return cr
		case <-time.After(grace):
			return caseResult{Plan: p, Queue: q, Status: "blocked_unjoined_owner", Blocked: true, Failure: &failure{Class: "owner_did_not_join"}, Workload: progress.book.snapshot(), Operations: progress.operations()}
		}
	}
}

func runCase(ctx context.Context, b backend, p casePlan, q string, provider *providerTrace, progress *caseProgress, base *http.Transport, owned []ownedQueue, warm bool) (res caseResult) {
	res = caseResult{Plan: p, Queue: q, Status: "failed", CallerBudgetSeconds: int(callerBudget / time.Second)}
	phase := func(s string) { provider.SetPhase(p.ID + "/" + s) }
	phase("open")
	provider.SetDelay(0)
	providerBefore := provider.Snapshot()
	logger := &logCollector{counts: map[string]int{}}
	dir := ""
	dsn := b.dsn
	var err error
	if b.Alias == "sqlite" {
		dir, err = os.MkdirTemp("", "mqlite-diag-")
		if err != nil {
			res.Failure = &failure{Class: "local_directory"}
			return
		}
		dsn = "file:" + filepath.Join(dir, "queue.db")
	}
	started := time.Now()
	eng, err := engine.Open(ctx, engine.Options{DB: dsn, AuthToken: b.token, DisableBackground: !p.Background, Logger: slog.New(logger)})
	res.OpenMS = float64(time.Since(started)) / float64(time.Millisecond)
	if err != nil {
		res.Failure = safeFailure(err)
		if b.Alias != "sqlite" {
			quiet, stopQuiet := context.WithTimeout(context.Background(), joinBudget)
			res.ProviderQuiet = waitProviderQuiet(quiet, provider.Snapshot, 200*time.Millisecond, 25*time.Millisecond)
			stopQuiet()
		}
		res.Provider = providerDelta(providerBefore, provider.Snapshot())
		res.OwnerReleased = res.Provider.Active == 0 && res.Provider.OpenBodies == 0 && (!res.ProviderQuiet.Required || res.ProviderQuiet.ObservedQuiet)
		res.InstrumentationComplete = res.Provider.Dropped == 0
		res.Blocked = !res.OwnerReleased
		if res.Blocked {
			res.Status = "blocked_open_owner"
		}
		res.BackgroundLogs, res.BackgroundErrors = logger.snapshot()
		if res.OwnerReleased && dir != "" {
			_ = os.RemoveAll(dir)
		}
		return
	}
	// Every termination path joins the HTTP owner and closes the engine before a
	// later case may open the same DB. A bounded Close failure blocks all owners.
	var ts *httptest.Server
	var a *adapter
	var clientTransport *http.Transport
	defer func() {
		phase("close")
		provider.SetDelay(0)
		if a != nil {
			a.close()
		}
		if clientTransport != nil {
			clientTransport.CloseIdleConnections()
		}
		start := time.Now()
		closeDone := make(chan error, 1)
		go func() {
			if ts != nil {
				ts.CloseClientConnections()
				ts.Close()
			}
			closeDone <- eng.Close()
		}()
		select {
		case closeErr := <-closeDone:
			res.OwnerReleased = true
			if closeErr != nil {
				res.Blocked = true
				res.OwnerReleased = false
				if res.Failure == nil {
					res.Failure = safeFailure(closeErr)
				}
			}
		case <-time.After(joinBudget):
			res.Blocked = true
			res.OwnerReleased = false
			res.Failure = &failure{Class: "close_did_not_join"}
		}
		res.CloseMS = float64(time.Since(start)) / float64(time.Millisecond)
		if res.OwnerReleased && b.Alias != "sqlite" {
			quiet, stopQuiet := context.WithTimeout(context.Background(), joinBudget)
			res.ProviderQuiet = waitProviderQuiet(quiet, provider.Snapshot, 200*time.Millisecond, 25*time.Millisecond)
			stopQuiet()
			if !res.ProviderQuiet.ObservedQuiet {
				res.Blocked = true
				res.OwnerReleased = false
				if res.Failure == nil {
					res.Failure = &failure{Class: "provider_not_quiet"}
				}
			}
		}
		if res.OwnerReleased && dir != "" {
			_ = os.RemoveAll(dir)
		}
		res.Provider = providerDelta(providerBefore, provider.Snapshot())
		if res.Provider.Active != 0 || res.Provider.OpenBodies != 0 {
			res.Blocked = true
			res.OwnerReleased = false
			if res.Failure == nil {
				res.Failure = &failure{Class: "provider_owner_still_active"}
			}
		}
		res.BackgroundLogs, res.BackgroundErrors = logger.snapshot()
		res.Workload = progress.book.snapshot()
		if res.SendMS > 0 {
			res.ConfirmedSendPerSecond = float64(res.Workload.Confirmed) * 1000 / res.SendMS
		}
		if res.DrainMS > 0 {
			res.ConfirmedSettlePerSecond = float64(res.Workload.Settled) * 1000 / res.DrainMS
		}
		res.Operations = progress.operations()
		res.Latency = latencies(res.Operations)
		res.InstrumentationComplete = res.Provider.Dropped == 0 && res.Provider.Active == 0 && res.Provider.OpenBodies == 0 && res.BrokerHTTPDropped == 0
		if !res.InstrumentationComplete && res.Failure == nil {
			res.Failure = &failure{Class: "instrumentation_incomplete"}
		}
		if res.BackgroundErrors > 0 && res.Failure == nil {
			res.Failure = &failure{Class: "background_error"}
		}
		res.Passed = res.Failure == nil && !res.Blocked && res.OwnerReleased && res.InstrumentationComplete && res.Cleanup.Deleted && res.Cleanup.AbsentReadback
		if res.Passed {
			res.Status = "passed"
		} else if res.Blocked {
			res.Status = "blocked"
		}
	}()
	if p.PoolLimit > 0 {
		if err = eng.DiagnosticSetPoolLimit(p.PoolLimit); err != nil {
			res.Failure = safeFailure(err)
			return
		}
	}
	if warm {
		phase("select_one_baseline")
		for i := 0; i < 7; i++ {
			kind := "select_one_sample"
			if i < 2 {
				kind = "select_one_warmup"
			}
			if err = progress.call(ctx, p.ID, kind, nil, eng.DiagnosticSelectOne); err != nil {
				res.Failure = safeFailure(err)
				return
			}
		}
	}
	bt := &brokerTrace{}
	clientTransport = base.Clone()
	clientTransport.MaxIdleConnsPerHost = 64
	if clientTransport.MaxIdleConns < 64 {
		clientTransport.MaxIdleConns = 64
	}
	bt.next = clientTransport
	srv := server.New(eng, nil)
	handler := srv.Handler()
	ts = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(diagnosticHeader)
		if !safeID(id) {
			http.Error(w, "invalid diagnostic operation", 400)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	ts.Config.ErrorLog = log.New(io.Discard, "", 0)
	ts.Start()
	a, err = openAdapter(ctx, p.SDK, ts.URL, &http.Client{Transport: bt})
	if err != nil {
		res.Failure = safeFailure(err)
		return
	}
	phase("setup")
	started = time.Now()
	err = progress.call(ctx, p.ID, "create_queue", nil, func(call context.Context) error {
		return a.createWithLock(call, q, time.Duration(p.DedupSeconds)*time.Second, time.Duration(p.LockSeconds)*time.Second)
	})
	res.SetupMS = float64(time.Since(started)) / float64(time.Millisecond)
	if err != nil {
		res.Failure = safeFailure(err)
	} else {
		provider.SetDelay(time.Duration(p.DelayMS) * time.Millisecond)
		before := eng.DiagnosticPoolStats()
		res.PoolBefore = poolView(before)
		phase("send")
		started = time.Now()
		if p.Family == "mixed_receiver" {
			res.Mixed, err = runMixed(ctx, a, p, q, progress, bt, provider, b.Alias != "sqlite")
			if err != nil {
				res.Failure = safeFailure(err)
				if res.Mixed != nil && (!res.Mixed.ReceiverJoined || !res.Mixed.WorkersJoined) {
					res.Blocked = true
					return
				}
			}
		} else {
			runSend(ctx, a, p, q, progress)
			res.SendMS = float64(time.Since(started)) / float64(time.Millisecond)
			phase("drain")
			started = time.Now()
			runDrain(ctx, a, p, q, progress)
			res.DrainMS = float64(time.Since(started)) / float64(time.Millisecond)
		}
		after := eng.DiagnosticPoolStats()
		res.PoolAfter = poolView(after)
		res.PoolWaitDeltaMS = float64(after.WaitDuration-before.WaitDuration) / float64(time.Millisecond)
		res.PoolWaitCountDelta = after.WaitCount - before.WaitCount
		provider.SetDelay(0)
		phase("oracle")
		err = progress.call(ctx, p.ID, "final_stats", nil, func(call context.Context) error { var e error; res.FinalStats, e = a.stats(call, q); return e })
		res.FinalStatsValid = err == nil
		if err != nil {
			res.Failure = safeFailure(err)
		} else if !progress.book.snapshot().valid(p.Count, res.FinalStats) && res.Failure == nil {
			res.Failure = &failure{Class: "oracle_integrity_or_operation_failure"}
		}
		if err = progress.call(ctx, p.ID, "final_empty", nil, func(call context.Context) error { return a.empty(call, q) }); err != nil {
			if res.Failure == nil {
				res.Failure = safeFailure(err)
			}
		} else {
			res.FinalPeekEmpty = true
		}
	}
	// No cleanup while a canceled HTTP request could still be inside the driver.
	shutdown, stop := context.WithTimeout(context.Background(), joinBudget)
	err = ts.Config.Shutdown(shutdown)
	stop()
	if err != nil {
		res.Blocked = true
		res.Failure = &failure{Class: "http_owner_did_not_join"}
		res.BrokerHTTP, res.BrokerHTTPDropped = bt.snapshot()
		return
	}
	phase("cleanup")
	res.Cleanup = cleanupOwned(eng, b.Alias, p.ID, q, owned)
	if res.Cleanup.Failure != nil && res.Failure == nil {
		res.Failure = res.Cleanup.Failure
	}
	res.BrokerHTTP, res.BrokerHTTPDropped = bt.snapshot()
	return
}

func cleanupOwned(eng *engine.Engine, alias, id, q string, owned []ownedQueue) cleanupResult {
	r := cleanupResult{}
	for _, o := range owned {
		if o.Backend == alias && o.Case == id && o.Queue == q {
			r.ExactOwnedQueue = true
			break
		}
	}
	if !r.ExactOwnedQueue {
		r.Failure = &failure{Class: "cleanup_scope_refused"}
		return r
	}
	r.Attempted = true
	ctx, cancel := context.WithTimeout(context.Background(), cleanupBudget)
	defer cancel()
	for attempt := 0; ctx.Err() == nil && attempt < 64; attempt++ {
		d, e := eng.DeleteQueueWithOptions(ctx, q, engine.DeleteOptions{Force: true})
		if e != nil {
			r.Failure = safeFailure(e)
			return r
		}
		r.Deleted = d.Deleted
		r.State = d.State
		if d.Deleted && d.State == "deleted" {
			break
		}
		// Repeating the exact scoped deletion advances its bounded cleanup.
		// Never run global maintenance as a diagnostic cleanup shortcut.
	}
	if !r.Deleted || r.State != "deleted" {
		r.Failure = &failure{Class: "cleanup_not_deleted"}
		return r
	}
	_, e := eng.Stats(ctx, q)
	if errors.Is(e, engine.ErrQueueNotFound) {
		r.AbsentReadback = true
	} else if e != nil {
		r.Failure = safeFailure(e)
	} else {
		r.Failure = &failure{Class: "cleanup_readback_present"}
	}
	return r
}
func latencies(samples []operationSample) map[string]latency {
	by := map[string][]time.Duration{}
	for _, s := range samples {
		by[s.Phase] = append(by[s.Phase], time.Duration(s.ElapsedMS*float64(time.Millisecond)))
	}
	out := map[string]latency{}
	for k, v := range by {
		out[k] = distribution(v)
	}
	return out
}
func providerDelta(before, after providerTraceSnapshot) providerTraceSnapshot {
	r := after
	r.Events = nil
	for _, e := range after.Events {
		if e.ID > before.Observed {
			r.Events = append(r.Events, e)
		}
	}
	r.Observed -= before.Observed
	r.Sent -= before.Sent
	r.NotSent -= before.NotSent
	r.Finished -= before.Finished
	r.Dropped -= before.Dropped
	r.LateEOF -= before.LateEOF
	r.LateClose -= before.LateClose
	return r
}
