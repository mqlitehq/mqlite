// Command mqlite-bench stress-tests the local SQLite engine (embedded, no HTTP)
// across several high-frequency request patterns, and attributes per-scenario
// CPU and disk I/O via /proc/self (Linux/Docker). It is an in-process probe:
// cheap (atomic counters + a microsecond histogram) but not zero-cost — see the
// stress report for the measured probe overhead.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mqlitehq/mqlite/engine"
)

// ── latency histogram: exact microsecond buckets up to 100ms + overflow ──────

const maxUs = 100_000

const (
	postWorkloadIdleBeforeGC = 1500 * time.Millisecond
	postWorkloadIdleAfterGC  = 400 * time.Millisecond
	postWorkloadIdle         = postWorkloadIdleBeforeGC + postWorkloadIdleAfterGC
)

type hist struct {
	b               []uint32
	count, sum, max uint64
}

func newHist() *hist { return &hist{b: make([]uint32, maxUs+1)} }

func (h *hist) add(d time.Duration) {
	us := uint64(d.Microseconds())
	h.count++
	h.sum += us
	if us > h.max {
		h.max = us
	}
	if us >= maxUs {
		h.b[maxUs]++
	} else {
		h.b[us]++
	}
}

func (h *hist) merge(o *hist) {
	for i := range h.b {
		h.b[i] += o.b[i]
	}
	h.count += o.count
	h.sum += o.sum
	if o.max > h.max {
		h.max = o.max
	}
}

func (h *hist) pct(p float64) uint64 {
	if h.count == 0 {
		return 0
	}
	target := uint64(float64(h.count) * p)
	if target < 1 {
		target = 1
	}
	var cum uint64
	for i, c := range h.b {
		cum += uint64(c)
		if cum >= target {
			return uint64(i)
		}
	}
	return h.max
}

func (h *hist) mean() float64 {
	if h.count == 0 {
		return 0
	}
	return float64(h.sum) / float64(h.count)
}

// ── /proc/self probe (Linux) ─────────────────────────────────────────────────

type procSnap struct {
	writeBytes, readBytes, wchar, rchar, syscw, syscr uint64
	utime, stime                                      uint64 // clock ticks
}

func readProc() procSnap {
	var s procSnap
	if b, err := os.ReadFile("/proc/self/io"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			f := strings.Fields(line)
			if len(f) != 2 {
				continue
			}
			v, _ := strconv.ParseUint(f[1], 10, 64)
			switch strings.TrimSuffix(f[0], ":") {
			case "rchar":
				s.rchar = v
			case "wchar":
				s.wchar = v
			case "read_bytes":
				s.readBytes = v
			case "write_bytes":
				s.writeBytes = v
			case "syscr":
				s.syscr = v
			case "syscw":
				s.syscw = v
			}
		}
	}
	if b, err := os.ReadFile("/proc/self/stat"); err == nil {
		c := string(b)
		if i := strings.LastIndex(c, ")"); i >= 0 {
			rest := strings.Fields(c[i+1:])
			// after comm: rest[0]=state(field3); utime=field14=rest[11], stime=field15=rest[12]
			if len(rest) > 12 {
				s.utime, _ = strconv.ParseUint(rest[11], 10, 64)
				s.stime, _ = strconv.ParseUint(rest[12], 10, 64)
			}
		}
	}
	return s
}

const clkTck = 100.0 // _SC_CLK_TCK on Linux

// ── scenario + result ────────────────────────────────────────────────────────

type scen struct {
	Name                              string
	Kind                              string // produce|batch|e2e|drain|sessions|props|bloat|rampdown|churn|lifecycle
	P, C, Batch, Groups, Prefill, Msg int
	Sync                              string
}

type Result struct {
	OK              bool    `json:"ok"`
	Error           string  `json:"error,omitempty"`
	Name            string  `json:"name"`
	Kind            string  `json:"kind"`
	Producers       int     `json:"producers"`
	Consumers       int     `json:"consumers"`
	Batch           int     `json:"batch"`
	MsgSize         int     `json:"msg_size"`
	Sync            string  `json:"sync"`
	DurationS       float64 `json:"duration_s"`
	Ops             int64   `json:"ops"`
	OpsPerSec       float64 `json:"ops_per_sec"`
	MeanUs          float64 `json:"mean_us"`
	P50us           uint64  `json:"p50_us"`
	P90us           uint64  `json:"p90_us"`
	P99us           uint64  `json:"p99_us"`
	P999us          uint64  `json:"p999_us"`
	MaxUs           uint64  `json:"max_us"`
	DiskWriteBytes  uint64  `json:"disk_write_bytes"`
	DiskReadBytes   uint64  `json:"disk_read_bytes"`
	WriteBytesPerOp float64 `json:"write_bytes_per_op"`
	SyscW           uint64  `json:"syscall_write"`
	CPUSeconds      float64 `json:"cpu_seconds"`
	CPUPct          float64 `json:"cpu_pct"`
	GCCount         uint32  `json:"gc_count"`
	GCPauseMs       float64 `json:"gc_pause_ms"`
	HeapMB          float64 `json:"heap_mb"`
	DBSizeMB        float64 `json:"db_size_mb"`
	WALSizeMB       float64 `json:"wal_size_mb"`
	// Resource time-series (sampler): peak vs the snapshot after a short,
	// fixed post-workload idle. That snapshot does not imply that the 60-second
	// storage janitor has run.
	RSSPeakMB       float64  `json:"rss_peak_mb"`
	RSSFinalMB      float64  `json:"rss_final_mb"`
	HeapInusePeakMB float64  `json:"heap_inuse_peak_mb"`
	HeapReleasedMB  float64  `json:"heap_released_mb"`
	DBPeakMB        float64  `json:"db_peak_mb"`  // db+wal peak (during load)
	DBFinalMB       float64  `json:"db_final_mb"` // db+wal after the short post-workload idle
	FileBytesPerMsg float64  `json:"file_bytes_per_msg"`
	Series          []sample `json:"series,omitempty"`
	Note            string   `json:"note,omitempty"`

	// Lifecycle churn counters are populated only by lifecycle_churn. Generic
	// Ops/OpsPerSec count fully converged queue lifecycles; the generic latency
	// histogram measures force-delete acknowledgement latency.
	LifecycleCycles            int64   `json:"lifecycle_cycles,omitempty"`
	LifecycleCreates           int64   `json:"lifecycle_creates,omitempty"`
	LifecycleMessages          int64   `json:"lifecycle_messages,omitempty"`
	LifecycleDeleteAttempts    int64   `json:"lifecycle_delete_attempts,omitempty"`
	LifecycleDeleteAccepted    int64   `json:"lifecycle_delete_accepted,omitempty"`
	LifecycleDeletePending     int64   `json:"lifecycle_delete_pending,omitempty"`
	LifecycleMaintenancePasses int64   `json:"lifecycle_maintenance_passes,omitempty"`
	LifecycleConvergeMeanMs    float64 `json:"lifecycle_converge_mean_ms,omitempty"`
	LifecycleConvergeP99Ms     float64 `json:"lifecycle_converge_p99_ms,omitempty"`
	LifecycleConvergeMaxMs     float64 `json:"lifecycle_converge_max_ms,omitempty"`
	LifecyclePeakPayloadBytes  int64   `json:"lifecycle_peak_payload_bytes,omitempty"`
}

var dir = flag.String("dir", "/data", "data directory for bench DBs (local-file mode)")
var dur = flag.Duration("dur", 5*time.Second, "duration per timed scenario")
var msg = flag.Int("msgsize", 256, "message body size in bytes")
var outPath = flag.String("out", "", "JSON results output path (default: -dir/results.json locally, ./results.json remotely)")
var only = flag.String("only", "", "run only scenarios whose name contains this substring")

// dbDSN selects the storage backend. Empty (default) = a fresh local SQLite file per
// scenario under -dir (local-disk benchmark). A shared DSN — e.g. a remote
// libsql://host (Turso) — runs every scenario against that one database instead. A
// process-unique namespace isolates each run and is force-deleted after every scenario;
// the auth token comes from MQLITE_DB_AUTH_TOKEN. File-size metrics are N/A in shared
// mode (the store is remote).
var dbDSN = flag.String("db", "", "shared DB DSN (libsql://… for Turso); empty = a fresh local file per scenario")

// prefillCap bounds each scenario's (unmeasured) prefill so the fixed-volume
// scenarios (drain/bloat) stay feasible on a slow remote backend; the same cap is
// passed to every backend so the matrix stays comparable. 0 = no cap.
var prefillCap = flag.Int("prefillcap", 0, "cap each scenario's prefill at N messages (0 = no cap)")

var benchmarkRunID = fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())

const minRemoteCleanupTimeout = 30 * time.Second

func main() {
	flag.Parse()
	if *dbDSN == "" {
		if err := ensureBenchmarkDir(*dir); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	resultPath := resolveBenchmarkOutputPath(*outPath, *dir, *dbDSN != "")
	if err := validateBenchmarkOutputPath(resultPath); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	scens := []scen{
		{Name: "produce_p1", Kind: "produce", P: 1, Msg: *msg},
		{Name: "produce_p4", Kind: "produce", P: 4, Msg: *msg},
		{Name: "produce_p8", Kind: "produce", P: 8, Msg: *msg},
		{Name: "batch_16_p4", Kind: "batch", P: 4, Batch: 16, Msg: *msg},
		{Name: "batch_64_p4", Kind: "batch", P: 4, Batch: 64, Msg: *msg},
		{Name: "e2e_4x4", Kind: "e2e", P: 4, C: 4, Msg: *msg},
		{Name: "sessions_64g", Kind: "sessions", P: 4, C: 4, Groups: 64, Msg: *msg},
		{Name: "produce_p4_FULL", Kind: "produce", P: 4, Msg: *msg, Sync: "FULL"},
		// ── enriched messages: realistic KV properties + ASB metadata ──
		{Name: "props_p4", Kind: "props", P: 4, Msg: *msg},
		{Name: "props_batch64", Kind: "props", P: 4, Batch: 64, Msg: *msg},
		// ── body-size sweep: write amplification vs payload size ──
		{Name: "size_64B", Kind: "produce", P: 4, Msg: 64},
		{Name: "size_1KB", Kind: "produce", P: 4, Msg: 1024},
		{Name: "size_4KB", Kind: "produce", P: 4, Msg: 4096},
		{Name: "size_16KB", Kind: "produce", P: 4, Msg: 16384},
		// ── DB file footprint: fill exactly once, drain, then take a short-idle snapshot ──
		{Name: "bloat_50k", Kind: "bloat", Prefill: 50_000, Batch: 64, Msg: *msg},
		// ── Load ramp: load up then down; memory/file state after a short idle ──
		{Name: "rampdown_4x4", Kind: "rampdown", P: 4, C: 4, Msg: *msg},
		{Name: "churn_consumers", Kind: "churn", P: 4, C: 4, Msg: *msg},
		// Queue lifecycle churn is opt-in through -only. It deliberately creates
		// pending deletions and is too write-heavy for the default remote matrix.
		{Name: "lifecycle_churn", Kind: "lifecycle", Msg: lifecycleMessageSize(*msg)},
		// heaviest (200k prefill) LAST so a constrained box still gets all the rest
		{Name: "drain_4c", Kind: "drain", C: 4, Prefill: 200_000, Batch: 64, Msg: *msg},
	}

	fmt.Printf("mqlite-bench · GOMAXPROCS=%d · dur=%s · msgsize=%dB · linux=%v\n",
		runtime.GOMAXPROCS(0), *dur, *msg, runtime.GOOS == "linux")

	var results []Result
	failed := false
	for _, s := range scens {
		if s.Kind == "lifecycle" && *only == "" {
			continue
		}
		if *only != "" && !strings.Contains(s.Name, *only) {
			continue
		}
		r, err := run(s)
		results = append(results, r)
		status := "done"
		if err != nil {
			failed = true
			status = "fail"
		}
		// incremental line so a later OOM (e.g. on a 256MB cloud box) does not lose
		// the scenarios that already finished
		fmt.Printf("[%s] %-16s ops/s=%-7s p99us=%-7d wB/op=%-6.0f rssPeak=%4.0fM rssIdle=%4.0fM dbPeak=%5.1fM dbIdle=%5.1fM\n",
			status, r.Name, hnum(r.OpsPerSec), r.P99us, r.WriteBytesPerOp,
			r.RSSPeakMB, r.RSSFinalMB, r.DBPeakMB, r.DBFinalMB)
		if err != nil {
			fmt.Fprintf(os.Stderr, "benchmark %s failed: %v\n", r.Name, err)
		}
	}

	printTable(results)
	printResourceTable(results)
	printLifecycleTable(results)
	if err := writeBenchmarkJSON(resultPath, map[string]any{
		"meta": map[string]any{
			"gomaxprocs": runtime.GOMAXPROCS(0), "duration_s": dur.Seconds(),
			"msg_size": *msg, "goos": runtime.GOOS, "goarch": runtime.GOARCH,
			"num_cpu": runtime.NumCPU(), "post_workload_idle_s": postWorkloadIdle.Seconds(),
		},
		"results": results,
	}); err != nil {
		failed = true
		fmt.Fprintln(os.Stderr, err)
	} else {
		fmt.Printf("\nresults written to %s\n", resultPath)
	}
	if failed {
		os.Exit(1)
	}
}

func run(s scen) (res Result, runErr error) {
	defer func() {
		res.OK = runErr == nil
		if runErr != nil {
			res.Error = runErr.Error()
		}
	}()
	res = Result{Name: s.Name, Kind: s.Kind, Producers: s.P, Consumers: s.C,
		Batch: s.Batch, MsgSize: s.Msg, Sync: orDefault(s.Sync, "NORMAL")}
	remote := *dbDSN != ""
	var dbPath, dbArg, q string
	if remote {
		// Shared remote store: every process gets its own namespace, and every run
		// removes that namespace before returning. No local file is measured.
		dbArg, q = *dbDSN, benchmarkQueueName(s.Name)
	} else {
		dbPath = filepath.Join(*dir, s.Name+".db")
		if err := resetBenchmarkDBFiles(dbPath); err != nil {
			return res, err
		}
		dbArg, q = "file:"+dbPath, "bench"
	}
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{
		DB:                dbArg,
		AuthToken:         os.Getenv("MQLITE_DB_AUTH_TOKEN"),
		DisableBackground: s.Kind == "lifecycle",
		Synchronous:       s.Sync,
	})
	if err != nil {
		return res, err
	}
	defer func() {
		if remote {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), remoteCleanupTimeout(*dur))
			cleanupErr := cleanupBenchmarkQueues(cleanupCtx, eng, q)
			cancel()
			if cleanupErr != nil {
				runErr = errors.Join(runErr, cleanupErr)
			}
		}
		if closeErr := eng.Close(); closeErr != nil {
			runErr = errors.Join(runErr, fmt.Errorf("close benchmark engine: %w", closeErr))
		}
	}()
	if s.Kind != "lifecycle" {
		if err := eng.CreateQueue(ctx, q, engine.QueueConfig{LockDurationMs: 60_000, MaxDeliveryCount: 1000}); err != nil {
			return res, err
		}
	}

	// prefill (not measured) for drain/bloat, capped for slow backends.
	prefillN := s.Prefill
	if *prefillCap > 0 && prefillN > *prefillCap {
		prefillN = *prefillCap
	}
	prefilled := 0
	if prefillN > 0 {
		prefilled, err = prefill(ctx, eng, q, prefillN, s.Batch, s.Msg)
		if err != nil {
			return res, err
		}
	}

	runtime.GC()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	p0 := readProc()
	samp := newSampler(dbPath, 200*time.Millisecond)
	samp.start()
	if prefilled > 0 {
		// Make the full-backlog footprint deterministic even when a small prefill
		// drains before the first periodic sample.
		samp.snap()
	}
	t0 := time.Now()

	var ops int64
	var h *hist
	var workloadErr error
	var accepted, completed int64
	var checkAccounting, requireEmpty bool
	switch s.Kind {
	case "produce":
		ops, h, workloadErr = produce(ctx, eng, q, *dur, s.P, s.Msg, "")
		accepted, checkAccounting = ops, true
	case "batch":
		ops, h, workloadErr = batchProduce(ctx, eng, q, *dur, s.P, s.Batch, s.Msg)
		accepted, checkAccounting = ops, true
	case "sessions":
		ops, h, accepted, workloadErr = sessionsRun(ctx, eng, q, *dur, s.P, s.C, s.Groups, s.Msg)
		completed, checkAccounting = ops, true
	case "e2e":
		ops, h, accepted, workloadErr = e2eRun(ctx, eng, q, *dur, s.P, s.C, s.Msg)
		completed, checkAccounting = ops, true
	case "drain":
		ops, h, workloadErr = drain(ctx, eng, q, s.C)
		accepted, completed, checkAccounting, requireEmpty = int64(prefilled), ops, true, true
	case "props":
		ops, h, workloadErr = propsRun(ctx, eng, q, *dur, s.P, s.Batch, s.Msg)
		accepted, checkAccounting = ops, true
	case "bloat":
		ops, h, workloadErr = bloatRun(ctx, eng, q, prefilled)
		accepted, completed, checkAccounting, requireEmpty = int64(prefilled), ops, true, true
	case "rampdown":
		ops, h, accepted, workloadErr = rampDownRun(ctx, eng, q, *dur, s.P, s.C, s.Msg)
		completed, checkAccounting = ops, true
	case "churn":
		ops, h, accepted, workloadErr = churnRun(ctx, eng, q, *dur, s.P, s.C, s.Msg)
		completed, checkAccounting = ops, true
	case "lifecycle":
		var lifecycle lifecycleRunResult
		lifecycle, workloadErr = lifecycleChurnRun(ctx, eng, q, *dur, s.Msg, samp)
		ops, h = lifecycle.converged, lifecycle.deleteLatency
		res.LifecycleCycles = lifecycle.cycles
		res.LifecycleCreates = lifecycle.created
		res.LifecycleMessages = lifecycle.messages
		res.LifecycleDeleteAttempts = lifecycle.deleteAttempts
		res.LifecycleDeleteAccepted = lifecycle.deleteAccepted
		res.LifecycleDeletePending = lifecycle.deletePending
		res.LifecycleMaintenancePasses = lifecycle.maintenancePasses
		res.LifecycleConvergeMeanMs = lifecycle.convergeLatency.meanMs()
		res.LifecycleConvergeP99Ms = lifecycle.convergeLatency.pctMs(.99)
		res.LifecycleConvergeMaxMs = lifecycle.convergeLatency.maxMs()
		res.LifecyclePeakPayloadBytes = lifecycle.peakPayloadBytes
	default:
		workloadErr = fmt.Errorf("unknown benchmark scenario kind %q", s.Kind)
	}

	elapsed := time.Since(t0)
	p1 := readProc()
	runtime.ReadMemStats(&m1)

	// ── Short post-workload snapshot: pause briefly, force GC + return memory
	// to the OS, then re-measure. This 1.9-second window is intentionally shorter
	// than the 60-second storage-janitor interval, so DB-final is an immediate
	// post-idle state rather than evidence of background page reclamation. ──
	time.Sleep(postWorkloadIdleBeforeGC)
	runtime.GC()
	debug.FreeOSMemory()
	time.Sleep(postWorkloadIdleAfterGC)
	samp.stop()
	fin := samp.snap()
	rssPeak, heapPeak, dbPeak := samp.peaks()
	if workloadErr == nil && checkAccounting {
		_, workloadErr = verifyQueueAccounting(ctx, eng, q, accepted, completed, requireEmpty)
	}

	res.DurationS = elapsed.Seconds()
	res.Ops = ops
	res.OpsPerSec = float64(ops) / elapsed.Seconds()
	if h != nil {
		res.MeanUs = h.mean()
		res.P50us, res.P90us, res.P99us, res.P999us = h.pct(.50), h.pct(.90), h.pct(.99), h.pct(.999)
		res.MaxUs = h.max
	}
	res.DiskWriteBytes = p1.writeBytes - p0.writeBytes
	res.DiskReadBytes = p1.readBytes - p0.readBytes
	res.SyscW = p1.syscw - p0.syscw
	if ops > 0 {
		res.WriteBytesPerOp = float64(res.DiskWriteBytes) / float64(ops)
	}
	res.CPUSeconds = float64((p1.utime+p1.stime)-(p0.utime+p0.stime)) / clkTck
	res.CPUPct = res.CPUSeconds / elapsed.Seconds() * 100
	res.GCCount = m1.NumGC - m0.NumGC
	res.GCPauseMs = float64(m1.PauseTotalNs-m0.PauseTotalNs) / 1e6
	res.HeapMB = float64(m1.HeapAlloc) / (1 << 20)
	res.DBSizeMB = fileMB(dbPath)
	res.WALSizeMB = fileMB(dbPath + "-wal")
	res.RSSPeakMB = float64(rssPeak) / (1 << 20)
	res.RSSFinalMB = float64(fin.RSS) / (1 << 20)
	res.HeapInusePeakMB = float64(heapPeak) / (1 << 20)
	res.HeapReleasedMB = float64(fin.HeapReleased) / (1 << 20)
	res.DBPeakMB = float64(dbPeak) / (1 << 20)
	res.DBFinalMB = float64(fin.DB+fin.WAL) / (1 << 20)
	res.Series = samp.downsample(24)
	if s.Kind == "lifecycle" {
		res.Note = "ops count queues whose force deletion converged; latency columns measure delete acknowledgement"
	}
	switch s.Kind { // file bytes per resident message — only where messages stay resident
	case "produce", "batch", "props":
		if ops > 0 {
			res.FileBytesPerMsg = float64(dbPeak) / float64(ops)
		}
	case "bloat":
		if prefilled > 0 {
			res.FileBytesPerMsg = float64(dbPeak) / float64(prefilled)
		}
	}
	return res, workloadErr
}

// ── workloads ────────────────────────────────────────────────────────────────

func body(n int) []byte {
	b := make([]byte, n)
	for i := range b { // opaque payload; content is irrelevant to the benchmark
		b[i] = byte(i)
	}
	return b
}

type workloadControl struct {
	ctx    context.Context
	cancel context.CancelFunc
	once   sync.Once
	mu     sync.Mutex
	err    error
}

func newWorkloadControl(parent context.Context) *workloadControl {
	ctx, cancel := context.WithCancel(parent)
	return &workloadControl{ctx: ctx, cancel: cancel}
}

func (w *workloadControl) fail(err error) {
	if err == nil {
		return
	}
	w.once.Do(func() {
		w.mu.Lock()
		w.err = err
		w.mu.Unlock()
		w.cancel()
	})
}

func (w *workloadControl) stopped() bool {
	return w.ctx.Err() != nil
}

func (w *workloadControl) result() error {
	w.mu.Lock()
	err := w.err
	w.mu.Unlock()
	if err != nil {
		return err
	}
	return w.ctx.Err()
}

func verifyQueueAccounting(ctx context.Context, eng *engine.Engine, q string, accepted, completed int64, requireEmpty bool) (int64, error) {
	stats, err := eng.Stats(ctx, q)
	if err != nil {
		return 0, fmt.Errorf("read final queue accounting for %q: %w", q, err)
	}
	remaining := stats.Total
	if stats.Locked != 0 {
		return remaining, fmt.Errorf("queue accounting for %q left %d locked messages (accepted=%d completed=%d remaining=%d)", q, stats.Locked, accepted, completed, remaining)
	}
	if accepted != completed+remaining {
		return remaining, fmt.Errorf("queue accounting for %q does not reconcile: accepted=%d completed=%d remaining=%d", q, accepted, completed, remaining)
	}
	if requireEmpty && remaining != 0 {
		return remaining, fmt.Errorf("queue %q retained %d messages after draining %d of %d", q, remaining, completed, accepted)
	}
	return remaining, nil
}

func waitUntil(ctx context.Context, deadline time.Time) error {
	d := time.Until(deadline)
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func produce(ctx context.Context, eng *engine.Engine, q string, d time.Duration, P, msg int, _ string) (int64, *hist, error) {
	b := body(msg)
	deadline := time.Now().Add(d)
	work := newWorkloadControl(ctx)
	defer work.cancel()
	hs := make([]*hist, P)
	var total int64
	var wg sync.WaitGroup
	for p := 0; p < P; p++ {
		hs[p] = newHist()
		wg.Add(1)
		go func(h *hist) {
			defer wg.Done()
			var local int64
			defer func() { atomic.AddInt64(&total, local) }()
			for time.Now().Before(deadline) && !work.stopped() {
				t := time.Now()
				seq, err := eng.SendOne(work.ctx, q, engine.OutMessage{Body: b})
				if err != nil {
					work.fail(fmt.Errorf("send one to %q after %d accepted messages: %w", q, local, err))
					return
				}
				if seq <= 0 {
					work.fail(fmt.Errorf("send one to %q returned invalid sequence number %d", q, seq))
					return
				}
				h.add(time.Since(t))
				local++
			}
		}(hs[p])
	}
	wg.Wait()
	return total, mergeAll(hs), work.result()
}

func batchProduce(ctx context.Context, eng *engine.Engine, q string, d time.Duration, P, batch, msg int) (int64, *hist, error) {
	b := body(msg)
	ms := make([]engine.OutMessage, batch)
	for i := range ms {
		ms[i] = engine.OutMessage{Body: b}
	}
	deadline := time.Now().Add(d)
	work := newWorkloadControl(ctx)
	defer work.cancel()
	hs := make([]*hist, P)
	var total int64
	var wg sync.WaitGroup
	for p := 0; p < P; p++ {
		hs[p] = newHist()
		wg.Add(1)
		go func(h *hist) {
			defer wg.Done()
			var local int64
			defer func() { atomic.AddInt64(&total, local) }()
			for time.Now().Before(deadline) && !work.stopped() {
				t := time.Now()
				seqs, err := eng.Send(work.ctx, q, ms...)
				if err != nil {
					work.fail(fmt.Errorf("send batch to %q after %d accepted messages: %w", q, local, err))
					return
				}
				if err := validateBatchSequences(seqs, batch); err != nil {
					work.fail(fmt.Errorf("send batch to %q after %d accepted messages: %w", q, local, err))
					return
				}
				h.add(time.Since(t)) // per-batch latency
				local += int64(batch)
			}
		}(hs[p])
	}
	wg.Wait()
	return total, mergeAll(hs), work.result()
}

func sessionsRun(ctx context.Context, eng *engine.Engine, q string, d time.Duration, P, C, groups, msg int) (int64, *hist, int64, error) {
	b := body(msg)
	deadline := time.Now().Add(d)
	work := newWorkloadControl(ctx)
	defer work.cancel()
	stop := make(chan struct{})
	var accepted int64
	var wg sync.WaitGroup
	for p := 0; p < P; p++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(seed) + 1))
			for time.Now().Before(deadline) && !work.stopped() {
				gid := "g" + strconv.Itoa(rng.Intn(groups))
				seq, err := eng.SendOne(work.ctx, q, engine.OutMessage{Body: b, GroupID: gid})
				if err != nil {
					work.fail(fmt.Errorf("send session message to %q: %w", q, err))
					return
				}
				if seq <= 0 {
					work.fail(fmt.Errorf("send session message to %q returned invalid sequence number %d", q, seq))
					return
				}
				atomic.AddInt64(&accepted, 1)
			}
		}(p)
	}
	cons := consumeUntil(work, eng, q, C, stop)
	wg.Wait()
	close(stop)
	completed, h, err := cons()
	return completed, h, atomic.LoadInt64(&accepted), err
}

func e2eRun(ctx context.Context, eng *engine.Engine, q string, d time.Duration, P, C, msg int) (int64, *hist, int64, error) {
	b := body(msg)
	deadline := time.Now().Add(d)
	work := newWorkloadControl(ctx)
	defer work.cancel()
	stop := make(chan struct{})
	var accepted int64
	var wg sync.WaitGroup
	for p := 0; p < P; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) && !work.stopped() {
				seq, err := eng.SendOne(work.ctx, q, engine.OutMessage{Body: b})
				if err != nil {
					work.fail(fmt.Errorf("send e2e message to %q: %w", q, err))
					return
				}
				if seq <= 0 {
					work.fail(fmt.Errorf("send e2e message to %q returned invalid sequence number %d", q, seq))
					return
				}
				atomic.AddInt64(&accepted, 1)
			}
		}()
	}
	cons := consumeUntil(work, eng, q, C, stop)
	wg.Wait()
	close(stop)
	completed, h, err := cons()
	return completed, h, atomic.LoadInt64(&accepted), err
}

// consumeUntil starts C consumers that Receive(16)+Ack until stop closes.
// Returns a func that (after stop) blocks until all consumers finish, then
// returns the consumed count and the merged per-message Ack-latency histogram.
func consumeUntil(work *workloadControl, eng *engine.Engine, q string, C int, stop <-chan struct{}) func() (int64, *hist, error) {
	hs := make([]*hist, C)
	var total int64
	var wg sync.WaitGroup
	for c := 0; c < C; c++ {
		hs[c] = newHist()
		wg.Add(1)
		go func(h *hist) {
			defer wg.Done()
			var local int64
			defer func() { atomic.AddInt64(&total, local) }()
			for {
				select {
				case <-stop:
					return
				case <-work.ctx.Done():
					return
				default:
				}
				msgs, err := eng.Receive(work.ctx, q, engine.ReceiveOptions{MaxMessages: 16, WaitMs: 20})
				if err != nil {
					work.fail(fmt.Errorf("receive from %q after %d completed messages: %w", q, local, err))
					return
				}
				for _, m := range msgs {
					t := time.Now()
					if err := eng.Complete(work.ctx, q, m.SeqNumber, m.LockToken); err != nil {
						work.fail(fmt.Errorf("complete message %d from %q after %d completed messages: %w", m.SeqNumber, q, local, err))
						return
					}
					h.add(time.Since(t))
					local++
				}
			}
		}(hs[c])
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	return func() (int64, *hist, error) {
		<-done
		return atomic.LoadInt64(&total), mergeAll(hs), work.result()
	}
}

func drain(ctx context.Context, eng *engine.Engine, q string, C int) (int64, *hist, error) {
	work := newWorkloadControl(ctx)
	defer work.cancel()
	hs := make([]*hist, C)
	var total int64
	var wg sync.WaitGroup
	for c := 0; c < C; c++ {
		hs[c] = newHist()
		wg.Add(1)
		go func(h *hist) {
			defer wg.Done()
			var local int64
			defer func() { atomic.AddInt64(&total, local) }()
			empty := 0
			for !work.stopped() {
				msgs, err := eng.Receive(work.ctx, q, engine.ReceiveOptions{MaxMessages: 64, WaitMs: 0})
				if err != nil {
					work.fail(fmt.Errorf("receive while draining %q after %d completed messages: %w", q, local, err))
					return
				}
				if len(msgs) == 0 {
					empty++
					if empty > 5 {
						break
					}
					continue
				}
				empty = 0
				for _, m := range msgs {
					t := time.Now()
					if err := eng.Complete(work.ctx, q, m.SeqNumber, m.LockToken); err != nil {
						work.fail(fmt.Errorf("complete message %d while draining %q after %d completed messages: %w", m.SeqNumber, q, local, err))
						return
					}
					h.add(time.Since(t))
					local++
				}
			}
		}(hs[c])
	}
	wg.Wait()
	return total, mergeAll(hs), work.result()
}

func prefill(ctx context.Context, eng *engine.Engine, q string, n, batch, msg int) (int, error) {
	if batch <= 0 {
		batch = 64
	}
	b := body(msg)
	ms := make([]engine.OutMessage, batch)
	for i := range ms {
		ms[i] = engine.OutMessage{Body: b}
	}
	prefilled := 0
	for prefilled < n {
		remaining := n - prefilled
		if remaining > batch {
			remaining = batch
		}
		seqs, err := eng.Send(ctx, q, ms[:remaining]...)
		if err != nil {
			return prefilled, fmt.Errorf("send batch after %d of %d messages: %w", prefilled, n, err)
		}
		if err := validateBatchSequences(seqs, remaining); err != nil {
			return prefilled, fmt.Errorf("send batch after %d of %d messages: %w", prefilled, n, err)
		}
		prefilled += len(seqs)
	}
	return prefilled, nil
}

// ── helpers ──────────────────────────────────────────────────────────────────

func resolveBenchmarkOutputPath(configured, dataDir string, remote bool) string {
	if configured != "" {
		return configured
	}
	if remote {
		return "results.json"
	}
	return filepath.Join(dataDir, "results.json")
}

func validateBenchmarkOutputPath(path string) error {
	parent := filepath.Dir(path)
	info, err := os.Stat(parent)
	if err != nil {
		return fmt.Errorf("inspect benchmark result directory %q: %w", parent, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("benchmark result directory %q is not a directory", parent)
	}
	return nil
}

func ensureBenchmarkDir(path string) error {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return fmt.Errorf("create benchmark data directory %q: %w", path, err)
	}
	return nil
}

func resetBenchmarkDBFiles(dbPath string) error {
	for _, ext := range []string{"", "-wal", "-shm"} {
		path := dbPath + ext
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove stale benchmark database file %q: %w", path, err)
		}
	}
	return nil
}

func writeBenchmarkJSON(path string, value any) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode benchmark results: %w", err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return fmt.Errorf("write benchmark results to %q: %w", path, err)
	}
	return nil
}

func benchmarkQueueName(scenario string) string {
	return fmt.Sprintf("mqlite-bench-%s-%s", benchmarkRunID, scenario)
}

func remoteCleanupTimeout(workloadDuration time.Duration) time.Duration {
	if workloadDuration > minRemoteCleanupTimeout {
		return workloadDuration
	}
	return minRemoteCleanupTimeout
}

func cleanupBenchmarkQueues(ctx context.Context, eng *engine.Engine, prefix string) error {
	var cleanupErr error
	for pass := 0; ; pass++ {
		queues, err := eng.ListQueues(ctx)
		if err != nil {
			return errors.Join(cleanupErr, fmt.Errorf("list benchmark queues for cleanup: %w", err))
		}
		remaining := 0
		for _, q := range queues {
			if q.Name != prefix && !strings.HasPrefix(q.Name, prefix+"-") {
				continue
			}
			remaining++
			if q.Kind != "queue" {
				return errors.Join(cleanupErr, fmt.Errorf("refuse to clean benchmark namespace %q containing %s %q", prefix, q.Kind, q.Name))
			}
			if _, err := eng.DeleteQueueWithOptions(ctx, q.Name, engine.DeleteOptions{Force: true}); err != nil && !errors.Is(err, engine.ErrQueueNotFound) {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("force delete benchmark queue %q: %w", q.Name, err))
			}
		}
		if remaining == 0 {
			return cleanupErr
		}
		if err := ctx.Err(); err != nil {
			return errors.Join(cleanupErr, fmt.Errorf("clean %d benchmark queues under %q after %d delete passes: %w", remaining, prefix, pass+1, err))
		}
	}
}

func validateBatchSequences(seqs []int64, want int) error {
	if len(seqs) != want {
		return fmt.Errorf("returned %d sequence numbers for %d messages", len(seqs), want)
	}
	for i, seq := range seqs {
		if seq <= 0 {
			return fmt.Errorf("returned invalid sequence number %d at index %d", seq, i)
		}
	}
	return nil
}

func mergeAll(hs []*hist) *hist {
	m := newHist()
	for _, h := range hs {
		if h != nil {
			m.merge(h)
		}
	}
	return m
}

func fileMB(p string) float64 {
	if fi, err := os.Stat(p); err == nil {
		return float64(fi.Size()) / (1 << 20)
	}
	return 0
}

func orDefault(s, d string) string {
	if s == "" {
		return d
	}
	return s
}

func printTable(rs []Result) {
	fmt.Printf("\n%-16s %5s %7s %9s %7s %7s %7s %9s %7s %6s\n",
		"scenario", "sync", "ops/s", "p50us", "p99us", "maxms", "wB/op", "diskMB", "cpu%", "gc")
	fmt.Println(strings.Repeat("-", 96))
	for _, r := range rs {
		fmt.Printf("%-16s %5s %7s %9d %7d %7.1f %9.0f %7.2f %6.0f %4d\n",
			r.Name, r.Sync, hnum(r.OpsPerSec), r.P50us, r.P99us,
			float64(r.MaxUs)/1000, r.WriteBytesPerOp,
			float64(r.DiskWriteBytes)/(1<<20), r.CPUPct, r.GCCount)
	}
}

// printResourceTable shows the memory + disk story the throughput table omits:
// peak vs the snapshot after a short 1.9-second idle. The storage janitor runs
// every 60 seconds, so these columns do not claim background file reclamation.
func printResourceTable(rs []Result) {
	fmt.Printf("\n%-16s %8s %8s %9s %8s %8s %8s %10s\n",
		"scenario", "rssPeak", "rssIdle", "heapPeak", "heapRel", "dbPeak", "dbIdle", "fileB/msg")
	fmt.Println(strings.Repeat("-", 92))
	for _, r := range rs {
		fmt.Printf("%-16s %7.1fM %7.1fM %8.1fM %7.1fM %7.1fM %7.1fM %10.0f\n",
			r.Name, r.RSSPeakMB, r.RSSFinalMB, r.HeapInusePeakMB, r.HeapReleasedMB,
			r.DBPeakMB, r.DBFinalMB, r.FileBytesPerMsg)
	}
}

// printLifecycleTable exposes the lifecycle-specific acceptance and convergence
// signals without widening the main throughput table for every other scenario.
func printLifecycleTable(rs []Result) {
	var lifecycle []Result
	for _, r := range rs {
		if r.Kind == "lifecycle" {
			lifecycle = append(lifecycle, r)
		}
	}
	if len(lifecycle) == 0 {
		return
	}
	fmt.Printf("\n%-16s %6s %8s %9s %8s %8s %9s %10s %10s\n",
		"scenario", "cycles", "creates", "accept%", "pending", "passes", "convP99", "convMax", "payload")
	fmt.Println(strings.Repeat("-", 105))
	for _, r := range lifecycle {
		acceptPct := 0.0
		if r.LifecycleDeleteAttempts > 0 {
			acceptPct = 100 * float64(r.LifecycleDeleteAccepted) / float64(r.LifecycleDeleteAttempts)
		}
		fmt.Printf("%-16s %6d %8d %8.2f%% %8d %8d %8.1fms %8.1fms %8.2fM\n",
			r.Name, r.LifecycleCycles, r.LifecycleCreates, acceptPct,
			r.LifecycleDeletePending, r.LifecycleMaintenancePasses,
			r.LifecycleConvergeP99Ms, r.LifecycleConvergeMaxMs,
			float64(r.LifecyclePeakPayloadBytes)/(1<<20))
	}
}

func hnum(f float64) string {
	switch {
	case f >= 1e6:
		return fmt.Sprintf("%.2fM", f/1e6)
	case f >= 1e3:
		return fmt.Sprintf("%.1fk", f/1e3)
	default:
		return fmt.Sprintf("%.0f", f)
	}
}
