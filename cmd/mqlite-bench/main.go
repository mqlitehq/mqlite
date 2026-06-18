// Command mqlite-bench stress-tests the local SQLite engine (embedded, no HTTP)
// across several high-frequency request patterns, and attributes per-scenario
// CPU and disk I/O via /proc/self (Linux/Docker). It is an in-process probe:
// cheap (atomic counters + a microsecond histogram) but not zero-cost — see the
// stress report for the measured probe overhead.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mqlitehq/mqlite/engine"
)

// ── latency histogram: exact microsecond buckets up to 100ms + overflow ──────

const maxUs = 100_000

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
	Kind                              string // produce|batch|e2e|drain|sessions
	P, C, Batch, Groups, Prefill, Msg int
	Sync                              string
}

type Result struct {
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
	Note            string  `json:"note,omitempty"`
}

var dir = flag.String("dir", "/data", "data directory for bench DBs")
var dur = flag.Duration("dur", 5*time.Second, "duration per timed scenario")
var msg = flag.Int("msgsize", 256, "message body size in bytes")
var outPath = flag.String("out", "/data/results.json", "JSON results output path")
var only = flag.String("only", "", "run only scenarios whose name contains this substring")

func main() {
	flag.Parse()
	_ = os.MkdirAll(*dir, 0o755)

	scens := []scen{
		{Name: "produce_p1", Kind: "produce", P: 1, Msg: *msg},
		{Name: "produce_p4", Kind: "produce", P: 4, Msg: *msg},
		{Name: "produce_p8", Kind: "produce", P: 8, Msg: *msg},
		{Name: "batch_16_p4", Kind: "batch", P: 4, Batch: 16, Msg: *msg},
		{Name: "batch_64_p4", Kind: "batch", P: 4, Batch: 64, Msg: *msg},
		{Name: "e2e_4x4", Kind: "e2e", P: 4, C: 4, Msg: *msg},
		{Name: "drain_4c", Kind: "drain", C: 4, Prefill: 200_000, Batch: 64, Msg: *msg},
		{Name: "sessions_64g", Kind: "sessions", P: 4, C: 4, Groups: 64, Msg: *msg},
		{Name: "produce_p4_FULL", Kind: "produce", P: 4, Msg: *msg, Sync: "FULL"},
	}

	fmt.Printf("mqlite-bench · GOMAXPROCS=%d · dur=%s · msgsize=%dB · linux=%v\n",
		runtime.GOMAXPROCS(0), *dur, *msg, runtime.GOOS == "linux")

	var results []Result
	for _, s := range scens {
		if *only != "" && !strings.Contains(s.Name, *only) {
			continue
		}
		results = append(results, run(s))
	}

	printTable(results)
	if b, err := json.MarshalIndent(map[string]any{
		"meta": map[string]any{
			"gomaxprocs": runtime.GOMAXPROCS(0), "duration_s": dur.Seconds(),
			"msg_size": *msg, "goos": runtime.GOOS, "goarch": runtime.GOARCH,
			"num_cpu": runtime.NumCPU(),
		},
		"results": results,
	}, "", "  "); err == nil {
		_ = os.WriteFile(*outPath, b, 0o644)
		fmt.Printf("\nresults written to %s\n", *outPath)
	}
}

func run(s scen) Result {
	dbPath := filepath.Join(*dir, s.Name+".db")
	for _, ext := range []string{"", "-wal", "-shm"} {
		os.Remove(dbPath + ext)
	}
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: "file:" + dbPath, Synchronous: s.Sync})
	if err != nil {
		return Result{Name: s.Name, Note: "open error: " + err.Error()}
	}
	defer eng.Close()
	const q = "bench"
	_ = eng.CreateQueue(ctx, q, engine.QueueConfig{LockDurationMs: 60_000, MaxDeliveryCount: 1000})

	res := Result{Name: s.Name, Kind: s.Kind, Producers: s.P, Consumers: s.C,
		Batch: s.Batch, MsgSize: s.Msg, Sync: orDefault(s.Sync, "NORMAL")}

	// prefill (not measured) for drain
	if s.Prefill > 0 {
		prefill(ctx, eng, q, s.Prefill, s.Batch, s.Msg)
	}

	runtime.GC()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	p0 := readProc()
	t0 := time.Now()

	var ops int64
	var h *hist
	switch s.Kind {
	case "produce":
		ops, h = produce(ctx, eng, q, *dur, s.P, s.Msg, "")
	case "batch":
		ops, h = batchProduce(ctx, eng, q, *dur, s.P, s.Batch, s.Msg)
	case "sessions":
		ops, h = sessionsRun(ctx, eng, q, *dur, s.P, s.C, s.Groups, s.Msg)
	case "e2e":
		ops, h = e2eRun(ctx, eng, q, *dur, s.P, s.C, s.Msg)
	case "drain":
		ops, h = drain(ctx, eng, q, s.C)
	}

	elapsed := time.Since(t0)
	p1 := readProc()
	runtime.ReadMemStats(&m1)

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
	return res
}

// ── workloads ────────────────────────────────────────────────────────────────

func body(n int) []byte {
	b := make([]byte, n)
	for i := range b { // opaque payload; content is irrelevant to the benchmark
		b[i] = byte(i)
	}
	return b
}

func produce(ctx context.Context, eng *engine.Engine, q string, d time.Duration, P, msg int, _ string) (int64, *hist) {
	b := body(msg)
	deadline := time.Now().Add(d)
	hs := make([]*hist, P)
	var total int64
	var wg sync.WaitGroup
	for p := 0; p < P; p++ {
		hs[p] = newHist()
		wg.Add(1)
		go func(h *hist) {
			defer wg.Done()
			var local int64
			for time.Now().Before(deadline) {
				t := time.Now()
				if _, err := eng.SendOne(ctx, q, engine.OutMessage{Body: b}); err == nil {
					h.add(time.Since(t))
					local++
				}
			}
			atomic.AddInt64(&total, local)
		}(hs[p])
	}
	wg.Wait()
	return total, mergeAll(hs)
}

func batchProduce(ctx context.Context, eng *engine.Engine, q string, d time.Duration, P, batch, msg int) (int64, *hist) {
	b := body(msg)
	ms := make([]engine.OutMessage, batch)
	for i := range ms {
		ms[i] = engine.OutMessage{Body: b}
	}
	deadline := time.Now().Add(d)
	hs := make([]*hist, P)
	var total int64
	var wg sync.WaitGroup
	for p := 0; p < P; p++ {
		hs[p] = newHist()
		wg.Add(1)
		go func(h *hist) {
			defer wg.Done()
			var local int64
			for time.Now().Before(deadline) {
				t := time.Now()
				if _, err := eng.Send(ctx, q, ms...); err == nil {
					h.add(time.Since(t)) // per-batch latency
					local += int64(batch)
				}
			}
			atomic.AddInt64(&total, local)
		}(hs[p])
	}
	wg.Wait()
	return total, mergeAll(hs)
}

func sessionsRun(ctx context.Context, eng *engine.Engine, q string, d time.Duration, P, C, groups, msg int) (int64, *hist) {
	b := body(msg)
	deadline := time.Now().Add(d)
	stop := make(chan struct{})
	var prod int64
	var wg sync.WaitGroup
	for p := 0; p < P; p++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(seed) + 1))
			for time.Now().Before(deadline) {
				gid := "g" + strconv.Itoa(rng.Intn(groups))
				if _, err := eng.SendOne(ctx, q, engine.OutMessage{Body: b, GroupID: gid}); err == nil {
					atomic.AddInt64(&prod, 1)
				}
			}
		}(p)
	}
	cons := consumeUntil(ctx, eng, q, C, stop)
	wg.Wait()
	close(stop)
	_ = prod
	return cons()
}

func e2eRun(ctx context.Context, eng *engine.Engine, q string, d time.Duration, P, C, msg int) (int64, *hist) {
	b := body(msg)
	deadline := time.Now().Add(d)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for p := 0; p < P; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				eng.SendOne(ctx, q, engine.OutMessage{Body: b})
			}
		}()
	}
	cons := consumeUntil(ctx, eng, q, C, stop)
	wg.Wait()
	close(stop)
	return cons()
}

// consumeUntil starts C consumers that Receive(16)+Ack until stop closes.
// Returns a func that (after stop) blocks until all consumers finish, then
// returns the consumed count and the merged per-message Ack-latency histogram.
func consumeUntil(ctx context.Context, eng *engine.Engine, q string, C int, stop <-chan struct{}) func() (int64, *hist) {
	hs := make([]*hist, C)
	var total int64
	var wg sync.WaitGroup
	for c := 0; c < C; c++ {
		hs[c] = newHist()
		wg.Add(1)
		go func(h *hist) {
			defer wg.Done()
			var local int64
			for {
				select {
				case <-stop:
					atomic.AddInt64(&total, local)
					return
				default:
				}
				msgs, err := eng.Receive(ctx, q, engine.ReceiveOptions{MaxMessages: 16, WaitMs: 20})
				if err != nil {
					continue
				}
				for _, m := range msgs {
					t := time.Now()
					if eng.Complete(ctx, q, m.SeqNumber, m.LockToken) == nil {
						h.add(time.Since(t))
						local++
					}
				}
			}
		}(hs[c])
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	return func() (int64, *hist) { <-done; return atomic.LoadInt64(&total), mergeAll(hs) }
}

func drain(ctx context.Context, eng *engine.Engine, q string, C int) (int64, *hist) {
	hs := make([]*hist, C)
	var total int64
	var wg sync.WaitGroup
	for c := 0; c < C; c++ {
		hs[c] = newHist()
		wg.Add(1)
		go func(h *hist) {
			defer wg.Done()
			var local int64
			empty := 0
			for {
				msgs, err := eng.Receive(ctx, q, engine.ReceiveOptions{MaxMessages: 64, WaitMs: 0})
				if err != nil {
					continue
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
					if eng.Complete(ctx, q, m.SeqNumber, m.LockToken) == nil {
						h.add(time.Since(t))
						local++
					}
				}
			}
			atomic.AddInt64(&total, local)
		}(hs[c])
	}
	wg.Wait()
	return total, mergeAll(hs)
}

func prefill(ctx context.Context, eng *engine.Engine, q string, n, batch, msg int) {
	if batch <= 0 {
		batch = 64
	}
	b := body(msg)
	ms := make([]engine.OutMessage, batch)
	for i := range ms {
		ms[i] = engine.OutMessage{Body: b}
	}
	for i := 0; i < n; i += batch {
		eng.Send(ctx, q, ms...)
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

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
