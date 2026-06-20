package main

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// readRSS returns (VmRSS, VmHWM-peak) in bytes from /proc/self/status.
// Linux only; returns (0,0) elsewhere — the host-level memory probe the Go
// heap counters can't give (RSS includes the SQLite mmap/page cache + runtime).
func readRSS() (rss, peak uint64) {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0, 0
	}
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) < 2 {
			continue
		}
		switch f[0] {
		case "VmRSS:":
			v, _ := strconv.ParseUint(f[1], 10, 64)
			rss = v * 1024
		case "VmHWM:":
			v, _ := strconv.ParseUint(f[1], 10, 64)
			peak = v * 1024
		}
	}
	return
}

func fileBytes(p string) uint64 {
	if fi, err := os.Stat(p); err == nil {
		return uint64(fi.Size())
	}
	return 0
}

// sample is one point in the resource time-series.
type sample struct {
	Tms          int64  `json:"t_ms"`
	RSS          uint64 `json:"rss"`
	HeapInuse    uint64 `json:"heap_inuse"`
	HeapReleased uint64 `json:"heap_released"`
	DB           uint64 `json:"db"`
	WAL          uint64 `json:"wal"`
}

// sampler polls RSS + Go heap + DB/WAL file sizes on an interval, in the
// background, so a scenario gets a memory/disk time-series (not a single
// end-snapshot) and we can see growth vs reclamation across an idle phase.
type sampler struct {
	dbPath   string
	interval time.Duration
	stopCh   chan struct{}
	wg       sync.WaitGroup
	t0       time.Time

	mu       sync.Mutex
	series   []sample
	rssPeak  uint64
	heapPeak uint64
	dbPeak   uint64 // peak of db+wal combined
}

func newSampler(dbPath string, interval time.Duration) *sampler {
	return &sampler{dbPath: dbPath, interval: interval, stopCh: make(chan struct{}), t0: time.Now()}
}

func (s *sampler) snap() sample {
	rss, _ := readRSS()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	db := fileBytes(s.dbPath)
	wal := fileBytes(s.dbPath + "-wal")
	smp := sample{
		Tms: time.Since(s.t0).Milliseconds(), RSS: rss,
		HeapInuse: m.HeapInuse, HeapReleased: m.HeapReleased, DB: db, WAL: wal,
	}
	s.mu.Lock()
	if rss > s.rssPeak {
		s.rssPeak = rss
	}
	if m.HeapInuse > s.heapPeak {
		s.heapPeak = m.HeapInuse
	}
	if db+wal > s.dbPeak {
		s.dbPeak = db + wal
	}
	s.series = append(s.series, smp)
	s.mu.Unlock()
	return smp
}

func (s *sampler) start() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		t := time.NewTicker(s.interval)
		defer t.Stop()
		s.snap()
		for {
			select {
			case <-s.stopCh:
				return
			case <-t.C:
				s.snap()
			}
		}
	}()
}

func (s *sampler) stop() { close(s.stopCh); s.wg.Wait() }

func (s *sampler) peaks() (rss, heap, db uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rssPeak, s.heapPeak, s.dbPeak
}

// downsample returns at most n points (first, last, evenly spaced) for the JSON.
func (s *sampler) downsample(n int) []sample {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.series) <= n {
		return append([]sample(nil), s.series...)
	}
	out := make([]sample, 0, n)
	step := float64(len(s.series)-1) / float64(n-1)
	for i := 0; i < n; i++ {
		out = append(out, s.series[int(float64(i)*step)])
	}
	return out
}
