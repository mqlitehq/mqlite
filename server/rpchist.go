package server

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// rpcLatency is a per-RPC latency histogram exposed at /metrics, so a slow dequeue
// (the kind of regression this project cares about) shows up in monitoring, not just in
// tests. Hand-rolled — no Prometheus client dependency, keeping the binary dependency-light:
// per-RPC atomic bucket counters; the RWMutex is taken only to lazily create a method's
// entry, so the hot path is an RLock + a few atomic adds.
type rpcLatency struct {
	mu   sync.RWMutex
	rpcs map[string]*rpcCounters
}

// latencyBuckets are the histogram upper bounds in seconds (ascending). They span the
// fast in-region path (sub-ms) through the slow-claim / long-poll tail (tens of seconds),
// so both healthy latency and a dequeue stall are visible.
var latencyBuckets = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20}

type rpcCounters struct {
	bucket []atomic.Uint64 // len = len(latencyBuckets)+1; the last is the +Inf bucket
	sumUs  atomic.Uint64   // sum of observed durations, in microseconds
	count  atomic.Uint64
}

func newRPCLatency() *rpcLatency { return &rpcLatency{rpcs: map[string]*rpcCounters{}} }

// observe records one RPC duration against its method.
func (h *rpcLatency) observe(rpc string, d time.Duration) {
	h.mu.RLock()
	c := h.rpcs[rpc]
	h.mu.RUnlock()
	if c == nil {
		h.mu.Lock()
		if c = h.rpcs[rpc]; c == nil {
			c = &rpcCounters{bucket: make([]atomic.Uint64, len(latencyBuckets)+1)}
			h.rpcs[rpc] = c
		}
		h.mu.Unlock()
	}
	// SearchFloat64s returns the first bucket whose upper bound >= the duration (le
	// semantics); past the last bound that's len(latencyBuckets) — the +Inf bucket.
	c.bucket[sort.SearchFloat64s(latencyBuckets, d.Seconds())].Add(1)
	if us := d.Microseconds(); us > 0 {
		c.sumUs.Add(uint64(us))
	}
	c.count.Add(1)
}

// write renders the histogram in Prometheus text format (cumulative buckets).
func (h *rpcLatency) write(b *strings.Builder) {
	h.mu.RLock()
	names := make([]string, 0, len(h.rpcs))
	for n := range h.rpcs {
		names = append(names, n)
	}
	h.mu.RUnlock()
	if len(names) == 0 {
		return
	}
	sort.Strings(names)
	b.WriteString("# HELP mqlite_rpc_duration_seconds RPC handler latency by method.\n")
	b.WriteString("# TYPE mqlite_rpc_duration_seconds histogram\n")
	for _, rpc := range names {
		h.mu.RLock()
		c := h.rpcs[rpc]
		h.mu.RUnlock()
		var cum uint64
		for i, ub := range latencyBuckets {
			cum += c.bucket[i].Load()
			fmt.Fprintf(b, "mqlite_rpc_duration_seconds_bucket{rpc=%q,le=%q} %d\n",
				rpc, strconv.FormatFloat(ub, 'g', -1, 64), cum)
		}
		cum += c.bucket[len(latencyBuckets)].Load() // +Inf bucket = total count
		fmt.Fprintf(b, "mqlite_rpc_duration_seconds_bucket{rpc=%q,le=\"+Inf\"} %d\n", rpc, cum)
		fmt.Fprintf(b, "mqlite_rpc_duration_seconds_sum{rpc=%q} %g\n", rpc, float64(c.sumUs.Load())/1e6)
		fmt.Fprintf(b, "mqlite_rpc_duration_seconds_count{rpc=%q} %d\n", rpc, c.count.Load())
	}
}

// observe is the always-on middleware that times every RPC (/mqlite.v1.*) and feeds the
// histogram. Non-RPC paths (/, /healthz, /metrics, /ui) pass straight through — we don't
// want /metrics to time itself, and static/discovery latency isn't interesting.
func (s *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/mqlite.v1.") {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		next.ServeHTTP(w, r)
		s.rpcLat.observe(strings.TrimPrefix(r.URL.Path, "/mqlite.v1."), time.Since(start))
	})
}
