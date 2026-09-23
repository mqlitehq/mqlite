package server

import (
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mqlitehq/mqlite/wire"
)

// rpcLatency is a per-RPC latency histogram exposed at /metrics, so a slow dequeue
// (the kind of regression this project cares about) shows up in monitoring, not just in
// tests. Hand-rolled — no Prometheus client dependency, keeping the binary dependency-light:
// A per-method mutex makes count, sum and buckets one coherent observation.
type rpcLatency struct {
	mu   sync.RWMutex
	rpcs map[string]*rpcCounters
}

// latencyBuckets are the histogram upper bounds in seconds (ascending). They span the
// fast in-region path (sub-ms) through the slow-claim / long-poll tail (tens of seconds),
// so both healthy latency and a dequeue stall are visible.
var latencyBuckets = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20}

type rpcCounters struct {
	mu     sync.Mutex
	bucket []uint64 // noncumulative; last bucket is greater than the largest bound
	sumUs  uint64
	count  uint64
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
			c = &rpcCounters{bucket: make([]uint64, len(latencyBuckets)+1)}
			h.rpcs[rpc] = c
		}
		h.mu.Unlock()
	}
	// SearchFloat64s returns the first bucket whose upper bound >= the duration (le
	// semantics); past the last bound that's len(latencyBuckets) — the +Inf bucket.
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bucket[sort.SearchFloat64s(latencyBuckets, d.Seconds())]++
	if us := d.Microseconds(); us > 0 {
		c.sumUs += uint64(us)
	}
	c.count++
}

// snapshot supplies both native and Prometheus output from the same counters.
func (h *rpcLatency) snapshot() []wire.RPCLatencyObservation {
	h.mu.RLock()
	defer h.mu.RUnlock()
	names := make([]string, 0, len(h.rpcs))
	for n := range h.rpcs {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]wire.RPCLatencyObservation, 0, len(names))
	for _, rpc := range names {
		c := h.rpcs[rpc]
		c.mu.Lock()
		v := wire.RPCLatencyObservation{RPC: rpc, Count: c.count, SumSeconds: float64(c.sumUs) / 1e6,
			Buckets: make([]wire.LatencyBucket, len(latencyBuckets))}
		var cum uint64
		for i, ub := range latencyBuckets {
			cum += c.bucket[i]
			v.Buckets[i] = wire.LatencyBucket{UpperBoundSeconds: ub, Count: cum}
		}
		c.mu.Unlock()
		out = append(out, v)
	}
	return out
}

// observe is the always-on middleware that times every RPC (/mqlite.v1.*) and feeds the
// histogram. Non-RPC paths (/, /healthz, /metrics, /ui) pass straight through — we don't
// want /metrics to time itself, and static/discovery latency isn't interesting.
//
// Only REGISTERED routes are ever labeled (MQLITE-62): an unregistered
// /mqlite.v1.* path falls through the mux to the "/" catch-all (404), and its
// pattern therefore differs from the request path. Labeling those would let any
// client grow the histogram map without bound — one counter pair per invented
// path name, never evicted — a memory / scrape-size DoS on the broker.
// Asking the mux (instead of keeping a hand-written allowlist) means new routes
// are covered automatically and nothing can drift.
func (s *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/mqlite.v1.") {
			next.ServeHTTP(w, r)
			return
		}
		if _, pattern := s.mux.Handler(r); pattern != r.URL.Path {
			next.ServeHTTP(w, r) // unregistered RPC path: serve the 404, record nothing
			return
		}
		start := time.Now()
		next.ServeHTTP(w, r)
		s.rpcLat.observe(strings.TrimPrefix(r.URL.Path, "/mqlite.v1."), time.Since(start))
	})
}
