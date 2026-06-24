// Command httpload is an HTTP load generator for a *running mqlite broker*. Unlike
// test/bench (which drives the embedded engine, no network), this drives the real wire
// path — the client SDK over HTTP + Bearer auth — so it measures the broker end to end.
// It is the reproducible harness behind the MQLITE-50 dequeue-throughput work: run it
// once for a baseline, apply a change, run it again, and diff the SUMMARY line.
//
// Each run does three things against one queue (default `bench-load`):
//  1. a single-connection latency probe (send, and send→receive→complete) — the RTT floor;
//  2. a SEND throughput phase (conc workers spamming SendOne for -dur);
//  3. a DRAIN throughput phase (conc workers: Receive≤50 then CompleteBatch, until empty).
//
// It drains any leftover backlog first and reports p50/p95/p99/max for each op.
//
// Usage:
//
//	MQLITE_TOKEN=mqk_… go run ./test/bench/httpload -endpoint https://mqlite.fly.dev -conc 32 -dur 10s
//
// Note on Fly: driving a Fly broker from far away is network-RTT-bound (the numbers
// measure distance, not the broker). For true capacity run this co-located in the same
// region as a one-off Fly app, pointing at the broker's PRIVATE address
// (http://<app>.internal:8080) — the public address hairpins through the edge and hangs.
package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mqlitehq/mqlite"
)

func main() {
	endpoint := flag.String("endpoint", "http://127.0.0.1:8080", "broker URL")
	queue := flag.String("queue", "bench-load", "queue name")
	dur := flag.Duration("dur", 10*time.Second, "duration of the SEND throughput phase")
	conc := flag.Int("conc", 32, "concurrency (workers / max keep-alive conns)")
	msgsize := flag.Int("msgsize", 256, "message body bytes")
	probeN := flag.Int("probe", 50, "sequential ops for the single-connection latency probe")
	flag.Parse()

	tok := os.Getenv("MQLITE_TOKEN")
	ctx := context.Background()

	// Reuse up to `conc` keep-alive connections so we measure the broker, not TLS
	// handshakes. No client timeout — long-poll Receive relies on the request context.
	tr := &http.Transport{
		MaxIdleConns:        *conc * 2,
		MaxIdleConnsPerHost: *conc,
		MaxConnsPerHost:     *conc,
		IdleConnTimeout:     90 * time.Second,
	}
	cli, err := mqlite.Open(ctx, *endpoint, mqlite.WithToken(tok), mqlite.WithHTTPClient(&http.Client{Transport: tr}))
	if err != nil {
		fatal("open: %v", err)
	}

	body := make([]byte, *msgsize)
	_, _ = rand.Read(body)
	fmt.Printf("== mqlite httpload ==\nendpoint=%s queue=%s conc=%d msgsize=%dB dur=%s\n\n",
		*endpoint, *queue, *conc, *msgsize, *dur)

	// Wake the broker (Fly auto-stops; first call cold-starts) + ensure the queue exists.
	t0 := time.Now()
	if err := cli.CreateQueue(ctx, *queue, mqlite.QueueConfig{}); err != nil {
		fatal("create queue (wake): %v", err)
	}
	fmt.Printf("warmup/create-queue: %s\n", time.Since(t0).Round(time.Millisecond))
	drainAll(ctx, cli, *queue, *conc) // clear leftovers from a prior run

	// ── single-connection latency probe (RTT floor, no saturation) ──
	var sendProbe, e2eProbe []time.Duration
	for i := 0; i < *probeN; i++ {
		s := time.Now()
		if _, err := cli.SendOne(ctx, *queue, mqlite.OutMessage{Body: body}); err == nil {
			sendProbe = append(sendProbe, time.Since(s))
		}
	}
	drainAll(ctx, cli, *queue, 1)
	for i := 0; i < *probeN; i++ {
		s := time.Now()
		if _, err := cli.SendOne(ctx, *queue, mqlite.OutMessage{Body: body}); err != nil {
			continue
		}
		msgs, err := cli.Receive(ctx, *queue, mqlite.RecvOpts{Max: 1, Wait: 2 * time.Second})
		if err != nil || len(msgs) == 0 {
			continue
		}
		_ = msgs[0].Complete(ctx)
		e2eProbe = append(e2eProbe, time.Since(s))
	}
	drainAll(ctx, cli, *queue, 1)

	// ── SEND throughput ──
	sendLat := make([][]time.Duration, *conc)
	var sent, sendErr int64
	deadline := time.Now().Add(*dur)
	var wg sync.WaitGroup
	tSend := time.Now()
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for time.Now().Before(deadline) {
				s := time.Now()
				if _, err := cli.SendOne(ctx, *queue, mqlite.OutMessage{Body: body}); err != nil {
					atomic.AddInt64(&sendErr, 1)
					continue
				}
				sendLat[w] = append(sendLat[w], time.Since(s))
				atomic.AddInt64(&sent, 1)
			}
		}(w)
	}
	wg.Wait()
	sendElapsed := time.Since(tSend)

	// ── DRAIN throughput (Receive batch → CompleteBatch, until empty) ──
	recvLat := make([][]time.Duration, *conc)
	complLat := make([][]time.Duration, *conc)
	var completed, drainErr int64
	tDrain := time.Now()
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for {
				s := time.Now()
				msgs, err := cli.Receive(ctx, *queue, mqlite.RecvOpts{Max: 50, Wait: 0})
				if err != nil {
					atomic.AddInt64(&drainErr, 1)
					continue
				}
				if len(msgs) == 0 {
					return // drained (no concurrent sender)
				}
				recvLat[w] = append(recvLat[w], time.Since(s))
				cs := time.Now()
				if _, err := cli.CompleteBatch(ctx, *queue, msgs...); err != nil {
					atomic.AddInt64(&drainErr, 1)
					continue
				}
				complLat[w] = append(complLat[w], time.Since(cs))
				atomic.AddInt64(&completed, int64(len(msgs)))
			}
		}(w)
	}
	wg.Wait()
	drainElapsed := time.Since(tDrain)

	sendRate := float64(sent) / sendElapsed.Seconds()
	drainRate := float64(completed) / drainElapsed.Seconds()

	fmt.Printf("\n── single-connection latency (RTT floor) ──\n")
	report("send          ", sendProbe)
	report("send→recv→done", e2eProbe)
	fmt.Printf("\n── SEND throughput (conc=%d) ──\nsent=%d errs=%d in %s => %.0f msg/s\n",
		*conc, sent, sendErr, sendElapsed.Round(time.Millisecond), sendRate)
	report("send latency  ", merge(sendLat))
	fmt.Printf("\n── DRAIN: receive(≤50)+completeBatch (conc=%d) ──\ncompleted=%d errs=%d in %s => %.0f msg/s\n",
		*conc, completed, drainErr, drainElapsed.Round(time.Millisecond), drainRate)
	report("receive RPC   ", merge(recvLat))
	report("completeBatch ", merge(complLat))

	// One-line, diff-friendly summary for before/after comparison.
	sl, rl := merge(sendLat), merge(recvLat)
	sort.Slice(sl, func(i, j int) bool { return sl[i] < sl[j] })
	sort.Slice(rl, func(i, j int) bool { return rl[i] < rl[j] })
	fmt.Printf("\nSUMMARY conc=%d send=%.0f/s send_p50=%s drain=%.0f/s recv_p50=%s recv_max=%s errs=%d\n",
		*conc, sendRate, pct(sl, .50).Round(time.Millisecond), drainRate,
		pct(rl, .50).Round(time.Millisecond), dmax(rl).Round(time.Millisecond), sendErr+drainErr)
}

func drainAll(ctx context.Context, cli *mqlite.Client, q string, conc int) {
	var wg sync.WaitGroup
	for i := 0; i < conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				msgs, err := cli.Receive(ctx, q, mqlite.RecvOpts{Max: 100, Wait: 0})
				if err != nil || len(msgs) == 0 {
					return
				}
				_, _ = cli.CompleteBatch(ctx, q, msgs...)
			}
		}()
	}
	wg.Wait()
}

func merge(ss [][]time.Duration) []time.Duration {
	var out []time.Duration
	for _, s := range ss {
		out = append(out, s...)
	}
	return out
}

func report(label string, d []time.Duration) {
	if len(d) == 0 {
		fmt.Printf("%s  (no samples)\n", label)
		return
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	fmt.Printf("%s  n=%-6d p50=%-8s p95=%-8s p99=%-8s max=%s\n", label, len(d),
		pct(d, .50).Round(time.Millisecond), pct(d, .95).Round(time.Millisecond),
		pct(d, .99).Round(time.Millisecond), dmax(d).Round(time.Millisecond))
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(p * float64(len(sorted)))
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}

func dmax(sorted []time.Duration) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[len(sorted)-1]
}

func fatal(f string, a ...any) {
	fmt.Fprintf(os.Stderr, "fatal: "+f+"\n", a...)
	os.Exit(1)
}
