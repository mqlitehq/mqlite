package main

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mqlitehq/mqlite/engine"
)

// propMsg builds a realistically-enriched message: a body plus a typical set of
// KV properties (headers) + ASB-style metadata — the shape a real producer sends
// (tenant / trace / event-type / ...). A heavier per-message write than a bare
// body; this is what "normal usage" looks like, not the baseline.
func propMsg(b []byte, i int) engine.OutMessage {
	return engine.OutMessage{
		Body:          b,
		CorrelationID: "corr-" + strconv.Itoa(i),
		Subject:       "order.created",
		ContentType:   "application/json",
		Properties: map[string]string{
			"tenant":     "acme-" + strconv.Itoa(i%32),
			"trace_id":   "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
			"event_type": "OrderCreated",
			"priority":   "high",
			"source":     "checkout-service",
			"schema_ver": "3",
		},
	}
}

// propsRun produces enriched (KV-property) messages — single or batched.
func propsRun(ctx context.Context, eng *engine.Engine, q string, d time.Duration, P, batch, msg int) (int64, *hist) {
	b := body(msg)
	deadline := time.Now().Add(d)
	hs := make([]*hist, P)
	var total int64
	var wg sync.WaitGroup
	for p := 0; p < P; p++ {
		hs[p] = newHist()
		wg.Add(1)
		go func(h *hist, seed int) {
			defer wg.Done()
			var local int64
			i := seed * 1_000_000
			for time.Now().Before(deadline) {
				t := time.Now()
				if batch > 1 {
					ms := make([]engine.OutMessage, batch)
					for k := range ms {
						ms[k] = propMsg(b, i+k)
					}
					if _, err := eng.Send(ctx, q, ms...); err == nil {
						h.add(time.Since(t))
						local += int64(batch)
					}
					i += batch
				} else {
					if _, err := eng.SendOne(ctx, q, propMsg(b, i)); err == nil {
						h.add(time.Since(t))
						local++
					}
					i++
				}
			}
			atomic.AddInt64(&total, local)
		}(hs[p], p)
	}
	wg.Wait()
	return total, mergeAll(hs)
}

// bloatRun fills `prefill` messages then drains ALL of them (Complete = DELETE),
// returning the drained count. The sampler captures the DB-file PEAK (after fill)
// vs the FINAL size (after drain + idle): does the file shrink when the queue
// empties? SQLite has no auto-VACUUM, so freed pages go to the freelist and are
// reused — this scenario measures that bloat empirically.
func bloatRun(ctx context.Context, eng *engine.Engine, q string, prefillN, batch, msg int) (int64, *hist) {
	if batch <= 0 {
		batch = 64
	}
	b := body(msg)
	ms := make([]engine.OutMessage, batch)
	for i := range ms {
		ms[i] = engine.OutMessage{Body: b}
	}
	for i := 0; i < prefillN; i += batch {
		eng.Send(ctx, q, ms...)
	}
	h := newHist()
	var drained int64
	empty := 0
	for {
		msgs, err := eng.Receive(ctx, q, engine.ReceiveOptions{MaxMessages: 256, WaitMs: 0})
		if err != nil {
			continue
		}
		if len(msgs) == 0 {
			if empty++; empty > 3 {
				break
			}
			continue
		}
		empty = 0
		for _, m := range msgs {
			t := time.Now()
			if eng.Complete(ctx, q, m.SeqNumber, m.LockToken) == nil {
				h.add(time.Since(t))
				drained++
			}
		}
	}
	return drained, h
}

// rampDownRun produces hard for the first ~55% of d, then STOPS producing while
// consumers keep draining for the remainder — the "上下线" (load up then down)
// curve. The sampler captures peak RSS/heap/DB during the busy phase vs the idle
// tail, so we see whether memory and file are reclaimed once load goes away.
func rampDownRun(ctx context.Context, eng *engine.Engine, q string, d time.Duration, P, C, msg int) (int64, *hist) {
	b := body(msg)
	start := time.Now()
	busy := start.Add(time.Duration(float64(d) * 0.55))
	end := start.Add(d)
	stop := make(chan struct{})
	var produced int64
	var wg sync.WaitGroup
	for p := 0; p < P; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(busy) {
				if _, err := eng.SendOne(ctx, q, engine.OutMessage{Body: b}); err == nil {
					atomic.AddInt64(&produced, 1)
				}
			}
		}()
	}
	cons := consumeUntil(ctx, eng, q, C, stop)
	wg.Wait()                   // producers stopped — the down-ramp
	time.Sleep(time.Until(end)) // idle tail: consumers drain the backlog, then idle
	close(stop)
	n, h := cons()
	_ = produced
	return n, h
}

// churnRun keeps producers steady while consumers cycle online/offline (a worker
// fleet scaling in and out). Checks that per-cycle connect/disconnect doesn't leak
// memory — RSS should stay flat across cycles, not stair-step up.
func churnRun(ctx context.Context, eng *engine.Engine, q string, d time.Duration, P, C, msg int) (int64, *hist) {
	b := body(msg)
	deadline := time.Now().Add(d)
	var prodStop int32
	var wg sync.WaitGroup
	for p := 0; p < P; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for atomic.LoadInt32(&prodStop) == 0 {
				eng.SendOne(ctx, q, engine.OutMessage{Body: b})
			}
		}()
	}
	h := newHist()
	var consumed int64
	for time.Now().Before(deadline) {
		stop := make(chan struct{})
		cons := consumeUntil(ctx, eng, q, C, stop) // a fresh consumer cohort comes online
		time.Sleep(250 * time.Millisecond)
		close(stop) // ...then all go offline
		n, hh := cons()
		consumed += n
		h.merge(hh)
	}
	atomic.StoreInt32(&prodStop, 1)
	wg.Wait()
	return consumed, h
}
