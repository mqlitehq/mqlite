package main

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mqlitehq/mqlite/engine"
)

const (
	lifecycleQueueCount    = 64
	lifecycleHeavyQueues   = 4
	lifecycleLightMessages = 16
	lifecycleHeavyMessages = 1024 // exceeds the 1,000-row foreground cleanup budget
	lifecycleMaxPasses     = 64
	lifecycleMaxMessage    = 128 // < 1 MiB live body payload per 64-queue cycle
)

type lifecycleRunResult struct {
	cycles, created, messages                      int64
	deleteAttempts, deleteAccepted, deletePending  int64
	maintenancePasses, converged, peakPayloadBytes int64
	deleteLatency                                  *hist
	convergeLatency                                durationSamples
}

// durationSamples retains the small number of per-cycle convergence durations.
// The generic histogram intentionally caps percentiles at 100ms, while remote
// lifecycle convergence can take longer and must remain exact in the result.
type durationSamples struct {
	values []time.Duration
	sum    time.Duration
	max    time.Duration
}

func (s *durationSamples) add(d time.Duration) {
	s.values = append(s.values, d)
	s.sum += d
	if d > s.max {
		s.max = d
	}
}

func (s durationSamples) meanMs() float64 {
	if len(s.values) == 0 {
		return 0
	}
	return float64(s.sum) / float64(len(s.values)) / float64(time.Millisecond)
}

func (s durationSamples) pctMs(p float64) float64 {
	if len(s.values) == 0 {
		return 0
	}
	values := append([]time.Duration(nil), s.values...)
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	index := int(float64(len(values)) * p)
	if index < 1 {
		index = 1
	}
	if index > len(values) {
		index = len(values)
	}
	return float64(values[index-1]) / float64(time.Millisecond)
}

func (s durationSamples) maxMs() float64 {
	return float64(s.max) / float64(time.Millisecond)
}

func lifecycleMessageSize(requested int) int {
	if requested < 1 {
		return 1
	}
	if requested > lifecycleMaxMessage {
		return lifecycleMaxMessage
	}
	return requested
}

// lifecycleChurnRun repeatedly creates a fixed 64-name working set, batch-sends
// small messages, force-deletes every queue, and explicitly drives bounded
// maintenance until all names disappear. Four queues exceed the 1,000-row
// foreground budget, so every healthy cycle observes both immediate and pending
// acknowledgements without retaining a large payload. Names are reused only
// after ListQueues confirms convergence, matching the documented ABA boundary.
func lifecycleChurnRun(ctx context.Context, eng *engine.Engine, prefix string, d time.Duration, msgSize int, samp *sampler) (lifecycleRunResult, error) {
	result := lifecycleRunResult{deleteLatency: newHist()}
	body := body(lifecycleMessageSize(msgSize))
	deadline := time.Now().Add(d)
	queuePrefix := fmt.Sprintf("%s-lifecycle-%d", prefix, time.Now().UnixNano())
	names := make([]string, lifecycleQueueCount)
	for i := range names {
		names[i] = fmt.Sprintf("%s-%02d", queuePrefix, i)
	}
	lightBatch := lifecycleBatch(body, lifecycleLightMessages)
	heavyBatch := lifecycleBatch(body, lifecycleHeavyMessages)

	for first := true; first || time.Now().Before(deadline); first = false {
		for _, name := range names {
			if err := eng.CreateQueue(ctx, name, engine.QueueConfig{}); err != nil {
				return result, fmt.Errorf("create %q: %w", name, err)
			}
			result.created++
		}

		var livePayload int64
		for i, name := range names {
			messages := lightBatch
			if i < lifecycleHeavyQueues {
				messages = heavyBatch
			}
			if _, err := eng.Send(ctx, name, messages...); err != nil {
				return result, fmt.Errorf("send batch to %q: %w", name, err)
			}
			result.messages += int64(len(messages))
			livePayload += int64(len(messages) * len(body))
		}
		if livePayload > result.peakPayloadBytes {
			result.peakPayloadBytes = livePayload
		}
		// Capture the populated database before fast local deletion can make the
		// periodic sampler miss its peak.
		if samp != nil {
			samp.snap()
		}

		convergeStart := time.Now()
		for _, name := range names {
			started := time.Now()
			result.deleteAttempts++
			ack, err := eng.DeleteQueueWithOptions(ctx, name, engine.DeleteOptions{Force: true})
			if err != nil {
				return result, fmt.Errorf("force delete %q: %w", name, err)
			}
			result.deleteLatency.add(time.Since(started))
			result.deleteAccepted++
			if !ack.Deleted {
				result.deletePending++
			}
		}

		for pass := 0; ; pass++ {
			remaining, err := lifecycleQueuesPresent(ctx, eng, names)
			if err != nil {
				return result, err
			}
			if remaining == 0 {
				break
			}
			if pass >= lifecycleMaxPasses {
				return result, fmt.Errorf("%d queues remain after %d maintenance passes", remaining, lifecycleMaxPasses)
			}
			eng.RunMaintenanceOnce(ctx)
			result.maintenancePasses++
		}
		result.convergeLatency.add(time.Since(convergeStart))
		result.converged += int64(len(names))
		result.cycles++
	}
	return result, nil
}

func lifecycleBatch(body []byte, count int) []engine.OutMessage {
	messages := make([]engine.OutMessage, count)
	for i := range messages {
		messages[i] = engine.OutMessage{Body: body}
	}
	return messages
}

func lifecycleQueuesPresent(ctx context.Context, eng *engine.Engine, names []string) (int, error) {
	listed, err := eng.ListQueues(ctx)
	if err != nil {
		return 0, fmt.Errorf("list queues during lifecycle convergence: %w", err)
	}
	want := make(map[string]struct{}, len(names))
	for _, name := range names {
		want[name] = struct{}{}
	}
	remaining := 0
	for _, queue := range listed {
		if _, ok := want[queue.Name]; ok {
			remaining++
		}
	}
	return remaining, nil
}

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
// consumers keep draining for the remainder — the "load ramp" (load up then down)
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
