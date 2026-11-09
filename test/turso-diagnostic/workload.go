//go:build mqlite_perf_diag

package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	mq "github.com/mqlitehq/mqlite"
)

func admissionFor(p casePlan) chan struct{} {
	if p.Admission > 0 {
		return make(chan struct{}, p.Admission)
	}
	return nil
}
func runSend(ctx context.Context, a *adapter, p casePlan, q string, progress *caseProgress) {
	var next atomic.Int64
	var wg sync.WaitGroup
	admission := admissionFor(p)
	for worker := 0; worker < p.Concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				start := int(next.Add(int64(p.Batch))) - p.Batch
				if start >= p.Count {
					return
				}
				n := p.Batch
				if n > p.Count-start {
					n = p.Count - start
				}
				ms := make([]mq.OutMessage, n)
				for i := range ms {
					ms[i] = inputFor(fmt.Sprintf("%s/%d", q, start+i), p.Payload)
				}
				progress.book.add(ms)
				var seqs []int64
				err := progress.call(ctx, p.ID, "send", admission, func(call context.Context) error {
					var e error
					seqs, e = a.send(call, q, ms)
					if e == nil {
						e = checkACK(seqs, n)
					}
					return e
				})
				progress.book.sent(ms, seqs, err)
			}
		}()
	}
	wg.Wait()
}
func runDrain(ctx context.Context, a *adapter, p casePlan, q string, progress *caseProgress) {
	var wg sync.WaitGroup
	var attempts atomic.Uint64
	admission := admissionFor(p)
	// Deliberately preserve V11's send-then-drain structure and one successful
	// empty Receive per worker. Changing either would change the original profile.
	for worker := 0; worker < p.Concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				var ms []*delivery
				attempt := fmt.Sprintf("%s-a%d", q, attempts.Add(1))
				err := progress.call(ctx, p.ID, "receive", admission, func(call context.Context) error { var e error; ms, e = a.receive(call, q, p.Batch, attempt); return e })
				if err != nil {
					progress.book.failed()
					return
				}
				if len(ms) == 0 {
					return
				}
				progress.book.observe(ms, p.Payload)
				var mask []bool
				err = progress.call(ctx, p.ID, "complete_batch", admission, func(call context.Context) error { var e error; mask, e = a.complete(call, q, ms); return e })
				progress.book.complete(ms, mask, err)
				if err != nil {
					return
				}
			}
		}()
	}
	wg.Wait()
}
