package engine

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
)

// Go-native benchmarks for the embedded engine (no HTTP) — run with:
//
//	go test -bench=. -benchmem ./engine
//	go test -bench=BenchmarkSendBatch64 -benchtime=3s ./engine
//
// These measure the in-process SDK path the same way real embedded users drive it,
// and complement the container-based throughput/disk-IO matrix in test/bench/. Each
// reports a msg/s metric alongside the standard ns/op + allocs.

func benchEngine(b *testing.B) *Engine {
	b.Helper()
	e, err := Open(context.Background(), Options{
		DB:                "file:" + filepath.Join(b.TempDir(), "mq.db"),
		DisableBackground: true,
	})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	b.Cleanup(func() { _ = e.Close() })
	return e
}

func BenchmarkSendOne(b *testing.B) {
	ctx := context.Background()
	e := benchEngine(b)
	_ = e.CreateQueue(ctx, "q", QueueConfig{})
	body := make([]byte, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := e.SendOne(ctx, "q", OutMessage{Body: body}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msg/s")
}

func BenchmarkSendBatch64(b *testing.B) {
	ctx := context.Background()
	e := benchEngine(b)
	_ = e.CreateQueue(ctx, "q", QueueConfig{})
	const batch = 64
	msgs := make([]OutMessage, batch)
	for i := range msgs {
		msgs[i] = OutMessage{Body: make([]byte, 256)}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := e.Send(ctx, "q", msgs...); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N*batch)/b.Elapsed().Seconds(), "msg/s")
}

func BenchmarkReceiveComplete(b *testing.B) {
	ctx := context.Background()
	e := benchEngine(b)
	_ = e.CreateQueue(ctx, "q", QueueConfig{LockDurationMs: 600_000, MaxDeliveryCount: 100})
	body := make([]byte, 256)
	for i := 0; i < b.N; i++ { // prefill (untimed)
		if _, err := e.SendOne(ctx, "q", OutMessage{Body: body}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for got := 0; got < b.N; {
		msgs, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 64})
		if err != nil {
			b.Fatal(err)
		}
		for _, m := range msgs {
			if err := e.Complete(ctx, "q", m.SeqNumber, m.LockToken); err != nil {
				b.Fatal(err)
			}
			got++
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msg/s")
}

// BenchmarkDrainConcurrent measures the path the MQLITE-50 fix actually changed: many
// consumers draining one queue with Receive(batch)+CompleteBatch. The claim is a write,
// so all workers serialize on the single writer — this is where N-commits-per-Receive
// used to collapse. The msg/s metric is the regression signal: prefill b.N, drain with
// `workers` goroutines, report throughput. (Single-threaded dequeue is BenchmarkReceiveComplete.)
func BenchmarkDrainConcurrent(b *testing.B) {
	ctx := context.Background()
	e := benchEngine(b)
	_ = e.CreateQueue(ctx, "q", QueueConfig{LockDurationMs: 600_000, MaxDeliveryCount: 100})
	body := make([]byte, 256)
	for i := 0; i < b.N; i++ { // prefill (untimed)
		if _, err := e.SendOne(ctx, "q", OutMessage{Body: body}); err != nil {
			b.Fatal(err)
		}
	}
	const workers = 16
	var mu sync.Mutex
	var benchErr error
	fail := func(err error) {
		mu.Lock()
		if benchErr == nil {
			benchErr = err
		}
		mu.Unlock()
	}
	b.ReportAllocs()
	b.ResetTimer()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				msgs, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 50})
				if err != nil {
					fail(err)
					return
				}
				if len(msgs) == 0 {
					return // drained
				}
				items := make([]SettleItem, len(msgs))
				for i, m := range msgs {
					items[i] = SettleItem{SeqNumber: m.SeqNumber, LockToken: m.LockToken}
				}
				if _, err := e.CompleteBatch(ctx, "q", items); err != nil {
					fail(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
	if benchErr != nil {
		b.Fatal(benchErr)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msg/s")
}

func BenchmarkE2E(b *testing.B) {
	ctx := context.Background()
	e := benchEngine(b)
	_ = e.CreateQueue(ctx, "q", QueueConfig{LockDurationMs: 600_000, MaxDeliveryCount: 100})
	body := make([]byte, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := e.SendOne(ctx, "q", OutMessage{Body: body}); err != nil {
			b.Fatal(err)
		}
		msgs, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 1})
		if err != nil {
			b.Fatal(err)
		}
		for _, m := range msgs {
			if err := e.Complete(ctx, "q", m.SeqNumber, m.LockToken); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msg/s")
}
