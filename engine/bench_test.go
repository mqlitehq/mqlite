package engine

import (
	"context"
	"fmt"
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

// benchmarkTopicFanout exercises routing plus enqueue for a stable topic roster.
// Queue configuration is carried by the roster JOIN; an accidental per-subscription
// metadata lookup makes this path visibly regress as fanout grows, especially on a
// remote store where every lookup is a round trip.
func benchmarkTopicFanout(b *testing.B, subscriptions int) {
	ctx := context.Background()
	e := benchEngine(b)
	for i := 0; i < subscriptions; i++ {
		if err := e.Subscribe(ctx, "topic", fmt.Sprintf("sub-%02d", i), nil); err != nil {
			b.Fatal(err)
		}
	}
	body := make([]byte, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := e.SendOne(ctx, "topic", OutMessage{Body: body}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "publish/s")
	b.ReportMetric(float64(b.N*subscriptions)/b.Elapsed().Seconds(), "delivery/s")
}

func BenchmarkTopicFanout1(b *testing.B)   { benchmarkTopicFanout(b, 1) }
func BenchmarkTopicFanout32(b *testing.B)  { benchmarkTopicFanout(b, 32) }
func BenchmarkTopicFanout256(b *testing.B) { benchmarkTopicFanout(b, 256) }

// BenchmarkSubscriptionCreateDelete measures serialized, one-use subscription
// churn with low and high retained metadata cardinality. Seed setup is untimed.
// Each lifecycle uses a fresh name and deletes an empty subscription, which is the
// safe request/reply cleanup case without an ABA name reuse race.
func BenchmarkSubscriptionCreateDelete(b *testing.B) {
	for _, inventory := range []int{100, 10_000} {
		b.Run(fmt.Sprintf("Retained%d", inventory), func(b *testing.B) {
			ctx := context.Background()
			e, err := Open(ctx, Options{
				DB:                "file:" + filepath.Join(b.TempDir(), "mq.db"),
				DisableBackground: true,
				MaxSubscriptions:  inventory + 1,
			})
			if err != nil {
				b.Fatalf("open: %v", err)
			}
			b.Cleanup(func() { _ = e.Close() })
			if _, err := e.db.exec(ctx, `WITH RECURSIVE n(i) AS (
				SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<?
			) INSERT INTO queues(name,kind,created_at,updated_at)
			  SELECT printf('retained-%05d',i),'subscription',1,1 FROM n`, inventory-1); err != nil {
				b.Fatal(err)
			}
			if _, err := e.db.exec(ctx, `
				INSERT INTO subscriptions(topic,subscription,created_at)
				SELECT 'retained',name,1 FROM queues WHERE kind='subscription'`); err != nil {
				b.Fatal(err)
			}

			names := make([]string, b.N)
			for i := range names {
				names[i] = fmt.Sprintf("temporary-%08d", i)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for _, name := range names {
				if err := e.Subscribe(ctx, "replies", name, nil); err != nil {
					b.Fatal(err)
				}
				result, err := e.DeleteSubscriptionWithOptions(ctx, "replies", name, DeleteOptions{})
				if err != nil {
					b.Fatal(err)
				}
				if !result.Deleted {
					b.Fatalf("empty subscription %q remained in state %q", name, result.State)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "lifecycle/s")
		})
	}
}

// BenchmarkListSubscriptionsPage measures the first 100-entry page together with
// its exact snapshot total at representative inventories. Setup is untimed. The
// result is page latency; it does not claim that creating 10,000 subscriptions is
// a low-latency operation.
func BenchmarkListSubscriptionsPage(b *testing.B) {
	for _, inventory := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("Inventory%d/Page100", inventory), func(b *testing.B) {
			ctx := context.Background()
			e := benchEngine(b)
			if _, err := e.db.exec(ctx, `WITH RECURSIVE n(i) AS (
				SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<?
			) INSERT INTO queues(name,kind,created_at,updated_at)
			  SELECT printf('sub-%05d',i),'subscription',1,1 FROM n`, inventory-1); err != nil {
				b.Fatal(err)
			}
			if _, err := e.db.exec(ctx, `
				INSERT INTO subscriptions(topic,subscription,created_at)
				SELECT 'inventory',name,1 FROM queues WHERE kind='subscription'`); err != nil {
				b.Fatal(err)
			}

			options := ListSubscriptionsOptions{Topic: "inventory", Limit: defaultSubscriptionPageSize}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				page, err := e.ListSubscriptionsWithOptions(ctx, options)
				if err != nil {
					b.Fatal(err)
				}
				if page.Total != inventory || len(page.Subscriptions) != 100 {
					b.Fatalf("page total/items = %d/%d, want %d/100", page.Total, len(page.Subscriptions), inventory)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "page/s")
		})
	}
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
