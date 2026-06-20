package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

func envN(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if x, err := strconv.Atoi(v); err == nil && x > 0 {
			return x
		}
	}
	return def
}

// TestMessageIntegrity is the no-loss + no-corruption sweep. It sends a contiguous
// sequence 1..N, each message carrying RANDOM body content plus that body's SHA-256
// in a property, then consumes concurrently with redelivery stress (some Abandons).
// It verifies two distinct guarantees:
//
//	(a) NO MESSAGE LOSS — every value 1..N is completed exactly once (no gap means
//	    nothing was dropped; >1 would mean a message was double-*completed*, which the
//	    delete-on-complete model forbids — at-least-once duplicates show up as extra
//	    *receives*, counted separately).
//	(b) NO CONTENT CORRUPTION — every received body still hashes to its property, so
//	    the body and its KV properties survived the round-trip through SQLite intact.
//
// N defaults to 10k (fast for CI under -race); set MQLITE_INTEGRITY_N=500000 for the
// large sweep the maintainer described.
func TestMessageIntegrity(t *testing.T) {
	// Default is modest so the test stays fast under CI's `-race`; set
	// MQLITE_INTEGRITY_N=500000 for the large sweep the maintainer described.
	n := envN("MQLITE_INTEGRITY_N", 1000)
	ctx := context.Background()
	e, err := Open(ctx, Options{DB: "file:" + filepath.Join(t.TempDir(), "mq.db"), DisableBackground: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	mustQueue(t, e, "q", QueueConfig{LockDurationMs: 600_000, MaxDeliveryCount: 1_000_000})

	// producer: enqueue the contiguous sequence 1..N in order, each with RANDOM body
	// content and that body's SHA-256 carried in a property.
	prng := rand.New(rand.NewSource(42))
	for i := 1; i <= n; i++ {
		body := make([]byte, 64+prng.Intn(192)) // random size 64..255
		_, _ = prng.Read(body)                  // random content (deterministic seed)
		sum := sha256.Sum256(body)
		if _, err := e.SendOne(ctx, "q", OutMessage{
			Body: body,
			Properties: map[string]string{
				"seq":    strconv.Itoa(i),
				"sha256": hex.EncodeToString(sum[:]),
			},
		}); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}

	// drain concurrently with competing consumers + redelivery stress (some Abandons)
	const C = 4
	completedTimes := make([]int32, n+1) // [seq] -> times completed; must be exactly 1 for 1..n
	var completed, receives, corrupt int64
	var wg sync.WaitGroup
	for c := 0; c < C; c++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(seed*7919 + 1)))
			for atomic.LoadInt64(&completed) < int64(n) {
				msgs, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 64, WaitMs: 0})
				if err != nil || len(msgs) == 0 {
					continue
				}
				for _, m := range msgs {
					atomic.AddInt64(&receives, 1)
					// (b) content integrity: the body must still hash to its property
					sum := sha256.Sum256(m.Body)
					ok := hex.EncodeToString(sum[:]) == m.Properties["sha256"]
					if !ok {
						atomic.AddInt64(&corrupt, 1)
					}
					// redelivery stress: abandon ~3% of healthy messages so they come
					// back (exercises at-least-once); always make progress otherwise.
					if ok && rng.Intn(100) < 3 {
						_ = e.Abandon(ctx, "q", m.SeqNumber, m.LockToken, 0)
						continue
					}
					seq, _ := strconv.Atoi(m.Properties["seq"])
					if e.Complete(ctx, "q", m.SeqNumber, m.LockToken) == nil && seq >= 1 && seq <= n {
						if atomic.AddInt32(&completedTimes[seq], 1) == 1 {
							atomic.AddInt64(&completed, 1)
						}
					}
				}
			}
		}(c)
	}
	wg.Wait()

	if c := atomic.LoadInt64(&corrupt); c > 0 {
		t.Fatalf("content corruption: %d message(s) whose body hash != stored property hash", c)
	}
	missing, dup := 0, 0
	for i := 1; i <= n; i++ {
		switch {
		case completedTimes[i] == 0:
			missing++
		case completedTimes[i] > 1:
			dup++
		}
	}
	if missing > 0 || dup > 0 {
		t.Fatalf("message loss: %d of %d missing, %d double-completed (total receives=%d)",
			missing, n, dup, atomic.LoadInt64(&receives))
	}
	rcv := atomic.LoadInt64(&receives)
	t.Logf("integrity OK: %d/%d delivered & content-verified, each completed exactly once; "+
		"total receives=%d (%.1f%% redelivered by at-least-once)",
		n, n, rcv, float64(rcv-int64(n))/float64(n)*100)
}
