package engine

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// openWithDLQ opens a deterministic (clock-injected, no background) engine with
// the DLQ retention bounds set, for the reapDLQ tests.
func openWithDLQ(t *testing.T, ageMs int64, count int) (*Engine, *int64) {
	t.Helper()
	var ms int64 = 1_700_000_000_000
	e, err := Open(context.Background(), Options{
		DB:                "file:" + filepath.Join(t.TempDir(), "mq.db"),
		Now:               func() int64 { return atomic.LoadInt64(&ms) },
		DisableBackground: true,
		DLQMaxAgeMs:       ageMs,
		DLQMaxCount:       count,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e, &ms
}

func sendBodies(t *testing.T, e *Engine, q string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := e.SendOne(context.Background(), q, OutMessage{Body: []byte{byte(i)}}); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
}

func dlqBodies(t *testing.T, e *Engine, q string) []byte {
	t.Helper()
	msgs, err := e.Peek(context.Background(), q, PeekOptions{State: StateDeadLettered, Max: 100})
	if err != nil {
		t.Fatalf("peek: %v", err)
	}
	var b []byte
	for _, m := range msgs { // Peek is ORDER BY id ASC -> oldest first
		if len(m.Body) > 0 {
			b = append(b, m.Body[0])
		}
	}
	return b
}

// A per-queue count cap keeps the newest N dead letters (drop-oldest) and never
// touches messages in any other state or queue.
func TestDLQRetentionCountDropsOldest(t *testing.T) {
	ctx := context.Background()
	e, ms := openWithDLQ(t, 0, 2) // keep at most 2 dead letters per queue
	mustQueue(t, e, "q", QueueConfig{DefaultTTLMs: 1000})
	mustQueue(t, e, "live", QueueConfig{}) // no TTL -> stays active, must be untouched

	sendBodies(t, e, "q", 5)    // bodies 0..4
	sendBodies(t, e, "live", 3) // active work, never expires

	advance(ms, 2*time.Second)
	e.RunMaintenanceOnce(ctx) // expireTTL dead-letters the 5; reapDLQ keeps newest 2

	if mt, _ := e.Stats(ctx, "q"); mt.DeadLettered != 2 {
		t.Fatalf("DLQ count cap: want 2 dead-lettered, got %d", mt.DeadLettered)
	}
	if live, _ := e.Stats(ctx, "live"); live.Active != 3 {
		t.Fatalf("retention must never touch non-DLQ work: want 3 active in 'live', got %d", live.Active)
	}
	// drop-oldest: survivors are the two NEWEST (bodies 3 and 4)
	if got := dlqBodies(t, e, "q"); len(got) != 2 || got[0] != 3 || got[1] != 4 {
		t.Fatalf("want survivors {3,4} (newest), got %v", got)
	}
}

// An age cap drops dead letters older than the bound while keeping fresh ones.
func TestDLQRetentionAgeDropsOld(t *testing.T) {
	ctx := context.Background()
	e, ms := openWithDLQ(t, time.Hour.Milliseconds(), 0) // drop dead letters older than 1h
	mustQueue(t, e, "q", QueueConfig{DefaultTTLMs: 1000})

	sendBodies(t, e, "q", 3) // enqueued at T0
	advance(ms, 2*time.Second)
	e.RunMaintenanceOnce(ctx) // 3 dead-lettered, age ~0 -> under 1h, kept
	if mt, _ := e.Stats(ctx, "q"); mt.DeadLettered != 3 {
		t.Fatalf("fresh dead letters must be kept: got %d", mt.DeadLettered)
	}

	advance(ms, 2*time.Hour) // the 3 are now >1h old
	sendBodies(t, e, "q", 1) // a fresh message, enqueued at ~T0+2h
	advance(ms, 2*time.Second)
	e.RunMaintenanceOnce(ctx) // age purge drops the 3 old; the fresh one stays

	if mt, _ := e.Stats(ctx, "q"); mt.DeadLettered != 1 {
		t.Fatalf("age retention: want 1 (only the fresh dead letter), got %d", mt.DeadLettered)
	}
}

// With no bounds set (engine default), reapDLQ is a no-op — the DLQ is unbounded.
func TestDLQRetentionDisabledByDefault(t *testing.T) {
	ctx := context.Background()
	e, ms := openWithDLQ(t, 0, 0) // both bounds off
	mustQueue(t, e, "q", QueueConfig{DefaultTTLMs: 1000})
	sendBodies(t, e, "q", 5)
	advance(ms, 2*time.Second)
	e.RunMaintenanceOnce(ctx)
	if mt, _ := e.Stats(ctx, "q"); mt.DeadLettered != 5 {
		t.Fatalf("unbounded DLQ: want all 5 kept, got %d", mt.DeadLettered)
	}
}
