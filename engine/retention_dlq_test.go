package engine

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// openWithDLQ opens a deterministic (clock-injected, no background) engine with
// the DLQ retention bounds set, for the reapDLQ tests.
func openWithDLQ(t *testing.T, ageMs int64, count int, maxBytes int64) (*Engine, *int64) {
	t.Helper()
	var ms int64 = 1_700_000_000_000
	e, err := Open(context.Background(), Options{
		DB:                "file:" + filepath.Join(t.TempDir(), "mq.db"),
		Now:               func() int64 { return atomic.LoadInt64(&ms) },
		DisableBackground: true,
		DLQMaxAgeMs:       ageMs,
		DLQMaxCount:       count,
		DLQMaxBytes:       maxBytes,
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
	e, ms := openWithDLQ(t, 0, 2, 0) // keep at most 2 dead letters per queue
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

// A per-queue byte cap keeps the newest dead letters whose bodies fit, dropping the
// oldest until total body bytes are under the cap.
func TestDLQRetentionBytesDropsOldest(t *testing.T) {
	ctx := context.Background()
	e, ms := openWithDLQ(t, 0, 0, 250) // keep newest dead letters whose bodies sum <= 250B
	mustQueue(t, e, "q", QueueConfig{DefaultTTLMs: 1000})
	for i := 0; i < 5; i++ { // 100-byte bodies, first byte = index
		b := make([]byte, 100)
		b[0] = byte(i)
		if _, err := e.SendOne(ctx, "q", OutMessage{Body: b}); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	advance(ms, 2*time.Second)
	e.RunMaintenanceOnce(ctx) // 5 dead-lettered; byte cap keeps newest 2 (200B <= 250)
	if mt, _ := e.Stats(ctx, "q"); mt.DeadLettered != 2 {
		t.Fatalf("byte cap: want 2 (2*100B <= 250B), got %d", mt.DeadLettered)
	}
	if got := dlqBodies(t, e, "q"); len(got) != 2 || got[0] != 3 || got[1] != 4 {
		t.Fatalf("want survivors {3,4} (newest), got %v", got)
	}
}

// An age cap drops dead letters older than the bound while keeping fresh ones.
func TestDLQRetentionAgeDropsOld(t *testing.T) {
	ctx := context.Background()
	e, ms := openWithDLQ(t, time.Hour.Milliseconds(), 0, 0) // drop dead letters older than 1h
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
	e, ms := openWithDLQ(t, 0, 0, 0) // all bounds off
	mustQueue(t, e, "q", QueueConfig{DefaultTTLMs: 1000})
	sendBodies(t, e, "q", 5)
	advance(ms, 2*time.Second)
	e.RunMaintenanceOnce(ctx)
	if mt, _ := e.Stats(ctx, "q"); mt.DeadLettered != 5 {
		t.Fatalf("unbounded DLQ: want all 5 kept, got %d", mt.DeadLettered)
	}
}

// ─── Per-queue retention overrides (MQLITE-29) ──────────────────────────────

func TestEffectiveBound(t *testing.T) {
	cases := []struct{ perQueue, def, want int64 }{
		{0, 100, 100}, // 0 inherits the engine default
		{50, 100, 50}, // a positive override wins
		{200, 0, 200}, // override even when the default is off
		{-1, 100, 0},  // negative = explicitly unbounded (opt out)
		{0, 0, 0},     // both off
		{-1, 0, 0},    // unbounded over an off default
	}
	for _, c := range cases {
		if got := effectiveBound(c.perQueue, c.def); got != c.want {
			t.Errorf("effectiveBound(%d, %d) = %d, want %d", c.perQueue, c.def, got, c.want)
		}
	}
}

// A per-queue count bound applies even when the engine default is off, and a queue
// without an override stays unbounded.
func TestDLQRetentionPerQueueOverride(t *testing.T) {
	ctx := context.Background()
	e, ms := openWithDLQ(t, 0, 0, 0) // engine defaults OFF
	mustQueue(t, e, "capped", QueueConfig{DefaultTTLMs: 1000, DLQMaxCount: 2})
	mustQueue(t, e, "uncapped", QueueConfig{DefaultTTLMs: 1000}) // inherits off -> unbounded

	sendBodies(t, e, "capped", 5)
	sendBodies(t, e, "uncapped", 5)
	advance(ms, 2*time.Second)
	e.RunMaintenanceOnce(ctx)

	if mt, _ := e.Stats(ctx, "capped"); mt.DeadLettered != 2 {
		t.Fatalf("per-queue cap=2: want 2 dead-lettered, got %d", mt.DeadLettered)
	}
	if mt, _ := e.Stats(ctx, "uncapped"); mt.DeadLettered != 5 {
		t.Fatalf("no override inherits off (unbounded): want 5, got %d", mt.DeadLettered)
	}
	if got := dlqBodies(t, e, "capped"); len(got) != 2 || got[0] != 3 || got[1] != 4 {
		t.Fatalf("drop-oldest survivors {3,4}, got %v", got)
	}
}

// A per-queue bound of 0 inherits the engine default; -1 explicitly opts out.
func TestDLQRetentionPerQueueInheritAndOptOut(t *testing.T) {
	ctx := context.Background()
	e, ms := openWithDLQ(t, 0, 2, 0)                                             // engine default: keep 2
	mustQueue(t, e, "inherit", QueueConfig{DefaultTTLMs: 1000})                  // 0 -> inherit (2)
	mustQueue(t, e, "keepall", QueueConfig{DefaultTTLMs: 1000, DLQMaxCount: -1}) // -1 -> unbounded

	sendBodies(t, e, "inherit", 5)
	sendBodies(t, e, "keepall", 5)
	advance(ms, 2*time.Second)
	e.RunMaintenanceOnce(ctx)

	if mt, _ := e.Stats(ctx, "inherit"); mt.DeadLettered != 2 {
		t.Fatalf("inherit engine default (2): want 2, got %d", mt.DeadLettered)
	}
	if mt, _ := e.Stats(ctx, "keepall"); mt.DeadLettered != 5 {
		t.Fatalf("per-queue -1 overrides default to unbounded: want 5, got %d", mt.DeadLettered)
	}
}

// A per-queue age bound is applied per queue independent of the engine default.
func TestDLQRetentionPerQueueAge(t *testing.T) {
	ctx := context.Background()
	e, ms := openWithDLQ(t, 0, 0, 0) // engine defaults off
	mustQueue(t, e, "q", QueueConfig{DefaultTTLMs: 1000, DLQMaxAgeMs: time.Hour.Milliseconds()})

	sendBodies(t, e, "q", 3) // dead-lettered at ~T0
	advance(ms, 2*time.Second)
	e.RunMaintenanceOnce(ctx) // fresh (<1h) -> kept
	if mt, _ := e.Stats(ctx, "q"); mt.DeadLettered != 3 {
		t.Fatalf("fresh dead letters kept: got %d", mt.DeadLettered)
	}
	advance(ms, 2*time.Hour) // the 3 are now >1h old
	sendBodies(t, e, "q", 1)
	advance(ms, 2*time.Second)
	e.RunMaintenanceOnce(ctx) // per-queue age drops the 3 old; fresh one stays
	if mt, _ := e.Stats(ctx, "q"); mt.DeadLettered != 1 {
		t.Fatalf("per-queue age bound: want 1, got %d", mt.DeadLettered)
	}
}

// Round-3 §3.2: a DLQ that is UNDER its cap has nothing to purge — the cutoff query returns
// sql.ErrNoRows, which is the normal steady state, not a failure. It used to be logged as an
// ERROR, so a perfectly healthy broker cried wolf every 60 seconds; with the default cap of a
// million dead letters that is very nearly every broker.
//
// Captures the retention loop's log for BOTH bounds (count and bytes) and for both an empty and
// a populated-but-under-cap DLQ, and requires silence.
func TestDLQRetentionUnderCapLogsNoError(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		name         string
		count        int
		maxBytes     int64
		deadLettered int
	}{
		{"count bound, empty DLQ", 100, 0, 0},
		{"count bound, under cap", 100, 0, 3},
		{"bytes bound, empty DLQ", 0, 1 << 20, 0},
		{"bytes bound, under cap", 0, 1 << 20, 3},
		{"both bounds, under cap", 100, 1 << 20, 3},
	} {
		t.Run(c.name, func(t *testing.T) {
			var logbuf bytes.Buffer
			var ms int64 = 1_700_000_000_000
			e, err := Open(ctx, Options{
				DB:                "file:" + filepath.Join(t.TempDir(), "mq.db"),
				Now:               func() int64 { return atomic.LoadInt64(&ms) },
				DisableBackground: true,
				DLQMaxCount:       c.count,
				DLQMaxBytes:       c.maxBytes,
				Logger:            slog.New(slog.NewTextHandler(&logbuf, &slog.HandlerOptions{Level: slog.LevelDebug})),
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Close() })
			mustQueue(t, e, "q", QueueConfig{DefaultTTLMs: 1000})
			if c.deadLettered > 0 {
				sendBodies(t, e, "q", c.deadLettered)
				advance(&ms, 2*time.Second) // TTL dead-letters them
			}

			e.RunMaintenanceOnce(ctx)
			e.RunMaintenanceOnce(ctx) // a second pass: the reviewer saw this repeat every minute

			if m, _ := e.Stats(ctx, "q"); m.DeadLettered != int64(c.deadLettered) {
				t.Errorf("an under-cap DLQ must not be purged: dead_lettered=%d, want %d", m.DeadLettered, c.deadLettered)
			}
			if out := logbuf.String(); strings.Contains(out, "level=ERROR") {
				t.Errorf("an under-cap DLQ logged an ERROR (it has simply nothing to purge):\n%s", out)
			}
		})
	}
}
