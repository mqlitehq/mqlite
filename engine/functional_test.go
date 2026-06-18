package engine

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Every application-set field survives a send -> receive round-trip.
func TestAllFieldsRoundTrip(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{})

	in := OutMessage{
		Body:          []byte("the body \x00\x01 with NULs"),
		MessageID:     "msg-123",
		GroupID:       "sess-A",
		CorrelationID: "corr-9",
		Subject:       "orders.created",
		ContentType:   "application/json",
		Properties:    map[string]string{"tenant": "acme", "trace": "abc-中文", "x": ""},
	}
	if _, err := e.SendOne(ctx, "q", in); err != nil {
		t.Fatal(err)
	}
	m := recvOne(t, e, "q")
	if m == nil {
		t.Fatal("no message")
	}
	if string(m.Body) != string(in.Body) {
		t.Errorf("body mismatch: %q != %q", m.Body, in.Body)
	}
	if m.MessageID != in.MessageID || m.GroupID != in.GroupID || m.CorrelationID != in.CorrelationID {
		t.Errorf("id/session/correlation mismatch: %+v", m)
	}
	if m.Subject != in.Subject || m.ContentType != in.ContentType {
		t.Errorf("subject/content_type mismatch: %+v", m)
	}
	if !reflect.DeepEqual(m.Properties, in.Properties) {
		t.Errorf("properties mismatch: %#v != %#v", m.Properties, in.Properties)
	}
}

// Empty and nil bodies are valid (NOT NULL column coerces nil -> empty blob).
func TestEmptyAndNilBody(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{})
	if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte{}}); err != nil {
		t.Fatalf("empty body: %v", err)
	}
	if _, err := e.SendOne(ctx, "q", OutMessage{Body: nil}); err != nil {
		t.Fatalf("nil body: %v", err)
	}
	for i := 0; i < 2; i++ {
		m := recvOne(t, e, "q")
		if m == nil || len(m.Body) != 0 {
			t.Fatalf("expected empty body, got %+v", m)
		}
	}
}

// Unicode / binary payloads and property values survive intact.
func TestUnicodeAndBinary(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{})
	body := []byte("emoji 🚀 中文 \xff\xfe binary")
	props := map[string]string{"键": "值🚀", "emoji": "😀"}
	e.SendOne(ctx, "q", OutMessage{Body: body, Properties: props})
	m := recvOne(t, e, "q")
	if string(m.Body) != string(body) {
		t.Errorf("unicode/binary body corrupted")
	}
	if !reflect.DeepEqual(m.Properties, props) {
		t.Errorf("unicode properties corrupted: %#v", m.Properties)
	}
}

// Max message size: at the limit succeeds, one byte over is rejected.
func TestMaxMessageSize(t *testing.T) {
	ctx := context.Background()
	var ms int64 = 1_700_000_000_000
	e, err := Open(ctx, Options{
		DB:                "file:" + filepath.Join(t.TempDir(), "mq.db"),
		Now:               func() int64 { return ms },
		DisableBackground: true,
		MaxMessageBytes:   1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	mustQueue(t, e, "q", QueueConfig{})

	if _, err := e.SendOne(ctx, "q", OutMessage{Body: make([]byte, 1024)}); err != nil {
		t.Fatalf("1024 bytes should pass: %v", err)
	}
	if _, err := e.SendOne(ctx, "q", OutMessage{Body: make([]byte, 1025)}); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("1025 bytes should be ErrMessageTooLarge, got %v", err)
	}
	// default cap is 1 MiB
	e2, _ := testEngine(t)
	mustQueue(t, e2, "q", QueueConfig{})
	if _, err := e2.SendOne(ctx, "q", OutMessage{Body: make([]byte, DefaultMaxMessageBytes+1)}); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("over default 1MiB should be rejected, got %v", err)
	}
	if _, err := e2.SendOne(ctx, "q", OutMessage{Body: make([]byte, DefaultMaxMessageBytes)}); err != nil {
		t.Fatalf("exactly 1MiB should pass: %v", err)
	}
}

// Abandon with a delay re-hides the message until the delay elapses (backoff).
func TestAbandonWithDelay(t *testing.T) {
	ctx := context.Background()
	e, msp := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{MaxDeliveryCount: 10})
	e.SendOne(ctx, "q", OutMessage{Body: []byte("x")})
	m := recvOne(t, e, "q")
	if err := e.Abandon(ctx, "q", m.SeqNumber, m.LockToken, (2 * time.Second).Milliseconds()); err != nil {
		t.Fatal(err)
	}
	if got := recvOne(t, e, "q"); got != nil {
		t.Fatal("message should be hidden during backoff delay")
	}
	advance(msp, 3*time.Second)
	if got := recvOne(t, e, "q"); got == nil {
		t.Fatal("message should reappear after backoff delay")
	}
}

// TTL expiry routes to the DLQ (dead_letter_on_expire=1) with reason TTLExpired.
func TestTTLToDeadLetter(t *testing.T) {
	ctx := context.Background()
	e, msp := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{DefaultTTLMs: 1000})
	e.SendOne(ctx, "q", OutMessage{Body: []byte("perishable")})
	advance(msp, 2*time.Second)
	e.RunMaintenanceOnce(ctx)
	if got := recvOne(t, e, "q"); got != nil {
		t.Fatal("expired message must not be deliverable")
	}
	pk, _ := e.Peek(ctx, "q", PeekOptions{State: StateDeadLettered})
	if len(pk) != 1 || pk[0].DeadLetterReason != ReasonTTLExpired {
		t.Fatalf("expected 1 DLQ msg with TTLExpired, got %+v", pk)
	}
}

// TTL expiry discards when dead_letter_on_expire=0.
func TestTTLDiscard(t *testing.T) {
	ctx := context.Background()
	e, msp := testEngine(t)
	no := false
	mustQueue(t, e, "q", QueueConfig{DefaultTTLMs: 1000, DeadLetterOnExpire: &no})
	e.SendOne(ctx, "q", OutMessage{Body: []byte("perishable")})
	advance(msp, 2*time.Second)
	e.RunMaintenanceOnce(ctx)
	mt, _ := e.Stats(ctx, "q")
	if mt.Total != 0 {
		t.Fatalf("expired message should be discarded, got %+v", mt)
	}
}

// Cancel prevents a future scheduled message from ever activating.
func TestCancelScheduled(t *testing.T) {
	ctx := context.Background()
	e, msp := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{})
	at := atomicNow(msp) + (5 * time.Second).Milliseconds()
	seq, _ := e.Schedule(ctx, "q", OutMessage{Body: []byte("x")}, at)
	if err := e.Cancel(ctx, "q", seq); err != nil {
		t.Fatal(err)
	}
	advance(msp, 6*time.Second)
	e.RunMaintenanceOnce(ctx)
	if got := recvOne(t, e, "q"); got != nil {
		t.Fatal("cancelled scheduled message must not be delivered")
	}
	if err := e.Cancel(ctx, "q", 99999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cancel of unknown seq should be ErrNotFound, got %v", err)
	}
}

// RenewLock extends the lease so the reaper does not reclaim mid-processing.
func TestRenewLockExtendsLease(t *testing.T) {
	ctx := context.Background()
	e, msp := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{LockDurationMs: 10_000})
	e.SendOne(ctx, "q", OutMessage{Body: []byte("x")})
	m := recvOne(t, e, "q")
	first := m.LockedUntilMs
	advance(msp, 5*time.Second)
	if err := e.Renew(ctx, "q", m.SeqNumber, m.LockToken); err != nil {
		t.Fatal(err)
	}
	pk, _ := e.Peek(ctx, "q", PeekOptions{State: StateLocked})
	if len(pk) != 1 || pk[0].LockedUntilMs <= first {
		t.Fatalf("renew should advance locked_until beyond %d, got %+v", first, pk)
	}
	// renew with a stale token fails safely.
	if err := e.Renew(ctx, "q", m.SeqNumber, "bad"); !errors.Is(err, ErrLockLost) {
		t.Fatalf("renew with bad token should be LockLost, got %v", err)
	}
}

// Dedup window expiry: the same key becomes a fresh message after the window.
func TestDedupWindowExpiry(t *testing.T) {
	ctx := context.Background()
	e, msp := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{DedupWindowMs: (10 * time.Minute).Milliseconds()})
	s1, _ := e.SendOne(ctx, "q", OutMessage{Body: []byte("a"), MessageID: "k"})
	s2, _ := e.SendOne(ctx, "q", OutMessage{Body: []byte("a"), MessageID: "k"})
	if s1 != s2 {
		t.Fatal("in-window duplicate should return original seq")
	}
	advance(msp, 11*time.Minute) // past the window
	s3, _ := e.SendOne(ctx, "q", OutMessage{Body: []byte("a"), MessageID: "k"})
	if s3 == s1 {
		t.Fatalf("after window expiry the key should produce a NEW message, got same seq %d", s3)
	}
	mt, _ := e.Stats(ctx, "q")
	if mt.Active != 2 {
		t.Fatalf("expected 2 active after window expiry, got %d", mt.Active)
	}
}

// Deferred and scheduled messages must be discoverable via Peek (§8.7).
func TestDeferredScheduledPeekable(t *testing.T) {
	ctx := context.Background()
	e, msp := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{})
	e.Schedule(ctx, "q", OutMessage{Body: []byte("sched")}, atomicNow(msp)+100000)
	e.SendOne(ctx, "q", OutMessage{Body: []byte("def")})
	m := recvOne(t, e, "q")
	e.Defer(ctx, "q", m.SeqNumber, m.LockToken)

	sch, _ := e.Peek(ctx, "q", PeekOptions{State: StateScheduled})
	def, _ := e.Peek(ctx, "q", PeekOptions{State: StateDeferred})
	if len(sch) != 1 || len(def) != 1 {
		t.Fatalf("scheduled and deferred must be peekable: sched=%d def=%d", len(sch), len(def))
	}
}

// Competing consumers: every message delivered exactly once, no double-delivery.
func TestCompetingConsumersNoDouble(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{LockDurationMs: 60_000, MaxDeliveryCount: 100})
	const n = 500
	for i := 0; i < n; i++ {
		e.SendOne(ctx, "q", OutMessage{Body: []byte(fmt.Sprintf("m%d", i))})
	}
	var mu sync.Mutex
	seen := map[int64]int{}
	var completed int64
	var wg sync.WaitGroup
	for c := 0; c < 6; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				msgs, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 8})
				if err != nil || len(msgs) == 0 {
					if atomic.LoadInt64(&completed) >= n {
						return
					}
					continue
				}
				for _, m := range msgs {
					mu.Lock()
					seen[m.SeqNumber]++
					mu.Unlock()
					e.Complete(ctx, "q", m.SeqNumber, m.LockToken)
					atomic.AddInt64(&completed, 1)
				}
			}
		}()
	}
	wg.Wait()
	if completed != n {
		t.Fatalf("expected %d completed, got %d", n, completed)
	}
	for seq, cnt := range seen {
		if cnt != 1 {
			t.Fatalf("seq %d delivered %d times (double delivery)", seq, cnt)
		}
	}
}

// Topic/subscription isolation: a subscription's backing queue is the bare
// subscription name, so reusing a name across topics (or against a plain queue)
// must be rejected rather than silently merging two delivery targets into one
// queue (eval report r2 §architecture / P0-3). Same (topic,name) stays idempotent.
func TestTopicSubscriptionIsolation(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)

	// First subscription under "orders" creates backing queue "processor".
	if err := e.Subscribe(ctx, "orders", "processor", nil); err != nil {
		t.Fatalf("first subscription: %v", err)
	}
	// Re-creating the same (topic, name) is idempotent, not an error.
	if err := e.Subscribe(ctx, "orders", "processor", nil); err != nil {
		t.Fatalf("idempotent re-create should succeed: %v", err)
	}
	// Same name under a DIFFERENT topic is rejected (would alias the backing queue).
	if err := e.Subscribe(ctx, "payments", "processor", nil); !errors.Is(err, ErrNameConflict) {
		t.Fatalf("cross-topic duplicate name should be ErrNameConflict, got %v", err)
	}
	// The rejected subscription must not have registered "payments" as a topic.
	if _, err := e.SendOne(ctx, "payments", OutMessage{Body: []byte("x")}); !errors.Is(err, ErrQueueNotFound) {
		t.Fatalf("payments must not exist after rejected subscription, got %v", err)
	}
	// A subscription name clashing with an existing plain queue is rejected too.
	mustQueue(t, e, "plainq", QueueConfig{})
	if err := e.Subscribe(ctx, "topicX", "plainq", nil); !errors.Is(err, ErrNameConflict) {
		t.Fatalf("subscription name clashing with a queue should be ErrNameConflict, got %v", err)
	}

	// Distinct names under distinct topics stay fully isolated.
	if err := e.Subscribe(ctx, "orders", "orders_audit", nil); err != nil {
		t.Fatal(err)
	}
	if err := e.Subscribe(ctx, "payments", "payments_audit", nil); err != nil {
		t.Fatal(err)
	}
	e.SendOne(ctx, "orders", OutMessage{Body: []byte("o")})
	e.SendOne(ctx, "payments", OutMessage{Body: []byte("p")})
	oa, _ := e.Stats(ctx, "orders_audit")
	pa, _ := e.Stats(ctx, "payments_audit")
	proc, _ := e.Stats(ctx, "processor")
	if oa.Active != 1 || pa.Active != 1 {
		t.Fatalf("each topic's own subscription gets exactly its message: orders_audit=%d payments_audit=%d", oa.Active, pa.Active)
	}
	if proc.Active != 1 { // processor subscribes to "orders" only
		t.Fatalf("processor (orders) should have 1, got %d", proc.Active)
	}
}

// Concurrent claim with mixed session/no-session messages (eval report r2 P0-4 TCK):
// no-session messages are claimed in parallel by competing consumers, while each
// session group is delivered strictly FIFO and groups never block one another.
func TestConcurrentSessionClaimIsolation(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{LockDurationMs: 60_000, MaxDeliveryCount: 100})

	const noSession = 12
	for i := 0; i < noSession; i++ {
		if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte(fmt.Sprintf("free-%d", i))}); err != nil {
			t.Fatal(err)
		}
	}
	groups := map[string]int{"A": 3, "B": 2, "C": 2}
	total := noSession
	for g, n := range groups {
		for i := 0; i < n; i++ {
			if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte(fmt.Sprintf("%s-%d", g, i)), GroupID: g}); err != nil {
				t.Fatal(err)
			}
			total++
		}
	}

	var mu sync.Mutex
	seen := map[string]int{}    // body -> delivery count
	order := map[string][]int{} // session -> claim order (parsed indices)
	var completed int64

	var wg sync.WaitGroup
	for c := 0; c < 6; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for atomic.LoadInt64(&completed) < int64(total) {
				msgs, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 2})
				if err != nil {
					t.Errorf("receive: %v", err)
					return
				}
				for _, m := range msgs {
					mu.Lock()
					seen[string(m.Body)]++
					if m.GroupID != "" {
						var idx int
						fmt.Sscanf(string(m.Body), m.GroupID+"-%d", &idx)
						order[m.GroupID] = append(order[m.GroupID], idx)
					}
					mu.Unlock()
					if err := e.Complete(ctx, "q", m.SeqNumber, m.LockToken); err != nil {
						t.Errorf("complete: %v", err)
						return
					}
					atomic.AddInt64(&completed, 1)
				}
			}
		}()
	}
	wg.Wait()

	if completed != int64(total) {
		t.Fatalf("expected %d completed, got %d", total, completed)
	}
	if len(seen) != total {
		t.Fatalf("expected %d distinct messages, got %d", total, len(seen))
	}
	for body, cnt := range seen {
		if cnt != 1 {
			t.Fatalf("message %q delivered %d times (double delivery)", body, cnt)
		}
	}
	// Each session group was claimed strictly in FIFO order (0,1,2,...).
	for g, n := range groups {
		got := order[g]
		if len(got) != n {
			t.Fatalf("session %s: expected %d messages, got %v", g, n, got)
		}
		for i, idx := range got {
			if idx != i {
				t.Fatalf("session %s claimed out of FIFO order: %v", g, got)
			}
		}
	}
}

// atomicNow reads the test clock pointer.
func atomicNow(ms *int64) int64 { return atomic.LoadInt64(ms) }
