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
		SessionID:     "sess-A",
		CorrelationID: "corr-9",
		Subject:       "orders.created",
		ContentType:   "application/json",
		Properties:    map[string]string{"tenant": "acme", "trace": "abc-中文", "x": ""},
	}
	if _, err := e.Send(ctx, "q", in); err != nil {
		t.Fatal(err)
	}
	m := recvOne(t, e, "q")
	if m == nil {
		t.Fatal("no message")
	}
	if string(m.Body) != string(in.Body) {
		t.Errorf("body mismatch: %q != %q", m.Body, in.Body)
	}
	if m.MessageID != in.MessageID || m.SessionID != in.SessionID || m.CorrelationID != in.CorrelationID {
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
	if _, err := e.Send(ctx, "q", OutMessage{Body: []byte{}}); err != nil {
		t.Fatalf("empty body: %v", err)
	}
	if _, err := e.Send(ctx, "q", OutMessage{Body: nil}); err != nil {
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
	e.Send(ctx, "q", OutMessage{Body: body, Properties: props})
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

	if _, err := e.Send(ctx, "q", OutMessage{Body: make([]byte, 1024)}); err != nil {
		t.Fatalf("1024 bytes should pass: %v", err)
	}
	if _, err := e.Send(ctx, "q", OutMessage{Body: make([]byte, 1025)}); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("1025 bytes should be ErrMessageTooLarge, got %v", err)
	}
	// default cap is 1 MiB
	e2, _ := testEngine(t)
	mustQueue(t, e2, "q", QueueConfig{})
	if _, err := e2.Send(ctx, "q", OutMessage{Body: make([]byte, DefaultMaxMessageBytes+1)}); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("over default 1MiB should be rejected, got %v", err)
	}
	if _, err := e2.Send(ctx, "q", OutMessage{Body: make([]byte, DefaultMaxMessageBytes)}); err != nil {
		t.Fatalf("exactly 1MiB should pass: %v", err)
	}
}

// Abandon with a delay re-hides the message until the delay elapses (backoff).
func TestAbandonWithDelay(t *testing.T) {
	ctx := context.Background()
	e, msp := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{MaxDeliveryCount: 10})
	e.Send(ctx, "q", OutMessage{Body: []byte("x")})
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
	e.Send(ctx, "q", OutMessage{Body: []byte("perishable")})
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
	e.Send(ctx, "q", OutMessage{Body: []byte("perishable")})
	advance(msp, 2*time.Second)
	e.RunMaintenanceOnce(ctx)
	mt, _ := e.GetQueueMetrics(ctx, "q")
	if mt.Total != 0 {
		t.Fatalf("expired message should be discarded, got %+v", mt)
	}
}

// CancelScheduled prevents a future message from ever activating.
func TestCancelScheduled(t *testing.T) {
	ctx := context.Background()
	e, msp := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{})
	at := atomicNow(msp) + (5 * time.Second).Milliseconds()
	seq, _ := e.Schedule(ctx, "q", OutMessage{Body: []byte("x")}, at)
	if err := e.CancelScheduled(ctx, "q", seq); err != nil {
		t.Fatal(err)
	}
	advance(msp, 6*time.Second)
	e.RunMaintenanceOnce(ctx)
	if got := recvOne(t, e, "q"); got != nil {
		t.Fatal("cancelled scheduled message must not be delivered")
	}
	if err := e.CancelScheduled(ctx, "q", 99999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cancel of unknown seq should be ErrNotFound, got %v", err)
	}
}

// RenewLock extends the lease so the reaper does not reclaim mid-processing.
func TestRenewLockExtendsLease(t *testing.T) {
	ctx := context.Background()
	e, msp := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{LockDurationMs: 10_000})
	e.Send(ctx, "q", OutMessage{Body: []byte("x")})
	m := recvOne(t, e, "q")
	first := m.LockedUntilMs
	advance(msp, 5*time.Second)
	if err := e.RenewLock(ctx, "q", m.SeqNumber, m.LockToken); err != nil {
		t.Fatal(err)
	}
	pk, _ := e.Peek(ctx, "q", PeekOptions{State: StateLocked})
	if len(pk) != 1 || pk[0].LockedUntilMs <= first {
		t.Fatalf("renew should advance locked_until beyond %d, got %+v", first, pk)
	}
	// renew with a stale token fails safely.
	if err := e.RenewLock(ctx, "q", m.SeqNumber, "bad"); !errors.Is(err, ErrLockLost) {
		t.Fatalf("renew with bad token should be LockLost, got %v", err)
	}
}

// Dedup window expiry: the same key becomes a fresh message after the window.
func TestDedupWindowExpiry(t *testing.T) {
	ctx := context.Background()
	e, msp := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{DedupWindowMs: (10 * time.Minute).Milliseconds()})
	s1, _ := e.Send(ctx, "q", OutMessage{Body: []byte("a"), MessageID: "k"})
	s2, _ := e.Send(ctx, "q", OutMessage{Body: []byte("a"), MessageID: "k"})
	if s1 != s2 {
		t.Fatal("in-window duplicate should return original seq")
	}
	advance(msp, 11*time.Minute) // past the window
	s3, _ := e.Send(ctx, "q", OutMessage{Body: []byte("a"), MessageID: "k"})
	if s3 == s1 {
		t.Fatalf("after window expiry the key should produce a NEW message, got same seq %d", s3)
	}
	mt, _ := e.GetQueueMetrics(ctx, "q")
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
	e.Send(ctx, "q", OutMessage{Body: []byte("def")})
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
		e.Send(ctx, "q", OutMessage{Body: []byte(fmt.Sprintf("m%d", i))})
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

// atomicNow reads the test clock pointer.
func atomicNow(ms *int64) int64 { return atomic.LoadInt64(ms) }
