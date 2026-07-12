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
		ReplyTo:       "reply.queue.A",
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
	if m.ReplyTo != in.ReplyTo {
		t.Errorf("reply_to mismatch: %q != %q", m.ReplyTo, in.ReplyTo)
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

// A message that expires while STILL in state 'scheduled' is dead-lettered by the
// TTL pass, same as active/locked/deferred (MQLITE-61) — both TTL branches cover
// the same state set. TTL is anchored at visible_at (§8.7), so this state is only
// reachable when the scheduler lags past visible_at + ttl: a paused/stopped broker
// (downtime longer than the ttl) or the 1s scheduler cadence with a sub-second ttl.
// The TTL pass is called directly (white-box) because RunMaintenanceOnce runs the
// scheduler first, which would activate the row and mask the scheduled branch.
func TestTTLScheduledToDeadLetter(t *testing.T) {
	ctx := context.Background()
	e, msp := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{DefaultTTLMs: 1000})
	// visible_at = now+1s, expires_at = visible_at + 1s (queue default) = now+2s.
	if _, err := e.Schedule(ctx, "q", OutMessage{Body: []byte("stale")}, e.now()+1000); err != nil {
		t.Fatal(err)
	}
	advance(msp, 3*time.Second) // both lapsed; no scheduler ran (downtime simulation)
	e.expireTTL(ctx)
	pk, _ := e.Peek(ctx, "q", PeekOptions{State: StateDeadLettered})
	if len(pk) != 1 || pk[0].DeadLetterReason != ReasonTTLExpired {
		t.Fatalf("scheduled message must dead-letter once expired, got %+v", pk)
	}
	// The scheduler afterwards must not resurrect it.
	e.RunMaintenanceOnce(ctx)
	if got := recvOne(t, e, "q"); got != nil {
		t.Fatalf("dead-lettered row must not be deliverable, got %q", got.Body)
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

// TTL cap: a per-message TTL is honored but the queue default is a ceiling (Azure Service
// Bus DefaultMessageTimeToLive semantics). Also exercises Peek surfacing expires_at.
func TestTTLCapAtQueueDefault(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	now := e.now()
	mustQueue(t, e, "q", QueueConfig{DefaultTTLMs: 3_600_000}) // 1h ceiling

	expiresOf := func(seq int64) int64 {
		t.Helper()
		pk, _ := e.Peek(ctx, "q", PeekOptions{Max: 1000})
		for _, p := range pk {
			if p.SeqNumber == seq {
				return p.ExpiresAtMs
			}
		}
		t.Fatalf("seq %d not found", seq)
		return 0
	}

	long, _ := e.SendOne(ctx, "q", OutMessage{Body: []byte("a"), TTLMs: 2 * 3_600_000}) // 2h > 1h
	if got := expiresOf(long); got != now+3_600_000 {
		t.Errorf("over-long TTL: expires=%d, want capped to now+1h=%d", got, now+3_600_000)
	}
	short, _ := e.SendOne(ctx, "q", OutMessage{Body: []byte("b"), TTLMs: 600_000}) // 10m < 1h
	if got := expiresOf(short); got != now+600_000 {
		t.Errorf("short TTL: expires=%d, want honored now+10m=%d", got, now+600_000)
	}
	deflt, _ := e.SendOne(ctx, "q", OutMessage{Body: []byte("c")}) // none → queue default
	if got := expiresOf(deflt); got != now+3_600_000 {
		t.Errorf("no msg TTL: expires=%d, want queue default now+1h=%d", got, now+3_600_000)
	}
}

// A queue with no default TTL does not cap a per-message TTL; Peek returns 0 when no TTL.
func TestTTLNoQueueDefault(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	now := e.now()
	mustQueue(t, e, "q", QueueConfig{}) // unlimited

	withTTL, _ := e.SendOne(ctx, "q", OutMessage{Body: []byte("x"), TTLMs: 5 * 60_000})
	noTTL, _ := e.SendOne(ctx, "q", OutMessage{Body: []byte("y")})
	pk, _ := e.Peek(ctx, "q", PeekOptions{Max: 10})
	exp := map[int64]int64{}
	for _, p := range pk {
		exp[p.SeqNumber] = p.ExpiresAtMs
	}
	if exp[withTTL] != now+5*60_000 {
		t.Errorf("uncapped TTL: expires=%d, want now+5m=%d", exp[withTTL], now+5*60_000)
	}
	if exp[noTTL] != 0 {
		t.Errorf("no TTL: expires=%d, want 0", exp[noTTL])
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

// ─── seq allocation: never reused (MQLITE-71) ───────────────────────────────

// After the highest message is deleted, the next insert must NOT reuse its seq: without
// AUTOINCREMENT SQLite recycles a freed max rowid, which let a stale handle alias a later
// message. The id must be strictly greater.
func TestSeqNumberNeverReused(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{})

	first, err := e.SendOne(ctx, "q", OutMessage{Body: []byte("first")})
	if err != nil {
		t.Fatal(err)
	}
	m := recvOne(t, e, "q")
	if err := e.Complete(ctx, "q", m.SeqNumber, m.LockToken); err != nil {
		t.Fatal(err)
	}
	// a full VACUUM must not rebase the AUTOINCREMENT high-water either.
	if err := e.Compact(ctx, true); err != nil {
		t.Fatal(err)
	}
	// the table is empty again; the next send must get a strictly greater seq, not `first`.
	second, err := e.SendOne(ctx, "q", OutMessage{Body: []byte("second")})
	if err != nil {
		t.Fatal(err)
	}
	if second <= first {
		t.Fatalf("seq reused/regressed after drain+vacuum: first=%d second=%d (want second > first)", first, second)
	}
}

// The never-reuse guarantee must survive a broker restart: the AUTOINCREMENT high-water
// lives in the DB (sqlite_sequence), so a stale Cancel replayed after a bounce still can't
// alias a message scheduled post-restart.
func TestSeqHighWaterSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "mq.db")
	e, msp := testEngineAt(t, path)
	mustQueue(t, e, "q", QueueConfig{})
	at := atomicNow(msp) + time.Hour.Milliseconds()

	seqA, err := e.Schedule(ctx, "q", OutMessage{Body: []byte("A")}, at)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Cancel(ctx, "q", seqA); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	// reopen the same file — a fresh broker process.
	e2, msp2 := testEngineAt(t, path)
	at2 := atomicNow(msp2) + time.Hour.Milliseconds()
	seqB, err := e2.Schedule(ctx, "q", OutMessage{Body: []byte("B")}, at2)
	if err != nil {
		t.Fatal(err)
	}
	if seqB <= seqA {
		t.Fatalf("seq high-water not durable across reopen: A=%d B=%d (want B > A)", seqA, seqB)
	}
	// a Cancel(seqA) replayed after the restart must not touch B.
	if err := e2.Cancel(ctx, "q", seqA); !errors.Is(err, ErrNotFound) {
		t.Fatalf("post-restart stale Cancel(%d) should be ErrNotFound, got %v", seqA, err)
	}
	pk, err := e2.Peek(ctx, "q", PeekOptions{State: StateScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if len(pk) != 1 || pk[0].SeqNumber != seqB {
		t.Fatalf("message B must survive the post-restart stale cancel; peek=%+v", pk)
	}
}

// The B03 data-loss scenario: a stale Cancel of a former seq must not delete a later
// message. With AUTOINCREMENT the freed id is never reused, so B gets a fresh seq and the
// replayed Cancel(seqA) hits no row.
func TestStaleCancelDoesNotDeleteLaterMessage(t *testing.T) {
	ctx := context.Background()
	e, msp := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{})
	at := atomicNow(msp) + time.Hour.Milliseconds()

	seqA, err := e.Schedule(ctx, "q", OutMessage{Body: []byte("A")}, at)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Cancel(ctx, "q", seqA); err != nil {
		t.Fatal(err)
	}
	seqB, err := e.Schedule(ctx, "q", OutMessage{Body: []byte("B")}, at)
	if err != nil {
		t.Fatal(err)
	}
	if seqB == seqA {
		t.Fatalf("seq reused: A and B both got %d", seqA)
	}
	// replay the stale Cancel(seqA): it must be a no-op (row gone), never delete B.
	if err := e.Cancel(ctx, "q", seqA); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stale Cancel(%d) should be ErrNotFound, got %v", seqA, err)
	}
	pk, err := e.Peek(ctx, "q", PeekOptions{State: StateScheduled})
	if err != nil {
		t.Fatal(err)
	}
	if len(pk) != 1 || pk[0].SeqNumber != seqB || string(pk[0].Body) != "B" {
		t.Fatalf("message B must survive the stale cancel; peek=%+v", pk)
	}
}

// A scheduled multi-message batch is atomic: a mid-batch failure rolls back the whole batch,
// never leaving earlier items scheduled (MQLITE-72).
func TestScheduleBatchAtomic(t *testing.T) {
	ctx := context.Background()
	e, msp := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{Ordering: OrderGroupFIFO})
	at := atomicNow(msp) + time.Hour.Milliseconds()

	// item 2 lacks a group_id → group_fifo requires it → the whole batch must roll back.
	_, err := e.ScheduleBatch(ctx, "q", []OutMessage{
		{Body: []byte("a"), GroupID: "g1"},
		{Body: []byte("b")},
	}, at)
	if !errors.Is(err, ErrGroupRequired) {
		t.Fatalf("want ErrGroupRequired, got %v", err)
	}
	if pk, _ := e.Peek(ctx, "q", PeekOptions{State: StateScheduled}); len(pk) != 0 {
		t.Fatalf("failed scheduled batch must leave nothing; got %d scheduled", len(pk))
	}

	// a valid batch schedules all of them in one transaction.
	seqs, err := e.ScheduleBatch(ctx, "q", []OutMessage{
		{Body: []byte("a"), GroupID: "g1"},
		{Body: []byte("b"), GroupID: "g2"},
	}, at)
	if err != nil || len(seqs) != 2 {
		t.Fatalf("valid batch: seqs=%v err=%v", seqs, err)
	}
	if pk, _ := e.Peek(ctx, "q", PeekOptions{State: StateScheduled}); len(pk) != 2 {
		t.Fatalf("valid batch should schedule 2, got %d", len(pk))
	}
}

// Re-subscribing to update a filter must NOT reset the backing queue's config (MQLITE-73):
// only the mapping/filter changes; lock/delivery/etc. set on the backing queue survive.
func TestSubscribeFilterUpdatePreservesQueueConfig(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)

	if err := e.Subscribe(ctx, "events", "sub", &Filter{Expr: `subject == "a"`}); err != nil {
		t.Fatal(err)
	}
	// give the backing queue non-default config (the documented reconfigure path).
	on := true
	if err := e.CreateQueue(ctx, "sub", QueueConfig{
		Kind: "subscription", LockDurationMs: 60000, MaxDeliveryCount: 25, DeadLetterOnExpire: &on,
	}); err != nil {
		t.Fatal(err)
	}
	// update the filter by re-subscribing — must leave the queue config untouched.
	if err := e.Subscribe(ctx, "events", "sub", &Filter{Expr: `subject == "b"`}); err != nil {
		t.Fatal(err)
	}
	q, err := e.loadQueue(ctx, "sub")
	if err != nil {
		t.Fatal(err)
	}
	if q.lockDurationMs != 60000 || q.maxDeliveryCount != 25 {
		t.Fatalf("filter update reset backing queue config: lock=%d maxdc=%d (want 60000/25)",
			q.lockDurationMs, q.maxDeliveryCount)
	}
}

// Receive stops claiming once the response reaches the byte budget, and only locks the
// messages it returns — the rest stay active and nothing is lost (MQLITE-80).
func TestReceiveRespectsByteBudget(t *testing.T) {
	old := maxResponseBytes
	maxResponseBytes = 4096
	defer func() { maxResponseBytes = old }()

	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{})
	body := make([]byte, 2048) // ~2 KiB each; the 4 KiB budget stops the batch early
	for i := 0; i < 5; i++ {
		if _, err := e.SendOne(ctx, "q", OutMessage{Body: body}); err != nil {
			t.Fatal(err)
		}
	}

	got, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 || len(got) >= 5 {
		t.Fatalf("byte budget should cap the batch below 5; got %d", len(got))
	}
	// Drain the rest: every message must still be claimable — nothing was claimed-then-dropped.
	total := 0
	for _, batch := range [][]*Message{got} {
		for _, m := range batch {
			if m.LockToken == "" {
				t.Fatalf("returned message %d must be locked", m.SeqNumber)
			}
			if err := e.Complete(ctx, "q", m.SeqNumber, m.LockToken); err != nil {
				t.Fatal(err)
			}
			total++
		}
	}
	for {
		more, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 5})
		if err != nil {
			t.Fatal(err)
		}
		if len(more) == 0 {
			break
		}
		for _, m := range more {
			if err := e.Complete(ctx, "q", m.SeqNumber, m.LockToken); err != nil {
				t.Fatal(err)
			}
			total++
		}
	}
	if total != 5 {
		t.Fatalf("draining must retrieve all 5 messages (none lost/dropped); got %d", total)
	}
}

// Peek bounds its response by the byte budget and pages past it with FromSeq (MQLITE-80).
func TestPeekRespectsByteBudget(t *testing.T) {
	old := maxResponseBytes
	maxResponseBytes = 4096
	defer func() { maxResponseBytes = old }()

	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{})
	body := make([]byte, 2048)
	for i := 0; i < 5; i++ {
		if _, err := e.SendOne(ctx, "q", OutMessage{Body: body}); err != nil {
			t.Fatal(err)
		}
	}

	pk, err := e.Peek(ctx, "q", PeekOptions{Max: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(pk) == 0 || len(pk) >= 5 {
		t.Fatalf("peek byte budget should cap below 5; got %d", len(pk))
	}
	// FromSeq paging returns the remaining messages.
	pk2, err := e.Peek(ctx, "q", PeekOptions{Max: 5, FromSeq: pk[len(pk)-1].SeqNumber + 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(pk2) == 0 {
		t.Fatal("FromSeq paging should return the messages past the first budgeted page")
	}
}

// A single message larger than the whole budget must still be delivered — Receive returns
// and locks it, Peek returns it — otherwise it would be stuck forever. Golden-pins the
// append-then-break ordering (a refactor to "check budget before claiming" would strand it).
func TestSingleOverBudgetMessageStillDelivered(t *testing.T) {
	old := maxResponseBytes
	maxResponseBytes = 1024
	defer func() { maxResponseBytes = old }()

	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{})
	if _, err := e.SendOne(ctx, "q", OutMessage{Body: make([]byte, 2048)}); err != nil { // one body > budget
		t.Fatal(err)
	}
	pk, err := e.Peek(ctx, "q", PeekOptions{Max: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(pk) != 1 {
		t.Fatalf("Peek must return the single oversized message; got %d", len(pk))
	}
	got, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].LockToken == "" {
		t.Fatalf("Receive must return and lock the single oversized message; got %+v", got)
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

// Queue, subscription and topic names are ONE disjoint namespace (MQLITE-57).
// resolveTargets resolves a subscribed name as a topic FIRST, so before the
// symmetric guards a topic could shadow a live queue and every Send for that
// name silently rerouted to the fan-out (review F2) — and, in reverse, a queue
// created under a live topic's name could never be reached by Send. Conflicts
// must fail loud (ErrNameConflict, HTTP 409) at creation time, in BOTH
// directions, including against subscription backing queues and the degenerate
// self-reference. Legal upserts (same-name queue reconfig, same (topic,name)
// re-subscribe) stay open.
func TestTopicQueueNamespaceDisjoint(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)

	// Forward guard: a live plain queue's name cannot become a topic.
	mustQueue(t, e, "orders", QueueConfig{})
	if err := e.Subscribe(ctx, "orders", "s1", nil); !errors.Is(err, ErrNameConflict) {
		t.Fatalf("topic shadowing a live queue must be ErrNameConflict, got %v", err)
	}
	// ...and Send still reaches the queue afterwards (nothing was half-created).
	if _, err := e.SendOne(ctx, "orders", OutMessage{Body: []byte("x")}); err != nil {
		t.Fatalf("send to the queue after the rejected subscribe: %v", err)
	}
	if m := recvOne(t, e, "orders"); m == nil || string(m.Body) != "x" {
		t.Fatalf("queue must still receive its messages, got %+v", m)
	}

	// Reverse guard: a live topic's name cannot become a queue.
	if err := e.Subscribe(ctx, "billing", "billing_sub", nil); err != nil {
		t.Fatal(err)
	}
	if err := e.CreateQueue(ctx, "billing", QueueConfig{}); !errors.Is(err, ErrNameConflict) {
		t.Fatalf("queue under a live topic's name must be ErrNameConflict, got %v", err)
	}

	// A topic cannot shadow a subscription backing queue either (queues table
	// holds both kinds — the guard must not filter to kind='queue').
	if err := e.Subscribe(ctx, "billing_sub", "y", nil); !errors.Is(err, ErrNameConflict) {
		t.Fatalf("topic shadowing a subscription backing queue must be ErrNameConflict, got %v", err)
	}

	// A subscription cannot take a live topic's name (its backing queue would
	// itself resolve as that topic), and the failed call must not half-register
	// its own topic.
	if err := e.Subscribe(ctx, "t2", "billing", nil); !errors.Is(err, ErrNameConflict) {
		t.Fatalf("subscription named after a live topic must be ErrNameConflict, got %v", err)
	}
	if _, err := e.SendOne(ctx, "t2", OutMessage{Body: []byte("x")}); !errors.Is(err, ErrQueueNotFound) {
		t.Fatalf("t2 must not exist after the rejected subscribe, got %v", err)
	}

	// Degenerate self-reference: Subscribe(topic=X, name=X) is rejected.
	if err := e.Subscribe(ctx, "same", "same", nil); !errors.Is(err, ErrNameConflict) {
		t.Fatalf("self-referential subscription must be ErrNameConflict, got %v", err)
	}

	// Cross-kind upsert is rejected: a plain CreateQueue must not silently
	// retune a subscription's backing queue, nor a kind=subscription request a
	// plain queue. A deliberate same-kind reconfig stays open.
	if err := e.CreateQueue(ctx, "billing_sub", QueueConfig{}); !errors.Is(err, ErrNameConflict) {
		t.Fatalf("plain CreateQueue over a backing queue must be ErrNameConflict, got %v", err)
	}
	if err := e.CreateQueue(ctx, "orders", QueueConfig{Kind: "subscription"}); !errors.Is(err, ErrNameConflict) {
		t.Fatalf("kind=subscription over a plain queue must be ErrNameConflict, got %v", err)
	}
	if err := e.CreateQueue(ctx, "billing_sub", QueueConfig{Kind: "subscription", LockDurationMs: 60_000}); err != nil {
		t.Fatalf("explicit same-kind backing-queue reconfig should succeed: %v", err)
	}

	// Legal upserts stay open: reconfiguring an existing queue by name, and
	// re-subscribing the same (topic, name).
	mustQueue(t, e, "orders", QueueConfig{LockDurationMs: 60_000})
	if err := e.Subscribe(ctx, "billing", "billing_sub", nil); err != nil {
		t.Fatalf("idempotent re-subscribe should succeed: %v", err)
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

// strict_fifo: the queue delivers strictly one message at a time in id order,
// regardless of grouping. A second message is not claimable while the head is
// still in flight; completing the head releases the next id in order.
func TestStrictFIFOOrdering(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{Ordering: OrderStrictFIFO})

	// Three ungrouped messages (no GroupID). Under strict_fifo they must still
	// be delivered strictly head-of-line.
	for _, b := range []string{"m1", "m2", "m3"} {
		if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte(b)}); err != nil {
			t.Fatalf("send %s: %v", b, err)
		}
	}

	// First claim returns m1 only.
	first := recvOne(t, e, "q")
	if first == nil || string(first.Body) != "m1" {
		t.Fatalf("expected m1 first, got %+v", first)
	}
	// While m1 is locked (unsettled), nothing else is claimable.
	if blocked := recvOne(t, e, "q"); blocked != nil {
		t.Fatalf("expected head-of-line block, but got %q", blocked.Body)
	}
	// Complete m1 -> m2 becomes claimable, in id order.
	if err := e.Complete(ctx, "q", first.SeqNumber, first.LockToken); err != nil {
		t.Fatalf("complete m1: %v", err)
	}
	second := recvOne(t, e, "q")
	if second == nil || string(second.Body) != "m2" {
		t.Fatalf("expected m2 second, got %+v", second)
	}
	// m3 still blocked behind the in-flight m2.
	if blocked := recvOne(t, e, "q"); blocked != nil {
		t.Fatalf("expected m3 blocked behind m2, got %q", blocked.Body)
	}
	if err := e.Complete(ctx, "q", second.SeqNumber, second.LockToken); err != nil {
		t.Fatalf("complete m2: %v", err)
	}
	third := recvOne(t, e, "q")
	if third == nil || string(third.Body) != "m3" {
		t.Fatalf("expected m3 third, got %+v", third)
	}
}

// group_fifo: every message must carry a GroupID; sending one without surfaces
// ErrGroupRequired. A message with a GroupID enqueues normally.
func TestGroupFIFORequiresGroupID(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	mustQueue(t, e, "q", QueueConfig{Ordering: OrderGroupFIFO})

	if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte("no-group")}); !errors.Is(err, ErrGroupRequired) {
		t.Fatalf("expected ErrGroupRequired for missing GroupID, got %v", err)
	}
	if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte("grouped"), GroupID: "g1"}); err != nil {
		t.Fatalf("grouped send should succeed: %v", err)
	}
	m := recvOne(t, e, "q")
	if m == nil || string(m.Body) != "grouped" || m.GroupID != "g1" {
		t.Fatalf("expected grouped message, got %+v", m)
	}
}

// ─── defer × ordering: head-of-line (MQLITE-48) ────────────────────────────────

// A Defer'd message keeps its head-of-line position: in an ordered queue the
// messages behind it (its group, or the whole queue under strict_fifo) are NOT
// claimable until it is retrieved via ReceiveDeferred and settled. Ungrouped
// standard messages are exempt (group_id IS NULL short-circuits the claim's
// head-of-line check). This pins the claim-SQL semantics documented in
// docs/conformance.md §2.4; removing deferred from the head-of-line block set
// would silently violate FIFO.
func TestDeferHoldsHeadOfLine(t *testing.T) {
	ctx := context.Background()

	t.Run("standard ungrouped does not block", func(t *testing.T) {
		e, _ := testEngine(t)
		mustQueue(t, e, "q", QueueConfig{})
		e.SendOne(ctx, "q", OutMessage{Body: []byte("a")})
		e.SendOne(ctx, "q", OutMessage{Body: []byte("b")})

		head := recvOne(t, e, "q")
		if err := e.Defer(ctx, "q", head.SeqNumber, head.LockToken); err != nil {
			t.Fatal(err)
		}
		// "b" stays claimable — no head-of-line for ungrouped standard messages.
		if got := recvOne(t, e, "q"); got == nil || string(got.Body) != "b" {
			t.Fatalf("ungrouped defer must not block later messages, got %+v", got)
		}
	})

	t.Run("group_fifo: deferred head blocks its group, others proceed", func(t *testing.T) {
		e, _ := testEngine(t)
		mustQueue(t, e, "q", QueueConfig{Ordering: OrderGroupFIFO})
		e.SendOne(ctx, "q", OutMessage{Body: []byte("g1a"), GroupID: "G1"})
		e.SendOne(ctx, "q", OutMessage{Body: []byte("g1b"), GroupID: "G1"})
		e.SendOne(ctx, "q", OutMessage{Body: []byte("g2a"), GroupID: "G2"})

		head := recvOne(t, e, "q") // G1 head
		if head == nil || head.GroupID != "G1" {
			t.Fatalf("expected G1 head first, got %+v", head)
		}
		if err := e.Defer(ctx, "q", head.SeqNumber, head.LockToken); err != nil {
			t.Fatal(err)
		}
		// Other group proceeds; G1's next stays blocked behind the deferred head.
		if other := recvOne(t, e, "q"); other == nil || other.GroupID != "G2" {
			t.Fatalf("independent group must proceed, got %+v", other)
		}
		if blocked := recvOne(t, e, "q"); blocked != nil {
			t.Fatalf("G1 must stay blocked behind its deferred head, got %q", blocked.Body)
		}
		// Retrieve + settle the deferred head -> G1 advances to g1b.
		got, err := e.ReceiveDeferred(ctx, "q", head.SeqNumber)
		if err != nil || len(got) != 1 {
			t.Fatalf("receive deferred: %v %+v", err, got)
		}
		if err := e.Complete(ctx, "q", got[0].SeqNumber, got[0].LockToken); err != nil {
			t.Fatal(err)
		}
		if next := recvOne(t, e, "q"); next == nil || string(next.Body) != "g1b" {
			t.Fatalf("after settling the deferred head, G1 should advance to g1b, got %+v", next)
		}
	})

	t.Run("strict_fifo: one deferred message stalls the whole queue", func(t *testing.T) {
		e, _ := testEngine(t)
		mustQueue(t, e, "q", QueueConfig{Ordering: OrderStrictFIFO})
		e.SendOne(ctx, "q", OutMessage{Body: []byte("m1")})
		e.SendOne(ctx, "q", OutMessage{Body: []byte("m2")})

		m1 := recvOne(t, e, "q")
		if m1 == nil || string(m1.Body) != "m1" {
			t.Fatalf("expected m1 first, got %+v", m1)
		}
		if err := e.Defer(ctx, "q", m1.SeqNumber, m1.LockToken); err != nil {
			t.Fatal(err)
		}
		// Whole queue stalls behind the single deferred message.
		if blocked := recvOne(t, e, "q"); blocked != nil {
			t.Fatalf("strict_fifo must stall the whole queue behind a deferred message, got %q", blocked.Body)
		}
		got, err := e.ReceiveDeferred(ctx, "q", m1.SeqNumber)
		if err != nil || len(got) != 1 {
			t.Fatalf("receive deferred: %v %+v", err, got)
		}
		if err := e.Complete(ctx, "q", got[0].SeqNumber, got[0].LockToken); err != nil {
			t.Fatal(err)
		}
		if m2 := recvOne(t, e, "q"); m2 == nil || string(m2.Body) != "m2" {
			t.Fatalf("after settling the deferred m1, m2 should be claimable, got %+v", m2)
		}
	})
}

// ─── lock expiry × ordering: head-of-line (MQLITE-56) ──────────────────────────

// FIFO must survive a consumer timeout. A locked head whose lock has EXPIRED but
// which the reaper has not yet resettled keeps blocking its group (strict_fifo:
// the whole queue): successors are never delivered ahead of it, and the expired
// head itself is redelivered first — in id order — once the reaper runs. Before
// MQLITE-56 the locked head-of-line probe carried `AND b.locked_until>?`, so in
// the ≤1s reaper window successors overtook the head (out-of-order delivery) and
// the group ran two messages in flight at once. SQS FIFO semantics: a group with
// an in-flight message stays blocked for the whole in-flight window, expired or
// not. Cost of the fix: a consumer timeout stalls the group ≤ one reaper
// interval; Abandon (explicit settle) still releases immediately.
func TestFIFOHoldsAcrossLockExpiry(t *testing.T) {
	ctx := context.Background()

	t.Run("group_fifo: expired head still blocks its group, others proceed", func(t *testing.T) {
		e, ms := testEngine(t)
		mustQueue(t, e, "q", QueueConfig{Ordering: OrderGroupFIFO})
		e.SendOne(ctx, "q", OutMessage{Body: []byte("g1a"), GroupID: "G1"})
		e.SendOne(ctx, "q", OutMessage{Body: []byte("g1b"), GroupID: "G1"})
		e.SendOne(ctx, "q", OutMessage{Body: []byte("g2a"), GroupID: "G2"})

		head := recvOne(t, e, "q") // G1 head, locked for the default 30s
		if head == nil || string(head.Body) != "g1a" {
			t.Fatalf("expected g1a first, got %+v", head)
		}
		advance(ms, 31*time.Second) // lock expired; reaper has NOT run

		// The expired-but-unreaped head must still hold its group's line...
		if got := recvOne(t, e, "q"); got == nil || got.GroupID != "G2" {
			t.Fatalf("only G2 may proceed past G1's expired head, got %+v", got)
		}
		if blocked := recvOne(t, e, "q"); blocked != nil {
			t.Fatalf("g1b must not overtake G1's expired head, got %q", blocked.Body)
		}

		// ...until the reaper resettles it; then the HEAD is redelivered first.
		e.RunMaintenanceOnce(ctx)
		redelivered := recvOne(t, e, "q")
		if redelivered == nil || string(redelivered.Body) != "g1a" {
			t.Fatalf("expired head must be redelivered before its successors, got %+v", redelivered)
		}
		if redelivered.DeliveryCount != 2 {
			t.Fatalf("redelivery must bump delivery_count to 2, got %d", redelivered.DeliveryCount)
		}
		if err := e.Complete(ctx, "q", redelivered.SeqNumber, redelivered.LockToken); err != nil {
			t.Fatal(err)
		}
		if next := recvOne(t, e, "q"); next == nil || string(next.Body) != "g1b" {
			t.Fatalf("after the head settles, G1 advances to g1b, got %+v", next)
		}
	})

	t.Run("strict_fifo: expired head stalls the whole queue until reaped", func(t *testing.T) {
		e, ms := testEngine(t)
		mustQueue(t, e, "q", QueueConfig{Ordering: OrderStrictFIFO})
		e.SendOne(ctx, "q", OutMessage{Body: []byte("m1")})
		e.SendOne(ctx, "q", OutMessage{Body: []byte("m2")})

		m1 := recvOne(t, e, "q")
		if m1 == nil || string(m1.Body) != "m1" {
			t.Fatalf("expected m1 first, got %+v", m1)
		}
		advance(ms, 31*time.Second) // lock expired; reaper has NOT run

		if blocked := recvOne(t, e, "q"); blocked != nil {
			t.Fatalf("m2 must not overtake the expired head, got %q", blocked.Body)
		}
		e.RunMaintenanceOnce(ctx)
		redelivered := recvOne(t, e, "q")
		if redelivered == nil || string(redelivered.Body) != "m1" {
			t.Fatalf("expired head must be redelivered first, got %+v", redelivered)
		}
		if err := e.Complete(ctx, "q", redelivered.SeqNumber, redelivered.LockToken); err != nil {
			t.Fatal(err)
		}
		if m2 := recvOne(t, e, "q"); m2 == nil || string(m2.Body) != "m2" {
			t.Fatalf("m2 follows once the head settles, got %+v", m2)
		}
	})
}
