package engine

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A canceled long poll must not retain one map entry per temporary queue. A
// release from the old channel must also leave a replacement waiter reachable.
func TestNotifierRetiresCanceledAndReplacedWaiters(t *testing.T) {
	n := newNotifier()
	for i := 0; i < 1000; i++ {
		_, release := n.subscribe(fmt.Sprintf("temporary-%d", i))
		release()
		release() // cleanup is safe when both an early exit and a defer run
	}
	if len(n.chans) != 0 {
		t.Fatalf("canceled waiters retained %d names", len(n.chans))
	}
	old, releaseOld := n.subscribe("reused")
	n.notify("reused")
	select {
	case <-old:
	default:
		t.Fatal("old waiter was not notified")
	}
	fresh, releaseFresh := n.subscribe("reused")
	releaseOld()
	select {
	case <-fresh:
		t.Fatal("retired waiter closed a replacement channel")
	default:
	}
	n.notify("reused")
	select {
	case <-fresh:
	default:
		t.Fatal("replacement waiter lost its notification")
	}
	releaseFresh()
	if len(n.chans) != 0 {
		t.Fatalf("completed waiters retained %d names", len(n.chans))
	}
}

func TestNotifierReleaseKeepsOtherWaiters(t *testing.T) {
	n := newNotifier()
	first, releaseFirst := n.subscribe("q")
	second, releaseSecond := n.subscribe("q")
	if first != second {
		t.Fatal("concurrent waiters must observe the same notification")
	}
	releaseFirst()
	n.notify("q")
	select {
	case <-second:
	default:
		t.Fatal("one canceled waiter removed another waiter's notification")
	}
	releaseSecond()
}

func TestObservabilityRetirementFencesLateCommit(t *testing.T) {
	o := observations{initialized: true, known: map[string]bool{"q": true}}
	o.commit(&txn{events: map[eventKey]uint64{{"q", "enqueued"}: 3}})
	late := &txn{events: map[eventKey]uint64{{"q", "completed"}: 2}}
	o.commit(&txn{forgottenQueues: map[string]bool{"q": true}})
	o.commit(&txn{observationEpoch: o.epoch, rememberedQueues: map[string]bool{"q": true}})
	o.commit(late)
	o.commit(&txn{observationEpoch: o.epoch, events: map[eventKey]uint64{{"q", "enqueued"}: 1}})
	if !o.known["q"] || len(o.known) != 1 {
		t.Fatalf("replacement inventory=%v", o.known)
	}
	if o.events[eventKey{"q", "enqueued"}] != 1 || o.events[eventKey{"q", "completed"}] != 0 {
		t.Fatalf("old lifetime contaminated replacement: %v", o.events)
	}
	if o.events[eventKey{"", "enqueued"}] != 3 || o.events[eventKey{"", "completed"}] != 2 {
		t.Fatalf("retired aggregate lost confirmed effects: %v", o.events)
	}
	// Confirmed creation hooks follow database commit order, even if the
	// transaction began before another name retired and carries an older epoch.
	prior := o.epoch
	o.commit(&txn{rememberedQueues: map[string]bool{"new-name": true}})
	if !o.known["new-name"] || o.epoch <= prior {
		t.Fatal("confirmed creation was lost or failed to fence older inventory")
	}
}

func TestObservabilityUniqueNameChurnRetainsOnlyAggregate(t *testing.T) {
	o := observations{initialized: true}
	for i := 0; i < 1000; i++ {
		name := fmt.Sprintf("request-%d", i)
		epoch := o.epoch
		o.commit(&txn{observationEpoch: epoch, rememberedQueues: map[string]bool{name: true}})
		o.commit(&txn{observationEpoch: epoch, events: map[eventKey]uint64{{name, "enqueued"}: 1}})
		o.commit(&txn{observationEpoch: epoch, forgottenQueues: map[string]bool{name: true}})
		o.commit(&txn{observationEpoch: epoch, events: map[eventKey]uint64{{name, "completed"}: 1}})
	}
	if len(o.known) != 0 || len(o.events) != 2 {
		t.Fatalf("retired names retained: known=%d event keys=%d", len(o.known), len(o.events))
	}
	if o.events[eventKey{"", "enqueued"}] != 1000 || o.events[eventKey{"", "completed"}] != 1000 {
		t.Fatalf("retired counts=%v", o.events)
	}
}

func TestObservabilityRetiredAggregateHasCompleteEventDomain(t *testing.T) {
	e, _ := testEngine(t)
	ctx := context.Background()
	mustQueue(t, e, "retired", QueueConfig{})
	if _, err := e.SendOne(ctx, "retired", OutMessage{Body: []byte("work")}); err != nil {
		t.Fatal(err)
	}
	queues, err := e.ListQueues(ctx)
	if err != nil || len(queues) != 1 {
		t.Fatalf("inventory=%v err=%v", queues, err)
	}
	if result, err := e.DeleteQueueWithOptions(ctx, "retired", queues[0].Generation, DeleteOptions{Force: true}); err != nil || !result.Deleted {
		t.Fatalf("delete=%+v err=%v", result, err)
	}
	snapshot := e.Observability(ctx)
	if len(snapshot.Messages) != len(MessageEventNames()) {
		t.Fatalf("retired event domain is partial: %+v", snapshot.Messages)
	}
	for _, row := range snapshot.Messages {
		if row.Queue != "" {
			t.Fatalf("retired name retained: %+v", row)
		}
	}
}

func TestSubscriptionChurnReclaimsPersistentAndMemoryState(t *testing.T) {
	ctx := context.Background()
	e, clock := testEngine(t)
	for i := 0; i < 300; i++ {
		name := fmt.Sprintf("temporary-%d", i)
		sub, err := e.SubscribeWithOptions(ctx, "replies", name, &Filter{Expr: `subject == "reply"`}, SubscribeOptions{
			TTLMs: 1000, RequestID: name, RequestExpiresAtMs: e.now() + 1000,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := e.SendOne(ctx, "replies", OutMessage{Subject: "reply", Body: []byte(name)}); err != nil {
			t.Fatal(err)
		}
		msgs, err := e.Receive(ctx, name, ReceiveOptions{Generation: sub.Generation, AttemptID: name})
		if err != nil || len(msgs) != 1 {
			t.Fatalf("receive iteration %d: %d messages %v", i, len(msgs), err)
		}
		if i%2 == 0 {
			if err := e.Complete(ctx, name, msgs[0].SeqNumber, msgs[0].LockToken); err != nil {
				t.Fatal(err)
			}
			result, err := e.DeleteSubscription(ctx, "replies", name, sub.Generation, DeleteOptions{})
			if err != nil || !result.Deleted {
				t.Fatalf("delete iteration %d: %+v %v", i, result, err)
			}
		}
		advance(clock, 1001*time.Millisecond)
		e.maintainLifecycle(ctx)
	}
	for _, table := range []string{"queues", "subscriptions", "messages", "dedup", "settlement_receipts", "receive_attempts", "subscription_creations"} {
		var count int
		if err := e.db.queryRowScan(ctx, []any{&count}, "SELECT COUNT(*) FROM "+table); err != nil || count != 0 {
			t.Fatalf("retained %s rows=%d err=%v", table, count, err)
		}
	}
	if len(e.qcache) != 0 || len(e.filterCache) != 0 || len(e.note.chans) != 0 || len(e.observation.known) != 0 || len(e.observation.events) > len(MessageEventNames()) {
		t.Fatalf("retained name state: queues=%d filters=%d waits=%d known=%d counters=%d", len(e.qcache), len(e.filterCache), len(e.note.chans), len(e.observation.known), len(e.observation.events))
	}
}

func TestSubscriptionExpiryBetweenMetadataAndReceiveStatement(t *testing.T) {
	for _, replay := range []bool{false, true} {
		t.Run(fmt.Sprintf("replay=%t", replay), func(t *testing.T) {
			ctx := context.Background()
			e, _ := testEngine(t)
			sub, err := e.SubscribeWithOptions(ctx, "topic", "reply", nil, SubscribeOptions{TTLMs: 1000, RequestID: "create", RequestExpiresAtMs: e.now() + 1000})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := e.SendOne(ctx, "topic", OutMessage{Body: []byte("reply")}); err != nil {
				t.Fatal(err)
			}
			msgs, err := e.Receive(ctx, "reply", ReceiveOptions{Generation: sub.Generation, AttemptID: "receive"})
			if err != nil || len(msgs) != 1 {
				t.Fatalf("receive: %v %v", msgs, err)
			}
			if !replay {
				if err := e.Defer(ctx, "reply", msgs[0].SeqNumber, msgs[0].LockToken); err != nil {
					t.Fatal(err)
				}
			}
			q, err := e.loadQueue(ctx, "reply")
			if err != nil {
				t.Fatal(err)
			}
			reads, expirationRead := 0, 3 // cached metadata, transactional metadata, claim statement
			if replay {
				expirationRead = 2 // transactional metadata, receipt lookup statement
			}
			e.now = func() int64 {
				reads++
				if reads >= expirationRead {
					return sub.ExpiresAtMs
				}
				return sub.ExpiresAtMs - 1
			}
			if replay {
				msgs, err = e.claimRound(ctx, q, 1, PeekLock, "receive")
			} else {
				msgs, err = e.ReceiveDeferredWithGeneration(ctx, "reply", sub.Generation, msgs[0].SeqNumber)
			}
			if !errors.Is(err, ErrQueueNotFound) || len(msgs) != 0 {
				t.Fatalf("delivered across expiration: messages=%v err=%v clock reads=%d", msgs, err, reads)
			}
		})
	}
}

// Concurrency invariants — the properties that make a queue a queue, asserted under real parallelism.
//
// Everything else in this suite tests what the engine SAYS when you ask it something. These tests
// watch what it DOES while many consumers hammer it at once, and they are here because that is
// precisely where this codebase has no eyes: every P1 found in review rounds 4-6 lived in behaviour
// that was observable but unobserved.
//
// The invariants below are the ones whose violation is unrecoverable for a user — a message handled
// twice at the same time, a message handled after it was completed, a message that simply vanishes.
// They are checked by an oracle that watches the deliveries, not by asking the engine to grade
// itself, because an engine with a bug will happily report that everything is fine.

// exclusivity is the oracle. It records who holds what, and screams the moment two consumers hold
// the same message at once — which is the failure a lock-based queue exists to prevent.
//
// It models the lease, because exclusivity IS the lease: a consumer owns a message only until
// locked_until, and a redelivery after that instant is not a violation, it is the documented
// at-least-once contract doing its job. The first version of this oracle ignored expiry and duly
// "found" dozens of violations that were the reaper working correctly — a reminder that an oracle
// which does not model the spec just measures its own misunderstanding.
//
// Holders are keyed by LOCK TOKEN, not by consumer: after a redelivery the same consumer may hold
// the same seq under a new token, and the old holder's late settle must not be mistaken for the new
// one's.
type holder struct {
	consumer    int
	token       string
	lockedUntil int64 // epoch ms; past this, the holder owns nothing
}

type exclusivity struct {
	mu         sync.Mutex
	held       map[int64]holder
	done       map[int64]bool // completed: a delivery after this is a settled message coming back
	deliveries map[int64]int

	violations []string
}

func newExclusivity() *exclusivity {
	return &exclusivity{held: map[int64]holder{}, done: map[int64]bool{}, deliveries: map[int64]int{}}
}

func (x *exclusivity) fail(format string, a ...any) {
	x.violations = append(x.violations, fmt.Sprintf(format, a...))
}

// claim records a delivery, the instant a consumer receives a message.
func (x *exclusivity) claim(seq int64, consumer int, token string, lockedUntil, now int64) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.deliveries[seq]++
	if prev, busy := x.held[seq]; busy && prev.lockedUntil > now {
		x.fail("seq %d was delivered to consumer %d while consumer %d STILL HELD A LIVE LOCK on it "+
			"(%dms of lease left) — two consumers are processing the same message at the same time",
			seq, consumer, prev.consumer, prev.lockedUntil-now)
		return
	}
	if x.done[seq] {
		x.fail("seq %d was delivered to consumer %d AFTER it had been completed "+
			"— a settled message came back", seq, consumer)
		return
	}
	x.held[seq] = holder{consumer: consumer, token: token, lockedUntil: lockedUntil}
}

// renewed extends the lease the oracle believes in, so a renewing holder does not look expired.
func (x *exclusivity) renewed(seq int64, token string, lockedUntil int64) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if h, ok := x.held[seq]; ok && h.token == token {
		h.lockedUntil = lockedUntil
		x.held[seq] = h
	}
}

// settled records the outcome of a settle by the holder of `token`. It is a no-op when the message
// has already moved on to a newer token — that holder lost its lease and owns nothing.
func (x *exclusivity) settled(seq int64, token string, completed bool) {
	x.mu.Lock()
	defer x.mu.Unlock()
	h, ok := x.held[seq]
	if !ok || h.token != token {
		return // stale holder; the engine will have told it ErrLockLost
	}
	delete(x.held, seq)
	if completed {
		x.done[seq] = true
	}
}

func (x *exclusivity) completedCount() int {
	x.mu.Lock()
	defer x.mu.Unlock()
	return len(x.done)
}

func (x *exclusivity) report(t *testing.T) {
	t.Helper()
	x.mu.Lock()
	defer x.mu.Unlock()
	if len(x.violations) == 0 {
		return
	}
	for i, v := range x.violations {
		if i == 8 {
			t.Errorf("... and %d more", len(x.violations)-8)
			break
		}
		t.Errorf("INVARIANT VIOLATED: %s", v)
	}
	t.FailNow()
}

// TestConcurrentConsumersNeverShareAMessage runs many consumers against one queue, in parallel, for
// real, and asserts the three invariants a user cannot work around:
//
//  1. EXCLUSIVE DELIVERY — while a consumer holds a lock, nobody else may be handed that message.
//  2. NO ZOMBIE DELIVERY — a completed message is never delivered again.
//  3. NO LOSS — every message sent is eventually completed. Not "most": every one.
//
// The consumers do not behave: they abandon, defer, renew, let locks expire on purpose, and settle
// with the wrong token. Locks are short and the reaper is running, so expiry races the handler —
// which is the exact window where a real system double-delivers.
func TestConcurrentConsumersNeverShareAMessage(t *testing.T) {
	eachLocalStore(t, func(t *testing.T, dsn string) {
		const (
			messages  = 400
			consumers = 8
		)
		ctx := context.Background()
		e, err := Open(ctx, Options{DB: dsn})
		if err != nil {
			t.Fatal(err)
		}
		defer e.Close()

		// A short lease with the reaper live: expiry genuinely races the handler. This is the
		// setting under which a queue either holds its exclusivity guarantee or does not.
		mustQueue(t, e, "q", QueueConfig{LockDurationMs: 300, MaxDeliveryCount: 100})

		for i := 0; i < messages; i++ {
			if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte(fmt.Sprintf("m%d", i))}); err != nil {
				t.Fatal(err)
			}
		}

		x := newExclusivity()
		var completed int64
		deadline := time.Now().Add(20 * time.Second)

		var wg sync.WaitGroup
		for c := 0; c < consumers; c++ {
			wg.Add(1)
			go func(consumer int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(int64(consumer) + 1))
				for atomic.LoadInt64(&completed) < messages && time.Now().Before(deadline) {
					msgs, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 1 + rng.Intn(4), WaitMs: 50})
					if err != nil {
						t.Errorf("consumer %d: receive: %v", consumer, err)
						return
					}
					for _, m := range msgs {
						x.claim(m.SeqNumber, consumer, m.LockToken, m.LockedUntilMs, time.Now().UnixMilli())
					}
					for _, m := range msgs {
						switch rng.Intn(10) {
						case 0, 1:
							// Give it back. Somebody else must be able to take it — but only after
							// we have let go.
							x.settled(m.SeqNumber, m.LockToken, false)
							if err := e.Abandon(ctx, "q", m.SeqNumber, m.LockToken, 0); err != nil &&
								!errors.Is(err, ErrLockLost) {
								t.Errorf("consumer %d: abandon: %v", consumer, err)
							}
						case 2:
							// OVERRUN the lease and then try to settle. Expiry itself fences the
							// holder, even if the reaper has not reclaimed the row yet.
							time.Sleep(time.Duration(400+rng.Intn(200)) * time.Millisecond)
							err := e.Complete(ctx, "q", m.SeqNumber, m.LockToken)
							x.settled(m.SeqNumber, m.LockToken, false)
							if !errors.Is(err, ErrLockLost) {
								t.Errorf("consumer %d: late complete: %v, want ErrLockLost", consumer, err)
								return
							}
						case 3:
							// Renew, then complete: the lease must hold across the renewal.
							// The request's start gives a conservative lease bound. Adding the
							// duration after a slow response could invent lease time never granted.
							started := time.Now().UnixMilli()
							if err := e.Renew(ctx, "q", m.SeqNumber, m.LockToken); err == nil {
								x.renewed(m.SeqNumber, m.LockToken, started+300)
							} else if !errors.Is(err, ErrLockLost) {
								t.Errorf("consumer %d: renew: %v", consumer, err)
							}
							fallthrough
						default:
							err := e.Complete(ctx, "q", m.SeqNumber, m.LockToken)
							x.settled(m.SeqNumber, m.LockToken, err == nil)
							if err == nil {
								atomic.AddInt64(&completed, 1)
							} else if !errors.Is(err, ErrLockLost) {
								t.Errorf("consumer %d: complete: %v", consumer, err)
							}
						}
					}
				}
			}(c)
		}
		wg.Wait()

		x.report(t) // invariants 1 and 2

		// Invariant 3: nothing was lost. Every message sent came out the other side.
		if got := x.completedCount(); got != messages {
			var stuck []int64
			for seq := int64(1); seq <= messages; seq++ {
				if !x.done[seq] {
					stuck = append(stuck, seq)
					if len(stuck) == 10 {
						break
					}
				}
			}
			m, _ := e.Stats(context.Background(), "q")
			t.Fatalf("MESSAGE LOSS: %d of %d messages were completed.\n"+
				"  still in the queue: active=%d locked=%d dead=%d deferred=%d\n"+
				"  never completed (first few): %v\n"+
				"  a queue may deliver a message more than once; it may not lose one.",
				got, messages, m.Active, m.Locked, m.DeadLettered, m.Deferred, stuck)
		}
	})
}

// TestConcurrentSettlementPicksExactlyOneWinner is the exclusivity invariant reduced to its sharpest
// form: two consumers, one message, a lock that has just expired. Both of them try to settle it.
//
// Exactly the new, still-live holder may win. The fake clock is held still while both calls race:
// if both leases expired, refusing both would be correct and would say nothing about exclusivity.
//
// It runs many rounds because this is a race: a single round proves nothing.
func TestConcurrentSettlementPicksExactlyOneWinner(t *testing.T) {
	eachLocalStore(t, func(t *testing.T, dsn string) {
		ctx := context.Background()
		var now atomic.Int64
		now.Store(1_700_000_000_000)
		e, err := Open(ctx, Options{DB: dsn, Now: now.Load, DisableBackground: true})
		if err != nil {
			t.Fatal(err)
		}
		defer e.Close()
		mustQueue(t, e, "q", QueueConfig{LockDurationMs: 50, MaxDeliveryCount: 100})

		const rounds = 60
		for r := 0; r < rounds; r++ {
			if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte("x")}); err != nil {
				t.Fatal(err)
			}
			first, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 1})
			if err != nil || len(first) != 1 {
				t.Fatalf("round %d: first receive: %v (%d)", r, err, len(first))
			}
			a := first[0]

			// Let the lease lapse and hand the message to a second consumer. Now TWO tokens exist
			// for one message: A's, which is stale, and B's, which is live.
			now.Store(a.LockedUntilMs)
			e.RunMaintenanceOnce(ctx)
			second, err := e.Receive(ctx, "q", ReceiveOptions{MaxMessages: 1})
			if err != nil || len(second) != 1 {
				t.Fatalf("round %d: the reaper did not release the expired lock: %v (%d)", r, err, len(second))
			}
			b := second[0]
			if b.SeqNumber != a.SeqNumber {
				t.Fatalf("round %d: expected the same message back, got %d and %d", r, a.SeqNumber, b.SeqNumber)
			}
			if a.LockToken == b.LockToken {
				t.Fatalf("round %d: the redelivery reused the lock token — the fence is not a fence", r)
			}

			// Both consumers finish at the same instant and both try to acknowledge.
			var wins int64
			var staleWon int64
			var wg sync.WaitGroup
			for _, m := range []*Message{a, b} {
				wg.Add(1)
				go func(m *Message, stale bool) {
					defer wg.Done()
					err := e.Complete(ctx, "q", m.SeqNumber, m.LockToken)
					switch {
					case err == nil:
						atomic.AddInt64(&wins, 1)
						if stale {
							atomic.AddInt64(&staleWon, 1)
						}
					case errors.Is(err, ErrLockLost): // the honest answer for the stale holder
					default:
						t.Errorf("round %d: unexpected error: %v", r, err)
					}
				}(m, m.LockToken == a.LockToken)
			}
			wg.Wait()

			if wins != 1 {
				t.Fatalf("round %d: %d of 2 racing consumers were told their Complete succeeded.\n"+
					"  exactly one may win. Telling both they succeeded means the message was\n"+
					"  processed twice and both acknowledgements were accepted.", r, wins)
			}
			// And "exactly one" is not enough on its own: the winner must be the RIGHT one. A queue
			// that lets the STALE holder settle has no fence at all — it merely has a race, and the
			// live holder (which is the one actually processing the message) is told it lost a lock
			// it still held. Counting winners alone cannot see this; asking WHO won can.
			if staleWon != 0 {
				t.Fatalf("round %d: the settle was accepted from the holder of the EXPIRED lock.\n"+
					"  the message had already been reassigned, and its new owner — the one actually\n"+
					"  processing it — was told ErrLockLost. lock_token is not fencing anything.", r)
			}
			// And it really is gone.
			left, err := e.Peek(ctx, "q", PeekOptions{Max: 10})
			if err != nil {
				t.Fatal(err)
			}
			if len(left) != 0 {
				t.Fatalf("round %d: the message survived a successful Complete (%d left)", r, len(left))
			}
		}
	})
}
