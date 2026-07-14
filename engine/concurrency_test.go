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
							// OVERRUN the lease and then try to settle. The reaper will have taken
							// the message back and handed it to somebody else, so this settle must
							// be refused — that refusal is the fence doing its job, and it is what
							// stops the two holders from both acknowledging.
							time.Sleep(time.Duration(400+rng.Intn(200)) * time.Millisecond)
							err := e.Complete(ctx, "q", m.SeqNumber, m.LockToken)
							if err == nil {
								// The lease had NOT actually lapsed (nobody took it): still ours.
								x.settled(m.SeqNumber, m.LockToken, true)
								atomic.AddInt64(&completed, 1)
							} else if errors.Is(err, ErrLockLost) {
								x.settled(m.SeqNumber, m.LockToken, false)
							} else {
								t.Errorf("consumer %d: late complete: %v", consumer, err)
							}
						case 3:
							// Renew, then complete: the lease must hold across the renewal.
							if err := e.Renew(ctx, "q", m.SeqNumber, m.LockToken); err == nil {
								x.renewed(m.SeqNumber, m.LockToken, time.Now().UnixMilli()+300)
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
// Exactly one may win. If both do, the message was processed twice and acknowledged twice, and the
// fencing token — the entire mechanism this queue rests on — has failed. If neither does, the
// message is stuck.
//
// It runs many rounds because this is a race: a single round proves nothing.
func TestConcurrentSettlementPicksExactlyOneWinner(t *testing.T) {
	eachLocalStore(t, func(t *testing.T, dsn string) {
		ctx := context.Background()
		e, err := Open(ctx, Options{DB: dsn, DisableBackground: true})
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
			time.Sleep(80 * time.Millisecond)
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
