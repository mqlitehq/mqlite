package engine

// Model-based differential testing.
//
// Every bug this project shipped and had caught by a reviewer had the same shape: nobody had
// THOUGHT OF THE CASE. A test suite encodes the cases its author imagined, so it is structurally
// blind to exactly the ones that hurt — a settle aimed at the wrong message, a verb replaying
// another verb's receipt, an argument nobody would "sensibly" pass.
//
// So stop imagining cases. Write down what the queue MEANS — a small reference model — then let a
// generator throw operation sequences at both, including deliberately WRONG ones (someone else's
// token, another queue's seq, a replay of a settle that already happened), and demand they agree
// at every step.
//
// The model is the specification. If the engine disagrees with it, one of them is wrong, and
// either way we have learned something we did not know.

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// ─── the model: what a queue MEANS ────────────────────────────────────────────

type modelMsg struct {
	seq         int64
	state       State  // active | locked | dead_lettered | deferred | scheduled ("" = gone)
	token       string // retained until settlement/reaping, even after the lease expires
	queue       string
	delivs      int
	lockedUntil int64
}

// The documented replay window belongs to the specification, not the engine's
// implementation constant. A mutation to either deadline must cause disagreement.
const modelReceiptTTL = int64(30 * 60 * 1000)

type model struct {
	msgs map[int64]*modelMsg // by seq
	// receipts: what a settle promised, keyed by the REQUEST it settled.
	receipts map[modelReceipt]int64 // exclusive expiry; replay does not extend it
	maxDeliv int
	now      int64
	lockMs   int64
}

// MQLITE-105: keep fields separate, independent of the engine's receipt encoding.
// Delimiter concatenation aliases distinct Reject reason/description pairs.
type modelReceipt struct {
	queue, token, verb string
	seq                int64
	delay              int64
	reason             [2]string
}

func modelReceiptKey(queue string, seq int64, token, verb string, delay int64, reason [2]string) modelReceipt {
	key := modelReceipt{queue: queue, seq: seq, token: token, verb: verb}
	switch verb {
	case "abandoned":
		key.delay = delay
	case "dead_lettered":
		if reason[0] == "" {
			reason[0] = ReasonAppRequested
		}
		key.reason = reason
	}
	return key
}

// settle returns what the ENGINE is expected to answer for this exact request.
//
// This is the entire specification of settlement, and it is three lines: you may settle a message
// you currently hold an unexpired lock on; replaying the SAME request that already succeeded
// within its receipt's lifetime is an idempotent success; everything else is a lost lock.
//
// Note what is NOT here — nothing says a token vouches for a different message, that one verb
// inherits another's receipt, or that a request may keep its success when its ARGUMENTS change.
// The key carries the parameters that change what the settle DOES (Abandon's delay,
// Reject's reason/description), so a replay that alters them is a
// different request and gets no receipt. The model was blind to this bug for three rounds for one
// reason — it always passed the same delay and the same reason, so the arguments never varied and
// the spec was never exercised. A model only finds what its generator is willing to say.
func (m *model) settle(queue string, seq int64, token, verb string, delay int64, reason [2]string) (ok bool) {
	key := modelReceiptKey(queue, seq, token, verb, delay, reason)
	msg := m.msgs[seq]
	if m.holds(queue, seq, token) {
		switch verb {
		case "completed":
			msg.state = "" // gone
		case "abandoned":
			if msg.delivs >= m.maxDeliv {
				msg.state = StateDeadLettered
			} else if delay > 0 {
				// MQLITE-66: a backoff parks the message in 'scheduled' until the scheduler
				// re-activates it. This run uses DisableBackground, so the scheduler never
				// runs and the parking is permanent for the model's horizon — exactly what
				// the engine does.
				msg.state = StateScheduled
			} else {
				msg.state = StateActive
			}
		case "dead_lettered":
			msg.state = StateDeadLettered
		case "deferred":
			msg.state = StateDeferred
		}
		msg.token = ""
		msg.lockedUntil = 0
		m.receipts[key] = m.now + modelReceiptTTL
		return true
	}
	// Expired rows may still exist: neither lease nor receipt validity waits for a janitor.
	return m.receipts[key] > m.now
}

func (m *model) holds(queue string, seq int64, token string) bool {
	msg := m.msgs[seq]
	return token != "" && msg != nil && msg.queue == queue && msg.state == StateLocked &&
		msg.token == token && msg.lockedUntil > m.now
}

func (m *model) renew(queue string, seq int64, token string) bool {
	if !m.holds(queue, seq, token) {
		return false
	}
	msg := m.msgs[seq]
	if until := m.now + m.lockMs; until > msg.lockedUntil {
		msg.lockedUntil = until
	}
	return true
}

// Both directions of the old delimiter collision must be rejected by the model
// and engine. Empty reasons and their explicit default remain equivalent.
func TestModelRejectReceiptArguments(t *testing.T) {
	reasons := [][2]string{
		{"x|desc=y", "z"},
		{"x", "y|desc=z"},
		{"", ""},
		{ReasonAppRequested, ""},
		{ReasonAppRequested, "|desc="},
	}
	ctx := context.Background()
	for i, first := range reasons {
		for j, second := range reasons {
			t.Run(fmt.Sprintf("%d_to_%d", i, j), func(t *testing.T) {
				e, clock := testEngine(t)
				mustQueue(t, e, "q", QueueConfig{})
				if _, err := e.SendOne(ctx, "q", OutMessage{Body: []byte("m")}); err != nil {
					t.Fatal(err)
				}
				msg := recvOne(t, e, "q")
				m := &model{
					msgs: map[int64]*modelMsg{msg.SeqNumber: {
						seq: msg.SeqNumber, queue: "q", state: StateLocked, token: msg.LockToken,
						lockedUntil: msg.LockedUntilMs,
					}},
					receipts: map[modelReceipt]int64{}, now: atomic.LoadInt64(clock),
				}
				if !m.settle("q", msg.SeqNumber, msg.LockToken, "dead_lettered", 0, first) {
					t.Fatal("model rejected initial settlement")
				}
				if err := e.Reject(ctx, "q", msg.SeqNumber, msg.LockToken, first[0], first[1]); err != nil {
					t.Fatal(err)
				}
				// Spell out equivalence independently of modelReceiptKey and settleArgs.
				sameReason := first[0] == second[0] ||
					(first[0] == "" && second[0] == ReasonAppRequested) ||
					(first[0] == ReasonAppRequested && second[0] == "")
				want := sameReason && first[1] == second[1]
				if got := m.settle("q", msg.SeqNumber, msg.LockToken, "dead_lettered", 0, second); got != want {
					t.Errorf("model Reject(%q) after Reject(%q): ok=%v, want %v", second, first, got, want)
				}
				err := e.Reject(ctx, "q", msg.SeqNumber, msg.LockToken, second[0], second[1])
				if (err == nil) != want || (err != nil && !errors.Is(err, ErrLockLost)) {
					t.Errorf("engine Reject(%q) after Reject(%q): err=%v, want ok=%v", second, first, err, want)
				}
			})
		}
	}
}

func (m *model) counts(queue string) (active, locked, dead, deferred, scheduled, total int64) {
	for _, msg := range m.msgs {
		if msg.queue != queue || msg.state == "" {
			continue
		}
		total++
		switch msg.state {
		case StateActive:
			active++
		case StateLocked:
			locked++
		case StateDeadLettered:
			dead++
		case StateDeferred:
			deferred++
		case StateScheduled:
			scheduled++
		}
	}
	return
}

// ─── the differential test ────────────────────────────────────────────────────

func TestEngineMatchesTheModel(t *testing.T) {
	// Fixed seeds keep CI reproducible; a random one keeps the suite HONEST. Three frozen seeds
	// walk three frozen paths forever — they cannot find what they did not happen to generate on
	// the day they were chosen (round-6 §3.2). The random seed is printed, so any failure it turns
	// up is replayable by pinning MQLITE_MODEL_SEED.
	seeds := []int64{1, 2, 3}
	if raceEnabled {
		// -race makes every SQLite call ~10x dearer and the package shares one 10m budget.
		// Keep shorter reproducible and random walks here; every non-race CI leg runs the full
		// horizon. The missing lease fence itself fails at round 66 with seed 1.
		seeds = []int64{1, 2}
	}
	if env := os.Getenv("MQLITE_MODEL_SEED"); env != "" {
		n, err := strconv.ParseInt(env, 10, 64)
		if err != nil {
			t.Fatalf("MQLITE_MODEL_SEED=%q: %v", env, err)
		}
		seeds = []int64{n}
	} else {
		seeds = append(seeds, time.Now().UnixNano())
	}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			t.Logf("model seed %d — reproduce a failure with MQLITE_MODEL_SEED=%d", seed, seed)
			runModel(t, seed)
		})
	}
}

func runModel(t *testing.T, seed int64) {
	const (
		queues   = 2
		maxDeliv = 3
		lockMs   = 60_000
	)
	rounds := 4000
	if raceEnabled {
		rounds = 1200
	}
	ctx := context.Background()
	e, clock := testEngine(t)

	qs := []string{"q0", "q1"}
	for _, q := range qs {
		mustQueue(t, e, q, QueueConfig{LockDurationMs: lockMs, MaxDeliveryCount: maxDeliv})
	}
	m := &model{
		msgs: map[int64]*modelMsg{}, receipts: map[modelReceipt]int64{}, maxDeliv: maxDeliv,
		now: atomic.LoadInt64(clock), lockMs: lockMs,
	}

	rng := rand.New(rand.NewSource(seed))
	verbs := []string{"completed", "abandoned", "dead_lettered", "deferred"}
	// Every token the run has ever seen — so the generator can aim a STALE or SOMEBODY ELSE'S
	// token at a message. That is the class of bug nobody writes a test for.
	var seenTokens []string
	var seenSeqs []int64

	// Every settle the run has ISSUED. Receipts only matter on a replay, and a replay means the
	// very same (queue, seq, token, verb) coming back — which random sampling over an ever-growing
	// pool of seqs and tokens essentially never reproduces. That is why the generator drew wrong
	// pairs for three rounds and still never exercised the receipt path it was supposed to guard.
	// So replays are now DELIBERATE: re-issue a request that ALREADY SUCCEEDED — only those leave a
	// receipt, and a receipt is the whole thing under test — sometimes verbatim (it must still
	// succeed while its receipt is live) and sometimes with its arguments changed (it must NOT
	// inherit the first one's success).
	type issued struct {
		q, token, verb string
		seq            int64
		delay          int64
		reason         [2]string
	}
	var history []issued
	advanceTo := func(now int64) {
		if now > m.now {
			m.now = now
			atomic.StoreInt64(clock, now)
		}
	}
	// Select retained lock rows independently of engine state. Half the selections favor recent
	// claims, so advancing time cannot drown every valid operation in old expired tokens.
	lockedPair := func(queue string) *modelMsg {
		var locked []*modelMsg
		for _, seq := range seenSeqs {
			if msg := m.msgs[seq]; msg.queue == queue && msg.state == StateLocked {
				locked = append(locked, msg)
			}
		}
		if len(locked) == 0 {
			return nil
		}
		if len(locked) > 8 && rng.Intn(2) == 0 {
			locked = locked[len(locked)-8:]
		}
		return locked[rng.Intn(len(locked))]
	}
	var expiredLocks, liveReplays, expiredReplays, renewals int

	// The argument sets a settle may be replayed with, so a replay can differ from the
	// original in exactly the way a real client's would: same message, same token, same verb, a
	// different backoff or a different dead-letter reason.
	delays := []int64{0, 30_000}
	reasons := [][2]string{
		{ReasonAppRequested, ""}, {"PoisonMessage", "gave up after 3 tries"},
		{"x|desc=y", "z"}, {"x", "y|desc=z"}, {"", ""},
	}

	settleEngine := func(q string, seq int64, token, verb string, delay int64, reason [2]string) error {
		switch verb {
		case "completed":
			return e.Complete(ctx, q, seq, token)
		case "abandoned":
			return e.Abandon(ctx, q, seq, token, delay)
		case "dead_lettered":
			return e.Reject(ctx, q, seq, token, reason[0], reason[1])
		default:
			return e.Defer(ctx, q, seq, token)
		}
	}

	for i := 0; i < rounds; i++ {
		q := qs[rng.Intn(queues)]
		switch rng.Intn(100) {

		case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14: // send
			seq, err := e.SendOne(ctx, q, OutMessage{Body: []byte("m")})
			if err != nil {
				t.Fatalf("round %d: send: %v", i, err)
			}
			m.msgs[seq] = &modelMsg{seq: seq, state: StateActive, queue: q}
			seenSeqs = append(seenSeqs, seq)

		case 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25: // receive
			msgs, err := e.Receive(ctx, q, ReceiveOptions{MaxMessages: 1 + rng.Intn(3)})
			if err != nil {
				t.Fatalf("round %d: receive: %v", i, err)
			}
			for _, got := range msgs {
				mm := m.msgs[got.SeqNumber]
				if mm == nil || mm.queue != q || mm.state != StateActive {
					t.Fatalf("round %d: engine delivered seq %d from %s, which the model says is %q — a message was delivered that should not have been",
						i, got.SeqNumber, q, stateOf(mm))
				}
				mm.state = StateLocked
				mm.token = got.LockToken
				mm.lockedUntil = m.now + m.lockMs
				mm.delivs++
				if got.LockedUntilMs != mm.lockedUntil || got.DeliveryCount != mm.delivs {
					t.Fatalf("round %d: receive seq %d deadline/count=(%d,%d), model=(%d,%d)",
						i, got.SeqNumber, got.LockedUntilMs, got.DeliveryCount, mm.lockedUntil, mm.delivs)
				}
				seenTokens = append(seenTokens, got.LockToken)
			}

		case 26, 27, 28, 29, 30, 31, 32, 33: // BATCH settle/renew — the newest, riskiest code
			if len(seenSeqs) == 0 || len(seenTokens) == 0 {
				continue
			}
			n := 1 + rng.Intn(4)
			items := make([]SettleItem, n)
			complete := rng.Intn(2) == 0
			for k := range items {
				// Same generator, same point: the pairs are often deliberately wrong. And every so
				// often a pair is REPEATED inside one batch — the same request twice in the same
				// statement, which is its own identity mutation (round-6 §3.2): the second copy has
				// to agree with the first, whatever the first decided.
				if k > 0 && rng.Intn(4) == 0 {
					items[k] = items[rng.Intn(k)]
					continue
				}
				items[k] = SettleItem{
					SeqNumber: seenSeqs[rng.Intn(len(seenSeqs))],
					LockToken: seenTokens[rng.Intn(len(seenTokens))],
				}
				// Include matching pairs: live ones create receipts, expired ones must be refused.
				if mm := lockedPair(q); mm != nil && rng.Intn(3) == 0 {
					items[k] = SettleItem{SeqNumber: mm.seq, LockToken: mm.token}
				}
			}
			if complete {
				// MQLITE-104: deliberate single/batch-to-batch replays, including receipts for
				// other verbs and one-field identity mutations. Random pairs almost never hit one.
				if len(history) > 0 && rng.Intn(3) == 0 {
					h := history[rng.Intn(len(history))]
					q = h.q
					items[0] = SettleItem{SeqNumber: h.seq, LockToken: h.token}
					switch rng.Intn(4) {
					case 1:
						if q == qs[0] {
							q = qs[1]
						} else {
							q = qs[0]
						}
					case 2:
						items[0].SeqNumber++
					case 3:
						items[0].LockToken += "-wrong"
					}
				}
				res, err := e.CompleteBatch(ctx, q, items)
				if err != nil {
					t.Fatalf("round %d: CompleteBatch: %v", i, err)
				}
				if len(res) != len(items) {
					t.Fatalf("round %d: CompleteBatch returned %d results for %d items", i, len(res), len(items))
				}
				for k, r := range res {
					want := m.settle(q, items[k].SeqNumber, items[k].LockToken, "completed", 0, [2]string{})
					if r.Ok != want || r.SeqNumber != items[k].SeqNumber || r.LockedUntilMs != 0 {
						t.Fatalf(`round %d: BATCH SETTLE DISAGREEMENT
  item   : seq=%d token=%s in queue %s
  engine : ok=%v
  model  : ok=%v`, i, items[k].SeqNumber, items[k].LockToken, q, r.Ok, want)
					}
					if r.Ok {
						history = append(history, issued{q: q, seq: items[k].SeqNumber, token: items[k].LockToken, verb: "completed"})
					}
				}
			} else {
				res, err := e.RenewBatch(ctx, q, items)
				if err != nil {
					t.Fatalf("round %d: RenewBatch: %v", i, err)
				}
				if len(res) != len(items) {
					t.Fatalf("round %d: RenewBatch returned %d results for %d items", i, len(res), len(items))
				}
				for k, r := range res {
					want := m.renew(q, items[k].SeqNumber, items[k].LockToken)
					var until int64
					if want {
						until = m.msgs[items[k].SeqNumber].lockedUntil
						renewals++
					}
					if r.Ok != want || r.SeqNumber != items[k].SeqNumber || r.LockedUntilMs != until {
						t.Fatalf(`round %d: RENEW DISAGREEMENT
  item   : seq=%d token=%s in queue %s
  engine : ok=%v  (a renewal may only succeed for a lock you actually hold)
  model  : ok=%v`, i, items[k].SeqNumber, items[k].LockToken, q, r.Ok, want)
					}
				}
			}

		case 34, 35, 36, 37: // Single Renew must extend a live lease and refuse an expired one.
			mm := lockedPair(q)
			if mm == nil {
				continue
			}
			if rng.Intn(2) == 0 {
				advanceTo(mm.lockedUntil + int64(rng.Intn(3)-1))
			}
			want := m.renew(q, mm.seq, mm.token)
			err := e.Renew(ctx, q, mm.seq, mm.token)
			if (err == nil) != want || (err != nil && !errors.Is(err, ErrLockLost)) {
				t.Fatalf("round %d: Renew(seq=%d now=%d): err=%v, model ok=%v", i, mm.seq, m.now, err, want)
			}
			if want {
				renewals++
			}
			rows, err := e.Peek(ctx, q, PeekOptions{FromSeq: mm.seq, Max: 1})
			if err != nil || len(rows) != 1 || rows[0].SeqNumber != mm.seq || rows[0].LockedUntilMs != mm.lockedUntil {
				t.Fatalf("round %d: Renew persisted deadline: rows=%+v err=%v, model=%d", i, rows, err, mm.lockedUntil)
			}

		case 38, 39, 40, 41: // Expire leases WITHOUT maintenance; rows and tokens stay present.
			if mm := lockedPair(q); mm != nil {
				advanceTo(mm.lockedUntil + int64(rng.Intn(3)-1))
			} else {
				advanceTo(m.now + 1)
			}

		case 42, 43, 44, 45: // Deliberately replay at both sides of the receipt's expiry.
			if len(history) == 0 {
				continue
			}
			h := history[rng.Intn(len(history))]
			key := modelReceiptKey(h.q, h.seq, h.token, h.verb, h.delay, h.reason)
			advanceTo(m.receipts[key] + int64(rng.Intn(3)-1))
			want := m.settle(h.q, h.seq, h.token, h.verb, h.delay, h.reason)
			err := settleEngine(h.q, h.seq, h.token, h.verb, h.delay, h.reason)
			if (err == nil) != want || (err != nil && !errors.Is(err, ErrLockLost)) {
				t.Fatalf("round %d: receipt replay at %d (expires %d): err=%v, model ok=%v",
					i, m.now, m.receipts[key], err, want)
			}
			if want {
				liveReplays++
			} else {
				expiredReplays++
			}

		default: // settle — and here is the point: the arguments are often WRONG on purpose.
			if len(seenSeqs) == 0 || len(seenTokens) == 0 {
				continue
			}
			verb := verbs[rng.Intn(len(verbs))]
			seq := seenSeqs[rng.Intn(len(seenSeqs))]       // maybe not a message you hold
			token := seenTokens[rng.Intn(len(seenTokens))] // maybe somebody else's token, maybe stale
			tq := qs[rng.Intn(queues)]                     // maybe the wrong queue entirely
			delay := delays[rng.Intn(len(delays))]         // and maybe not the delay the first call used
			reason := reasons[rng.Intn(len(reasons))]      // nor the reason
			if mm := lockedPair(tq); mm != nil && rng.Intn(3) == 0 {
				seq, token = mm.seq, mm.token
			}

			// A third of the time, replay a request this run already made — the only way the
			// receipt path gets walked at all. Half of those replays mutate the arguments.
			if len(history) > 0 && rng.Intn(3) == 0 {
				h := history[rng.Intn(len(history))]
				tq, seq, token, verb = h.q, h.seq, h.token, h.verb
				delay, reason = h.delay, h.reason
				if rng.Intn(2) == 0 { // ... and change what it would DO
					delay = delays[rng.Intn(len(delays))]
					reason = reasons[rng.Intn(len(reasons))]
				}
			}
			if mm := m.msgs[seq]; mm != nil && mm.queue == tq && mm.state == StateLocked &&
				mm.token == token && mm.lockedUntil <= m.now {
				expiredLocks++
			}

			want := m.settle(tq, seq, token, verb, delay, reason)
			err := settleEngine(tq, seq, token, verb, delay, reason)
			got := err == nil

			if got != want || (err != nil && !errors.Is(err, ErrLockLost)) {
				t.Fatalf(`round %d: SETTLE DISAGREEMENT
  request: %s(queue=%s seq=%d token=%s delay=%d reason=%q) at %d
  engine : ok=%v (err=%v)
  model  : ok=%v
  the model requires an unexpired lease or a live receipt for the exact request
  — SAME message, SAME verb, SAME arguments.`,
					i, verb, tq, seq, token, delay, reason, m.now, got, err, want)
			}
			if got { // it left a receipt — so it is worth replaying
				history = append(history, issued{q: tq, seq: seq, token: token, verb: verb, delay: delay, reason: reason})
			}
		}

		// After every operation, the engine's view of the world must match the model's.
		for _, cq := range qs {
			wa, wl, wd, wdef, wsch, wt := m.counts(cq)
			st, err := e.Stats(ctx, cq)
			if err != nil {
				t.Fatalf("round %d: stats: %v", i, err)
			}
			if st.Active != wa || st.Locked != wl || st.DeadLettered != wd || st.Deferred != wdef || st.Scheduled != wsch || st.Total != wt {
				t.Fatalf(`round %d: STATE DIVERGENCE in %s
  engine: active=%d locked=%d dead=%d deferred=%d scheduled=%d total=%d
  model : active=%d locked=%d dead=%d deferred=%d scheduled=%d total=%d`,
					i, cq, st.Active, st.Locked, st.DeadLettered, st.Deferred, st.Scheduled, st.Total,
					wa, wl, wd, wdef, wsch, wt)
			}
		}
	}
	t.Logf("%d operations agreed: %d expired-lock requests, %d live and %d expired receipt replays, %d live renewals",
		rounds, expiredLocks, liveReplays, expiredReplays, renewals)
}

func stateOf(m *modelMsg) State {
	if m == nil {
		return "(unknown)"
	}
	return m.state
}
