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
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// ─── the model: what a queue MEANS ────────────────────────────────────────────

type modelMsg struct {
	seq    int64
	state  State  // active | locked | dead_lettered | deferred  ("" = gone)
	token  string // the live lock token, when locked
	queue  string
	delivs int
}

type model struct {
	msgs map[int64]*modelMsg // by seq
	// receipts: what a settle promised, keyed by the REQUEST it settled.
	receipts map[string]bool // key: queue|seq|token|verb|args
	maxDeliv int
}

func rkey(queue string, seq int64, token, verb, args string) string {
	return fmt.Sprintf("%s|%d|%s|%s|%s", queue, seq, token, verb, args)
}

// settle returns what the ENGINE is expected to answer for this exact request.
//
// This is the entire specification of settlement, and it is three lines: you may settle a message
// you currently hold the lock on; replaying the SAME request that already succeeded is an
// idempotent success; everything else is a lost lock.
//
// Note what is NOT here — nothing says a token vouches for a different message, that one verb
// inherits another's receipt, or that a request may keep its success when its ARGUMENTS change.
// `args` is what makes that last one checkable: it carries the parameters that change what the
// settle DOES (Abandon's delay, Reject's reason/description), so a replay that alters them is a
// different request and gets no receipt. The model was blind to this bug for three rounds for one
// reason — it always passed the same delay and the same reason, so the arguments never varied and
// the spec was never exercised. A model only finds what its generator is willing to say.
func (m *model) settle(queue string, seq int64, token, verb, args string) (ok bool) {
	msg := m.msgs[seq]
	if msg != nil && msg.queue == queue && msg.state == StateLocked && msg.token == token {
		switch verb {
		case "completed":
			msg.state = "" // gone
		case "abandoned":
			msg.delivs++
			if msg.delivs >= m.maxDeliv {
				msg.state = StateDeadLettered
			} else {
				msg.state = StateActive
			}
		case "dead_lettered":
			msg.state = StateDeadLettered
		case "deferred":
			msg.state = StateDeferred
		}
		msg.token = ""
		m.receipts[rkey(queue, seq, token, verb, args)] = true
		return true
	}
	// A replay of the SAME request that already succeeded.
	return m.receipts[rkey(queue, seq, token, verb, args)]
}

func (m *model) counts(queue string) (active, locked, dead, deferred, total int64) {
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
		// -race makes every SQLite call ~10x dearer and the package shares one 10m budget. The
		// model's value is in the mutations it CATCHES, and it catches them in the first few
		// hundred rounds (removing args from the receipt key fails at round 29/48/803), so a
		// shorter race run loses no signal — the long run happens on every non-race CI leg.
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
	)
	rounds := 4000
	if raceEnabled {
		rounds = 1200
	}
	ctx := context.Background()
	e, err := Open(ctx, Options{
		DB: "file:" + filepath.Join(t.TempDir(), "mq.db"), DisableBackground: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	qs := []string{"q0", "q1"}
	for _, q := range qs {
		mustQueue(t, e, q, QueueConfig{LockDurationMs: 600_000, MaxDeliveryCount: maxDeliv})
	}
	m := &model{msgs: map[int64]*modelMsg{}, receipts: map[string]bool{}, maxDeliv: maxDeliv}

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
	// succeed) and sometimes with its arguments changed (it must NOT inherit the first one's
	// success).
	type issued struct {
		q, token, verb string
		seq            int64
		delay          int64
		reason         [2]string
	}
	var history []issued

	// The argument sets a settle may be replayed with. Two of each, so a replay can differ from the
	// original in exactly the way a real client's would: same message, same token, same verb, a
	// different backoff or a different dead-letter reason.
	delays := []int64{0, 30_000}
	reasons := [][2]string{{ReasonAppRequested, ""}, {"PoisonMessage", "gave up after 3 tries"}}

	// settleArgs mirrors the ENGINE's own encoding on purpose: the model asserts that the arguments
	// are part of the request's identity, not how they happen to be hashed.
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
				seenTokens = append(seenTokens, got.LockToken)
			}

		case 26, 27, 28, 29, 30, 31, 32, 33: // BATCH settle/renew — the newest, riskiest code
			if len(seenSeqs) == 0 || len(seenTokens) == 0 {
				continue
			}
			n := 1 + rng.Intn(4)
			items := make([]SettleItem, n)
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
			}
			if rng.Intn(2) == 0 {
				res, err := e.CompleteBatch(ctx, q, items)
				if err != nil {
					t.Fatalf("round %d: CompleteBatch: %v", i, err)
				}
				for k, r := range res {
					want := m.settle(q, items[k].SeqNumber, items[k].LockToken, "completed", "")
					if r.Ok != want {
						t.Fatalf(`round %d: BATCH SETTLE DISAGREEMENT
  item   : seq=%d token=%s in queue %s
  engine : ok=%v
  model  : ok=%v`, i, items[k].SeqNumber, items[k].LockToken, q, r.Ok, want)
					}
				}
			} else {
				res, err := e.RenewBatch(ctx, q, items)
				if err != nil {
					t.Fatalf("round %d: RenewBatch: %v", i, err)
				}
				for k, r := range res {
					// Renewal changes no state; ok means "you really hold this lock".
					mm := m.msgs[items[k].SeqNumber]
					want := mm != nil && mm.queue == q && mm.state == StateLocked && mm.token == items[k].LockToken
					if r.Ok != want {
						t.Fatalf(`round %d: RENEW DISAGREEMENT
  item   : seq=%d token=%s in queue %s
  engine : ok=%v  (a renewal may only succeed for a lock you actually hold)
  model  : ok=%v`, i, items[k].SeqNumber, items[k].LockToken, q, r.Ok, want)
					}
				}
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

			// Only the arguments THIS verb actually reads belong to its identity; Complete and
			// Defer take none, so they must stay replayable across everything else that varies.
			args := ""
			switch verb {
			case "abandoned":
				args = fmt.Sprintf("delay=%d", delay)
			case "dead_lettered":
				args = fmt.Sprintf("reason=%s|desc=%s", reason[0], reason[1])
			}

			want := m.settle(tq, seq, token, verb, args)
			err := settleEngine(tq, seq, token, verb, delay, reason)
			got := err == nil

			if got != want {
				t.Fatalf(`round %d: SETTLE DISAGREEMENT
  request: %s(queue=%s seq=%d token=%s %s)
  engine : ok=%v (err=%v)
  model  : ok=%v
  the model is the specification: a settle succeeds only for a message you hold the lock on, or as
  an idempotent replay of that exact request — SAME message, SAME verb, SAME arguments.`,
					i, verb, tq, seq, token, args, got, err, want)
			}
			if got { // it left a receipt — so it is worth replaying
				history = append(history, issued{q: tq, seq: seq, token: token, verb: verb, delay: delay, reason: reason})
			}
		}

		// After every operation, the engine's view of the world must match the model's.
		for _, cq := range qs {
			wa, wl, wd, wdef, wt := m.counts(cq)
			st, err := e.Stats(ctx, cq)
			if err != nil {
				t.Fatalf("round %d: stats: %v", i, err)
			}
			if st.Active != wa || st.Locked != wl || st.DeadLettered != wd || st.Deferred != wdef || st.Total != wt {
				t.Fatalf(`round %d: STATE DIVERGENCE in %s
  engine: active=%d locked=%d dead=%d deferred=%d total=%d
  model : active=%d locked=%d dead=%d deferred=%d total=%d`,
					i, cq, st.Active, st.Locked, st.DeadLettered, st.Deferred, st.Total,
					wa, wl, wd, wdef, wt)
			}
		}
	}
	t.Logf("%d operations, engine and model agreed at every step", rounds)
}

func stateOf(m *modelMsg) State {
	if m == nil {
		return "(unknown)"
	}
	return m.state
}
