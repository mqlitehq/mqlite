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
	"path/filepath"
	"testing"
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
	receipts map[string]bool // key: queue|seq|token|verb
	maxDeliv int
}

func rkey(queue string, seq int64, token, verb string) string {
	return fmt.Sprintf("%s|%d|%s|%s", queue, seq, token, verb)
}

// settle returns what the ENGINE is expected to answer for this exact request.
//
// This is the entire specification of settlement, and it is three lines: you may settle a message
// you currently hold the lock on; replaying the SAME request that already succeeded is an
// idempotent success; everything else is a lost lock. Note what is NOT here — nothing says a
// token vouches for a different message, or that one verb inherits another's receipt.
func (m *model) settle(queue string, seq int64, token, verb string) (ok bool) {
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
		m.receipts[rkey(queue, seq, token, verb)] = true
		return true
	}
	// A replay of the SAME request that already succeeded.
	return m.receipts[rkey(queue, seq, token, verb)]
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
	for _, seed := range []int64{1, 2, 3} {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) { runModel(t, seed) })
	}
}

func runModel(t *testing.T, seed int64) {
	const (
		queues   = 2
		rounds   = 4000
		maxDeliv = 3
	)
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

	settleEngine := func(q string, seq int64, token, verb string) error {
		switch verb {
		case "completed":
			return e.Complete(ctx, q, seq, token)
		case "abandoned":
			return e.Abandon(ctx, q, seq, token, 0)
		case "dead_lettered":
			return e.Reject(ctx, q, seq, token, ReasonAppRequested, "")
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
				// Same generator, same point: the pairs are often deliberately wrong.
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
					want := m.settle(q, items[k].SeqNumber, items[k].LockToken, "completed")
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

			want := m.settle(tq, seq, token, verb)
			err := settleEngine(tq, seq, token, verb)
			got := err == nil

			if got != want {
				t.Fatalf(`round %d: SETTLE DISAGREEMENT
  request: %s(queue=%s seq=%d token=%s)
  engine : ok=%v (err=%v)
  model  : ok=%v
  the model is the specification: a settle succeeds only for a message you hold the lock on, or as
  an idempotent replay of that exact request.`, i, verb, tq, seq, token, got, err, want)
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
