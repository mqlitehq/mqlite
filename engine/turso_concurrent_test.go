package engine

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// TestTursoConcurrent stresses the two correctness invariants that the remote
// connection pool (MaxOpen=4) could threaten but the single-logical-writer model
// must still uphold against a real Turso/libSQL primary (MQLITE-4, evaluation
// Bug-5): concurrent dedup must collapse to one row, and concurrent claims must
// never hand the same message to two consumers. Same creds gating as the other
// Turso tests — skipped unless MQLITE_TEST_DB is set, run live in nightly.
func TestTursoConcurrent(t *testing.T) {
	dsn := os.Getenv("MQLITE_TEST_DB")
	if dsn == "" {
		t.Skip("set MQLITE_TEST_DB (and MQLITE_TEST_DB_AUTH_TOKEN) to run the Turso concurrency test")
	}
	token := os.Getenv("MQLITE_TEST_DB_AUTH_TOKEN")

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Background loops off so the run is deterministic: a reaper must not reclaim a
	// lock mid-test and make a message look double-delivered.
	e, err := Open(ctx, Options{DB: dsn, AuthToken: token, DisableBackground: true})
	if err != nil {
		t.Fatalf("open remote: %v", err)
	}
	defer e.Close()
	if !e.Remote() {
		t.Fatalf("expected remote store for dsn %q", dsn)
	}

	q := fmt.Sprintf("cc_%d", time.Now().UnixNano())
	if err := e.CreateQueue(ctx, q, QueueConfig{
		LockDurationMs: 60000, MaxDeliveryCount: 5, DedupWindowMs: (10 * time.Minute).Milliseconds(),
	}); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = e.db.sql.ExecContext(bg, `DELETE FROM messages WHERE queue=?`, q)
		_, _ = e.db.sql.ExecContext(bg, `DELETE FROM queues WHERE name=?`, q)
	})

	// ── Part 1: concurrent dedup — N goroutines race to send the SAME message id.
	// All must collapse to one row (one seq), exercising the dedup path through the
	// 4-conn remote pool simultaneously.
	const N = 12
	var wg sync.WaitGroup
	seqs := make([]int64, N)
	errs := make([]error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			seqs[i], errs[i] = e.SendOne(ctx, q, OutMessage{Body: []byte("dup"), MessageID: "same"})
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent send %d: %v", i, err)
		}
	}
	for i := 1; i < N; i++ {
		if seqs[i] != seqs[0] {
			t.Fatalf("concurrent dedup: seq %d != %d — same message_id produced distinct rows", seqs[i], seqs[0])
		}
	}
	if m, _ := e.Stats(ctx, q); m.Active != 1 {
		t.Fatalf("concurrent dedup: want exactly 1 active row, got %d", m.Active)
	}

	// ── Part 2: concurrent claim — no double-delivery. Seed M distinct messages
	// (plus the 1 dedup row already active = M+1), then drain with C concurrent
	// consumers; every seq must be claimed by exactly one consumer.
	const M = 40
	for i := 0; i < M; i++ {
		if _, err := e.SendOne(ctx, q, OutMessage{
			Body: []byte(fmt.Sprintf("m%d", i)), MessageID: fmt.Sprintf("claim-%d", i),
		}); err != nil {
			t.Fatalf("seed send %d: %v", i, err)
		}
	}
	const want = M + 1

	const C = 6
	var (
		cwg     sync.WaitGroup
		mu      sync.Mutex
		claimed = make(map[int64]int)
		cerrs   = make(chan error, C)
	)
	for c := 0; c < C; c++ {
		cwg.Add(1)
		go func() {
			defer cwg.Done()
			for {
				ms, err := e.Receive(ctx, q, ReceiveOptions{MaxMessages: 1})
				if err != nil {
					cerrs <- err
					return
				}
				if len(ms) == 0 {
					return // drained
				}
				mu.Lock()
				claimed[ms[0].SeqNumber]++
				mu.Unlock()
				if err := e.Complete(ctx, q, ms[0].SeqNumber, ms[0].LockToken); err != nil {
					cerrs <- err
					return
				}
			}
		}()
	}
	cwg.Wait()
	close(cerrs)
	for err := range cerrs {
		t.Fatalf("concurrent consumer: %v", err)
	}

	for seq, n := range claimed {
		if n != 1 {
			t.Fatalf("seq %d claimed %d times — double-delivery under concurrent claim", seq, n)
		}
	}
	if len(claimed) != want {
		t.Fatalf("claimed %d distinct messages, want %d (lost or duplicated under concurrency)", len(claimed), want)
	}
	if m, _ := e.Stats(ctx, q); m.Total != 0 {
		t.Fatalf("queue should be fully drained, got %+v", m)
	}

	t.Logf("Turso concurrency OK: dedup collapsed %d→1 under race; %d messages claimed exactly once by %d consumers", N, want, C)
}
