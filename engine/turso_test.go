package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mqlitehq/mqlite/internal/authkey"
)

// Live remote Turso/libSQL tests — all gated on MQLITE_TEST_DB (skipped otherwise,
// run in turso-nightly.yml). Sections: lifecycle smoke (TestTursoIntegration),
// correctness paths (TestTursoExtended), and concurrency under the 4-conn remote
// pool (TestTursoConcurrent). The hermetic retry-classification tests live in
// storage_test.go.

// Register after the engine's Close cleanup: testing runs cleanups last-in-first-out.
// Only exact names owned by the test are removed, including auxiliary rows that
// have no queue foreign key. Attempt every statement and fail visibly on errors.
func cleanupTursoQueues(t *testing.T, e *Engine, queues ...string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		failed := false
		for _, q := range queues {
			for _, stmt := range []string{
				`DELETE FROM messages WHERE queue=?`,
				`DELETE FROM dedup WHERE queue=?`,
				`DELETE FROM settlement_receipts WHERE queue=?`,
				`DELETE FROM receive_attempts WHERE queue=?`,
				`DELETE FROM subscriptions WHERE subscription=?`,
				`DELETE FROM queues WHERE name=?`,
			} {
				if _, err := e.db.exec(ctx, stmt, q); err != nil {
					failed = true
					t.Errorf("cleanup queue %q (%s): %v", q, stmt, err)
				}
			}
		}
		if !failed {
			t.Logf("Turso cleanup OK: removed %d test queue(s) and their auxiliary rows", len(queues))
		}
	})
}

// TestTursoIntegration runs the full lifecycle against a real remote Turso/libSQL
// database. It is skipped unless MQLITE_TEST_DB is set, so `go test` stays
// hermetic by default. The connection string and token come from the
// environment only — never compiled into the binary.
//
//	MQLITE_TEST_DB=libsql://<db>.turso.io
//	MQLITE_TEST_DB_AUTH_TOKEN=<jwt>
func TestTursoIntegration(t *testing.T) {
	dsn := os.Getenv("MQLITE_TEST_DB")
	if dsn == "" {
		t.Skip("set MQLITE_TEST_DB (and MQLITE_TEST_DB_AUTH_TOKEN) to run the Turso integration test")
	}
	token := os.Getenv("MQLITE_TEST_DB_AUTH_TOKEN")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	e, err := Open(ctx, Options{DB: dsn, AuthToken: token})
	if err != nil {
		t.Fatalf("open remote: %v", err)
	}
	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Errorf("close remote: %v", err)
		}
	})
	if !e.Remote() {
		t.Fatalf("expected remote store for dsn %q", dsn)
	}

	// unique queue per run so repeated runs don't collide.
	q := fmt.Sprintf("itest_%d", time.Now().UnixNano())
	cleanupTursoQueues(t, e, q)
	if err := e.CreateQueue(ctx, q, QueueConfig{LockDurationMs: 30000, MaxDeliveryCount: 5}); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	// send a batch
	seqs, err := e.Send(ctx, q,
		OutMessage{Body: []byte("one"), MessageID: "a"},
		OutMessage{Body: []byte("two"), MessageID: "b"},
	)
	if err != nil || len(seqs) != 2 {
		t.Fatalf("send batch: %v %v", err, seqs)
	}
	t.Logf("Turso send ok: seqs=%v queue=%s", seqs, q)

	// receive + complete the first
	m := recvOne(t, e, q)
	if m == nil {
		t.Fatal("expected a message from Turso")
	}
	if err := e.Complete(ctx, q, m.SeqNumber, m.LockToken); err != nil {
		t.Fatalf("complete: %v", err)
	}

	// receive + abandon + redeliver the second (delivery_count must increase)
	m2 := recvOne(t, e, q)
	if m2 == nil {
		t.Fatal("expected the second message")
	}
	if err := e.Abandon(ctx, q, m2.SeqNumber, m2.LockToken, 0); err != nil {
		t.Fatalf("abandon: %v", err)
	}
	m3 := recvOne(t, e, q)
	if m3 == nil || m3.SeqNumber != m2.SeqNumber || m3.DeliveryCount <= m2.DeliveryCount {
		t.Fatalf("expected redelivery with higher delivery_count: %+v -> %+v", m2, m3)
	}
	if err := e.Complete(ctx, q, m3.SeqNumber, m3.LockToken); err != nil {
		t.Fatalf("complete 2: %v", err)
	}

	mt, err := e.Stats(ctx, q)
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}
	if mt.Total != 0 {
		t.Fatalf("queue should be drained, got %+v", mt)
	}
	t.Logf("Turso integration OK: round-trip + abandon/redeliver + drain verified")
}

// TestTursoAccessKeys exercises real indexed authentication, independently
// revocable credentials and restart persistence through the four-connection pool.
func TestTursoAccessKeys(t *testing.T) {
	dsn := os.Getenv("MQLITE_TEST_DB")
	if dsn == "" {
		t.Skip("set MQLITE_TEST_DB (and MQLITE_TEST_DB_AUTH_TOKEN) to run the Turso integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	var now atomic.Int64
	now.Store(time.Now().UnixMilli())
	opts := Options{DB: dsn, AuthToken: os.Getenv("MQLITE_TEST_DB_AUTH_TOKEN"), DisableBackground: true, Now: now.Load}
	e, err := Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !e.Remote() || e.db.sql.Stats().MaxOpenConnections != 4 {
		t.Fatal("access key live test requires the four-connection remote pool")
	}
	var ids []string
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cleanupCancel()
		failed := false
		for _, id := range ids {
			if _, err := e.db.exec(cleanupCtx, `DELETE FROM access_keys WHERE id = ?`, id); err != nil {
				failed = true
				t.Errorf("cleanup test access key %s: %v", id, err)
			}
		}
		if !failed {
			t.Logf("Turso cleanup OK: removed %d test access keys", len(ids))
		}
		if err := e.Close(); err != nil {
			t.Errorf("close remote: %v", err)
		}
	})
	var keys []AccessKey
	var tokens []string
	for _, p := range []KeyPermissions{KeySend, KeyListen, KeySend | KeyListen, KeyManage} {
		id, err := authkey.GenerateID()
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id) // Track before the write, including an uncertain result.
		key, token, err := e.CreateAccessKey(ctx, CreateAccessKeyOptions{ID: id, Name: "turso access key test", Permissions: p, ExpiresAtMs: now.Load() + 60_000})
		if err != nil {
			t.Fatal(err)
		}
		keys, tokens = append(keys, key), append(tokens, token)
		if got, err := e.AuthenticateAccessKey(ctx, token); err != nil || got != key {
			t.Fatalf("create not visible to authentication: %+v %v", got, err)
		}
	}
	// Four callers competing for a single preselected ID produce one record.
	id, err := authkey.GenerateID()
	if err != nil {
		t.Fatal(err)
	}
	ids = append(ids, id)
	var wg sync.WaitGroup
	var created atomic.Int32
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := e.CreateAccessKey(ctx, CreateAccessKeyOptions{ID: id, Name: "turso same id", Permissions: KeySend})
			if err == nil {
				created.Add(1)
			} else if !errors.Is(err, ErrKeyConflict) {
				t.Errorf("concurrent creation: %v", err)
			}
		}()
	}
	wg.Wait()
	if created.Load() != 1 {
		t.Fatalf("same ID produced %d credentials", created.Load())
	}
	// Revocation's completed response must fence every later pooled lookup.
	if err := e.RevokeAccessKey(ctx, keys[0].ID); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := e.AuthenticateAccessKey(ctx, tokens[0]); !errors.Is(err, ErrUnauthenticated) {
				t.Errorf("post-revoke pooled lookup accepted: %v", err)
			}
			if _, err := e.AuthenticateAccessKey(ctx, tokens[1]); err != nil {
				t.Errorf("independent pooled key failed: %v", err)
			}
		}()
	}
	wg.Wait()
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	for _, operation := range []func() error{
		func() error {
			_, _, err := e.CreateAccessKey(ctx, CreateAccessKeyOptions{ID: id, Name: "closed", Permissions: KeySend})
			return err
		},
		func() error { _, err := e.ListAccessKeys(ctx, "", 1); return err },
		func() error { return e.RevokeAccessKey(ctx, id) },
		func() error { _, err := e.AuthenticateAccessKey(ctx, tokens[1]); return err },
	} {
		if err := operation(); !errors.Is(err, ErrClosed) {
			t.Fatalf("remote access key after close: %v", err)
		}
	}
	// Use a separate variable so failed reopening does not erase the cleanup handle.
	reopened, err := Open(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	e = reopened
	if _, err := e.AuthenticateAccessKey(ctx, tokens[0]); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("restart reactivated revoked key: %v", err)
	}
	for i := 1; i < len(tokens); i++ {
		if got, err := e.AuthenticateAccessKey(ctx, tokens[i]); err != nil || got != keys[i] {
			t.Fatalf("restart lost key: %+v %v", got, err)
		}
	}
	var listed []AccessKey
	for cursor := ""; ; {
		page, err := e.ListAccessKeys(ctx, cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		listed = append(listed, page.Keys...)
		if page.NextAfterID == "" {
			break
		}
		if page.NextAfterID <= cursor {
			t.Fatal("remote cursor failed to advance")
		}
		cursor = page.NextAfterID
	}
	for _, id := range ids {
		found := 0
		for _, key := range listed {
			if key.ID == id {
				found++
			}
		}
		if found != 1 {
			t.Fatalf("pagination found test key %s %d times", id, found)
		}
	}
	// Exercise the indexed tuple cursor against real libSQL, including timestamp
	// ties and a newly inserted head between pages. Compare only this test's IDs.
	owned := map[string]bool{}
	for _, id := range ids {
		owned[id] = true
	}
	var expectedDescending []AccessKey
	for _, key := range listed {
		if owned[key.ID] {
			expectedDescending = append(expectedDescending, key)
		}
	}
	sort.Slice(expectedDescending, func(i, j int) bool {
		if expectedDescending[i].CreatedAtMs != expectedDescending[j].CreatedAtMs {
			return expectedDescending[i].CreatedAtMs > expectedDescending[j].CreatedAtMs
		}
		return expectedDescending[i].ID > expectedDescending[j].ID
	})
	var descending []AccessKey
	var previous AccessKey
	seen := map[string]bool{}
	newHeadID := ""
	for cursor := ""; ; {
		page, err := e.ListAccessKeysWithOptions(ctx, ListAccessKeysOptions{Sort: KeySortCreatedDesc, AfterID: cursor, Limit: 2})
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range page.Keys {
			if seen[key.ID] || key.ID == newHeadID || previous.ID != "" && (key.CreatedAtMs > previous.CreatedAtMs || key.CreatedAtMs == previous.CreatedAtMs && key.ID >= previous.ID) {
				t.Fatal("remote creation-order pagination duplicated or misordered a key")
			}
			seen[key.ID], previous = true, key
			if owned[key.ID] {
				descending = append(descending, key)
			}
		}
		if cursor == "" {
			newHeadID, err = authkey.GenerateID()
			if err != nil {
				t.Fatal(err)
			}
			ids = append(ids, newHeadID)
			clockBeforeInsert := now.Load()
			newestTime := clockBeforeInsert
			for _, existing := range listed {
				if existing.CreatedAtMs > newestTime {
					newestTime = existing.CreatedAtMs
				}
			}
			now.Store(newestTime + 1)
			if _, _, err := e.CreateAccessKey(ctx, CreateAccessKeyOptions{ID: newHeadID, Name: "new pagination head", Permissions: KeySend}); err != nil {
				t.Fatal(err)
			}
			now.Store(clockBeforeInsert)
		}
		if page.NextAfterID == "" {
			break
		}
		if page.NextAfterID == cursor {
			t.Fatal("remote creation-order cursor failed to advance")
		}
		cursor = page.NextAfterID
	}
	if len(descending) != len(expectedDescending) {
		t.Fatal("remote creation-order pagination lost owned keys")
	}
	for i := range descending {
		if descending[i] != expectedDescending[i] {
			t.Fatalf("remote creation-order metadata differs at %d", i)
		}
	}
	head, err := e.ListAccessKeysWithOptions(ctx, ListAccessKeysOptions{Sort: KeySortCreatedDesc, Limit: 1})
	if err != nil || len(head.Keys) != 1 || head.Keys[0].ID != newHeadID {
		t.Fatalf("remote newest head is not visible: %+v %v", head, err)
	}
	missing, err := authkey.GenerateID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.ListAccessKeysWithOptions(ctx, ListAccessKeysOptions{Sort: KeySortCreatedDesc, AfterID: missing}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("remote missing creation cursor = %v", err)
	}
	t.Log("Turso creation-order key pagination OK: ties, stable cursor, new head, metadata and missing cursor verified")
	// Measure actual network lookup cost without claiming it equals static auth.
	latencies := make([]time.Duration, 20)
	var total time.Duration
	for i := range latencies {
		started := time.Now()
		if _, err := e.AuthenticateAccessKey(ctx, tokens[1]); err != nil {
			t.Fatal(err)
		}
		latencies[i] = time.Since(started)
		total += latencies[i]
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	t.Logf("Turso indexed access key lookup: n=%d mean=%s p50=%s p95=%s throughput=%.2f/s", len(latencies), total/time.Duration(len(latencies)), latencies[9], latencies[18], float64(len(latencies))/total.Seconds())
	now.Store(keys[1].ExpiresAtMs)
	if _, err := e.AuthenticateAccessKey(ctx, tokens[1]); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("remote key accepted at its exact expiry: %v", err)
	}
}

// TestTursoExtended exercises the correctness paths the basic smoke test does not
// — dedup, idempotent receive/settle, DLQ -> redrive -> purge, the topic
// subscription isolation guard (#11), and topic fan-out + filter — against a real
// remote Turso/libSQL DB. Same gating as TestTursoIntegration. Background loops are
// disabled so the remote run is deterministic and not slowed by per-second reaper
// polling across the network.
func TestTursoExtended(t *testing.T) {
	dsn := os.Getenv("MQLITE_TEST_DB")
	if dsn == "" {
		t.Skip("set MQLITE_TEST_DB (and MQLITE_TEST_DB_AUTH_TOKEN) to run the Turso integration test")
	}
	token := os.Getenv("MQLITE_TEST_DB_AUTH_TOKEN")

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	e, err := Open(ctx, Options{DB: dsn, AuthToken: token, DisableBackground: true})
	if err != nil {
		t.Fatalf("open remote: %v", err)
	}
	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Errorf("close remote: %v", err)
		}
	})
	if !e.Remote() {
		t.Fatalf("expected remote store for dsn %q", dsn)
	}

	run := time.Now().UnixNano()
	q := fmt.Sprintf("xq_%d", run)
	topic := fmt.Sprintf("xt_%d", run)
	subAll := fmt.Sprintf("xs_all_%d", run)
	subPaid := fmt.Sprintf("xs_paid_%d", run)
	cleanupTursoQueues(t, e, q, subAll, subPaid)

	if err := e.CreateQueue(ctx, q, QueueConfig{
		LockDurationMs: 30000, MaxDeliveryCount: 5, DedupWindowMs: (10 * time.Minute).Milliseconds(),
	}); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	// 1) dedup: same message_id within the window collapses to one row.
	s1, err := e.SendOne(ctx, q, OutMessage{Body: []byte("dup"), MessageID: "k1"})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	s2, err := e.SendOne(ctx, q, OutMessage{Body: []byte("dup"), MessageID: "k1"})
	if err != nil {
		t.Fatalf("dedup send: %v", err)
	}
	if s1 != s2 {
		t.Fatalf("remote dedup: expected same seq, got %d and %d", s1, s2)
	}

	// 2) idempotent receive: same attempt-id replays the same lock token and does
	//    not burn delivery_count.
	b1, err := e.Receive(ctx, q, ReceiveOptions{MaxMessages: 1, AttemptID: "att-1"})
	if err != nil || len(b1) != 1 {
		t.Fatalf("receive att-1: %v %v", err, b1)
	}
	b2, err := e.Receive(ctx, q, ReceiveOptions{MaxMessages: 1, AttemptID: "att-1"})
	if err != nil || len(b2) != 1 {
		t.Fatalf("replay att-1: %v %v", err, b2)
	}
	if b1[0].LockToken != b2[0].LockToken || b1[0].DeliveryCount != b2[0].DeliveryCount {
		t.Fatalf("idempotent receive should replay same token/dc: %+v vs %+v", b1[0], b2[0])
	}

	// 3) idempotent settle: Complete twice with the same token both succeed.
	if err := e.Complete(ctx, q, b1[0].SeqNumber, b1[0].LockToken); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if err := e.Complete(ctx, q, b1[0].SeqNumber, b1[0].LockToken); err != nil {
		t.Fatalf("idempotent re-complete should succeed, got %v", err)
	}

	// 4) DLQ -> redrive (in place) -> dead-letter again -> purge.
	if _, err := e.SendOne(ctx, q, OutMessage{Body: []byte("poison"), MessageID: "k2"}); err != nil {
		t.Fatalf("send poison: %v", err)
	}
	pm := recvOne(t, e, q)
	if pm == nil {
		t.Fatal("expected poison message")
	}
	if err := e.Reject(ctx, q, pm.SeqNumber, pm.LockToken, ReasonAppRequested, "manual"); err != nil {
		t.Fatalf("deadletter: %v", err)
	}
	if dlq, _ := e.Peek(ctx, q, PeekOptions{State: StateDeadLettered}); len(dlq) != 1 {
		t.Fatalf("expected 1 in DLQ, got %d", len(dlq))
	}
	if n, err := e.Redrive(ctx, q, RedriveOptions{}); err != nil || n != 1 {
		t.Fatalf("redrive: n=%d err=%v", n, err)
	}
	rm := recvOne(t, e, q)
	if rm == nil {
		t.Fatal("expected redriven message back as active")
	}
	if err := e.Reject(ctx, q, rm.SeqNumber, rm.LockToken, ReasonAppRequested, "again"); err != nil {
		t.Fatalf("deadletter 2: %v", err)
	}
	if n, err := e.Purge(ctx, q, RedriveOptions{}); err != nil || n != 1 {
		t.Fatalf("purge: n=%d err=%v", n, err)
	}
	if dlq, _ := e.Peek(ctx, q, PeekOptions{State: StateDeadLettered}); len(dlq) != 0 {
		t.Fatalf("DLQ should be empty after purge, got %d", len(dlq))
	}

	// 5) #11 subscription isolation guard holds on the real remote too.
	if err := e.Subscribe(ctx, topic, subAll, nil); err != nil {
		t.Fatalf("create sub all: %v", err)
	}
	if err := e.Subscribe(ctx, topic, subPaid, &Filter{Expr: `subject startsWith "payment."`}); err != nil {
		t.Fatalf("create sub paid: %v", err)
	}
	if err := e.Subscribe(ctx, topic+"_b", subAll, nil); !errors.Is(err, ErrNameConflict) {
		t.Fatalf("cross-topic duplicate subscription should be ErrNameConflict, got %v", err)
	}

	// 6) topic fan-out + subject-prefix filter.
	if _, err := e.SendOne(ctx, topic, OutMessage{Body: []byte("o"), Subject: "order.created"}); err != nil {
		t.Fatalf("topic send: %v", err)
	}
	if _, err := e.SendOne(ctx, topic, OutMessage{Body: []byte("p"), Subject: "payment.captured"}); err != nil {
		t.Fatalf("topic send 2: %v", err)
	}
	allM, _ := e.Stats(ctx, subAll)
	paidM, _ := e.Stats(ctx, subPaid)
	if allM.Active != 2 || paidM.Active != 1 {
		t.Fatalf("fan-out/filter on remote: all=%d paid=%d (want 2/1)", allM.Active, paidM.Active)
	}

	t.Logf("Turso extended OK: dedup + idempotent receive/settle + DLQ redrive/purge + #11 subscription guard + topic filter")
}

// TestTursoConcurrent stresses the two correctness invariants that the remote
// connection pool (MaxOpen=4) could threaten but the single-logical-writer model
// must still uphold against a real Turso/libSQL primary (MQLITE-4, evaluation
// Bug-5): concurrent dedup must collapse to one row, and concurrent claims must
// never hand the same message to two consumers. Same creds gating as the other
// Turso tests — skipped unless MQLITE_TEST_DB is set, run live in nightly.
//
// Load is sized for real-remote timing, not a local DB: every queue op is a
// durable Hrana round-trip to the primary (~1s each), and the primary serializes
// writes, so C consumers contend for the 4-conn pool and lose claim races they
// retry. C=4 saturates the pool (its natural max-contention level); the assertions
// (no double-delivery, exact-once drain, dedup-collapse) are what matter, not
// throughput — the generous deadline is only a hang guard.
func TestTursoConcurrent(t *testing.T) {
	dsn := os.Getenv("MQLITE_TEST_DB")
	if dsn == "" {
		t.Skip("set MQLITE_TEST_DB (and MQLITE_TEST_DB_AUTH_TOKEN) to run the Turso concurrency test")
	}
	token := os.Getenv("MQLITE_TEST_DB_AUTH_TOKEN")

	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	start := time.Now()

	// Background loops off so the run is deterministic: a reaper must not reclaim a
	// lock mid-test and make a message look double-delivered.
	e, err := Open(ctx, Options{DB: dsn, AuthToken: token, DisableBackground: true})
	if err != nil {
		t.Fatalf("open remote: %v", err)
	}
	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Errorf("close remote: %v", err)
		}
	})
	if !e.Remote() {
		t.Fatalf("expected remote store for dsn %q", dsn)
	}

	q := fmt.Sprintf("cc_%d", time.Now().UnixNano())
	cleanupTursoQueues(t, e, q)
	if err := e.CreateQueue(ctx, q, QueueConfig{
		LockDurationMs: 60000, MaxDeliveryCount: 5, DedupWindowMs: (10 * time.Minute).Milliseconds(),
	}); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	// ── Part 1: concurrent dedup — N goroutines race to send the SAME message id.
	// All must collapse to one row (one seq), exercising the dedup path through the
	// 4-conn remote pool simultaneously.
	const N = 8
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
	t.Logf("Part 1 (concurrent dedup, N=%d) collapsed to 1 row in %s", N, time.Since(start).Round(time.Second))

	// ── Part 2: concurrent claim — no double-delivery. Seed M distinct messages
	// (plus the 1 dedup row already active = M+1), then drain with C concurrent
	// consumers; every seq must be claimed by exactly one consumer.
	const M = 24
	for i := 0; i < M; i++ {
		if _, err := e.SendOne(ctx, q, OutMessage{
			Body: []byte(fmt.Sprintf("m%d", i)), MessageID: fmt.Sprintf("claim-%d", i),
		}); err != nil {
			t.Fatalf("seed send %d: %v", i, err)
		}
	}
	const want = M + 1

	const C = 4
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

	t.Logf("Turso concurrency OK in %s: dedup collapsed %d→1 under race; %d messages claimed exactly once by %d consumers",
		time.Since(start).Round(time.Second), N, want, C)
}

// TestTursoBatchSettle exercises the batch settle path against a REAL remote libSQL DB, which is
// the backend it exists for: RenewBatch/CompleteBatch are set-based precisely because every
// statement on Hrana is its own network round trip. It also proves the SQL those two rely on —
// a row-value `(id, lock_token) IN (VALUES ...)` set and `UPDATE ... RETURNING` — actually works
// remotely, rather than only on the local pure-Go SQLite driver (MQLITE-97).
//
// Same gating as the other Turso tests: skipped unless MQLITE_TEST_DB is set.
func TestTursoBatchSettle(t *testing.T) {
	dsn := os.Getenv("MQLITE_TEST_DB")
	if dsn == "" {
		t.Skip("set MQLITE_TEST_DB (and MQLITE_TEST_DB_AUTH_TOKEN) to run the Turso integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	e, err := Open(ctx, Options{DB: dsn, AuthToken: os.Getenv("MQLITE_TEST_DB_AUTH_TOKEN"), DisableBackground: true})
	if err != nil {
		t.Fatalf("open remote: %v", err)
	}
	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Errorf("close remote: %v", err)
		}
	})

	// Keep this test's reserved fixed name so a run can drain leftovers from older
	// runs that did not clean up. Remove its rows and metadata before closing the DB.
	const q = "batch-settle-turso"
	cleanupTursoQueues(t, e, q)
	if err := e.CreateQueue(ctx, q, QueueConfig{LockDurationMs: 600_000, MaxDeliveryCount: 10}); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if _, err := e.Purge(ctx, q, RedriveOptions{}); err != nil { // any dead letters from a past run
		t.Fatalf("purge: %v", err)
	}
	for { // and any live messages from a past run
		leftover, err := e.Receive(ctx, q, ReceiveOptions{MaxMessages: 256, Mode: ReceiveAndDelete})
		if err != nil {
			t.Fatalf("drain: %v", err)
		}
		if len(leftover) == 0 {
			break
		}
	}

	const n = 32 // enough to be a real batch; small enough to keep the remote run quick
	seed := make([]OutMessage, n)
	for i := range seed {
		seed[i] = OutMessage{Body: []byte("m")}
	}
	if _, err := e.Send(ctx, q, seed...); err != nil {
		t.Fatalf("send: %v", err)
	}
	msgs, err := e.Receive(ctx, q, ReceiveOptions{MaxMessages: n})
	if err != nil || len(msgs) != n {
		t.Fatalf("receive: got %d (err %v)", len(msgs), err)
	}

	items := make([]SettleItem, 0, n+1)
	for _, m := range msgs {
		items = append(items, SettleItem{SeqNumber: m.SeqNumber, LockToken: m.LockToken})
	}
	items = append(items, SettleItem{SeqNumber: msgs[0].SeqNumber, LockToken: "stale"}) // must not settle

	// UPDATE ... RETURNING over a row-value set, remotely.
	res, err := e.RenewBatch(ctx, q, items)
	if err != nil {
		t.Fatalf("RenewBatch on remote: %v", err)
	}
	for i, r := range res {
		if want := i < n; r.Ok != want {
			t.Errorf("RenewBatch item %d (seq %d): Ok=%v, want %v", i, r.SeqNumber, r.Ok, want)
		}
	}

	res, err = e.CompleteBatch(ctx, q, items)
	if err != nil {
		t.Fatalf("CompleteBatch on remote: %v", err)
	}
	for i, r := range res {
		if want := i < n; r.Ok != want {
			t.Errorf("CompleteBatch item %d (seq %d): Ok=%v, want %v", i, r.SeqNumber, r.Ok, want)
		}
	}
	if m, err := e.Stats(ctx, q); err != nil || m.Total != 0 {
		t.Fatalf("total=%d after settling the batch remotely, want 0 (err %v)", m.Total, err)
	}
}
