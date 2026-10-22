package engine

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
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

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
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
	deleteQ := q + "_delete"
	raceQ := q + "_race"
	emptySub := q + "_sub_empty"
	forcedSub := q + "_sub_force"
	cleanupTursoQueues(t, e, q, deleteQ, raceQ, emptySub, forcedSub)
	if err := e.CreateQueue(ctx, q, QueueConfig{LockDurationMs: 30000, MaxDeliveryCount: 5}); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if err := e.CreateQueue(ctx, deleteQ, QueueConfig{}); err != nil {
		t.Fatalf("create lifecycle queue: %v", err)
	}
	if _, err := e.SendOne(ctx, deleteQ, OutMessage{Body: []byte("discard")}); err != nil {
		t.Fatalf("seed lifecycle queue: %v", err)
	}
	if _, err := e.db.exec(ctx, `DELETE FROM queues WHERE name=?`, deleteQ); err == nil {
		t.Fatal("remote message-parent trigger allowed a non-empty queue delete")
	} else {
		requireMessageParentFailure(t, err)
	}
	deleted, err := e.DeleteQueueWithOptions(ctx, deleteQ, DeleteOptions{Force: true})
	if err != nil || !deleted.Deleted || deleted.State != "deleted" {
		t.Fatalf("force delete lifecycle queue: %+v %v", deleted, err)
	}
	t.Log("Turso lifecycle triggers OK: non-empty parent rejected and bounded force delete converged")

	// Subscription retirement must remove both physical metadata rows explicitly.
	// Turso/libSQL deployments do not promise that PRAGMA foreign_keys is enabled.
	if err := e.Subscribe(ctx, q+"_topic_empty", emptySub, nil); err != nil {
		t.Fatalf("create empty remote subscription: %v", err)
	}
	if result, err := e.DeleteSubscriptionWithOptions(ctx, q+"_topic_empty", emptySub, DeleteOptions{}); err != nil || !result.Deleted {
		t.Fatalf("delete empty remote subscription: %+v %v", result, err)
	}
	if mappings, queues := subscriptionMetadataRows(t, e, emptySub); mappings != 0 || queues != 0 {
		t.Fatalf("empty remote subscription retained metadata: mappings=%d queues=%d", mappings, queues)
	}
	if err := e.Subscribe(ctx, q+"_topic_force", forcedSub, nil); err != nil {
		t.Fatalf("create forced remote subscription: %v", err)
	}
	if _, err := e.SendOne(ctx, q+"_topic_force", OutMessage{Body: []byte("discard")}); err != nil {
		t.Fatalf("seed forced remote subscription: %v", err)
	}
	if err := e.DeleteSubscription(ctx, q+"_topic_force", forcedSub); !errors.Is(err, ErrSubscriptionNotEmpty) {
		t.Fatalf("non-empty remote subscription delete=%v, want ErrSubscriptionNotEmpty", err)
	}
	if result, err := e.DeleteSubscriptionWithOptions(ctx, q+"_topic_force", forcedSub, DeleteOptions{Force: true}); err != nil || !result.Deleted {
		t.Fatalf("force delete remote subscription: %+v %v", result, err)
	}
	if mappings, queues := subscriptionMetadataRows(t, e, forcedSub); mappings != 0 || queues != 0 {
		t.Fatalf("forced remote subscription retained metadata: mappings=%d queues=%d", mappings, queues)
	}
	t.Log("Turso subscription lifecycle OK: mapping and backing queue retired explicitly")

	if err := e.CreateQueue(ctx, raceQ, QueueConfig{}); err != nil {
		t.Fatalf("create lifecycle race queue: %v", err)
	}
	start := make(chan struct{})
	sent := make(chan error, 1)
	retired := make(chan struct {
		result DeleteResult
		err    error
	}, 1)
	go func() {
		<-start
		_, err := e.SendOne(ctx, raceQ, OutMessage{Body: []byte("race")})
		sent <- err
	}()
	go func() {
		<-start
		result, err := e.DeleteQueueWithOptions(ctx, raceQ, DeleteOptions{Force: true})
		retired <- struct {
			result DeleteResult
			err    error
		}{result: result, err: err}
	}()
	close(start)
	if err := <-sent; err != nil && !errors.Is(err, ErrQueueNotFound) {
		t.Fatalf("concurrent remote send: %v", err)
	}
	retirement := <-retired
	if retirement.err != nil || !retirement.result.Deleted || retirement.result.State != "deleted" {
		t.Fatalf("concurrent remote force delete: %+v %v", retirement.result, retirement.err)
	}
	var orphans int
	if err := e.db.queryRowScan(ctx, []any{&orphans}, `SELECT COUNT(*) FROM messages m
		LEFT JOIN queues q ON q.name=m.queue WHERE m.queue=? AND q.name IS NULL`, raceQ); err != nil || orphans != 0 {
		t.Fatalf("concurrent remote lifecycle left %d orphan messages: %v", orphans, err)
	}
	t.Log("Turso lifecycle race smoke passed: concurrent send and force deletion left no orphan")

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
	snapshot := e.Observability(ctx)
	if snapshot.Collection.State != "available" || snapshot.Backend != "remote" || !snapshot.Runtime.ReadAvailable || snapshot.Runtime.DBSizeAvailable || snapshot.Storage.Pool.MaxOpen != 4 {
		t.Fatalf("remote observation unavailable: collection=%+v runtime=%+v", snapshot.Collection, snapshot.Runtime)
	}
	var observedQueue bool
	for _, row := range snapshot.Queues {
		if row.Queue == q {
			observedQueue = true
			if row.Metrics != mt {
				t.Fatalf("remote native stats and observation differ: %+v %+v", row.Metrics, mt)
			}
		}
	}
	if !observedQueue {
		t.Fatal("remote observation omitted owned queue")
	}
	own := map[string]uint64{}
	for _, event := range snapshot.Messages {
		if event.Queue == q {
			own[event.Event] = event.Count
		}
	}
	if own["enqueued"] != 2 || own["delivered"] != 3 || own["redelivered"] != 1 || own["completed"] != 2 || own["abandoned"] != 1 {
		t.Fatalf("remote committed effects differ: %+v", own)
	}
	t.Log("Turso canonical observation OK: batched gauges, committed events, runtime probe and pool verified")
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
					cerrs <- fmt.Errorf("receive: %w", err)
					return
				}
				if len(ms) == 0 {
					return // drained
				}
				mu.Lock()
				claimed[ms[0].SeqNumber]++
				mu.Unlock()
				if err := e.Complete(ctx, q, ms[0].SeqNumber, ms[0].LockToken); err != nil {
					cerrs <- fmt.Errorf("complete: %w", err)
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

// The performance soak is a deliberately opt-in, closed-loop test of the real
// verification database. It lives beside the other live Turso tests so the
// credentials gate and destructive TestMain reset remain visible in one place.
const (
	tursoSoakDurationEnv    = "MQLITE_TURSO_SOAK_DURATION"
	tursoSoakWindowEnv      = "MQLITE_TURSO_SOAK_WINDOW"
	tursoSoakCallTimeoutEnv = "MQLITE_TURSO_SOAK_CALL_TIMEOUT"
	tursoSoakOutputEnv      = "MQLITE_TURSO_SOAK_OUTPUT"
	tursoSoakConfirmEnv     = "MQLITE_TURSO_SOAK_CONFIRM_TEST_DB"
	tursoSoakBodyBytes      = 256
	tursoSoakMaxInFlight    = 1
	tursoSoakLaneStagger    = 250 * time.Millisecond
)

type tursoSoakConfig struct {
	Duration    time.Duration
	Window      time.Duration
	CallTimeout time.Duration
	OutputDir   string
}

func readTursoSoakConfig() (tursoSoakConfig, bool, error) {
	rawDuration := strings.TrimSpace(os.Getenv(tursoSoakDurationEnv))
	if rawDuration == "" {
		return tursoSoakConfig{}, false, nil
	}
	parse := func(name, raw string, fallback time.Duration) (time.Duration, error) {
		if strings.TrimSpace(raw) == "" {
			return fallback, nil
		}
		d, err := time.ParseDuration(strings.TrimSpace(raw))
		if err != nil || d <= 0 {
			return 0, fmt.Errorf("%s must be a positive Go duration", name)
		}
		return d, nil
	}
	duration, err := parse(tursoSoakDurationEnv, rawDuration, 0)
	if err != nil {
		return tursoSoakConfig{}, true, err
	}
	window, err := parse(tursoSoakWindowEnv, os.Getenv(tursoSoakWindowEnv), time.Minute)
	if err != nil {
		return tursoSoakConfig{}, true, err
	}
	callTimeout, err := parse(tursoSoakCallTimeoutEnv, os.Getenv(tursoSoakCallTimeoutEnv), 30*time.Second)
	if err != nil {
		return tursoSoakConfig{}, true, err
	}
	if window < 100*time.Millisecond {
		return tursoSoakConfig{}, true, fmt.Errorf("%s must be at least 100ms", tursoSoakWindowEnv)
	}
	out := strings.TrimSpace(os.Getenv(tursoSoakOutputEnv))
	if out == "" {
		out = "turso-soak-results"
	}
	out, err = filepath.Abs(out)
	if err != nil {
		return tursoSoakConfig{}, true, err
	}
	return tursoSoakConfig{Duration: duration, Window: window, CallTimeout: callTimeout, OutputDir: out}, true, nil
}

type tursoSoakLaneSpec struct {
	Name     string
	Queue    string
	Ordering OrderingMode
	GroupID  string
}

var tursoSoakLanes = []tursoSoakLaneSpec{
	{Name: "standard_a", Queue: "mqlite_turso_soak_standard_a", Ordering: OrderStandard},
	{Name: "standard_b", Queue: "mqlite_turso_soak_standard_b", Ordering: OrderStandard},
	{Name: "group_fifo", Queue: "mqlite_turso_soak_group_fifo", Ordering: OrderGroupFIFO, GroupID: "soak-group"},
	{Name: "strict_fifo", Queue: "mqlite_turso_soak_strict_fifo", Ordering: OrderStrictFIFO},
}

func tursoSoakBody(lane string, cycle uint64) []byte {
	body := make([]byte, tursoSoakBodyBytes)
	prefix := fmt.Sprintf("mqlite-turso-soak/%s/%020d/", lane, cycle)
	copy(body, prefix)
	for i := len(prefix); i < len(body); i++ {
		body[i] = byte('a' + (int(cycle)+i)%26)
	}
	return body
}

type tursoSoakHistogram struct {
	DurationHistogram
	Max time.Duration
}

func newTursoSoakHistogram() tursoSoakHistogram {
	return tursoSoakHistogram{DurationHistogram: newDurationHistogram()}
}

func (h *tursoSoakHistogram) observe(d time.Duration) {
	h.DurationHistogram.observe(d)
	if d > h.Max {
		h.Max = d
	}
}

func (h *tursoSoakHistogram) merge(other tursoSoakHistogram) {
	if h.BucketCounts == nil {
		*h = newTursoSoakHistogram()
	}
	for i, n := range other.BucketCounts {
		h.BucketCounts[i] += n
	}
	h.Count += other.Count
	h.SumSeconds += other.SumSeconds
	if other.Max > h.Max {
		h.Max = other.Max
	}
}

func (h tursoSoakHistogram) quantile(q float64) time.Duration {
	if h.Count == 0 {
		return 0
	}
	want := uint64(math.Ceil(q * float64(h.Count)))
	if want == 0 {
		want = 1
	}
	for i, seen := range h.BucketCounts {
		if seen >= want {
			bound := time.Duration(h.BoundsSeconds[i] * float64(time.Second))
			if bound > h.Max {
				return h.Max
			}
			return bound
		}
	}
	return h.Max
}

type tursoSoakLatency struct {
	Count uint64  `json:"count"`
	P50Ms float64 `json:"p50_ms"`
	P95Ms float64 `json:"p95_ms"`
	P99Ms float64 `json:"p99_ms"`
	MaxMs float64 `json:"max_ms"`
}

func (h tursoSoakHistogram) summary() tursoSoakLatency {
	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	return tursoSoakLatency{Count: h.Count, P50Ms: ms(h.quantile(.50)), P95Ms: ms(h.quantile(.95)), P99Ms: ms(h.quantile(.99)), MaxMs: ms(h.Max)}
}

type tursoSoakCounters struct {
	Cycles    uint64
	Sends     uint64
	Receives  uint64
	Completes uint64
	Empty     uint64
	Errors    uint64
	Send      tursoSoakHistogram
	Receive   tursoSoakHistogram
	Complete  tursoSoakHistogram
	EndToEnd  tursoSoakHistogram
}

func newTursoSoakCounters() tursoSoakCounters {
	return tursoSoakCounters{
		Send: newTursoSoakHistogram(), Receive: newTursoSoakHistogram(),
		Complete: newTursoSoakHistogram(), EndToEnd: newTursoSoakHistogram(),
	}
}

func (c *tursoSoakCounters) merge(other tursoSoakCounters) {
	c.Cycles += other.Cycles
	c.Sends += other.Sends
	c.Receives += other.Receives
	c.Completes += other.Completes
	c.Empty += other.Empty
	c.Errors += other.Errors
	c.Send.merge(other.Send)
	c.Receive.merge(other.Receive)
	c.Complete.merge(other.Complete)
	c.EndToEnd.merge(other.EndToEnd)
}

func (c tursoSoakCounters) clone() tursoSoakCounters {
	for _, histogram := range []*tursoSoakHistogram{&c.Send, &c.Receive, &c.Complete, &c.EndToEnd} {
		histogram.DurationHistogram = cloneHistogram(histogram.DurationHistogram)
	}
	return c
}

type tursoSoakLaneState struct {
	spec   tursoSoakLaneSpec
	mu     sync.Mutex
	window tursoSoakCounters
	total  tursoSoakCounters
}

func newTursoSoakLaneState(spec tursoSoakLaneSpec) *tursoSoakLaneState {
	return &tursoSoakLaneState{spec: spec, window: newTursoSoakCounters(), total: newTursoSoakCounters()}
}

func (s *tursoSoakLaneState) record(operation string, elapsed time.Duration, success, empty bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range []*tursoSoakCounters{&s.window, &s.total} {
		switch operation {
		case "send":
			c.Send.observe(elapsed)
			if success {
				c.Sends++
			}
		case "receive":
			c.Receive.observe(elapsed)
			if success && !empty {
				c.Receives++
			}
			if empty {
				c.Empty++
			}
		case "complete":
			c.Complete.observe(elapsed)
			if success {
				c.Completes++
			}
		}
		if !success {
			c.Errors++
		}
	}
}

func (s *tursoSoakLaneState) recordCycle(elapsed time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range []*tursoSoakCounters{&s.window, &s.total} {
		c.Cycles++
		c.EndToEnd.observe(elapsed)
	}
}

func (s *tursoSoakLaneState) takeWindow() tursoSoakCounters {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.window
	s.window = newTursoSoakCounters()
	return out
}

func (s *tursoSoakLaneState) totalSnapshot() tursoSoakCounters {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.total.clone()
}

type tursoSoakBacklog struct {
	Active       int64 `json:"active"`
	Locked       int64 `json:"locked"`
	Deferred     int64 `json:"deferred"`
	Scheduled    int64 `json:"scheduled"`
	DeadLettered int64 `json:"dead_lettered"`
	Total        int64 `json:"total"`
}

func tursoSoakBacklogFromMetrics(m Metrics) tursoSoakBacklog {
	return tursoSoakBacklog{Active: m.Active, Locked: m.Locked, Deferred: m.Deferred, Scheduled: m.Scheduled, DeadLettered: m.DeadLettered, Total: m.Total}
}

func (b *tursoSoakBacklog) add(other tursoSoakBacklog) {
	b.Active += other.Active
	b.Locked += other.Locked
	b.Deferred += other.Deferred
	b.Scheduled += other.Scheduled
	b.DeadLettered += other.DeadLettered
	b.Total += other.Total
}

type tursoSoakCounterReport struct {
	Cycles          uint64           `json:"cycles"`
	CyclesPerSecond float64          `json:"cycles_per_second"`
	Sends           uint64           `json:"sends"`
	Receives        uint64           `json:"receives"`
	Completes       uint64           `json:"completes"`
	EmptyReceives   uint64           `json:"empty_receives"`
	Errors          uint64           `json:"errors"`
	Send            tursoSoakLatency `json:"send"`
	Receive         tursoSoakLatency `json:"receive"`
	Complete        tursoSoakLatency `json:"complete"`
	EndToEnd        tursoSoakLatency `json:"end_to_end"`
}

func tursoSoakCounterReportFor(c tursoSoakCounters, elapsed time.Duration) tursoSoakCounterReport {
	rate := 0.0
	if elapsed > 0 {
		rate = float64(c.Cycles) / elapsed.Seconds()
	}
	return tursoSoakCounterReport{
		Cycles: c.Cycles, CyclesPerSecond: rate, Sends: c.Sends, Receives: c.Receives,
		Completes: c.Completes, EmptyReceives: c.Empty, Errors: c.Errors,
		Send: c.Send.summary(), Receive: c.Receive.summary(), Complete: c.Complete.summary(), EndToEnd: c.EndToEnd.summary(),
	}
}

type tursoSoakLaneWindow struct {
	Name               string                 `json:"name"`
	Ordering           OrderingMode           `json:"ordering"`
	Work               tursoSoakCounterReport `json:"work"`
	Backlog            *tursoSoakBacklog      `json:"stats_backlog"`
	SettlementReceipts *int64                 `json:"settlement_receipts"`
}

type tursoSoakStorageOperation struct {
	Operation string `json:"operation"`
	Outcome   string `json:"outcome"`
	ErrorCode string `json:"error_code"`
	Count     uint64 `json:"count"`
	Retries   uint64 `json:"retries"`
}

type tursoSoakStorageWindow struct {
	Operations      []tursoSoakStorageOperation `json:"operations"`
	PoolMaxOpen     int                         `json:"pool_max_open"`
	PoolOpen        int                         `json:"pool_open"`
	PoolInUse       int                         `json:"pool_in_use"`
	PoolIdle        int                         `json:"pool_idle"`
	PoolWaitCount   int64                       `json:"pool_wait_count"`
	PoolWaitSeconds float64                     `json:"pool_wait_seconds"`
}

func tursoSoakStorageDelta(before, after StorageObservation) tursoSoakStorageWindow {
	type key struct{ operation, outcome, code string }
	prior := make(map[key]StorageOperation, len(before.Operations))
	for _, op := range before.Operations {
		prior[key{op.Operation, op.Outcome, op.ErrorCode}] = op
	}
	out := tursoSoakStorageWindow{
		Operations:      []tursoSoakStorageOperation{},
		PoolMaxOpen:     after.Pool.MaxOpen,
		PoolOpen:        after.Pool.Open,
		PoolInUse:       after.Pool.InUse,
		PoolIdle:        after.Pool.Idle,
		PoolWaitCount:   after.Pool.WaitCount - before.Pool.WaitCount,
		PoolWaitSeconds: after.Pool.WaitDurationSeconds - before.Pool.WaitDurationSeconds,
	}
	for _, op := range after.Operations {
		p := prior[key{op.Operation, op.Outcome, op.ErrorCode}]
		count, retries := op.Duration.Count-p.Duration.Count, op.Retries-p.Retries
		if count == 0 && retries == 0 {
			continue
		}
		out.Operations = append(out.Operations, tursoSoakStorageOperation{
			Operation: op.Operation, Outcome: op.Outcome, ErrorCode: op.ErrorCode, Count: count, Retries: retries,
		})
	}
	return out
}

type tursoSoakResources struct {
	RSSBytes               *uint64  `json:"rss_bytes"`
	HeapAllocBytes         uint64   `json:"heap_alloc_bytes"`
	HeapInuseBytes         uint64   `json:"heap_inuse_bytes"`
	Goroutines             int      `json:"goroutines"`
	GCCount                uint32   `json:"gc_count"`
	GCPauseTotalSeconds    float64  `json:"gc_pause_total_seconds"`
	ProcessCPUSecondsTotal *float64 `json:"process_cpu_seconds_total"`
}

func tursoSoakResourceSnapshot() tursoSoakResources {
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	return tursoSoakResources{
		RSSBytes: readTursoSoakRSS(), HeapAllocBytes: memory.HeapAlloc, HeapInuseBytes: memory.HeapInuse,
		Goroutines: runtime.NumGoroutine(), GCCount: memory.NumGC,
		GCPauseTotalSeconds:    float64(memory.PauseTotalNs) / float64(time.Second),
		ProcessCPUSecondsTotal: readTursoSoakCPU(),
	}
}

func readTursoSoakRSS() *uint64 {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return nil
	}
	for _, line := range strings.Split(string(b), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == "VmRSS:" {
			kb, err := strconv.ParseUint(fields[1], 10, 64)
			if err == nil {
				value := kb * 1024
				return &value
			}
		}
	}
	return nil
}

func readTursoSoakCPU() *float64 {
	// Linux exposes process user/system time as clock ticks in /proc. GitHub's
	// ubuntu-24.04 runners use 100 Hz; on other hosts report unavailable instead
	// of inventing a zero or guessing another platform's representation.
	if runtime.GOOS != "linux" {
		return nil
	}
	b, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return nil
	}
	end := strings.LastIndexByte(string(b), ')')
	if end < 0 {
		return nil
	}
	fields := strings.Fields(string(b[end+1:]))
	if len(fields) <= 12 {
		return nil
	}
	user, errUser := strconv.ParseUint(fields[11], 10, 64)
	system, errSystem := strconv.ParseUint(fields[12], 10, 64)
	if errUser != nil || errSystem != nil {
		return nil
	}
	seconds := float64(user+system) / 100
	return &seconds
}

type tursoSoakWindow struct {
	Sequence           int                      `json:"sequence"`
	StartedAt          time.Time                `json:"started_at"`
	EndedAt            time.Time                `json:"ended_at"`
	DurationSeconds    float64                  `json:"duration_seconds"`
	Partial            bool                     `json:"partial"`
	Lanes              []tursoSoakLaneWindow    `json:"lanes"`
	Overall            tursoSoakCounterReport   `json:"overall"`
	StatsBacklog       *tursoSoakBacklog        `json:"stats_backlog"`
	SettlementReceipts *int64                   `json:"settlement_receipts"`
	Storage            tursoSoakStorageWindow   `json:"storage"`
	Maintenance        []MaintenanceObservation `json:"maintenance"`
	Resources          tursoSoakResources       `json:"resources"`
	MonitorError       string                   `json:"monitor_error,omitempty"`
}

func tursoSoakWindowPartial(_ time.Duration, final bool) bool {
	// A final sample includes stop signalling and in-flight cycle drain. Even if
	// that takes a full interval, it is not comparable to a periodic load window.
	return final
}

type tursoSoakFailure struct {
	Lane      string `json:"lane,omitempty"`
	Operation string `json:"operation"`
	Code      string `json:"code"`
}

func (f tursoSoakFailure) Error() string {
	if f.Lane == "" {
		return fmt.Sprintf("%s failed (%s)", f.Operation, f.Code)
	}
	return fmt.Sprintf("lane %s %s failed (%s)", f.Lane, f.Operation, f.Code)
}

func safeTursoSoakFailure(lane, operation string, err error) error {
	return tursoSoakFailure{Lane: lane, Operation: operation, Code: storageErrorCode(err)}
}

type tursoSoakLogHandler struct {
	report func(error)
}

func (h tursoSoakLogHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= slog.LevelInfo
}
func (h tursoSoakLogHandler) Handle(_ context.Context, r slog.Record) error {
	// storage.failure is emitted synchronously before the foreground API returns.
	// The lane records that returned error with its queue and operation, so reporting
	// the log here would win the first-failure race and hide the useful attribution.
	if r.Message == "storage.failure" {
		return nil
	}
	// A tracked maintenance failure is emitted before runMaintenance records its
	// complete pass. Per-window snapshots below use that authoritative state so
	// one transient failure gets a chance to recover. Startup/manual failures do
	// not enter that state and therefore remain immediately fatal.
	if r.Message == "maintenance.failure" {
		code, task, stage := "unknown", "", ""
		r.Attrs(func(attr slog.Attr) bool {
			switch attr.Key {
			case "task":
				task = attr.Value.String()
			case "stage":
				stage = attr.Value.String()
			case "category":
				if category := attr.Value.String(); category != "" {
					code = category
				}
			}
			return true
		})
		if !tursoSoakTrackedMaintenanceTask(task) {
			operation := "maintenance"
			if task != "" {
				operation += "/" + task
			}
			if stage != "" {
				operation += "/" + stage
			}
			h.report(tursoSoakFailure{Operation: operation, Code: code})
		}
		return nil
	}
	// A fast maintenance task can fail twice and recover between minute samples.
	// The recovery event carries the completed pass streak, so retain the same
	// two-pass policy without polling the database or expanding production state.
	if r.Message == "maintenance.recovered" {
		task := "unknown"
		var failures uint64
		r.Attrs(func(attr slog.Attr) bool {
			switch attr.Key {
			case "task":
				if value := attr.Value.String(); value != "" {
					task = value
				}
			case "consecutive_failures":
				if attr.Value.Kind() == slog.KindUint64 {
					failures = attr.Value.Uint64()
				}
			}
			return true
		})
		if failures >= 2 {
			h.report(tursoSoakFailure{Operation: "maintenance/" + task, Code: "repeated_failure"})
		}
		return nil
	}
	if r.Message == "maintenance.interrupted" {
		return nil
	}
	// Preserve fail-closed behavior for a future warning/error event that the soak
	// has not classified yet, without copying its potentially sensitive message.
	if r.Level >= slog.LevelWarn {
		code := "warning"
		if r.Level >= slog.LevelError {
			code = "error"
		}
		h.report(tursoSoakFailure{Operation: "engine_log", Code: "unclassified_" + code})
	}
	return nil
}
func (h tursoSoakLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h tursoSoakLogHandler) WithGroup(string) slog.Handler      { return h }

func tursoSoakTrackedMaintenanceTask(task string) bool {
	for _, tracked := range MaintenanceTaskNames() {
		if task == tracked {
			return true
		}
	}
	return false
}

func tursoSoakMaintenanceSnapshot(e *Engine) []MaintenanceObservation {
	e.observation.mu.Lock()
	defer e.observation.mu.Unlock()

	reclaimEnabled := false
	if e.db != nil && !e.db.remote {
		_, reclaimEnabled = localFilePath(e.db.dsn)
	}
	out := make([]MaintenanceObservation, 0, len(MaintenanceTaskNames()))
	for _, task := range MaintenanceTaskNames() {
		m := e.observation.maintenance[task]
		m.Task = task
		m.Enabled = e.observation.background && (task != "reclaim" || reclaimEnabled)
		m.Duration = cloneHistogram(m.Duration)
		if m.Diagnostics == nil {
			m.Diagnostics = &MaintenanceDiagnostics{LastOutcome: "never", RecentFailures: []MaintenanceFailure{}}
		} else {
			diagnostics := *m.Diagnostics
			diagnostics.RecentFailures = append([]MaintenanceFailure{}, diagnostics.RecentFailures...)
			m.Diagnostics = &diagnostics
		}
		out = append(out, m)
	}
	return out
}

func tursoSoakMaintenanceFailure(m MaintenanceObservation) tursoSoakFailure {
	failure := tursoSoakFailure{Operation: "maintenance/" + m.Task, Code: "unknown"}
	if m.Diagnostics == nil || len(m.Diagnostics.RecentFailures) == 0 {
		return failure
	}
	recent := m.Diagnostics.RecentFailures[len(m.Diagnostics.RecentFailures)-1]
	if recent.Stage != "" {
		failure.Operation += "/" + recent.Stage
	}
	if recent.Error.Category != "" {
		failure.Code = recent.Error.Category
	}
	return failure
}

func tursoSoakMaintenanceHealth(maintenance []MaintenanceObservation, final bool) error {
	for _, m := range maintenance {
		if !m.Enabled || m.Diagnostics == nil {
			continue
		}
		failures := m.Diagnostics.ConsecutiveFailures
		if failures >= 2 || (final && failures > 0) {
			return tursoSoakMaintenanceFailure(m)
		}
	}
	return nil
}

func tursoSoakWindowMaintenanceHealth(maintenance []MaintenanceObservation) error {
	return tursoSoakMaintenanceHealth(maintenance, false)
}

func tursoSoakTerminalMaintenanceHealth(maintenance []MaintenanceObservation) error {
	return tursoSoakMaintenanceHealth(maintenance, true)
}

type tursoSoakEvidence struct {
	dir     string
	windows *os.File
}

func newTursoSoakEvidence(dir string) (*tursoSoakEvidence, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	for _, name := range []string{"metadata.json", "windows.jsonl", "summary.json", "summary.md"} {
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	f, err := os.OpenFile(filepath.Join(dir, "windows.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}
	return &tursoSoakEvidence{dir: dir, windows: f}, nil
}

func (e *tursoSoakEvidence) close() error {
	if e == nil || e.windows == nil {
		return nil
	}
	windows := e.windows
	e.windows = nil
	return windows.Close()
}

func (e *tursoSoakEvidence) writeJSON(name string, value any) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return tursoSoakAtomicWrite(filepath.Join(e.dir, name), b)
}

func tursoSoakAtomicWrite(path string, body []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	ok := false
	defer func() {
		_ = tmp.Close()
		if !ok {
			_ = os.Remove(tmpName)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return err
	}
	if _, err := tmp.Write(body); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		// Windows does not replace an existing destination with Rename. The soak
		// workflow runs on Linux (atomic replacement); this fallback keeps the
		// hermetic harness smoke portable across the normal CI matrix.
		if runtime.GOOS != "windows" {
			return err
		}
		if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return removeErr
		}
		if err := os.Rename(tmpName, path); err != nil {
			return err
		}
	}
	ok = true
	return nil
}

func (e *tursoSoakEvidence) appendWindow(window tursoSoakWindow) error {
	b, err := json.Marshal(window)
	if err != nil {
		return err
	}
	if _, err := e.windows.Write(append(b, '\n')); err != nil {
		return err
	}
	return e.windows.Sync()
}

type tursoSoakTrend struct {
	FirstCyclesPerSecond float64  `json:"first_cycles_per_second"`
	LastCyclesPerSecond  float64  `json:"last_cycles_per_second"`
	ChangePercent        *float64 `json:"change_percent"`
	WindowCount          int      `json:"window_count"`
}

func tursoSoakTrendFor(first, last *tursoSoakWindow, count int) tursoSoakTrend {
	trend := tursoSoakTrend{WindowCount: count}
	if first == nil || last == nil {
		return trend
	}
	trend.FirstCyclesPerSecond = first.Overall.CyclesPerSecond
	trend.LastCyclesPerSecond = last.Overall.CyclesPerSecond
	if count >= 2 && trend.FirstCyclesPerSecond > 0 {
		change := (trend.LastCyclesPerSecond/trend.FirstCyclesPerSecond - 1) * 100
		trend.ChangePercent = &change
	}
	return trend
}

type tursoSoakSummary struct {
	Status                    string                   `json:"status"`
	RequestedDurationSeconds  float64                  `json:"requested_duration_seconds"`
	ElapsedSeconds            float64                  `json:"elapsed_seconds"`
	WindowSeconds             float64                  `json:"window_seconds"`
	WindowTrend               tursoSoakTrend           `json:"window_trend"`
	Overall                   tursoSoakCounterReport   `json:"overall"`
	Lanes                     []tursoSoakLaneWindow    `json:"lanes"`
	FinalStatsBacklog         *tursoSoakBacklog        `json:"final_stats_backlog"`
	FinalSettlementReceipts   *int64                   `json:"final_settlement_receipts"`
	FinalMaintenance          []MaintenanceObservation `json:"final_maintenance"`
	AccountingValid           bool                     `json:"accounting_valid"`
	DurationCompleted         bool                     `json:"duration_completed"`
	CleanupComplete           bool                     `json:"cleanup_complete"`
	Failure                   *tursoSoakFailure        `json:"failure,omitempty"`
	RemotePhysicalFileMetrics map[string]*uint64       `json:"remote_physical_file_metrics"`
	Notes                     []string                 `json:"notes"`
}

func (s tursoSoakSummary) markdown() []byte {
	var b strings.Builder
	fmt.Fprintln(&b, "# Turso performance soak")
	fmt.Fprintf(&b, "\n- Status: **%s**\n", s.Status)
	fmt.Fprintf(&b, "- Duration: %.1fs / %.1fs requested\n", s.ElapsedSeconds, s.RequestedDurationSeconds)
	fmt.Fprintf(&b, "- Completed cycles: %d (%.2f cycles/s over elapsed time)\n", s.Overall.Cycles, s.Overall.CyclesPerSecond)
	fmt.Fprintf(&b, "- Accounting valid: %t; final backlog: ", s.AccountingValid)
	if s.FinalStatsBacklog == nil {
		fmt.Fprintln(&b, "unavailable")
	} else {
		fmt.Fprintf(&b, "%d\n", s.FinalStatsBacklog.Total)
	}
	fmt.Fprintln(&b, "\n| Lane | Cycles | cycles/s | e2e p50 ms | e2e p95 ms | e2e p99 ms | max ms | Errors |")
	fmt.Fprintln(&b, "|---|---:|---:|---:|---:|---:|---:|---:|")
	for _, lane := range s.Lanes {
		fmt.Fprintf(&b, "| %s | %d | %.2f | %.3f | %.3f | %.3f | %.3f | %d |\n",
			lane.Name, lane.Work.Cycles, lane.Work.CyclesPerSecond, lane.Work.EndToEnd.P50Ms,
			lane.Work.EndToEnd.P95Ms, lane.Work.EndToEnd.P99Ms, lane.Work.EndToEnd.MaxMs, lane.Work.Errors)
	}
	fmt.Fprintln(&b, "\n## Maintenance")
	if len(s.FinalMaintenance) == 0 {
		fmt.Fprintln(&b, "\nUnavailable; the engine did not open far enough to collect a snapshot.")
	} else {
		fmt.Fprintln(&b, "\n| Task | Runs | Failed passes | Last outcome | Consecutive failures |")
		fmt.Fprintln(&b, "|---|---:|---:|---|---:|")
		for _, maintenance := range s.FinalMaintenance {
			outcome := "unknown"
			var consecutive uint64
			if maintenance.Diagnostics != nil {
				outcome = maintenance.Diagnostics.LastOutcome
				consecutive = maintenance.Diagnostics.ConsecutiveFailures
			}
			if !maintenance.Enabled {
				outcome = "disabled"
			}
			fmt.Fprintf(&b, "| %s | %d | %d | %s | %d |\n", maintenance.Task, maintenance.Runs, maintenance.Failures, outcome, consecutive)
		}
	}
	fmt.Fprintln(&b, "\n## Window trend")
	fmt.Fprintf(&b, "\nFirst %.2f cycles/s; last %.2f cycles/s across %d window(s)",
		s.WindowTrend.FirstCyclesPerSecond, s.WindowTrend.LastCyclesPerSecond, s.WindowTrend.WindowCount)
	if s.WindowTrend.ChangePercent == nil {
		fmt.Fprintln(&b, "; change unavailable until two windows complete.")
	} else {
		fmt.Fprintf(&b, "; change %.2f%%. No performance pass/fail threshold is applied.\n", *s.WindowTrend.ChangePercent)
	}
	if s.Failure != nil {
		fmt.Fprintf(&b, "\nFailure: %s\n", s.Failure.Error())
	}
	fmt.Fprintln(&b, "\nRemote physical database and WAL sizes are unavailable; null values are intentional.")
	return []byte(b.String())
}

type tursoSoakRunner struct {
	e               *Engine
	cfg             tursoSoakConfig
	evidence        *tursoSoakEvidence
	admission       *tursoSoakAdmission
	started         time.Time
	finished        time.Time
	windowStarted   time.Time
	states          []*tursoSoakLaneState
	failureSignal   chan struct{}
	failureMu       sync.Mutex
	firstFailure    error
	stop            chan struct{}
	stopOnce        sync.Once
	storage         StorageObservation
	firstWindow     *tursoSoakWindow
	lastWindow      *tursoSoakWindow
	latestWindow    *tursoSoakWindow
	windowCount     int
	fullWindows     int
	lastBacklog     *tursoSoakBacklog
	lastReceipts    *int64
	lastMaintenance []MaintenanceObservation
	durationDone    bool
	cleanupDone     bool
}

func (r *tursoSoakRunner) report(err error) {
	if err == nil {
		return
	}
	r.failureMu.Lock()
	first := r.firstFailure == nil
	if first {
		r.firstFailure = err
	}
	r.failureMu.Unlock()
	if !first {
		return
	}
	select {
	case r.failureSignal <- struct{}{}:
	default:
	}
}

func (r *tursoSoakRunner) failure() error {
	r.failureMu.Lock()
	defer r.failureMu.Unlock()
	return r.firstFailure
}

func (r *tursoSoakRunner) stopLanes() { r.stopOnce.Do(func() { close(r.stop) }) }

func (r *tursoSoakRunner) call(fn func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.CallTimeout)
	defer cancel()
	if err := r.admission.acquire(ctx); err != nil {
		return err
	}
	defer r.admission.release()
	return fn(ctx)
}

// tursoSoakAdmission bounds every harness-originated database call. Limiting the
// harness to one call keeps foreground demand below the remote pool's
// four-connection limit while all four logical lanes remain active, providing
// headroom for background maintenance. Background workers do not use this gate,
// and it does not reserve the Turso primary's serialized write lock.
type tursoSoakAdmission struct {
	slots chan struct{}
}

func newTursoSoakAdmission(limit int) *tursoSoakAdmission {
	return &tursoSoakAdmission{slots: make(chan struct{}, limit)}
}

func (a *tursoSoakAdmission) acquire(ctx context.Context) error {
	select {
	case a.slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *tursoSoakAdmission) release() { <-a.slots }

func tursoSoakWaitForLaneStart(stop <-chan struct{}, delay time.Duration) bool {
	if delay <= 0 {
		select {
		case <-stop:
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-stop:
		return false
	}
}

func tursoSoakBoundedCall(parent context.Context, timeout time.Duration, fn func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	return fn(ctx)
}

func (r *tursoSoakRunner) runLane(state *tursoSoakLaneState, startDelay time.Duration) {
	if !tursoSoakWaitForLaneStart(r.stop, startDelay) {
		return
	}
	for cycle := uint64(1); ; cycle++ {
		select {
		case <-r.stop:
			return
		default:
		}
		body := tursoSoakBody(state.spec.Name, cycle)
		message := OutMessage{Body: body, MessageID: fmt.Sprintf("%s-%d", state.spec.Name, cycle), GroupID: state.spec.GroupID}
		cycleStart := time.Now()
		started := time.Now()
		err := r.call(func(ctx context.Context) error {
			_, err := r.e.SendOne(ctx, state.spec.Queue, message)
			return err
		})
		state.record("send", time.Since(started), err == nil, false)
		if err != nil {
			r.report(safeTursoSoakFailure(state.spec.Name, "send", err))
			return
		}

		var received []*Message
		started = time.Now()
		err = r.call(func(ctx context.Context) error {
			var err error
			received, err = r.e.Receive(ctx, state.spec.Queue, ReceiveOptions{MaxMessages: 1})
			return err
		})
		empty := err == nil && len(received) == 0
		validCount := err == nil && len(received) == 1
		state.record("receive", time.Since(started), validCount, empty)
		if err != nil {
			r.report(safeTursoSoakFailure(state.spec.Name, "receive", err))
			return
		}
		if len(received) != 1 {
			r.report(tursoSoakFailure{Lane: state.spec.Name, Operation: "receive", Code: "unexpected_message_count"})
			return
		}
		if !bytes.Equal(received[0].Body, body) || received[0].MessageID != message.MessageID || received[0].GroupID != message.GroupID {
			state.mu.Lock()
			state.window.Errors++
			state.total.Errors++
			state.mu.Unlock()
			r.report(tursoSoakFailure{Lane: state.spec.Name, Operation: "verify", Code: "message_mismatch"})
			return
		}

		started = time.Now()
		err = r.call(func(ctx context.Context) error {
			return r.e.Complete(ctx, state.spec.Queue, received[0].SeqNumber, received[0].LockToken)
		})
		state.record("complete", time.Since(started), err == nil, false)
		if err != nil {
			r.report(safeTursoSoakFailure(state.spec.Name, "complete", err))
			return
		}
		state.recordCycle(time.Since(cycleStart))
	}
}

func (r *tursoSoakRunner) collectStats() ([]*tursoSoakBacklog, map[string]int64, error) {
	backlogs := make([]*tursoSoakBacklog, len(r.states))
	for i, state := range r.states {
		var metrics Metrics
		err := r.call(func(ctx context.Context) error {
			var err error
			metrics, err = r.e.Stats(ctx, state.spec.Queue)
			return err
		})
		if err != nil {
			return backlogs, nil, safeTursoSoakFailure(state.spec.Name, "stats", err)
		}
		backlog := tursoSoakBacklogFromMetrics(metrics)
		backlogs[i] = &backlog
		if backlog.Total > 1 || backlog.Deferred != 0 || backlog.Scheduled != 0 || backlog.DeadLettered != 0 {
			return backlogs, nil, tursoSoakFailure{Lane: state.spec.Name, Operation: "stats", Code: "working_set_invariant"}
		}
	}
	receipts := make(map[string]int64, len(r.states))
	err := r.call(func(ctx context.Context) error {
		return r.e.db.queryRows(ctx, `SELECT queue,COUNT(*) FROM settlement_receipts
			WHERE queue IN (?,?,?,?) GROUP BY queue`, func(rows *sql.Rows) error {
			for rows.Next() {
				var queue string
				var count int64
				if err := rows.Scan(&queue, &count); err != nil {
					return err
				}
				receipts[queue] = count
			}
			return rows.Err()
		}, r.states[0].spec.Queue, r.states[1].spec.Queue, r.states[2].spec.Queue, r.states[3].spec.Queue)
	})
	if err != nil {
		return backlogs, nil, safeTursoSoakFailure("", "receipt_count", err)
	}
	return backlogs, receipts, nil
}

func (r *tursoSoakRunner) flushWindow(final bool) error {
	ended := time.Now()
	elapsed := ended.Sub(r.windowStarted)
	window := tursoSoakWindow{
		Sequence: r.windowCount + 1, StartedAt: r.windowStarted.UTC(), EndedAt: ended.UTC(),
		DurationSeconds: elapsed.Seconds(), Partial: tursoSoakWindowPartial(elapsed, final),
		Lanes: make([]tursoSoakLaneWindow, 0, len(r.states)),
	}
	windowCounters := make([]tursoSoakCounters, len(r.states))
	for i, state := range r.states {
		windowCounters[i] = state.takeWindow()
	}
	backlogs, receipts, monitorErr := r.collectStats()
	overallCounters := newTursoSoakCounters()
	overallBacklog := tursoSoakBacklog{}
	var receiptTotal int64
	for i, state := range r.states {
		counters := windowCounters[i]
		overallCounters.merge(counters)
		lane := tursoSoakLaneWindow{
			Name: state.spec.Name, Ordering: state.spec.Ordering,
			Work: tursoSoakCounterReportFor(counters, elapsed), Backlog: backlogs[i],
		}
		if backlogs[i] != nil {
			overallBacklog.add(*backlogs[i])
		}
		if receipts != nil {
			count := receipts[state.spec.Queue]
			lane.SettlementReceipts = &count
			receiptTotal += count
		}
		window.Lanes = append(window.Lanes, lane)
	}
	window.Overall = tursoSoakCounterReportFor(overallCounters, elapsed)
	if monitorErr == nil {
		window.StatsBacklog = &overallBacklog
		window.SettlementReceipts = &receiptTotal
		r.lastBacklog = &overallBacklog
		r.lastReceipts = &receiptTotal
	} else {
		window.MonitorError = monitorErr.Error()
	}
	currentStorage := r.e.db.storage.snapshot(r.e.db.poolStats())
	window.Storage = tursoSoakStorageDelta(r.storage, currentStorage)
	r.storage = currentStorage
	window.Resources = tursoSoakResourceSnapshot()
	window.Maintenance = tursoSoakMaintenanceSnapshot(r.e)
	// A window, including the final workload sample, is not the engine terminal
	// state: an already-running maintenance pass can still finish during cleanup.
	// Close waits for those workers; its snapshot below performs the strict check.
	maintenanceErr := tursoSoakWindowMaintenanceHealth(window.Maintenance)
	if err := r.evidence.appendWindow(window); err != nil {
		return err
	}
	r.windowCount++
	r.recordWindow(window)
	r.windowStarted = ended
	if err := r.writeSummary("RUNNING", nil); err != nil {
		return err
	}
	return errors.Join(monitorErr, maintenanceErr)
}

func (r *tursoSoakRunner) recordWindow(window tursoSoakWindow) {
	latest := window
	r.latestWindow = &latest
	r.lastMaintenance = window.Maintenance
	r.recordTrendWindow(window)
}

func (r *tursoSoakRunner) recordTrendWindow(window tursoSoakWindow) {
	if window.Partial {
		return
	}
	if r.firstWindow == nil {
		copy := window
		r.firstWindow = &copy
	}
	copy := window
	r.lastWindow = &copy
	r.fullWindows++
}

func (r *tursoSoakRunner) totals(elapsed time.Duration) (tursoSoakCounterReport, []tursoSoakLaneWindow, bool) {
	overall := newTursoSoakCounters()
	lanes := make([]tursoSoakLaneWindow, 0, len(r.states))
	valid := true
	for _, state := range r.states {
		c := state.totalSnapshot()
		if c.Cycles == 0 || c.Errors != 0 || c.Empty != 0 || c.Sends != c.Cycles || c.Receives != c.Cycles || c.Completes != c.Cycles {
			valid = false
		}
		overall.merge(c)
		lane := tursoSoakLaneWindow{Name: state.spec.Name, Ordering: state.spec.Ordering, Work: tursoSoakCounterReportFor(c, elapsed)}
		if r.latestWindow != nil {
			for _, last := range r.latestWindow.Lanes {
				if last.Name == state.spec.Name {
					lane.Backlog, lane.SettlementReceipts = last.Backlog, last.SettlementReceipts
					break
				}
			}
		}
		lanes = append(lanes, lane)
	}
	return tursoSoakCounterReportFor(overall, elapsed), lanes, valid
}

func (r *tursoSoakRunner) summary(status string, failure *tursoSoakFailure) tursoSoakSummary {
	elapsed := r.elapsed()
	overall, lanes, valid := r.totals(elapsed)
	return tursoSoakSummary{
		Status: status, RequestedDurationSeconds: r.cfg.Duration.Seconds(), ElapsedSeconds: elapsed.Seconds(),
		WindowSeconds: r.cfg.Window.Seconds(), WindowTrend: tursoSoakTrendFor(r.firstWindow, r.lastWindow, r.fullWindows),
		Overall: overall, Lanes: lanes, FinalStatsBacklog: r.lastBacklog, FinalSettlementReceipts: r.lastReceipts,
		FinalMaintenance: r.lastMaintenance,
		AccountingValid:  valid, DurationCompleted: r.durationDone, CleanupComplete: r.cleanupDone, Failure: failure,
		RemotePhysicalFileMetrics: map[string]*uint64{"database_bytes": nil, "wal_bytes": nil},
		Notes: []string{
			"Trend evidence only; no performance threshold or SLA verdict is applied.",
			fmt.Sprintf("One Engine connects directly to the dedicated verification database with four closed-loop lanes, a harness database-call concurrency limit of %d, and fixed %d ms lane-start staggering.", tursoSoakMaxInFlight, tursoSoakLaneStagger.Milliseconds()),
			"The workload deliberately keeps harness foreground calls below the four-connection remote pool limit to provide headroom for background maintenance; it is not a capacity test.",
			"A single failed maintenance pass may recover on the task's next complete pass; two consecutive failures or a failure still unrecovered after background workers stop fail the run, and recovered incidents remain in the evidence.",
			"Maintenance evidence is copied from process-local observations and adds no remote database calls.",
			"Settlement receipts expire after 30 minutes, so their count is a retention snapshot rather than a lifetime completion ledger.",
			"Remote Turso does not expose physical database or WAL sizes.",
		},
	}
}

func (r *tursoSoakRunner) elapsed() time.Duration {
	if !r.finished.IsZero() {
		return r.finished.Sub(r.started)
	}
	return time.Since(r.started)
}

func (r *tursoSoakRunner) writeSummary(status string, failure *tursoSoakFailure) error {
	summary := r.summary(status, failure)
	if err := r.evidence.writeJSON("summary.json", summary); err != nil {
		return err
	}
	return tursoSoakAtomicWrite(filepath.Join(r.evidence.dir, "summary.md"), summary.markdown())
}

func (r *tursoSoakRunner) run() error {
	var workers sync.WaitGroup
	for i, state := range r.states {
		workers.Add(1)
		go func(state *tursoSoakLaneState, startDelay time.Duration) {
			defer workers.Done()
			r.runLane(state, startDelay)
		}(state, time.Duration(i)*tursoSoakLaneStagger)
	}
	done := make(chan struct{})
	go func() {
		workers.Wait()
		close(done)
	}()
	duration := time.NewTimer(r.cfg.Duration)
	defer duration.Stop()
	windows := time.NewTicker(r.cfg.Window)
	defer windows.Stop()
	var runErr error
	for {
		select {
		case <-duration.C:
			r.durationDone = true
			r.stopLanes()
		case <-r.failureSignal:
			if runErr == nil {
				runErr = r.failure()
			}
			r.stopLanes()
		case <-windows.C:
			// A tick can race with the duration or failure signal. Once stopping
			// begins, leave all remaining work for the final partial window.
			if r.durationDone || runErr != nil {
				continue
			}
			if err := r.flushWindow(false); err != nil && runErr == nil {
				runErr = err
				r.stopLanes()
			}
		case <-done:
			if err := r.failure(); err != nil && runErr == nil {
				runErr = err
			}
			if err := r.flushWindow(true); err != nil && runErr == nil {
				runErr = err
			}
			r.finished = time.Now()
			return runErr
		}
	}
}

func tursoSoakCleanup(ctx context.Context, e *Engine, callTimeout time.Duration) error {
	queues := []any{tursoSoakLanes[0].Queue, tursoSoakLanes[1].Queue, tursoSoakLanes[2].Queue, tursoSoakLanes[3].Queue}
	subscriptions := append(append([]any{}, queues...), queues...)
	for _, statement := range []struct {
		query string
		args  []any
	}{
		{`DELETE FROM messages WHERE queue IN (?,?,?,?)`, queues},
		{`DELETE FROM dedup WHERE queue IN (?,?,?,?)`, queues},
		{`DELETE FROM settlement_receipts WHERE queue IN (?,?,?,?)`, queues},
		{`DELETE FROM receive_attempts WHERE queue IN (?,?,?,?)`, queues},
		{`DELETE FROM subscriptions WHERE subscription IN (?,?,?,?) OR topic IN (?,?,?,?)`, subscriptions},
		{`DELETE FROM queues WHERE name IN (?,?,?,?)`, queues},
	} {
		err := tursoSoakBoundedCall(ctx, callTimeout, func(callCtx context.Context) error {
			_, err := e.db.exec(callCtx, statement.query, statement.args...)
			return err
		})
		if err != nil {
			return safeTursoSoakFailure("", "cleanup", err)
		}
	}
	e.qmu.Lock()
	for _, spec := range tursoSoakLanes {
		delete(e.qcache, spec.Queue)
	}
	e.qepoch++
	e.qmu.Unlock()
	e.observation.mu.Lock()
	for _, spec := range tursoSoakLanes {
		e.observation.forgetLocked(spec.Queue)
	}
	e.observation.mu.Unlock()
	for _, check := range []struct {
		query string
		args  []any
	}{
		{`SELECT COUNT(*) FROM messages WHERE queue IN (?,?,?,?)`, queues},
		{`SELECT COUNT(*) FROM dedup WHERE queue IN (?,?,?,?)`, queues},
		{`SELECT COUNT(*) FROM settlement_receipts WHERE queue IN (?,?,?,?)`, queues},
		{`SELECT COUNT(*) FROM receive_attempts WHERE queue IN (?,?,?,?)`, queues},
		{`SELECT COUNT(*) FROM subscriptions WHERE subscription IN (?,?,?,?) OR topic IN (?,?,?,?)`, subscriptions},
		{`SELECT COUNT(*) FROM queues WHERE name IN (?,?,?,?)`, queues},
	} {
		var count int64
		err := tursoSoakBoundedCall(ctx, callTimeout, func(callCtx context.Context) error {
			return e.db.queryRowScan(callCtx, []any{&count}, check.query, check.args...)
		})
		if err != nil {
			return safeTursoSoakFailure("", "cleanup_verify", err)
		}
		if count != 0 {
			return tursoSoakFailure{Operation: "cleanup_verify", Code: "rows_remain"}
		}
	}
	return nil
}

type tursoSoakJudgement struct {
	DurationCompleted bool
	AccountingValid   bool
	FinalBacklog      *tursoSoakBacklog
	CleanupComplete   bool
	Failure           error
}

func judgeTursoSoak(j tursoSoakJudgement) error {
	if j.Failure != nil {
		return j.Failure
	}
	if !j.DurationCompleted {
		return tursoSoakFailure{Operation: "duration", Code: "ended_early"}
	}
	if !j.AccountingValid {
		return tursoSoakFailure{Operation: "accounting", Code: "invariant_failed"}
	}
	if j.FinalBacklog == nil {
		return tursoSoakFailure{Operation: "final_stats", Code: "unavailable"}
	}
	if j.FinalBacklog.Total != 0 {
		return tursoSoakFailure{Operation: "final_stats", Code: "backlog_remaining"}
	}
	if !j.CleanupComplete {
		return tursoSoakFailure{Operation: "cleanup", Code: "incomplete"}
	}
	return nil
}

func joinTursoSoakFailures(workloadErr, backgroundErr error, additional ...error) error {
	errs := []error{workloadErr}
	if backgroundErr != nil && !errors.Is(workloadErr, backgroundErr) {
		errs = append(errs, backgroundErr)
	}
	errs = append(errs, additional...)
	return errors.Join(errs...)
}

func tursoSoakControlTimeout(callTimeout time.Duration) time.Duration {
	if timeout := 4 * callTimeout; timeout > 2*time.Minute {
		return timeout
	}
	return 2 * time.Minute
}

func runTursoPerformanceSoak(cfg tursoSoakConfig) (resultErr error) {
	evidence, err := newTursoSoakEvidence(cfg.OutputDir)
	if err != nil {
		return err
	}
	defer func() {
		if err := evidence.close(); err != nil {
			resultErr = errors.Join(resultErr, err)
		}
	}()
	started := time.Now()
	metadata := map[string]any{
		"schema_version": 1, "started_at": started.UTC(), "duration_seconds": cfg.Duration.Seconds(),
		"window_seconds": cfg.Window.Seconds(), "call_timeout_seconds": cfg.CallTimeout.Seconds(),
		"backend": "remote Turso/libSQL", "engine_count": 1, "message_body_bytes": tursoSoakBodyBytes,
		"foreground_max_in_flight":        tursoSoakMaxInFlight,
		"lane_start_stagger_milliseconds": tursoSoakLaneStagger.Milliseconds(),
		"load_intent":                     "sustained stability and trend workload, not a capacity test",
		"background_maintenance":          true, "latency_method": "fixed-memory logarithmic histogram over each complete window",
		"go_version": runtime.Version(), "goos": runtime.GOOS, "goarch": runtime.GOARCH,
		"git_sha":                             strings.TrimSpace(os.Getenv("GITHUB_SHA")),
		"remote_physical_file_metrics":        map[string]*uint64{"database_bytes": nil, "wal_bytes": nil},
		"remote_physical_file_metrics_reason": "remote Turso does not expose database or WAL file sizes",
		"lanes":                               tursoSoakLanes,
	}
	if err := evidence.writeJSON("metadata.json", metadata); err != nil {
		return err
	}

	runner := &tursoSoakRunner{
		cfg: cfg, evidence: evidence, admission: newTursoSoakAdmission(tursoSoakMaxInFlight),
		failureSignal: make(chan struct{}, 1), stop: make(chan struct{}),
	}
	openCtx, cancelOpen := context.WithTimeout(context.Background(), cfg.CallTimeout)
	e, err := Open(openCtx, Options{
		DB: os.Getenv("MQLITE_TEST_DB"), AuthToken: os.Getenv("MQLITE_TEST_DB_AUTH_TOKEN"),
		Logger: slog.New(tursoSoakLogHandler{report: runner.report}),
	})
	cancelOpen()
	if err != nil {
		failure := tursoSoakFailure{Operation: "open", Code: storageErrorCode(err)}
		summary := tursoSoakSummary{
			Status: "FAIL", RequestedDurationSeconds: cfg.Duration.Seconds(), ElapsedSeconds: time.Since(started).Seconds(),
			WindowSeconds: cfg.Window.Seconds(), Failure: &failure, RemotePhysicalFileMetrics: map[string]*uint64{"database_bytes": nil, "wal_bytes": nil},
		}
		_ = evidence.writeJSON("summary.json", summary)
		_ = tursoSoakAtomicWrite(filepath.Join(evidence.dir, "summary.md"), summary.markdown())
		return failure
	}
	runner.e = e

	setupErr := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), tursoSoakControlTimeout(cfg.CallTimeout))
		defer cancel()
		if err := tursoSoakCleanup(ctx, e, cfg.CallTimeout); err != nil {
			return err
		}
		for _, lane := range tursoSoakLanes {
			err := tursoSoakBoundedCall(ctx, cfg.CallTimeout, func(callCtx context.Context) error {
				return e.CreateQueue(callCtx, lane.Queue, QueueConfig{
					LockDurationMs: 60_000, MaxDeliveryCount: 5, Ordering: lane.Ordering,
				})
			})
			if err != nil {
				return safeTursoSoakFailure(lane.Name, "create_queue", err)
			}
		}
		return nil
	}()

	workloadStarted := time.Now()
	runner.started, runner.windowStarted = workloadStarted, workloadStarted
	runner.storage = e.db.storage.snapshot(e.db.poolStats())
	for _, lane := range tursoSoakLanes {
		runner.states = append(runner.states, newTursoSoakLaneState(lane))
	}
	var workloadErr error
	if setupErr == nil {
		if backgroundErr := runner.failure(); backgroundErr != nil {
			workloadErr = backgroundErr
		} else {
			workloadErr = runner.run()
		}
	} else {
		workloadErr = setupErr
	}

	cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), tursoSoakControlTimeout(cfg.CallTimeout))
	cleanupErr := tursoSoakCleanup(cleanupCtx, e, cfg.CallTimeout)
	cancelCleanup()
	runner.cleanupDone = cleanupErr == nil
	closeErr := e.Close()
	terminalMaintenance := tursoSoakMaintenanceSnapshot(e)
	runner.lastMaintenance = terminalMaintenance
	maintenanceErr := tursoSoakTerminalMaintenanceHealth(terminalMaintenance)
	if closeErr != nil {
		closeErr = tursoSoakFailure{Operation: "close", Code: storageErrorCode(closeErr)}
	}
	evidenceCloseErr := evidence.close()
	backgroundErr := runner.failure()
	_, _, accountingValid := runner.totals(runner.elapsed())
	judgementErr := judgeTursoSoak(tursoSoakJudgement{
		DurationCompleted: runner.durationDone, AccountingValid: accountingValid,
		FinalBacklog: runner.lastBacklog, CleanupComplete: runner.cleanupDone,
		Failure: joinTursoSoakFailures(workloadErr, backgroundErr, cleanupErr, closeErr, maintenanceErr, evidenceCloseErr),
	})
	status := "PASS"
	var failure *tursoSoakFailure
	if judgementErr != nil {
		status = "FAIL"
		var typed tursoSoakFailure
		if errors.As(judgementErr, &typed) {
			failure = &typed
		} else {
			failure = &tursoSoakFailure{Operation: "harness", Code: "evidence_io"}
		}
	}
	if err := runner.writeSummary(status, failure); err != nil {
		judgementErr = errors.Join(judgementErr, err)
	}
	return judgementErr
}

// TestTursoPerformanceSoak runs only when an explicit duration and a second
// verification-database acknowledgement are present. TestMain independently
// checks the host before its destructive reset, so a misconfigured secret fails
// closed before this function is reached.
func TestTursoPerformanceSoak(t *testing.T) {
	cfg, enabled, err := readTursoSoakConfig()
	if !enabled {
		t.Skip("set MQLITE_TURSO_SOAK_DURATION to run the dedicated Turso performance soak")
	}
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv(tursoSoakConfirmEnv) != "1" {
		t.Fatalf("%s=1 is required", tursoSoakConfirmEnv)
	}
	dsn, token := os.Getenv("MQLITE_TEST_DB"), os.Getenv("MQLITE_TEST_DB_AUTH_TOKEN")
	if !approvedTursoSoakDSN(dsn) {
		t.Fatal("Turso soak refused: MQLITE_TEST_DB is not the dedicated verification database")
	}
	if token == "" {
		t.Fatal("MQLITE_TEST_DB_AUTH_TOKEN is required")
	}
	if err := runTursoPerformanceSoak(cfg); err != nil {
		t.Fatalf("Turso performance soak failed: %v", err)
	}
}

func TestTursoSoakHistogramBounded(t *testing.T) {
	h := newTursoSoakHistogram()
	for _, d := range []time.Duration{time.Millisecond, 2 * time.Millisecond, 10 * time.Millisecond, 100 * time.Millisecond} {
		h.observe(d)
	}
	s := h.summary()
	if len(h.BucketCounts) != len(durationBounds) || s.Count != 4 || s.P50Ms <= 0 || s.P50Ms > s.P95Ms || s.P95Ms > s.P99Ms || s.P99Ms > s.MaxMs || s.MaxMs != 100 {
		t.Fatalf("unexpected fixed histogram summary: %+v", s)
	}
	for i := 0; i < 10_000; i++ {
		h.observe(time.Duration(i+1) * time.Microsecond)
	}
	if len(h.BucketCounts) != len(durationBounds) {
		t.Fatal("histogram storage grew with observations")
	}
}

func TestTursoSoakConfigurationGate(t *testing.T) {
	t.Setenv(tursoSoakDurationEnv, "")
	if _, enabled, err := readTursoSoakConfig(); enabled || err != nil {
		t.Fatalf("disabled config: enabled=%v err=%v", enabled, err)
	}
	t.Setenv(tursoSoakDurationEnv, "2m")
	t.Setenv(tursoSoakWindowEnv, "1s")
	t.Setenv(tursoSoakCallTimeoutEnv, "3s")
	t.Setenv(tursoSoakOutputEnv, t.TempDir())
	cfg, enabled, err := readTursoSoakConfig()
	if err != nil || !enabled || cfg.Duration != 2*time.Minute || cfg.Window != time.Second || cfg.CallTimeout != 3*time.Second || !filepath.IsAbs(cfg.OutputDir) {
		t.Fatalf("valid config: %+v enabled=%v err=%v", cfg, enabled, err)
	}
	t.Setenv(tursoSoakWindowEnv, "1ms")
	if _, enabled, err := readTursoSoakConfig(); !enabled || err == nil {
		t.Fatalf("short window: enabled=%v err=%v", enabled, err)
	}
}

func TestTursoSoakAdmissionLimit(t *testing.T) {
	admission := newTursoSoakAdmission(tursoSoakMaxInFlight)
	for i := 0; i < tursoSoakMaxInFlight; i++ {
		if err := admission.acquire(context.Background()); err != nil {
			t.Fatalf("acquire slot %d: %v", i, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := admission.acquire(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("fourth concurrent call admitted: %v", err)
	}
	admission.release()
	if err := admission.acquire(context.Background()); err != nil {
		t.Fatalf("call not admitted after release: %v", err)
	}
	for i := 0; i < tursoSoakMaxInFlight; i++ {
		admission.release()
	}
}

func TestTursoSoakAdmissionWaitUsesCallTimeout(t *testing.T) {
	admission := newTursoSoakAdmission(1)
	if err := admission.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer admission.release()
	runner := &tursoSoakRunner{
		cfg:       tursoSoakConfig{CallTimeout: 20 * time.Millisecond},
		admission: admission,
	}
	called := false
	err := runner.call(func(context.Context) error {
		called = true
		return nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("admission wait error = %v, want context deadline exceeded", err)
	}
	if called {
		t.Fatal("database call ran without admission")
	}
}

func TestTursoSoakLaneStaggerStopsPromptly(t *testing.T) {
	stop := make(chan struct{})
	done := make(chan bool, 1)
	go func() { done <- tursoSoakWaitForLaneStart(stop, time.Hour) }()
	close(stop)
	select {
	case started := <-done:
		if started {
			t.Fatal("stopped lane was allowed to start")
		}
	case <-time.After(time.Second):
		t.Fatal("lane stagger did not respond to stop")
	}
}

func TestTursoSoakHostGuard(t *testing.T) {
	if !approvedTursoSoakDSN("libsql://" + tursoSoakVerificationHost) {
		t.Fatal("verification DSN rejected")
	}
	for _, dsn := range []string{
		"libsql://unexpected-verification-host.turso.io",
		"https://" + tursoSoakVerificationHost,
		"libsql://user@" + tursoSoakVerificationHost,
		"libsql://" + tursoSoakVerificationHost + "?token=secret",
	} {
		if approvedTursoSoakDSN(dsn) {
			t.Fatalf("unsafe DSN accepted")
		}
	}
}

func TestTursoSoakPartialWindowAndJudgement(t *testing.T) {
	if tursoSoakWindowPartial(time.Minute, false) {
		t.Fatal("periodic full window marked partial")
	}
	for _, elapsed := range []time.Duration{20 * time.Second, 80 * time.Second} {
		if !tursoSoakWindowPartial(elapsed, true) {
			t.Fatal("final drain window must remain partial regardless of elapsed length")
		}
	}
	zero := &tursoSoakBacklog{}
	if err := judgeTursoSoak(tursoSoakJudgement{DurationCompleted: true, AccountingValid: true, FinalBacklog: zero, CleanupComplete: true}); err != nil {
		t.Fatalf("valid run rejected: %v", err)
	}
	if err := judgeTursoSoak(tursoSoakJudgement{DurationCompleted: true, AccountingValid: false, FinalBacklog: zero, CleanupComplete: true}); err == nil {
		t.Fatal("broken accounting accepted")
	}
	if err := judgeTursoSoak(tursoSoakJudgement{DurationCompleted: true, AccountingValid: true, FinalBacklog: &tursoSoakBacklog{Total: 1}, CleanupComplete: true}); err == nil {
		t.Fatal("remaining backlog accepted")
	}
	runner := &tursoSoakRunner{}
	runner.recordWindow(tursoSoakWindow{Overall: tursoSoakCounterReport{CyclesPerSecond: 10}})
	runner.recordWindow(tursoSoakWindow{Partial: true, Overall: tursoSoakCounterReport{CyclesPerSecond: 0}})
	trend := tursoSoakTrendFor(runner.firstWindow, runner.lastWindow, runner.fullWindows)
	if trend.WindowCount != 1 || trend.FirstCyclesPerSecond != 10 || trend.LastCyclesPerSecond != 10 || trend.ChangePercent != nil {
		t.Fatalf("short final partial changed full-window trend: %+v", trend)
	}
	if runner.latestWindow == nil || !runner.latestWindow.Partial {
		t.Fatal("latest final-state sample did not retain the partial window")
	}
}

func TestTursoSoakFirstFailurePersists(t *testing.T) {
	runner := &tursoSoakRunner{failureSignal: make(chan struct{}, 1)}
	first := tursoSoakFailure{Operation: "send", Code: "busy"}
	runner.report(first)
	<-runner.failureSignal // model a select consuming the notification before done
	runner.report(tursoSoakFailure{Operation: "complete", Code: "connection"})
	if got := runner.failure(); !errors.Is(got, first) {
		t.Fatalf("first failure was lost: %v", got)
	}
}

func TestTursoSoakMaintenanceHealth(t *testing.T) {
	observation := func(enabled bool, consecutive uint64, outcome string) []MaintenanceObservation {
		return []MaintenanceObservation{{
			Enabled: enabled, Task: "receipts", Runs: 2, Failures: 1,
			Diagnostics: &MaintenanceDiagnostics{
				LastOutcome: outcome, ConsecutiveFailures: consecutive,
				RecentFailures: []MaintenanceFailure{{
					Stage: "prune_settlement_receipts",
					Error: ErrorDetail{Category: "connection"},
				}},
			},
		}}
	}

	pending := observation(true, 1, "failed")
	if err := tursoSoakWindowMaintenanceHealth(pending); err != nil {
		t.Fatalf("single pass was not allowed to recover: %v", err)
	}
	if err := tursoSoakTerminalMaintenanceHealth(observation(true, 0, "ok")); err != nil {
		t.Fatalf("recovered incident failed final judgement: %v", err)
	}
	want := tursoSoakFailure{Operation: "maintenance/receipts/prune_settlement_receipts", Code: "connection"}
	if got := tursoSoakWindowMaintenanceHealth(observation(true, 2, "failed")); !errors.Is(got, want) {
		t.Fatalf("repeated window failure = %v, want %v", got, want)
	}
	if got := tursoSoakTerminalMaintenanceHealth(pending); !errors.Is(got, want) {
		t.Fatalf("unrecovered terminal failure = %v, want %v", got, want)
	}
	if err := tursoSoakTerminalMaintenanceHealth(observation(false, 2, "failed")); err != nil {
		t.Fatalf("disabled maintenance task affected judgement: %v", err)
	}
}

func TestTursoSoakMaintenanceSnapshotIsIsolated(t *testing.T) {
	duration := newDurationHistogram()
	duration.observe(time.Second)
	e := &Engine{db: &db{remote: true}}
	e.observation.background = true
	e.observation.maintenance = map[string]MaintenanceObservation{
		"locks": {
			Runs: 1, Duration: duration,
			Diagnostics: &MaintenanceDiagnostics{
				LastOutcome: "failed", ConsecutiveFailures: 1,
				RecentFailures: []MaintenanceFailure{{Stage: "reclaim_locks", Error: ErrorDetail{Category: "busy"}}},
			},
		},
	}

	snapshot := tursoSoakMaintenanceSnapshot(e)
	if len(snapshot) != len(MaintenanceTaskNames()) {
		t.Fatalf("unexpected maintenance task inventory: %+v", snapshot)
	}
	byTask := make(map[string]*MaintenanceObservation, len(snapshot))
	for i := range snapshot {
		byTask[snapshot[i].Task] = &snapshot[i]
	}
	if !byTask["locks"].Enabled || byTask["reclaim"].Enabled {
		t.Fatalf("unexpected remote maintenance enablement: %+v", snapshot)
	}
	byTask["locks"].Duration.BoundsSeconds[0] = 99
	byTask["locks"].Diagnostics.RecentFailures[0].Stage = "mutated"
	original := e.observation.maintenance["locks"]
	if original.Duration.BoundsSeconds[0] == 99 || original.Diagnostics.RecentFailures[0].Stage != "reclaim_locks" {
		t.Fatal("maintenance snapshot aliases engine observation state")
	}
}

func TestTursoSoakLogHandlerAttributesFailures(t *testing.T) {
	newLogger := func() (*tursoSoakRunner, *slog.Logger) {
		runner := &tursoSoakRunner{failureSignal: make(chan struct{}, 1)}
		return runner, slog.New(tursoSoakLogHandler{report: runner.report})
	}
	waitFailure := func(t *testing.T, runner *tursoSoakRunner) error {
		t.Helper()
		select {
		case <-runner.failureSignal:
			return runner.failure()
		case <-time.After(time.Second):
			t.Fatal("engine log failure was not reported")
			return nil
		}
	}

	t.Run("foreground storage log defers to lane", func(t *testing.T) {
		runner, logger := newLogger()
		logger.Error("storage.failure", "operation", "transaction", "category", "busy")
		select {
		case <-runner.failureSignal:
			t.Fatal("foreground storage failure was misclassified as background maintenance")
		default:
		}
		want := tursoSoakFailure{Lane: "standard_b", Operation: "send", Code: "busy"}
		runner.report(want)
		if got := waitFailure(t, runner); !errors.Is(got, want) {
			t.Fatalf("foreground failure = %v, want %v", got, want)
		}
	})

	t.Run("maintenance defers to pass state", func(t *testing.T) {
		runner, logger := newLogger()
		logger.Error("maintenance.failure", "task", "ttl", "stage", "expire_to_dlq", "category", "busy")
		select {
		case <-runner.failureSignal:
			t.Fatal("maintenance log bypassed pass-level recovery policy")
		default:
		}
	})

	t.Run("untracked maintenance remains fail closed", func(t *testing.T) {
		runner, logger := newLogger()
		logger.Error("maintenance.failure", "task", "manual", "stage", "resource_cleanup", "category", "connection")
		want := tursoSoakFailure{Operation: "maintenance/manual/resource_cleanup", Code: "connection"}
		if got := waitFailure(t, runner); !errors.Is(got, want) {
			t.Fatalf("untracked maintenance failure = %v, want %v", got, want)
		}
	})

	t.Run("single recovered maintenance pass is retained as evidence", func(t *testing.T) {
		runner, logger := newLogger()
		logger.Info("maintenance.recovered", "task", "locks", "consecutive_failures", uint64(1))
		select {
		case <-runner.failureSignal:
			t.Fatal("one recovered maintenance pass failed the soak")
		default:
		}
	})

	t.Run("recovered repeated maintenance failures still fail", func(t *testing.T) {
		runner, logger := newLogger()
		logger.Info("maintenance.recovered", "task", "locks", "consecutive_failures", uint64(2))
		want := tursoSoakFailure{Operation: "maintenance/locks", Code: "repeated_failure"}
		if got := waitFailure(t, runner); !errors.Is(got, want) {
			t.Fatalf("recovered maintenance streak = %v, want %v", got, want)
		}
	})

	t.Run("unknown event fails closed", func(t *testing.T) {
		runner, logger := newLogger()
		logger.Warn("future.engine.warning", "secret", "must not be copied")
		want := tursoSoakFailure{Operation: "engine_log", Code: "unclassified_warning"}
		if got := waitFailure(t, runner); !errors.Is(got, want) {
			t.Fatalf("unclassified failure = %v, want %v", got, want)
		}
	})
}

func TestJoinTursoSoakFailuresDoesNotRepeatFirstFailure(t *testing.T) {
	first := tursoSoakFailure{Lane: "standard_b", Operation: "send", Code: "busy"}
	if got := joinTursoSoakFailures(first, first); strings.Count(got.Error(), first.Error()) != 1 {
		t.Fatalf("same first failure was repeated: %q", got)
	}
	second := tursoSoakFailure{Operation: "cleanup", Code: "connection"}
	got := joinTursoSoakFailures(first, second)
	if !errors.Is(got, first) || !errors.Is(got, second) {
		t.Fatalf("distinct failures were not preserved: %v", got)
	}
}

func TestTursoSoakBodyIsFixedAndUnique(t *testing.T) {
	a, b := tursoSoakBody("standard_a", 1), tursoSoakBody("standard_a", 2)
	if len(a) != tursoSoakBodyBytes || len(b) != tursoSoakBodyBytes || bytes.Equal(a, b) {
		t.Fatal("soak bodies are not fixed-size and unique")
	}
}

func TestTursoSoakHarnessLocalSmoke(t *testing.T) {
	e, _ := testEngine(t)
	for _, lane := range tursoSoakLanes {
		mustQueue(t, e, lane.Queue, QueueConfig{LockDurationMs: 60_000, MaxDeliveryCount: 5, Ordering: lane.Ordering})
	}
	evidence, err := newTursoSoakEvidence(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = evidence.close() })
	started := time.Now()
	runner := &tursoSoakRunner{
		e: e, cfg: tursoSoakConfig{Duration: 1250 * time.Millisecond, Window: 100 * time.Millisecond, CallTimeout: time.Second},
		evidence: evidence, started: started, windowStarted: started, states: make([]*tursoSoakLaneState, 0, len(tursoSoakLanes)),
		admission: newTursoSoakAdmission(tursoSoakMaxInFlight), failureSignal: make(chan struct{}, 1),
		stop: make(chan struct{}), storage: e.db.storage.snapshot(e.db.poolStats()),
	}
	for _, lane := range tursoSoakLanes {
		runner.states = append(runner.states, newTursoSoakLaneState(lane))
	}
	if err := runner.run(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := tursoSoakCleanup(ctx, e, time.Second); err != nil {
		t.Fatal(err)
	}
	runner.cleanupDone = true
	_, _, accounting := runner.totals(runner.elapsed())
	if err := judgeTursoSoak(tursoSoakJudgement{
		DurationCompleted: runner.durationDone, AccountingValid: accounting,
		FinalBacklog: runner.lastBacklog, CleanupComplete: runner.cleanupDone,
	}); err != nil {
		t.Fatal(err)
	}
	if err := runner.writeSummary("PASS", nil); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(evidence.dir, "windows.jsonl"))
	if err != nil || bytes.Count(data, []byte{'\n'}) < 1 {
		t.Fatalf("window evidence missing: lines=%d err=%v", bytes.Count(data, []byte{'\n'}), err)
	}
	lines := bytes.Split(bytes.TrimSpace(data), []byte{'\n'})
	var window tursoSoakWindow
	if err := json.Unmarshal(lines[len(lines)-1], &window); err != nil || len(window.Maintenance) != len(MaintenanceTaskNames()) {
		t.Fatalf("window maintenance evidence missing: tasks=%d err=%v", len(window.Maintenance), err)
	}
	var summary tursoSoakSummary
	data, err = os.ReadFile(filepath.Join(evidence.dir, "summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &summary); err != nil || len(summary.FinalMaintenance) != len(MaintenanceTaskNames()) {
		t.Fatalf("summary maintenance evidence missing: tasks=%d err=%v", len(summary.FinalMaintenance), err)
	}
	data, err = os.ReadFile(filepath.Join(evidence.dir, "summary.md"))
	if err != nil || !bytes.Contains(data, []byte("## Maintenance")) {
		t.Fatalf("maintenance markdown missing: err=%v", err)
	}
}
