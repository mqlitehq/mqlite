package engine

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

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
	defer e.Close()
	if !e.Remote() {
		t.Fatalf("expected remote store for dsn %q", dsn)
	}

	// unique queue per run so repeated runs don't collide.
	q := fmt.Sprintf("itest_%d", time.Now().UnixNano())
	if err := e.CreateQueue(ctx, q, QueueConfig{LockDurationMs: 30000, MaxDeliveryCount: 5}); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	t.Cleanup(func() {
		// best-effort cleanup of this run's rows + queue metadata.
		_, _ = e.db.sql.ExecContext(context.Background(), `DELETE FROM messages WHERE queue=?`, q)
		_, _ = e.db.sql.ExecContext(context.Background(), `DELETE FROM queues WHERE name=?`, q)
	})

	// send a batch
	seqs, err := e.SendBatch(ctx, q, []OutMessage{
		{Body: []byte("one"), MessageID: "a"},
		{Body: []byte("two"), MessageID: "b"},
	})
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

	mt, err := e.GetQueueMetrics(ctx, q)
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}
	if mt.Total != 0 {
		t.Fatalf("queue should be drained, got %+v", mt)
	}
	t.Logf("Turso integration OK: round-trip + abandon/redeliver + drain verified")
}
