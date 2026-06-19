package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/mqlitehq/mqlite"
)

// TestCmdPurgeDLQ exercises the purge-dlq command end to end against an embedded
// DB: dead-letter a message, then confirm the command clears it (MQLITE-9).
func TestCmdPurgeDLQ(t *testing.T) {
	ctx := context.Background()
	t.Setenv("MQLITE_ENDPOINT", "") // force the embedded path in dial()
	t.Setenv("MQLITE_DB", "file:"+filepath.Join(t.TempDir(), "mq.db"))
	t.Setenv("MQLITE_SYNC", "FULL") // exercise the durability knob through embeddedOpts (MQLITE-7)

	// Arrange: one dead-lettered message. Close releases the file lock (MQLITE-6)
	// before the command opens the same DB.
	c, err := dial(ctx)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if err := c.CreateQueue(ctx, "q", mqlite.QueueConfig{}); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if _, err := c.SendOne(ctx, "q", mqlite.OutMessage{Body: []byte("x")}); err != nil {
		t.Fatalf("send: %v", err)
	}
	msgs, err := c.Receive(ctx, "q", mqlite.RecvOpts{})
	if err != nil || len(msgs) != 1 {
		t.Fatalf("receive: %v n=%d", err, len(msgs))
	}
	if err := msgs[0].Reject(ctx, mqlite.RejectOpts{Reason: "test"}); err != nil {
		t.Fatalf("reject (dead-letter): %v", err)
	}
	_ = c.Close()

	// Act: the CLI command purges the DLQ.
	if err := cmdPurgeDLQ(ctx, []string{"q"}); err != nil {
		t.Fatalf("purge-dlq: %v", err)
	}

	// Assert: nothing dead-lettered remains.
	c2, err := dial(ctx)
	if err != nil {
		t.Fatalf("re-dial: %v", err)
	}
	defer c2.Close()
	dlq, err := c2.Peek(ctx, "q", mqlite.PeekOpts{State: mqlite.DeadLettered})
	if err != nil {
		t.Fatalf("peek: %v", err)
	}
	if len(dlq) != 0 {
		t.Fatalf("DLQ should be empty after purge-dlq, got %d", len(dlq))
	}
}
