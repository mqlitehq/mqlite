package main

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mqlitehq/mqlite"
)

func TestEmbeddedOptsMaxSubscriptions(t *testing.T) {
	resetGlobals(t)
	ctx := context.Background()
	t.Setenv("MQLITE_ENDPOINT", "")
	t.Setenv("MQLITE_DB", "file:"+filepath.Join(t.TempDir(), "bounded.db"))
	t.Setenv("MQLITE_MAX_SUBSCRIPTIONS", "1")
	c, err := dial(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Subscribe(ctx, "topic", "first", nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Subscribe(ctx, "topic", "second", nil); !errors.Is(err, mqlite.ErrSubscriptionLimit) {
		t.Fatalf("second subscription = %v, want ErrSubscriptionLimit", err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	t.Setenv("MQLITE_DB", "file:"+filepath.Join(t.TempDir(), "default.db"))
	t.Setenv("MQLITE_MAX_SUBSCRIPTIONS", "not-a-number")
	oldStderr := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = w
	c, openErr := dial(ctx)
	_ = w.Close()
	os.Stderr = oldStderr
	warning, _ := io.ReadAll(r)
	_ = r.Close()
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer c.Close()
	if !strings.Contains(string(warning), `ignoring unparseable MQLITE_MAX_SUBSCRIPTIONS="not-a-number"`) {
		t.Fatalf("malformed limit warning = %q", warning)
	}
	for _, name := range []string{"first", "second"} {
		if err := c.Subscribe(ctx, "topic", name, nil); err != nil {
			t.Fatalf("default limit subscription %q: %v", name, err)
		}
	}
}

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

	// Act: the CLI command purges the DLQ (unbounded purge now needs an explicit --all).
	if err := cmdPurgeDLQ(ctx, []string{"q", "--all"}); err != nil {
		t.Fatalf("purge-dlq: %v", err)
	}
	// An unbounded purge without --all must be refused.
	if err := cmdPurgeDLQ(ctx, []string{"q"}); err == nil {
		t.Error("purge-dlq without a bound or --all should be refused")
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
