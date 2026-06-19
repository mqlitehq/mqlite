package mqlite_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/mqlitehq/mqlite"
)

// Example_embedded runs the whole queue in-process — no broker, no HTTP — against a
// local SQLite file, and shows the single-process / single-writer guarantee: a
// second opener of the same DB is rejected with ErrDBLocked (MQLITE-6 / MQLITE-15).
func Example_embedded() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "mqlite-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	dsn := "file:" + filepath.Join(dir, "mq.db")

	eng, err := mqlite.OpenEmbedded(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = eng.Close() }()

	if err := eng.CreateQueue(ctx, "orders", mqlite.QueueConfig{}); err != nil {
		log.Fatal(err)
	}
	if _, err := eng.SendOne(ctx, "orders", mqlite.OutMessage{Body: []byte("order-42")}); err != nil {
		log.Fatal(err)
	}

	msgs, err := eng.Receive(ctx, "orders", mqlite.RecvOpts{})
	if err != nil {
		log.Fatal(err)
	}
	for _, m := range msgs {
		fmt.Printf("received: %s\n", m.Body)
		_ = m.Complete(ctx) // at-least-once; the handler must be idempotent
	}

	// Embedded mode is single-writer: a second process — or a second OpenEmbedded on
	// the same file — is rejected rather than racing the first.
	if _, err := mqlite.OpenEmbedded(ctx, dsn); errors.Is(err, mqlite.ErrDBLocked) {
		fmt.Println("second open rejected: single writer")
	}

	// Output:
	// received: order-42
	// second open rejected: single writer
}
