# Examples

Copy-runnable examples for the four ways to use mqlite: **embedded** in-process, the
**transactional outbox** (the killer feature), a **remote** client with a hands-off
consumer, and raw **curl**. Full API: [api-reference.md](api-reference.md).

## 1. Embedded (in-process, no broker)

One SQLite file, no network, no second process.

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/mqlitehq/mqlite"
)

func main() {
	ctx := context.Background()
	mq, err := mqlite.OpenEmbedded(ctx, "file:./mq.db")
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Close()

	if err := mq.CreateQueue(ctx, "orders", mqlite.QueueConfig{
		LockDuration:     30 * time.Second,
		MaxDeliveryCount: 5,
	}); err != nil {
		log.Fatal(err)
	}

	// produce
	if _, err := mq.SendOne(ctx, "orders", mqlite.OutMessage{Body: []byte(`{"id":42}`)}); err != nil {
		log.Fatal(err)
	}

	// consume (Peek-Lock): receive, do work, then Complete
	msgs, err := mq.Receive(ctx, "orders", mqlite.RecvOpts{Max: 10, Wait: 5 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	for _, m := range msgs {
		fmt.Printf("got: %s\n", m.Body)
		if err := m.Complete(ctx); err != nil { // or m.Abandon(ctx) to retry, m.Reject(ctx) -> DLQ
			log.Fatal(err)
		}
	}
}
```

## 2. Transactional outbox (the killer feature)

Commit a business write **and** the enqueue in **one** SQLite transaction — the
message is enqueued if and only if the business change commits. No dual-write race,
no separate outbox poller. Embedded-only (same DB).

```go
package main

import (
	"context"
	"log"

	"github.com/mqlitehq/mqlite"
	"github.com/mqlitehq/mqlite/engine" // EngineTx + engine.OutMessage are used inside Tx
)

func main() {
	ctx := context.Background()
	mq, err := mqlite.OpenEmbedded(ctx, "file:./app.db")
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Close()
	_ = mq.CreateQueue(ctx, "order-events", mqlite.QueueConfig{})

	// Your app tables live in the same DB. Tx gives you that transaction.
	err = mq.Tx(ctx, func(tx *engine.EngineTx) error {
		// 1) the business write, on the SAME transaction. Use tx.Context(), NOT the ctx you
		//    passed to Tx: on a local store, cancelling a statement mid-transaction leaks the
		//    SQLite connection and wedges the database.
		if _, err := tx.SQL().ExecContext(tx.Context(),
			`INSERT INTO orders(id, total) VALUES(?, ?)`, 42, 1999); err != nil {
			return err // rollback: nothing enqueued
		}
		// 2) enqueue in the same tx — commits atomically with the write above:
		_, err := tx.SendOne("order-events", engine.OutMessage{Body: []byte(`{"order":42}`)})
		return err
	})
	if err != nil {
		log.Fatal(err)
	}
	// Either both happened, or neither did.
}
```

## 3. Remote client + hands-off consumer

Talk to a running broker over HTTP. The `Receiver` runs a managed consume loop:
return `nil` → Complete, return an error → Abandon (with auto lock-renewal and
configurable concurrency).

```go
package main

import (
	"context"
	"log"

	"github.com/mqlitehq/mqlite"
)

func main() {
	ctx := context.Background()
	cli, err := mqlite.Open(ctx, "https://your-mqlite.fly.dev", mqlite.WithToken("mqk_prod_xxx"))
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	// produce
	if _, err := cli.SendOne(ctx, "orders", mqlite.OutMessage{Body: []byte("hi")}); err != nil {
		log.Fatal(err)
	}

	// consume: nil -> Complete, error -> Abandon; auto-renew the lock; 4 in flight.
	err = cli.Receiver("orders", mqlite.WithAutoRenew(), mqlite.WithConcurrency(4)).
		Run(ctx, func(ctx context.Context, m *mqlite.Message) error {
			return process(m.Body) // MUST be idempotent (at-least-once)
		})
	if err != nil {
		log.Fatal(err)
	}
}

func process(body []byte) error { log.Printf("got: %s", body); return nil }
```

## 4. curl (raw HTTP)

Any language can drive the broker — it's one POST per operation. (`body` is base64.)

```bash
HOST=https://your-mqlite.fly.dev
T=mqk_prod_xxx

curl $HOST/                                   # discovery (open, no auth)

curl -H "Authorization: Bearer $T" -H 'Content-Type: application/json' \
  --data '{"name":"orders","config":{}}' \
  $HOST/mqlite.v1.AdminService/CreateQueue

curl -H "Authorization: Bearer $T" -H 'Content-Type: application/json' \
  --data "{\"queue\":\"orders\",\"messages\":[{\"body\":\"$(printf hi | base64)\"}]}" \
  $HOST/mqlite.v1.QueueService/Send

# receive -> returns seq_number + lock_token
curl -H "Authorization: Bearer $T" -H 'Content-Type: application/json' \
  --data '{"queue":"orders","max_messages":1,"wait_time_ms":5000}' \
  $HOST/mqlite.v1.QueueService/Receive

# complete with the lock_token from the receive
curl -H "Authorization: Bearer $T" -H 'Content-Type: application/json' \
  --data '{"queue":"orders","seq_number":1,"lock_token":"<token>"}' \
  $HOST/mqlite.v1.QueueService/Complete
```
