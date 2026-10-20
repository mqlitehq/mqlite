# Examples

For authenticated producer/consumer credentials, Go SDK key management, console and
MCP workflows, see [Access keys](access-keys.md) (v0.3.1 and later).

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

For local transactions, `tx.Context()` is deliberately not cancellable. Raw `tx.SQL()`
statements can still execute after the original caller cancels: check `ctx.Err()` between
business statements and return promptly. Mqlite-owned statements such as `tx.SendOne` have
their own checks. Cancellation observed before commit rolls back all the transaction's
writes, even if the callback returns nil. Use raw SQL only with caller-owned tables;
directly modifying mqlite's internal tables is unsupported. See the
[cancellation contract](conformance.md#13--cancellation-context-deadlines).

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

## 4. Topic subscription lifecycle

Use one stable subscription for high-rate request/reply traffic and route replies
inside the consumer by a globally unique `correlation_id`. A subscription per
request adds metadata and cleanup work to the single writer.

```go
if err := cli.Subscribe(ctx, "replies", "checkout-instance-7", nil); err != nil {
	log.Fatal(err)
}

// For a genuinely temporary owner, TTL is a cleanup backstop. It is fixed at
// first creation and does not replace explicit deletion after normal success.
if err := cli.SubscribeWithOptions(ctx, "replies", "checkout-request-01J...", nil,
	mqlite.SubscribeOptions{TTL: 15 * time.Minute}); err != nil {
	log.Fatal(err)
}

page, err := cli.ListSubscriptionsWithOptions(ctx, mqlite.ListSubscriptionsOptions{
	Topic: "replies",
	Limit: 100,
})
if err != nil {
	log.Fatal(err)
}
log.Printf("subscriptions=%d first-page=%d", page.Total, len(page.Subscriptions))

// Empty-only by default. Drain and settle retained replies before retirement.
if err := cli.DeleteSubscription(ctx, "replies", "checkout-instance-7"); err != nil {
	log.Fatal(err)
}
```

For intentional abandonment, call `DeleteSubscriptionWithOptions` with
`mqlite.DeleteSubscriptionOpts{Force: true}`. Force can discard locked work whose
external side effects already happened. The operation may finish as `deleting`;
keep ownership of the globally unique, one-use name and reconcile it through a
paged list with `IncludeDeleting: true`. Do not replay a destructive request after
an unknown transport outcome. At a fixed deadline the temporary subscription is
unavailable immediately and expiry preauthorizes force cleanup. Use globally
unique one-use names; after physical cleanup, same-name creation begins a new
lifetime and the name-only API cannot distinguish it from a delayed old retry.

## 5. curl (raw HTTP)

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
