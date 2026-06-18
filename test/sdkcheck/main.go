// Command sdkcheck is the SDK end-to-end suite (the "用 SDK" path). It drives a
// running broker through the remote mqlite.Client, and exercises the embedded
// engine (mqlite.OpenEmbedded) for the same-DB transactional enqueue (Tx) that
// only exists in-process.
//
// Env: MQLITE_ENDPOINT, MQLITE_TOKEN, MQLITE_RUNID (set by run.sh).
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/mqlitehq/mqlite"
	"github.com/mqlitehq/mqlite/engine"
)

var (
	passed int
	failed int
	rid    = envOr("MQLITE_RUNID", "local")
)

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func section(s string) { fmt.Printf("\n\033[33m── %s\033[0m\n", s) }

func check(cond bool, label string) {
	if cond {
		passed++
		fmt.Printf("  \033[32m✓\033[0m %s\n", label)
	} else {
		failed++
		fmt.Printf("  \033[31m✗ %s\033[0m\n", label)
	}
}

func main() {
	ctx := context.Background()
	endpoint := os.Getenv("MQLITE_ENDPOINT")
	if endpoint == "" {
		fmt.Println("MQLITE_ENDPOINT not set")
		os.Exit(2)
	}
	cli, err := mqlite.Open(ctx, endpoint, mqlite.WithToken(os.Getenv("MQLITE_TOKEN")))
	if err != nil {
		fmt.Println("open client:", err)
		os.Exit(2)
	}
	defer cli.Close()

	fmt.Printf("SDK suite — endpoint=%s runid=%s\n", endpoint, rid)

	remoteLifecycle(ctx, cli)
	remoteRedelivery(ctx, cli)
	remoteDLQRedrive(ctx, cli)
	remoteSessions(ctx, cli)
	remoteTopic(ctx, cli)
	remoteDedup(ctx, cli)
	remoteDefer(ctx, cli)
	remoteSchedule(ctx, cli)
	remoteReceiveAndDelete(ctx, cli)
	remoteReceiverRun(ctx, cli)
	remoteAllFields(ctx, cli)
	remoteMaxSize(ctx, cli)
	remoteCancelScheduled(ctx, cli)
	embeddedTx(ctx)

	fmt.Printf("\nSDK: \033[32m%d passed\033[0m, \033[31m%d failed\033[0m\n", passed, failed)
	if failed > 0 {
		os.Exit(1)
	}
}

func recv1(ctx context.Context, cli *mqlite.Client, q string, wait time.Duration) *mqlite.Message {
	ms, err := cli.Receive(ctx, q, mqlite.RecvOpts{Wait: wait})
	if err != nil || len(ms) == 0 {
		return nil
	}
	return ms[0]
}

func remoteLifecycle(ctx context.Context, cli *mqlite.Client) {
	section("client: lifecycle (send → receive → ack)")
	q := rid + "_sdk_basic"
	check(cli.CreateQueue(ctx, q, mqlite.QueueConfig{}) == nil, "create queue")
	seq, err := cli.SendOne(ctx, q, mqlite.OutMessage{Body: []byte("hi"), MessageID: "m1", Subject: "x"})
	check(err == nil && seq >= 1, "send returns seq")
	m := recv1(ctx, cli, q, 3*time.Second)
	check(m != nil && string(m.Body) == "hi" && m.MessageID == "m1", "receive round-trips body+id")
	check(m != nil && m.Complete(ctx) == nil, "ack")
	mt, _ := cli.Stats(ctx, q)
	check(mt.Total == 0, "queue drained")
}

func remoteRedelivery(ctx context.Context, cli *mqlite.Client) {
	section("client: nack → redelivery (delivery_count grows)")
	q := rid + "_sdk_redeliver"
	cli.CreateQueue(ctx, q, mqlite.QueueConfig{MaxDeliveryCount: 10})
	cli.SendOne(ctx, q, mqlite.OutMessage{Body: []byte("x")})
	m := recv1(ctx, cli, q, 2*time.Second)
	check(m != nil && m.DeliveryCount == 1, "first delivery count 1")
	if m != nil {
		check(m.Abandon(ctx) == nil, "nack")
	}
	m2 := recv1(ctx, cli, q, 2*time.Second)
	check(m2 != nil && m2.DeliveryCount == 2, "redelivered with count 2")
	if m2 != nil {
		m2.Complete(ctx)
	}
}

func remoteDLQRedrive(ctx context.Context, cli *mqlite.Client) {
	section("client: dead-letter + redrive")
	q := rid + "_sdk_dlq"
	cli.CreateQueue(ctx, q, mqlite.QueueConfig{MaxDeliveryCount: 2})
	cli.SendOne(ctx, q, mqlite.OutMessage{Body: []byte("poison")})
	for i := 0; i < 2; i++ {
		if m := recv1(ctx, cli, q, 2*time.Second); m != nil {
			m.Abandon(ctx)
		}
	}
	pk, _ := cli.Peek(ctx, q, mqlite.PeekOpts{State: mqlite.DeadLettered})
	check(len(pk) == 1, "message in DLQ after max deliveries")
	moved, err := cli.Redrive(ctx, q)
	check(err == nil && moved >= 1, "redrive moved >= 1")
	check(recv1(ctx, cli, q, 2*time.Second) != nil, "receivable again after redrive")
}

func remoteSessions(ctx context.Context, cli *mqlite.Client) {
	section("client: MessageGroupId ordering")
	q := rid + "_sdk_sess"
	cli.CreateQueue(ctx, q, mqlite.QueueConfig{})
	cli.SendOne(ctx, q, mqlite.OutMessage{Body: []byte("s1"), GroupID: "A"})
	cli.SendOne(ctx, q, mqlite.OutMessage{Body: []byte("s2"), GroupID: "A"})
	m1 := recv1(ctx, cli, q, 2*time.Second)
	check(m1 != nil && string(m1.Body) == "s1", "group head delivered first")
	check(recv1(ctx, cli, q, 300*time.Millisecond) == nil, "rest of group blocked while head in-flight")
	if m1 != nil {
		m1.Complete(ctx)
	}
	m2 := recv1(ctx, cli, q, 2*time.Second)
	check(m2 != nil && string(m2.Body) == "s2", "next in group after ack")
}

func remoteTopic(ctx context.Context, cli *mqlite.Client) {
	section("client: topic fan-out + filter")
	topic := rid + "_sdk_topic"
	cli.Subscribe(ctx, topic, topic+"_all", nil)
	cli.Subscribe(ctx, topic, topic+"_paid", &mqlite.Filter{SubjectPrefix: "payment."})
	cli.SendOne(ctx, topic, mqlite.OutMessage{Body: []byte("o"), Subject: "order.created"})
	cli.SendOne(ctx, topic, mqlite.OutMessage{Body: []byte("p"), Subject: "payment.captured"})
	all, _ := cli.Stats(ctx, topic+"_all")
	paid, _ := cli.Stats(ctx, topic+"_paid")
	check(all.Active == 2, "subscription 'all' received both")
	check(paid.Active == 1, "subscription 'paid' filtered to payment.*")
}

func remoteDedup(ctx context.Context, cli *mqlite.Client) {
	section("client: dedup window + conflict")
	q := rid + "_sdk_dedup"
	cli.CreateQueue(ctx, q, mqlite.QueueConfig{DedupWindow: 10 * time.Minute})
	s1, _ := cli.SendOne(ctx, q, mqlite.OutMessage{Body: []byte("p"), MessageID: "d1"})
	s2, _ := cli.SendOne(ctx, q, mqlite.OutMessage{Body: []byte("p"), MessageID: "d1"})
	check(s1 == s2, "duplicate returns original seq")
	_, err := cli.SendOne(ctx, q, mqlite.OutMessage{Body: []byte("DIFFERENT"), MessageID: "d1"})
	check(errors.Is(err, mqlite.ErrDedupConflict), "same id / different body -> conflict")
}

func remoteDefer(ctx context.Context, cli *mqlite.Client) {
	section("client: defer / receive-deferred")
	q := rid + "_sdk_defer"
	cli.CreateQueue(ctx, q, mqlite.QueueConfig{})
	seq, _ := cli.SendOne(ctx, q, mqlite.OutMessage{Body: []byte("later")})
	m := recv1(ctx, cli, q, 2*time.Second)
	check(m != nil && m.Defer(ctx) == nil, "defer")
	check(recv1(ctx, cli, q, 300*time.Millisecond) == nil, "hidden from normal receive")
	dm, _ := cli.Receive(ctx, q, mqlite.RecvOpts{Pick: []int64{seq}})
	check(len(dm) == 1, "Pick fetches deferred by seq")
}

func remoteSchedule(ctx context.Context, cli *mqlite.Client) {
	section("client: scheduled delivery")
	q := rid + "_sdk_sched"
	cli.CreateQueue(ctx, q, mqlite.QueueConfig{})
	// generous delay so the "before" check finishes before activation even on a
	// remote backend with hundreds-of-ms round-trips.
	cli.SendOne(ctx, q, mqlite.OutMessage{Body: []byte("s")}, mqlite.SendOpts{At: time.Now().Add(2 * time.Second)})
	check(recv1(ctx, cli, q, 0) == nil, "not visible before time") // immediate, no long-poll
	time.Sleep(4 * time.Second)
	check(recv1(ctx, cli, q, 3*time.Second) != nil, "visible after time")
}

func remoteReceiveAndDelete(ctx context.Context, cli *mqlite.Client) {
	section("client: receive-and-delete")
	q := rid + "_sdk_rad"
	cli.CreateQueue(ctx, q, mqlite.QueueConfig{})
	cli.SendOne(ctx, q, mqlite.OutMessage{Body: []byte("t")})
	ms, _ := cli.Receive(ctx, q, mqlite.RecvOpts{Wait: 2 * time.Second, AtMostOnce: true})
	check(len(ms) == 1, "received one")
	mt, _ := cli.Stats(ctx, q)
	check(mt.Total == 0, "removed immediately")
}

func remoteReceiverRun(ctx context.Context, cli *mqlite.Client) {
	section("client: Receiver.Run (auto-complete, concurrency)")
	q := rid + "_sdk_receiver"
	cli.CreateQueue(ctx, q, mqlite.QueueConfig{})
	const n = 4
	for i := 0; i < n; i++ {
		cli.SendOne(ctx, q, mqlite.OutMessage{Body: []byte("job")})
	}
	var done int64
	runCtx, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	go cli.Receiver(q, mqlite.WithConcurrency(2)).Run(runCtx, func(c context.Context, m *mqlite.Message) error {
		atomic.AddInt64(&done, 1)
		return nil // -> auto Ack
	})
	deadline := time.Now().Add(8 * time.Second)
	for atomic.LoadInt64(&done) < n && time.Now().Before(deadline) {
		time.Sleep(30 * time.Millisecond)
	}
	check(atomic.LoadInt64(&done) >= n, "Receiver processed all jobs")
	mt, _ := cli.Stats(ctx, q)
	check(mt.Total == 0, "queue drained by Receiver")
}

func remoteAllFields(ctx context.Context, cli *mqlite.Client) {
	section("client: all message fields round-trip")
	q := rid + "_sdk_fields"
	cli.CreateQueue(ctx, q, mqlite.QueueConfig{})
	in := mqlite.OutMessage{
		Body: []byte("body \x00\xff"), MessageID: "M1", GroupID: "S1", CorrelationID: "C1",
		Subject: "sub.x", ContentType: "application/json",
		Properties: map[string]string{"tenant": "acme", "k": "中文🚀"},
	}
	cli.SendOne(ctx, q, in)
	m := recv1(ctx, cli, q, 2*time.Second)
	ok := m != nil && string(m.Body) == string(in.Body) && m.MessageID == "M1" && m.GroupID == "S1" &&
		m.CorrelationID == "C1" && m.Subject == "sub.x" && m.ContentType == "application/json" &&
		m.Properties["k"] == "中文🚀"
	check(ok, "all fields (id/group/correlation/subject/content_type/properties/body) preserved")
}

func remoteMaxSize(ctx context.Context, cli *mqlite.Client) {
	section("client: max message size boundary")
	q := rid + "_sdk_size"
	cli.CreateQueue(ctx, q, mqlite.QueueConfig{})
	cap := 1 << 20
	if v := os.Getenv("MQLITE_MAX_MESSAGE_BYTES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cap = n
		}
	}
	if _, err := cli.SendOne(ctx, q, mqlite.OutMessage{Body: make([]byte, cap)}); err != nil {
		check(false, "body == cap accepted: "+err.Error())
	} else {
		check(true, "body == cap accepted")
	}
	_, err := cli.SendOne(ctx, q, mqlite.OutMessage{Body: make([]byte, cap+1)})
	check(errors.Is(err, mqlite.ErrMessageTooLarge), "body == cap+1 -> ErrMessageTooLarge")
}

func remoteCancelScheduled(ctx context.Context, cli *mqlite.Client) {
	section("client: cancel scheduled")
	q := rid + "_sdk_cancel"
	cli.CreateQueue(ctx, q, mqlite.QueueConfig{})
	seq, _ := cli.SendOne(ctx, q, mqlite.OutMessage{Body: []byte("later")}, mqlite.SendOpts{At: time.Now().Add(time.Minute)})
	pk, _ := cli.Peek(ctx, q, mqlite.PeekOpts{State: mqlite.Scheduled})
	check(len(pk) == 1, "scheduled present before cancel")
	check(cli.Cancel(ctx, q, seq) == nil, "cancel ok")
	pk, _ = cli.Peek(ctx, q, mqlite.PeekOpts{State: mqlite.Scheduled})
	check(len(pk) == 0, "scheduled gone after cancel")
}

func embeddedTx(ctx context.Context) {
	section("embedded: same-DB transactional enqueue (Tx)")
	dir, _ := os.MkdirTemp("", "mqlite-tx")
	defer os.RemoveAll(dir)
	emb, err := mqlite.OpenEmbedded(ctx, "file:"+dir+"/e.db")
	if err != nil {
		check(false, "open embedded: "+err.Error())
		return
	}
	defer emb.Close()
	emb.CreateQueue(ctx, "q", mqlite.QueueConfig{})

	// commit: business write + enqueue together
	err = emb.Tx(ctx, func(tx *engine.EngineTx) error {
		if _, e := tx.SQL().ExecContext(ctx, `CREATE TABLE IF NOT EXISTS biz(id INTEGER PRIMARY KEY)`); e != nil {
			return e
		}
		if _, e := tx.SQL().ExecContext(ctx, `INSERT INTO biz(id) VALUES (1)`); e != nil {
			return e
		}
		_, e := tx.SendOne("q", engine.OutMessage{Body: []byte("evt")})
		return e
	})
	check(err == nil, "tx commit succeeds")
	ms, _ := emb.Receive(ctx, "q", mqlite.RecvOpts{Wait: time.Second})
	check(len(ms) == 1, "committed tx enqueued the message")
	for _, m := range ms {
		m.Complete(ctx)
	}

	// rollback: returning an error enqueues nothing
	boom := errors.New("boom")
	err = emb.Tx(ctx, func(tx *engine.EngineTx) error {
		tx.SendOne("q", engine.OutMessage{Body: []byte("ghost")})
		return boom
	})
	check(errors.Is(err, boom), "tx rollback returns the error")
	ms, _ = emb.Receive(ctx, "q", mqlite.RecvOpts{Wait: 200 * time.Millisecond})
	check(len(ms) == 0, "rolled-back tx enqueued nothing")
}
