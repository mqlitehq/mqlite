package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	native "github.com/mqlitehq/mqlite"
	sdk "github.com/mqlitehq/mqlite-go"
	"github.com/mqlitehq/mqlite/engine"
)

// These values stay internal. Evidence contains independent field hashes and
// equality results, never bearer credentials or raw fencing tokens.
type outletValue struct {
	sequence, count               int64
	id, group, correlation, reply string
	subject, content              string
	body                          []byte
	properties                    map[string]string
	token                         string
	deadline, enqueued            time.Time
	state, deadReason, deadDetail string
	native                        *native.Message
	sdk                           *sdk.Message
}

type outletProof struct {
	Client   string            `json:"client"`
	Outlet   string            `json:"outlet"`
	Sequence int64             `json:"sequence"`
	Count    int64             `json:"delivery_count"`
	Expected map[string]string `json:"expected_sha256"`
	Actual   map[string]string `json:"actual_sha256"`
	Matches  map[string]bool   `json:"matches"`
	Passed   bool              `json:"passed"`
}

func outletHashes(v outletValue) map[string]string {
	properties, _ := json.Marshal(v.properties)
	return map[string]string{"message_id": hashBytes([]byte(v.id)), "group_id": hashBytes([]byte(v.group)),
		"correlation_id": hashBytes([]byte(v.correlation)), "reply_to": hashBytes([]byte(v.reply)),
		"subject": hashBytes([]byte(v.subject)), "content_type": hashBytes([]byte(v.content)),
		"body": hashBytes(v.body), "properties": hashBytes(properties), "state": hashBytes([]byte(v.state)),
		"dead_letter_reason": hashBytes([]byte(v.deadReason)), "dead_letter_description": hashBytes([]byte(v.deadDetail))}
}

func proveOutlet(client, outlet string, want, got outletValue) outletProof {
	p := outletProof{Client: client, Outlet: outlet, Sequence: got.sequence, Count: got.count,
		Expected: outletHashes(want), Actual: outletHashes(got), Matches: map[string]bool{}, Passed: true}
	for field, expected := range p.Expected {
		p.Matches[field] = expected == p.Actual[field]
		p.Passed = p.Passed && p.Matches[field]
	}
	// Compare the actual values too; a digest is evidence, not the online oracle.
	p.Passed = p.Passed && want.sequence > 0 && want.sequence == got.sequence && want.count == got.count &&
		want.id == got.id && want.group == got.group && want.correlation == got.correlation && want.reply == got.reply &&
		want.subject == got.subject && want.content == got.content && want.state == got.state && want.deadReason == got.deadReason && want.deadDetail == got.deadDetail && bytes.Equal(want.body, got.body) && reflect.DeepEqual(want.properties, got.properties)
	p.Matches["sequence"], p.Matches["delivery_count"] = want.sequence == got.sequence, want.count == got.count
	return p
}

func outletNative(m *native.Message) outletValue {
	return outletValue{sequence: m.SequenceNumber, count: int64(m.DeliveryCount), id: m.MessageID, group: m.GroupID,
		correlation: m.CorrelationID, reply: m.ReplyTo, subject: m.Subject, content: m.ContentType, body: m.Body,
		properties: m.Properties, token: m.LockToken(), deadline: m.LockedUntil, enqueued: m.EnqueuedAt, native: m}
}
func outletSDK(m *sdk.Message) outletValue {
	return outletValue{sequence: m.SequenceNumber, count: int64(m.DeliveryCount), id: m.MessageID, group: m.GroupID,
		correlation: m.CorrelationID, reply: m.ReplyTo, subject: m.Subject, content: m.ContentType, body: m.Body,
		properties: m.Properties, token: m.LockToken(), deadline: m.LockedUntil, enqueued: m.EnqueuedAt, sdk: m}
}

func outletReceive(ctx context.Context, a *adapter, queue string, max int, attempt string, pick []int64) ([]outletValue, error) {
	var values []outletValue
	if a.sdk != nil {
		messages, err := a.sdk.Receive(ctx, queue, sdk.RecvOpts{Max: max, Attempt: attempt, Pick: pick})
		if err != nil {
			return nil, err
		}
		for _, message := range messages {
			values = append(values, outletSDK(message))
		}
	} else {
		messages, err := a.native.Receive(ctx, queue, native.RecvOpts{Max: max, Attempt: attempt, Pick: pick})
		if err != nil {
			return nil, err
		}
		for _, message := range messages {
			values = append(values, outletNative(message))
		}
	}
	return values, nil
}

func outletPeek(ctx context.Context, a *adapter, queue, state string) ([]outletValue, error) {
	var values []outletValue
	if a.sdk != nil {
		messages, err := a.sdk.Peek(ctx, queue, sdk.PeekOpts{Max: 3, State: sdk.State(state)})
		if err != nil {
			return nil, err
		}
		for _, m := range messages {
			values = append(values, outletValue{sequence: m.SequenceNumber, count: int64(m.DeliveryCount), id: m.MessageID,
				group: m.GroupID, correlation: m.CorrelationID, reply: m.ReplyTo, subject: m.Subject, content: m.ContentType,
				body: m.Body, properties: m.Properties, state: string(m.State), deadReason: m.DeadLetterReason, deadDetail: m.DeadLetterDescription})
		}
	} else {
		messages, err := a.native.Peek(ctx, queue, native.PeekOpts{Max: 3, State: native.State(state)})
		if err != nil {
			return nil, err
		}
		for _, m := range messages {
			values = append(values, outletValue{sequence: m.SequenceNumber, count: int64(m.DeliveryCount), id: m.MessageID,
				group: m.GroupID, correlation: m.CorrelationID, reply: m.ReplyTo, subject: m.Subject, content: m.ContentType,
				body: m.Body, properties: m.Properties, state: string(m.State), deadReason: m.DeadLetterReason, deadDetail: m.DeadLetterDescription})
		}
	}
	return values, nil
}

func outletSettle(ctx context.Context, v outletValue, operation, reason, detail string) error {
	if v.sdk != nil {
		switch operation {
		case "defer":
			return v.sdk.Defer(ctx)
		case "reject":
			return v.sdk.Reject(ctx, sdk.RejectOpts{Reason: reason, Detail: detail})
		default:
			return v.sdk.Complete(ctx)
		}
	}
	switch operation {
	case "defer":
		return v.native.Defer(ctx)
	case "reject":
		return v.native.Reject(ctx, native.RejectOpts{Reason: reason, Detail: detail})
	default:
		return v.native.Complete(ctx)
	}
}

func TestM197OutletOracleRejectsNULCorruption(t *testing.T) {
	want := outletValue{sequence: 1, count: 1, id: "id\x00tail", group: "group\x00tail", correlation: "correlation\x00tail", reply: "reply\x00tail", subject: "subject\x00tail", content: "type\x00tail", body: []byte{0, 1, 255}, properties: map[string]string{"nul": "\x00"}, deadReason: "reason\x00tail", deadDetail: "detail\x00tail"}
	if !proveOutlet("control", "valid", want, want).Passed {
		t.Fatal("valid metadata rejected")
	}
	for _, field := range []string{"id", "group", "correlation", "reply", "subject", "content", "deadReason", "deadDetail"} {
		got := want
		switch field {
		case "id":
			got.id = strings.Replace(got.id, "\x00", "x", 1)
		case "group":
			got.group = strings.Replace(got.group, "\x00", "x", 1)
		case "correlation":
			got.correlation = strings.Replace(got.correlation, "\x00", "x", 1)
		case "reply":
			got.reply = strings.Replace(got.reply, "\x00", "x", 1)
		case "subject":
			got.subject = strings.Replace(got.subject, "\x00", "x", 1)
		case "content":
			got.content = strings.Replace(got.content, "\x00", "x", 1)
		case "deadReason":
			got.deadReason = strings.Replace(got.deadReason, "\x00", "x", 1)
		case "deadDetail":
			got.deadDetail = strings.Replace(got.deadDetail, "\x00", "x", 1)
		}
		if proveOutlet("control", field, want, got).Passed {
			t.Fatal("one-byte NUL corruption accepted")
		}
	}
}

func TestM197MetadataOutlets(t *testing.T) {
	mode, output := os.Getenv("M197_OUTLETS_MODE"), os.Getenv("M197_OUTLETS_OUT")
	if mode == "" {
		t.Skip("explicit metadata outlet fixture mode required")
	}
	if (mode != "local" && mode != "remote") || output == "" {
		t.Fatal("metadata outlet configuration rejected")
	}
	if err := os.MkdirAll(output, 0700); err != nil {
		t.Fatal("evidence directory unavailable")
	}
	files, err := os.ReadDir(output)
	if err != nil || len(files) != 0 {
		t.Fatal("fresh empty evidence directory required")
	}
	local := mode == "local"
	dsn, token := os.Getenv("MQLITE_TEST_DB"), os.Getenv("MQLITE_TEST_DB_AUTH_TOKEN")
	if local {
		dsn = "file:" + filepath.Join(t.TempDir(), "outlets.db")
		token = ""
	} else if !validRemoteDestination(dsn, token, os.Getenv("MQLITE_TEST_DB_RESET_CONFIRM")) {
		t.Fatal("dedicated verification destination rejected")
	}
	observer, restore := observeProvider(dsn, token)
	ctx, cancel := context.WithTimeout(context.Background(), scenarioBudget)
	defer cancel()
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal("owned identity unavailable")
	}
	prefix := "m197-outlets-" + hex.EncodeToString(nonce[:])
	trace := &rpcRecorder{allowed: map[string]bool{}}
	var o *owner
	resources := []string{}
	proofs := []outletProof{}
	acks := []string{}
	intent := map[string][]map[string]string{}
	completed, joined, absent := false, false, false
	graceCompleted := local
	physical := []map[string]any{}
	clientOutlets := map[string]map[string]bool{}
	retainedStates := map[string]native.Metrics{}
	save := func(name string, value any) {
		raw, err := json.MarshalIndent(value, "", "  ")
		if err != nil || os.WriteFile(filepath.Join(output, name), append(raw, '\n'), 0600) != nil {
			t.Error("metadata outlet evidence publication failed")
		}
	}
	defer func() {
		cleanupStarted := time.Now()
		cleanup, stop := context.WithTimeout(context.Background(), cleanupBudget)
		defer stop()
		deadline, _ := cleanup.Deadline()
		work, workCancel := context.WithDeadline(cleanup, deadline.Add(-20*time.Second))
		defer workCancel()
		absent = o != nil && len(resources) == 2
		if o != nil {
			for _, queue := range resources {
				observer.setStage("outlets-owned-cleanup")
				err := call(work, func(c context.Context) error {
					for {
						result, err := o.embedded.DeleteQueueWithOptions(c, queue, native.DeleteQueueOpts{Force: true})
						if errors.Is(err, native.ErrQueueNotFound) {
							return nil
						}
						if err != nil || result.Deleted {
							return err
						}
						if err := c.Err(); err != nil {
							return err
						}
					}
				})
				var queueRows, messageRows int64
				if err == nil {
					err = call(work, func(c context.Context) error {
						return o.embedded.Tx(c, func(tx *engine.EngineTx) error {
							return tx.SQL().QueryRowContext(tx.Context(), `SELECT (SELECT COUNT(*) FROM queues WHERE name=?),(SELECT COUNT(*) FROM messages WHERE queue=?)`, queue, queue).Scan(&queueRows, &messageRows)
						})
					})
				}
				physical = append(physical, map[string]any{"queue": queue, "query_succeeded": err == nil, "queue_rows": queueRows, "message_rows": messageRows})
				if err != nil || queueRows != 0 || messageRows != 0 {
					absent = false
					t.Error("owned physical queue/message absence was not confirmed")
				}
			}
			observer.setStage("outlets-owner-close")
			closeCtx, closeCancel := context.WithDeadline(cleanup, deadline.Add(-10*time.Second))
			joined = o.stop(closeCtx) == nil
			closeCancel()
			if !joined {
				t.Error("metadata outlet owner did not join")
			}
		}
		// Passive only: do not create new requests while observing detached close.
		if !local {
			select {
			case <-time.After(10 * time.Second):
				graceCompleted = true
			case <-cleanup.Done():
				t.Error("cleanup observation budget exhausted")
			}
		}
		provider := observer.snapshot()
		save("provider.json", map[string]any{"events": provider.Events, "dropped": provider.Dropped, "grace_completed": graceCompleted, "server_stream_absence_verified": false, "passive_grace_seconds": map[bool]int{true: 0, false: 10}[local]})
		restore()
		clients := map[string]any{}
		for _, client := range []string{"native", "sdk"} {
			passed := len(clientOutlets[client]) == 5
			for _, ok := range clientOutlets[client] {
				passed = passed && ok
			}
			clients[client] = map[string]any{"passed": passed, "outlets": clientOutlets[client]}
		}
		save("outlets.json", map[string]any{"harness_completed": completed && !t.Failed(), "owner_joined": joined,
			"physical_absence_confirmed": absent, "passed": completed && joined && absent && !t.Failed(), "mode": mode,
			"resources": resources, "retained_states": retainedStates, "physical_rows": physical, "cleanup_elapsed_seconds": time.Since(cleanupStarted).Seconds(), "clients": clients, "intent_sha256": intent, "confirmed_operations": acks, "observations": proofs})
	}()
	cfg := configuration{localFixture: local, output: output}
	cfg.open = func(c context.Context) (*native.Embedded, error) {
		return native.OpenEmbedded(c, dsn, native.WithDBAuthToken(token), native.WithBrokerMode(), native.WithoutBackground(), native.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	}
	o, err = openOwner(ctx, cfg, trace)
	if err != nil {
		t.Fatal("metadata outlet owner open failed")
	}
	op := func(stage string, fn func(context.Context) error) {
		t.Helper()
		observer.setStage(stage)
		if err := call(ctx, fn); err != nil {
			t.Fatalf("metadata outlet operation %s failed (%T)", stage, err)
		}
		acks = append(acks, stage)
	}
	check := func(client, outlet string, want, got outletValue) {
		t.Helper()
		proof := proveOutlet(client, outlet, want, got)
		proofs = append(proofs, proof)
		if !proof.Passed {
			t.Fatal("metadata outlet value/sequence/count mismatch")
		}
	}
	for _, client := range []string{"native", "sdk"} {
		clientOutlets[client] = map[string]bool{}
		queue := prefix + "-" + client // ASCII resource addresses; only message text is under test.
		trace.allowed[queue] = true
		op(client+"/preflight", func(c context.Context) error {
			_, err := o.embedded.Stats(c, queue)
			if !errors.Is(err, native.ErrQueueNotFound) {
				return errors.New("owned name already present or unavailable")
			}
			return nil
		})
		resources = append(resources, queue)
		save("resources.json", resources)
		op(client+"/create", func(c context.Context) error {
			return o.embedded.CreateQueue(c, queue, native.QueueConfig{LockDuration: 60 * time.Second, MaxDeliveryCount: 3})
		})
		want := make([]outletValue, 3)
		for i := range want {
			suffix := fmt.Sprintf("%s-%d", queue, i)
			want[i] = outletValue{id: suffix + "\x00id\"\u65e5\u672c\u8a9e", group: "\x00group-" + suffix,
				correlation: "correlation-" + suffix + "\x00tail", reply: "reply-\"" + suffix + "\x00",
				subject: "subject\x00" + suffix + "\u65e5\u672c\u8a9e", content: "type/" + suffix + "\x00end",
				body: bytes.Repeat([]byte{0, 1, 255, 2}, 64), properties: map[string]string{"nul": strings.Repeat("\x00", 800), "quoted": "\"\\\n\u65e5\u672c\u8a9e"}}
			intent[client] = append(intent[client], outletHashes(want[i]))
		}
		save("intent.json", intent) // Publish independent expected content before Send.
		a := o.clients[client]
		var seqs []int64
		op(client+"/send", func(c context.Context) error {
			if a.sdk != nil {
				messages := make([]sdk.OutMessage, 3)
				for i, v := range want {
					messages[i] = sdk.OutMessage{Body: v.body, MessageID: v.id, GroupID: v.group, CorrelationID: v.correlation, ReplyTo: v.reply, Subject: v.subject, ContentType: v.content, Properties: v.properties}
				}
				seqs, err = a.sdk.Send(c, queue, messages...)
			} else {
				messages := make([]native.OutMessage, 3)
				for i, v := range want {
					messages[i] = native.OutMessage{Body: v.body, MessageID: v.id, GroupID: v.group, CorrelationID: v.correlation, ReplyTo: v.reply, Subject: v.subject, ContentType: v.content, Properties: v.properties}
				}
				seqs, err = a.native.Send(c, queue, messages...)
			}
			return err
		})
		if len(seqs) != 3 || seqs[0] <= 0 || seqs[1] <= seqs[0] || seqs[2] <= seqs[1] {
			t.Fatal("send sequence acknowledgement invalid")
		}
		for i := range want {
			want[i].sequence = seqs[i]
		}
		save(client+"-send-ack.json", seqs)
		var values []outletValue
		op(client+"/peek", func(c context.Context) error { values, err = outletPeek(c, a, queue, "active"); return err })
		if len(values) != 3 {
			t.Fatal("normal Peek roster mismatch")
		}
		for i, got := range values {
			expected := want[i]
			expected.state = "active"
			check(client, "peek", expected, got)
			if got.state != "active" {
				t.Fatal("normal Peek state mismatch")
			}
		}
		clientOutlets[client]["peek"] = true
		attempt := queue + "-attempt"
		op(client+"/receive", func(c context.Context) error { values, err = outletReceive(c, a, queue, 2, attempt, nil); return err })
		if len(values) != 2 {
			t.Fatal("Receive roster mismatch")
		}
		for i, got := range values {
			want[i].count = 1
			check(client, "receive", want[i], got)
			proof := &proofs[len(proofs)-1]
			proof.Actual["fence"] = hashBytes([]byte(got.token))
			proof.Actual["deadline"] = hashBytes([]byte(got.deadline.Format(time.RFC3339Nano)))
			proof.Actual["enqueued"] = hashBytes([]byte(got.enqueued.Format(time.RFC3339Nano)))
			proof.Matches["live_fenced_lease"] = got.token != "" && got.deadline.After(time.Now())
			proof.Passed = proof.Passed && proof.Matches["live_fenced_lease"]
			if got.token == "" || !got.deadline.After(time.Now()) {
				t.Fatal("Receive did not grant a live fenced lease")
			}
		}
		clientOutlets[client]["receive"] = true
		first := append([]outletValue(nil), values...)
		op(client+"/live-attempt", func(c context.Context) error { values, err = outletReceive(c, a, queue, 1, attempt, nil); return err })
		if len(values) != 2 {
			t.Fatal("live attempt roster was truncated")
		}
		for i, got := range values {
			check(client, "attempt_replay", want[i], got)
			proof := &proofs[len(proofs)-1]
			proof.Matches["fence_equal"] = got.token == first[i].token
			proof.Matches["deadline_equal"] = got.deadline.Equal(first[i].deadline)
			proof.Matches["enqueued_equal"] = got.enqueued.Equal(first[i].enqueued)
			proof.Expected["fence"], proof.Actual["fence"] = hashBytes([]byte(first[i].token)), hashBytes([]byte(got.token))
			proof.Expected["deadline"], proof.Actual["deadline"] = hashBytes([]byte(first[i].deadline.Format(time.RFC3339Nano))), hashBytes([]byte(got.deadline.Format(time.RFC3339Nano)))
			proof.Expected["enqueued"], proof.Actual["enqueued"] = hashBytes([]byte(first[i].enqueued.Format(time.RFC3339Nano))), hashBytes([]byte(got.enqueued.Format(time.RFC3339Nano)))
			proof.Passed = proof.Passed && proof.Matches["fence_equal"] && proof.Matches["deadline_equal"] && proof.Matches["enqueued_equal"]
			if !proof.Passed {
				t.Fatal("live replay changed fencing, deadline or enqueue time")
			}
		}
		clientOutlets[client]["attempt_replay"] = true
		op(client+"/defer", func(c context.Context) error { return outletSettle(c, first[0], "defer", "", "") })
		op(client+"/complete-second", func(c context.Context) error { return outletSettle(c, first[1], "complete", "", "") })
		op(client+"/deferred", func(c context.Context) error {
			values, err = outletReceive(c, a, queue, 1, "", []int64{seqs[0]})
			return err
		})
		if len(values) != 1 {
			t.Fatal("deferred roster mismatch")
		}
		want[0].count = 2
		check(client, "deferred", want[0], values[0])
		deferredProof := &proofs[len(proofs)-1]
		deferredProof.Actual["fence"] = hashBytes([]byte(values[0].token))
		deferredProof.Matches["fresh_live_fence"] = values[0].token != "" && values[0].token != first[0].token && values[0].deadline.After(time.Now())
		deferredProof.Passed = deferredProof.Passed && deferredProof.Matches["fresh_live_fence"]
		if values[0].token == "" || values[0].token == first[0].token || !values[0].deadline.After(time.Now()) {
			t.Fatal("deferred receive did not issue a fresh live fence")
		}
		clientOutlets[client]["deferred"] = true
		op(client+"/complete-deferred", func(c context.Context) error { return outletSettle(c, values[0], "complete", "", "") })
		op(client+"/receive-third", func(c context.Context) error {
			values, err = outletReceive(c, a, queue, 1, queue+"-third", nil)
			return err
		})
		if len(values) != 1 {
			t.Fatal("third roster mismatch")
		}
		want[2].count = 1
		check(client, "receive-third", want[2], values[0])
		if values[0].token == "" || !values[0].deadline.After(time.Now()) {
			t.Fatal("third receive did not grant a live fence")
		}
		reason, detail := "reason\x00tail\"", "detail\x00\u65e5\u672c\u8a9e\""
		want[2].state, want[2].deadReason, want[2].deadDetail = "dead_lettered", reason, detail
		save(client+"-dlq-intent.json", outletHashes(want[2]))
		op(client+"/reject", func(c context.Context) error { return outletSettle(c, values[0], "reject", reason, detail) })
		op(client+"/peek-dlq", func(c context.Context) error { values, err = outletPeek(c, a, queue, "dead_lettered"); return err })
		if len(values) != 1 {
			t.Fatal("DLQ roster mismatch")
		}
		check(client, "dlq", want[2], values[0])
		if values[0].state != "dead_lettered" || values[0].deadReason != reason || values[0].deadDetail != detail {
			t.Fatal("DLQ state/reason/detail bytes changed")
		}
		clientOutlets[client]["dlq"] = true
		op(client+"/final-state", func(c context.Context) error {
			stats, err := o.embedded.Stats(c, queue)
			if err != nil {
				return err
			}
			retainedStates[client] = stats
			if stats.Total != 1 || stats.DeadLettered != 1 || stats.Active != 0 || stats.Locked != 0 || stats.Deferred != 0 || stats.Scheduled != 0 {
				return errors.New("expected one verified DLQ row and no other retained state")
			}
			return nil
		})
	}
	completed = true
}
