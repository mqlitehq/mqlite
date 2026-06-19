package wire_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/wire"
)

// The JSON field names ARE the contract between the broker and every client
// (curl, the Go SDK, future SDKs). Pin them so a struct-tag edit can't silently
// break the wire format (MQLITE-26).
func TestMessageJSONContract(t *testing.T) {
	m := wire.Message{
		SeqNumber: 7, EnqueuedAtMs: 1700000000000, DeliveryCount: 2, LockToken: "tok",
		State: "active", MessageID: "m1", CorrelationID: "c1", ReplyTo: "r", GroupID: "g",
		ContentType: "application/json", Subject: "subj", Properties: map[string]string{"k": "v"},
		Body: []byte{0x00, 0x01, 0xff, 'h', 'i'},
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	js := string(b)
	for _, want := range []string{
		`"seq_number":7`, `"enqueued_at_ms":1700000000000`, `"delivery_count":2`,
		`"lock_token":"tok"`, `"group_id":"g"`, `"message_id":"m1"`, `"body":"`,
	} {
		if !strings.Contains(js, want) {
			t.Errorf("wire.Message JSON missing %q:\n%s", want, js)
		}
	}

	var back wire.Message
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !bytes.Equal(back.Body, m.Body) {
		t.Errorf("body did not round-trip through base64: %v != %v", back.Body, m.Body)
	}
	if back.SeqNumber != m.SeqNumber || back.GroupID != m.GroupID ||
		back.EnqueuedAtMs != m.EnqueuedAtMs || back.Properties["k"] != "v" {
		t.Errorf("round-trip mismatch: %+v", back)
	}
}

// The idempotent-receive key travels as receive_attempt_id (SQS-style); a rename
// would silently disable idempotent receive end to end.
func TestReceiveAttemptIDFieldName(t *testing.T) {
	b, _ := json.Marshal(wire.ReceiveRequest{Queue: "q", MaxMessages: 2, AttemptID: "a1"})
	if !strings.Contains(string(b), `"receive_attempt_id":"a1"`) {
		t.Fatalf("receive_attempt_id field name drifted: %s", b)
	}
}

func TestConversions(t *testing.T) {
	em := &engine.Message{
		SeqNumber: 3, Body: []byte("x"), GroupID: "g", MessageID: "m", CorrelationID: "c",
		ReplyTo: "r", Subject: "s", ContentType: "ct", Properties: map[string]string{"a": "b"},
		DeliveryCount: 1, EnqueuedAtMs: 9, LockedUntilMs: 99, LockToken: "lt",
	}
	wm := wire.FromEngineMessage(em)
	if wm.SeqNumber != 3 || string(wm.Body) != "x" || wm.GroupID != "g" ||
		wm.LockToken != "lt" || wm.DeliveryCount != 1 || wm.Properties["a"] != "b" {
		t.Errorf("FromEngineMessage: %+v", wm)
	}

	out := wire.Message{Body: []byte("y"), MessageID: "m2", GroupID: "g2", Subject: "s2",
		ContentType: "ct2", ReplyTo: "rt", CorrelationID: "co", Properties: map[string]string{"c": "d"}}.ToOut()
	if string(out.Body) != "y" || out.MessageID != "m2" || out.GroupID != "g2" ||
		out.Subject != "s2" || out.Properties["c"] != "d" {
		t.Errorf("ToOut: %+v", out)
	}

	p := &engine.PeekedMessage{
		SeqNumber: 4, State: engine.StateDeadLettered, Body: []byte("z"),
		DeadLetterReason: "rr", DeadLetterDescription: "dd", VisibleAtMs: 5,
	}
	wp := wire.FromPeeked(p)
	if wp.SeqNumber != 4 || wp.State != "dead_lettered" || string(wp.Body) != "z" ||
		wp.DeadLetterReason != "rr" || wp.DeadLetterDescription != "dd" || wp.VisibleAtMs != 5 {
		t.Errorf("FromPeeked: %+v", wp)
	}

	dle := false
	cfg := wire.QueueConfigJSON{
		Kind: "subscription", LockDurationMs: 30000, MaxDeliveryCount: 5, DefaultTTLMs: 1000,
		DeadLetterOnExpire: &dle, DedupWindowMs: 60000, OrderingMode: "group_fifo",
	}.ToConfig()
	if cfg.Kind != "subscription" || cfg.LockDurationMs != 30000 || cfg.MaxDeliveryCount != 5 ||
		cfg.DefaultTTLMs != 1000 || cfg.DeadLetterOnExpire == nil || *cfg.DeadLetterOnExpire ||
		cfg.DedupWindowMs != 60000 || cfg.Ordering != engine.OrderGroupFIFO {
		t.Errorf("ToConfig: %+v", cfg)
	}
}
