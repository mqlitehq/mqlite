package wire_test

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"
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

// ── Access-key JSON contract ────────────────────────────────────────────────

func TestAccessKeyJSONContract(t *testing.T) {
	key := wire.FromAccessKey(engine.AccessKey{ID: "id", Name: "worker", Permissions: engine.KeyManage,
		CreatedAtMs: 11, ExpiresAtMs: 22, RevokedAtMs: 33})
	keyJSON := `{"id":"id","name":"worker","permissions":["manage"],"created_at_ms":11,"expires_at_ms":22,"revoked_at_ms":33}`
	for _, tt := range []struct {
		name  string
		value any
		want  string
	}{
		{"metadata", key, keyJSON},
		{"create request", wire.CreateKeyRequest{ID: "id", Name: "worker", Permissions: []string{"send", "listen"}, ExpiresAtMs: 22}, `{"id":"id","name":"worker","permissions":["send","listen"],"expires_at_ms":22}`},
		{"create response", wire.CreateKeyResponse{Key: key, Token: "secret"}, `{"key":` + keyJSON + `,"token":"secret"}`},
		{"list request", wire.ListKeysRequest{AfterID: "id", Limit: 2}, `{"after_id":"id","limit":2}`},
		{"default list", wire.ListKeysRequest{}, `{}`},
		{"list response", wire.ListKeysResponse{Keys: []wire.AccessKey{key}, NextAfterID: "id"}, `{"keys":[` + keyJSON + `],"next_after_id":"id"}`},
		{"empty list", wire.ListKeysResponse{Keys: []wire.AccessKey{}}, `{"keys":[]}`},
		{"revoke request", wire.RevokeKeyRequest{ID: "id"}, `{"id":"id"}`},
		{"revoke response", wire.RevokeKeyResponse{Ok: true}, `{"ok":true}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := json.Marshal(tt.value)
			if err != nil || string(got) != tt.want {
				t.Fatalf("JSON contract = %s (%v), want %s", got, err, tt.want)
			}
		})
	}
}

func TestAccessKeyResponseValidation(t *testing.T) {
	id := strings.Repeat("a", 32)
	token := "mqk_" + strings.Repeat("b", 64)
	request := wire.CreateKeyRequest{ID: id, Name: "worker", Permissions: []string{"listen", "send"}, ExpiresAtMs: 200}
	metadata := func() map[string]any {
		return map[string]any{"id": id, "name": "worker", "permissions": []string{"send", "listen"},
			"created_at_ms": 100, "expires_at_ms": 200, "revoked_at_ms": 0}
	}
	encode := func(value any) []byte {
		t.Helper()
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	create := func(key any, secret any) []byte { return encode(map[string]any{"key": key, "token": secret}) }
	good := create(metadata(), token)
	if result, err := wire.DecodeCreateKeyResponse(good, request); err != nil || result.Key.ID != id || result.Token != token {
		t.Fatalf("valid create rejected: %v", err)
	}
	invalidCreate := [][]byte{nil, []byte("null"), []byte("{}"), []byte("[]"), good[:len(good)-1], append(append([]byte{}, good...), []byte(" {}")...),
		[]byte(`{"key":null,"token":null}`), create(metadata(), nil), create(metadata(), 1), create(metadata(), "partial-secret"),
		create(nil, token), []byte(`{"token":"` + token + `"}`)}
	for _, field := range []string{"id", "name", "permissions", "created_at_ms", "expires_at_ms", "revoked_at_ms"} {
		for _, null := range []bool{false, true} {
			key := metadata()
			delete(key, field)
			if null {
				key[field] = nil
			}
			invalidCreate = append(invalidCreate, create(key, token))
		}
	}
	for field, values := range map[string][]any{
		"id":            {"bad", strings.Repeat("c", 32), 1},
		"name":          {"", "different", " worker", strings.Repeat("x", 129), "nul\x00", 1},
		"permissions":   {[]string{}, []string{"send"}, []string{"send", "send", "listen"}, []string{"manage"}, []string{"read"}, "send"},
		"created_at_ms": {-1, 200, 201, 1.5, "100"},
		"expires_at_ms": {-1, 99, 100, 201, 0},
		"revoked_at_ms": {-1, 1},
	} {
		for _, value := range values {
			key := metadata()
			key[field] = value
			invalidCreate = append(invalidCreate, create(key, token))
		}
	}
	for i, data := range invalidCreate {
		result, err := wire.DecodeCreateKeyResponse(data, request)
		if err == nil || !reflect.DeepEqual(result, wire.CreateKeyResponse{}) || strings.Contains(err.Error(), token) || strings.Contains(err.Error(), "partial-secret") {
			t.Fatalf("invalid create response %d accepted or leaked data", i)
		}
	}
	invalidRequest := request
	invalidRequest.Permissions = []string{"manage", "unknown"}
	if _, err := wire.DecodeCreateKeyResponse(good, invalidRequest); err == nil {
		t.Fatal("invalid requested permissions accepted")
	}
	// Unknown fields never propagate into normalized metadata or one-time results.
	key := metadata()
	key["token_hash"] = "hidden-digest"
	if result, err := wire.DecodeCreateKeyResponse(create(key, token), request); err != nil || strings.Contains(string(encode(result)), "hidden-digest") {
		t.Fatal("unknown creation fields were not safely discarded")
	}
	for _, rights := range [][]string{{"send"}, {"listen"}, {"send", "listen"}, {"manage"}} {
		key := metadata()
		key["permissions"], key["created_at_ms"], key["expires_at_ms"] = rights, 0, 0
		req := request
		req.Permissions, req.ExpiresAtMs = rights, 0
		if _, err := wire.DecodeCreateKeyResponse(create(key, token), req); err != nil {
			t.Fatalf("canonical permission set %v or valid epoch-zero metadata rejected: %v", rights, err)
		}
	}

	for _, bad := range []string{"null", "{}", `{"keys":null}`, `{"keys":{}}`, `{"keys":[]}{}`, `{"keys":[],"next_after_id":"` + id + `"}`, `{"keys":[],"next_after_id":null}`, `{"keys":[],"next_after_id":1}`} {
		if result, err := wire.DecodeListKeysResponse([]byte(bad), wire.ListKeysRequest{}); err == nil || !reflect.DeepEqual(result, wire.ListKeysResponse{}) {
			t.Fatalf("invalid list response accepted: %s", bad)
		}
	}
	validList := encode(map[string]any{"keys": []any{metadata()}, "next_after_id": id})
	if page, err := wire.DecodeListKeysResponse(validList, wire.ListKeysRequest{Limit: 1}); err != nil || len(page.Keys) != 1 || page.NextAfterID != id {
		t.Fatalf("valid list page rejected: %v", err)
	}
	for _, req := range []wire.ListKeysRequest{{AfterID: "bad"}, {Limit: -1}, {Limit: 1001}, {Limit: 2}, {Limit: 1, AfterID: id}, {Limit: 1, AfterID: strings.Repeat("c", 32)}} {
		if _, err := wire.DecodeListKeysResponse(validList, req); err == nil {
			t.Fatalf("invalid paging contract accepted: %+v", req)
		}
	}
	first, second := metadata(), metadata()
	second["id"] = strings.Repeat("c", 32)
	for _, bad := range []any{
		map[string]any{"keys": []any{first, first}},
		map[string]any{"keys": []any{second, first}},
		map[string]any{"keys": []any{first, second}, "next_after_id": id},
		map[string]any{"keys": []any{first}, "next_after_id": strings.Repeat("c", 32)},
		map[string]any{"keys": []any{map[string]any{"id": id}}},
	} {
		if _, err := wire.DecodeListKeysResponse(encode(bad), wire.ListKeysRequest{Limit: 2}); err == nil {
			t.Fatal("unordered, incomplete, or inconsistent page accepted")
		}
	}
	if _, err := wire.DecodeListKeysResponse(encode(map[string]any{"keys": []any{first, second}}), wire.ListKeysRequest{Limit: 1}); err == nil {
		t.Fatal("oversized page accepted")
	}
	key = metadata()
	key["revoked_at_ms"] = 1 // A wall-clock rollback can revoke before created_at.
	key["token"] = token
	if page, err := wire.DecodeListKeysResponse(encode(map[string]any{"keys": []any{key}}), wire.ListKeysRequest{}); err != nil || len(page.Keys) != 1 || strings.Contains(string(encode(page)), token) {
		t.Fatal("valid revoked metadata rejected or unknown secret field returned")
	}
	if page, err := wire.DecodeListKeysResponse([]byte(`{"keys":[]}`), wire.ListKeysRequest{}); err != nil || page.Keys == nil || len(page.Keys) != 0 {
		t.Fatal("valid empty page rejected")
	}
	for _, bad := range []string{"null", "{}", `{"ok":false}`, `{"ok":null}`, `{"ok":1}`, `{"ok":"true"}`, `{"ok":true}{}`, `{"ok":`} {
		if result, err := wire.DecodeRevokeKeyResponse([]byte(bad)); err == nil || result.Ok {
			t.Fatalf("invalid revocation acknowledgement accepted: %s", bad)
		}
	}
	if result, err := wire.DecodeRevokeKeyResponse([]byte(`{"ok":true,"unknown":"secret"}`)); err != nil || !result.Ok {
		t.Fatal("valid revocation acknowledgement rejected")
	}
}

type failedKeyResponseReader struct{}

func (failedKeyResponseReader) Read(buf []byte) (int, error) {
	return copy(buf, "partial-secret"), io.ErrUnexpectedEOF
}

func TestReadKeyResponseBound(t *testing.T) {
	for _, size := range []int{0, 1, wire.MaxKeyResponseBytes, wire.MaxKeyResponseBytes + 1} {
		data, err := wire.ReadKeyResponse(strings.NewReader(strings.Repeat("x", size)))
		if size > wire.MaxKeyResponseBytes {
			if err == nil || data != nil {
				t.Fatal("oversized response accepted or partially returned")
			}
		} else if err != nil || len(data) != size {
			t.Fatalf("bounded response rejected: size=%d err=%v", size, err)
		}
	}
	data, err := wire.ReadKeyResponse(failedKeyResponseReader{})
	if err == nil || data != nil || strings.Contains(err.Error(), "partial-secret") {
		t.Fatal("truncated response returned partial content")
	}
}
