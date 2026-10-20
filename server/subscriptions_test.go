package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/server"
	"github.com/mqlitehq/mqlite/wire"
)

// The console's two endpoints over HTTP: ListSubscriptions exposes topic+filter (which
// ListQueues does not), and TestFilter dry-runs an expression.
func TestListSubscriptionsAndTestFilter(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer eng.Close()
	if err := eng.Subscribe(ctx, "events", "gold", &engine.Filter{Expr: `properties["tier"]=="gold"`}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	ts := httptest.NewServer(server.New(eng, nil).Handler()) // no auth
	defer ts.Close()

	post := func(path string, body, out any) []byte {
		t.Helper()
		b, _ := json.Marshal(body)
		res, err := http.Post(ts.URL+path, "application/json", bytes.NewReader(b))
		if err != nil {
			t.Fatalf("POST %s: %v", path, err)
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("POST %s: status %d", path, res.StatusCode)
		}
		response, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatalf("read POST %s: %v", path, err)
		}
		if out != nil {
			if err := json.Unmarshal(response, out); err != nil {
				t.Fatalf("decode POST %s: %v", path, err)
			}
		}
		return response
	}

	var ls wire.ListSubscriptionsResponse
	legacyBody := post(wire.PathListSubscriptions, wire.Empty{}, &ls)
	if len(ls.Subscriptions) != 1 || ls.Subscriptions[0].Topic != "events" ||
		ls.Subscriptions[0].Name != "gold" || ls.Subscriptions[0].Expr != `properties["tier"]=="gold"` ||
		ls.Subscriptions[0].State != "" {
		t.Fatalf("ListSubscriptions = %+v", ls.Subscriptions)
	}
	var legacyWire struct {
		Subscriptions []map[string]json.RawMessage `json:"subscriptions"`
	}
	if err := json.Unmarshal(legacyBody, &legacyWire); err != nil || len(legacyWire.Subscriptions) != 1 {
		t.Fatalf("legacy wire response = %s: %v", legacyBody, err)
	}
	if _, present := legacyWire.Subscriptions[0]["state"]; present {
		t.Fatalf("legacy wire response added state: %s", legacyBody)
	}
	var page wire.ListSubscriptionsPageResponse
	post(wire.PathListSubscriptionsPage, wire.ListSubscriptionsPageRequest{Topic: "events", Limit: 1}, &page)
	if page.Total != 1 || page.NextAfter != "" || len(page.Subscriptions) != 1 ||
		page.Subscriptions[0].Topic != ls.Subscriptions[0].Topic ||
		page.Subscriptions[0].Name != ls.Subscriptions[0].Name ||
		page.Subscriptions[0].Expr != ls.Subscriptions[0].Expr ||
		page.Subscriptions[0].State != "active" {
		t.Fatalf("ListSubscriptionsPage = %+v", page)
	}

	// valid expr + a matching sample.
	var tfMatch wire.TestFilterResponse
	post(wire.PathTestFilter, wire.TestFilterRequest{
		Expr:    `properties["tier"]=="gold"`,
		Message: &wire.Message{Properties: map[string]string{"tier": "gold"}},
	}, &tfMatch)
	if !tfMatch.Valid || !tfMatch.Ran || !tfMatch.Matched || tfMatch.Error != "" {
		t.Fatalf("TestFilter match = %+v", tfMatch)
	}

	// a non-matching sample.
	var tfNo wire.TestFilterResponse
	post(wire.PathTestFilter, wire.TestFilterRequest{
		Expr:    `properties["tier"]=="gold"`,
		Message: &wire.Message{Properties: map[string]string{"tier": "silver"}},
	}, &tfNo)
	if !tfNo.Ran || tfNo.Matched {
		t.Fatalf("TestFilter no-match = %+v", tfNo)
	}

	// an invalid expression compiles to an error (not a 400 — the dry run reports it).
	var tfBad wire.TestFilterResponse
	post(wire.PathTestFilter, wire.TestFilterRequest{Expr: `subject ==`}, &tfBad)
	if tfBad.Valid || tfBad.Error == "" {
		t.Fatalf("TestFilter invalid = %+v", tfBad)
	}
}

func TestSubscriptionLifecycleRoutes(t *testing.T) {
	ctx := context.Background()
	eng, err := engine.Open(ctx, engine.Options{DB: ":memory:", DisableBackground: true, MaxSubscriptions: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	for _, name := range []string{"a", "b"} {
		if err := eng.Subscribe(ctx, "events", name, nil); err != nil {
			t.Fatal(err)
		}
	}
	h := server.New(eng, nil).Handler()
	call := func(path string, body any) *httptest.ResponseRecorder {
		t.Helper()
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, bytes.NewReader(data)))
		return rec
	}
	errorCode := func(rec *httptest.ResponseRecorder) string {
		t.Helper()
		var body wire.ErrorBody
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		return body.Code
	}

	first := call(wire.PathListSubscriptionsPage, wire.ListSubscriptionsPageRequest{Topic: "events", Limit: 1})
	if first.Code != http.StatusOK {
		t.Fatalf("first page status = %d: %s", first.Code, first.Body.String())
	}
	var p1 wire.ListSubscriptionsPageResponse
	if err := json.Unmarshal(first.Body.Bytes(), &p1); err != nil {
		t.Fatal(err)
	}
	if p1.Total != 2 || len(p1.Subscriptions) != 1 || p1.Subscriptions[0].State != "active" || p1.NextAfter == "" {
		t.Fatalf("first page = %+v", p1)
	}
	second := call(wire.PathListSubscriptionsPage, wire.ListSubscriptionsPageRequest{Topic: "events", Limit: 1, After: p1.NextAfter})
	var p2 wire.ListSubscriptionsPageResponse
	if second.Code != http.StatusOK || json.Unmarshal(second.Body.Bytes(), &p2) != nil || p2.Total != 2 || len(p2.Subscriptions) != 1 || p2.NextAfter != "" {
		t.Fatalf("second page = status %d, %+v", second.Code, p2)
	}

	limit := call(wire.PathSubscribe, wire.SubscribeRequest{Topic: "events", Name: "c"})
	if limit.Code != http.StatusTooManyRequests || errorCode(limit) != "resource_exhausted" {
		t.Fatalf("subscription limit = %d %s", limit.Code, limit.Body.String())
	}
	if _, err := eng.Send(ctx, "events", engine.OutMessage{Body: []byte("retained")}); err != nil {
		t.Fatal(err)
	}
	notEmpty := call(wire.PathDeleteSubscription, wire.DeleteSubscriptionRequest{Topic: "events", Name: "a"})
	if notEmpty.Code != http.StatusConflict || errorCode(notEmpty) != "subscription_not_empty" {
		t.Fatalf("nonempty delete = %d %s", notEmpty.Code, notEmpty.Body.String())
	}
	missing := call(wire.PathDeleteSubscription, wire.DeleteSubscriptionRequest{Topic: "wrong", Name: "a", Force: true})
	if missing.Code != http.StatusNotFound || errorCode(missing) != "not_found" {
		t.Fatalf("wrong-topic delete = %d %s", missing.Code, missing.Body.String())
	}
	deleted := call(wire.PathDeleteSubscription, wire.DeleteSubscriptionRequest{Topic: "events", Name: "a", Force: true})
	ack, decodeErr := wire.DecodeDeleteResponse(deleted.Body.Bytes())
	if deleted.Code != http.StatusOK || decodeErr != nil || !ack.Deleted || ack.State != "deleted" {
		t.Fatalf("force delete = %d %s: %v", deleted.Code, deleted.Body.String(), decodeErr)
	}
}
