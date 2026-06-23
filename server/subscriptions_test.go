package server_test

import (
	"bytes"
	"context"
	"encoding/json"
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

	post := func(path string, body, out any) {
		b, _ := json.Marshal(body)
		res, err := http.Post(ts.URL+path, "application/json", bytes.NewReader(b))
		if err != nil {
			t.Fatalf("POST %s: %v", path, err)
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("POST %s: status %d", path, res.StatusCode)
		}
		if out != nil {
			_ = json.NewDecoder(res.Body).Decode(out)
		}
	}

	var ls wire.ListSubscriptionsResponse
	post(wire.PathListSubscriptions, wire.Empty{}, &ls)
	if len(ls.Subscriptions) != 1 || ls.Subscriptions[0].Topic != "events" ||
		ls.Subscriptions[0].Name != "gold" || ls.Subscriptions[0].Expr != `properties["tier"]=="gold"` {
		t.Fatalf("ListSubscriptions = %+v", ls.Subscriptions)
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
