package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/mqlitehq/mqlite/wire"
)

// The full fixture pins every native field and every Prometheus family, type,
// label, bucket, unit conversion and compatibility projection. Review both files
// together when intentionally changing the canonical observation contract.
func TestObservabilityFullContract(t *testing.T) {
	data, err := os.ReadFile("testdata/observability.json")
	if err != nil {
		t.Fatal(err)
	}
	s, err := wire.DecodeObserveResponse(data)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var fixture, roundTrip any
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &roundTrip); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fixture, roundTrip) {
		t.Fatal("canonical JSON field inventory drift")
	}
	want, err := os.ReadFile("testdata/observability.prom")
	if err != nil {
		t.Fatal(err)
	}
	got := prometheusSnapshot(s)
	if got != string(want) {
		actual, expected := strings.Split(got, "\n"), strings.Split(string(want), "\n")
		for i := 0; i < len(actual) && i < len(expected); i++ {
			if actual[i] != expected[i] {
				t.Fatalf("Prometheus contract drift at line %d; review JSON + exposition together:\ngot  %q\nwant %q", i+1, actual[i], expected[i])
			}
		}
		t.Fatalf("Prometheus contract line count changed: got %d, want %d", len(actual), len(expected))
	}
	// The reverse availability edge must omit failed measurements, while keeping
	// the real in-memory event facts and explicit failure indicators.
	s.Collection.State = "unavailable"
	s.Collection.ErrorCode = "closed"
	s.Queues = nil
	s.Runtime.ReadAvailable = false
	s.Runtime.DBSizeAvailable = false
	failed := prometheusSnapshot(s)
	for _, line := range strings.Split(failed, "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		for _, prefix := range []string{"mqlite_queue_", "mqlite_entities{", "mqlite_storage_ping_seconds ", "mqlite_storage_size_bytes "} {
			if strings.HasPrefix(line, prefix) {
				t.Fatalf("failed collection exported healthy-looking gauge: %s", line)
			}
		}
	}
	if !strings.Contains(failed, "mqlite_collection_success 0\n") || !strings.Contains(failed, "mqlite_message_events_total{") {
		t.Fatal("partial availability lost real process facts")
	}
}

func TestRequestOutcomeVocabulary(t *testing.T) {
	want := []string{"ok", "unauthenticated", "permission_denied", "key_conflict", "not_found", "already_exists", "name_conflict", "group_required", "invalid_argument", "message_too_large", "outcome_unknown", "canceled", "internal", "lock_lost", "unimplemented"}
	if !reflect.DeepEqual(requestCodes[:], want) {
		t.Fatal("complete RPC outcome vocabulary drift")
	}
	for _, code := range want[1:] {
		if got := requestCode(&statusRecorder{status: 400, code: code}); got != code {
			t.Errorf("bounded code %s lost as %s", code, got)
		}
	}
	if requestCode(&statusRecorder{status: 500, code: "private-error-text"}) != "internal" {
		t.Fatal("unbounded error label")
	}
	if requestCode(&statusRecorder{status: 200}) != "ok" {
		t.Fatal("successful requests must be ok")
	}
}

func TestFirstAuthenticationFailuresHaveZeroBaselines(t *testing.T) {
	s := New(nil, []string{"configured-admin"})
	registered := make([]string, 0, len(s.rpcPaths))
	for _, path := range s.rpcPaths {
		registered = append(registered, strings.TrimPrefix(path, "/mqlite.v1."))
	}
	if !reflect.DeepEqual(registered, wire.ObservationRPCNames()) {
		t.Fatal("complete RPC observation vocabulary differs from registered routes")
	}
	before := s.httpSnapshot()
	if len(before.Requests) != len(s.rpcPaths)*len(requestCodes) {
		t.Fatal("every registered RPC outcome must exist before the first request")
	}
	baseline := make(map[requestKey]wire.RequestObservation)
	for _, row := range before.Requests {
		key := requestKey{row.RPC, row.Code}
		if _, exists := baseline[key]; exists || row.Count != 0 || row.DurationSeconds != 0 {
			t.Fatal("initial request measurements must be unique zero baselines")
		}
		baseline[key] = row
	}
	handler := s.Handler()
	for _, path := range s.rpcPaths {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`)))
		if response.Code != http.StatusUnauthorized {
			t.Fatalf("%s: missing authentication returned %d", path, response.Code)
		}
	}
	for _, row := range s.httpSnapshot().Requests {
		old, exists := baseline[requestKey{row.RPC, row.Code}]
		var want uint64
		if row.Code == "unauthenticated" {
			want = 1
		}
		// A request shorter than the platform clock resolution can take zero
		// measured time. Preserve that fact while requiring exact counter deltas.
		if !exists || row.Count-old.Count != want || row.DurationSeconds < old.DurationSeconds || (want == 0 && row.DurationSeconds != 0) {
			t.Fatalf("first request outcome cannot be measured from baseline: %+v", row)
		}
	}
}
