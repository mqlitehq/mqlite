package server

// White-box (package server) so it can call the unexported fail() directly. The full
// auth + error-envelope surface is covered black-box in errors_test.go; this pins the
// one mapping that can't be induced through a real handler hermetically.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/wire"
)

// A remote write whose commit ack was lost surfaces as engine.ErrOutcomeUnknown. It
// MUST map to a distinct "outcome_unknown" code (503, not a generic 500 a caller
// blindly retries) so the SDK reconstructs the typed sentinel and reconciles by
// message_id/dedup instead of double-applying (MQLITE-59). The client half of this
// round-trip lives in TestRemoteOutcomeUnknownPropagates (sdk_test.go).
func TestFailOutcomeUnknown(t *testing.T) {
	rec := httptest.NewRecorder()
	(&Server{}).fail(rec, fmt.Errorf("send: %w", engine.ErrOutcomeUnknown))

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
	var e wire.ErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil {
		t.Fatalf("decode body %q: %v", rec.Body.String(), err)
	}
	if e.Code != "outcome_unknown" {
		t.Errorf("code = %q, want outcome_unknown", e.Code)
	}
}
