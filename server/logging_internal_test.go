package server

import (
	"context"
	"fmt"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/mqlitehq/mqlite/engine"
)

func TestLogDeleteAcceptance(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want string
	}{
		{name: "application rejection", err: engine.ErrQueueNotEmpty, want: "rejected"},
		{name: "canceled transaction", err: context.Canceled, want: "rejected"},
		{name: "unknown commit", err: engine.ErrOutcomeUnknown, want: "unknown"},
		{name: "wrapped unknown commit", err: fmt.Errorf("delete: %w", engine.ErrOutcomeUnknown), want: "unknown"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rec := &statusRecorder{ResponseWriter: httptest.NewRecorder(), logging: true}
			logDeleteAcceptance(rec, tt.err)
			if want := []any{"acceptance", tt.want}; !reflect.DeepEqual(rec.kv, want) {
				t.Fatalf("fields = %#v, want %#v", rec.kv, want)
			}
		})
	}
}
