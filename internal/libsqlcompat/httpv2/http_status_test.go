package hranaV2

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mqlitehq/mqlite/internal/libsqlcompat/hrana"
)

func TestProviderHTTPStatusSurvivesBodyFallbacks(t *testing.T) {
	for _, body := range []string{
		`{"error":"database is locked"}`,
		`{"error":{},"message":"gateway overload"}`,
		`{"error":{},"code":"STREAM_EXPIRED","message":"expired"}`,
		`not-json`,
	} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = io.WriteString(w, body)
			}))
			defer server.Close()
			_, _, err := sendPipelineRequest(context.Background(), &hrana.PipelineRequest{}, server.URL, "", "", "")
			if err == nil || HTTPStatus(fmt.Errorf("operation: %w", err)) != 503 {
				t.Fatalf("status=%d err=%v", HTTPStatus(err), err)
			}
			if HTTPStatus(errors.Join(err, io.EOF)) != 0 {
				t.Fatal("joined causes must not be interpreted as one provider response")
			}
		})
	}

	t.Run("body-read-error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", "100")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "short")
		}))
		defer server.Close()
		_, _, err := sendPipelineRequest(context.Background(), &hrana.PipelineRequest{}, server.URL, "", "", "")
		if HTTPStatus(err) != 503 || !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("body-read cause/status lost: %v, status=%d", err, HTTPStatus(err))
		}
	})

	for _, err := range []error{nil, errors.New("error code 503: body text"), driver.ErrBadConn, context.Canceled} {
		if HTTPStatus(err) != 0 {
			t.Fatalf("invented status for %v", err)
		}
	}
}
