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

func TestOnlyDefiniteSingleStatementBusyCarriesSQLiteCode(t *testing.T) {
	for _, name := range []string{"valid", "message-only", "other-code", "wrong-type", "mixed-result", "extra-request", "extra-result", "batch"} {
		t.Run(name, func(t *testing.T) {
			code := "SQLITE_BUSY"
			request := &hrana.PipelineRequest{Requests: []hrana.StreamRequest{{Type: "execute", Stmt: &hrana.Stmt{}}}}
			failure := hrana.StreamResult{Type: "error", Error: &hrana.Error{Code: &code, Message: "database is locked"}}
			reply := &hrana.PipelineResponse{Results: []hrana.StreamResult{failure}}
			switch name {
			case "message-only":
				failure.Error.Code = nil
			case "other-code":
				code = "SQLITE_ERROR"
			case "wrong-type":
				failure.Type = "ok"
			case "mixed-result":
				failure.Response = &hrana.StreamResponse{Type: "execute"}
			case "extra-request":
				request.Requests = append(request.Requests, request.Requests[0])
			case "extra-result":
				reply.Results = append(reply.Results, failure)
			case "batch":
				request.Requests[0].Type = "batch"
			}
			err := providerExecutionError(request, reply, failure)
			var coded interface{ Code() int }
			got := errors.As(err, &coded)
			if got != (name == "valid") || got && coded.Code() != 5 {
				t.Fatalf("unexpected definite Busy evidence: %s, %T, %v", name, err, err)
			}
			if err.Error() != "database is locked" || HTTPStatus(err) != 0 {
				t.Fatalf("message/status changed: %v", err)
			}
		})
	}
}
