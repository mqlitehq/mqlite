package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const good = `{"results":[{"type":"ok","response":{"type":"execute","result":{"rows":[[{"type":"integer","value":"1"}]]}}},{"type":"ok","response":{"type":"close"}}]}`

func TestBaselineOnlySendsReadOnlyStatementAndClose(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var d struct {
			Requests []struct {
				Type string
				Stmt struct{ SQL string }
			}
		}
		if json.Unmarshal(body, &d) != nil || len(d.Requests) != 2 || d.Requests[0].Stmt.SQL != "SELECT 1" || d.Requests[1].Type != "close" {
			t.Error("unsafe control statement")
			w.WriteHeader(400)
			return
		}
		if r.Header.Get("Authorization") != "Bearer sentinel-token" {
			t.Error("missing auth")
		}
		_, _ = io.WriteString(w, good)
	}))
	defer server.Close()
	client := server.Client()
	for i := 0; i < 3; i++ {
		s := request(context.Background(), client, server.URL, "sentinel-token", i)
		if !s.ResultOne || s.Class != "ok" || (i > 0 && !s.Reused) {
			t.Fatalf("unexpected control result: %+v", s)
		}
		out, _ := json.Marshal(s)
		if strings.Contains(string(out), "sentinel") || strings.Contains(string(out), server.URL) {
			t.Fatal("sensitive control values leaked")
		}
	}
}

func TestBaselineRejectsNonResultsAndBoundsBodies(t *testing.T) {
	for _, item := range []struct {
		status      int
		body, class string
	}{
		{401, `{"message":"sentinel-token"}`, "http_error"},
		{200, `{"results":[{"type":"error","error":{"message":"sentinel-token"}}]}`, "protocol_or_sql_error"},
		{200, good + strings.Repeat(" ", 1<<20), "response_limit"},
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(item.status)
			_, _ = io.WriteString(w, item.body)
		}))
		s := request(context.Background(), server.Client(), server.URL, "secret", 0)
		server.Close()
		if s.ResultOne || s.Class != item.class {
			t.Fatalf("wrong class: %+v", s)
		}
		data, _ := json.Marshal(s)
		if strings.Contains(string(data), "sentinel") {
			t.Fatal("body leaked")
		}
	}
}

func TestBaselineTimeoutIsClassifiedWithoutURL(t *testing.T) {
	stop := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-stop:
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	s := request(ctx, server.Client(), server.URL, "secret", 0)
	close(stop)
	if s.ResultOne || s.Class != "deadline" {
		t.Fatalf("unexpected timeout: %+v", s)
	}
	data, _ := json.Marshal(s)
	if strings.Contains(string(data), server.URL) {
		t.Fatal("URL leaked")
	}
}
