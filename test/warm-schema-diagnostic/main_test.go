package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func actualFromCanonical(t *testing.T) ([]object, map[string]object) {
	t.Helper()
	objects, err := inventory()
	if err != nil {
		t.Fatal(err)
	}
	actual := map[string]object{}
	for _, o := range objects {
		actual[o.Name] = o
	}
	return objects, actual
}
func TestHistoricalRustMetaQuotedKeyIsVisible(t *testing.T) {
	objects, actual := actualFromCanonical(t)
	o := actual["meta"]
	o.SQL = `CREATE TABLE meta ("key" TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT`
	actual["meta"] = o
	results, all := compare(objects, actual)
	if all {
		t.Fatal("historical quoted key silently matched")
	}
	mismatches := 0
	for _, r := range results {
		if !r.Matches {
			mismatches++
			if r.Name != "meta" || len(r.Differences) != 1 || r.Differences[0].Expected != "KEY" || r.Differences[0].Actual != "quoted_identifier(KEY)" {
				t.Fatalf("unexpected diff: %+v", r)
			}
		}
	}
	if mismatches != 1 {
		t.Fatalf("mismatches=%d", mismatches)
	}
}
func TestUnknownCatalogTokensAreNeverPublished(t *testing.T) {
	objects, actual := actualFromCanonical(t)
	private := "private-token-customer-unique"
	o := actual["meta"]
	o.SQL = `CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL DEFAULT '` + private + ` https://private.example/SQL-secret') STRICT`
	o.Kind = "private-kind"
	o.Table = "private-table"
	actual["meta"] = o
	results, _ := compare(objects, actual)
	data, _ := json.Marshal(results)
	for _, secret := range []string{private, "private.example", "SQL-secret", "private-kind", "private-table"} {
		if bytes.Contains(data, []byte(secret)) {
			t.Fatalf("secret was published: %q", secret)
		}
	}
}
func TestCanonicalInventoryMatchesAndQueryHasOnlyOwnedNames(t *testing.T) {
	objects, actual := actualFromCanonical(t)
	_, all := compare(objects, actual)
	if !all {
		t.Fatal("canonical self comparison failed")
	}
	var in struct {
		Requests []struct {
			Type string `json:"type"`
			Stmt struct {
				SQL  string                         `json:"sql"`
				Args []struct{ Type, Value string } `json:"args"`
			} `json:"stmt"`
		} `json:"requests"`
	}
	if json.Unmarshal(requestBody(objects), &in) != nil || len(in.Requests) != 2 || in.Requests[0].Type != "execute" || in.Requests[1].Type != "close" {
		t.Fatal("invalid bounded pipeline")
	}
	q := in.Requests[0].Stmt
	if !strings.HasPrefix(q.SQL, "SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name COLLATE NOCASE IN (") || len(q.Args) != 29 {
		t.Fatal("query escaped catalog scope")
	}
	seen := map[string]bool{}
	for _, a := range q.Args {
		if a.Type != "text" || actual[a.Value].Name != a.Value || seen[a.Value] {
			t.Fatal("unexpected query arg")
		}
		seen[a.Value] = true
	}
}
func response(objects []object) []byte {
	rows := make([][]map[string]string, 0, len(objects))
	for _, o := range objects {
		r := []map[string]string{}
		for _, v := range []string{o.Kind, o.Name, o.Table, o.SQL} {
			r = append(r, map[string]string{"type": "text", "value": v})
		}
		rows = append(rows, r)
	}
	cols := []map[string]string{}
	for _, v := range []string{"type", "name", "tbl_name", "sql"} {
		cols = append(cols, map[string]string{"name": v})
	}
	data, _ := json.Marshal(map[string]any{"results": []any{map[string]any{"type": "ok", "response": map[string]any{"type": "execute", "result": map[string]any{"cols": cols, "rows": rows}}}, map[string]any{"type": "ok", "response": map[string]any{"type": "close"}}}})
	return data
}
func TestSingleReadOnlyHTTPAndStrictProtocol(t *testing.T) {
	objects, _ := actualFromCanonical(t)
	for _, bad := range []bool{false, true} {
		name := "valid"
		if bad {
			name = "malformed"
		}
		t.Run(name, func(t *testing.T) {
			calls := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				data, _ := io.ReadAll(r.Body)
				if string(data) != string(requestBody(objects)) {
					t.Error("request changed scope")
				}
				if bad {
					_, _ = io.WriteString(w, `{"results":[{"type":"error","error":{"message":"private-secret"}}]}`)
				} else {
					_, _ = w.Write(response(objects))
				}
			}))
			defer srv.Close()
			r := report{}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			fetch(ctx, srv.Client(), srv.URL, "fixture-token", objects, &r)
			if calls != 1 || r.Complete == bad || !r.BodyComplete || !r.BodyClosed {
				t.Fatalf("unexpected capture: %+v calls=%d", r, calls)
			}
			encoded, _ := json.Marshal(r)
			if bytes.Contains(encoded, []byte("private-secret")) {
				t.Fatal("raw error escaped")
			}
		})
	}
}

func TestOversizedResponseDoesNotEscapeBound(t *testing.T) {
	objects, _ := actualFromCanonical(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, strings.Repeat("x", 1024*1024+32))
	}))
	defer srv.Close()
	r := report{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	fetch(ctx, srv.Client(), srv.URL, "fixture-token", objects, &r)
	if r.Complete || r.Failure != "incomplete_response" || r.BodyComplete || !r.BodyClosed || r.Requests != 1 {
		t.Fatalf("unbounded or incomplete response was accepted: %+v", r)
	}
}
