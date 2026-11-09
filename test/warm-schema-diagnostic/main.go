// A single read-only catalog request; no engine Open or schema writes occur.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

//go:embed canonical.json main.go normalize_generated.go
var sources embed.FS

const canonicalRuntime = "a2f69d3b5698f07f5b9436e01a876ac70df5fcd5"

var approved = map[string]string{"standard": "b72bfdad8f3e7696b3d216a2e947705b8efbd6c183e77f44d2cdcb16cef544b3", "rust": "3f0ad22a38621cd9fd112e1e249d97d093ab53b407eba7b897ccfe7c0e3ffe17"}

type object struct {
	Kind  string `json:"kind"`
	Name  string `json:"name"`
	Table string `json:"table"`
	SQL   string `json:"sql"`
}
type difference struct {
	Index    int    `json:"index"`
	Expected string `json:"expected"`
	Actual   string `json:"actual_filtered"`
}
type objectResult struct {
	Name           string       `json:"name"`
	Found          bool         `json:"found"`
	KindMatches    bool         `json:"kind_matches"`
	TableMatches   bool         `json:"table_matches"`
	SQLPresent     bool         `json:"sql_present"`
	Matches        bool         `json:"matches_current_warm_rule"`
	ActualBytes    int          `json:"actual_bytes"`
	ActualSHA      string       `json:"actual_sha256,omitempty"`
	NormalizedSHA  string       `json:"normalized_sha256,omitempty"`
	ExpectedSHA    string       `json:"expected_normalized_sha256"`
	ExpectedTokens int          `json:"expected_tokens"`
	ActualTokens   int          `json:"actual_tokens"`
	Differences    []difference `json:"differences"`
}
type report struct {
	Scope         string            `json:"scope"`
	Backend       string            `json:"backend"`
	Runtime       string            `json:"canonical_runtime_sha"`
	ProgramHashes map[string]string `json:"program_sha256"`
	ReadOnly      bool              `json:"read_only"`
	Requests      int               `json:"http_requests"`
	SQLStatements int               `json:"sql_statements"`
	Status        int               `json:"http_status"`
	BodyComplete  bool              `json:"body_complete"`
	BodyClosed    bool              `json:"body_closed"`
	ElapsedMS     int64             `json:"elapsed_ms"`
	Failure       string            `json:"failure,omitempty"`
	Complete      bool              `json:"observation_complete"`
	AllMatch      bool              `json:"all_match_current_warm_rule"`
	Objects       []objectResult    `json:"objects"`
}

func sha(data []byte) string { x := sha256.Sum256(data); return hex.EncodeToString(x[:]) }
func inventory() ([]object, error) {
	data, e := sources.ReadFile("canonical.json")
	if e != nil {
		return nil, e
	}
	var out []object
	e = json.Unmarshal(data, &out)
	if e != nil || len(out) != 29 {
		return nil, fmt.Errorf("canonical inventory invalid")
	}
	return out, nil
}
func vocabulary(objects []object) map[string]bool {
	v := map[string]bool{}
	for _, o := range objects {
		for _, token := range schemaTokens(o.SQL) {
			v[token] = true
		}
	}
	return v
}
func safeToken(token string, known map[string]bool) string {
	if known[token] {
		return token
	}
	if len(token) >= 2 {
		closing := token[len(token)-1]
		first := token[0]
		if (first == '"' && closing == '"') || (first == '`' && closing == '`') || (first == '[' && closing == ']') {
			inside := strings.ToUpper(token[1 : len(token)-1])
			if known[inside] && !strings.ContainsAny(inside, "'\"`[] \t\r\n") {
				return "quoted_identifier(" + inside + ")"
			}
		}
	}
	return "redacted_sha256:" + sha([]byte(token))[:12]
}
func compare(objects []object, actual map[string]object) ([]objectResult, bool) {
	known := vocabulary(objects)
	out := make([]objectResult, 0, len(objects))
	all := true
	for _, expected := range objects {
		at, found := actual[expected.Name]
		eTokens := schemaTokens(expected.SQL)
		r := objectResult{Name: expected.Name, Found: found, ExpectedSHA: sha([]byte(normalizedSchemaSQL(expected.SQL))), ExpectedTokens: len(eTokens), Differences: []difference{}}
		if found {
			r.KindMatches = at.Kind == expected.Kind
			r.TableMatches = strings.EqualFold(at.Table, expected.Table)
			r.SQLPresent = at.SQL != ""
			r.ActualBytes = len(at.SQL)
			r.ActualSHA = sha([]byte(at.SQL))
			aTokens := schemaTokens(at.SQL)
			r.ActualTokens = len(aTokens)
			r.NormalizedSHA = sha([]byte(normalizedSchemaSQL(at.SQL)))
			equal := normalizedSchemaSQL(at.SQL) == normalizedSchemaSQL(expected.SQL)
			if expected.Name == "trg_queues_messages_delete" || expected.Name == "trg_queues_messages_rename" {
				equal = matchesParentGuardDefinition(at.SQL, expected.SQL)
			}
			r.Matches = r.KindMatches && r.TableMatches && r.SQLPresent && equal
			n := len(eTokens)
			if len(aTokens) > n {
				n = len(aTokens)
			}
			for i := 0; i < n && len(r.Differences) < 8; i++ {
				et, at := "<end>", "<end>"
				if i < len(eTokens) {
					et = eTokens[i]
				}
				if i < len(aTokens) {
					at = aTokens[i]
				}
				if et != at {
					r.Differences = append(r.Differences, difference{Index: i, Expected: et, Actual: safeToken(at, known)})
				}
			}
		}
		all = all && r.Matches
		out = append(out, r)
	}
	return out, all
}
func requestBody(objects []object) []byte {
	marks := make([]string, len(objects))
	args := make([]map[string]string, len(objects))
	for i, o := range objects {
		marks[i] = "?"
		args[i] = map[string]string{"type": "text", "value": o.Name}
	}
	sql := "SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name COLLATE NOCASE IN (" + strings.Join(marks, ",") + ") ORDER BY name"
	body, _ := json.Marshal(map[string]any{"requests": []any{map[string]any{"type": "execute", "stmt": map[string]any{"sql": sql, "args": args, "want_rows": true}}, map[string]string{"type": "close"}}})
	return body
}

type value struct {
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
}

func decode(data []byte, objects []object) (map[string]object, error) {
	var reply struct {
		Results []struct {
			Type     string `json:"type"`
			Response struct {
				Type   string `json:"type"`
				Result *struct {
					Cols []struct {
						Name string `json:"name"`
					} `json:"cols"`
					Rows [][]value `json:"rows"`
				} `json:"result"`
			} `json:"response"`
		} `json:"results"`
	}
	if json.Unmarshal(data, &reply) != nil || len(reply.Results) != 2 || reply.Results[0].Type != "ok" || reply.Results[0].Response.Type != "execute" || reply.Results[1].Type != "ok" || reply.Results[1].Response.Type != "close" {
		return nil, fmt.Errorf("invalid protocol")
	}
	rows := reply.Results[0].Response.Result
	if rows == nil || len(rows.Cols) != 4 || len(rows.Rows) > 29 {
		return nil, fmt.Errorf("invalid rows")
	}
	for i, n := range []string{"type", "name", "tbl_name", "sql"} {
		if !strings.EqualFold(rows.Cols[i].Name, n) {
			return nil, fmt.Errorf("invalid columns")
		}
	}
	allowed := map[string]bool{}
	for _, o := range objects {
		allowed[o.Name] = true
	}
	out := map[string]object{}
	for _, row := range rows.Rows {
		if len(row) != 4 {
			return nil, fmt.Errorf("invalid row")
		}
		fields := make([]string, 4)
		for i, v := range row {
			if i == 3 && v.Type == "null" {
				continue
			}
			if v.Type != "text" || json.Unmarshal(v.Value, &fields[i]) != nil {
				return nil, fmt.Errorf("invalid value")
			}
		}
		name := strings.ToLower(fields[1])
		if !allowed[name] {
			return nil, fmt.Errorf("unowned result")
		}
		if _, duplicate := out[name]; duplicate {
			return nil, fmt.Errorf("duplicate result")
		}
		if len(fields[3]) > 64*1024 {
			return nil, fmt.Errorf("oversized definition")
		}
		out[name] = object{Kind: fields[0], Name: name, Table: fields[2], SQL: fields[3]}
	}
	return out, nil
}
func fetch(ctx context.Context, client *http.Client, endpoint, token string, objects []object, r *report) {
	req, e := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(requestBody(objects)))
	if e != nil {
		r.Failure = "request_setup"
		return
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	r.Requests = 1
	r.SQLStatements = 1
	resp, e := client.Do(req)
	if e != nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		r.Failure = "transport_or_deadline"
		return
	}
	r.Status = resp.StatusCode
	data, e := io.ReadAll(io.LimitReader(resp.Body, 1024*1024+1))
	closed := resp.Body.Close()
	r.BodyComplete = e == nil && len(data) <= 1024*1024
	r.BodyClosed = closed == nil
	if !r.BodyComplete || !r.BodyClosed {
		r.Failure = "incomplete_response"
		return
	}
	if resp.StatusCode != 200 {
		r.Failure = "http_error"
		return
	}
	actual, e := decode(data, objects)
	if e != nil {
		r.Failure = "protocol_or_catalog_shape"
		return
	}
	r.Objects, r.AllMatch = compare(objects, actual)
	r.Complete = true
}
func main() { os.Exit(run()) }
func run() int {
	backend := flag.String("backend", "", "standard or rust")
	out := flag.String("out", "", "fresh output directory")
	flag.Parse()
	objects, e := inventory()
	if e != nil || approved[*backend] == "" || *out == "" {
		fmt.Println("catalog diagnostic setup failed")
		return 2
	}
	prefix := "MQLITE_DIAG_" + strings.ToUpper(*backend)
	dsn, token := os.Getenv(prefix+"_DB"), os.Getenv(prefix+"_TOKEN")
	if sha([]byte(dsn)) != approved[*backend] || token == "" {
		fmt.Println("dedicated configuration required")
		return 2
	}
	u, e := url.Parse(dsn)
	if e != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" {
		fmt.Println("invalid dedicated configuration")
		return 2
	}
	u.Scheme = "https"
	u.Path = "/v2/pipeline"
	if e = os.Mkdir(*out, 0700); e != nil {
		fmt.Println("fresh output directory required")
		return 2
	}
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		fmt.Println("default transport unavailable")
		return 2
	}
	transport := base.Clone()
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	r := report{Scope: "single-read-only-owned-catalog-diff", Backend: *backend, Runtime: canonicalRuntime, ReadOnly: true, ProgramHashes: map[string]string{}, Objects: []objectResult{}}
	names := []string{"canonical.json", "main.go", "normalize_generated.go"}
	sort.Strings(names)
	for _, name := range names {
		data, _ := sources.ReadFile(name)
		r.ProgramHashes[name] = sha(data)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	start := time.Now()
	fetch(ctx, client, u.String(), token, objects, &r)
	r.ElapsedMS = time.Since(start).Milliseconds()
	cancel()
	data, _ := json.MarshalIndent(r, "", "  ")
	if os.WriteFile(filepath.Join(*out, "catalog-diff.json"), append(data, '\n'), 0600) != nil {
		fmt.Println("cannot write catalog diagnostic")
		return 2
	}
	fmt.Printf("catalog observation complete=%t all_match=%t\n", r.Complete, r.AllMatch)
	if !r.Complete {
		return 1
	}
	return 0
}
