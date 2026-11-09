package hranaV2

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/mqlitehq/mqlite/internal/libsqlcompat/hrana"
)

func TestAcknowledgementRejectsIncompleteSuccessWithoutPanic(t *testing.T) {
	const stmt = `{"cols":[],"rows":[],"affected_row_count":0}`
	const execute = `{"type":"ok","response":{"type":"execute","result":` + stmt + `}}`
	const batch = `{"type":"ok","response":{"type":"batch","result":{"step_results":[` + stmt + `,` + stmt + `,null],"step_errors":[null,null,null]}}}`
	for _, tc := range []struct {
		name, sql, body string
		invalid, reject bool
	}{
		{"execute", "SELECT 1", `{"results":[` + execute + `]}`, false, false},
		{"forward-compatible-fields", "SELECT 1", `{"future":true,"results":[{"future":{},"type":"ok","response":{"type":"execute","result":{"cols":[],"rows":[],"affected_row_count":0,"future":[]}}}]}`, false, false},
		{"batch", "SELECT 1; SELECT 2", `{"results":[` + batch + `]}`, false, false},
		{"malformed-json", "SELECT 1", `{`, true, false},
		{"missing-results", "SELECT 1", `{}`, true, false},
		{"empty-results", "SELECT 1", `{"results":[]}`, true, false},
		{"extra-results", "SELECT 1", `{"results":[` + execute + `,` + execute + `]}`, true, false},
		{"missing-response", "SELECT 1", `{"results":[{"type":"ok"}]}`, true, false},
		{"wrong-result-type", "SELECT 1", `{"results":[{"type":"other","response":{"type":"execute","result":` + stmt + `}}]}`, true, false},
		{"wrong-response-type", "SELECT 1", `{"results":[` + batch + `]}`, true, false},
		{"error-and-response", "SELECT 1", `{"results":[{"type":"ok","error":{"message":"busy"},"response":{"type":"execute","result":` + stmt + `}}]}`, true, false},
		{"missing-statement-fields", "SELECT 1", `{"results":[{"type":"ok","response":{"type":"execute","result":{}}}]}`, true, false},
		{"null-statement", "SELECT 1", `{"results":[{"type":"ok","response":{"type":"execute","result":null}}]}`, true, false},
		{"row-width", "SELECT 1", `{"results":[{"type":"ok","response":{"type":"execute","result":{"cols":[{"name":"v"}],"rows":[[]],"affected_row_count":0}}}]}`, true, false},
		{"wrong-integer-value-type", "SELECT 1", `{"results":[{"type":"ok","response":{"type":"execute","result":{"cols":[{"name":"v"}],"rows":[[{"type":"integer","value":1}]],"affected_row_count":0}}}]}`, true, false},
		{"invalid-integer-string", "SELECT 1", `{"results":[{"type":"ok","response":{"type":"execute","result":{"cols":[{"name":"v"}],"rows":[[{"type":"integer","value":"not-an-integer"}]],"affected_row_count":0}}}]}`, true, false},
		{"overflow-integer-string", "SELECT 1", `{"results":[{"type":"ok","response":{"type":"execute","result":{"cols":[{"name":"v"}],"rows":[[{"type":"integer","value":"9223372036854775808"}]],"affected_row_count":0}}}]}`, true, false},
		{"maximum-integer", "SELECT 1", `{"results":[{"type":"ok","response":{"type":"execute","result":{"cols":[{"name":"v"}],"rows":[[{"type":"integer","value":"9223372036854775807"}]],"affected_row_count":0}}}]}`, false, false},
		{"minimum-integer", "SELECT 1", `{"results":[{"type":"ok","response":{"type":"execute","result":{"cols":[{"name":"v"}],"rows":[[{"type":"integer","value":"-9223372036854775808"}]],"affected_row_count":0}}}]}`, false, false},
		{"protocol-nonfinite-float", "SELECT 1", `{"results":[{"type":"ok","response":{"type":"execute","result":{"cols":[{"name":"v"}],"rows":[[{"type":"float","value":null}]],"affected_row_count":0}}}]}`, false, false},
		{"finite-float", "SELECT 1", `{"results":[{"type":"ok","response":{"type":"execute","result":{"cols":[{"name":"v"}],"rows":[[{"type":"float","value":1.25}]],"affected_row_count":0}}}]}`, false, false},
		{"valid-unpadded-blob", "SELECT 1", `{"results":[{"type":"ok","response":{"type":"execute","result":{"cols":[{"name":"v"}],"rows":[[{"type":"blob","base64":"aGk"}]],"affected_row_count":0}}}]}`, false, false},
		{"empty-blob", "SELECT 1", `{"results":[{"type":"ok","response":{"type":"execute","result":{"cols":[{"name":"v"}],"rows":[[{"type":"blob","base64":""}]],"affected_row_count":0}}}]}`, false, false},
		{"invalid-blob-encoding", "SELECT 1", `{"results":[{"type":"ok","response":{"type":"execute","result":{"cols":[{"name":"v"}],"rows":[[{"type":"blob","base64":"!!!"}]],"affected_row_count":0}}}]}`, true, false},
		{"missing-batch-step", "SELECT 1; SELECT 2", `{"results":[{"type":"ok","response":{"type":"batch","result":{"step_results":[` + stmt + `,null],"step_errors":[null,null]}}}]}`, true, false},
		{"unequal-batch-arrays", "SELECT 1; SELECT 2", `{"results":[{"type":"ok","response":{"type":"batch","result":{"step_results":[` + stmt + `,` + stmt + `,null],"step_errors":[null]}}}]}`, true, false},
		{"empty-batch", "SELECT 1; SELECT 2", `{"results":[{"type":"ok","response":{"type":"batch","result":{"step_results":[],"step_errors":[]}}}]}`, true, false},
		{"unacknowledged-unconditional-step", "SELECT 1; SELECT 2", `{"results":[{"type":"ok","response":{"type":"batch","result":{"step_results":[null,null,null],"step_errors":[null,null,null]}}}]}`, true, false},
		{"missing-required-conditional-step", "SELECT 1; SELECT 2", `{"results":[{"type":"ok","response":{"type":"batch","result":{"step_results":[` + stmt + `,null,null],"step_errors":[null,null,null]}}}]}`, true, false},
		{"batch-result-and-error", "SELECT 1; SELECT 2", `{"results":[{"type":"ok","response":{"type":"batch","result":{"step_results":[` + stmt + `,null,` + stmt + `],"step_errors":[{"message":"busy"},null,null]}}}]}`, true, false},
		{"definite-execute-error", "SELECT 1", `{"results":[{"type":"error","error":{"message":"database is locked"}}]}`, false, true},
		{"definite-batch-step-error", "SELECT 1; SELECT 2", `{"results":[{"type":"ok","response":{"type":"batch","result":{"step_results":[null,null,` + stmt + `],"step_errors":[{"message":"database is locked"},null,null]}}}]}`, false, true},
	} {
		for _, mode := range []string{"exec", "query"} {
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					defer r.Body.Close()
					_, _ = io.Copy(io.Discard, r.Body)
					calls.Add(1)
					_, _ = io.WriteString(w, tc.body)
				}))
				defer server.Close()
				conn := Connect(server.URL, "", "", false, "", NewCleanupManager())
				defer conn.Close()
				defer func() {
					if value := recover(); value != nil {
						t.Fatalf("acknowledgement panicked: %v", value)
					}
				}()
				var err error
				if mode == "exec" {
					_, err = conn.(driver.ExecerContext).ExecContext(context.Background(), tc.sql, nil)
				} else {
					var rows driver.Rows
					rows, err = conn.(driver.QueryerContext).QueryContext(context.Background(), tc.sql, nil)
					if rows != nil {
						_ = rows.Close()
					}
				}
				var invalid *hrana.InvalidAcknowledgementError
				if errors.As(err, &invalid) != tc.invalid || (err != nil) != (tc.invalid || tc.reject) || calls.Load() != 1 {
					t.Fatalf("acknowledgement: error=%v invalid=%v calls=%d", err, invalid != nil, calls.Load())
				}
			})
		}
	}
}

func TestAcknowledgementAllowsMatchingCloseResult(t *testing.T) {
	for _, reject := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "statement-error"}[reject], func(t *testing.T) {
			testAcknowledgementMatchingClose(t, reject)
		})
	}
}

func testAcknowledgementMatchingClose(t *testing.T, reject bool) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		_, _ = io.Copy(io.Discard, r.Body)
		first := `{"type":"ok","response":{"type":"execute","result":{"cols":[],"rows":[],"affected_row_count":0}}}`
		if reject {
			first = `{"type":"error","error":{"message":"database is locked","code":"SQLITE_BUSY"}}`
		}
		_, _ = io.WriteString(w, `{"results":[`+first+`,{"type":"ok","response":{"type":"close"}}]}`)
	}))
	defer server.Close()
	conn := Connect(server.URL, "", "", false, "", NewCleanupManager()).(*hranaV2Conn)
	defer conn.Close()
	stmt, err := hrana.ExecuteStream("SELECT 1", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	request := &hrana.PipelineRequest{}
	request.Add(*stmt)
	request.Add(hrana.CloseStream())
	response, err := conn.executeMsg(context.Background(), request)
	var invalid *hrana.InvalidAcknowledgementError
	if reject {
		if err == nil || errors.As(err, &invalid) {
			t.Fatalf("matching error/close results lost definite rejection: %v", err)
		}
	} else if err != nil || len(response.Results) != 2 {
		t.Fatalf("matching execute/close results: %v", err)
	}
}
