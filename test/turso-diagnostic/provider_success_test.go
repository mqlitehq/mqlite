//go:build mqlite_perf_diag

package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

// Shapes match pinned Hrana StmtResult/BatchResult and the SQLite fixture;
// only the conditional trailing ROLLBACK is skipped when the batch succeeds.
const strictStatementFixture = `{"cols":[{"name":"seq_number","decltype":null}],"rows":[[{"type":"integer","value":"1"}]],"affected_row_count":1,"last_insert_rowid":"1"}`
const strictExecuteRequest = `{"requests":[{"type":"execute","stmt":{"sql":"INSERT INTO messages(body) VALUES (?) RETURNING seq_number","args":[{"type":"text","value":"private-argument-canary"}],"want_rows":true}}]}`
const strictBatchRequest = `{"requests":[{"type":"batch","batch":{"steps":[{"stmt":{"sql":"INSERT INTO messages(body) VALUES (?) RETURNING seq_number"}},{"stmt":{"sql":"INSERT INTO messages(body) VALUES (?) RETURNING seq_number"},"condition":{"type":"ok","step":0}},{"stmt":{"sql":"ROLLBACK"},"condition":{"type":"not","cond":{"type":"ok","step":1}}}]}}]}`

func strictExecuteEnvelope(result string) string {
	return `{"baton":null,"results":[{"type":"ok","response":{"type":"execute","result":` + result + `}}]}`
}
func strictBatchEnvelope(results, errs string) string {
	return `{"baton":"private-baton-canary","results":[{"type":"ok","response":{"type":"batch","result":{"step_results":` + results + `,"step_errors":` + errs + `}}}]}`
}
func TestProviderStrictProtocolProofAndWriteCoverage(t *testing.T) {
	batchResults := `[` + strictStatementFixture + `,` + strictStatementFixture + `,null]`
	goodBatch := strictBatchEnvelope(batchResults, `[null,null,null]`)
	for _, tc := range []struct {
		name, request, response string
		want                    bool
	}{
		{"execute", strictExecuteRequest, strictExecuteEnvelope(strictStatementFixture), true},
		{"batch_skipped_rollback", strictBatchRequest, goodBatch, true},
		{"invalid_json", strictExecuteRequest, `{"results":[private-invalid-canary`, false},
		{"missing_results", strictExecuteRequest, `{}`, false},
		{"empty_results", strictExecuteRequest, `{"results":[]}`, false},
		{"unknown_type", strictExecuteRequest, strings.Replace(strictExecuteEnvelope(strictStatementFixture), `"type":"ok"`, `"type":"mystery"`, 1), false},
		{"200_error_without_code", strictExecuteRequest, `{"results":[{"type":"error","error":{"message":"Busy private-error-canary"}}]}`, false},
		{"200_error_unknown_code", strictExecuteRequest, `{"results":[{"type":"error","error":{"code":"PRIVATE_UNKNOWN_BUSY","message":"private-error-canary"}}]}`, false},
		{"ok_with_error", strictExecuteRequest, strings.Replace(strictExecuteEnvelope(strictStatementFixture), `"type":"ok"`, `"type":"ok","error":{}`, 1), false},
		{"malformed_execute", strictExecuteRequest, strictExecuteEnvelope(`{"rows":[],"affected_row_count":1}`), false},
		{"null_rows", strictExecuteRequest, strictExecuteEnvelope(`{"cols":[],"rows":null,"affected_row_count":1}`), false},
		{"null_row", strictExecuteRequest, strictExecuteEnvelope(`{"cols":[],"rows":[null],"affected_row_count":1}`), false},
		{"bare_busy_step_error", strictBatchRequest, strictBatchEnvelope(batchResults, `[null,{"message":"Busy private-error-canary"},null]`), false},
		{"unknown_step_error_code", strictBatchRequest, strictBatchEnvelope(batchResults, `[null,{"code":"PRIVATE_UNKNOWN_BUSY"},null]`), false},
		{"partial_batch_error", strictBatchRequest, strictBatchEnvelope(`[`+strictStatementFixture+`,null,null]`, `[null,{"message":"Busy"},null]`), false},
		{"malformed_step_errors", strictBatchRequest, strictBatchEnvelope(batchResults, `{}`), false},
		{"step_count_mismatch", strictBatchRequest, strictBatchEnvelope(batchResults, `[null,null]`), false},
		{"skipped_insert", strictBatchRequest, strictBatchEnvelope(`[`+strictStatementFixture+`,null,null]`, `[null,null,null]`), false},
		{"truncated", strictBatchRequest, goodBatch + strings.Repeat(" ", maxProviderInspectBytes), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const target = "https://strict-private-provider.invalid"
			trace, err := newProviderTrace(target, providerRoundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(tc.response))}, nil
			}))
			if err != nil {
				t.Fatal("trace setup")
			}
			req := providerTestRequest(t, target, tc.request)
			resp, err := trace.RoundTrip(req)
			if err != nil {
				t.Fatal("roundtrip")
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil || string(body) != tc.response {
				t.Fatal("response changed")
			}
			if resp.Body.Close() != nil {
				t.Fatal("close changed")
			}
			original, err := io.ReadAll(req.Body)
			if err != nil || string(original) != tc.request {
				t.Fatal("request body consumed")
			}
			snapshot := trace.Snapshot()
			if len(snapshot.Events) != 1 || snapshot.Events[0].ProtocolSuccess != tc.want {
				t.Fatal("wrong strict success classification")
			}
			if (mixedWriteCoverage(snapshot.Events, 0, []observationInterval{{0, 1e9}}) == 1) != tc.want {
				t.Fatal("unverified protocol response counted as successful INSERT")
			}
			safe, err := json.Marshal(snapshot)
			if err != nil {
				t.Fatal("safe evidence encode")
			}
			for _, private := range []string{target, "private-argument-canary", "private-baton-canary", "private-invalid-canary", "private-error-canary", "PRIVATE_UNKNOWN_BUSY", "INSERT INTO messages"} {
				if bytes.Contains(safe, []byte(private)) {
					t.Fatal("private provider data disclosed")
				}
			}
		})
	}
}
func TestStrictProtocolProofRequiresExactPipelineResponseCount(t *testing.T) {
	execute := `{"type":"ok","response":{"type":"execute","result":` + strictStatementFixture + `}}`
	closeResult := `{"type":"ok","response":{"type":"close"}}`
	expected := []providerResponseShape{{kind: "execute"}, {kind: "close"}}
	if !inspectProviderSuccess([]byte(`{"results":[`+execute+`,`+closeResult+`]}`), 200, expected) {
		t.Fatal("legitimate execute plus close rejected")
	}
	if inspectProviderSuccess([]byte(`{"results":[`+closeResult+`]}`), 200, expected) {
		t.Fatal("missing execute response accepted")
	}
	if inspectProviderSuccess([]byte(`{"results":[`+execute+`,`+closeResult+`,`+closeResult+`]}`), 200, expected) {
		t.Fatal("extra response accepted")
	}
}
