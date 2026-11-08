//go:build mqlite_perf_diag

package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"strconv"
)

// This conservative diagnostic proof follows the pinned Hrana v2 response
// structs. A safe code allowlist is only for reporting, never proof of success.
// Request shapes contain operation kinds/conditional flags, no SQL or values.
func inspectProviderSuccess(data []byte, status int, expected []providerResponseShape) bool {
	if status < 200 || status >= 300 || len(expected) == 0 {
		return false
	}
	top, ok := providerObject(data)
	if !ok || !providerNoError(top) || providerNonNull(top["step_errors"]) {
		return false
	}
	var results []json.RawMessage
	if json.Unmarshal(top["results"], &results) != nil || len(results) != len(expected) {
		return false
	}
	for i, raw := range results {
		result, ok := providerObject(raw)
		if !ok || !providerNoError(result) || providerNonNull(result["step_errors"]) {
			return false
		}
		var kind string
		if json.Unmarshal(result["type"], &kind) != nil || kind != "ok" {
			return false
		}
		response, ok := providerObject(result["response"])
		if !ok || !providerNoError(response) || providerNonNull(response["step_errors"]) {
			return false
		}
		if json.Unmarshal(response["type"], &kind) != nil || kind != expected[i].kind {
			return false
		}
		switch kind {
		case "execute":
			if !providerStatementSuccess(response["result"]) {
				return false
			}
		case "batch":
			batch, ok := providerObject(response["result"])
			if !ok || !providerNoError(batch) {
				return false
			}
			var steps, errs []json.RawMessage
			if json.Unmarshal(batch["step_results"], &steps) != nil || json.Unmarshal(batch["step_errors"], &errs) != nil || len(steps) != len(expected[i].allowSkipped) || len(errs) != len(steps) || len(steps) == 0 {
				return false
			}
			for j := range steps {
				if providerNonNull(errs[j]) {
					return false
				}
				if !providerNonNull(steps[j]) {
					if !expected[i].allowSkipped[j] {
						return false
					}
				} else if !providerStatementSuccess(steps[j]) {
					return false
				}
			}
		case "close":
			if providerNonNull(response["result"]) {
				return false
			}
		default:
			return false
		}
	}
	return true
}
func providerNonNull(raw json.RawMessage) bool {
	return len(raw) > 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}
func providerObject(raw json.RawMessage) (map[string]json.RawMessage, bool) {
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil || object == nil {
		return nil, false
	}
	return object, true
}
func providerNoError(object map[string]json.RawMessage) bool {
	return !providerNonNull(object["error"])
}
func providerStatementSuccess(raw json.RawMessage) bool {
	result, ok := providerObject(raw)
	if !ok || !providerNoError(result) || providerNonNull(result["step_errors"]) {
		return false
	}
	var cols []json.RawMessage
	var rows []json.RawMessage
	// Both arrays and the integer row count are required by StmtResult; null
	// arrays cannot establish a complete protocol response.
	if !providerNonNull(result["cols"]) || !providerNonNull(result["rows"]) || json.Unmarshal(result["cols"], &cols) != nil || json.Unmarshal(result["rows"], &rows) != nil {
		return false
	}
	var affected json.Number
	if json.Unmarshal(result["affected_row_count"], &affected) != nil {
		return false
	}
	n, err := affected.Int64()
	if err != nil || n < 0 {
		return false
	}
	if providerNonNull(result["last_insert_rowid"]) {
		var rowID string
		if json.Unmarshal(result["last_insert_rowid"], &rowID) != nil {
			return false
		}
		if _, err := strconv.ParseInt(rowID, 10, 64); err != nil {
			return false
		}
	}
	for _, raw := range cols {
		col, ok := providerObject(raw)
		if !ok || !providerNoError(col) {
			return false
		}
		for _, key := range []string{"name", "decltype"} {
			if providerNonNull(col[key]) {
				var text string
				if json.Unmarshal(col[key], &text) != nil {
					return false
				}
			}
		}
	}
	for _, rawRow := range rows {
		var row []json.RawMessage
		if !providerNonNull(rawRow) || json.Unmarshal(rawRow, &row) != nil {
			return false
		}
		if len(row) != len(cols) {
			return false
		}
		for _, raw := range row {
			if !providerValueValid(raw) {
				return false
			}
		}
	}
	return true
}
func providerValueValid(raw json.RawMessage) bool {
	value, ok := providerObject(raw)
	if !ok || !providerNoError(value) {
		return false
	}
	var kind string
	if json.Unmarshal(value["type"], &kind) != nil {
		return false
	}
	switch kind {
	case "null":
		return !providerNonNull(value["value"]) && !providerNonNull(value["base64"])
	case "integer":
		var text string
		if json.Unmarshal(value["value"], &text) != nil {
			return false
		}
		_, err := strconv.ParseInt(text, 10, 64)
		return err == nil
	case "text":
		var text string
		return providerNonNull(value["value"]) && json.Unmarshal(value["value"], &text) == nil
	case "float":
		var number float64
		return json.Unmarshal(value["value"], &number) == nil && providerNonNull(value["value"])
	case "blob":
		var text string
		if !providerNonNull(value["base64"]) || json.Unmarshal(value["base64"], &text) != nil {
			return false
		}
		if _, err := base64.RawStdEncoding.DecodeString(text); err == nil {
			return true
		}
		_, err := base64.StdEncoding.DecodeString(text)
		return err == nil
	default:
		return false
	}
}
