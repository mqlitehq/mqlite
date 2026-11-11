package hrana

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
)

// InvalidAcknowledgementError means a received success response cannot confirm
// the requested operation. It is not a definite SQL rejection or a safe replay.
type InvalidAcknowledgementError struct {
	Cause    error
	BodyRead bool // HTTP headers arrived, but the response body was incomplete
}

func (e *InvalidAcknowledgementError) Error() string {
	return fmt.Sprintf("invalid libSQL acknowledgement: %v", e.Cause)
}

func (e *InvalidAcknowledgementError) Unwrap() error { return e.Cause }

func InvalidAcknowledgement(cause error) error {
	return &InvalidAcknowledgementError{Cause: cause}
}

func IncompleteAcknowledgement(cause error) error {
	return &InvalidAcknowledgementError{Cause: cause, BodyRead: true}
}

func validateResultObject(result json.RawMessage) error {
	trimmed := bytes.TrimSpace(result)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return InvalidAcknowledgement(fmt.Errorf("acknowledgement is missing its result object"))
	}
	return nil
}

func validateStatementAcknowledgement(result *StmtResult, affected *int32) error {
	if affected == nil || result.Cols == nil || result.Rows == nil {
		return fmt.Errorf("statement acknowledgement is missing its result fields")
	}
	if *affected < 0 {
		return fmt.Errorf("statement acknowledgement has a negative affected-row count")
	}
	result.AffectedRowCount = *affected
	for _, row := range result.Rows {
		if row == nil || len(row) != len(result.Cols) {
			return fmt.Errorf("statement acknowledgement row width differs from its columns")
		}
		for _, value := range row {
			switch value.Type {
			case "integer":
				integer, ok := value.Value.(string)
				if !ok {
					return fmt.Errorf("statement acknowledgement contains an invalid integer value")
				}
				if _, err := strconv.ParseInt(integer, 10, 64); err != nil {
					return fmt.Errorf("statement acknowledgement contains an invalid integer value")
				}
			case "text":
				if _, ok := value.Value.(string); !ok {
					return fmt.Errorf("statement acknowledgement contains an invalid %s value", value.Type)
				}
			case "float":
				// The protocol encodes non-finite floats as JSON null. Retain the
				// pinned decoder's nil representation instead of rejecting them.
				if _, ok := value.Value.(float64); value.Value != nil && !ok {
					return fmt.Errorf("statement acknowledgement contains an invalid float value")
				}
			case "blob":
				if value.Base64 == nil || !validRawBase64(*value.Base64) {
					return fmt.Errorf("statement acknowledgement contains an invalid blob value")
				}
			case "null":
			default:
				return fmt.Errorf("statement acknowledgement contains an unknown value type")
			}
		}
	}
	return nil
}

// Match the pinned RawStdEncoding decoder without copying a payload merely to
// validate it. It ignores CR/LF, rejects padding, and permits nonzero unused bits
// because the pinned decoder does not enable Strict mode.
func validRawBase64(encoded string) bool {
	count := 0
	for i := 0; i < len(encoded); i++ {
		c := encoded[i]
		if c == '\r' || c == '\n' {
			continue
		}
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '+', c == '/':
			count++
		default:
			return false
		}
	}
	return count%4 != 1
}

func validateBatchAcknowledgement(response *StreamResponse, result *BatchResult) error {
	if len(result.StepResults) == 0 || len(result.StepResults) != len(result.StepErrors) ||
		response.RequestedBatch != nil && len(result.StepResults) != len(response.RequestedBatch.Steps) {
		return InvalidAcknowledgement(fmt.Errorf("batch result does not cover the requested steps"))
	}
	for i, step := range result.StepResults {
		if step != nil && result.StepErrors[i] != nil {
			return InvalidAcknowledgement(fmt.Errorf("batch step contains both a result and an error"))
		}
		if response.RequestedBatch != nil {
			runs, known := batchConditionOutcome(response.RequestedBatch.Steps[i].Condition, result.StepResults[:i])
			acknowledged := step != nil || result.StepErrors[i] != nil
			if !known || runs != acknowledged {
				return InvalidAcknowledgement(fmt.Errorf("batch step acknowledgement does not match its condition"))
			}
		}
	}
	return nil
}

// Conditions come from the sent request, not from the response. A skipped step
// is a valid double-null position only when its actual condition was false.
func batchConditionOutcome(condition *BatchCondition, previous []*StmtResult) (bool, bool) {
	if condition == nil {
		return true, true
	}
	switch condition.Type {
	case "ok", "error":
		if condition.Step == nil || *condition.Step < 0 || int(*condition.Step) >= len(previous) {
			return false, false
		}
		ok := previous[*condition.Step] != nil
		return ok == (condition.Type == "ok"), true
	case "not":
		value, known := batchConditionOutcome(condition.Cond, previous)
		return !value, condition.Cond != nil && known
	case "and", "or":
		value := condition.Type == "and"
		for i := range condition.Conds {
			part, known := batchConditionOutcome(&condition.Conds[i], previous)
			if !known {
				return false, false
			}
			if condition.Type == "and" {
				value = value && part
			} else {
				value = value || part
			}
		}
		return value, true
	default:
		return false, false
	}
}
