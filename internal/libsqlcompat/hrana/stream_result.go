package hrana

import (
	"encoding/json"
	"errors"
	"fmt"
)

type StreamResult struct {
	Type     string          `json:"type"`
	Response *StreamResponse `json:"response,omitempty"`
	Error    *Error          `json:"error,omitempty"`
}

type StreamResponse struct {
	Type   string          `json:"type"`
	Result json.RawMessage `json:"result,omitempty"`
	// RequestedBatch is set from the sent request, never from provider JSON.
	RequestedBatch *Batch `json:"-"`
}

func (r *StreamResponse) ExecuteResult() (*StmtResult, error) {
	if r.Type != "execute" {
		return nil, fmt.Errorf("invalid response type: %s", r.Type)
	}
	if err := validateResultObject(r.Result); err != nil {
		return nil, err
	}

	var res StmtResult
	if err := json.Unmarshal(r.Result, &res); err != nil {
		return nil, InvalidAcknowledgement(err)
	}
	return &res, nil
}

func (r *StreamResponse) BatchResult() (*BatchResult, error) {
	if r.Type != "batch" {
		return nil, fmt.Errorf("invalid response type: %s", r.Type)
	}
	if err := validateResultObject(r.Result); err != nil {
		return nil, err
	}

	var res BatchResult
	if err := json.Unmarshal(r.Result, &res); err != nil {
		return nil, InvalidAcknowledgement(err)
	}
	if err := validateBatchAcknowledgement(r, &res); err != nil {
		return nil, err
	}
	for _, e := range res.StepErrors {
		if e != nil {
			return nil, errors.New(e.Message)
		}
	}
	return &res, nil
}

type Error struct {
	Message string  `json:"message"`
	Code    *string `json:"code,omitempty"`
}
