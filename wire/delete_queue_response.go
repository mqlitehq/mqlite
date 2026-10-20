package wire

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

var errDeleteQueueResponse = errors.New("mqlite: invalid delete queue response")

// MaxDeleteQueueResponseBytes bounds the complete acknowledgement returned by a
// broker or intermediary. A valid response contains only two small fields.
const MaxDeleteQueueResponseBytes = 4 << 10

// ReadDeleteQueueResponse reads one bounded acknowledgement so truncated or
// oversized successes cannot be mistaken for an accepted deletion.
func ReadDeleteQueueResponse(reader io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, MaxDeleteQueueResponseBytes+1))
	if err != nil || len(data) > MaxDeleteQueueResponseBytes {
		return nil, errDeleteQueueResponse
	}
	return data, nil
}

// DecodeDeleteQueueResponse requires an explicit, internally consistent
// acknowledgement. Unknown fields remain forward compatible, but both current
// fields must be present and carry one of the two defined state combinations.
func DecodeDeleteQueueResponse(data []byte) (DeleteQueueResponse, error) {
	if len(data) > MaxDeleteQueueResponseBytes {
		return DeleteQueueResponse{}, errDeleteQueueResponse
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil || fields == nil {
		return DeleteQueueResponse{}, errDeleteQueueResponse
	}
	deletedJSON, hasDeleted := fields["deleted"]
	stateJSON, hasState := fields["state"]
	if !hasDeleted || !hasState || bytes.Equal(bytes.TrimSpace(deletedJSON), []byte("null")) ||
		bytes.Equal(bytes.TrimSpace(stateJSON), []byte("null")) {
		return DeleteQueueResponse{}, errDeleteQueueResponse
	}
	var response DeleteQueueResponse
	if json.Unmarshal(deletedJSON, &response.Deleted) != nil || json.Unmarshal(stateJSON, &response.State) != nil ||
		response.Deleted && response.State != "deleted" || !response.Deleted && response.State != "deleting" {
		return DeleteQueueResponse{}, errDeleteQueueResponse
	}
	return response, nil
}
