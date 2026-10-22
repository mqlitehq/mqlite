package wire

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

var errDeleteResponse = errors.New("mqlite: invalid deletion response")

// MaxDeleteResponseBytes bounds the complete acknowledgement returned by a
// broker or intermediary. A valid response contains only two small fields.
const MaxDeleteResponseBytes = 4 << 10

// ReadDeleteResponse reads one bounded acknowledgement so truncated or
// oversized successes cannot be mistaken for an accepted deletion.
func ReadDeleteResponse(reader io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, MaxDeleteResponseBytes+1))
	if err != nil || len(data) > MaxDeleteResponseBytes {
		return nil, errDeleteResponse
	}
	return data, nil
}

// DecodeDeleteResponse requires an explicit, internally consistent
// acknowledgement. Unknown fields remain forward compatible, but both current
// fields must be present and carry one of the two defined state combinations.
func DecodeDeleteResponse(data []byte) (DeleteResponse, error) {
	if len(data) > MaxDeleteResponseBytes {
		return DeleteResponse{}, errDeleteResponse
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil || fields == nil {
		return DeleteResponse{}, errDeleteResponse
	}
	deletedJSON, hasDeleted := fields["deleted"]
	stateJSON, hasState := fields["state"]
	if !hasDeleted || !hasState || bytes.Equal(bytes.TrimSpace(deletedJSON), []byte("null")) ||
		bytes.Equal(bytes.TrimSpace(stateJSON), []byte("null")) {
		return DeleteResponse{}, errDeleteResponse
	}
	var response DeleteResponse
	if json.Unmarshal(deletedJSON, &response.Deleted) != nil || json.Unmarshal(stateJSON, &response.State) != nil ||
		response.Deleted && response.State != "deleted" || !response.Deleted && response.State != "deleting" {
		return DeleteResponse{}, errDeleteResponse
	}
	return response, nil
}
