package wire

import (
	"bytes"
	"encoding/json"
	"errors"
)

var errSubscribeTTL = errors.New("mqlite: ttl_ms must be a non-negative integer or omitted")

// UnmarshalJSON distinguishes an omitted or explicit zero TTL from null. Both
// omission and zero mean permanent creation or preservation of an existing
// lifetime; positive integers request a fixed lifetime.
func (r *SubscribeWithOptionsRequest) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	if fields == nil {
		return errSubscribeTTL
	}
	ttl, hasTTL := fields["ttl_ms"]
	if hasTTL && bytes.Equal(bytes.TrimSpace(ttl), []byte("null")) {
		return errSubscribeTTL
	}

	type plain SubscribeWithOptionsRequest
	var decoded plain
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return err
	}
	if hasTTL && decoded.TTLMs < 0 {
		return errSubscribeTTL
	}
	*r = SubscribeWithOptionsRequest(decoded)
	return nil
}
