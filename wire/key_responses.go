package wire

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/internal/authkey"
)

// Malformed success responses must never be mistaken for a delivered secret or
// a completed revocation. Keep errors independent of untrusted response text.
var errKeyResponse = errors.New("mqlite: invalid access key response")

// MaxKeyResponseBytes bounds even a full 1000-key page with escaped 128-byte names.
const MaxKeyResponseBytes = 2 << 20

// ReadKeyResponse reads one bounded response without exposing partial contents
// when the connection is truncated or an intermediary returns an oversized body.
func ReadKeyResponse(reader io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, MaxKeyResponseBytes+1))
	if err != nil || len(data) > MaxKeyResponseBytes {
		return nil, errKeyResponse
	}
	return data, nil
}

func keyResponseObject(data []byte, required ...string) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil || fields == nil {
		return nil, errKeyResponse
	}
	for _, name := range required {
		value, ok := fields[name]
		if !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return nil, errKeyResponse
		}
	}
	return fields, nil
}

func decodeAccessKey(data []byte) (AccessKey, error) {
	if _, err := keyResponseObject(data, "id", "name", "permissions", "created_at_ms", "expires_at_ms", "revoked_at_ms"); err != nil {
		return AccessKey{}, err
	}
	var key AccessKey
	if json.Unmarshal(data, &key) != nil || !authkey.ValidID(key.ID) || key.Name == "" ||
		strings.TrimSpace(key.Name) != key.Name || len(key.Name) > 128 || !utf8.ValidString(key.Name) || strings.ContainsRune(key.Name, '\x00') ||
		key.CreatedAtMs < 0 || key.ExpiresAtMs < 0 || key.ExpiresAtMs != 0 && key.ExpiresAtMs <= key.CreatedAtMs || key.RevokedAtMs < 0 {
		return AccessKey{}, errKeyResponse
	}
	permissions, err := engine.ParseKeyPermissions(key.Permissions)
	if err != nil || strings.Join(key.Permissions, ",") != strings.Join(permissions.Names(), ",") {
		return AccessKey{}, errKeyResponse
	}
	return key, nil
}

// DecodeCreateKeyResponse validates the complete one-time result against its
// request. Unknown fields are discarded; errors never include response secrets.
func DecodeCreateKeyResponse(data []byte, request CreateKeyRequest) (CreateKeyResponse, error) {
	fields, err := keyResponseObject(data, "key", "token")
	if err != nil {
		return CreateKeyResponse{}, err
	}
	key, err := decodeAccessKey(fields["key"])
	if err != nil {
		return CreateKeyResponse{}, err
	}
	var token string
	requested, err := engine.ParseKeyPermissions(request.Permissions)
	if err != nil || json.Unmarshal(fields["token"], &token) != nil || !authkey.ValidToken(token) ||
		key.ID != request.ID || key.Name != request.Name || key.ExpiresAtMs != request.ExpiresAtMs || key.RevokedAtMs != 0 ||
		strings.Join(key.Permissions, ",") != strings.Join(requested.Names(), ",") {
		return CreateKeyResponse{}, errKeyResponse
	}
	return CreateKeyResponse{Key: key, Token: token}, nil
}

// DecodeListKeysResponse validates bounded, ordered metadata and cursor progress.
func DecodeListKeysResponse(data []byte, request ListKeysRequest) (ListKeysResponse, error) {
	if request.Sort != "" && request.Sort != engine.KeySortIDAsc && request.Sort != engine.KeySortCreatedDesc {
		return ListKeysResponse{}, errKeyResponse
	}
	fields, err := keyResponseObject(data, "keys")
	if err != nil {
		return ListKeysResponse{}, err
	}
	limit := request.Limit
	if limit == 0 {
		limit = 100
	}
	var rawKeys []json.RawMessage
	if limit < 1 || limit > 1000 || request.AfterID != "" && !authkey.ValidID(request.AfterID) ||
		json.Unmarshal(fields["keys"], &rawKeys) != nil || len(rawKeys) > limit {
		return ListKeysResponse{}, errKeyResponse
	}
	page := ListKeysResponse{Keys: make([]AccessKey, len(rawKeys))}
	previous := request.AfterID
	var previousCreatedAt int64
	seen := map[string]bool{request.AfterID: true}
	for i, raw := range rawKeys {
		key, err := decodeAccessKey(raw)
		if err != nil || seen[key.ID] {
			return ListKeysResponse{}, errKeyResponse
		}
		if request.Sort == engine.KeySortCreatedDesc {
			if i > 0 && (key.CreatedAtMs > previousCreatedAt || key.CreatedAtMs == previousCreatedAt && key.ID >= previous) {
				return ListKeysResponse{}, errKeyResponse
			}
		} else if key.ID <= previous {
			return ListKeysResponse{}, errKeyResponse
		}
		seen[key.ID] = true
		page.Keys[i], previous = key, key.ID
		previousCreatedAt = key.CreatedAtMs
	}
	if next, ok := fields["next_after_id"]; ok {
		if bytes.Equal(bytes.TrimSpace(next), []byte("null")) || json.Unmarshal(next, &page.NextAfterID) != nil {
			return ListKeysResponse{}, errKeyResponse
		}
	}
	if page.NextAfterID != "" && (len(page.Keys) != limit || page.NextAfterID != previous || page.NextAfterID == request.AfterID ||
		request.Sort != engine.KeySortCreatedDesc && page.NextAfterID <= request.AfterID) {
		return ListKeysResponse{}, errKeyResponse
	}
	return page, nil
}

// DecodeRevokeKeyResponse accepts only an explicit successful acknowledgement.
func DecodeRevokeKeyResponse(data []byte) (RevokeKeyResponse, error) {
	fields, err := keyResponseObject(data, "ok")
	var response RevokeKeyResponse
	if err != nil || json.Unmarshal(fields["ok"], &response.Ok) != nil || !response.Ok {
		return RevokeKeyResponse{}, errKeyResponse
	}
	return response, nil
}
