package mqlite

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/internal/authkey"
	"github.com/mqlitehq/mqlite/wire"
)

// AccessKey is public key metadata. It never contains a token or its digest.
// Permissions are send, listen, or manage (which includes send and listen).
// All timestamps are UTC epoch milliseconds; zero means no expiry or revocation.
type AccessKey = wire.AccessKey

// CreateKeyOptions describes a new broker-wide credential. ID is required: generate
// it with GenerateKeyID before calling CreateKey and retain it if the response is
// lost. A repeated ID returns ErrKeyConflict; an existing secret is never returned.
type CreateKeyOptions = wire.CreateKeyRequest

// CreateKeyResult contains the credential's metadata and its one-time secret.
// Store Token securely: neither ListKeys nor a repeated CreateKey can recover it.
type CreateKeyResult = wire.CreateKeyResponse

// KeyPage is a page of metadata, including revoked and expired keys. Pass
// NextAfterID to ListKeys for the next page; an empty value marks the final page.
type KeyPage = wire.ListKeysResponse

// CreateKey creates a managed credential. The caller must have manage permission.
// It sends one request and never retries a lost response: retain opts.ID to list
// and revoke a possibly-created key before issuing a replacement.
func (c *Client) CreateKey(ctx context.Context, opts CreateKeyOptions) (CreateKeyResult, error) {
	if !authkey.ValidID(opts.ID) {
		return CreateKeyResult{}, fmt.Errorf("%w: key ID must be 32 lowercase hexadecimal characters", ErrInvalidArgument)
	}
	var raw json.RawMessage
	if err := c.post(ctx, wire.PathCreateKey, opts, &raw); err != nil {
		return CreateKeyResult{}, fmt.Errorf("create key ID %s: %w", opts.ID, err)
	}
	result, err := wire.DecodeCreateKeyResponse(raw, opts)
	if err != nil {
		return CreateKeyResult{}, fmt.Errorf("%w: create key ID %s received an invalid response; list and revoke this ID before issuing a replacement", ErrOutcomeUnknown, opts.ID)
	}
	return result, nil
}

// ListKeys lists managed credentials without secrets. Limit defaults to 100 and
// may not exceed 1000. Static administrator tokens are not part of this list.
func (c *Client) ListKeys(ctx context.Context, afterID string, limit int) (KeyPage, error) {
	request := wire.ListKeysRequest{AfterID: afterID, Limit: limit}
	var raw json.RawMessage
	if err := c.post(ctx, wire.PathListKeys, request, &raw); err != nil {
		return KeyPage{}, err
	}
	return wire.DecodeListKeysResponse(raw, request)
}

// RevokeKey permanently revokes a managed key. Repeating a successful revocation
// succeeds; an unknown ID returns ErrNotFound. Static tokens cannot be revoked.
func (c *Client) RevokeKey(ctx context.Context, id string) error {
	var raw json.RawMessage
	if err := c.post(ctx, wire.PathRevokeKey, wire.RevokeKeyRequest{ID: id}, &raw); err != nil {
		return err
	}
	if _, err := wire.DecodeRevokeKeyResponse(raw); err != nil {
		return fmt.Errorf("%w: invalid revocation acknowledgement; verify the key state or repeat revocation", ErrOutcomeUnknown)
	}
	return nil
}

// CreateKey creates a managed credential in the embedded database. Embedded mode
// is trusted local administration and does not require a bearer token.
func (e *Embedded) CreateKey(ctx context.Context, opts CreateKeyOptions) (CreateKeyResult, error) {
	permissions, err := engine.ParseKeyPermissions(opts.Permissions)
	if err != nil {
		return CreateKeyResult{}, err
	}
	key, token, err := e.eng.CreateAccessKey(ctx, engine.CreateAccessKeyOptions{
		ID: opts.ID, Name: opts.Name, Permissions: permissions, ExpiresAtMs: opts.ExpiresAtMs,
	})
	if err != nil {
		return CreateKeyResult{}, err
	}
	return CreateKeyResult{Key: wire.FromAccessKey(key), Token: token}, nil
}

// ListKeys lists managed credential metadata in the embedded database.
func (e *Embedded) ListKeys(ctx context.Context, afterID string, limit int) (KeyPage, error) {
	page, err := e.eng.ListAccessKeys(ctx, afterID, limit)
	if err != nil {
		return KeyPage{}, err
	}
	out := KeyPage{Keys: make([]AccessKey, len(page.Keys)), NextAfterID: page.NextAfterID}
	for i, key := range page.Keys {
		out.Keys[i] = wire.FromAccessKey(key)
	}
	return out, nil
}

// RevokeKey permanently revokes a managed credential in the embedded database.
func (e *Embedded) RevokeKey(ctx context.Context, id string) error {
	return e.eng.RevokeAccessKey(ctx, id)
}
