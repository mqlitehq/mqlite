package engine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/mqlitehq/mqlite/internal/authkey"
)

// KeyPermissions is a broker-wide set of access key capabilities.
type KeyPermissions uint8

const (
	KeySend   KeyPermissions = 1
	KeyListen KeyPermissions = 2
	// KeyManage includes sending, listening, entity administration and key issuance.
	KeyManage KeyPermissions = 7
)

func (p KeyPermissions) valid() bool {
	return p == KeySend || p == KeyListen || p == KeySend|KeyListen || p == KeyManage
}

// ParseKeyPermissions validates and normalizes an explicit permission set.
// Manage includes send and listen; unknown names are rejected even beside manage.
func ParseKeyPermissions(names []string) (KeyPermissions, error) {
	var p KeyPermissions
	for _, name := range names {
		switch name {
		case "send":
			p |= KeySend
		case "listen":
			p |= KeyListen
		case "manage":
			p |= KeyManage
		default:
			return 0, fmt.Errorf("%w: unknown key permission", ErrInvalidArgument)
		}
	}
	if !p.valid() {
		return 0, fmt.Errorf("%w: at least one key permission is required", ErrInvalidArgument)
	}
	return p, nil
}

// Names returns the canonical public representation, or nil for an invalid set.
func (p KeyPermissions) Names() []string {
	switch p {
	case KeySend:
		return []string{"send"}
	case KeyListen:
		return []string{"listen"}
	case KeySend | KeyListen:
		return []string{"send", "listen"}
	case KeyManage:
		return []string{"manage"}
	default:
		return nil
	}
}

// Allows reports whether a valid permission set grants every required capability.
func (p KeyPermissions) Allows(required KeyPermissions) bool {
	return p.valid() && required.valid() && p&required == required
}

// AccessKey contains public metadata only: neither the token nor its digest.
type AccessKey struct {
	ID, Name                              string
	Permissions                           KeyPermissions
	CreatedAtMs, ExpiresAtMs, RevokedAtMs int64
}

// CreateAccessKeyOptions describes an independently revocable broker credential.
// ID must be generated and retained by the caller before requesting creation.
type CreateAccessKeyOptions struct {
	ID, Name    string
	Permissions KeyPermissions
	ExpiresAtMs int64
}

// AccessKeyPage is ordered by ID and includes revoked and expired records.
type AccessKeyPage struct {
	Keys        []AccessKey
	NextAfterID string
}

const accessKeyColumns = `id, name, permissions, created_at, expires_at, revoked_at`

// CreateAccessKey persists a digest and returns the new secret exactly once.
// Repeated IDs fail with ErrKeyConflict; an existing secret is never recovered.
func (e *Engine) CreateAccessKey(ctx context.Context, opts CreateAccessKeyOptions) (AccessKey, string, error) {
	if !authkey.ValidID(opts.ID) {
		return AccessKey{}, "", fmt.Errorf("%w: key id must be 32 lowercase hex characters", ErrInvalidArgument)
	}
	if opts.Name == "" || strings.TrimSpace(opts.Name) != opts.Name || len(opts.Name) > 128 || !utf8.ValidString(opts.Name) || strings.ContainsRune(opts.Name, '\x00') {
		return AccessKey{}, "", fmt.Errorf("%w: key name must be trimmed UTF-8 text of 1 to 128 bytes", ErrInvalidArgument)
	}
	if !opts.Permissions.valid() {
		return AccessKey{}, "", fmt.Errorf("%w: invalid key permissions", ErrInvalidArgument)
	}
	if opts.ExpiresAtMs < 0 || opts.ExpiresAtMs != 0 && opts.ExpiresAtMs <= e.now() {
		return AccessKey{}, "", fmt.Errorf("%w: key expiry must be zero or a future epoch millisecond", ErrInvalidArgument)
	}
	token, err := authkey.GenerateToken()
	if err != nil {
		return AccessKey{}, "", fmt.Errorf("generate access key: %w", err)
	}
	digest := sha256.Sum256([]byte(token))
	var key AccessKey
	// The secret and public ID remain stable across safe transaction retries.
	err = e.inTx(ctx, func(ctx context.Context, tx *txn) error {
		now := e.now()
		if now < 0 || opts.ExpiresAtMs != 0 && opts.ExpiresAtMs <= now {
			return fmt.Errorf("%w: key expiry must be zero or a future epoch millisecond", ErrInvalidArgument)
		}
		res, err := tx.ExecContext(ctx, `INSERT INTO access_keys (`+accessKeyColumns+`, token_hash) VALUES (?, ?, ?, ?, ?, 0, ?) ON CONFLICT(id) DO NOTHING`, opts.ID, opts.Name, opts.Permissions, now, opts.ExpiresAtMs, digest[:])
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return ErrKeyConflict
		}
		key = AccessKey{ID: opts.ID, Name: opts.Name, Permissions: opts.Permissions, CreatedAtMs: now, ExpiresAtMs: opts.ExpiresAtMs}
		return nil
	})
	if err != nil {
		return AccessKey{}, "", err
	}
	return key, token, nil
}

// ListAccessKeys lists public metadata in ID order, using a bounded cursor page.
func (e *Engine) ListAccessKeys(ctx context.Context, afterID string, limit int) (AccessKeyPage, error) {
	if afterID != "" && !authkey.ValidID(afterID) {
		return AccessKeyPage{}, fmt.Errorf("%w: invalid key cursor", ErrInvalidArgument)
	}
	if limit == 0 {
		limit = 100
	}
	if limit < 1 || limit > 1000 {
		return AccessKeyPage{}, fmt.Errorf("%w: key page limit must be between 1 and 1000", ErrInvalidArgument)
	}
	page := AccessKeyPage{Keys: []AccessKey{}}
	err := e.db.queryRows(ctx, `SELECT `+accessKeyColumns+` FROM access_keys WHERE id > ? ORDER BY id LIMIT ?`, func(rows *sql.Rows) error {
		for rows.Next() {
			var key AccessKey
			if err := rows.Scan(&key.ID, &key.Name, &key.Permissions, &key.CreatedAtMs, &key.ExpiresAtMs, &key.RevokedAtMs); err != nil {
				return err
			}
			page.Keys = append(page.Keys, key)
		}
		return rows.Err()
	}, afterID, limit+1)
	if err != nil {
		return AccessKeyPage{}, err
	}
	if len(page.Keys) > limit {
		page.Keys = page.Keys[:limit]
		page.NextAfterID = page.Keys[limit-1].ID
	}
	return page, nil
}

// RevokeAccessKey permanently marks a database key revoked. Replays are idempotent;
// credentials issued by this key are independent and are not revoked with it.
func (e *Engine) RevokeAccessKey(ctx context.Context, id string) error {
	if !authkey.ValidID(id) {
		return fmt.Errorf("%w: invalid key id", ErrInvalidArgument)
	}
	res, err := e.db.execFresh(ctx, `UPDATE access_keys SET revoked_at = CASE WHEN revoked_at = 0 THEN ? ELSE revoked_at END WHERE id = ?`, func() []any {
		now := e.now()
		if now < 1 {
			now = 1 // Zero is reserved for "not revoked", even with an epoch test clock.
		}
		return []any{now, id}
	})
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// AuthenticateAccessKey validates a database credential without caching it or
// updating the record. A request starting after a successful revocation is denied.
func (e *Engine) AuthenticateAccessKey(ctx context.Context, token string) (AccessKey, error) {
	if !authkey.ValidToken(token) {
		return AccessKey{}, ErrUnauthenticated
	}
	digest := sha256.Sum256([]byte(token))
	var key AccessKey
	err := e.db.queryRowScan(ctx, []any{&key.ID, &key.Name, &key.Permissions, &key.CreatedAtMs, &key.ExpiresAtMs, &key.RevokedAtMs}, `SELECT `+accessKeyColumns+` FROM access_keys WHERE token_hash = ?`, digest[:])
	if errors.Is(err, sql.ErrNoRows) {
		return AccessKey{}, ErrUnauthenticated
	}
	if err != nil {
		return AccessKey{}, err
	}
	if !key.Permissions.valid() || key.RevokedAtMs != 0 || key.ExpiresAtMs != 0 && key.ExpiresAtMs <= e.now() {
		return AccessKey{}, ErrUnauthenticated
	}
	return key, nil
}
