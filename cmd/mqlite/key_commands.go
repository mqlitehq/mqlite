package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/mqlitehq/mqlite"
	"github.com/mqlitehq/mqlite/internal/authkey"
)

func cmdKey(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: key create|list|revoke [flags]")
	}
	switch args[0] {
	case "create":
		return cmdKeyCreate(ctx, args[1:])
	case "list":
		return cmdKeyList(ctx, args[1:])
	case "revoke":
		return cmdKeyRevoke(ctx, args[1:])
	default:
		return fmt.Errorf("usage: key create|list|revoke [flags]")
	}
}

func cmdKeyCreate(ctx context.Context, args []string) error {
	fs := newFlags("key create")
	name := fs.String("name", "", "key name (required)")
	permissions := fs.String("permissions", "", "send, listen, send,listen, or manage (required)")
	id := fs.String("id", "", "public key ID (32 lowercase hex; generated if omitted)")
	expires := fs.Int64("expires-at-ms", 0, "expiry in UTC epoch milliseconds (0 = no expiry)")
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		return err
	}
	if !flagGiven("id") {
		*id = mqlite.GenerateKeyID()
	}
	if !authkey.ValidID(*id) {
		// An operator might accidentally paste a token into --id. Never echo an
		// invalid value as if it were a safe public identifier.
		return fmt.Errorf("invalid key ID: want 32 lowercase hexadecimal characters")
	}
	// An uncertain create must remain identifiable even if dialing, the HTTP reply,
	// or writing the one-time secret fails. Never include the secret in an error.
	failed := func(err error) error { return fmt.Errorf("create key ID %s: %w", *id, err) }
	if len(pos) != 0 || strings.TrimSpace(*name) == "" || *permissions == "" {
		return failed(fmt.Errorf("usage: key create --name NAME --permissions send,listen|manage [--id ID] [--expires-at-ms MS]"))
	}
	rights := strings.Split(*permissions, ",")
	for i := range rights {
		rights[i] = strings.TrimSpace(rights[i])
	}
	c, err := dial(ctx)
	if err != nil {
		return failed(err)
	}
	defer c.Close()
	result, err := c.CreateKey(ctx, mqlite.CreateKeyOptions{
		ID: *id, Name: *name, Permissions: rights, ExpiresAtMs: *expires,
	})
	if err != nil {
		return failed(err)
	}
	if jsonOut() {
		err = emitJSON(result)
	} else {
		_, err = fmt.Fprintf(os.Stdout, "Key ID: %s\nName: %s\nPermissions: %s\nToken (save now; shown only once): %s\n",
			result.Key.ID, result.Key.Name, strings.Join(result.Key.Permissions, ","), result.Token)
	}
	if err != nil {
		return failed(err)
	}
	return nil
}

func cmdKeyList(ctx context.Context, args []string) error {
	fs := newFlags("key list")
	after := fs.String("after-id", "", "continue after this public key ID")
	limit := fs.Int("limit", 100, "page size (1-1000)")
	order := fs.String("sort", "id_asc", "id_asc or created_desc (newest first)")
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 0 {
		return fmt.Errorf("usage: key list [--after-id ID] [--limit N] [--sort id_asc|created_desc]")
	}
	if *order != "id_asc" && *order != "created_desc" {
		return fmt.Errorf("invalid key sort: want id_asc or created_desc")
	}
	c, err := dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	page, err := c.ListKeysWithOptions(ctx, mqlite.ListKeysOptions{AfterID: *after, Limit: *limit, Sort: *order})
	if err != nil {
		return err
	}
	if jsonOut() {
		return emitJSON(page)
	}
	for _, key := range page.Keys {
		if _, err := fmt.Fprintf(os.Stdout, "%s\t%s\t%s\tcreated=%d\texpires=%d\trevoked=%d\n",
			key.ID, key.Name, strings.Join(key.Permissions, ","), key.CreatedAtMs, key.ExpiresAtMs, key.RevokedAtMs); err != nil {
			return err
		}
	}
	if page.NextAfterID != "" {
		sortFlag := ""
		if *order == "created_desc" {
			sortFlag = " --sort created_desc"
		}
		_, err = fmt.Fprintf(os.Stdout, "Next page: key list --after-id %s --limit %d%s\n", page.NextAfterID, *limit, sortFlag)
	}
	return err
}

func cmdKeyRevoke(ctx context.Context, args []string) error {
	fs := newFlags("key revoke")
	id := fs.String("id", "", "public key ID (required)")
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 0 || *id == "" {
		return fmt.Errorf("usage: key revoke --id ID")
	}
	c, err := dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	if err := c.RevokeKey(ctx, *id); err != nil {
		return err
	}
	if jsonOut() {
		return emitJSON(struct {
			Ok bool `json:"ok"`
		}{Ok: true})
	}
	_, err = fmt.Fprintln(os.Stdout, "Revoked key ID:", *id)
	return err
}
