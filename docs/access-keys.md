# Access keys

Runtime access keys are available in the **v0.3.1 source tree (unreleased)**.
The published v0.3.0 supports configured administrator tokens only.

## Choose a permission

| Permission | Operations |
|---|---|
| `send` | Send, schedule and cancel scheduled messages |
| `listen` | Receive, settle, renew, peek and read queue statistics |
| `send` + `listen` | Both application roles |
| `manage` | Both application roles, queue/topic administration, console, metrics, and issuing/revoking keys, including other `manage` keys |

Permissions apply across the broker, not to individual queues. A `listen` key can
delete consumed messages through completion or receive-and-delete; it is not a
read-only auditing role. A `send` key can cancel scheduled messages from other
producers. Use separate brokers when applications need a stronger isolation boundary.

The keys in `MQLITE_TOKENS` remain administrators. Keep one configured administrator
available for recovery. Existing configured token strings remain valid; every newly
generated token uses `mqk_` followed by 64 lowercase hexadecimal characters. The
broker stores only the SHA-256 digest of managed tokens, together with their public
ID, name, permissions and lifecycle timestamps. Names need not be unique.

Managed authentication performs one indexed database query for every request,
without caching credentials. On Turso this includes a remote round trip; measure
the application's request latency and throughput with its intended credentials
and storage location before sizing a deployment.

## Create and use a key

Start the broker with authentication enabled and connect the CLI using an
administrator credential from your secret manager:

```sh
export MQLITE_ENDPOINT=http://127.0.0.1:6754
# Set MQLITE_TOKEN securely to a configured administrator or managed manage key.
mqlite create-queue orders
mqlite key create --name orders-producer --permissions send --output json
mqlite key create --name orders-worker --permissions listen --output json
```

Save each returned `token` securely. It is shown only on successful creation and
cannot be recovered through listing or by repeating creation. Give the producer
its `send` token as `MQLITE_TOKEN`, then run `mqlite send orders 'hello'`. Give the
worker its `listen` token, then run `mqlite receive orders`. The worker command
prints and completes the message by default.

Each creation has a separate public `id` (32 lowercase hex characters). The CLI
generates it before making the request, or accepts `--id`. An optional
`--expires-at-ms` sets a future UTC epoch-millisecond expiry; zero means no expiry.

Use an administrator for `mqlite key list --output json`. Pagination uses
`--after-id` with the previous `next_after_id`; the default page limit is 100,
maximum 1000. Listings include expired and revoked metadata, never secrets or
digests. Configured tokens are managed in configuration and do not appear here.

## Go SDK

Both `Client` and `Embedded` expose `CreateKey`, `ListKeys` and `RevokeKey`.
Embedded administration is trusted local access and needs exclusive ownership of
a local database. Use `Client` to manage a running broker.

```go
id := mqlite.GenerateKeyID() // retain this public ID before the request
created, err := admin.CreateKey(ctx, mqlite.CreateKeyOptions{
    ID: id, Name: "orders-producer", Permissions: []string{"send"},
})
if err != nil {
    // Preserve id; reconcile an ambiguous result before issuing a replacement.
    return fmt.Errorf("create key %s: %w", id, err)
}
// Store created.Token in your secret manager; never log the result struct.
producer, err := mqlite.Open(ctx, endpoint, mqlite.WithToken(created.Token))
if err != nil {
    return err
}
defer producer.Close()
_, err = producer.SendOne(ctx, "orders", mqlite.OutMessage{Body: []byte("hello")})
```

Pass `page.NextAfterID` to the next `ListKeys(ctx, afterID, limit)` call until it is
empty. Revoke a managed key with `RevokeKey(ctx, id)`. The SDK returns
`ErrUnauthenticated` for missing/invalid/revoked/expired credentials and
`ErrPermissionDenied` for insufficient permissions. `Receiver.Run` stops on these
permanent authentication/authorization failures, including settlement and renewal.

## HTTP, console and MCP

The three HTTP operations are `AuthService/CreateKey`, `ListKeys` and `RevokeKey`.
See the complete [request/response contract](api-reference.md#createkey--listkeys--revokekey).
All require `Authorization: Bearer <administrator-token>`. Authentication disabled
with `MQLITE_TOKENS=off` does not enable anonymous key administration.

The console at `/ui/` accepts a configured administrator or managed `manage` key.
Open **Access keys** to create, inspect and revoke credentials. Save the newly
created secret before dismissing its one-time display. The console explains when
an older broker does not support the feature. Application `send`/`listen` keys
belong in clients, not the administrator console.

The MCP server exposes `create_key`, `list_keys` and `revoke_key`. Configure its
`MQLITE_TOKEN` with the permission required by each tool. Key creation needs a
public ID allocated before the call. See [MCP usage](mcp.md); do not paste new
secrets into prompts, tool logs or generated source files.

## Rotate, revoke and recover

1. Create a replacement key and store its secret.
2. Move the application to the replacement and verify a send/receive/settle cycle.
3. Revoke the old public ID with `mqlite key revoke --id <old-id>`.

No restart is needed. Revocation is permanent and repeated revocation succeeds.
New authentication fails immediately after a successful revocation, but a request
already authorized, including a long poll, may finish. Later settle/renew requests
authenticate again. Keys issued by another key are independent: revoking their
issuer does not revoke them. An administrator can revoke itself; retain another
administrator before doing so.

If a creation response is lost, keep the original public ID and list keys as an
administrator to find it. If it exists without a saved secret, revoke it and
create a replacement with a new ID. A repeated ID returns `409 key_conflict` and
never replays the secret. A timeout or `unavailable` response is not proof that
creation failed; clients must not blindly retry with a new ID.

A token explicitly present in `MQLITE_TOKENS` is always an administrator, even if
a matching managed record is restricted, revoked or expired. Remove that static
configuration and restart to remove the configured grant. To rotate configured
tokens, temporarily accept old and new values, move clients, then remove the old
value and restart again.

Backups include managed key state. Restoring an older snapshot can restore an old
revocation state or discard newer credentials. Review keys using a configured
administrator before reopening traffic. See [backup and upgrade procedures](operations.md).
