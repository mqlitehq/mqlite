# Independent remote SDK HTTP pipeline

This separate test module runs the remote-only `mqlite-go` client against a real
HTTP broker backed by SQLite or the dedicated Turso verification database. It
keeps the main module's dependencies unchanged. Its Go 1.21 floor belongs to the
broker harness; it does not change the remote SDK's Go 1.13 floor.

Place the intended `mqlite-go` checkout beside the main repository, then run:

```sh
cd test/sdkcheck/remotesdk
go test -race -run '^TestSQLiteHTTPBrokerDelivery$' -count=1 -v -timeout 150s .
```

The gated `TestTursoHTTPBrokerDelivery` requires both `MQLITE_TEST_DB` and
`MQLITE_TEST_DB_AUTH_TOKEN`, accepts only the nightly verification database, and
creates and deletes its own unique queue. It never resets tables. The nightly
workflow pins the remote SDK checkout by full commit SHA and runs this module
under the existing shared Turso concurrency lock.

`TestSQLiteInFlightBrokerRestart` and `TestTursoInFlightBrokerRestart` run one
shared vector through both SDKs. Two handlers remain active while the real
`Embedded.Serve` stops, the engine closes, and the same database reopens at the
same HTTP address after 500ms. With a 30-second lease and a delivery limit of
one, each handler must execute once and receive a successful Complete response;
the reopened database must preserve both live leases, then end empty with no
dead letters. The remote test uses the same allowlist and only its own queues.

```sh
go test -race -run '^TestSQLiteInFlightBrokerRestart$' -count=1 -v -timeout 120s .
```

This tests application-managed broker close/reopen while the lease remains
valid. It does not test operating-system signals, suspended hosts, power loss,
outages beyond the lease, or graceful draining of client handlers. Those cases
have different recovery semantics; at-least-once delivery still requires
idempotent handlers.
