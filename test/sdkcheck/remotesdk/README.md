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
same HTTP address after an additional 500ms stopped wait. The actual unavailable
interval also includes drain, engine close, reopen and listener readiness, and is
logged separately. SQLite uses a 30-second lease; the remote profile uses an
explicit two-minute lease because an observed Turso reopen took 35.375 seconds.
The earlier 30-second profile's lease-expiry/DLQ result remains a failed
precondition observation, not evidence of wrongful revocation. The queue API's
default is unchanged. With a delivery limit of one, each handler must execute
once and receive a successful Complete response while its lease remains valid;
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

On failure, the pipeline records handler/settlement counts and probes its own
queue with a separate five-second context before cleanup. Error diagnostics
contain only types, HTTP statuses and fixed sentinel flags; provider error text,
SQL, credentials and endpoints are excluded and covered by a privacy control.
The nightly workflow runs the restart vector independently after SDK checkout,
even when the earlier pipeline fails; that failure still fails the workflow.
Manual `verification_scope=receivers` skips the destructive engine suite and the
main SDK pipeline, while preserving the dedicated-database allowlist and shared
concurrency lock. `full` remains the default and matches scheduled verification.
The manual `restart` scope runs only the shared in-flight restart vector after
the pinned SDK checkout; it also skips database reset and unrelated pipelines.

Pipeline success is counted from the client transport reading and validating a
complete `200` / `ok:true` response bound to an accepted queue and sequence.
Server-buffered responses remain a separate observation. Local controls cover
lost and truncated responses, malformed acknowledgements and mismatched request
identities. The 120-second pipeline budget includes database open, queue setup
and serial sends; phase timestamps distinguish those costs from Receiver time
and show when a test deadline itself caused cancellation or Abandon.
Restart logs also bind each handler to its initial lease deadline and server
time. If that known live-lease bound has expired by readiness or the post-open
probe, the test reports an expired precondition, rather than calling it wrongful
revocation of a live token. A later successful renewal can extend that bound;
these initial-deadline logs alone do not prove that an actual token has expired.
