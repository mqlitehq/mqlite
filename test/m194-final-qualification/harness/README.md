# Current-source standard Turso client compatibility smoke

This temporary diagnostic uses the finite harness from run 42495993691, whose
runtime was 5692113 and SDK was 3843e2c. Those old results are historical only.
The Go scenarios, ledger, assertions, supervisor and budgets are retained byte
for byte. This run instead binds runtime 031af85e85385bd9184255b0c05fcca019afe36d
and SDK 5127a9062dad4de917c13c7ccdee06cdf9085b7e, plus a newly frozen harness and
workflow hash. The legacy private M164_* environment names and random queue
prefix are intentionally retained; they do not identify the tested source.

The scope is standard-Turso compatibility through an authenticated loopback HTTP
broker handler and both clients, plus engine transaction commit/controlled
rollback and unclaimed-message close/reopen. It is not weak-network injection,
capacity, long-duration soak, Rust support, actual mqlite serve signals/drain,
in-flight lease restart or a Go 1.13 runtime execution test. The independent SDK
source floor remains verified by its own CI. WithBrokerMode and WithoutBackground
are explicit. Custom client transports keep the recorder isolated; this is not a
measurement of SDK default-pool behavior.

Six random, collision-checked queue names are recorded before creation. Ten
acknowledged identities must match complete payloads and sequences, receive once
and complete. Native and standalone clients each exercise two-message manual
send/receive/RenewBatch/CompleteBatch and one managed delivery: concurrency one,
three-second lease, four-second handler, confirmed renewal and Complete,
cancellation and join. Two acknowledged unclaimed messages must survive close
and reopen before each client consumes one. A live lease need not survive an
arbitrarily slow cold open, and no result is described as that guarantee.

Budgets remain 240 seconds for scenarios, 60 seconds for exact-resource cleanup,
45 seconds per engine open, 15 seconds per ordinary RPC, 120 seconds for build
and 310 seconds for the supervised execution. Managed work and its join must fit
the scenario, leaving thirty seconds before its deadline for confirmation.
Failures do not extend budgets or trigger automatic retries.

There is no schema reset, TestMain, prefix-wide delete or global maintenance.
The existing dedicated standard database is checked by its approved destination
hash and a nonempty existing token, never printed. Reset confirmation must be
unset. The workflow shares exactly turso-verification-db, cancellation disabled,
with nightly and soak: it queues behind either and cannot reset their data.
Cleanup operates only on this run's exact manifest, waits for managed work and
any closing owner, then confirms deletion and not-found/inventory readback.
Unjoined or forcibly killed work may leave the manifest unresolved and fails;
provider stream disposal and rollback are not inferred from HTTP cancellation.

The only diagnostic delta from runtime is this support directory and the added
.github/workflows/m163-shutdown-clients.yml. The original performance-soak workflow
is unchanged. The new workflow runs only for a push to the exact reviewed
upstream diagnostic/verify-transaction-shutdown-clients branch, not forks or pull requests. A new
workflow absent from the default branch must not rely on workflow_dispatch.
Official checkout/setup-go/upload-artifact actions reuse reviewed immutable pins.
Only the bounded live step receives the repository's existing verification
secrets; local controls receive none. Do not add personal, Rust or production keys.

Before pushing, commit only these two diagnostic paths, excluding the separate
.diagnostic-sdk checkout. The root checkout must contain complete history and the
SDK checkout must be at the exact reviewed SHA. The source guard checks actual
filesystem inventories, clean Git state, exact Git blobs and declared hashes,
rejecting ignored/extra files and symlinks. It records source maps before and
after; both must match. The known old shallow-clone failure is not bypassed.
The guard expects this workflow to be an addition, not a runtime file overwrite.

Run the local Go controls, supervisor controls and source-map negative controls
before freezing the expected map. They use only new local SQLite/loopback data
and temporary Git fixtures; the source guard's real-checkout before/after checks
need a clean committed diagnostic tree. No runtime or SDK source is edited.

The supervisor hashes opaque compiler/provider stdout and stderr in memory;
only fixed status messages and sanitized evidence are saved. Never upload raw
environment, headers, URLs, error bodies, SQL, panic output or credentials.
Artifacts contain identity ledgers, safe operation counters/fingerprints, exact
owned-resource cleanup, source maps, supervisor results and a binary hash.
Successful execution requires zero exit, no timeout, pass=true, exact identities
and both receivers joined. It does not turn the existing engine-only functional
nightly or a separate thirty-minute engine soak into client-path evidence.

This execution specifically requalifies the committed shutdown-drain fix 031af85;
prior D613 compatibility passed but is not transferred. Product Close now gives
admitted SQL a bounded drain opportunity. No workload, lease, cleanup budget or
identity assertion changes, and no live secrets are used in local controls.
