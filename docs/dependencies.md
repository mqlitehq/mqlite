# Dependency policy & the go 1.21 floor

MQLite pins its `go.mod` floor at **`go 1.21`** so the SDK stays drop-in
embeddable in projects that haven't moved to a newer toolchain (MQLITE-1). That
single decision freezes two dependencies — this note records why, what it costs,
and how the freeze is enforced so it can't be lifted by accident.

## Remote-only companion module

Applications that only call a broker can instead import the official
[`github.com/mqlitehq/mqlite-go`](https://github.com/mqlitehq/mqlite-go) module.
Its `v0.1.0` release has a Go 1.13 source floor and uses only the standard
library; it does not link the embedded engine, SQLite, libSQL/Turso, or CGO.
The low floor is a compatibility promise, not a recommendation to run an
end-of-life toolchain in production: build with a supported Go release whenever
possible. Use this main module when an application needs embedded mode,
transactional outbox, canonical observation decoding, or managed access-key
administration.

## What the floor freezes

| dependency | pinned | next release | why frozen |
|---|---|---|---|
| `modernc.org/sqlite` | **v1.36.1** | v1.36.2 → `go 1.23` | the pure-Go SQLite engine; **1.36.1 is the last release that builds on go 1.21** — even the next *patch* bumped the floor to 1.23 |
| `golang.org/x/sys` | **v0.30.0** | v0.31.0 → `go 1.23` | transitive (file locking); v0.31.0+ all require go ≥ 1.23 |

There is **no go-1.21-compatible upgrade** for either — the very next version of
each already requires go ≥ 1.23. So Dependabot bumps like `sqlite → 1.52.0` or
`x/sys → 0.46.0` are not "upgrades we're behind on"; they are mutually exclusive
with the go 1.21 floor and fail the `go 1.21.x` CI matrix by construction.

## Remote libSQL driver

[`github.com/tursodatabase/libsql-client-go`](https://github.com/tursodatabase/libsql-client-go),
the pure-Go Hrana client, is pinned even though upstream now marks the repository
deprecated. This is a temporary compatibility choice, not an assertion that an
unmaintained dependency is ideal. The upstream-recommended
[`go-libsql`](https://github.com/tursodatabase/go-libsql) uses CGO and currently
publishes native libraries for only four Linux/macOS targets, while the newer
[`tursogo`](https://github.com/tursodatabase/turso/tree/main/bindings/go) module
requires Go 1.24 and implements a local database plus remote synchronization
rather than MQLite's current direct-primary Hrana path. The official
[`tursogo-serverless`](https://github.com/tursodatabase/turso/tree/main/serverless/go)
module is a pure-Go `database/sql` driver that connects directly to Turso Cloud
over HTTP, but its module also requires Go 1.24 and it has not passed MQLite's
local/remote parity, live lifecycle, sustained-load or six-target release gates.
None currently satisfies the complete Go 1.21, CGO-free, six-target and
storage-semantics contract.

MQLite contains the known HTTP logical-stream lifecycle defect behind a narrow
single-lease adapter: a `libsql://` driver connection is discarded when its
`database/sql` lease ends, while Go's shared HTTP transport continues to reuse
physical TCP/TLS connections. Migration to a maintained driver requires the full
local/remote conformance suite, live Turso lifecycle and sustained-load runs, and
all release-platform builds. Do not swap this dependency or remove the adapter
based only on API similarity; there is no promised replacement or migration date.

## Security posture

Freezing requires checking upstream defects as well as running scanners. CI runs
`govulncheck` on source and uses the GoReleaser release matrix to scan both actual
binaries on all six OS/architecture pairs. Release hooks repeat those binary scans;
Docker scans its actual output too. Artifacts retain symbols so the scanner can
identify linked code instead of falling back to module-level matches. No advisory
is excluded. A reachable vulnerability requires a fix or a deliberate change to
the compatibility floor; the floor does not override security.

For example, [GO-2026-5024](https://pkg.go.dev/vuln/GO-2026-5024) affects an
`x/sys/windows` function that the current MQLite binaries do not call. Both Windows
source analysis and symbol-preserving binary scans confirm that boundary. The
binary gates must continue to pass if a later code/dependency change adds a call.

Go vulnerability scanning does not cover every native SQLite defect. SQLite's
[WAL-reset issue](https://www.sqlite.org/wal.html#the_wal_reset_bug) affects older
SQLite versions when multiple connections concurrently write/checkpoint one WAL
database. MQLite serializes local writes and checkpoints on one SQL connection.
A second local connection serves observation reads with SQLite `mode=ro` and
`query_only`, so it cannot write or checkpoint. Keep external backup connections
read-only and never run another writer/checkpointer against the live file. The
[operations runbook](operations.md#consistent-backups) preserves that restriction.
Review native engine advisories before changing this connection model.

Container OS packages have a separate security boundary from Go dependencies.
The runtime image upgrades installed APKs within the supported Alpine branch
before adding certificates and time-zone data. A supported base tag can still
contain older packages: Alpine 3.24.1 included OpenSSL 3.5.7, while its repository
provides the 3.5.8 patch for [CVE-2026-14456](https://openssl-library.org/news/secadv/20260813.txt).
The existing Docker CI job scans its actual image's OS packages independently
of `govulncheck`, using a checksum-verified Trivy release and a fresh vulnerability
database. HIGH/CRITICAL findings fail the job without advisory exclusions; its
JSON report is retained as an artifact. Update the scanner version and checksum
together after reviewing the official release. CI and release image builds bypass
the runtime stage's cache so the APK upgrade runs even when the base tag is unchanged.
The exact final release candidate image must also pass an OS scan before promotion.
MQLite's static Go binary does not link these system OpenSSL libraries; that does not justify
shipping avoidable vulnerable runtime packages.

## How the freeze is enforced

1. **`.github/dependabot.yml`** ignores `modernc.org/sqlite >=1.36.2` and
   `golang.org/x/sys >=0.31.0`, so Dependabot stops opening PRs that can't merge.
2. **`go 1.21.x` CI matrix** — a dependency (or a `go.mod` edit) that needs a newer
   toolchain fails to build there.
3. **`TestGoModFloorStaysAt121`** (`sdk_test.go`) asserts the floor is exactly
   `go 1.21`, failing with a clear message if it's bumped — so unfreezing the
   dependencies is always a conscious edit, never a side effect.

## `expr-lang/expr` — the topic-filter dependency (MQLITE-17)

Subscription filters are an [`expr-lang/expr`](https://github.com/expr-lang/expr)
boolean predicate (see [concepts.md § filters](concepts.md#subscription-filters-expr)),
a **direct core dependency** pinned at **v1.17.8**. It's a deliberate long-term choice:
maintained by the `expr-lang` org (not a single author), **zero transitive
dependencies**, MIT, and "memory-safe, side-effect-free, always-terminating" by
construction — a sandboxed predicate evaluator. Its `go.mod` floor is only go 1.18, so
it puts no pressure on the freeze above.

**Security.** The one relevant advisory, **CVE-2025-29786** (parser memory exhaustion
from an unbounded AST), is fixed in the v1.17.x line we pin. Defense in depth in
`engine/filter.go`: a source-length cap + `expr.MaxNodes`, plus a fail-closed `recover`
so a hostile or buggy filter can never crash the broker or silently match. (The CVSS-9.8
RCE search engines surface for "expr" is the **JavaScript** `expr-eval` library — a
different project.) All filter use funnels through one wrapper, so an upstream change or
evaluator swap touches one file; `govulncheck` (CI) covers expr at the pin.

## When the freeze lifts

When the project intentionally raises the go floor (e.g. the embedding-compat
requirement is dropped, or a security CVE forces it): bump `go.mod`, update
`TestGoModFloorStaysAt121` and this table, remove the Dependabot ignore rules, and
let the held bumps (`modernc.org/sqlite`, `golang.org/x/sys`) flow in together.
