# Dependency policy & the go 1.21 floor

MQLite pins its `go.mod` floor at **`go 1.21`** so the SDK stays drop-in
embeddable in projects that haven't moved to a newer toolchain (MQLITE-1). That
single decision freezes two dependencies — this note records why, what it costs,
and how the freeze is enforced so it can't be lifted by accident.

## What the floor freezes

| dependency | pinned | next release | why frozen |
|---|---|---|---|
| `modernc.org/sqlite` | **v1.36.1** | v1.36.2 → `go 1.23` | the pure-Go SQLite engine; **1.36.1 is the last release that builds on go 1.21** — even the next *patch* bumped the floor to 1.23 |
| `golang.org/x/sys` | **v0.30.0** | v0.31.0 → `go 1.23` | transitive (file locking); v0.31.0+ all require go ≥ 1.23 |

There is **no go-1.21-compatible upgrade** for either — the very next version of
each already requires go ≥ 1.23. So Dependabot bumps like `sqlite → 1.52.0` or
`x/sys → 0.46.0` are not "upgrades we're behind on"; they are mutually exclusive
with the go 1.21 floor and fail the `go 1.21.x` CI matrix by construction.

`github.com/tursodatabase/libsql-client-go` (the pure-Go Hrana client) is subject
to the same floor; the CI matrix is the backstop if a future version raises it.

## Security posture

Freezing means we forgo upstream *non-security* fixes from later releases. The
compensating control is **`govulncheck` in CI** (the `govulncheck` job), which
fails the build if a known vulnerability is reachable at the pinned versions. It is
green today. If a CVE ever lands against `modernc.org/sqlite` v1.36.1 specifically,
that forces the decision below — security wins over the embedding floor.

## How the freeze is enforced

1. **`.github/dependabot.yml`** ignores `modernc.org/sqlite >=1.36.2` and
   `golang.org/x/sys >=0.31.0`, so Dependabot stops opening PRs that can't merge.
2. **`go 1.21.x` CI matrix** — a dependency (or a `go.mod` edit) that needs a newer
   toolchain fails to build there.
3. **`TestGoModFloorStaysAt121`** (`go_floor_test.go`) asserts the floor is exactly
   `go 1.21`, failing with a clear message if it's bumped — so unfreezing the
   dependencies is always a conscious edit, never a side effect.

## When the freeze lifts

When the project intentionally raises the go floor (e.g. the embedding-compat
requirement is dropped, or a security CVE forces it): bump `go.mod`, update
`TestGoModFloorStaysAt121` and this table, remove the Dependabot ignore rules, and
let the held bumps (`modernc.org/sqlite`, `golang.org/x/sys`) flow in together.
