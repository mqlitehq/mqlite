// Package version holds the single version constant every mqlite binary
// reports (mqlite CLI/broker and mqlite-mcp). release.yml verifies it against
// the release tag before building, so a tag whose binaries would self-report a
// different version cannot ship.
package version

// Version is the semantic version of this source tree, without the tag's "v"
// prefix. Bump it in the release PR; the tag must match (CI-enforced — a
// pre-release tag like v0.2.0-rc.1 is checked against its base version).
const Version = "0.2.0"
