// Package defaults holds mqlite's built-in network defaults in one place so the broker
// CLI (cmd/mqlite) and the MCP server (cmd/mqlite-mcp) can never drift apart. Packaging
// files (Dockerfile, compose, fly.toml) and docs cannot import these constants, so their
// consistency with this package is enforced by tests and release review, not the compiler.
package defaults

const (
	// BrokerPort is mqlite's product-specific default TCP port. Mnemonic: "MQLI" on a
	// phone keypad (M=6, Q=7, L=5, I=4). It is a User Port outside the ephemeral range,
	// on the WHATWG Fetch allow-list (the broker serves a browser console), and not a
	// standard port for any surveyed message-queue protocol, database, or their admin UIs.
	BrokerPort = "6754"

	// BrokerListenAddr is the default broker listen address: all interfaces on BrokerPort.
	// Changing the bind host to loopback-only is a separate security decision.
	BrokerListenAddr = ":" + BrokerPort

	// BrokerLoopbackEndpoint is the canonical local client/MCP endpoint. The broker does
	// not terminate TLS, so the direct endpoint is plain http.
	BrokerLoopbackEndpoint = "http://127.0.0.1:" + BrokerPort
)
