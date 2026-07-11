package defaults

import "testing"

// TestDefaultsConsistent pins the three broker defaults to one derivation of BrokerPort,
// so a future edit to any single constant that breaks the relationship fails loudly. The
// CLI and MCP server both consume these; drift here is a runtime footgun.
func TestDefaultsConsistent(t *testing.T) {
	if BrokerPort != "6754" {
		t.Fatalf("BrokerPort = %q, want 6754", BrokerPort)
	}
	if BrokerListenAddr != ":"+BrokerPort {
		t.Errorf("BrokerListenAddr = %q, want %q", BrokerListenAddr, ":"+BrokerPort)
	}
	if BrokerLoopbackEndpoint != "http://127.0.0.1:"+BrokerPort {
		t.Errorf("BrokerLoopbackEndpoint = %q, want %q", BrokerLoopbackEndpoint, "http://127.0.0.1:"+BrokerPort)
	}
}
