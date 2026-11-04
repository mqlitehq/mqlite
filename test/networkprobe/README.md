# Dedicated Turso network diagnostic

This bounded, direct-engine probe separates remote HTTP timing from SDK-to-broker
traffic. It does not reset a database or start a broker. Run it only under the
existing dedicated-verification-DB concurrency lock, with no other engine owner.
The exact database allowlist matches the existing Turso soak tests.

```sh
MQLITE_NETWORK_PROBE_OUTPUT=/path/to/private/report.json \
  go run ./test/networkprobe --opens=2 --call-timeout=90s
```

The caller provides `MQLITE_TEST_DB` and `MQLITE_TEST_DB_AUTH_TOKEN` through the
existing test secret mechanism. Do not print or copy their values. Impose an
outer process deadline as well; the workflow uses a 360-second limit.

The two sequential opens disable background maintenance and preserve unexpired
leases. The report contains each Open/Close duration and fixed SQL categories for
the Hrana requests, plus HTTP connection reuse, DNS/connect/TLS, response-header,
first-byte and full-body timing. HTTP connection wait is not database/sql pool
wait. These are transport categories, not exact internal Engine.Open stage hooks.
No SQL, argument, request/response body, endpoint, header or raw error text is
written. Event storage is bounded; overflow fails the probe.

A unique temporary queue then receives one message under a 120-second lease.
The probe reads a complete, successful HTTP 200 COMMIT response and only then
substitutes `io.ErrUnexpectedEOF` for its terminal EOF. The first Complete must
return `ErrOutcomeUnknown`; an exact retry must succeed through its durable
receipt, with final total and DLQ zero. The tool does not retry the original
unknown transaction closure. It deletes only its own queue; failure cleanup is
reported separately and does not turn a failed assertion into success.

This fault models loss of a successful storage COMMIT acknowledgement. It does
not simulate packet loss, a Turso outage, a crash, or an arbitrary distributed
transaction. A passing short probe neither explains an older unclassified error
nor replaces a long acceptance run.

Local controls:

```sh
go test -race -count=1 -v ./test/networkprobe
go vet ./test/networkprobe
```

They check request preservation and classification privacy, original body/read
errors, close forwarding, successful versus invalid COMMIT envelopes, bounded
events, exact endpoint restrictions, and real HTTP connection reuse.
