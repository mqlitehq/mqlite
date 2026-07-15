// Package crash holds the crash-injection layer (see crash_test.go). The tests are gated behind the
// `crash_injection` build tag: they re-exec the test binary and hard-kill it, which is deliberately
// kept out of the default `go test -race ./...` matrix (and off Windows CI) so the main suite stays
// deterministic. Run them with `make crash`, or `go test -tags crash_injection ./test/crash/`.
//
// This file exists, untagged, so the package is never empty — `go build ./...` and `go vet ./...`
// would otherwise fail with "build constraints exclude all Go files" when the tag is off.
package crash
