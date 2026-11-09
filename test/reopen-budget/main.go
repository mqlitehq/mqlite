// This finite verifier accepts only the two dedicated diagnostic databases.
// It never runs engine TestMain or resets any schema.
package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	mq "github.com/mqlitehq/mqlite"
	low "github.com/mqlitehq/mqlite-go"
)

var runtimeSHA, sdkSHA, runnerSHA string // supplied by the audited build workflow
const standardDSNHash = "b72bfdad8f3e7696b3d216a2e947705b8efbd6c183e77f44d2cdcb16cef544b3"
const rustDSNHash = "3f0ad22a38621cd9fd112e1e249d97d093ab53b407eba7b897ccfe7c0e3ffe17"

type failure string

func (e failure) Error() string { return string(e) }
func safeClass(err error) string {
	if err == nil {
		return ""
	}
	var own failure
	if errors.As(err, &own) {
		return string(own)
	}
	for _, x := range []struct {
		err   error
		label string
	}{{context.DeadlineExceeded, "deadline"}, {context.Canceled, "canceled"}, {mq.ErrOutcomeUnknown, "outcome_unknown"}, {low.ErrOutcomeUnknown, "outcome_unknown"}, {mq.ErrLockLost, "lock_lost"}, {low.ErrLockLost, "lock_lost"}, {mq.ErrQueueNotFound, "queue_not_found"}, {mq.ErrUnauthenticated, "unauthenticated"}, {mq.ErrPermissionDenied, "permission_denied"}, {mq.ErrClosed, "closed"}} {
		if errors.Is(err, x.err) {
			return x.label
		}
	}
	var n net.Error
	if errors.As(err, &n) {
		if n.Timeout() {
			return "network_timeout"
		}
		return "network"
	}
	return "unclassified"
}
func hash(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }
func validSHA(s string) bool {
	if len(s) != 40 {
		return false
	}
	_, e := hex.DecodeString(s)
	return e == nil
}
func credentials(backend string) (string, string, error) {
	prefix, want := "MQLITE_DIAG_STANDARD", standardDSNHash
	if backend == "rust" {
		prefix, want = "MQLITE_DIAG_RUST", rustDSNHash
	} else if backend != "standard" {
		return "", "", failure("invalid_backend")
	}
	dsn, token := os.Getenv(prefix+"_DB"), os.Getenv(prefix+"_TOKEN")
	if hash(dsn) != want || token == "" {
		return "", "", failure("dedicated_configuration_required")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" || u.Host == "" || (u.Scheme != "turso" && u.Scheme != "libsql") {
		return "", "", failure("invalid_dedicated_configuration")
	}
	return dsn, token, nil
}
func uniqueQueue() (string, error) {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return "", failure("random_identity_unavailable")
	}
	return "m162-budget-" + hex.EncodeToString(b), nil
}

type report struct {
	Schema     string       `json:"schema"`
	Backend    string       `json:"backend"`
	RuntimeSHA string       `json:"runtime_sha"`
	SDKSHA     string       `json:"sdk_sha"`
	RunnerSHA  string       `json:"runner_sha"`
	Go         string       `json:"go"`
	Passed     bool         `json:"passed"`
	Failure    string       `json:"failure,omitempty"`
	Cases      []caseResult `json:"cases"`
}

type ownedQueue struct {
	Queue string `json:"queue"`
	SDK   string `json:"sdk"`
}
type ownershipRecord struct {
	Schema     string       `json:"schema"`
	Backend    string       `json:"backend"`
	RuntimeSHA string       `json:"runtime_sha"`
	SDK_SHA    string       `json:"sdk_sha"`
	RunnerSHA  string       `json:"runner_sha"`
	HostHash   string       `json:"host_hash"`
	PID        int          `json:"pid"`
	Queues     []ownedQueue `json:"queues"`
}

func localHostHash() string { h, _ := os.Hostname(); return hash(h) }
func main()                 { os.Exit(runMain()) }
func runMain() int {
	backend := flag.String("backend", "", "standard or rust")
	mode := flag.String("mode", "check", "check or cleanup")
	confirmed := flag.Bool("owner-exit-confirmed", false, "cleanup only: operator verified the prior process/job ended")
	out := flag.String("out", "", "artifact directory")
	flag.Parse()
	r := report{Schema: "m162-reopen-budget-v1", Backend: *backend, RuntimeSHA: runtimeSHA, SDKSHA: sdkSHA, RunnerSHA: runnerSHA, Go: runtime.Version(), Cases: []caseResult{}}
	if !validSHA(runtimeSHA) || !validSHA(sdkSHA) || !validSHA(runnerSHA) || *out == "" {
		fmt.Println("verification setup failed: immutable source identities and output are required")
		return 2
	}
	dsn, token, err := credentials(*backend)
	if err != nil {
		fmt.Println("verification setup failed:", safeClass(err))
		return 2
	}
	if *mode == "cleanup" {
		return runCleanup(*out, *backend, dsn, token, *confirmed)
	}
	if *mode != "check" {
		fmt.Println("unsupported mode")
		return 2
	}
	if err := reserveOutput(*out); err != nil {
		fmt.Println("verification setup failed: output must be new and writable")
		return 2
	}
	ownership := ownershipRecord{Schema: "m162-owned-queues-v1", Backend: *backend, RuntimeSHA: runtimeSHA, SDK_SHA: sdkSHA, RunnerSHA: runnerSHA, HostHash: localHostHash(), PID: os.Getpid(), Queues: []ownedQueue{}}
	if err := writeJSON(*out, "ownership.json", ownership); err != nil {
		fmt.Println("ownership manifest unavailable")
		return 2
	}
	trace, err := newProviderObserver(dsn)
	if err != nil {
		fmt.Println("verification setup failed:", safeClass(err))
		return 2
	}
	restore := trace.install()
	defer restore()
	cfg := caseConfig{dsn: dsn, token: token, trace: trace, register: func(queue, sdk string) error {
		ownership.Queues = append(ownership.Queues, ownedQueue{queue, sdk})
		return writeJSON(*out, "ownership.json", ownership)
	}}
	for _, sdk := range []string{"native", "lowfloor"} {
		result := runCase(context.Background(), cfg, sdk)
		r.Cases = append(r.Cases, result)
		if err := writeJSON(*out, "result.json", r); err != nil {
			fmt.Println("cannot persist case result")
			return 2
		}
		// An unjoined owner or uncertain cleanup forbids any subsequent owner.
		if !result.OwnersJoined || !result.CleanupAbsent || !result.ProviderJoined {
			break
		}
	}
	r.Passed = len(r.Cases) == 2
	for _, c := range r.Cases {
		r.Passed = r.Passed && c.Passed
	}
	if !r.Passed {
		r.Failure = "finite_reopen_acceptance_failed"
	}
	if err := writeJSON(*out, "result.json", r); err != nil {
		fmt.Println("cannot write result")
		return 2
	}
	fmt.Printf("M162 finite %s verification passed=%t cases=%d\n", strings.TrimSpace(*backend), r.Passed, len(r.Cases))
	if !r.Passed {
		return 1
	}
	return 0
}

func reserveOutput(out string) error {
	if err := os.MkdirAll(filepath.Dir(out), 0700); err != nil {
		return err
	}
	return os.Mkdir(out, 0700)
}
func writeJSON(out, name string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(out, "."+name+".tmp"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, err = f.Write(append(data, '\n')); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(out, name))
}
