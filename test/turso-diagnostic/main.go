//go:build mqlite_perf_diag

// This opt-in command never uses engine TestMain, resets a database, or accepts
// a database URL on the command line. The public artifact schema is allowlisted.
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
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	mq "github.com/mqlitehq/mqlite"
	low "github.com/mqlitehq/mqlite-go"
)

const expectedSDK = "3843e2c50885499e9ee4633d03d56a387e71deaa"

// These constants are deliberately not overridable by dispatch inputs or env.
// They identify only the two maintainer-approved, dedicated diagnostic DBs.
const standardDSNSHA256 = "b72bfdad8f3e7696b3d216a2e947705b8efbd6c183e77f44d2cdcb16cef544b3"
const rustDSNSHA256 = "3f0ad22a38621cd9fd112e1e249d97d093ab53b407eba7b897ccfe7c0e3ffe17"
const callerBudget = 180 * time.Second
const backendBudget = 18 * time.Minute
const caseBudget = 7 * time.Minute
const joinBudget = 10 * time.Second
const cleanupBudget = 60 * time.Second

type oracle string

func (e oracle) Error() string { return string(e) }

type failure struct {
	Class      string `json:"class"`
	HTTPStatus int    `json:"http_status,omitempty"`
}

func safeFailure(err error) *failure {
	if err == nil {
		return nil
	}
	f := &failure{Class: "unclassified"}
	var o oracle
	if errors.As(err, &o) {
		f.Class = "oracle_" + string(o)
		return f
	}
	for _, v := range []struct {
		a, b error
		name string
	}{{mq.ErrOutcomeUnknown, low.ErrOutcomeUnknown, "outcome_unknown"}, {context.DeadlineExceeded, context.DeadlineExceeded, "deadline"}, {context.Canceled, context.Canceled, "canceled"}, {mq.ErrLockLost, low.ErrLockLost, "lock_lost"}, {mq.ErrInvalidArgument, low.ErrInvalidArgument, "invalid_argument"}, {mq.ErrPermissionDenied, low.ErrPermissionDenied, "permission_denied"}, {mq.ErrUnauthenticated, low.ErrUnauthenticated, "unauthenticated"}, {mq.ErrQueueNotFound, low.ErrQueueNotFound, "queue_not_found"}, {mq.ErrDedupConflict, low.ErrDedupConflict, "dedup_conflict"}} {
		if errors.Is(err, v.a) || errors.Is(err, v.b) {
			f.Class = v.name
			break
		}
	}
	var h interface{ HTTPStatus() int }
	if errors.As(err, &h) {
		n := h.HTTPStatus()
		if n >= 100 && n <= 599 {
			f.HTTPStatus = n
			if f.Class == "unclassified" {
				f.Class = "http_response"
			}
		}
	}
	var n net.Error
	if f.Class == "unclassified" && errors.As(err, &n) {
		if n.Timeout() {
			f.Class = "network_timeout"
		} else {
			f.Class = "network"
		}
	}
	return f
}
func mayHaveApplied(err error) bool {
	if err == nil {
		return false
	}
	f := safeFailure(err)
	if f.Class == "outcome_unknown" {
		return true
	}
	for _, p := range []struct {
		class  string
		status int
	}{{"invalid_argument", 400}, {"unauthenticated", 401}, {"permission_denied", 403}, {"queue_not_found", 404}, {"dedup_conflict", 409}} {
		if f.Class == p.class && (f.HTTPStatus == 0 || f.HTTPStatus == p.status) {
			return false
		}
	}
	return true
}

type backend struct {
	Alias      string
	dsn, token string
}
type options struct{ mode, selected, caseID, out string }
type sourceInfo struct {
	SHA             string `json:"source_sha"`
	SDK             string `json:"sdk_sha"`
	Go              string `json:"go"`
	OS              string `json:"os"`
	Arch            string `json:"arch"`
	ProxyConfigured bool   `json:"proxy_configured"`
}
type report struct {
	Schema             string          `json:"schema"`
	Run                string          `json:"run"`
	Mode               string          `json:"mode"`
	CaseSelection      string          `json:"case_selection"`
	Source             sourceInfo      `json:"source"`
	Started            string          `json:"started"`
	Finished           string          `json:"finished,omitempty"`
	Passed             bool            `json:"passed"`
	Blocked            bool            `json:"blocked"`
	Failure            *failure        `json:"failure,omitempty"`
	Backends           []backendResult `json:"backends"`
	EnginePhaseTracing bool            `json:"engine_phase_tracing"`
	Limitations        []string        `json:"limitations"`
}
type ownedQueue struct {
	Backend string `json:"backend"`
	Case    string `json:"case"`
	Queue   string `json:"queue"`
}
type manifest struct {
	Schema        string       `json:"schema"`
	Run           string       `json:"run"`
	CaseSelection string       `json:"case_selection"`
	Queues        []ownedQueue `json:"exact_owned_queues"`
}

func safeID(s string) bool {
	if len(s) == 0 || len(s) > 96 {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' || r == '/') {
			return false
		}
	}
	return true
}
func shaString(s string) string { b := sha256.Sum256([]byte(s)); return hex.EncodeToString(b[:]) }
func hexOfSize(s string, n int) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == n && s == strings.ToLower(s)
}

func approvedRemoteDSN(alias, dsn, want string) bool {
	// Hash the original approved spelling before any transport normalization.
	// The engine already normalizes turso:// to libsql:// in resolveDSN.
	if !hexOfSize(want, 32) || shaString(dsn) != want {
		return false
	}
	u, e := url.Parse(dsn)
	scheme := "libsql"
	if alias == "rust" {
		scheme = "turso"
	} else if alias != "standard" {
		return false
	}
	return e == nil && u.Scheme == scheme && u.Hostname() != "" && u.User == nil && u.RawQuery == "" && u.Fragment == "" && u.Path == ""
}
func preflight(o options, lookup func(string) (string, bool)) ([]backend, error) {
	for _, k := range []string{"MQLITE_TEST_DB", "MQLITE_TEST_DB_RESET_CONFIRM"} {
		if _, set := lookup(k); set {
			return nil, oracle("legacy_reset_environment_refused")
		}
	}
	if o.mode != "quick" && o.mode != "contention" && o.mode != "mixed" {
		return nil, oracle("invalid_mode")
	}
	if _, err := selectedPlans(o.mode, o.caseID); err != nil {
		return nil, err
	}
	if o.selected != "standard" && o.selected != "rust" && o.selected != "both" && o.selected != "local" {
		return nil, oracle("invalid_backend")
	}
	bs := []backend{{Alias: "sqlite"}}
	for _, a := range []string{"standard", "rust"} {
		if o.selected != a && o.selected != "both" {
			continue
		}
		prefix := "MQLITE_DIAG_" + strings.ToUpper(a)
		dsn, _ := lookup(prefix + "_DB")
		token, _ := lookup(prefix + "_TOKEN")
		want := standardDSNSHA256
		if a == "rust" {
			want = rustDSNSHA256
		}
		if !hexOfSize(want, 32) {
			return nil, oracle("test_database_allowlist_unconfigured")
		}
		if dsn == "" || token == "" {
			return nil, oracle("test_credentials_missing")
		}
		if !approvedRemoteDSN(a, dsn, want) {
			return nil, oracle("test_database_not_allowlisted")
		}
		bs = append(bs, backend{Alias: a, dsn: dsn, token: token})
	}
	s, _ := lookup("MQLITE_DIAG_SOURCE_SHA")
	if o.selected != "local" || s != "" {
		if !hexOfSize(s, 20) {
			return nil, oracle("source_identity_required")
		}
	}
	return bs, nil
}
func writeJSON(dir, name string, v any) error {
	if e := os.MkdirAll(dir, 0700); e != nil {
		return oracle("artifact_directory")
	}
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return oracle("artifact_encode")
	}
	tmp := filepath.Join(dir, name+".tmp")
	if e = os.WriteFile(tmp, append(b, '\n'), 0600); e != nil {
		return oracle("artifact_write")
	}
	if e = os.Rename(tmp, filepath.Join(dir, name)); e != nil {
		return oracle("artifact_rename")
	}
	return nil
}

func freshEvidenceDirectory(dir string) bool {
	for _, name := range []string{"manifest.json", "summary.json", "preflight.json", "process.json"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil || !os.IsNotExist(err) {
			return false
		}
	}
	return true
}

func main() { os.Exit(runMain()) }
func runMain() int {
	var o options
	var validateSelectionOnly bool
	flag.StringVar(&o.mode, "mode", "quick", "quick, contention, or mixed")
	flag.StringVar(&o.selected, "backend", "local", "local, standard, rust, or both")
	flag.StringVar(&o.caseID, "case", "all", "all, contention-only originals, or one exact case ID")
	flag.BoolVar(&validateSelectionOnly, "validate-selection-only", false, "check mode/case without credentials, artifacts or database access")
	flag.StringVar(&o.out, "out", "", "public output directory")
	flag.Parse()
	// Disable all unfiltered default logging before even attempting a DB open.
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	if validateSelectionOnly {
		if flag.NArg() != 0 {
			fmt.Println("diagnostic refused: invalid_arguments")
			return 2
		}
		if _, err := selectedPlans(o.mode, o.caseID); err != nil {
			fmt.Println("diagnostic refused: invalid_case_selection")
			return 2
		}
		fmt.Println("diagnostic case selection valid")
		return 0
	}
	if o.out == "" || flag.NArg() != 0 {
		fmt.Println("diagnostic refused: invalid_arguments")
		return 2
	}
	if !freshEvidenceDirectory(o.out) {
		fmt.Println("diagnostic refused: existing_evidence")
		return 2
	}
	bs, err := preflight(o, os.LookupEnv)
	if err != nil {
		_ = writeJSON(o.out, "preflight.json", safeFailure(err))
		fmt.Println("diagnostic refused: preflight")
		return 2
	}
	ps, err := selectedPlans(o.mode, o.caseID)
	if err != nil {
		return 2 // preflight validates this before any DB open
	}
	random := make([]byte, 8)
	if _, err = rand.Read(random); err != nil {
		fmt.Println("diagnostic refused: entropy")
		return 2
	}
	run := hex.EncodeToString(random)
	src := sourceInfo{SHA: os.Getenv("MQLITE_DIAG_SOURCE_SHA"), SDK: expectedSDK, Go: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH}
	for _, k := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"} {
		if os.Getenv(k) != "" {
			src.ProxyConfigured = true
		}
	}
	rep := report{Schema: "turso-performance-attribution-v1", Run: run, Mode: o.mode, CaseSelection: o.caseID, Source: src, Started: time.Now().UTC().Format(time.RFC3339Nano), Limitations: []string{"Finite acceptance; no production capacity, soak or power-loss claim. Only mixed mode exercises managed Receiver behavior.", "Original contention profiles retain send-then-drain, per-worker empty Receive, all errors and strict identity oracle; controls are separate cases.", "Engine phase tracing is disabled. Provider HTTP acquisition is not database/sql pool wait. Only verified inherited broker_request_id values correlate requests; missing context retains phase attribution without inferred transaction identity.", "Synthetic provider delay is separately reported; it is a latency control, not a fault-injection correctness proof.", "No reset, no foreign queue cleanup, no weakening of retries, deadlines, single-writer guard or transaction rules to obtain a pass."}}
	m := manifest{Schema: rep.Schema, Run: run, CaseSelection: o.caseID}
	for _, b := range bs {
		for i, p := range ps {
			m.Queues = append(m.Queues, ownedQueue{b.Alias, p.ID, queueName(run, i, p.SDK)})
		}
	}
	if err = writeJSON(o.out, "manifest.json", m); err != nil {
		fmt.Println("diagnostic failed: artifact_manifest")
		return 1
	}
	if err = writeJSON(o.out, "summary.json", rep); err != nil {
		return 1
	}
	for _, b := range bs {
		br := runBackendSelected(context.Background(), b, o.mode, o.caseID, run, m.Queues)
		rep.Backends = append(rep.Backends, br)
		if !br.Passed {
			rep.Failure = &failure{Class: "backend_failed"}
		}
		if br.Blocked {
			rep.Blocked = true
		}
		if err = writeJSON(o.out, "summary.json", rep); err != nil {
			fmt.Println("diagnostic failed: artifact_write")
			return 1
		}
		if br.Blocked {
			break
		} // A hung owner must never overlap a later owner, even on another backend.
	}
	rep.Passed = rep.Failure == nil && !rep.Blocked && len(rep.Backends) == len(bs)
	rep.Finished = time.Now().UTC().Format(time.RFC3339Nano)
	if err = writeJSON(o.out, "summary.json", rep); err != nil {
		return 1
	}
	fmt.Printf("diagnostic completed: passed=%t blocked=%t backends=%d\n", rep.Passed, rep.Blocked, len(rep.Backends))
	if !rep.Passed {
		return 1
	}
	return 0
}
