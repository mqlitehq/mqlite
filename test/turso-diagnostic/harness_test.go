//go:build mqlite_perf_diag

package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	mq "github.com/mqlitehq/mqlite"
)

func lookupMap(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}
func TestPreflightRefusesResetAndUnapprovedTargets(t *testing.T) {
	for _, key := range []string{"MQLITE_TEST_DB", "MQLITE_TEST_DB_RESET_CONFIRM"} {
		t.Run(key, func(t *testing.T) {
			_, e := preflight(options{mode: "quick", caseID: "all", selected: "local"}, lookupMap(map[string]string{key: ""}))
			if safeFailure(e).Class != "oracle_legacy_reset_environment_refused" {
				t.Fatal("legacy environment was not refused")
			}
		})
	}
	_, e := preflight(options{mode: "quick", caseID: "all", selected: "standard"}, lookupMap(map[string]string{"MQLITE_DIAG_STANDARD_DB": "libsql://unapproved.invalid", "MQLITE_DIAG_STANDARD_TOKEN": "test-canary"}))
	if e == nil || safeFailure(e).Class != "oracle_test_database_not_allowlisted" {
		t.Fatal("unapproved target was not refused")
	}
	_, e = preflight(options{mode: "quick", caseID: "all", selected: "rust"}, lookupMap(nil))
	if e == nil || safeFailure(e).Class != "oracle_test_credentials_missing" {
		t.Fatal("missing credentials were not refused")
	}
	if _, e = preflight(options{mode: "quick", caseID: "all", selected: "local"}, lookupMap(nil)); e != nil {
		t.Fatal("hermetic preflight failed")
	}
}
func TestOriginalProfilesRemainSeparateFromControls(t *testing.T) {
	ps := plans("contention")
	if len(ps) != 11 {
		t.Fatal("contention case count changed")
	}
	want := [][3]int{{1, 1, 16}, {1, 64, 64}, {8, 1, 32}, {32, 1, 32}}
	for i, p := range ps[:8] {
		w := want[i%4]
		if p.Concurrency != w[0] || p.Batch != w[1] || p.Count != w[2] || p.DedupSeconds != 3600 || !p.Background || p.DelayMS != 0 || p.PoolLimit != 0 || p.Admission != 0 || p.Family != "original_v11_contention" {
			t.Fatal("original profile changed")
		}
		sdk := "native"
		if i >= 4 {
			sdk = "low-floor"
		}
		if p.SDK != sdk {
			t.Fatal("SDK order changed")
		}
	}
	for _, p := range ps[8:] {
		if p.Family != "separate_counterfactual" || !strings.HasPrefix(p.ID, "control_") {
			t.Fatal("counterfactual replaced an original")
		}
	}
	quick := plans("quick")
	if len(quick) != 5 {
		t.Fatal("quick case count changed")
	}
	for _, p := range quick {
		if p.SDK != "native" || p.Background || p.Concurrency != 1 {
			t.Fatal("quick is no longer isolated")
		}
	}
}

func TestCaseSelectionPreservesPlansAndRejectsAmbiguity(t *testing.T) {
	for _, mode := range []string{"quick", "contention", "mixed"} {
		want := plans(mode)
		all, err := selectedPlans(mode, "all")
		if err != nil || !reflect.DeepEqual(all, want) {
			t.Fatal("all selection changed original plans")
		}
		for _, p := range want {
			one, err := selectedPlans(mode, p.ID)
			if err != nil || len(one) != 1 || one[0] != p {
				t.Fatal("exact selection changed the requested plan")
			}
		}
	}
	for _, x := range []struct{ mode, selection string }{
		{"quick", ""}, {"quick", " "}, {"quick", "all "}, {"quick", "ALL"},
		{"quick", "original_native_c32_b1"}, {"contention", "isolated_c1_b1"},
		{"contention", "original_native"}, {"contention", "original_native_c32_b1,control_pool1_c32_b1"},
		{"contention", "unknown_case"}, {"unknown_mode", "all"},
	} {
		if _, err := selectedPlans(x.mode, x.selection); err == nil {
			t.Fatal("unknown, incompatible or ambiguous selection was accepted")
		}
	}
}

func TestPreflightRejectsCaseSelectionBeforeReadingCredentials(t *testing.T) {
	_, err := preflight(options{mode: "quick", selected: "both", caseID: "original_native_c32_b1"}, func(k string) (string, bool) {
		if strings.HasPrefix(k, "MQLITE_DIAG_") {
			t.Fatal("invalid case read credentials before rejection")
		}
		return "", false
	})
	if err == nil || safeFailure(err).Class != "oracle_invalid_case_selection" {
		t.Fatal("incompatible case was not refused during preflight")
	}
}

func TestSQLiteExactCaseRunsOnlyTheSelectedPlan(t *testing.T) {
	const mode = "contention"
	const selection = "original_low-floor_c8_b1"
	const run = "0123456789abcdef"
	ps, err := selectedPlans(mode, selection)
	if err != nil {
		t.Fatal("fixture selection failed")
	}
	q := queueName(run, 0, ps[0].SDK)
	owned := []ownedQueue{{"sqlite", selection, q}}
	m := manifest{Schema: "turso-performance-attribution-v1", Run: run, CaseSelection: selection, Queues: owned}
	encoded, err := json.Marshal(m)
	if err != nil || strings.Contains(string(encoded), "original_native_c8_b1") || len(m.Queues) != 1 {
		t.Fatal("focused manifest includes an unselected case")
	}
	r := runBackendSelected(context.Background(), backend{Alias: "sqlite"}, mode, selection, run, m.Queues)
	if !r.Passed || r.Blocked || len(r.Cases) != 1 {
		t.Fatal("focused SQLite HTTP control did not run exactly one case")
	}
	c := r.Cases[0]
	if c.Plan != ps[0] || c.Queue != q || !c.Workload.valid(c.Plan.Count, c.FinalStats) || !c.Cleanup.Deleted || !c.Cleanup.AbsentReadback {
		t.Fatal("focused plan, manifest, client ACK ledger or cleanup changed")
	}
	reportJSON, err := json.Marshal(report{Mode: mode, CaseSelection: selection, Backends: []backendResult{r}})
	if err != nil || !strings.Contains(string(reportJSON), `"case_selection":"`+selection+`"`) {
		t.Fatal("report does not identify focused scope")
	}
}

func TestSQLiteSelectedPoolControlRemainsNotApplicable(t *testing.T) {
	r := runBackendSelected(context.Background(), backend{Alias: "sqlite"}, "contention", "control_pool1_c32_b1", "0123456789abcdef", nil)
	if !r.Passed || r.Blocked || len(r.Cases) != 1 || r.Cases[0].Status != "not_applicable_local_single_writer" || r.Cases[0].Workload.Attempted != 0 {
		t.Fatal("focused pool control weakened local single-writer guard")
	}
}
func TestApprovedDSNChecksOriginalAliasSpecificScheme(t *testing.T) {
	for _, x := range []struct{ alias, dsn string }{{"standard", "libsql://synthetic-standard.invalid"}, {"rust", "turso://synthetic-rust.invalid"}} {
		if !approvedRemoteDSN(x.alias, x.dsn, shaString(x.dsn)) {
			t.Fatal("approved fixture spelling was refused")
		}
		normalized := "libsql://synthetic-rust.invalid"
		if x.alias == "rust" && approvedRemoteDSN(x.alias, normalized, shaString(x.dsn)) {
			t.Fatal("raw hash was checked after normalization")
		}
	}
	if approvedRemoteDSN("standard", "turso://synthetic.invalid", shaString("turso://synthetic.invalid")) || approvedRemoteDSN("rust", "libsql://synthetic.invalid", shaString("libsql://synthetic.invalid")) {
		t.Fatal("alias scheme was not enforced")
	}
}
func TestLatencyHasNoHundredMillisecondClipping(t *testing.T) {
	r := distribution([]time.Duration{150 * time.Millisecond, time.Second, 4 * time.Second})
	if r.P50MS != 1000 || r.MaxMS != 4000 || r.Samples != 3 || r.MeanMS < 1700 {
		t.Fatal("latency distribution clipped or changed rank")
	}
}
func fixture(id string) *delivery {
	m := inputFor(id, 256)
	return &delivery{Sequence: 1, Body: m.Body, ID: m.MessageID, Group: m.GroupID, Correlation: m.CorrelationID, Reply: m.ReplyTo, Subject: m.Subject, ContentType: m.ContentType, Properties: m.Properties, Count: 1, Token: "synthetic-token", Enqueued: time.Now(), LockedUntil: time.Now().Add(time.Minute)}
}
func TestIdentityOracleNeverForgivesUnknownDuplicateOrCorrupt(t *testing.T) {
	for _, variant := range []string{"valid", "unknown_send", "missing", "duplicate", "corrupt", "wrong_sequence", "settle_unknown", "settle_rejected", "dlq"} {
		t.Run(variant, func(t *testing.T) {
			b := newBook()
			ms := []mq.OutMessage{inputFor("synthetic-identity", 256)}
			b.add(ms)
			if variant == "unknown_send" {
				b.sent(ms, nil, context.DeadlineExceeded)
			} else {
				b.sent(ms, []int64{1}, nil)
			}
			d := fixture(ms[0].MessageID)
			if variant == "corrupt" {
				d.Body = append([]byte(nil), d.Body...)
				d.Body[0] ^= 1
			}
			if variant == "wrong_sequence" {
				d.Sequence = 2
			}
			if variant != "missing" {
				b.observe([]*delivery{d}, 256)
			}
			if variant == "duplicate" {
				b.observe([]*delivery{d}, 256)
			}
			switch variant {
			case "settle_unknown":
				b.complete([]*delivery{d}, nil, errors.New("synthetic unknown"))
			case "settle_rejected":
				b.complete([]*delivery{d}, []bool{false}, nil)
			default:
				b.complete([]*delivery{d}, []bool{true}, nil)
			}
			stats := mq.Metrics{}
			if variant == "dlq" {
				stats.DeadLettered = 1
				stats.Total = 1
			}
			if b.snapshot().valid(1, stats) != (variant == "valid") {
				t.Fatal("integrity verdict incorrect")
			}
		})
	}
}
func TestPublicFailureDoesNotContainProviderText(t *testing.T) {
	canary := "sensitive-provider-canary-credential"
	data, e := json.Marshal(safeFailure(errors.New("libsql://" + canary + "/sql?token=" + canary)))
	if e != nil || strings.Contains(string(data), canary) || strings.Contains(string(data), "libsql") {
		t.Fatal("provider error text escaped into artifact")
	}
	if !mayHaveApplied(context.DeadlineExceeded) || !mayHaveApplied(mq.ErrOutcomeUnknown) || mayHaveApplied(mq.ErrInvalidArgument) {
		t.Fatal("unknown/rejected classification changed")
	}
}
func TestEvidenceDirectoryCannotOverwriteAnEarlierFailure(t *testing.T) {
	dir := t.TempDir()
	if !freshEvidenceDirectory(dir) {
		t.Fatal("new output was refused")
	}
	if e := os.WriteFile(filepath.Join(dir, "summary.json"), []byte(`{"passed":false}`), 0600); e != nil {
		t.Fatal("fixture write failed")
	}
	if freshEvidenceDirectory(dir) {
		t.Fatal("existing failed evidence could be overwritten")
	}
}
func TestCleanupRefusesForeignQueueWithoutDatabaseAccess(t *testing.T) {
	r := cleanupOwned(nil, "standard", "case", "foreign", []ownedQueue{{Backend: "standard", Case: "case", Queue: "owned"}})
	if r.Attempted || r.ExactOwnedQueue || r.Failure == nil {
		t.Fatal("foreign cleanup was accepted")
	}
}
func TestWatchdogBlocksAnUnjoinedOwner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan caseResult, 1)
	progress := &caseProgress{book: newBook()}
	cancel()
	r := awaitCase(ctx, done, progress, casePlan{ID: "synthetic"}, "synthetic_queue", time.Millisecond)
	if !r.Blocked || r.OwnerReleased || r.Passed || r.Status != "blocked_unjoined_owner" {
		t.Fatal("unjoined owner was not blocked")
	}
	done <- caseResult{Passed: true, OwnerReleased: true}
	r = awaitCase(ctx, done, progress, casePlan{}, "", time.Millisecond)
	if !r.Blocked || r.Passed {
		t.Fatal("late success erased an outer timeout")
	}
}
func TestProviderQuietBarrierSeesLateHTTPAndBlocksActiveBodies(t *testing.T) {
	var snapshots int
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r := waitProviderQuiet(ctx, func() providerTraceSnapshot {
		snapshots++
		if snapshots < 3 {
			return providerTraceSnapshot{}
		}
		return providerTraceSnapshot{Observed: 1, Sent: 1, Finished: 1}
	}, 5*time.Millisecond, time.Millisecond)
	if !r.ObservedQuiet || r.HiddenDriverGoroutinesJoined || r.WaitedMS < 5 || snapshots < 3 {
		t.Fatal("quiet barrier missed late HTTP or invented a driver join")
	}
	ctx2, stop := context.WithTimeout(context.Background(), 4*time.Millisecond)
	defer stop()
	r = waitProviderQuiet(ctx2, func() providerTraceSnapshot { return providerTraceSnapshot{OpenBodies: 1} }, time.Millisecond, time.Millisecond)
	if r.ObservedQuiet {
		t.Fatal("open response body was treated as quiet")
	}
}
func TestCallerDeadlineAndOperationContext(t *testing.T) {
	p := &caseProgress{book: newBook()}
	err := p.call(context.Background(), "synthetic_case", "send", nil, func(ctx context.Context) error {
		d, ok := ctx.Deadline()
		if !ok || time.Until(d) > callerBudget || time.Until(d) < callerBudget-time.Second {
			t.Fatal("caller deadline changed")
		}
		if id, _ := ctx.Value(operationKey{}).(string); !safeID(id) {
			t.Fatal("operation identity missing")
		}
		return context.DeadlineExceeded
	})
	if !errors.Is(err, context.DeadlineExceeded) || len(p.operations()) != 1 || p.operations()[0].Failure.Class != "deadline" {
		t.Fatal("caller failure lost")
	}
}
func TestSQLiteQuickThroughRealHTTPAndNativeClientACK(t *testing.T) { verifySQLiteMode(t, "quick") }
func TestSQLiteOriginalProfilesThroughBothSDKs(t *testing.T)        { verifySQLiteMode(t, "contention") }
func verifySQLiteMode(t *testing.T, mode string) {
	// The command has one owner; these controls must not be parallel because they
	// temporarily wrap the process default HTTP transport for provider tracing.
	if _, ok := http.DefaultTransport.(*http.Transport); !ok {
		t.Fatal("unexpected hermetic default transport")
	}
	run := "0123456789abcdef"
	var owned []ownedQueue
	for i, p := range plans(mode) {
		owned = append(owned, ownedQueue{"sqlite", p.ID, queueName(run, i, p.SDK)})
	}
	r := runBackend(context.Background(), backend{Alias: "sqlite"}, mode, run, owned)
	if !r.Passed || r.Blocked {
		for _, c := range r.Cases {
			if !c.Passed && c.Status != "not_applicable_local_single_writer" {
				t.Logf("case=%s status=%s failure_class=%v cleanup_deleted=%t cleanup_absent=%t instrumentation=%t", c.Plan.ID, c.Status, c.Failure, c.Cleanup.Deleted, c.Cleanup.AbsentReadback, c.InstrumentationComplete)
			}
		}
		t.Fatal("SQLite HTTP controls failed")
	}
	for _, c := range r.Cases {
		if c.Status == "not_applicable_local_single_writer" {
			continue
		}
		if !c.Workload.valid(c.Plan.Count, c.FinalStats) || !c.FinalStatsValid || !c.FinalPeekEmpty || !c.OwnerReleased || len(c.BrokerHTTP) == 0 || c.EnginePhaseTracing || c.Provider.Observed != 0 {
			t.Fatal("required accounting or HTTP evidence missing")
		}

	}
}

func TestOriginalsGroupPreservesExactEightProfilesAndOrder(t *testing.T) {
	want := plans("contention")[:8]
	got, err := selectedPlans("contention", "originals")
	if err != nil || len(got) != 8 || !reflect.DeepEqual(got, want) {
		t.Fatal("originals changed the original eight profiles or their order")
	}
	for _, p := range got {
		if p.Family != "original_v11_contention" {
			t.Fatal("group included a counterfactual")
		}
	}
	all, err := selectedPlans("contention", "all")
	if err != nil || len(all) != 11 || !reflect.DeepEqual(all, plans("contention")) {
		t.Fatal("all no longer retains all eleven profiles")
	}
	for _, x := range []struct{ mode, selection string }{{"quick", "originals"}, {"mixed", "originals"}, {"contention", "originals "}, {"contention", "ORIGINALS"}, {"contention", "originals,all"}, {"contention", "controls"}} {
		_, err := preflight(options{mode: x.mode, caseID: x.selection, selected: "both"}, func(k string) (string, bool) {
			if strings.HasPrefix(k, "MQLITE_DIAG_") {
				t.Fatal("invalid group read credentials before selection rejection")
			}
			return "", false
		})
		if err == nil || safeFailure(err).Class != "oracle_invalid_case_selection" {
			t.Fatal("invalid or cross-mode group was accepted")
		}
	}
	// Selection-only preflight never needs remote credentials or a DB owner.
	if _, err := preflight(options{mode: "contention", caseID: "originals", selected: "local"}, lookupMap(nil)); err != nil {
		t.Fatal("originals hermetic preflight failed")
	}
}
