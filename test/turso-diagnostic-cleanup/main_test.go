package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mqlitehq/mqlite/engine"
)

func lookup(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) { value, ok := values[key]; return value, ok }
}
func TestPreflightRefusesResetMissingAndUnapprovedTargets(t *testing.T) {
	for _, key := range []string{"MQLITE_TEST_DB", "MQLITE_TEST_DB_RESET_CONFIRM"} {
		_, failure := preflight(lookup(map[string]string{key: ""}))
		if failure == nil || failure.Class != "legacy_reset_environment_refused" {
			t.Fatal("legacy reset was not refused")
		}
	}
	if _, f := preflight(lookup(nil)); f == nil || f.Class != "test_credentials_missing" {
		t.Fatal("missing input was not refused")
	}
	_, f := preflight(lookup(map[string]string{"MQLITE_DIAG_STANDARD_DB": "libsql://unapproved.invalid", "MQLITE_DIAG_STANDARD_TOKEN": "secret-canary"}))
	if f == nil || f.Class != "test_database_not_allowlisted" {
		t.Fatal("unapproved target was not refused")
	}
	data, _ := json.Marshal(f)
	if strings.Contains(string(data), "secret-canary") || strings.Contains(string(data), "unapproved.invalid") {
		t.Fatal("preflight leaked input")
	}
	fixture := "libsql://fixture.invalid"
	sum := sha256.Sum256([]byte(fixture))
	if !matchesDSN(fixture, hex.EncodeToString(sum[:])) || matchesDSN(fixture+"/", hex.EncodeToString(sum[:])) {
		t.Fatal("raw DSN identity changed")
	}
}

func TestFixedTargetAndSourceSanitization(t *testing.T) {
	t.Setenv("MQLITE_DIAG_SOURCE_SHA", "secret-source-canary")
	r := newReport()
	if r.Queue != "diag_5b3a11f4b0594ecd_09_native" || r.PriorActionsRun != 42437949470 || r.PriorRun != "5b3a11f4b0594ecd" || r.PriorCase != "control_pool4_c32_b1" || r.Backend != "standard" {
		t.Fatal("cleanup scope changed")
	}
	data, _ := json.Marshal(r)
	if strings.Contains(string(data), "secret-source-canary") || r.SourceSHA != "unrecorded" {
		t.Fatal("unvalidated source text escaped")
	}
}

type fakeStore struct {
	stats  func(context.Context, string) (engine.Metrics, error)
	remove func(context.Context, string, engine.DeleteOptions) (engine.DeleteResult, error)
	close  func() error
}

func (f *fakeStore) Stats(ctx context.Context, q string) (engine.Metrics, error) {
	return f.stats(ctx, q)
}
func (f *fakeStore) DeleteQueueWithOptions(ctx context.Context, q string, o engine.DeleteOptions) (engine.DeleteResult, error) {
	return f.remove(ctx, q, o)
}
func (f *fakeStore) Close() error {
	if f.close != nil {
		return f.close()
	}
	return nil
}
func testLimits() limits {
	return limits{time.Second, time.Second, 5 * time.Millisecond, time.Millisecond}
}
func fixtureStore(t *testing.T, beforeMissing bool, deleteFailure error) (*fakeStore, *int, *int) {
	t.Helper()
	statsCalls, deleteCalls := 0, 0
	f := &fakeStore{}
	f.stats = func(_ context.Context, q string) (engine.Metrics, error) {
		if q != targetQueue {
			t.Fatal("foreign Stats target")
		}
		statsCalls++
		if statsCalls > 1 || beforeMissing {
			return engine.Metrics{}, engine.ErrQueueNotFound
		}
		return engine.Metrics{Queue: targetQueue, Active: 2, Locked: 4, Total: 6}, nil
	}
	f.remove = func(_ context.Context, q string, o engine.DeleteOptions) (engine.DeleteResult, error) {
		if q != targetQueue || !o.Force {
			t.Fatal("foreign or non-forced deletion")
		}
		deleteCalls++
		if deleteFailure != nil {
			return engine.DeleteResult{}, deleteFailure
		}
		if deleteCalls == 1 {
			return engine.DeleteResult{Deleted: true, State: "deleting"}, nil
		}
		return engine.DeleteResult{Deleted: true, State: "deleted"}, nil
	}
	return f, &statsCalls, &deleteCalls
}
func runFixture(t *testing.T, f *fakeStore, bound limits) report {
	t.Helper()
	return cleanup(context.Background(), credentials{"opaque-fixture", "secret-fixture"}, func(_ context.Context, o engine.Options) (store, error) {
		if !o.DisableBackground || !o.PreserveUnexpiredLeases || o.ReclaimUnexpiredLeases || o.DB != "opaque-fixture" || o.AuthToken != "secret-fixture" {
			t.Fatal("unsafe owner options")
		}
		return f, nil
	}, &trackedTransport{}, bound)
}
func TestCleanupCompletesOnlyExactQueueAndReadsBackAbsence(t *testing.T) {
	f, stats, deletes := fixtureStore(t, false, nil)
	r := runFixture(t, f, testLimits())
	if !r.Passed || r.Blocked || !r.Deleted || !r.AbsentReadback || !r.OwnerReleased || !r.EngineCloseJoined || r.HistoricalRunResultChanged || r.RawErrorPublished || r.ProviderQuiet.HiddenDriverGoroutinesJoined {
		t.Fatalf("cleanup verdict: %+v", r)
	}
	if *stats != 2 || *deletes != 2 || r.Before.Active != 2 || r.Before.Locked != 4 {
		t.Fatal("delete/readback sequence changed")
	}
}
func TestLogicalAbsenceDoesNotSkipPendingPhysicalCleanup(t *testing.T) {
	f, _, deletes := fixtureStore(t, true, nil)
	r := runFixture(t, f, testLimits())
	if !r.Passed || !r.BeforeLogicallyAbsent || !r.Deleted || r.AlreadyAbsent || *deletes != 2 {
		t.Fatal("logical deletion was mistaken for completed cleanup")
	}
}
func TestAlreadyAbsentRequiresDeleteNotFoundAndFreshReadback(t *testing.T) {
	f, stats, deletes := fixtureStore(t, true, engine.ErrQueueNotFound)
	r := runFixture(t, f, testLimits())
	if !r.Passed || !r.AlreadyAbsent || r.Deleted || *stats != 2 || *deletes != 1 {
		t.Fatal("physical absence was not explicitly confirmed")
	}
}
func TestCleanupNeverReplaysErrorOrPublishesRawError(t *testing.T) {
	canary := "libsql://secret-host.invalid/sql?token=private-canary"
	f, stats, deletes := fixtureStore(t, false, errors.New(canary))
	r := runFixture(t, f, testLimits())
	data, err := json.Marshal(r)
	if err != nil || strings.Contains(string(data), "private-canary") || strings.Contains(string(data), "secret-host") || r.Passed || r.Failure == nil || r.Failure.Class != "delete_failed_no_replay" || *deletes != 1 || *stats != 1 {
		t.Fatal("unsafe delete failure handling")
	}
}
func TestInvalidAcknowledgementIsNotCopiedOrAccepted(t *testing.T) {
	f, _, _ := fixtureStore(t, false, nil)
	f.remove = func(context.Context, string, engine.DeleteOptions) (engine.DeleteResult, error) {
		return engine.DeleteResult{Deleted: true, State: "secret-state-canary"}, nil
	}
	r := runFixture(t, f, testLimits())
	data, _ := json.Marshal(r)
	if r.Passed || r.Failure == nil || r.Failure.Class != "delete_ack_invalid" || strings.Contains(string(data), "secret-state-canary") {
		t.Fatal("invalid ACK escaped or passed")
	}
}

func TestOnlySuccessfulPendingAcknowledgementsMayRepeat(t *testing.T) {
	f, _, deletes := fixtureStore(t, false, nil)
	calls := 0
	f.remove = func(context.Context, string, engine.DeleteOptions) (engine.DeleteResult, error) {
		calls++
		return engine.DeleteResult{Deleted: false, State: "deleting"}, nil
	}
	r := runFixture(t, f, testLimits())
	if r.Passed || calls != 1 || *deletes != 0 || r.Failure == nil || r.Failure.Class != "delete_ack_invalid" {
		t.Fatal("unconfirmed acknowledgement was replayed")
	}
	f.remove = func(context.Context, string, engine.DeleteOptions) (engine.DeleteResult, error) {
		calls++
		return engine.DeleteResult{Deleted: true, State: "deleting"}, nil
	}
	calls = 0
	r = runFixture(t, f, testLimits())
	if r.Passed || calls != 64 || r.Failure == nil || r.Failure.Class != "delete_incomplete" {
		t.Fatal("pending cleanup exceeded its finite limit or passed")
	}
}
func TestReadbackFailureKeepsFailure(t *testing.T) {
	f, _, _ := fixtureStore(t, false, nil)
	f.stats = func(context.Context, string) (engine.Metrics, error) {
		return engine.Metrics{Queue: targetQueue, Total: 1}, nil
	}
	r := runFixture(t, f, testLimits())
	if r.Passed || r.Failure == nil || r.Failure.Class != "absence_readback_failed" {
		t.Fatal("nonempty readback passed")
	}
}
func TestCanceledContextCannotIssueDeletion(t *testing.T) {
	f, _, deletes := fixtureStore(t, false, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := cleanup(ctx, credentials{}, func(context.Context, engine.Options) (store, error) { return f, nil }, &trackedTransport{}, testLimits())
	if r.Passed || *deletes != 0 || r.Failure == nil || r.Failure.Class != "cleanup_context_ended" {
		t.Fatal("canceled cleanup issued a deletion")
	}
}
func TestOpenFailureIsSafeAndObservesQuiet(t *testing.T) {
	r := cleanup(context.Background(), credentials{}, func(context.Context, engine.Options) (store, error) { return nil, errors.New("private-open-canary") }, &trackedTransport{}, testLimits())
	data, _ := json.Marshal(r)
	if r.Passed || r.EngineOpened || !r.OwnerReleased || strings.Contains(string(data), "private-open-canary") {
		t.Fatal("open failure escaped or lost quiet observation")
	}
}
func TestUnjoinedCloseBlocksSubsequentOwner(t *testing.T) {
	f, _, _ := fixtureStore(t, false, nil)
	stop := make(chan struct{})
	f.close = func() error { <-stop; return nil }
	bound := testLimits()
	bound.close = 3 * time.Millisecond
	r := runFixture(t, f, bound)
	close(stop)
	if r.Passed || !r.Blocked || r.OwnerReleased || r.EngineCloseJoined || r.Failure == nil || r.Failure.Class != "close_did_not_join" {
		t.Fatal("unjoined Close was accepted")
	}
}
func TestFailedCloseBlocksEvenIfTransportQuiet(t *testing.T) {
	f, _, _ := fixtureStore(t, false, nil)
	f.close = func() error { return errors.New("private-close-canary") }
	r := runFixture(t, f, testLimits())
	data, _ := json.Marshal(r)
	if r.Passed || !r.Blocked || r.OwnerReleased || strings.Contains(string(data), "private-close-canary") {
		t.Fatal("failed Close was accepted")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type spyBody struct {
	io.Reader
	closed atomic.Int64
	err    error
}

func (b *spyBody) Close() error { b.closed.Add(1); return b.err }
func TestTransportPreservesBodyErrorCloseAndGauge(t *testing.T) {
	closeErr := errors.New("close-sentinel")
	body := &spyBody{Reader: bytes.NewBufferString("body-fixture"), err: closeErr}
	request, _ := http.NewRequest(http.MethodPost, "https://fixture.invalid", nil)
	var sameRequest bool
	tracker := &trackedTransport{next: roundTripFunc(func(got *http.Request) (*http.Response, error) {
		sameRequest = got == request
		return &http.Response{StatusCode: 200, Body: body}, nil
	})}
	response, err := tracker.RoundTrip(request)
	if err != nil || !sameRequest || tracker.snapshot() != (gauge{Observed: 1, OpenBodies: 1}) {
		t.Fatal("RoundTrip identity or gauges changed")
	}
	data, err := io.ReadAll(response.Body)
	if err != nil || string(data) != "body-fixture" || tracker.snapshot().OpenBodies != 1 {
		t.Fatal("body contents or EOF semantics changed")
	}
	if !errors.Is(response.Body.Close(), closeErr) || !errors.Is(response.Body.Close(), closeErr) || body.closed.Load() != 2 || tracker.snapshot() != (gauge{Observed: 1}) {
		t.Fatal("Close semantics or accounting changed")
	}
	roundErr := errors.New("roundtrip-sentinel")
	tracker.next = roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, roundErr })
	_, err = tracker.RoundTrip(request)
	if err != roundErr || tracker.snapshot().Active != 0 {
		t.Fatal("underlying error changed")
	}
}
func TestQuietWindowSeesLateTrafficAndRejectsOpenBody(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	calls := 0
	r := waitQuiet(ctx, func() gauge {
		calls++
		if calls < 4 {
			return gauge{}
		}
		return gauge{Observed: 1}
	}, 5*time.Millisecond, time.Millisecond)
	if !r.ObservedQuiet || r.HiddenDriverGoroutinesJoined || calls < 4 || r.Final.Observed != 1 {
		t.Fatal("late traffic was missed")
	}
	ctx2, stop := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer stop()
	r = waitQuiet(ctx2, func() gauge { return gauge{OpenBodies: 1} }, time.Millisecond, time.Millisecond)
	if r.ObservedQuiet {
		t.Fatal("unclosed body was called quiet")
	}
}
func TestExistingFailureEvidenceCannotBeOverwritten(t *testing.T) {
	dir := t.TempDir()
	if err := writeReport(dir, report{Passed: false}); err != nil {
		t.Fatal(err)
	}
	if err := writeReport(dir, report{Passed: true}); !errors.Is(err, os.ErrExist) {
		t.Fatal("prior failure overwritten")
	}
	data, err := os.ReadFile(filepath.Join(dir, "cleanup.json"))
	if err != nil || !strings.Contains(string(data), `"passed": false`) {
		t.Fatal("original failure was not retained")
	}
}
func TestCommandRejectsVariableTargetsBeforeCredentials(t *testing.T) {
	previous := os.Args
	defer func() { os.Args = previous }()
	os.Args = []string{"cleanup", "--out", t.TempDir(), "--queue", "foreign-queue"}
	if code := run(); code != 2 {
		t.Fatal("variable target argument was accepted")
	}
}
