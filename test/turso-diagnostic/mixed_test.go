//go:build mqlite_perf_diag

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	mq "github.com/mqlitehq/mqlite"
	"github.com/mqlitehq/mqlite/wire"
)

func TestManagedReceiverCompletionObserverRequiresCompleteSuccessfulBody(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		early      bool
		want       int
	}{
		{"complete", `{"ok":true}`, 200, false, 1},
		{"early_close", `{"ok":true}`, 200, true, 0},
		{"missing_ok", `{}`, 200, false, 0},
		{"negative", `{"ok":false}`, 200, false, 0},
		{"invalid", `{"ok":true} garbage`, 200, false, 0},
		{"server_error", `{"ok":true}`, 503, false, 0},
		{"too_large", `{"ok":true,"x":"` + strings.Repeat("x", 1<<20) + `"}`, 200, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const sentinel = "fake-private-token-canary"
			book := newBook()
			book.ids["owned/0"] = &identity{ID: "owned/0", Status: "confirmed", Sequence: 7, ObservedSequence: 7, Received: 1}
			state := &mixedState{book: book, queue: "owned"}
			request := `{"queue":"owned","seq_number":7,"lock_token":"` + sentinel + `"}`
			req, err := http.NewRequest(http.MethodPost, "http://loopback.invalid"+wire.PathComplete, strings.NewReader(request))
			if err != nil {
				t.Fatal(err)
			}
			resp := &http.Response{StatusCode: tc.status, Body: io.NopCloser(strings.NewReader(tc.body))}
			state.observe(req, resp)
			if !tc.early {
				got, err := io.ReadAll(resp.Body)
				if err != nil || string(got) != tc.body {
					t.Fatal("observer changed response")
				}
			}
			if err := resp.Body.Close(); err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(req.Body)
			if err != nil || string(got) != request {
				t.Fatal("observer consumed request")
			}
			r := state.snapshot()
			if r.ClientObservedCompleteResponse != tc.want {
				t.Fatal("incorrect complete observation")
			}
			a := book.snapshot()
			if a.Settled != tc.want || a.ClientObservedCompleteResponse != tc.want || a.ConfirmedClientSettlementItems != 0 {
				t.Fatal("observer overclaimed private SDK acknowledgement")
			}
			out, _ := json.Marshal(struct {
				Mixed mixedResult `json:"mixed"`
				Book  accounting  `json:"book"`
			}{r, a})
			if bytes.Contains(out, []byte(sentinel)) || bytes.Contains(out, []byte("lock_token")) {
				t.Fatal("token leaked into evidence")
			}
		})
	}
}
func TestManagedReceiverObserverPreservesReadAndCloseErrors(t *testing.T) {
	readErr, closeErr := errors.New("read sentinel"), errors.New("close sentinel")
	body := &providerErrorBody{readErr: readErr, closeErr: closeErr}
	called := false
	wrapped := &observedSDKBody{ReadCloser: body, done: func([]byte) { called = true }}
	_, err := io.ReadAll(wrapped)
	if !errors.Is(err, readErr) {
		t.Fatal("read error changed")
	}
	if !errors.Is(wrapped.Close(), closeErr) || body.closeCalls != 1 || called {
		t.Fatal("close delegation or premature observation")
	}
}
func TestOutcomeMetadataDoesNotFabricateEnginePhaseEvidence(t *testing.T) {
	data, err := json.Marshal(report{Backends: []backendResult{{Cases: []caseResult{{}}}}})
	if err != nil || !bytes.Contains(data, []byte(`"engine_phase_tracing":false`)) {
		t.Fatal("phase collection scope missing")
	}
	for _, absent := range []string{"engine_events", "engine_phase_totals_ms", "engine_observer_dropped"} {
		if bytes.Contains(data, []byte(absent)) {
			t.Fatal("fabricated engine phase collection")
		}
	}
}
func TestProviderInspectionAcceptsBoundedB64BatchWithoutPublishingPayload(t *testing.T) {
	steps := make([]any, 64)
	for i := range steps {
		steps[i] = map[string]any{"stmt": map[string]any{"sql": "INSERT INTO messages(body) VALUES (?) RETURNING seq_number", "args": []any{map[string]any{"type": "text", "value": strings.Repeat("private-canary", 100)}}}}
	}
	body, _ := json.Marshal(map[string]any{"requests": []any{map[string]any{"type": "batch", "batch": map[string]any{"steps": steps}}}})
	if len(body) <= 64<<10 {
		t.Fatal("fixture does not exceed old inspection limit")
	}
	req := providerTestRequest(t, "https://synthetic.invalid", string(body))
	got := inspectProviderRequest(req)
	if got.state != "ok" || got.statements != 64 {
		t.Fatal("valid B64 batch lost provider accounting")
	}
	safe, _ := json.Marshal(got)
	if bytes.Contains(safe, []byte("private-canary")) {
		t.Fatal("payload retained in safe classification")
	}
}
func TestMixedCasesUseBothSDKsShortAndDefaultLeases(t *testing.T) {
	ps := plans("mixed")
	if len(ps) != 4 {
		t.Fatal("mixed acceptance requires four cases")
	}
	for i, p := range ps {
		sdk := "native"
		if i >= 2 {
			sdk = "low-floor"
		}
		lock := 3
		if i%2 != 0 {
			lock = 30
		}
		if p.SDK != sdk || p.LockSeconds != lock || !p.Background || p.Count != 68 || p.Concurrency != 4 || p.Batch != 16 || p.PoolLimit != 0 || p.Admission != 0 {
			t.Fatal("mixed case settings changed")
		}
	}
}
func TestSQLiteManagedReceiverMixedBothSDKs(t *testing.T) {
	verifySQLiteMode(t, "mixed")
}
func TestMixedPreflightRefusesResetBeforeDatabaseAccess(t *testing.T) {
	for _, key := range []string{"MQLITE_TEST_DB", "MQLITE_TEST_DB_RESET_CONFIRM"} {
		_, err := preflight(options{mode: "mixed", caseID: "mixed_native_lock3", selected: "both"}, func(k string) (string, bool) {
			if k == key {
				return "", true
			}
			if strings.HasPrefix(k, "MQLITE_DIAG_") {
				t.Fatal("credentials accessed despite reset env")
			}
			return "", false
		})
		if err == nil || safeFailure(err).Class != "oracle_legacy_reset_environment_refused" {
			t.Fatal("reset guard bypassed")
		}
	}
}

func TestManagedReceiverObserverCloseUnblocksConcurrentRead(t *testing.T) {
	reader, writer := io.Pipe()
	defer writer.Close()
	observed := false
	body := &observedSDKBody{ReadCloser: reader, done: func([]byte) { observed = true }}
	done := make(chan error, 1)
	go func() { var b [1]byte; _, err := body.Read(b[:]); done <- err }()
	if err := body.Close(); err != nil {
		t.Fatal("close changed pipe result")
	}
	if err := <-done; !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal("close failed to unblock reader")
	}
	if observed {
		t.Fatal("incomplete concurrent close counted as acknowledgement")
	}
}

func TestMixedSnapshotOwnsMutableErrorMapAndIntervals(t *testing.T) {
	state := &mixedState{result: mixedResult{ReceiverErrorClasses: map[string]int{"busy": 1}, RenewalRPCIntervals: []observationInterval{{1, 2}}, QueuedWriteEvidence: []queuedWriteEvidence{{BrokerRequestID: 7}}}}
	snapshot := state.snapshot()
	snapshot.ReceiverErrorClasses["busy"] = 99
	snapshot.RenewalRPCIntervals[0].EndMS = 99
	snapshot.QueuedWriteEvidence[0].BrokerRequestID = 99
	if got := state.snapshot(); got.ReceiverErrorClasses["busy"] != 1 || got.RenewalRPCIntervals[0].EndMS != 2 || got.QueuedWriteEvidence[0].BrokerRequestID != 7 {
		t.Fatal("snapshot aliases live mutable state")
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			state.mu.Lock()
			state.result.ReceiverErrorClasses["busy"]++
			state.mu.Unlock()
		}
	}()
	for i := 0; i < 1000; i++ {
		snapshot := state.snapshot()
		snapshot.ReceiverErrorClasses["private_copy"] = i
		if _, err := json.Marshal(snapshot); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	if _, ok := state.snapshot().ReceiverErrorClasses["private_copy"]; ok {
		t.Fatal("snapshot mutation reached live state")
	}
}
func TestReceiveBeforeSendACKStillChecksFinalSequence(t *testing.T) {
	for _, seq := range []int64{1, 2} {
		book := newBook()
		in := []mq.OutMessage{inputFor("late-ack", 256)}
		book.add(in)
		message := fixture("late-ack")
		book.observe([]*delivery{message}, 256)
		book.sent(in, []int64{seq}, nil)
		book.complete([]*delivery{message}, []bool{true}, nil)
		if book.snapshot().valid(1, mq.Metrics{}) != (seq == message.Sequence) {
			t.Fatal("late ACK sequence mismatch escaped integrity check")
		}
	}
}
func TestReceiverJoinRejectsEarlyNilAndRequiresCanceledAfterStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	done <- nil
	joined, _, err := joinManagedReceiver(cancel, done)
	if !joined || err == nil || ctx.Err() == nil {
		t.Fatal("early nil Run return accepted")
	}
	for _, valid := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			<-ctx.Done()
			if valid {
				done <- context.Canceled
			} else {
				done <- nil
			}
		}()
		joined, _, err := joinManagedReceiver(cancel, done)
		if !joined || (err == nil) != valid {
			t.Fatal("Run join result contract not enforced")
		}
	}
}
func TestWriteCoverageRejectsReadOnlyReplaysAndDisjointWrites(t *testing.T) {
	renewal := []observationInterval{{10, 20}}
	successful := func(id uint64, start, end float64, tag string) providerHTTPEvent {
		return providerHTTPEvent{ID: id, StartedMS: start, FinishedMS: end, Finished: true, BodyEOF: true, HTTPStatus: 200, ErrorClass: "none", ProtocolSuccess: true, SQLTags: []string{tag}}
	}
	events := []providerHTTPEvent{successful(1, 11, 19, "message_insert"), successful(2, 11, 19, "dedup_lookup"), successful(3, 1, 10, "message_insert")}
	for i := uint64(4); i < 11; i++ {
		e := successful(i, 11, 19, "message_insert")
		switch i {
		case 4:
			e.NotSent = true
		case 5:
			e.Finished = false
		case 6:
			e.SQLiteCodes = []string{"SQLITE_BUSY"}
		case 7:
			e.HTTPStatus = 503
		case 8:
			e.BodyEOF = false
		case 9:
			e.ProtocolSuccess = false
		case 10:
			e.ResponseInspectionTruncated = true
		}
		events = append(events, e)
	}
	if mixedWriteCoverage(events, 1, renewal) != 0 {
		t.Fatal("failed, unverified, read-only, old, disjoint or unsent work counted as successful write coverage")
	}
	events = append(events, successful(11, 15, 25, "message_insert"))
	if mixedWriteCoverage(events, 1, renewal) != 1 {
		t.Fatal("actual overlapping successful message INSERT not counted")
	}
}

func TestMixedEarlyRemoteFailureReportsCoverageNotEvaluated(t *testing.T) {
	for _, sdk := range []string{"native", "low-floor"} {
		t.Run(sdk, func(t *testing.T) {
			reject := providerRoundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 400, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"code":"invalid_argument","message":"synthetic seed rejection"}`))}, nil
			})
			a, err := openAdapter(context.Background(), sdk, "http://uncontacted.invalid", &http.Client{Transport: reject})
			if err != nil {
				t.Fatal("SDK setup")
			}
			defer a.close()
			provider, err := newProviderTrace("libsql://uncontacted.invalid", reject)
			if err != nil {
				t.Fatal("observer setup")
			}
			var plan casePlan
			for _, p := range plans("mixed") {
				if p.SDK == sdk && p.LockSeconds == 3 {
					plan = p
				}
			}
			result, err := runMixed(context.Background(), a, plan, "synthetic", &caseProgress{book: newBook()}, &brokerTrace{}, provider, true)
			if err == nil || result == nil || !result.ProviderWriteCoverageRequired || result.ProviderWriteCoverage != "not_evaluated" || result.ClientObservedCompleteResponse != 0 {
				t.Fatal("early remote failure mislabeled as local or evaluated coverage")
			}
		})
	}
}
