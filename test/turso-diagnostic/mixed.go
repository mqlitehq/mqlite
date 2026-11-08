//go:build mqlite_perf_diag

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mq "github.com/mqlitehq/mqlite"
	"github.com/mqlitehq/mqlite/wire"
)

// Observations mean a complete successful response reached the SDK reader.
// Run joins, onError, identity accounting and final metrics independently check
// behavior; this observer does not claim access to private SDK settlement state.
type observationInterval struct {
	StartMS float64 `json:"start_ms"`
	EndMS   float64 `json:"end_ms"`
}
type queuedWriteEvidence struct {
	BrokerRequestID       uint64  `json:"broker_request_id"`
	SendOperationID       string  `json:"send_operation_id"`
	SendServerReceivedMS  float64 `json:"send_server_received_ms"`
	SendHeadersEndMS      float64 `json:"send_headers_end_ms"`
	RenewalStartMS        float64 `json:"renewal_start_ms"`
	InsertProviderEventID uint64  `json:"insert_provider_event_id"`
	InsertStartedMS       float64 `json:"insert_started_ms"`
	InsertFinishedMS      float64 `json:"insert_finished_ms"`
}
type mixedResult struct {
	LoadProfile                    string                `json:"load_profile"`
	SeedHandlersStarted            int                   `json:"seed_handlers_started"`
	SeedHandlersFinished           int                   `json:"seed_handlers_finished"`
	HandlerContextLost             int                   `json:"handler_context_lost"`
	ReceiverErrorClasses           map[string]int        `json:"receiver_error_classes"`
	ReceiverErrors                 int                   `json:"receiver_errors"`
	ReceiverJoined                 bool                  `json:"receiver_joined"`
	ReceiverReturn                 *failure              `json:"receiver_return,omitempty"`
	ClientObservedCompleteResponse int                   `json:"client_observed_complete_response"`
	RenewalResponses               int                   `json:"renewal_success_responses"`
	NewSendCalls                   int                   `json:"new_send_calls"`
	SendErrors                     int                   `json:"new_send_errors"`
	ReaderSamples                  int                   `json:"reader_samples"`
	ReaderErrors                   int                   `json:"reader_errors"`
	ReaderMaxMS                    float64               `json:"reader_max_ms"`
	WorkersJoined                  bool                  `json:"workers_joined"`
	ProviderWriteCoverageRequired  bool                  `json:"provider_write_coverage_required"`
	ProviderWriteCoverage          string                `json:"provider_write_coverage"`
	ProviderInsertRenewalOverlaps  int                   `json:"provider_insert_renewal_overlaps"`
	QueuedNewSendRenewalMatches    int                   `json:"queued_new_send_renewal_matches"`
	QueuedWriteEvidence            []queuedWriteEvidence `json:"queued_write_evidence"`
	RenewalRPCIntervals            []observationInterval `json:"successful_renewal_rpc_intervals"`
}
type mixedState struct {
	mu     sync.Mutex
	result mixedResult
	book   *identityBook
	queue  string
	origin time.Time
}

func (m *mixedState) snapshot() mixedResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.result
	r.QueuedWriteEvidence = append([]queuedWriteEvidence(nil), m.result.QueuedWriteEvidence...)
	r.RenewalRPCIntervals = append([]observationInterval(nil), m.result.RenewalRPCIntervals...)
	r.ReceiverErrorClasses = map[string]int{}
	for k, v := range m.result.ReceiverErrorClasses {
		r.ReceiverErrorClasses[k] = v
	}
	return r
}

func (m *mixedState) completed(seq int64) {
	m.book.mu.Lock()
	defer m.book.mu.Unlock()
	for _, id := range m.book.ids {
		if id.ObservedSequence == seq && id.Received > 0 && !id.Settled {
			id.Settled = true
			m.book.settled++
			m.book.observedSettled++
			id.SettlementEvidence = "client_observed_complete_response"
			m.mu.Lock()
			m.result.ClientObservedCompleteResponse++
			m.mu.Unlock()
			return
		}
	}
}

// Inspect only GetBody's copy; tokens and bodies are never saved in artifacts.
func (m *mixedState) observe(r *http.Request, resp *http.Response, starts ...time.Time) {
	if resp == nil || resp.Body == nil || resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return
	}
	path := r.URL.Path
	if path != wire.PathComplete && path != wire.PathRenewBatch && path != wire.PathRenew {
		return
	}
	if r.GetBody == nil {
		return
	}
	copy, err := r.GetBody()
	if err != nil {
		return
	}
	defer copy.Close()
	var req struct {
		Queue    string `json:"queue"`
		Sequence int64  `json:"seq_number"`
	}
	if json.NewDecoder(io.LimitReader(copy, 1<<20)).Decode(&req) != nil || req.Queue != m.queue {
		return
	}
	started := time.Now()
	if len(starts) > 0 {
		started = starts[0]
	}
	resp.Body = &observedSDKBody{ReadCloser: resp.Body, done: func(data []byte) {
		if path == wire.PathComplete {
			var v wire.SettleResponse
			if json.Unmarshal(data, &v) == nil && v.Ok && req.Sequence > 0 {
				m.completed(req.Sequence)
			}
			return
		}
		success := false
		if path == wire.PathRenewBatch {
			var v wire.RenewBatchResponse
			if json.Unmarshal(data, &v) == nil && len(v.Results) > 0 {
				success = true
				for _, r := range v.Results {
					if !r.Ok || r.LockedUntilMs <= 0 {
						success = false
					}
				}
			}
		} else {
			var v wire.SettleResponse
			success = json.Unmarshal(data, &v) == nil && v.Ok
		}
		if success {
			m.mu.Lock()
			m.result.RenewalResponses++
			m.result.RenewalRPCIntervals = append(m.result.RenewalRPCIntervals, observationInterval{providerMillis(started.Sub(m.origin)), providerMillis(time.Since(m.origin))})
			m.mu.Unlock()
		}
	}}
}

type observedSDKBody struct {
	io.ReadCloser
	mu                  sync.Mutex
	prefix              []byte
	truncated, finished bool
	done                func([]byte)
}

func (b *observedSDKBody) Read(dst []byte) (int, error) {
	n, err := b.ReadCloser.Read(dst)
	var done func([]byte)
	var data []byte
	b.mu.Lock()
	if !b.finished {
		if !b.truncated && n > 0 {
			if len(b.prefix)+n > 1<<20 {
				b.truncated = true
				b.prefix = nil
			} else {
				b.prefix = append(b.prefix, dst[:n]...)
			}
		}
		if err != nil {
			b.finished = true
			if err == io.EOF && !b.truncated {
				done = b.done
				data = b.prefix
			}
			b.prefix = nil
		}
	}
	b.mu.Unlock()
	if done != nil {
		done(data)
	}
	return n, err
}
func (b *observedSDKBody) Close() error {
	err := b.ReadCloser.Close()
	b.mu.Lock()
	b.finished = true
	b.prefix = nil
	b.mu.Unlock()
	return err
}

// joinManagedReceiver rejects an already-returned Run, even a nil result. A
// successful owner is canceled deliberately and must then return context.Canceled.
func joinManagedReceiver(cancel context.CancelFunc, done <-chan error) (bool, *failure, error) {
	select {
	case err := <-done:
		cancel()
		return true, safeFailure(err), oracle("receiver_early_exit")
	default:
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			return true, safeFailure(err), oracle("receiver_invalid_join_result")
		}
		return true, safeFailure(err), nil
	case <-time.After(joinBudget):
		return false, nil, oracle("receiver_did_not_join")
	}
}

func successfulMessageInsert(e providerHTTPEvent, minimum uint64) bool {
	if e.ID <= minimum || !e.ProtocolSuccess || e.ResponseInspectionTruncated || e.NotSent || !e.Finished || !e.BodyEOF || e.HTTPStatus < 200 || e.HTTPStatus >= 300 || e.ErrorClass != "none" || len(e.SQLiteCodes) != 0 || e.StartedMS <= 0 || e.FinishedMS < e.StartedMS {
		return false
	}
	for _, tag := range e.SQLTags {
		if tag == "message_insert" {
			return true
		}
	}
	return false
}

// SQL overlap remains a secondary timing observation. Priority renewals may
// intentionally run before an already-received Send reaches its INSERT.
func mixedWriteCoverage(events []providerHTTPEvent, minimum uint64, renewals []observationInterval) int {
	overlaps := 0
	for _, e := range events {
		if !successfulMessageInsert(e, minimum) {
			continue
		}
		for _, renew := range renewals {
			if e.StartedMS < renew.EndMS && renew.StartMS < e.FinishedMS {
				overlaps++
				break
			}
		}
	}
	return overlaps
}

// Correlation comes only from the validated loopback request context. A matching
// successful API operation proves this Send returned its ACK; the global exact
// 68-message ledger is checked independently. This does not infer queue order or
// private admission state from SQL timings.
func mixedQueuedWriteCoverage(events []providerHTTPEvent, requests []brokerEvent, minimum uint64, renewals []observationInterval, caseID string, operations []operationSample) []queuedWriteEvidence {
	acknowledged := map[string]bool{}
	prefix := caseID + "-mixed_new_send_b16-"
	for _, op := range operations {
		if op.Phase == "mixed_new_send_b16" && strings.HasPrefix(op.ID, prefix) && op.Failure == nil {
			acknowledged[op.ID] = true
		}
	}
	var evidence []queuedWriteEvidence
	for _, send := range requests {
		if send.BrokerRequestID == 0 || send.Method != "Send" || !send.ServerReceived || !acknowledged[send.OperationID] || send.Status < 200 || send.Status >= 300 || send.Failure != nil || send.ServerReceivedMS <= 0 || send.HeadersEndMS <= send.ServerReceivedMS {
			continue
		}
		found := false
		for _, renew := range renewals {
			if !(send.ServerReceivedMS < renew.StartMS && renew.StartMS < send.HeadersEndMS) || renew.EndMS < renew.StartMS {
				continue
			}
			for _, insert := range events {
				if insert.BrokerRequestID != send.BrokerRequestID || !successfulMessageInsert(insert, minimum) || insert.StartedMS < send.ServerReceivedMS || insert.FinishedMS > send.HeadersEndMS {
					continue
				}
				evidence = append(evidence, queuedWriteEvidence{send.BrokerRequestID, send.OperationID, send.ServerReceivedMS, send.HeadersEndMS, renew.StartMS, insert.ID, insert.StartedMS, insert.FinishedMS})
				found = true
				break
			}
			if found {
				break
			}
		}
	}
	return evidence
}

func runMixed(ctx context.Context, a *adapter, p casePlan, q string, progress *caseProgress, trace *brokerTrace, provider *providerTrace, remote bool) (result *mixedResult, resultErr error) {
	state := &mixedState{book: progress.book, queue: q, origin: provider.origin, result: mixedResult{LoadProfile: "periodic_b16", ReceiverErrorClasses: map[string]int{}, WorkersJoined: true, ProviderWriteCoverageRequired: remote, ProviderWriteCoverage: "not_applicable_local_sqlite"}}
	failed := make(chan struct{})
	var failOnce sync.Once
	fail := func() { failOnce.Do(func() { close(failed) }) }
	trace.mixed = state
	runCtx, stopRun := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	launched := false
	defer func() {
		if launched {
			joined, returned, err := joinManagedReceiver(stopRun, runDone)
			state.mu.Lock()
			state.result.ReceiverJoined = joined
			state.result.ReceiverReturn = returned
			state.mu.Unlock()
			if resultErr == nil && err != nil {
				resultErr = err
			}
		} else {
			stopRun()
		}
		r := state.snapshot()
		result = &r
		if resultErr == nil && (r.ReceiverErrors != 0 || r.HandlerContextLost != 0 || r.SendErrors != 0 || r.ReaderErrors != 0 || !r.ReceiverJoined || !r.WorkersJoined || r.SeedHandlersStarted != 4 || r.SeedHandlersFinished != 4 || r.ClientObservedCompleteResponse != 68 || r.RenewalResponses == 0 || r.ReaderSamples == 0 || r.NewSendCalls != 4 || (remote && r.QueuedNewSendRenewalMatches == 0)) {
			resultErr = oracle("mixed_contract_or_write_coverage")
		}
	}()
	seed := make([]mq.OutMessage, 4)
	for i := range seed {
		seed[i] = inputFor(fmt.Sprintf("%s/%d", q, i), p.Payload)
	}
	progress.book.add(seed)
	var seqs []int64
	err := progress.call(ctx, p.ID, "seed_send", nil, func(call context.Context) error {
		var e error
		seqs, e = a.send(call, q, seed)
		if e == nil {
			e = checkACK(seqs, len(seed))
		}
		return e
	})
	progress.book.sent(seed, seqs, err)
	if err != nil {
		return nil, err
	}
	hold := 6 * time.Second
	if p.LockSeconds == 30 {
		hold = 12 * time.Second
	}
	started := make(chan struct{}, 4)
	seedsFinished := make(chan struct{})
	var finished atomic.Int64
	handler := func(call context.Context, m *delivery) error {
		progress.book.observe([]*delivery{m}, p.Payload)
		isSeed := false
		for _, v := range seed {
			if v.MessageID == m.ID {
				isSeed = true
				break
			}
		}
		delay := 50 * time.Millisecond
		if isSeed {
			delay = hold
			state.mu.Lock()
			state.result.SeedHandlersStarted++
			state.mu.Unlock()
			select {
			case started <- struct{}{}:
			default:
			}
		}
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-call.Done():
			state.mu.Lock()
			state.result.HandlerContextLost++
			state.mu.Unlock()
			fail()
			return call.Err()
		case <-timer.C:
		}
		if isSeed {
			state.mu.Lock()
			state.result.SeedHandlersFinished++
			state.mu.Unlock()
			if finished.Add(1) == 4 {
				close(seedsFinished)
			}
		}
		return nil
	}
	launched = true
	go func() {
		runDone <- a.runReceiver(runCtx, q, handler, func(err error) {
			state.mu.Lock()
			state.result.ReceiverErrors++
			sf := safeFailure(err)
			class := "nil_error"
			if sf != nil {
				class = sf.Class
			}
			state.result.ReceiverErrorClasses[class]++
			state.mu.Unlock()
			fail()
		})
	}()
	early := func(err error) {
		state.mu.Lock()
		state.result.ReceiverJoined = true
		state.result.ReceiverReturn = safeFailure(err)
		state.mu.Unlock()
	}
	for i := 0; i < 4; i++ {
		select {
		case <-started:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-failed:
			return nil, oracle("mixed_operation_failure")
		case err := <-runDone:
			early(err)
			launched = false
			return nil, oracle("receiver_early_exit")
		}
	}
	minimum := provider.Snapshot().Observed
	provider.SetPhase(p.ID + "/mixed_periodic_b16")
	batches := make([][]mq.OutMessage, 4)
	for batch := range batches {
		batches[batch] = make([]mq.OutMessage, 16)
		for i := range batches[batch] {
			batches[batch][i] = inputFor(fmt.Sprintf("%s/%d", q, 4+batch*16+i), p.Payload)
		}
		progress.book.add(batches[batch])
	}
	delays := []time.Duration{750 * time.Millisecond, 1750 * time.Millisecond, 2750 * time.Millisecond, 3750 * time.Millisecond}
	if p.LockSeconds == 30 {
		delays = []time.Duration{2 * time.Second, 5 * time.Second, 8 * time.Second, 10 * time.Second}
	}
	epoch := time.Now()
	workersDone := make(chan struct{})
	var workers sync.WaitGroup
	state.mu.Lock()
	state.result.WorkersJoined = false
	state.mu.Unlock()
	workers.Add(5)
	for batch := range batches {
		batch := batch
		go func() {
			defer workers.Done()
			timer := time.NewTimer(time.Until(epoch.Add(delays[batch])))
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return
			case <-failed:
				return
			case <-timer.C:
			}
			ms := batches[batch]
			var ack []int64
			sendErr := progress.call(ctx, p.ID, "mixed_new_send_b16", nil, func(call context.Context) error {
				var e error
				ack, e = a.send(call, q, ms)
				if e == nil {
					e = checkACK(ack, len(ms))
				}
				return e
			})
			progress.book.sent(ms, ack, sendErr)
			state.mu.Lock()
			state.result.NewSendCalls++
			if sendErr != nil {
				state.result.SendErrors++
			}
			state.mu.Unlock()
			if sendErr != nil {
				fail()
			}
		}()
	}
	go func() {
		defer workers.Done()
		for {
			call, cancel := context.WithTimeout(ctx, 5*time.Second)
			start := time.Now()
			_, readErr := a.stats(call, q)
			cancel()
			elapsed := providerMillis(time.Since(start))
			state.mu.Lock()
			state.result.ReaderSamples++
			if readErr != nil {
				state.result.ReaderErrors++
			}
			if elapsed > state.result.ReaderMaxMS {
				state.result.ReaderMaxMS = elapsed
			}
			state.mu.Unlock()
			if readErr != nil {
				fail()
				return
			}
			select {
			case <-seedsFinished:
				return
			case <-failed:
				return
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	}()
	go func() { workers.Wait(); close(workersDone) }()
	select {
	case <-workersDone:
		state.mu.Lock()
		state.result.WorkersJoined = true
		state.mu.Unlock()
	case <-ctx.Done():
		select {
		case <-workersDone:
			state.mu.Lock()
			state.result.WorkersJoined = true
			state.mu.Unlock()
		case <-time.After(joinBudget):
		}
		return nil, ctx.Err()
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		r := state.snapshot()
		if r.ReceiverErrors != 0 || r.HandlerContextLost != 0 || r.SendErrors != 0 || r.ReaderErrors != 0 {
			return nil, oracle("mixed_operation_failure")
		}
		if r.ClientObservedCompleteResponse == 68 {
			providerEvents := provider.Snapshot().Events
			requests, _ := trace.snapshot()
			overlaps := mixedWriteCoverage(providerEvents, minimum, r.RenewalRPCIntervals)
			evidence := mixedQueuedWriteCoverage(providerEvents, requests, minimum, r.RenewalRPCIntervals, p.ID, progress.operations())
			state.mu.Lock()
			state.result.ProviderInsertRenewalOverlaps = overlaps
			state.result.QueuedNewSendRenewalMatches = len(evidence)
			state.result.QueuedWriteEvidence = evidence
			if remote {
				state.result.ProviderWriteCoverage = "observed"
				if len(evidence) == 0 {
					state.result.ProviderWriteCoverage = "not_observed"
				}
			}
			state.mu.Unlock()
			return nil, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err := <-runDone:
			early(err)
			launched = false
			return nil, oracle("receiver_early_exit")
		case <-ticker.C:
		}
	}
}

var _ io.ReadCloser = (*observedSDKBody)(nil)
