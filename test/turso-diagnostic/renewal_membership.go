//go:build mqlite_perf_diag

package main

// This is observation, not a renewal implementation. It never consumes a live
// request body, retries SQL, changes a deadline, or attributes a group to a leader.
import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mqlitehq/mqlite/wire"
)

const renewalMembershipKind = "renew_batch_update_returning_v1"

type renewalPosition struct {
	Sequence int64  `json:"seq_number"`
	Pair     string `json:"pair_hash"`
}
type renewalACK struct {
	Sequence int64  `json:"seq_number"`
	Pair     string `json:"pair_hash"`
	OK       bool   `json:"ok"`
	Deadline int64  `json:"deadline_ms"`
}
type renewalRow struct {
	Pair     string `json:"pair_hash"`
	Deadline int64  `json:"deadline_ms"`
}
type logicalRenewal struct {
	RequestValid  bool              `json:"request_valid"`
	Queue         string            `json:"queue_hash"`
	Request       []renewalPosition `json:"request_positions"`
	ResponseValid bool              `json:"response_valid"`
	Response      []renewalACK      `json:"response_positions"`
	EOF           bool              `json:"body_eof"`
	Closed        bool              `json:"body_closed"`
	EOFMS         float64           `json:"eof_ms"`
	CloseMS       float64           `json:"close_ms"`
	CloseError    string            `json:"close_error_class"`
}
type physicalRenewal struct {
	RequestValid bool         `json:"request_valid"`
	Kind         string       `json:"kind"`
	Queue        string       `json:"queue_hash"`
	Pairs        []string     `json:"input_pair_hashes"`
	RowsValid    bool         `json:"rows_valid"`
	Rows         []renewalRow `json:"returned_rows"`
	Rejected     bool         `json:"rejection_observed"`
}
type renewalLink struct {
	BrokerID   uint64   `json:"broker_request_id"`
	ProviderID uint64   `json:"provider_event_id"`
	Pairs      []string `json:"acknowledged_pair_hashes"`
}
type uncoveredRenewal struct {
	ProviderID uint64   `json:"provider_event_id"`
	Pairs      []string `json:"pair_hashes"`
}
type renewalMembership struct {
	Required         bool               `json:"required"`
	Passed           bool               `json:"passed"`
	Status           string             `json:"status"`
	Failure          string             `json:"failure,omitempty"`
	SuccessfulRPCs   int                `json:"successful_rpcs"`
	AcknowledgedRPCs int                `json:"acknowledged_rpcs"`
	ExpectedRPCs     int                `json:"expected_successful_rpcs"`
	MatchedRPCs      int                `json:"matched_rpcs"`
	ManyToOneEvents  int                `json:"many_to_one_events"`
	Links            []renewalLink      `json:"links"`
	Uncovered        []uncoveredRenewal `json:"uncovered_physical_pairs"`
	RejectedEvents   []uint64           `json:"rejected_physical_event_ids"`
}

func renewalPairHash(queue string, sequence int64, token string) string {
	return shaString(fmt.Sprintf("%d:%s/%d/%d:%s", len(queue), queue, sequence, len(token), token))
}
func validRenewalHash(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
func renewalTime(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) && v >= 0 }

// The positional pair hashes distinguish a legitimate duplicate input from a
// response that silently reordered or omitted an item. Tokens are discarded.
func inspectLogicalRenewal(r *http.Request) *logicalRenewal {
	v := &logicalRenewal{CloseError: "none"}
	if r.GetBody == nil {
		return v
	}
	body, err := r.GetBody()
	if err != nil || body == nil {
		return v
	}
	data, err := io.ReadAll(io.LimitReader(body, maxProviderInspectBytes+1))
	closeErr := body.Close()
	if err != nil || closeErr != nil || len(data) > maxProviderInspectBytes {
		return v
	}
	var req wire.RenewBatchRequest
	if json.Unmarshal(data, &req) != nil || req.Queue == "" || len(req.Messages) == 0 || len(req.Messages) > 512 {
		return v
	}
	v.Queue = shaString(req.Queue)
	for _, item := range req.Messages {
		if item.SeqNumber <= 0 {
			return v
		}
		pair := ""
		if item.LockToken != "" {
			pair = renewalPairHash(req.Queue, item.SeqNumber, item.LockToken)
		}
		v.Request = append(v.Request, renewalPosition{item.SeqNumber, pair})
	}
	v.RequestValid = true
	return v
}
func inspectLogicalRenewalACK(data []byte, v *logicalRenewal) {
	var ack wire.RenewBatchResponse
	if !v.RequestValid || json.Unmarshal(data, &ack) != nil || len(ack.Results) != len(v.Request) {
		return
	}
	var positions []renewalACK
	for i, result := range ack.Results {
		position := v.Request[i]
		if result.SeqNumber != position.Sequence || result.Ok && (position.Pair == "" || result.LockedUntilMs <= 0) || !result.Ok && result.LockedUntilMs != 0 {
			return
		}
		positions = append(positions, renewalACK{result.SeqNumber, position.Pair, result.Ok, result.LockedUntilMs})
	}
	v.Response, v.ResponseValid = positions, true
}

// Wrap only after the original mixed observer, preserving its implementation
// and its exact Send/Complete/Renewal counters. Read and Close still delegate.
func (t *brokerTrace) observeRenewal(r *http.Request, resp *http.Response, index int) {
	if r.URL.Path != wire.PathRenewBatch || index < 0 {
		return
	}
	v := inspectLogicalRenewal(r)
	t.mu.Lock()
	t.events[index].Renewal = v
	t.mu.Unlock()
	if resp == nil || resp.Body == nil {
		return
	}
	body := &observedSDKBody{ReadCloser: resp.Body, done: func(data []byte) {
		t.mu.Lock()
		v.EOF, v.EOFMS = true, providerMillis(time.Since(t.origin))
		inspectLogicalRenewalACK(data, v)
		t.mu.Unlock()
	}}
	resp.Body = &renewalLogicalBody{ReadCloser: body, trace: t, observation: v}
}

type renewalLogicalBody struct {
	io.ReadCloser
	trace       *brokerTrace
	observation *logicalRenewal
}

func (b *renewalLogicalBody) Close() error {
	err := b.ReadCloser.Close()
	b.trace.mu.Lock()
	if !b.observation.Closed {
		b.observation.Closed, b.observation.CloseMS = true, providerMillis(time.Since(b.trace.origin))
		b.observation.CloseError = providerErrorClass(err)
	}
	b.trace.mu.Unlock()
	return err
}

type membershipValue struct {
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
}

func membershipInteger(v membershipValue) (int64, bool) {
	var text string
	if v.Type != "integer" || json.Unmarshal(v.Value, &text) != nil {
		return 0, false
	}
	n, err := strconv.ParseInt(text, 10, 64)
	return n, err == nil
}
func membershipString(v membershipValue) (string, bool) {
	var text string
	if v.Type != "text" || json.Unmarshal(v.Value, &text) != nil {
		return "", false
	}
	return text, true
}
func membershipSQL(n int) string {
	return "UPDATE messages SET locked_until=MAX(locked_until, ?) WHERE state='locked' AND locked_until>? AND EXISTS (SELECT 1 FROM queues lq WHERE lq.name=messages.queue AND lq.lifecycle_state='active' AND (lq.expires_at=0 OR lq.expires_at>?)) AND queue=? AND (id, lock_token) IN (VALUES " + strings.TrimSuffix(strings.Repeat("(?,?),", n), ",") + ") RETURNING id, lock_token, locked_until"
}
func compactMembershipSQL(s string) string { return strings.Join(strings.Fields(s), " ") }

// Accept only the exact one-statement Hrana execute used by RenewBatch. A tag,
// a substring, a batch with extra SQL, or a guessed result slot is not proof.
func inspectPhysicalRenewal(r *http.Request) (*physicalRenewal, string) {
	if r.GetBody == nil {
		return nil, ""
	}
	body, err := r.GetBody()
	if err != nil || body == nil {
		return nil, ""
	}
	data, err := io.ReadAll(io.LimitReader(body, maxProviderInspectBytes+1))
	closeErr := body.Close()
	if err != nil || closeErr != nil || len(data) > maxProviderInspectBytes {
		return nil, ""
	}
	var envelope struct {
		Requests []struct {
			Type string `json:"type"`
			Stmt struct {
				SQL      string            `json:"sql"`
				Args     []membershipValue `json:"args"`
				Named    []json.RawMessage `json:"named_args"`
				WantRows bool              `json:"want_rows"`
			} `json:"stmt"`
		} `json:"requests"`
	}
	if json.Unmarshal(data, &envelope) != nil || len(envelope.Requests) != 1 || envelope.Requests[0].Type != "execute" {
		return nil, ""
	}
	stmt := envelope.Requests[0].Stmt
	if len(stmt.Args) < 6 || (len(stmt.Args)-4)%2 != 0 || (len(stmt.Args)-4)/2 > 512 || len(stmt.Named) != 0 || !stmt.WantRows {
		return nil, ""
	}
	if compactMembershipSQL(stmt.SQL) != membershipSQL((len(stmt.Args)-4)/2) {
		return nil, ""
	}
	v := &physicalRenewal{Kind: renewalMembershipKind}
	deadline, deadlineOK := membershipInteger(stmt.Args[0])
	now, nowOK := membershipInteger(stmt.Args[1])
	live, liveOK := membershipInteger(stmt.Args[2])
	queue, queueOK := membershipString(stmt.Args[3])
	if !deadlineOK || deadline <= 0 || !nowOK || now < 0 || !liveOK || live != now || !queueOK || queue == "" {
		return v, ""
	}
	v.Queue = shaString(queue)
	seen := map[string]bool{}
	for i := 4; i < len(stmt.Args); i += 2 {
		seq, seqOK := membershipInteger(stmt.Args[i])
		token, tokenOK := membershipString(stmt.Args[i+1])
		if !seqOK || seq <= 0 || !tokenOK || token == "" {
			return v, ""
		}
		pair := renewalPairHash(queue, seq, token)
		if !seen[pair] {
			v.Pairs = append(v.Pairs, pair)
			seen[pair] = true
		}
	}
	sort.Strings(v.Pairs)
	v.RequestValid = true
	return v, queue
}
func inspectPhysicalRenewalRows(queue string, data []byte, v *physicalRenewal) {
	if !v.RequestValid || queue == "" {
		return
	}
	var envelope struct {
		Results []struct {
			Type     string `json:"type"`
			Response struct {
				Type   string `json:"type"`
				Result struct {
					Cols []struct {
						Name string `json:"name"`
					} `json:"cols"`
					Rows [][]membershipValue `json:"rows"`
				} `json:"result"`
			} `json:"response"`
		} `json:"results"`
	}
	if json.Unmarshal(data, &envelope) != nil || len(envelope.Results) != 1 {
		return
	}
	response := envelope.Results[0]
	if response.Type != "ok" || response.Response.Type != "execute" || len(response.Response.Result.Cols) != 3 || response.Response.Result.Rows == nil {
		return
	}
	for i, name := range []string{"id", "lock_token", "locked_until"} {
		if response.Response.Result.Cols[i].Name != name {
			return
		}
	}
	inputs, seen := map[string]bool{}, map[string]bool{}
	for _, pair := range v.Pairs {
		inputs[pair] = true
	}
	rows := []renewalRow{}
	for _, row := range response.Response.Result.Rows {
		if len(row) != 3 {
			return
		}
		seq, seqOK := membershipInteger(row[0])
		token, tokenOK := membershipString(row[1])
		until, untilOK := membershipInteger(row[2])
		if !seqOK || seq <= 0 || !tokenOK || token == "" || !untilOK || until <= 0 {
			return
		}
		pair := renewalPairHash(queue, seq, token)
		if !inputs[pair] || seen[pair] {
			return
		}
		seen[pair] = true
		rows = append(rows, renewalRow{pair, until})
	}
	v.Rows, v.RowsValid = rows, true
}

// This exact response kind makes the pinned driver return an error, not rows.
// A missing/truncated/unknown success shape is deliberately NOT a rejection.
func inspectRenewalRejection(data []byte) bool {
	var envelope struct {
		Results []struct {
			Type  string          `json:"type"`
			Error json.RawMessage `json:"error"`
		} `json:"results"`
	}
	if json.Unmarshal(data, &envelope) != nil || len(envelope.Results) != 1 || envelope.Results[0].Type != "error" {
		return false
	}
	_, ok := providerObject(envelope.Results[0].Error)
	return ok
}
func completeRejectedRenewal(p providerHTTPEvent) bool {
	r := p.Renewal
	return r != nil && r.RequestValid && r.Kind == renewalMembershipKind && r.Rejected && !r.RowsValid && len(r.Rows) == 0 && p.ID > 0 && p.HTTPStatus == 200 && !p.NotSent && p.Finished && p.BodyEOF && p.BodyClosed && !p.ProtocolSuccess && !p.ResponseInspectionTruncated && p.ErrorClass == "none" && p.CloseErrorClass == "none" && p.StatementCount == 1 && renewalTime(p.StartedMS) && renewalTime(p.BodyEOFMS) && renewalTime(p.FinishedMS) && renewalTime(p.BodyCloseMS) && p.StartedMS <= p.BodyEOFMS && p.BodyEOFMS <= p.FinishedMS && p.FinishedMS <= p.BodyCloseMS
}

func positiveRenewalRPC(e brokerEvent) bool {
	v := e.Renewal
	if v == nil || !v.RequestValid || !v.ResponseValid || !v.EOF || !v.Closed || v.CloseError != "none" || e.BrokerRequestID == 0 || e.Method != "RenewBatch" || e.Status != 200 || !e.ServerReceived || e.Failure != nil || !validRenewalHash(v.Queue) || !renewalTime(e.StartedMS) || !renewalTime(e.ServerReceivedMS) || !renewalTime(v.EOFMS) || !renewalTime(v.CloseMS) || e.StartedMS > e.ServerReceivedMS || e.ServerReceivedMS > v.EOFMS || v.EOFMS > v.CloseMS || len(v.Request) == 0 || len(v.Request) != len(v.Response) {
		return false
	}
	positive := false
	for i, p := range v.Request {
		a := v.Response[i]
		if p.Sequence <= 0 || p.Pair != "" && !validRenewalHash(p.Pair) || a.Sequence != p.Sequence || a.Pair != p.Pair || a.OK && (!validRenewalHash(p.Pair) || a.Deadline <= 0) || !a.OK && a.Deadline != 0 {
			return false
		}
		positive = positive || a.OK
	}
	return positive
}
func allTrueRenewalRPC(e brokerEvent) bool {
	for _, position := range e.Renewal.Response {
		if !position.OK {
			return false
		}
	}
	return true
}
func completePhysicalRenewal(p providerHTTPEvent) bool {
	r := p.Renewal
	if r == nil || !r.RequestValid || !r.RowsValid || r.Rows == nil || r.Rejected || r.Kind != renewalMembershipKind || !validRenewalHash(r.Queue) || len(r.Pairs) == 0 || len(r.Pairs) > 512 || p.ID == 0 || p.HTTPStatus != 200 || p.NotSent || !p.Finished || !p.BodyEOF || !p.BodyClosed || !p.ProtocolSuccess || p.ResponseInspectionTruncated || p.ErrorClass != "none" || p.CloseErrorClass != "none" || len(p.SQLiteCodes) != 0 || p.StatementCount != 1 {
		return false
	}
	if !renewalTime(p.StartedMS) || !renewalTime(p.BodyEOFMS) || !renewalTime(p.FinishedMS) || !renewalTime(p.BodyCloseMS) || p.StartedMS > p.BodyEOFMS || p.BodyEOFMS > p.FinishedMS || p.FinishedMS > p.BodyCloseMS {
		return false
	}
	inputs, rows := map[string]bool{}, map[string]int64{}
	for _, pair := range r.Pairs {
		if !validRenewalHash(pair) || inputs[pair] {
			return false
		}
		inputs[pair] = true
	}
	for _, row := range r.Rows {
		if !inputs[row.Pair] || row.Deadline <= 0 {
			return false
		}
		if _, exists := rows[row.Pair]; exists {
			return false
		}
		rows[row.Pair] = row.Deadline
	}
	return true
}
func physicalRenewalMatches(l brokerEvent, p providerHTTPEvent) bool {
	v, r := l.Renewal, p.Renewal
	if !completePhysicalRenewal(p) || r.Queue != v.Queue || p.BrokerRequestID != 0 && p.BrokerRequestID != l.BrokerRequestID || l.ServerReceivedMS > p.StartedMS || p.BodyCloseMS > v.EOFMS {
		return false
	}
	inputs, rows := map[string]bool{}, map[string]int64{}
	for _, pair := range r.Pairs {
		inputs[pair] = true
	}
	for _, row := range r.Rows {
		rows[row.Pair] = row.Deadline
	}
	for i, request := range v.Request {
		if request.Pair != "" && !inputs[request.Pair] {
			return false
		}
		ack := v.Response[i]
		if ack.OK && rows[request.Pair] != ack.Deadline {
			return false
		}
	}
	return true
}

// Select ONE common physical event for the whole RPC. Per-item matches from
// different SQL statements cannot be stitched together into a successful batch.
func verifyRenewalMembership(remote bool, expected int, requests []brokerEvent, events []providerHTTPEvent, brokerDropped int, providerDropped uint64) renewalMembership {
	out := renewalMembership{Required: remote, Status: "not_applicable_local", Passed: !remote, ExpectedRPCs: expected, Links: []renewalLink{}, Uncovered: []uncoveredRenewal{}, RejectedEvents: []uint64{}}
	if !remote {
		return out
	}
	out.Status, out.Failure = "failed", "renewal_membership_incomplete"
	if expected <= 0 || brokerDropped != 0 || providerDropped != 0 {
		return out
	}
	brokerIDs, eventIDs := map[uint64]bool{}, map[uint64]bool{}
	for _, r := range requests {
		if r.BrokerRequestID == 0 || brokerIDs[r.BrokerRequestID] {
			return out
		}
		brokerIDs[r.BrokerRequestID] = true
	}
	for _, p := range events {
		if p.ID == 0 || eventIDs[p.ID] {
			return out
		}
		eventIDs[p.ID] = true
		if p.Renewal != nil && p.Renewal.Kind == renewalMembershipKind && p.HTTPStatus >= 200 && p.HTTPStatus < 300 {
			if p.HTTPStatus != 200 {
				out.Failure = "ineligible_renewal_sql_http_status"
				return out
			}
			if completeRejectedRenewal(p) {
				out.RejectedEvents = append(out.RejectedEvents, p.ID)
			} else if !completePhysicalRenewal(p) {
				out.Failure = "incomplete_renewal_sql_observation"
				return out
			}
		}
	}
	members, covered := map[uint64]map[uint64]bool{}, map[uint64]map[string]bool{}
	for _, logical := range requests {
		claimsPositiveACK := false
		if logical.Renewal != nil {
			for _, position := range logical.Renewal.Response {
				claimsPositiveACK = claimsPositiveACK || position.OK
			}
		}
		if !positiveRenewalRPC(logical) {
			if claimsPositiveACK {
				out.Failure = "invalid_positive_renewal_observation"
				return out
			}
			continue
		}
		out.AcknowledgedRPCs++
		allTrue := allTrueRenewalRPC(logical)
		if allTrue {
			out.SuccessfulRPCs++
		}
		var candidates []providerHTTPEvent
		for _, p := range events {
			if physicalRenewalMatches(logical, p) {
				candidates = append(candidates, p)
			}
		}
		if len(candidates) != 1 {
			continue
		}
		p := candidates[0]
		pairs := []string{}
		unique := map[string]bool{}
		for _, a := range logical.Renewal.Response {
			if a.OK && !unique[a.Pair] {
				pairs = append(pairs, a.Pair)
				unique[a.Pair] = true
			}
		}
		sort.Strings(pairs)
		out.Links = append(out.Links, renewalLink{logical.BrokerRequestID, p.ID, pairs})
		out.MatchedRPCs++
		if members[p.ID] == nil {
			members[p.ID], covered[p.ID] = map[uint64]bool{}, map[string]bool{}
		}
		if allTrue {
			members[p.ID][logical.BrokerRequestID] = true
		}
		for _, pair := range pairs {
			covered[p.ID][pair] = true
		}
	}
	for _, p := range events {
		if p.Renewal == nil || !p.Renewal.RequestValid {
			continue
		}
		if p.BrokerRequestID == 0 && len(members[p.ID]) >= 2 {
			out.ManyToOneEvents++
		}
		var missing []string
		for _, pair := range p.Renewal.Pairs {
			if !covered[p.ID][pair] {
				missing = append(missing, pair)
			}
		}
		if len(missing) > 0 {
			out.Uncovered = append(out.Uncovered, uncoveredRenewal{p.ID, missing})
		}
	}
	if out.SuccessfulRPCs == expected && out.MatchedRPCs == out.AcknowledgedRPCs {
		out.Passed, out.Status, out.Failure = true, "verified", ""
	}
	return out
}
