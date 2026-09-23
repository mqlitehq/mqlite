package wire

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"

	"github.com/mqlitehq/mqlite/engine"
)

// ObserveRequest samples all queues and runtime measurements without returning
// message bodies, credentials, database paths or administrative configuration.
type ObserveRequest struct{}

// ObserveResponse is the canonical observation shared by the SDK, CLI, console,
// MCP and Prometheus serializer. Access describes this request's visibility; it
// is not a metric. The HTTP domain is not_applicable in pure embedded mode.
type ObserveResponse struct {
	engine.ObservabilitySnapshot
	Version string          `json:"version"`
	Access  string          `json:"access"`
	HTTP    HTTPObservation `json:"http"`
}

// HTTPObservation contains process-lifetime transport measurements. All labels
// are bounded by registered routes and a fixed outcome vocabulary.
type HTTPObservation struct {
	State          string                      `json:"state"`
	Requests       []RequestObservation        `json:"requests"`
	Authentication []AuthenticationObservation `json:"authentication"`
	HandlerLatency []RPCLatencyObservation     `json:"handler_latency"`
}

// RequestObservation counts completed registered RPC requests, including auth
// rejection. DurationSeconds is cumulative elapsed time including authentication.
// Empty, successful Receive calls count as ok, even after a normal long poll.
type RequestObservation struct {
	RPC             string  `json:"rpc"`
	Code            string  `json:"code"`
	Count           uint64  `json:"count"`
	DurationSeconds float64 `json:"duration_seconds"`
}

// AuthenticationObservation counts one final authentication/authorization outcome
// per protected request. No credential, key identifier or client address is a label.
type AuthenticationObservation struct {
	Outcome string `json:"outcome"`
	Count   uint64 `json:"count"`
}

// RPCLatencyObservation preserves the original post-authentication handler timing
// scope. Its finite buckets are cumulative; Count is also the +Inf bucket.
type RPCLatencyObservation struct {
	RPC        string          `json:"rpc"`
	Buckets    []LatencyBucket `json:"buckets"`
	Count      uint64          `json:"count"`
	SumSeconds float64         `json:"sum_seconds"`
}

type LatencyBucket struct {
	UpperBoundSeconds float64 `json:"upper_bound_seconds"`
	Count             uint64  `json:"count"`
}

// MaxObserveResponseBytes bounds the complete inventory before client allocation.
const MaxObserveResponseBytes = 32 << 20

// ReadObserveResponse bounds full-response validation before allocating an inventory.
func ReadObserveResponse(reader io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, MaxObserveResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > MaxObserveResponseBytes {
		return nil, errors.New("observation response exceeds 32 MiB")
	}
	return data, nil
}

// DecodeObserveResponse rejects incomplete observations instead of treating absent
// measurements as healthy zeros. Unknown fields remain forward compatible. Errors
// never include response contents, which may come from an untrusted HTTP peer.
func DecodeObserveResponse(data []byte) (ObserveResponse, error) {
	var out ObserveResponse
	bad := errors.New("invalid observability response")
	if len(data) > MaxObserveResponseBytes || json.Unmarshal(data, &out) != nil {
		return ObserveResponse{}, bad
	}
	if err := requiredObservationFields(data, reflect.TypeOf(out)); err != nil {
		return ObserveResponse{}, fmt.Errorf("%w: %w", bad, err)
	}
	valid := out.Collection.State == "available" || out.Collection.State == "unavailable"
	valid = valid && (out.Access == "manage" || out.Access == "monitor" || out.Access == "anonymous" || out.Access == "embedded")
	valid = valid && (out.Backend == "memory" || out.Backend == "local" || out.Backend == "remote")
	valid = valid && out.Version != "" && nonnegative(out.Collection.DurationSeconds)
	valid = valid && out.QueueCount >= 0 && out.SubscriptionCount >= 0
	valid = valid && out.Runtime.SchemaVersion != "" && nonnegative(out.Runtime.PingSeconds) && out.Runtime.DBSizeBytes >= 0
	if out.Collection.State == "available" {
		valid = valid && out.Queues != nil && out.Collection.ErrorCode == ""
		queues, subscriptions := 0, 0
		seen := make(map[string]bool)
		for _, q := range out.Queues {
			valid = valid && q.Queue != "" && !seen[q.Queue] && (q.Kind == "queue" || q.Kind == "subscription") && q.Active >= 0 && q.Locked >= 0 && q.Deferred >= 0 &&
				q.Scheduled >= 0 && q.DeadLettered >= 0 && q.Unexpected >= 0 && q.OldestMessageAgeMs >= 0 &&
				q.Total == q.Active+q.Locked+q.Deferred+q.Scheduled+q.DeadLettered+q.Unexpected
			seen[q.Queue] = true
			if q.Kind == "subscription" {
				subscriptions++
			} else {
				queues++
			}
		}
		valid = valid && queues == out.QueueCount && subscriptions == out.SubscriptionCount
	} else {
		valid = valid && out.Queues == nil && out.Collection.ErrorCode != ""
	}
	valid = valid && out.Messages != nil && out.Storage.Operations != nil && out.Maintenance != nil && out.Filters != nil
	messageEvents := make(map[string][]string)
	for _, q := range out.Queues {
		messageEvents[q.Queue] = nil
	}
	for _, m := range out.Messages {
		valid = valid && m.Queue != "" && m.Event != ""
		messageEvents[m.Queue] = append(messageEvents[m.Queue], m.Event)
	}
	for _, events := range messageEvents {
		valid = valid && completeDomain(events, engine.MessageEventNames())
	}
	storageDimensions := make(map[engine.StorageDimension]bool)
	for _, o := range out.Storage.Operations {
		dimension := engine.StorageDimension{Operation: o.Operation, Outcome: o.Outcome, ErrorCode: o.ErrorCode}
		valid = valid && o.Operation != "" && o.Outcome != "" && !storageDimensions[dimension] && validHistogram(o.Duration)
		storageDimensions[dimension] = true
	}
	for _, dimension := range engine.StorageDimensions() {
		valid = valid && storageDimensions[dimension]
	}
	tasks := make([]string, 0, len(out.Maintenance))
	for _, m := range out.Maintenance {
		valid = valid && m.Failures <= m.Runs && m.Interrupted <= m.Runs-m.Failures && validHistogram(m.Duration) && m.Duration.Count == m.Runs
		tasks = append(tasks, m.Task)
	}
	valid = valid && completeDomain(tasks, engine.MaintenanceTaskNames())
	stages := make([]string, 0, len(out.Filters))
	for _, f := range out.Filters {
		stages = append(stages, f.Stage)
	}
	valid = valid && completeDomain(stages, []string{"compile", "evaluate"})
	p := out.Storage.Pool
	valid = valid && p.MaxOpen >= 0 && p.Open >= 0 && p.InUse >= 0 && p.Idle >= 0 && p.WaitCount >= 0 && nonnegative(p.WaitDurationSeconds)
	switch out.HTTP.State {
	case "available":
		valid = valid && out.HTTP.Requests != nil && out.HTTP.Authentication != nil && out.HTTP.HandlerLatency != nil
		outcomes := make([]string, 0, len(out.HTTP.Authentication))
		for _, a := range out.HTTP.Authentication {
			outcomes = append(outcomes, a.Outcome)
		}
		valid = valid && completeDomain(outcomes, []string{"success", "missing", "invalid", "expired", "revoked", "permission_denied", "backend_error"})
		type requestDimension struct{ rpc, code string }
		requests := make(map[requestDimension]bool)
		for _, r := range out.HTTP.Requests {
			dimension := requestDimension{r.RPC, r.Code}
			valid = valid && r.RPC != "" && r.Code != "" && !requests[dimension] && nonnegative(r.DurationSeconds)
			requests[dimension] = true
		}
		for _, rpc := range ObservationRPCNames() {
			for _, code := range ObservationRequestCodes() {
				valid = valid && requests[requestDimension{rpc, code}]
			}
		}
		handlers := make([]string, 0, len(out.HTTP.HandlerLatency))
		for _, r := range out.HTTP.HandlerLatency {
			h := engine.DurationHistogram{Count: r.Count, SumSeconds: r.SumSeconds}
			for _, b := range r.Buckets {
				h.BoundsSeconds = append(h.BoundsSeconds, b.UpperBoundSeconds)
				h.BucketCounts = append(h.BucketCounts, b.Count)
			}
			valid = valid && validHistogram(h)
			handlers = append(handlers, r.RPC)
		}
		valid = valid && completeDomain(handlers, nil)
	case "not_applicable":
		valid = valid && len(out.HTTP.Requests) == 0 && len(out.HTTP.Authentication) == 0 && len(out.HTTP.HandlerLatency) == 0
	default:
		valid = false
	}
	if !valid {
		return ObserveResponse{}, bad
	}
	return out, nil
}

// ObservationRPCNames returns the required registered RPC observation vocabulary.
// A server inventory test pins this contract against actual route registration.
func ObservationRPCNames() []string {
	paths := []string{PathSend, PathReceive, PathComplete, PathCompleteBatch, PathRenewBatch,
		PathAbandon, PathReject, PathDefer, PathReceiveDeferred, PathRenew, PathSchedule,
		PathCancel, PathPeek, PathStats, PathCreateQueue, PathSubscribe, PathListQueues,
		PathListSubscriptions, PathTestFilter, PathRedrive, PathPurge, PathStatus, PathObserve,
		PathCreateKey, PathListKeys, PathRevokeKey}
	for i, path := range paths {
		paths[i] = strings.TrimPrefix(path, "/mqlite.v1.")
	}
	return paths
}

// ObservationRequestCodes returns a fresh copy of the bounded whole-request outcomes.
func ObservationRequestCodes() []string {
	return []string{"ok", "unauthenticated", "permission_denied", "key_conflict", "not_found",
		"already_exists", "name_conflict", "group_required", "invalid_argument", "message_too_large",
		"outcome_unknown", "canceled", "internal", "lock_lost", "unimplemented"}
}

// Fixed domains cannot disappear into a healthy empty list. Extra unique names
// remain forward compatible when a future broker adds a task or outcome.
func completeDomain(names, required []string) bool {
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		if name == "" || seen[name] {
			return false
		}
		seen[name] = true
	}
	for _, name := range required {
		if !seen[name] {
			return false
		}
	}
	return true
}

func nonnegative(v float64) bool { return v >= 0 && !math.IsNaN(v) && !math.IsInf(v, 0) }

func validHistogram(h engine.DurationHistogram) bool {
	if len(h.BoundsSeconds) == 0 || len(h.BoundsSeconds) != len(h.BucketCounts) || !nonnegative(h.SumSeconds) {
		return false
	}
	var lastBound float64
	var lastCount uint64
	for i, bound := range h.BoundsSeconds {
		if !nonnegative(bound) || bound <= lastBound || h.BucketCounts[i] < lastCount || h.BucketCounts[i] > h.Count {
			return false
		}
		lastBound, lastCount = bound, h.BucketCounts[i]
	}
	return true
}

// All canonical fields are required (including explicit zeroes). Walking the
// typed schema keeps newly added fields in this completeness check automatically.
func requiredObservationFields(data []byte, typ reflect.Type) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		if typ.Kind() == reflect.Slice {
			return nil
		} // semantic availability checked above
		return errors.New("null measurement")
	}
	switch typ.Kind() {
	case reflect.Struct:
		var object map[string]json.RawMessage
		if json.Unmarshal(data, &object) != nil || object == nil {
			return errors.New("missing observation object")
		}
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			if field.Anonymous {
				if err := requiredObservationFields(data, field.Type); err != nil {
					return err
				}
				continue
			}
			name := strings.Split(field.Tag.Get("json"), ",")[0]
			value, ok := object[name]
			if !ok {
				return fmt.Errorf("missing %s", name)
			}
			if err := requiredObservationFields(value, field.Type); err != nil {
				return err
			}
		}
	case reflect.Slice:
		var items []json.RawMessage
		if json.Unmarshal(data, &items) != nil {
			return errors.New("invalid observation array")
		}
		for _, item := range items {
			if err := requiredObservationFields(item, typ.Elem()); err != nil {
				return err
			}
		}
	}
	return nil
}
