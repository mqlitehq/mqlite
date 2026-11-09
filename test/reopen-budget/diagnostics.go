package main

import (
	"context"
	"encoding/hex"
	"log/slog"
	"sync"
)

// These are surrounding storage observations, not inferred links to a failure.
// Fingerprints support lookup; equality is not evidence of a shared operation.
type storageEvent struct {
	Event       string `json:"event"`
	Operation   string `json:"operation"`
	Phase       string `json:"transaction_phase"`
	Category    string `json:"category"`
	Fingerprint string `json:"fingerprint,omitempty"`
	HTTPStatus  int64  `json:"http_status"`
	SQLiteCode  int64  `json:"sqlite_code"`
	Retries     uint64 `json:"retries"`
}
type safeLog struct {
	mu      sync.Mutex
	events  []storageEvent
	dropped int
}
type safeHandler struct {
	log   *safeLog
	attrs []slog.Attr
}

func (h safeHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h safeHandler) WithAttrs(a []slog.Attr) slog.Handler {
	return safeHandler{log: h.log, attrs: append(append([]slog.Attr(nil), h.attrs...), a...)}
}
func (h safeHandler) WithGroup(string) slog.Handler { return h }
func inList(s string, list ...string) bool {
	for _, v := range list {
		if s == v {
			return true
		}
	}
	return false
}
func (h safeHandler) Handle(_ context.Context, r slog.Record) error {
	if !inList(r.Message, "storage.failure", "storage.interrupted", "maintenance.failure", "maintenance.interrupted") {
		return nil
	}
	e := storageEvent{Event: r.Message, Operation: "unavailable", Phase: "unavailable", Category: "unclassified"}
	take := func(a slog.Attr) bool {
		switch a.Key {
		case "operation":
			if a.Value.Kind() == slog.KindString && inList(a.Value.String(), "read", "write", "transaction") {
				e.Operation = a.Value.String()
			}
		case "transaction_phase":
			if a.Value.Kind() == slog.KindString && inList(a.Value.String(), "unavailable", "attempt_wait", "admission", "connection", "begin", "body", "commit_wait", "commit_call") {
				e.Phase = a.Value.String()
			}
		case "category":
			if a.Value.Kind() == slog.KindString && inList(a.Value.String(), "unclassified", "outcome_unknown", "transaction_expired", "provider_auth", "provider_rate_limit", "provider_unavailable", "provider_http", "dns", "tls", "timeout", "protocol", "constraint", "readonly", "schema", "canceled", "closed", "busy", "connection", "full", "corrupt", "io", "application") {
				e.Category = a.Value.String()
			}
		case "fingerprint":
			if a.Value.Kind() == slog.KindString && len(a.Value.String()) == 24 {
				if _, err := hex.DecodeString(a.Value.String()); err == nil {
					e.Fingerprint = a.Value.String()
				}
			}
		case "http_status":
			if a.Value.Kind() == slog.KindInt64 {
				v := a.Value.Int64()
				if v >= 100 && v <= 599 {
					e.HTTPStatus = v
				}
			}
		case "sqlite_code":
			if a.Value.Kind() == slog.KindInt64 {
				v := a.Value.Int64()
				if v >= 0 && v <= 1<<31-1 {
					e.SQLiteCode = v
				}
			}
		case "retries":
			if a.Value.Kind() == slog.KindUint64 {
				e.Retries = a.Value.Uint64()
			}
		}
		return true
	}
	for _, a := range h.attrs {
		take(a)
	}
	r.Attrs(take)
	h.log.mu.Lock()
	defer h.log.mu.Unlock()
	if len(h.log.events) >= 64 {
		h.log.dropped++
	} else {
		h.log.events = append(h.log.events, e)
	}
	return nil
}
func (l *safeLog) snapshot() ([]storageEvent, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]storageEvent{}, l.events...), l.dropped
}
