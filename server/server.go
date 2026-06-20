// Package server exposes an mqlite Engine as a Connect-style JSON-over-HTTP
// broker (design §7). Every unary RPC is a plain HTTP POST to
// /mqlite.v1.<Service>/<Method> with a JSON body — curl-able by construction.
package server

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/wire"
)

//go:embed ui.html
var uiHTML []byte

// Server adapts an Engine to HTTP with static Bearer-token auth.
type Server struct {
	eng     *engine.Engine
	tokens  map[string]bool // empty -> auth disabled (dev/LAN only)
	mux     *http.ServeMux
	Version string // reported by the open "/" discovery endpoint; "" -> "dev"
}

// New builds a Server. tokens is the set of accepted Bearer tokens; pass nil/empty
// to disable auth (documented as a localhost/LAN downgrade, §7.5).
func New(eng *engine.Engine, tokens []string) *Server {
	s := &Server{eng: eng, tokens: map[string]bool{}, mux: http.NewServeMux()}
	for _, t := range tokens {
		if t = strings.TrimSpace(t); t != "" {
			s.tokens[t] = true
		}
	}
	s.routes()
	return s
}

// Handler returns the auth-wrapped HTTP handler.
func (s *Server) Handler() http.Handler { return s.auth(s.mux) }

func (s *Server) routes() {
	h := func(path string, fn http.HandlerFunc) { s.mux.HandleFunc(path, postOnly(fn)) }
	h(wire.PathSend, s.handleSend)
	h(wire.PathReceive, s.handleReceive)
	h(wire.PathComplete, s.handleComplete)
	h(wire.PathAbandon, s.handleAbandon)
	h(wire.PathReject, s.handleReject)
	h(wire.PathDefer, s.handleDefer)
	h(wire.PathReceiveDeferred, s.handleReceiveDeferred)
	h(wire.PathRenew, s.handleRenew)
	h(wire.PathSchedule, s.handleSchedule)
	h(wire.PathCancel, s.handleCancel)
	h(wire.PathPeek, s.handlePeek)
	h(wire.PathStats, s.handleStats)
	h(wire.PathCreateQueue, s.handleCreateQueue)
	h(wire.PathSubscribe, s.handleSubscribe)
	h(wire.PathListQueues, s.handleListQueues)
	h(wire.PathRedrive, s.handleRedrive)
	h(wire.PathPurge, s.handlePurge)
	s.mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	// Open discovery/index at "/" — hit the broker with no path/params/auth and get a
	// plain JSON telling you what this is, the version, and a basic status.
	s.mux.HandleFunc("/", s.handleIndex)
	// Prometheus metrics: per-queue gauges. Behind auth like the RPCs (a scraper
	// passes the Bearer token); only /healthz stays open for liveness.
	s.mux.HandleFunc("/metrics", s.handleMetrics)
	// read-only ops dashboard (static page; data still goes through the authed API).
	s.mux.HandleFunc("/ui", s.handleUI)
	s.mux.HandleFunc("/ui/", s.handleUI)
}

func (s *Server) handleUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(uiHTML)
}

// handleIndex is the open discovery endpoint at "/": no params, no auth, just a JSON
// card identifying the system, its version, and a basic status. "/" is also the mux
// catch-all, so anything else unmatched gets a JSON 404 here.
func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		writeErr(w, http.StatusNotFound, "not_found", "no such path: "+r.URL.Path)
		return
	}
	version := s.Version
	if version == "" {
		version = "dev"
	}
	writeJSON(w, map[string]any{
		"name":        "mqlite",
		"description": "Lightweight SQLite-backed message queue with Azure Service Bus-style semantics.",
		"version":     version,
		"status":      "ok",
		"auth":        len(s.tokens) > 0, // true -> RPCs need a Bearer token
		"docs":        "https://github.com/mqlitehq/mqlite",
		"endpoints": map[string]string{
			"health":  "GET /healthz",
			"metrics": "GET /metrics (Bearer)",
			"ui":      "GET /ui",
			"rpc":     "POST /mqlite.v1.{Service}/{Method} (Bearer)",
		},
	})
}

// handleMetrics exposes per-queue counters in Prometheus text format (MQLITE-5).
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	queues, err := s.eng.ListQueues(r.Context())
	if err != nil {
		http.Error(w, "metrics: "+err.Error(), http.StatusInternalServerError)
		return
	}
	type qm struct {
		name string
		m    engine.Metrics
	}
	stats := make([]qm, 0, len(queues))
	for _, q := range queues {
		m, err := s.eng.Stats(r.Context(), q.Name)
		if err != nil {
			http.Error(w, "metrics: "+err.Error(), http.StatusInternalServerError)
			return
		}
		stats = append(stats, qm{q.Name, m})
	}

	var b strings.Builder
	b.WriteString("# HELP mqlite_queue_messages Messages in a queue by state.\n")
	b.WriteString("# TYPE mqlite_queue_messages gauge\n")
	for _, st := range stats {
		for _, sv := range []struct {
			state string
			n     int64
		}{
			{"active", st.m.Active}, {"locked", st.m.Locked}, {"deferred", st.m.Deferred},
			{"scheduled", st.m.Scheduled}, {"dead_lettered", st.m.DeadLettered},
		} {
			fmt.Fprintf(&b, "mqlite_queue_messages{queue=%q,state=%q} %d\n", st.name, sv.state, sv.n)
		}
	}
	b.WriteString("# HELP mqlite_queue_total Total messages in a queue.\n")
	b.WriteString("# TYPE mqlite_queue_total gauge\n")
	for _, st := range stats {
		fmt.Fprintf(&b, "mqlite_queue_total{queue=%q} %d\n", st.name, st.m.Total)
	}
	b.WriteString("# HELP mqlite_queue_oldest_message_age_ms Age of the oldest message in a queue, in milliseconds.\n")
	b.WriteString("# TYPE mqlite_queue_oldest_message_age_ms gauge\n")
	for _, st := range stats {
		fmt.Fprintf(&b, "mqlite_queue_oldest_message_age_ms{queue=%q} %d\n", st.name, st.m.OldestMessageAgeMs)
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write([]byte(b.String()))
}

func postOnly(fn http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "unimplemented", "POST required")
			return
		}
		fn(w, r)
	}
}

// auth enforces Bearer tokens (skips /healthz and when no tokens configured).
func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(s.tokens) == 0 || r.URL.Path == "/" || r.URL.Path == "/healthz" || strings.HasPrefix(r.URL.Path, "/ui") {
			next.ServeHTTP(w, r)
			return
		}
		tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if tok == "" || !s.tokens[tok] {
			writeErr(w, http.StatusUnauthorized, "unauthenticated", "missing or invalid Bearer token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ── helpers ─────────────────────────────────────────────────────────────────

func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	return dec.Decode(v)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(wire.ErrorBody{Code: code, Message: msg})
}

// mapErr translates engine errors to a Connect-style (status, code) error.
func (s *Server) fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, engine.ErrQueueNotFound), errors.Is(err, engine.ErrNotFound):
		writeErr(w, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, engine.ErrDedupConflict):
		writeErr(w, http.StatusConflict, "already_exists", err.Error())
	case errors.Is(err, engine.ErrNameConflict):
		writeErr(w, http.StatusConflict, "name_conflict", err.Error())
	case errors.Is(err, engine.ErrGroupRequired):
		writeErr(w, http.StatusBadRequest, "group_required", err.Error())
	case errors.Is(err, engine.ErrMessageTooLarge):
		writeErr(w, http.StatusRequestEntityTooLarge, "message_too_large", err.Error())
	case errors.Is(err, context.Canceled):
		writeErr(w, 499, "canceled", err.Error())
	default:
		writeErr(w, http.StatusInternalServerError, "internal", err.Error())
	}
}

// ── QueueService handlers ───────────────────────────────────────────────────

func (s *Server) handleSend(w http.ResponseWriter, r *http.Request) {
	var req wire.SendRequest
	if err := decode(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return
	}
	ctx := r.Context()
	outs := make([]engine.OutMessage, len(req.Messages))
	for i, m := range req.Messages {
		o := m.ToOut()
		if req.TTLMs > 0 {
			o.TTLMs = req.TTLMs
		}
		outs[i] = o
	}
	var seqs []int64
	var err error
	if req.ScheduledEnqueueTimeMs > 0 {
		seqs = make([]int64, len(outs))
		for i, o := range outs {
			seqs[i], err = s.eng.Schedule(ctx, req.Queue, o, req.ScheduledEnqueueTimeMs)
			if err != nil {
				s.fail(w, err)
				return
			}
		}
	} else {
		seqs, err = s.eng.Send(ctx, req.Queue, outs...)
		if err != nil {
			s.fail(w, err)
			return
		}
		// A single Send that hit a dedup conflict (same id, different body) comes
		// back as seq 0 — the batch path skips the offending slot to keep the rest
		// of the batch. Surface it as 409, like engine.SendOne, so an HTTP client
		// isn't handed 200 with a bogus seq 0 for a message that was never enqueued.
		if len(req.Messages) == 1 && len(seqs) == 1 && seqs[0] == 0 {
			s.fail(w, engine.ErrDedupConflict)
			return
		}
	}
	writeJSON(w, wire.SendResponse{SeqNumbers: seqs})
}

func (s *Server) handleSchedule(w http.ResponseWriter, r *http.Request) { s.handleSend(w, r) }

func (s *Server) handleReceive(w http.ResponseWriter, r *http.Request) {
	var req wire.ReceiveRequest
	if err := decode(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return
	}
	msgs, err := s.eng.Receive(r.Context(), req.Queue, engine.ReceiveOptions{
		MaxMessages: req.MaxMessages,
		WaitMs:      req.WaitTimeMs,
		Mode:        engine.ReceiveMode(req.ReceiveMode),
		AttemptID:   req.AttemptID,
	})
	if err != nil {
		s.fail(w, err)
		return
	}
	resp := wire.ReceiveResponse{Messages: make([]wire.Message, len(msgs))}
	for i, m := range msgs {
		resp.Messages[i] = wire.FromEngineMessage(m)
	}
	writeJSON(w, resp)
}

func (s *Server) handleReceiveDeferred(w http.ResponseWriter, r *http.Request) {
	var req wire.ReceiveDeferredRequest
	if err := decode(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return
	}
	msgs, err := s.eng.ReceiveDeferred(r.Context(), req.Queue, req.SeqNumbers...)
	if err != nil {
		s.fail(w, err)
		return
	}
	resp := wire.ReceiveResponse{Messages: make([]wire.Message, len(msgs))}
	for i, m := range msgs {
		resp.Messages[i] = wire.FromEngineMessage(m)
	}
	writeJSON(w, resp)
}

// settleOK runs a settle action. A lost/expired lock is a distinct, typed error
// (HTTP 409 "lock_lost") — not a 200 with {ok:false}, which a status-only client
// would mistake for success. Idempotent replays of an already-settled token
// return success (the engine consults the settlement-receipt table).
func (s *Server) settleOK(w http.ResponseWriter, err error) {
	if err == nil {
		writeJSON(w, wire.SettleResponse{Ok: true})
		return
	}
	if errors.Is(err, engine.ErrLockLost) {
		writeErr(w, http.StatusConflict, "lock_lost", err.Error())
		return
	}
	s.fail(w, err)
}

func (s *Server) handleComplete(w http.ResponseWriter, r *http.Request) {
	var req wire.SettleRequest
	if err := decode(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return
	}
	s.settleOK(w, s.eng.Complete(r.Context(), req.Queue, req.SeqNumber, req.LockToken))
}

func (s *Server) handleAbandon(w http.ResponseWriter, r *http.Request) {
	var req wire.SettleRequest
	if err := decode(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return
	}
	s.settleOK(w, s.eng.Abandon(r.Context(), req.Queue, req.SeqNumber, req.LockToken, req.DelayMs))
}

func (s *Server) handleReject(w http.ResponseWriter, r *http.Request) {
	var req wire.SettleRequest
	if err := decode(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return
	}
	s.settleOK(w, s.eng.Reject(r.Context(), req.Queue, req.SeqNumber, req.LockToken,
		req.DeadLetterReason, req.DeadLetterDescription))
}

func (s *Server) handleDefer(w http.ResponseWriter, r *http.Request) {
	var req wire.SettleRequest
	if err := decode(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return
	}
	s.settleOK(w, s.eng.Defer(r.Context(), req.Queue, req.SeqNumber, req.LockToken))
}

func (s *Server) handleRenew(w http.ResponseWriter, r *http.Request) {
	var req wire.SettleRequest
	if err := decode(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return
	}
	s.settleOK(w, s.eng.Renew(r.Context(), req.Queue, req.SeqNumber, req.LockToken))
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	var req wire.CancelRequest
	if err := decode(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return
	}
	if err := s.eng.Cancel(r.Context(), req.Queue, req.SeqNumber); err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, wire.SettleResponse{Ok: true})
}

func (s *Server) handlePeek(w http.ResponseWriter, r *http.Request) {
	var req wire.PeekRequest
	if err := decode(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return
	}
	ms, err := s.eng.Peek(r.Context(), req.Queue, engine.PeekOptions{
		FromSeq: req.FromSeq, State: engine.State(req.State), Max: req.Max,
	})
	if err != nil {
		s.fail(w, err)
		return
	}
	resp := wire.PeekResponse{Messages: make([]wire.Message, len(ms))}
	for i, m := range ms {
		resp.Messages[i] = wire.FromPeeked(m)
	}
	writeJSON(w, resp)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	var req wire.MetricsRequest
	if err := decode(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return
	}
	m, err := s.eng.Stats(r.Context(), req.Queue)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, wire.MetricsResponse{
		Queue: m.Queue, Active: m.Active, Locked: m.Locked, Deferred: m.Deferred,
		Scheduled: m.Scheduled, DeadLettered: m.DeadLettered, Total: m.Total,
		OldestMessageAgeMs: m.OldestMessageAgeMs,
	})
}

// ── AdminService handlers ───────────────────────────────────────────────────

func (s *Server) handleCreateQueue(w http.ResponseWriter, r *http.Request) {
	var req wire.CreateQueueRequest
	if err := decode(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return
	}
	if err := s.eng.CreateQueue(r.Context(), req.Name, req.Config.ToConfig()); err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, wire.Empty{})
}

func (s *Server) handleSubscribe(w http.ResponseWriter, r *http.Request) {
	var req wire.SubscribeRequest
	if err := decode(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return
	}
	if err := s.eng.Subscribe(r.Context(), req.Topic, req.Name, req.Filter); err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, wire.Empty{})
}

func (s *Server) handleListQueues(w http.ResponseWriter, r *http.Request) {
	qs, err := s.eng.ListQueues(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	resp := wire.ListQueuesResponse{Queues: make([]wire.QueueInfoJSON, len(qs))}
	for i, q := range qs {
		resp.Queues[i] = wire.QueueInfoJSON{
			Name: q.Name, Kind: q.Kind, LockDurationMs: q.LockDurationMs,
			MaxDeliveryCount: q.MaxDeliveryCount, DefaultTTLMs: q.DefaultTTLMs, DedupWindowMs: q.DedupWindowMs,
		}
	}
	writeJSON(w, resp)
}

func (s *Server) handleRedrive(w http.ResponseWriter, r *http.Request) {
	var req wire.RedriveRequest
	if err := decode(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return
	}
	moved, err := s.eng.Redrive(r.Context(), req.Queue, engine.RedriveOptions{
		Target: req.Target, Max: req.Max, OlderThanMs: req.OlderThanMs, RatePerSec: req.RatePerSec,
	})
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, wire.RedriveResponse{Moved: moved})
}

func (s *Server) handlePurge(w http.ResponseWriter, r *http.Request) {
	var req wire.PurgeRequest
	if err := decode(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_argument", err.Error())
		return
	}
	purged, err := s.eng.Purge(r.Context(), req.Queue, engine.RedriveOptions{
		Max: req.Max, OlderThanMs: req.OlderThanMs,
	})
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, wire.PurgeResponse{Purged: purged})
}
