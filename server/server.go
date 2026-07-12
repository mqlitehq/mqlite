// Package server exposes an mqlite Engine as a Connect-style JSON-over-HTTP
// broker (design §7). Every unary RPC is a plain HTTP POST to
// /mqlite.v1.<Service>/<Method> with a JSON body — curl-able by construction.
package server

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/wire"
)

// Server adapts an Engine to HTTP with static Bearer-token auth.
type Server struct {
	eng     *engine.Engine
	tokens  map[string]bool // empty -> auth disabled (dev/LAN only)
	mux     *http.ServeMux
	Version string       // reported by the open "/" discovery endpoint; "" -> "dev"
	CORS    string       // Access-Control-Allow-Origin to send; "" -> CORS off (see cors.go)
	Logger  *slog.Logger // per-request access log; nil -> no request logging (see logging.go)
	UI      bool         // serve the embedded admin console at /ui (see console.go)
	// MaxBodyBytes bounds any single RPC request body BEFORE JSON decoding
	// (review F8): without it a multi-GB body OOMs the broker before the
	// per-message MaxMessageBytes check ever runs. Bodies are base64 (x4/3) and
	// a batched Send carries many messages, so the default (32 MiB) sits far
	// above normal use while keeping one hostile request a fraction of the
	// smallest supported deployment's RAM. Over the cap -> 413 message_too_large.
	MaxBodyBytes int64
	rpcLat       *rpcLatency // per-RPC latency histogram, exposed at /metrics (see rpchist.go)
	started      time.Time
}

// New builds a Server. tokens is the set of accepted Bearer tokens; pass nil/empty
// to disable auth (documented as a localhost/LAN downgrade, §7.5).
func New(eng *engine.Engine, tokens []string) *Server {
	s := &Server{eng: eng, tokens: map[string]bool{}, mux: http.NewServeMux(), rpcLat: newRPCLatency(), started: time.Now(),
		MaxBodyBytes: 32 << 20}
	for _, t := range tokens {
		if t = strings.TrimSpace(t); t != "" {
			s.tokens[t] = true
		}
	}
	s.routes()
	return s
}

// Handler returns the HTTP handler: CORS (outermost, so preflight bypasses auth) wrapping
// the optional request log, Bearer-token auth, the RPC-latency observer, and the route mux.
func (s *Server) Handler() http.Handler { return s.cors(s.logging(s.auth(s.observe(s.mux)))) }

func (s *Server) routes() {
	h := func(path string, fn http.HandlerFunc) { s.mux.HandleFunc(path, s.postOnly(fn)) }
	h(wire.PathSend, s.handleSend)
	h(wire.PathReceive, s.handleReceive)
	h(wire.PathComplete, s.handleComplete)
	h(wire.PathCompleteBatch, s.handleCompleteBatch)
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
	h(wire.PathListSubscriptions, s.handleListSubscriptions)
	h(wire.PathTestFilter, s.handleTestFilter)
	h(wire.PathRedrive, s.handleRedrive)
	h(wire.PathPurge, s.handlePurge)
	h(wire.PathStatus, s.handleStatus)
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
	// Embedded admin console (the built mqlite-web SPA) at /ui, when Server.UI is on
	// (MQLITE_UI). The static page is open; its API calls carry the Bearer token.
	s.mux.Handle("/ui/", s.console())
	s.mux.HandleFunc("/ui", func(w http.ResponseWriter, r *http.Request) {
		if !s.UI {
			writeErr(w, http.StatusNotFound, "not_found", "no such path: /ui")
			return
		}
		http.Redirect(w, r, "/ui/", http.StatusMovedPermanently)
	})
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
	endpoints := map[string]string{
		"health":  "GET /healthz",
		"metrics": "GET /metrics (Bearer)",
		"rpc":     "POST /mqlite.v1.{Service}/{Method} (Bearer)",
	}
	if s.UI {
		endpoints["ui"] = "GET /ui"
	}
	writeJSON(w, map[string]any{
		"name":        "mqlite",
		"description": "Lightweight SQLite-backed message queue with Azure Service Bus-style semantics.",
		"version":     version,
		"status":      "ok",
		"auth":        len(s.tokens) > 0, // true -> RPCs need a Bearer token
		"docs":        "https://github.com/mqlitehq/mqlite",
		"endpoints":   endpoints,
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
	// Lifetime completed messages per queue — a running count that survives the
	// row being deleted on Complete (MQLITE-54). In-process and resets on restart;
	// Prometheus rate()/increase() absorb the reset.
	completed := s.eng.CompletedCounts()
	b.WriteString("# HELP mqlite_messages_completed_total Messages successfully completed, cumulative since broker start.\n")
	b.WriteString("# TYPE mqlite_messages_completed_total counter\n")
	for _, st := range stats {
		fmt.Fprintf(&b, "mqlite_messages_completed_total{queue=%q} %d\n", st.name, completed[st.name])
	}
	s.rpcLat.write(&b) // per-RPC latency histogram (rpchist.go)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write([]byte(b.String()))
}

func (s *Server) postOnly(fn http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "unimplemented", "POST required")
			return
		}
		if s.MaxBodyBytes > 0 {
			r.Body = http.MaxBytesReader(w, r.Body, s.MaxBodyBytes)
		}
		fn(w, r)
	}
}

// auth enforces Bearer tokens (skips /healthz and when no tokens configured).
func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// /ui is matched exactly ("/ui" or "/ui/..."), not by loose prefix — a
		// loose HasPrefix would also auth-exempt /uixyz (review F11).
		if len(s.tokens) == 0 || r.URL.Path == "/" || r.URL.Path == "/healthz" ||
			r.URL.Path == "/ui" || strings.HasPrefix(r.URL.Path, "/ui/") {
			next.ServeHTTP(w, r)
			return
		}
		tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		// Constant-time comparison against every configured token (review F10):
		// a plain map lookup / == leaks how many leading bytes matched. No early
		// break, so timing is independent of which token (if any) matches.
		authed := false
		for t := range s.tokens {
			if len(t) == len(tok) && subtle.ConstantTimeCompare([]byte(t), []byte(tok)) == 1 {
				authed = true
			}
		}
		if tok == "" || !authed {
			writeErr(w, http.StatusUnauthorized, "unauthenticated", "missing or invalid Bearer token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ── helpers ─────────────────────────────────────────────────────────────────

// decode strictly parses exactly one JSON object into v: an unknown field or any
// data after the first value is a 400, not a silently-dropped typo (MQLITE-86). A
// typo'd field like "messsages" would otherwise decode to an empty, successful Send.
func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("unexpected data after JSON body (want a single object)")
	}
	return nil
}

// decodeErr maps a request-decoding failure: a body over the MaxBodyBytes cap is
// 413 message_too_large (same code as the per-message cap); anything else is a
// plain 400 invalid_argument.
func decodeErr(w http.ResponseWriter, err error) {
	var mbe *http.MaxBytesError
	if errors.As(err, &mbe) {
		writeErr(w, http.StatusRequestEntityTooLarge, "message_too_large",
			fmt.Sprintf("request body over %d bytes", mbe.Limit))
		return
	}
	writeErr(w, http.StatusBadRequest, "invalid_argument", err.Error())
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	logf(w, "code", code)
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
	case errors.Is(err, engine.ErrInvalidFilter), errors.Is(err, engine.ErrInvalidArgument):
		writeErr(w, http.StatusBadRequest, "invalid_argument", err.Error())
	case errors.Is(err, engine.ErrMessageTooLarge):
		writeErr(w, http.StatusRequestEntityTooLarge, "message_too_large", err.Error())
	case errors.Is(err, engine.ErrOutcomeUnknown):
		// The remote write may or may not have applied (lost commit ack). Give it a
		// distinct code so the SDK reconstructs ErrOutcomeUnknown and the caller
		// reconciles by message_id/dedup instead of blindly retrying a 500 into a
		// double-apply (MQLITE-59). 503: the durability is uncertain, not a bad request.
		writeErr(w, http.StatusServiceUnavailable, "outcome_unknown", err.Error())
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
		decodeErr(w, err)
		return
	}
	ctx := r.Context()
	logf(w, "queue", req.Queue)
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
		if len(outs) == 1 {
			// Single schedule via Schedule, which tells a real dedup conflict (409) apart
			// from a no-subscriber no-op (seq 0), matching the non-scheduled single path.
			var seq int64
			seq, err = s.eng.Schedule(ctx, req.Queue, outs[0], req.ScheduledEnqueueTimeMs)
			if err != nil {
				s.fail(w, err)
				return
			}
			seqs = []int64{seq}
		} else {
			// Multi schedule is one atomic transaction — all-or-nothing, no partial commit
			// on a mid-batch failure (MQLITE-72).
			seqs, err = s.eng.ScheduleBatch(ctx, req.Queue, outs, req.ScheduledEnqueueTimeMs)
			if err != nil {
				s.fail(w, err)
				return
			}
		}
	} else if len(outs) == 1 {
		// Single send via SendOne, which tells the two causes of a seq-0 apart: a real
		// dedup conflict (same id, different body) → ErrDedupConflict (409), versus a
		// topic publish that matched no subscription → a valid no-op (seq 0, 200). The
		// batch path drops the conflict flags, which is why a no-subscriber publish used
		// to be mislabeled as a dedup conflict.
		var seq int64
		seq, err = s.eng.SendOne(ctx, req.Queue, outs[0])
		if err != nil {
			s.fail(w, err)
			return
		}
		seqs = []int64{seq}
	} else {
		seqs, err = s.eng.Send(ctx, req.Queue, outs...)
		if err != nil {
			s.fail(w, err)
			return
		}
	}
	logf(w, "n", len(seqs))
	if len(outs) == 1 && outs[0].MessageID != "" {
		logf(w, "msg_id", outs[0].MessageID)
	}
	writeJSON(w, wire.SendResponse{SeqNumbers: seqs})
}

func (s *Server) handleSchedule(w http.ResponseWriter, r *http.Request) { s.handleSend(w, r) }

func (s *Server) handleReceive(w http.ResponseWriter, r *http.Request) {
	var req wire.ReceiveRequest
	if err := decode(r, &req); err != nil {
		decodeErr(w, err)
		return
	}
	logf(w, "queue", req.Queue)
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
	logf(w, "msgs", len(msgs))
	if len(msgs) == 0 {
		logQuiet(w) // empty receive / idle long-poll → demote to Debug
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
		decodeErr(w, err)
		return
	}
	logf(w, "queue", req.Queue)
	msgs, err := s.eng.ReceiveDeferred(r.Context(), req.Queue, req.SeqNumbers...)
	if err != nil {
		s.fail(w, err)
		return
	}
	logf(w, "msgs", len(msgs))
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
		decodeErr(w, err)
		return
	}
	logf(w, "queue", req.Queue, "seq", req.SeqNumber)
	s.settleOK(w, s.eng.Complete(r.Context(), req.Queue, req.SeqNumber, req.LockToken))
}

func (s *Server) handleCompleteBatch(w http.ResponseWriter, r *http.Request) {
	var req wire.CompleteBatchRequest
	if err := decode(r, &req); err != nil {
		decodeErr(w, err)
		return
	}
	logf(w, "queue", req.Queue, "n", len(req.Messages))
	items := make([]engine.SettleItem, len(req.Messages))
	for i, m := range req.Messages {
		items[i] = engine.SettleItem{SeqNumber: m.SeqNumber, LockToken: m.LockToken}
	}
	results, err := s.eng.CompleteBatch(r.Context(), req.Queue, items)
	if err != nil {
		s.fail(w, err)
		return
	}
	out := make([]wire.SettleItemResult, len(results))
	for i, res := range results {
		out[i] = wire.SettleItemResult{SeqNumber: res.SeqNumber, Ok: res.Ok}
	}
	writeJSON(w, wire.CompleteBatchResponse{Results: out})
}

func (s *Server) handleAbandon(w http.ResponseWriter, r *http.Request) {
	var req wire.SettleRequest
	if err := decode(r, &req); err != nil {
		decodeErr(w, err)
		return
	}
	logf(w, "queue", req.Queue, "seq", req.SeqNumber)
	s.settleOK(w, s.eng.Abandon(r.Context(), req.Queue, req.SeqNumber, req.LockToken, req.DelayMs))
}

func (s *Server) handleReject(w http.ResponseWriter, r *http.Request) {
	var req wire.SettleRequest
	if err := decode(r, &req); err != nil {
		decodeErr(w, err)
		return
	}
	logf(w, "queue", req.Queue, "seq", req.SeqNumber)
	s.settleOK(w, s.eng.Reject(r.Context(), req.Queue, req.SeqNumber, req.LockToken,
		req.DeadLetterReason, req.DeadLetterDescription))
}

func (s *Server) handleDefer(w http.ResponseWriter, r *http.Request) {
	var req wire.SettleRequest
	if err := decode(r, &req); err != nil {
		decodeErr(w, err)
		return
	}
	logf(w, "queue", req.Queue, "seq", req.SeqNumber)
	s.settleOK(w, s.eng.Defer(r.Context(), req.Queue, req.SeqNumber, req.LockToken))
}

func (s *Server) handleRenew(w http.ResponseWriter, r *http.Request) {
	var req wire.SettleRequest
	if err := decode(r, &req); err != nil {
		decodeErr(w, err)
		return
	}
	logf(w, "queue", req.Queue, "seq", req.SeqNumber)
	s.settleOK(w, s.eng.Renew(r.Context(), req.Queue, req.SeqNumber, req.LockToken))
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	var req wire.CancelRequest
	if err := decode(r, &req); err != nil {
		decodeErr(w, err)
		return
	}
	logf(w, "queue", req.Queue, "seq", req.SeqNumber)
	if err := s.eng.Cancel(r.Context(), req.Queue, req.SeqNumber); err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, wire.SettleResponse{Ok: true})
}

func (s *Server) handlePeek(w http.ResponseWriter, r *http.Request) {
	var req wire.PeekRequest
	if err := decode(r, &req); err != nil {
		decodeErr(w, err)
		return
	}
	logf(w, "queue", req.Queue)
	ms, err := s.eng.Peek(r.Context(), req.Queue, engine.PeekOptions{
		FromSeq: req.FromSeq, State: engine.State(req.State), Max: req.Max,
	})
	if err != nil {
		s.fail(w, err)
		return
	}
	logf(w, "msgs", len(ms))
	resp := wire.PeekResponse{Messages: make([]wire.Message, len(ms))}
	for i, m := range ms {
		resp.Messages[i] = wire.FromPeeked(m)
	}
	writeJSON(w, resp)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	var req wire.MetricsRequest
	if err := decode(r, &req); err != nil {
		decodeErr(w, err)
		return
	}
	logf(w, "queue", req.Queue)
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
		decodeErr(w, err)
		return
	}
	logf(w, "queue", req.Name)
	if err := s.eng.CreateQueue(r.Context(), req.Name, req.Config.ToConfig()); err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, wire.Empty{})
}

func (s *Server) handleSubscribe(w http.ResponseWriter, r *http.Request) {
	var req wire.SubscribeRequest
	if err := decode(r, &req); err != nil {
		decodeErr(w, err)
		return
	}
	logf(w, "topic", req.Topic, "sub", req.Name)
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

func (s *Server) handleListSubscriptions(w http.ResponseWriter, r *http.Request) {
	subs, err := s.eng.ListSubscriptions(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	resp := wire.ListSubscriptionsResponse{Subscriptions: make([]wire.SubscriptionJSON, len(subs))}
	for i, su := range subs {
		resp.Subscriptions[i] = wire.SubscriptionJSON{Topic: su.Topic, Name: su.Name, Expr: su.Expr}
	}
	writeJSON(w, resp)
}

// handleStatus reports a desensitized runtime snapshot for an ops view: backend kind,
// redacted location, read latency, local footprint, counts, uptime. Behind auth.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	st := s.eng.Status(ctx)
	version := s.Version
	if version == "" {
		version = "dev"
	}
	// Counts are best-effort — a count failure must not fail the whole status read.
	var queues, subs int
	if qs, err := s.eng.ListQueues(ctx); err == nil {
		for _, q := range qs {
			if q.Kind != "subscription" {
				queues++
			}
		}
	}
	if ss, err := s.eng.ListSubscriptions(ctx); err == nil {
		subs = len(ss)
	}
	writeJSON(w, wire.StatusResponse{
		Version:       version,
		Backend:       st.Backend,
		Remote:        st.Remote,
		Location:      st.Location,
		SchemaVersion: st.SchemaVersion,
		PingMs:        st.PingMs,
		DBSizeBytes:   st.SizeBytes,
		Queues:        queues,
		Subscriptions: subs,
		UptimeMs:      time.Since(s.started).Milliseconds(),
		Auth:          len(s.tokens) > 0,
	})
}

func (s *Server) handleTestFilter(w http.ResponseWriter, r *http.Request) {
	var req wire.TestFilterRequest
	if err := decode(r, &req); err != nil {
		decodeErr(w, err)
		return
	}
	now := time.Now().UnixMilli()
	enq, vis := now, now
	var sample *engine.OutMessage
	if req.Message != nil {
		m := req.Message.ToOut()
		sample = &m
		if req.Message.EnqueuedAtMs != 0 {
			enq = req.Message.EnqueuedAtMs
		}
		if req.Message.VisibleAtMs != 0 {
			vis = req.Message.VisibleAtMs
		} else {
			vis = enq
		}
	}
	res := engine.TestFilter(req.Expr, sample, enq, vis)
	writeJSON(w, wire.TestFilterResponse{Valid: res.Valid, Error: res.Error, Ran: res.Ran, Matched: res.Matched})
}

func (s *Server) handleRedrive(w http.ResponseWriter, r *http.Request) {
	var req wire.RedriveRequest
	if err := decode(r, &req); err != nil {
		decodeErr(w, err)
		return
	}
	logf(w, "queue", req.Queue)
	moved, err := s.eng.Redrive(r.Context(), req.Queue, engine.RedriveOptions{
		Target: req.Target, Max: req.Max, OlderThanMs: req.OlderThanMs, RatePerSec: req.RatePerSec,
	})
	if err != nil {
		s.fail(w, err)
		return
	}
	logf(w, "n", moved)
	writeJSON(w, wire.RedriveResponse{Moved: moved})
}

func (s *Server) handlePurge(w http.ResponseWriter, r *http.Request) {
	var req wire.PurgeRequest
	if err := decode(r, &req); err != nil {
		decodeErr(w, err)
		return
	}
	logf(w, "queue", req.Queue)
	purged, err := s.eng.Purge(r.Context(), req.Queue, engine.RedriveOptions{
		Max: req.Max, OlderThanMs: req.OlderThanMs,
	})
	if err != nil {
		s.fail(w, err)
		return
	}
	logf(w, "n", purged)
	writeJSON(w, wire.PurgeResponse{Purged: purged})
}
