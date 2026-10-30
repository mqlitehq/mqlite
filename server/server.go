// Package server exposes an mqlite Engine as a Connect-style JSON-over-HTTP
// broker (design §7). Every unary RPC is a plain HTTP POST to
// /mqlite.v1.<Service>/<Method> with a JSON body — curl-able by construction.
// A durable broker Engine must be opened with
// engine.Options.PreserveUnexpiredLeases so remote consumers retain live tokens
// across broker restarts. The higher-level Embedded.Serve path enforces the
// equivalent mqlite.WithBrokerMode option.
package server

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"mime"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/internal/authkey"
	"github.com/mqlitehq/mqlite/internal/receivegate"
	"github.com/mqlitehq/mqlite/wire"
)

// Server adapts an Engine to HTTP with static administrators and managed access keys.
type Server struct {
	eng     *engine.Engine
	tokens  map[string]bool // empty -> auth disabled (dev/LAN only)
	mux     *http.ServeMux
	Version string       // reported by the open "/" discovery endpoint; "" -> "dev"
	CORS    string       // Access-Control-Allow-Origin to send; "" -> CORS off (see cors.go)
	Logger  *slog.Logger // per-request access log; nil -> no request logging (see logging.go)
	UI      bool         // serve the embedded admin console at /ui (see console.go)
	// Metrics enables authenticated GET /metrics on the API listener and includes
	// its path in discovery. Off by default; configure before Handler is used.
	Metrics bool
	// MonitorTokens grants only Observe and /metrics. Configure before Handler is
	// used. These credentials never consult the database and cannot manage keys.
	MonitorTokens []string
	// MaxBodyBytes bounds any single RPC request body BEFORE JSON decoding
	// (review F8): without it a multi-GB body OOMs the broker before the
	// per-message MaxMessageBytes check ever runs. Bodies are base64 (x4/3) and
	// a batched Send carries many messages, so the default (32 MiB) sits far
	// above normal use while keeping one hostile request a fraction of the
	// smallest supported deployment's RAM. Over the cap -> 413 message_too_large.
	MaxBodyBytes int64
	rpcLat       *rpcLatency // per-RPC latency histogram, exposed at /metrics (see rpchist.go)
	transport    transportMetrics
	started      time.Time
	// rpcPaths is every RPC route, collected as routes() registers them, so the "/"
	// discovery card enumerates exactly what is actually served — it can't drift from
	// registration. Pinned by TestDiscoveryCardPinned (MQLITE-87).
	rpcPaths []string
}

// New builds a Server. tokens is the set of accepted Bearer tokens; pass nil/empty
// to disable auth (documented as a localhost/LAN downgrade, §7.5). For a durable
// broker, eng must have been opened with engine.Options.PreserveUnexpiredLeases;
// otherwise a reopen uses embedded ownership semantics and revokes old locks.
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
func (s *Server) Handler() http.Handler {
	admins := make([]string, 0, len(s.tokens))
	for token := range s.tokens {
		admins = append(admins, token)
	}
	if err := ValidateMonitorTokens(admins, s.MonitorTokens); err != nil {
		return s.withServerTime(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			writeErr(w, http.StatusInternalServerError, "internal", "invalid monitoring credential configuration")
		}))
	}
	if s.Metrics && len(s.tokens) == 0 {
		return s.withServerTime(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			writeErr(w, http.StatusInternalServerError, "internal", "metrics require administrator authentication")
		}))
	}
	handler := s.cors(s.observeRequests(s.logging(s.auth(s.observe(s.mux)))))
	return s.withServerTime(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" && !s.Metrics {
			http.NotFound(w, r)
			return
		}
		handler.ServeHTTP(w, r)
	}))
}

func (s *Server) withServerTime(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(&serverTimeResponseWriter{ResponseWriter: w, now: s.serverTimeMs}, r)
	})
}

type serverTimeResponseWriter struct {
	http.ResponseWriter
	now     func() int64
	written bool
}

// Unwrap lets http.ResponseController reach optional capabilities implemented
// by the underlying server writer (Flush, Hijack, full duplex, and deadlines).
func (w *serverTimeResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *serverTimeResponseWriter) stamp() {
	if w.written {
		return
	}
	w.written = true
	w.Header().Set(wire.ServerTimeHeader, strconv.FormatInt(w.now(), 10))
}

func (w *serverTimeResponseWriter) WriteHeader(status int) {
	w.stamp()
	w.ResponseWriter.WriteHeader(status)
}

func (w *serverTimeResponseWriter) Write(p []byte) (int, error) {
	w.stamp()
	return w.ResponseWriter.Write(p)
}

func (s *Server) serverTimeMs() int64 {
	if s.eng == nil {
		return time.Now().UnixMilli()
	}
	return s.eng.ServerTimeMs()
}

// ValidateMonitorTokens rejects ambiguous or ineffective monitoring credentials.
// Errors describe configuration only and never contain a credential value.
func ValidateMonitorTokens(admins, monitors []string) error {
	if len(monitors) == 0 {
		return nil
	}
	adminSet := make(map[string]bool)
	for _, token := range admins {
		if token = strings.TrimSpace(token); token != "" {
			adminSet[token] = true
		}
	}
	if len(adminSet) == 0 {
		return errors.New("monitor credentials require administrator authentication")
	}
	for _, token := range monitors {
		token = strings.TrimSpace(token)
		if token == "" || strings.ContainsAny(token, " \t\r\n,") {
			return errors.New("monitor credentials must be nonempty Bearer tokens")
		}
		if adminSet[token] {
			return errors.New("monitor and administrator credentials must be distinct")
		}
	}
	return nil
}

func (s *Server) routes() {
	h := func(path string, permission engine.KeyPermissions, fn http.HandlerFunc) {
		s.mux.HandleFunc(path, s.authorize(permission, s.postOnly(fn)))
		s.rpcPaths = append(s.rpcPaths, path) // feeds the "/" discovery catalog
	}
	h(wire.PathSend, engine.KeySend, s.handleSend)
	h(wire.PathReceive, engine.KeyListen, s.handleReceive)
	h(wire.PathComplete, engine.KeyListen, s.handleComplete)
	h(wire.PathCompleteBatch, engine.KeyListen, s.handleCompleteBatch)
	h(wire.PathRenewBatch, engine.KeyListen, s.handleRenewBatch)
	h(wire.PathAbandon, engine.KeyListen, s.handleAbandon)
	h(wire.PathReject, engine.KeyListen, s.handleReject)
	h(wire.PathDefer, engine.KeyListen, s.handleDefer)
	h(wire.PathReceiveDeferred, engine.KeyListen, s.handleReceiveDeferred)
	h(wire.PathRenew, engine.KeyListen, s.handleRenew)
	h(wire.PathSchedule, engine.KeySend, s.handleSchedule)
	h(wire.PathCancel, engine.KeySend, s.handleCancel)
	h(wire.PathPeek, engine.KeyListen, s.handlePeek)
	h(wire.PathStats, engine.KeyListen, s.handleStats)
	h(wire.PathCreateQueue, engine.KeyManage, s.handleCreateQueue)
	h(wire.PathSubscribe, engine.KeyListen, s.handleSubscribe)
	h(wire.PathListQueues, engine.KeyManage, s.handleListQueues)
	h(wire.PathListSubscriptions, engine.KeyManage, s.handleListSubscriptions)
	h(wire.PathListSubscriptionsPage, engine.KeyManage, s.handleListSubscriptionsPage)
	h(wire.PathTestFilter, engine.KeyListen, s.handleTestFilter)
	h(wire.PathRedrive, engine.KeyManage, s.handleRedrive)
	h(wire.PathPurge, engine.KeyManage, s.handlePurge)
	h(wire.PathStatus, engine.KeyManage, s.handleStatus)
	h(wire.PathObserve, monitorPermission, s.handleObserve)
	h(wire.PathCreateKey, engine.KeyManage, s.authEnabled(s.handleCreateKey))
	h(wire.PathListKeys, engine.KeyManage, s.authEnabled(s.handleListKeys))
	h(wire.PathRevokeKey, engine.KeyManage, s.authEnabled(s.handleRevokeKey))
	h(wire.PathDeleteQueue, engine.KeyManage, s.handleDeleteQueue)
	h(wire.PathDeleteSubscription, engine.KeyManage, s.handleDeleteSubscription)
	s.mux.HandleFunc("/metrics", s.authorize(monitorPermission, s.handleMetrics))
	s.mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	// Open discovery/index at "/" — hit the broker with no path/params/auth and get a
	// plain JSON telling you what this is, the version, and a basic status.
	s.mux.HandleFunc("/", s.handleIndex)
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
	if s.UI {
		// Keep the agent-facing JSON discovery contract as the default. An
		// interactive browser explicitly prefers HTML, so give it the console
		// without making curl, Go clients, or */* callers follow a redirect.
		w.Header().Add("Vary", "Accept")
		if (r.Method == http.MethodGet || r.Method == http.MethodHead) && prefersHTML(r.Header.Get("Accept")) {
			http.Redirect(w, r, "/ui/", http.StatusTemporaryRedirect)
			return
		}
	}
	version := s.Version
	if version == "" {
		version = "dev"
	}
	auth := "none" // auth off -> RPCs are open (dev/loopback)
	if s.authenticationEnabled() {
		auth = "bearer"
	}
	card := wire.DiscoveryCard{
		Name:        "mqlite",
		Description: "Lightweight SQLite-backed message queue with Azure Service Bus-style semantics.",
		Version:     version,
		Status:      "ok",
		Auth:        auth,
		Docs:        "https://github.com/mqlitehq/mqlite",
		Endpoints:   s.rpcPaths, // the complete RPC catalog, exactly as registered
		Health:      "/healthz",
	}
	if s.Metrics {
		card.Metrics = "/metrics"
	}
	if s.UI {
		card.UI = "/ui"
	}
	writeJSON(w, card)
}

type acceptPreference struct {
	quality     float64
	specificity int
}

// prefersHTML reports whether Accept explicitly prefers exact text/html over
// application/json. Missing Accept, */*, malformed values and exact ties all
// stay on JSON to preserve the root discovery contract for existing clients.
func prefersHTML(accept string) bool {
	html := preferenceFor(accept, "text", "html")
	if html.specificity != 2 || html.quality <= 0 {
		return false
	}
	json := preferenceFor(accept, "application", "json")
	if html.quality != json.quality {
		return html.quality > json.quality
	}
	return html.specificity > json.specificity
}

func preferenceFor(accept, mediaType, mediaSubtype string) acceptPreference {
	best := acceptPreference{specificity: -1}
	for _, part := range strings.Split(accept, ",") {
		parsed, params, err := mime.ParseMediaType(strings.TrimSpace(part))
		if err != nil {
			continue
		}
		quality := 1.0
		if raw, ok := params["q"]; ok {
			quality, err = strconv.ParseFloat(raw, 64)
			if err != nil || math.IsNaN(quality) || quality < 0 || quality > 1 {
				continue
			}
		}

		pieces := strings.SplitN(parsed, "/", 2)
		if len(pieces) != 2 {
			continue
		}
		specificity := -1
		switch {
		case pieces[0] == mediaType && pieces[1] == mediaSubtype:
			specificity = 2
		case pieces[0] == mediaType && pieces[1] == "*":
			specificity = 1
		case pieces[0] == "*" && pieces[1] == "*":
			specificity = 0
		}
		if specificity > best.specificity || (specificity == best.specificity && quality > best.quality) {
			best = acceptPreference{quality: quality, specificity: specificity}
		}
	}
	return best
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

// permissionContextKey is private so callers cannot inject HTTP permissions.
type permissionContextKey struct{}

// A transport-only capability, never a database access-key permission bit.
const monitorPermission engine.KeyPermissions = 128

func (s *Server) authenticationEnabled() bool {
	return len(s.tokens) > 0 || len(s.MonitorTokens) > 0
}

func openPath(path string) bool {
	return path == "/" || path == "/healthz" || path == "/ui" || strings.HasPrefix(path, "/ui/")
}

// auth authenticates once before dispatch. Revocation applies to the next request;
// an already-authorized operation, including a long poll, may finish normally.
func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Exact console paths only: /uixyz must not bypass authentication.
		if !s.authenticationEnabled() || openPath(r.URL.Path) {
			logf(w, "actor_kind", "anonymous")
			next.ServeHTTP(w, r)
			return
		}
		values := r.Header.Values("Authorization")
		if len(values) != 1 {
			outcome := "invalid"
			if len(values) == 0 {
				outcome = "missing"
			}
			logf(w, "actor_kind", "unverified")
			authOutcome(w, outcome)
			s.fail(w, engine.ErrUnauthenticated)
			return
		}
		scheme, tok, ok := strings.Cut(values[0], " ")
		// RFC Bearer syntax permits multiple spaces after the scheme, but never
		// multiple credentials or whitespace inside/after the opaque token.
		tok = strings.TrimLeft(tok, " ")
		if !ok || !strings.EqualFold(scheme, "Bearer") || tok == "" || strings.ContainsAny(tok, " \t\r\n,") {
			logf(w, "actor_kind", "unverified")
			authOutcome(w, "invalid")
			s.fail(w, engine.ErrUnauthenticated)
			return
		}
		// Preserve static administrator precedence, even if the same token is also
		// stored as an expired, revoked, or restricted database key. Compare every
		// configured token without an early exit.
		static := false
		for t := range s.tokens {
			if len(t) == len(tok) && subtle.ConstantTimeCompare([]byte(t), []byte(tok)) == 1 {
				static = true
			}
		}
		permission := engine.KeyManage
		actorKind, actorKeyID := "static_admin", ""
		if !static {
			monitor := false
			for _, t := range s.MonitorTokens {
				t = strings.TrimSpace(t)
				if len(t) == len(tok) && subtle.ConstantTimeCompare([]byte(t), []byte(tok)) == 1 {
					monitor = true
				}
			}
			if monitor {
				permission = monitorPermission
				actorKind = "static_monitor"
			} else {
				key, outcome, err := s.eng.AuthenticateAccessKeyWithOutcome(r.Context(), tok)
				if err != nil {
					logf(w, "actor_kind", "unverified")
					authOutcome(w, outcome)
					s.fail(w, err)
					return
				}
				permission = key.Permissions
				actorKind, actorKeyID = "managed_key", key.ID
			}
		}
		logf(w, "actor_kind", actorKind)
		if actorKeyID != "" {
			s.logPublicKeyID(w, "actor_key_id", actorKeyID)
		}
		authOutcome(w, "success")
		ctx := context.WithValue(r.Context(), permissionContextKey{}, permission)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// authorize binds a route and its required permission at registration, before
// decoding or invoking its handler. There is no separate policy catalog to drift.
func (s *Server) authorize(required engine.KeyPermissions, fn http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.authenticationEnabled() {
			// Anonymous development mode keeps data-plane RPCs usable for local
			// tests, but never turns observation into an information leak.
			if required == monitorPermission {
				authOutcome(w, "permission_denied")
				s.fail(w, engine.ErrPermissionDenied)
				return
			}
			fn(w, r)
			return
		}
		permission, ok := r.Context().Value(permissionContextKey{}).(engine.KeyPermissions)
		allowed := permission.Allows(required)
		if required == monitorPermission {
			allowed = permission == engine.KeyManage || permission == monitorPermission
		}
		if !ok || !allowed {
			authOutcome(w, "permission_denied")
			s.fail(w, engine.ErrPermissionDenied)
			return
		}
		fn(w, r)
	}
}

// Anonymous development mode never exposes credential issuance or metadata.
func (s *Server) authEnabled(fn http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.authenticationEnabled() {
			writeErr(w, http.StatusForbidden, "permission_denied", "key management requires authentication to be enabled")
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		fn(w, r)
	}
}

// ── helpers ─────────────────────────────────────────────────────────────────

// decode strictly parses exactly one non-null JSON object into v: an unknown field
// or any data after the first value is a 400, not a silently-dropped typo
// (MQLITE-86). A typo'd field like "messsages" would otherwise decode to an empty,
// successful Send. v is always a pointer to a request struct; decoding through a
// temporary pointer distinguishes JSON null from a real zero-valued object.
func decode(r *http.Request, v any) error {
	dst := reflect.ValueOf(v)
	if dst.Kind() != reflect.Pointer || dst.IsNil() || dst.Elem().Kind() != reflect.Struct {
		return errors.New("request destination must be a non-nil pointer to a struct")
	}
	tmp := reflect.New(dst.Type()) // **T: null stays nil, while {} allocates a *T.
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(tmp.Interface()); err != nil {
		return err
	}
	decoded := tmp.Elem()
	if decoded.IsNil() {
		return errors.New("JSON request body must be an object, not null")
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("unexpected data after JSON body (want a single object)")
		}
		return fmt.Errorf("unexpected data after JSON body: %w", err)
	}
	dst.Elem().Set(decoded.Elem())
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
	writeResourceErr(w, status, code, msg, "")
}

func writeResourceErr(w http.ResponseWriter, status int, code, msg, resource string) {
	if rec, ok := w.(*statusRecorder); ok {
		rec.code = code
	}
	logf(w, "code", code)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(wire.ErrorBody{Code: code, Message: msg, Resource: resource})
}

// mapErr translates engine errors to a Connect-style (status, code) error.
func (s *Server) fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, engine.ErrUnauthenticated):
		writeErr(w, http.StatusUnauthorized, "unauthenticated", "missing or invalid Bearer token")
	case errors.Is(err, engine.ErrPermissionDenied):
		writeErr(w, http.StatusForbidden, "permission_denied", "access key lacks the required permission")
	case errors.Is(err, engine.ErrKeyConflict):
		writeErr(w, http.StatusConflict, "key_conflict", err.Error())
	case errors.Is(err, engine.ErrSubscriptionNotFound):
		writeResourceErr(w, http.StatusNotFound, "not_found", err.Error(), "subscription")
	case errors.Is(err, engine.ErrQueueNotFound):
		writeResourceErr(w, http.StatusNotFound, "not_found", err.Error(), "queue")
	case errors.Is(err, engine.ErrNotFound):
		writeErr(w, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, engine.ErrDedupConflict):
		writeErr(w, http.StatusConflict, "already_exists", err.Error())
	case errors.Is(err, engine.ErrQueueNotEmpty):
		writeErr(w, http.StatusConflict, "queue_not_empty", err.Error())
	case errors.Is(err, engine.ErrSubscriptionNotEmpty):
		writeErr(w, http.StatusConflict, "subscription_not_empty", err.Error())
	case errors.Is(err, engine.ErrSubscriptionLimit):
		writeErr(w, http.StatusTooManyRequests, "resource_exhausted", err.Error())
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
	case errors.Is(err, receivegate.ErrDraining):
		// A first claim round was fenced by graceful shutdown. It may be a retry
		// of a claim that committed before its response was lost, so never answer
		// with an empty success that would make the client discard its AttemptID.
		w.Header().Set("Retry-After", "1")
		writeErr(w, http.StatusServiceUnavailable, "unavailable", err.Error())
	case errors.Is(err, context.Canceled):
		writeErr(w, 499, "canceled", err.Error())
	default:
		writeErr(w, http.StatusInternalServerError, "internal", err.Error())
	}
}

// ── QueueService handlers ───────────────────────────────────────────────────

func (s *Server) handleSend(w http.ResponseWriter, r *http.Request) {
	var req wire.SendWithMessageTTLsRequest
	if err := decode(r, &req); err != nil {
		decodeErr(w, err)
		return
	}
	ctx := r.Context()
	logf(w, "queue", req.Queue)
	if req.TTLMs < 0 {
		s.fail(w, fmt.Errorf("%w: ttl_ms must be non-negative", engine.ErrInvalidArgument))
		return
	}
	// ttl_ms is the legacy whole-request form. A mixed batch uses the additive
	// send-only SendMessage.ttl_ms field; accepting both would leave precedence
	// ambiguous for raw HTTP clients.
	outs := make([]engine.OutMessage, len(req.Messages))
	for i, m := range req.Messages {
		if m.TTLMs < 0 {
			s.fail(w, fmt.Errorf("%w: messages[%d].ttl_ms must be non-negative", engine.ErrInvalidArgument, i))
			return
		}
		if req.TTLMs > 0 && m.TTLMs > 0 {
			s.fail(w, fmt.Errorf("%w: ttl_ms and messages[%d].ttl_ms cannot both be set", engine.ErrInvalidArgument, i))
			return
		}
		o := m.ToOut()
		if req.TTLMs > 0 {
			o.TTLMs = req.TTLMs
		}
		outs[i] = o
	}
	// A negative scheduled_enqueue_time_ms is not a valid instant. It used to fall through the
	// `> 0` test into the plain-send branch and enqueue the message ACTIVE with a 200 — raw
	// HTTP silently disagreeing with embedded/CLI, where the engine rejects it (round-2 §3.1).
	// Zero still means "not scheduled" (the field is omitempty).
	if req.ScheduledEnqueueTimeMs < 0 {
		s.fail(w, fmt.Errorf("%w: scheduled_enqueue_time_ms must be a future epoch-ms instant (got %d)", engine.ErrInvalidArgument, req.ScheduledEnqueueTimeMs))
		return
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
// would mistake for success. Replaying the SAME request returns success (the engine
// consults the settlement-receipt table); a request that differs in the message, the
// verb, or an effect-bearing argument is not a replay and gets lock_lost.
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

// handleRenewBatch extends many leases in ONE request and ONE statement — not a transaction; the
// single statement is what lets every Ok be decided against one clock reading. N separate Renew
// calls cost N round trips, which on a slow link takes longer than the lease they are meant to
// save. Ok reports the lease was live when that statement finished; the deadline the caller should
// pace against is locked_until in the per-item result.
func (s *Server) handleRenewBatch(w http.ResponseWriter, r *http.Request) {
	var req wire.RenewBatchRequest
	if err := decode(r, &req); err != nil {
		decodeErr(w, err)
		return
	}
	logf(w, "queue", req.Queue, "n", len(req.Messages))
	items := make([]engine.SettleItem, len(req.Messages))
	for i, m := range req.Messages {
		items[i] = engine.SettleItem{SeqNumber: m.SeqNumber, LockToken: m.LockToken}
	}
	results, err := s.eng.RenewBatch(r.Context(), req.Queue, items)
	if err != nil {
		s.fail(w, err)
		return
	}
	out := make([]wire.SettleItemResult, len(results))
	for i, res := range results {
		out[i] = wire.SettleItemResult{
			SeqNumber: res.SeqNumber, Ok: res.Ok, LockedUntilMs: res.LockedUntilMs,
		}
	}
	writeJSON(w, wire.RenewBatchResponse{Results: out})
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

func (s *Server) handleDeleteQueue(w http.ResponseWriter, r *http.Request) {
	var req wire.DeleteQueueRequest
	if err := decode(r, &req); err != nil {
		decodeErr(w, err)
		return
	}
	logf(w, "queue", req.Name, "force", req.Force)
	result, err := s.eng.DeleteQueueWithOptions(r.Context(), req.Name, engine.DeleteOptions{Force: req.Force})
	if err != nil {
		logDeleteAcceptance(w, err)
		s.fail(w, err)
		return
	}
	logf(w, "acceptance", "accepted", "result_state", result.State, "deleted", result.Deleted)
	writeJSON(w, wire.DeleteQueueResponse{Deleted: result.Deleted, State: result.State})
}

func (s *Server) handleDeleteSubscription(w http.ResponseWriter, r *http.Request) {
	var req wire.DeleteSubscriptionRequest
	if err := decode(r, &req); err != nil {
		decodeErr(w, err)
		return
	}
	logf(w, "topic", req.Topic, "sub", req.Name, "force", req.Force)
	result, err := s.eng.DeleteSubscriptionWithOptions(r.Context(), req.Topic, req.Name, engine.DeleteOptions{Force: req.Force})
	if err != nil {
		logDeleteAcceptance(w, err)
		s.fail(w, err)
		return
	}
	logf(w, "acceptance", "accepted", "result_state", result.State, "deleted", result.Deleted)
	writeJSON(w, wire.DeleteSubscriptionResponse{Deleted: result.Deleted, State: result.State})
}

func logDeleteAcceptance(w http.ResponseWriter, err error) {
	acceptance := "rejected"
	if errors.Is(err, engine.ErrOutcomeUnknown) {
		acceptance = "unknown"
	}
	logf(w, "acceptance", acceptance)
}

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
	var req wire.SubscribeWithOptionsRequest
	if err := decode(r, &req); err != nil {
		decodeErr(w, err)
		return
	}
	logf(w, "topic", req.Topic, "sub", req.Name)
	if req.TTLMs < 0 {
		s.fail(w, fmt.Errorf("%w: subscription ttl_ms must be non-negative", engine.ErrInvalidArgument))
		return
	}
	if err := s.eng.SubscribeWithOptions(r.Context(), req.Topic, req.Name, req.Filter, engine.SubscribeOptions{TTLMs: req.TTLMs}); err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, wire.Empty{})
}

func (s *Server) handleListQueues(w http.ResponseWriter, r *http.Request) {
	var req wire.Empty
	if err := decode(r, &req); err != nil {
		decodeErr(w, err)
		return
	}
	qs, err := s.eng.ListQueues(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	resp := wire.ListQueuesResponse{Queues: make([]wire.QueueInfoJSON, len(qs))}
	for i, q := range qs {
		resp.Queues[i] = wire.QueueInfoJSON{
			Name: q.Name, Kind: q.Kind, State: q.State, LockDurationMs: q.LockDurationMs,
			MaxDeliveryCount: q.MaxDeliveryCount, DefaultTTLMs: q.DefaultTTLMs, DedupWindowMs: q.DedupWindowMs,
		}
	}
	writeJSON(w, resp)
}

func (s *Server) handleListSubscriptions(w http.ResponseWriter, r *http.Request) {
	var req wire.Empty
	if err := decode(r, &req); err != nil {
		decodeErr(w, err)
		return
	}
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

func (s *Server) handleListSubscriptionsPage(w http.ResponseWriter, r *http.Request) {
	var req wire.ListSubscriptionsPageRequest
	if err := decode(r, &req); err != nil {
		decodeErr(w, err)
		return
	}
	page, err := s.eng.ListSubscriptionsWithOptions(r.Context(), engine.ListSubscriptionsOptions{
		Topic: req.Topic, After: req.After, Limit: req.Limit, IncludeDeleting: req.IncludeDeleting,
	})
	if err != nil {
		s.fail(w, err)
		return
	}
	resp := wire.ListSubscriptionsPageResponse{
		Subscriptions: make([]wire.SubscriptionPageJSON, len(page.Subscriptions)),
		Total:         page.Total,
		NextAfter:     page.NextAfter,
	}
	for i, su := range page.Subscriptions {
		resp.Subscriptions[i] = wire.SubscriptionPageJSON{
			Topic: su.Topic, Name: su.Name, Expr: su.Expr, State: su.State,
			CreatedAtMs: su.CreatedAtMs, ExpiresAtMs: su.ExpiresAtMs,
		}
	}
	writeJSON(w, resp)
}

// handleStatus reports a desensitized runtime snapshot for an ops view: backend kind,
// redacted location, read latency, local footprint, counts, uptime. Behind auth.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	var req wire.Empty
	if err := decode(r, &req); err != nil {
		decodeErr(w, err)
		return
	}
	ctx := r.Context()
	st, queues, subs := s.eng.RuntimeStatus(ctx)
	version := s.Version
	if version == "" {
		version = "dev"
	}
	w.Header().Set("Cache-Control", "no-store")
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
		ServerTimeMs:  st.ServerTimeMs,
		UptimeMs:      time.Since(s.started).Milliseconds(),
		Auth:          s.authenticationEnabled(),
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
	logf(w, "queue", req.Queue, "max", req.Max, "older_than_ms", req.OlderThanMs)
	purged, err := s.eng.Purge(r.Context(), req.Queue, engine.RedriveOptions{
		Max: req.Max, OlderThanMs: req.OlderThanMs,
	})
	if err != nil {
		s.fail(w, err)
		return
	}
	logf(w, "n", purged, "purged", purged)
	writeJSON(w, wire.PurgeResponse{Purged: purged})
}

// ── AuthService handlers ────────────────────────────────────────────────────

func (s *Server) handleCreateKey(w http.ResponseWriter, r *http.Request) {
	var req wire.CreateKeyRequest
	if err := decode(r, &req); err != nil {
		decodeErr(w, err)
		return
	}
	s.logPublicKeyID(w, "key_id", req.ID)
	permissions, err := engine.ParseKeyPermissions(req.Permissions)
	if err != nil {
		s.fail(w, err)
		return
	}
	key, token, err := s.eng.CreateAccessKey(r.Context(), engine.CreateAccessKeyOptions{
		ID: req.ID, Name: req.Name, Permissions: permissions, ExpiresAtMs: req.ExpiresAtMs,
	})
	if err != nil {
		s.fail(w, err)
		return
	}
	logf(w, "result_state", "created")
	writeJSON(w, wire.CreateKeyResponse{Key: wire.FromAccessKey(key), Token: token})
}

func (s *Server) handleListKeys(w http.ResponseWriter, r *http.Request) {
	var req wire.ListKeysRequest
	if err := decode(r, &req); err != nil {
		decodeErr(w, err)
		return
	}
	page, err := s.eng.ListAccessKeysWithOptions(r.Context(), engine.ListAccessKeysOptions{AfterID: req.AfterID, Limit: req.Limit, Sort: req.Sort})
	if err != nil {
		s.fail(w, err)
		return
	}
	resp := wire.ListKeysResponse{Keys: make([]wire.AccessKey, len(page.Keys)), NextAfterID: page.NextAfterID}
	for i, key := range page.Keys {
		resp.Keys[i] = wire.FromAccessKey(key)
	}
	writeJSON(w, resp)
}

func (s *Server) handleRevokeKey(w http.ResponseWriter, r *http.Request) {
	var req wire.RevokeKeyRequest
	if err := decode(r, &req); err != nil {
		decodeErr(w, err)
		return
	}
	s.logPublicKeyID(w, "key_id", req.ID)
	if err := s.eng.RevokeAccessKey(r.Context(), req.ID); err != nil {
		s.fail(w, err)
		return
	}
	logf(w, "result_state", "revoked")
	writeJSON(w, wire.RevokeKeyResponse{Ok: true})
}

// logPublicKeyID records a syntactically valid public ID unless its bytes are
// also configured as a static administrator or monitor credential. Static
// credentials are intentionally unconstrained strings, so a valid-looking ID
// can still be a secret supplied in the wrong request field.
func (s *Server) logPublicKeyID(w http.ResponseWriter, field, id string) {
	if !authkey.ValidID(id) {
		return
	}
	secret := 0
	for token := range s.tokens {
		if len(token) == len(id) {
			secret |= subtle.ConstantTimeCompare([]byte(token), []byte(id))
		}
	}
	for _, token := range s.MonitorTokens {
		token = strings.TrimSpace(token)
		if len(token) == len(id) {
			secret |= subtle.ConstantTimeCompare([]byte(token), []byte(id))
		}
	}
	if secret == 0 {
		logf(w, field, id)
	}
}
