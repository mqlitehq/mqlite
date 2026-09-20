package server

import (
	"context"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mqlitehq/mqlite/engine"
	"github.com/mqlitehq/mqlite/wire"
)

type requestKey struct{ rpc, code string }

var requestCodes = wire.ObservationRequestCodes()

type transportMetrics struct {
	mu       sync.Mutex
	requests map[requestKey]wire.RequestObservation
	auth     map[string]uint64
}

func authOutcome(w http.ResponseWriter, outcome string) {
	if rec, ok := w.(*statusRecorder); ok {
		rec.auth = outcome
	}
}

// requestCode bounds labels even if a new handler introduces an arbitrary code.
// The inventory test deliberately requires updating the public vocabulary too.
func requestCode(rec *statusRecorder) string {
	switch rec.code {
	case "unauthenticated", "permission_denied", "key_conflict", "not_found",
		"already_exists", "name_conflict", "group_required", "invalid_argument",
		"message_too_large", "outcome_unknown", "canceled", "internal", "lock_lost", "unimplemented":
		return rec.code
	}
	if rec.status >= 200 && rec.status < 400 {
		return "ok"
	}
	return "internal"
}

// observeRequests surrounds authentication and runs regardless of logging. Each
// request has one final auth outcome; a permission rejection replaces success.
func (s *Server) observeRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK, logging: s.Logger != nil}
		start := time.Now()
		next.ServeHTTP(rec, r)
		_, pattern := s.mux.Handler(r)
		rpc := strings.HasPrefix(r.URL.Path, "/mqlite.v1.") && pattern == r.URL.Path
		if !rpc && rec.auth == "" {
			return
		}
		s.transport.mu.Lock()
		defer s.transport.mu.Unlock()
		if rec.auth != "" {
			if s.transport.auth == nil {
				s.transport.auth = make(map[string]uint64)
			}
			s.transport.auth[rec.auth]++
		}
		if rpc {
			if s.transport.requests == nil {
				s.transport.requests = make(map[requestKey]wire.RequestObservation)
			}
			key := requestKey{strings.TrimPrefix(r.URL.Path, "/mqlite.v1."), requestCode(rec)}
			v := s.transport.requests[key]
			v.RPC, v.Code = key.rpc, key.code
			v.Count++
			v.DurationSeconds += time.Since(start).Seconds()
			s.transport.requests[key] = v
		}
	})
}

func (s *Server) httpSnapshot() wire.HTTPObservation {
	out := wire.HTTPObservation{State: "available", Requests: []wire.RequestObservation{},
		Authentication: []wire.AuthenticationObservation{}, HandlerLatency: s.rpcLat.snapshot()}
	s.transport.mu.Lock()
	// Emit real zero counters before the first request/error. Otherwise the first
	// isolated failure appears as an initial sample of one and rate/increase miss it.
	for _, path := range s.rpcPaths {
		rpc := strings.TrimPrefix(path, "/mqlite.v1.")
		for _, code := range requestCodes {
			v := s.transport.requests[requestKey{rpc, code}]
			v.RPC, v.Code = rpc, code
			out.Requests = append(out.Requests, v)
		}
	}
	for _, outcome := range []string{"success", "missing", "invalid", "expired", "revoked", "permission_denied", "backend_error"} {
		out.Authentication = append(out.Authentication, wire.AuthenticationObservation{Outcome: outcome, Count: s.transport.auth[outcome]})
	}
	s.transport.mu.Unlock()
	sort.Slice(out.Requests, func(i, j int) bool {
		a, b := out.Requests[i], out.Requests[j]
		if a.RPC != b.RPC {
			return a.RPC < b.RPC
		}
		return a.Code < b.Code
	})
	return out
}

func (s *Server) observation(ctx context.Context) wire.ObserveResponse {
	version := s.Version
	if version == "" {
		version = "dev"
	}
	access := "anonymous"
	if s.authenticationEnabled() {
		access = "manage"
		if p, _ := ctx.Value(permissionContextKey{}).(engine.KeyPermissions); p == monitorPermission {
			access = "monitor"
		}
	}
	return wire.ObserveResponse{ObservabilitySnapshot: s.eng.Observability(ctx), Version: version,
		Access: access, HTTP: s.httpSnapshot()}
}

func (s *Server) handleObserve(w http.ResponseWriter, r *http.Request) {
	var req wire.ObserveRequest
	if err := decode(r, &req); err != nil {
		decodeErr(w, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, s.observation(r.Context()))
}
