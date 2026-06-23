package server

import (
	"net/http"
	"strings"
	"time"
)

// logging emits one access-log line per request through Server.Logger, choosing the level
// by HTTP status (2xx info, 4xx warn, 5xx error) so a colour-aware handler distinguishes
// them at a glance. Disabled (passthrough) when Logger is nil. It sits inside cors(), so a
// CORS preflight — answered in cors() — is not logged as request noise.
func (s *Server) logging(next http.Handler) http.Handler {
	if s.Logger == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(rec, r)

		// /mqlite.v1.QueueService/Send -> QueueService/Send (the message that matters).
		rpc := strings.TrimPrefix(r.URL.Path, "/mqlite.v1.")
		dur := time.Since(start).Round(time.Microsecond)
		switch {
		case rec.status >= 500:
			s.Logger.Error(rpc, "status", rec.status, "dur", dur.String())
		case rec.status >= 400:
			s.Logger.Warn(rpc, "status", rec.status, "dur", dur.String())
		default:
			s.Logger.Info(rpc, "status", rec.status, "dur", dur.String())
		}
	})
}

// statusRecorder captures the status code an inner handler writes (default 200 when the
// handler writes a body without an explicit WriteHeader).
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}
