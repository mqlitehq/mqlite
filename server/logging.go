package server

import (
	"net/http"
	"strings"
	"time"
)

// logging emits one access-log line per request through Server.Logger. The level is
// chosen by HTTP status (2xx info, 4xx warn, 5xx error) so a colour-aware handler tells
// them apart — except an idle/empty Receive (msgs=0) is demoted to Debug so the default
// Info stream isn't flooded with empty long-poll lines. Handlers enrich the line with
// per-request context (queue / counts / seq / message_id) via logf(w, …); writeErr adds
// the error code. Disabled (passthrough) when Logger is nil. Sits inside cors(), so a
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
		args := make([]any, 0, len(rec.kv)+4)
		args = append(args, "status", rec.status)
		args = append(args, rec.kv...) // queue / msgs / seq / msg_id / code, per handler
		args = append(args, "dur", dur.String())
		switch {
		case rec.status >= 500:
			s.Logger.Error(rpc, args...)
		case rec.status >= 400:
			s.Logger.Warn(rpc, args...)
		case rec.quiet:
			s.Logger.Debug(rpc, args...) // empty Receive — hidden at the default Info level
		default:
			s.Logger.Info(rpc, args...)
		}
	})
}

// statusRecorder captures the status code an inner handler writes (default 200 when the
// handler writes a body without an explicit WriteHeader), plus the per-request access-log
// fields handlers append (logf) and the error code (writeErr).
type statusRecorder struct {
	http.ResponseWriter
	status int
	kv     []any // key/value pairs appended to the access-log line
	quiet  bool  // log at Debug even on success (empty Receive)
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// logf appends key/value pairs to this request's access-log line. It is a no-op unless w
// is the logging recorder (i.e. Server.Logger is configured), so handlers may call it
// unconditionally on the hot path — the cost is one type assertion plus a slice append.
func logf(w http.ResponseWriter, kv ...any) {
	if rec, ok := w.(*statusRecorder); ok {
		rec.kv = append(rec.kv, kv...)
	}
}

// logQuiet marks this request to log at Debug even on a 2xx — used for an empty Receive
// (an idle long-poll wait that returned nothing) so it doesn't flood the Info stream.
func logQuiet(w http.ResponseWriter) {
	if rec, ok := w.(*statusRecorder); ok {
		rec.quiet = true
	}
}
