package server

import "net/http"

// cors optionally makes the broker reachable from a browser app served by a different
// origin — the standalone admin console pointed at this broker. It is disabled when
// Server.CORS == "" (the library default). The broker binary defaults it to "*" only while
// auth is on — every RPC then requires a Bearer token and the API sets no cookies, so a
// permissive Allow-Origin grants a cross-origin page nothing it couldn't do with the token
// from anywhere else. With auth disabled the binary defaults CORS off (a wildcard would let
// any page drive an unauthenticated broker). Set MQLITE_CORS to a specific origin to narrow it.
//
// It is the outermost middleware so a CORS preflight (OPTIONS, which carries no
// Authorization header) is answered here, before auth would reject it.
func (s *Server) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.CORS == "" {
			next.ServeHTTP(w, r)
			return
		}
		h := w.Header()
		h.Set("Access-Control-Allow-Origin", s.CORS)
		if s.CORS != "*" {
			h.Add("Vary", "Origin") // response varies by origin when not wildcard
		}
		if r.Method == http.MethodOptions {
			h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			h.Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
			h.Set("Access-Control-Max-Age", "86400")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
