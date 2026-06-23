package server

import (
	"embed"
	"io/fs"
	"net/http"
)

// The built admin console (mqlite-web dist), baked into the binary. The web project is the
// source of truth; only its built output is committed here and embedded — the Go build
// (and the Docker image) stay node-free. Sync is one-way: web → main.
//
//go:embed all:web
var webFS embed.FS

// console serves the embedded console under /ui, gated by Server.UI. The SPA is built
// path-relative (base "./"), so index.html and its hashed assets resolve under /ui/.
// Static files only — no node, no server-side rendering. When UI is off the path 404s
// (and is omitted from the discovery card), so a deployment can run headless.
func (s *Server) console() http.Handler {
	sub, err := fs.Sub(webFS, "web")
	if err != nil {
		panic("embed web: " + err.Error()) // guaranteed at build time
	}
	files := http.StripPrefix("/ui/", http.FileServer(http.FS(sub)))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.UI {
			writeErr(w, http.StatusNotFound, "not_found", "no such path: "+r.URL.Path)
			return
		}
		files.ServeHTTP(w, r)
	})
}
