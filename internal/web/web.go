// Package web serves the embedded management SPA. The build output in dist/
// is produced by `make web` (Vue 3 + Vite) and synced here before `go build`;
// the committed index.html stub is a placeholder that keeps plain `go build`
// working without a node toolchain and is always overwritten by the real
// build in Docker and CI.
package web

import (
	"embed"
	"io"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

//go:embed all:dist
var embedded embed.FS

// Handler returns an http.Handler serving the SPA:
//
//   - /assets/* are content-hashed by Vite and served with immutable caching;
//   - index.html is served with no-store so a redeployed UI is picked up
//     immediately (browser refresh must never pin a stale bundle);
//   - any extension-less GET that misses a file falls back to index.html —
//     history-mode routing, so deep links like /relays resolve client-side;
//   - anything else that misses is a plain 404.
func Handler() (http.Handler, error) {
	dist, err := fs.Sub(embedded, "dist")
	if err != nil {
		return nil, err
	}
	files := http.FileServerFS(dist)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
		if name == "" {
			name = "index.html"
		}
		if _, err := fs.Stat(dist, name); err != nil {
			// Miss: fall back to the SPA entry for extension-less GETs only.
			if r.Method == http.MethodGet && path.Ext(name) == "" {
				serveIndex(w, r, dist)
				return
			}
			http.NotFound(w, r)
			return
		}
		switch {
		case name == "index.html":
			w.Header().Set("Cache-Control", "no-store")
		case strings.HasPrefix(name, "assets/"):
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		}
		files.ServeHTTP(w, r)
	}), nil
}

// serveIndex writes the SPA entry with no-store caching. It reads through the
// same fs so the fallback honors whatever is embedded.
func serveIndex(w http.ResponseWriter, r *http.Request, dist fs.FS) {
	f, err := dist.Open("index.html")
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil || info.IsDir() {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// The embedded FS yields ReadSeekers; the assertion matches http.
	// ServeContent's contract.
	http.ServeContent(w, r, "index.html", info.ModTime(), f.(io.ReadSeeker))
}
