package admin

import (
	"embed"
	"io/fs"
	"net/http"
	"path/filepath"
	"strings"
)

//go:embed dist/*
var staticFiles embed.FS

func init() {
	serveStaticFn = serveEmbeddedStatic
}

func serveEmbeddedStatic(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if path == "/" || path == "" {
		path = "/index.html"
	}

	// Serve from embedded dist directory
	cleanPath := strings.TrimPrefix(path, "/")
	f, err := staticFiles.ReadFile("dist/" + cleanPath)
	if err != nil {
		// SPA fallback: serve index.html for unknown routes
		f, err = staticFiles.ReadFile("dist/index.html")
		if err != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(f)
		return
	}

	w.Header().Set("Content-Type", mimeForExt(filepath.Ext(cleanPath)))
	w.Write(f)
}

func mimeForExt(ext string) string {
	switch ext {
	case ".html":
		return "text/html; charset=utf-8"
	case ".css":
		return "text/css; charset=utf-8"
	case ".js":
		return "application/javascript; charset=utf-8"
	case ".json":
		return "application/json"
	case ".png":
		return "image/png"
	case ".svg":
		return "image/svg+xml"
	case ".ico":
		return "image/x-icon"
	default:
		return "application/octet-stream"
	}
}

// StaticFS returns the embedded filesystem for external use if needed.
func StaticFS() fs.FS {
	sub, _ := fs.Sub(staticFiles, "dist")
	return sub
}
