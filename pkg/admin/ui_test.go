package admin

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMimeForExt(t *testing.T) {
	tests := []struct {
		ext      string
		expected  string
	}{
		{".html", "text/html; charset=utf-8"},
		{".css", "text/css; charset=utf-8"},
		{".js", "application/javascript; charset=utf-8"},
		{".json", "application/json"},
		{".png", "image/png"},
		{".svg", "image/svg+xml"},
		{".ico", "image/x-icon"},
		{".unknown", "application/octet-stream"},
		{"", "application/octet-stream"},
	}

	for _, tt := range tests {
		got := mimeForExt(tt.ext)
		if got != tt.expected {
			t.Errorf("mimeForExt(%q) = %q, want %q", tt.ext, got, tt.expected)
		}
	}
}

func TestServeEmbeddedStatic_Root(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()

	serveEmbeddedStatic(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	ct := w.Header().Get("Content-Type")
	if ct != "text/html; charset=utf-8" {
		t.Errorf("content-type = %q, want text/html", ct)
	}
}

func TestServeEmbeddedStatic_IndexHTML(t *testing.T) {
	req := httptest.NewRequest("GET", "/index.html", nil)
	w := httptest.NewRecorder()

	serveEmbeddedStatic(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
}

func TestServeEmbeddedStatic_SPAFallback(t *testing.T) {
	req := httptest.NewRequest("GET", "/unknown-route", nil)
	w := httptest.NewRecorder()

	serveEmbeddedStatic(w, req)

	// Should fallback to index.html for SPA routing
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	ct := w.Header().Get("Content-Type")
	if ct != "text/html; charset=utf-8" {
		t.Errorf("content-type = %q, want text/html", ct)
	}
}

func TestStaticFS(t *testing.T) {
	fs := StaticFS()
	if fs == nil {
		t.Error("StaticFS() should not return nil")
	}
}

func TestServeEmbeddedStatic_CSS(t *testing.T) {
	req := httptest.NewRequest("GET", "/style.css", nil)
	w := httptest.NewRecorder()

	serveEmbeddedStatic(w, req)

	// CSS file doesn't exist in embedded fs, so should fallback to index.html (SPA behavior)
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
}
