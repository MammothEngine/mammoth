package admin

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCleanPath(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"", "/"},
		{"/", "/"},
		{"/api/v1/", "/api/v1"},
		{"/api/v1", "/api/v1"},
		{"/api///", "/api"},
	}

	for _, tt := range tests {
		got := cleanPath(tt.input)
		if got != tt.expected {
			t.Errorf("cleanPath(%q) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}

func TestMatchPattern(t *testing.T) {
	tests := []struct {
		pattern  string
		path     string
		wantOk   bool
		wantParams map[string]string
	}{
		{"/api/v1", "/api/v1", true, map[string]string{}},
		{"/api/v1", "/api/v2", false, nil},
		{"/api/:id", "/api/123", true, map[string]string{"id": "123"}},
		{"/api/:id", "/api", false, nil},
		{"/db/:db/coll/:coll", "/db/test/coll/users", true, map[string]string{"db": "test", "coll": "users"}},
	}

	for _, tt := range tests {
		params, ok := matchPattern(tt.pattern, tt.path)
		if ok != tt.wantOk {
			t.Errorf("matchPattern(%q, %q) ok = %v, want %v", tt.pattern, tt.path, ok, tt.wantOk)
		}
		if tt.wantOk && tt.wantParams != nil {
			for k, v := range tt.wantParams {
				if params[k] != v {
					t.Errorf("matchPattern(%q, %q) params[%q] = %q, want %q", tt.pattern, tt.path, k, params[k], v)
				}
			}
		}
	}
}

func TestRouter_Handle(t *testing.T) {
	r := NewRouter()
	
	var capturedParams map[string]string
	r.Handle("GET", "/test/:id", func(w http.ResponseWriter, req *http.Request, p map[string]string) {
		capturedParams = p
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest("GET", "/test/123", nil)
	w := httptest.NewRecorder()
	
	r.ServeHTTP(w, req)
	
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if capturedParams["id"] != "123" {
		t.Errorf("id param = %q, want 123", capturedParams["id"])
	}
}

func TestRouter_NotFound(t *testing.T) {
	r := NewRouter()
	r.Handle("GET", "/test", func(w http.ResponseWriter, req *http.Request, p map[string]string) {
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest("GET", "/notfound", nil)
	w := httptest.NewRecorder()
	
	r.ServeHTTP(w, req)
	
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

func TestRouter_MethodNotAllowed(t *testing.T) {
	r := NewRouter()
	r.Handle("GET", "/test", func(w http.ResponseWriter, req *http.Request, p map[string]string) {
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest("POST", "/test", nil)
	w := httptest.NewRecorder()
	
	r.ServeHTTP(w, req)
	
	// Should return 404 since method doesn't match (router doesn't have method-specific matching)
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

func TestDefaultServeStatic(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()

	defaultServeStatic(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	ct := w.Header().Get("Content-Type")
	if ct != "text/html; charset=utf-8" {
		t.Errorf("content-type = %q, want text/html", ct)
	}
	body := w.Body.String()
	if !strings.Contains(body, "Static files not embedded") {
		t.Errorf("body should contain 'Static files not embedded', got: %s", body)
	}
}
