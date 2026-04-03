package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandler(t *testing.T) {
	// Create a registry with some metrics
	reg := NewRegistry()
	c := NewCounter("test_requests_total")
	c.Add(10)
	reg.Register(c)

	// Get the handler
	handler := Handler(reg)
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}

	// Test the handler
	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	contentType := rec.Header().Get("Content-Type")
	if !strings.Contains(contentType, "text/plain") {
		t.Errorf("expected text/plain content-type, got %s", contentType)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "test_requests_total") {
		t.Errorf("expected body to contain test_requests_total, got: %s", body)
	}
	if !strings.Contains(body, "10") {
		t.Errorf("expected body to contain value 10, got: %s", body)
	}
}
