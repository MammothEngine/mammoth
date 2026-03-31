package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHealthServer(t *testing.T) {
	server := &healthServer{
		startTime: time.Now(),
		version:   "1.0.0",
	}

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	contentType := w.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("expected Content-Type application/json, got %s", contentType)
	}

	var response map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("failed to parse JSON response: %v", err)
	}

	if response["status"] != "ok" {
		t.Errorf("expected status 'ok', got %v", response["status"])
	}

	if response["version"] != "1.0.0" {
		t.Errorf("expected version '1.0.0', got %v", response["version"])
	}

	uptime, ok := response["uptime"].(string)
	if !ok || uptime == "" {
		t.Error("expected uptime to be a non-empty string")
	}
}

func TestHealthServerUptime(t *testing.T) {
	// Create server with fixed start time
	server := &healthServer{
		startTime: time.Now().Add(-5 * time.Second),
		version:   "0.9.0",
	}

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	server.ServeHTTP(w, req)

	var response map[string]any
	json.Unmarshal(w.Body.Bytes(), &response)

	// Uptime should contain "5s" or similar
	uptime := response["uptime"].(string)
	if uptime == "" {
		t.Error("uptime should not be empty")
	}
}
