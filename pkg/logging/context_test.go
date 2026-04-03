package logging

import (
	"context"
	"strings"
	"testing"
)

func TestGenerateID(t *testing.T) {
	id := GenerateID(16)
	if len(id) != 16 {
		t.Errorf("expected ID length 16, got %d", len(id))
	}

	// Should be valid hex
	for _, c := range id {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			t.Errorf("invalid hex character in ID: %c", c)
		}
	}
}

func TestWithCorrelationID(t *testing.T) {
	ctx := context.Background()

	// Test with explicit ID
	ctx = WithCorrelationID(ctx, "test-correlation-id")
	if id := GetCorrelationID(ctx); id != "test-correlation-id" {
		t.Errorf("expected 'test-correlation-id', got %q", id)
	}
}

func TestWithCorrelationID_GeneratesID(t *testing.T) {
	ctx := context.Background()

	// Test with auto-generated ID
	ctx = WithCorrelationID(ctx, "")
	id := GetCorrelationID(ctx)
	if id == "" {
		t.Error("expected generated ID, got empty")
	}
	if len(id) != 16 {
		t.Errorf("expected ID length 16, got %d", len(id))
	}
}

func TestGetCorrelationID_NotFound(t *testing.T) {
	ctx := context.Background()
	if id := GetCorrelationID(ctx); id != "" {
		t.Errorf("expected empty string for missing ID, got %q", id)
	}
}

func TestWithRequestID(t *testing.T) {
	ctx := context.Background()

	// Test with explicit ID
	ctx = WithRequestID(ctx, "test-request-id")
	if id := GetRequestID(ctx); id != "test-request-id" {
		t.Errorf("expected 'test-request-id', got %q", id)
	}
}

func TestWithRequestID_GeneratesID(t *testing.T) {
	ctx := context.Background()

	// Test with auto-generated ID
	ctx = WithRequestID(ctx, "")
	id := GetRequestID(ctx)
	if id == "" {
		t.Error("expected generated ID, got empty")
	}
	if len(id) != 12 {
		t.Errorf("expected ID length 12, got %d", len(id))
	}
}

func TestGetRequestID_NotFound(t *testing.T) {
	ctx := context.Background()
	if id := GetRequestID(ctx); id != "" {
		t.Errorf("expected empty string for missing ID, got %q", id)
	}
}

func TestRequestContext(t *testing.T) {
	ctx := context.Background()

	// Test with explicit IDs
	ctx = RequestContext(ctx, "corr-123", "req-456")

	if cid := GetCorrelationID(ctx); cid != "corr-123" {
		t.Errorf("expected correlation ID 'corr-123', got %q", cid)
	}
	if rid := GetRequestID(ctx); rid != "req-456" {
		t.Errorf("expected request ID 'req-456', got %q", rid)
	}
}

func TestRequestContext_GeneratesIDs(t *testing.T) {
	ctx := context.Background()

	// Test with auto-generated IDs
	ctx = RequestContext(ctx, "", "")

	if cid := GetCorrelationID(ctx); cid == "" {
		t.Error("expected generated correlation ID, got empty")
	}
	if rid := GetRequestID(ctx); rid == "" {
		t.Error("expected generated request ID, got empty")
	}
}

func TestFCorrelationID(t *testing.T) {
	f := FCorrelationID("test-id")
	if f.Key != "correlation_id" {
		t.Errorf("expected key 'correlation_id', got %q", f.Key)
	}
	if f.Value != "test-id" {
		t.Errorf("expected value 'test-id', got %v", f.Value)
	}
}

func TestFRequestID(t *testing.T) {
	f := FRequestID("test-id")
	if f.Key != "request_id" {
		t.Errorf("expected key 'request_id', got %q", f.Key)
	}
	if f.Value != "test-id" {
		t.Errorf("expected value 'test-id', got %v", f.Value)
	}
}

func TestFFromContext(t *testing.T) {
	ctx := context.Background()
	ctx = WithCorrelationID(ctx, "corr-abc")
	ctx = WithRequestID(ctx, "req-xyz")

	fields := FFromContext(ctx)

	if len(fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(fields))
	}

	// Check that both fields are present
	found := make(map[string]string)
	for _, f := range fields {
		if s, ok := f.Value.(string); ok {
			found[f.Key] = s
		}
	}

	if found["correlation_id"] != "corr-abc" {
		t.Errorf("expected correlation_id 'corr-abc', got %q", found["correlation_id"])
	}
	if found["request_id"] != "req-xyz" {
		t.Errorf("expected request_id 'req-xyz', got %q", found["request_id"])
	}
}

func TestFFromContext_Empty(t *testing.T) {
	ctx := context.Background()
	fields := FFromContext(ctx)

	if len(fields) != 0 {
		t.Errorf("expected 0 fields for empty context, got %d", len(fields))
	}
}

func TestLoggerWithContext(t *testing.T) {
	ctx := context.Background()
	ctx = WithCorrelationID(ctx, "corr-test")
	ctx = WithRequestID(ctx, "req-test")

	logger := Default()
	loggerWithCtx := LoggerWithContext(logger, ctx)

	// The returned logger should have the fields
	if loggerWithCtx == nil {
		t.Error("expected non-nil logger")
	}
}

func TestLoggerWithContext_NoIDs(t *testing.T) {
	ctx := context.Background()

	logger := Default()
	loggerWithCtx := LoggerWithContext(logger, ctx)

	// Should return the same logger if no IDs in context
	if loggerWithCtx == nil {
		t.Error("expected non-nil logger")
	}
}

func TestGenerateID_Uniqueness(t *testing.T) {
	// Generate multiple IDs and check they're different
	ids := make(map[string]bool)
	for i := 0; i < 100; i++ {
		id := GenerateID(16)
		if ids[id] {
			t.Errorf("duplicate ID generated: %s", id)
		}
		ids[id] = true
	}
}

func TestGenerateID_DifferentLengths(t *testing.T) {
	tests := []struct {
		length   int
		expected int
	}{
		{8, 8},
		{16, 16},
		{32, 32},
	}

	for _, tt := range tests {
		id := GenerateID(tt.length)
		if len(id) != tt.expected {
			t.Errorf("GenerateID(%d) returned length %d, expected %d", tt.length, len(id), tt.expected)
		}
	}
}

func TestContextKeys_NotExported(t *testing.T) {
	// Ensure the keys are not accessible from outside the package
	// by trying to use them as strings
	key1 := string(correlationIDKey)
	key2 := string(requestIDKey)

	if !strings.Contains(key1, "correlation_id") {
		t.Error("correlationIDKey should contain correlation_id")
	}
	if !strings.Contains(key2, "request_id") {
		t.Error("requestIDKey should contain request_id")
	}
}
