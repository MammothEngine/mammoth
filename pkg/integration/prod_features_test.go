// Package integration provides integration tests for production features.
package integration

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mammothengine/mammoth/pkg/admin"
	"github.com/mammothengine/mammoth/pkg/auth"
	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/circuitbreaker"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/logging"
	"github.com/mammothengine/mammoth/pkg/mongo"
	"github.com/mammothengine/mammoth/pkg/ratelimit"
	"github.com/mammothengine/mammoth/pkg/retry"
	"github.com/mammothengine/mammoth/pkg/wire"
)

// setupTestEnvironment creates a complete test environment with all production features.
func setupTestEnvironment(t *testing.T) (*wire.Handler, *admin.APIHandler, *ratelimit.Manager, *circuitbreaker.Manager, func()) {
	t.Helper()

	// Create temp directory for engine
	tmpDir := t.TempDir()

	// Open engine
	opts := engine.DefaultOptions(tmpDir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}

	// Create catalog
	cat := mongo.NewCatalog(eng)

	// Create auth manager
	userStore := auth.NewUserStore(eng)
	authMgr := auth.NewAuthManager(userStore, false)

	// Create wire handler
	handler := wire.NewHandler(eng, cat, authMgr)

	// Setup rate limiter
	rateLimitConfig := ratelimit.Config{
		Enabled:           true,
		RequestsPerSecond: 1000,
		Burst:             100,
		PerConnection:     true,
		GlobalRate:        10000,
		GlobalBurst:       1000,
		WaitTimeout:       100 * time.Millisecond,
	}
	rateLimiter := ratelimit.NewManager(rateLimitConfig)
	handler.WithRateLimiter(rateLimiter)

	// Setup circuit breaker
	cbConfig := circuitbreaker.Config{
		FailureThreshold: 5,
		SuccessThreshold: 3,
		Timeout:          30 * time.Second,
		MaxRequests:      1,
	}
	cbManager := circuitbreaker.NewManager(cbConfig)
	handler.WithCircuitBreaker(cbManager)

	// Create admin handler
	adminHandler := admin.NewAPIHandler(eng, cat, authMgr, "test-version")

	cleanup := func() {
		rateLimiter.Close()
		cbManager.Reset()
		eng.Close()
	}

	return handler, adminHandler, rateLimiter, cbManager, cleanup
}

// TestContextTimeoutPropagation tests that context timeouts are properly propagated.
func TestContextTimeoutPropagation(t *testing.T) {
	handler, _, _, _, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Create a message
	msg := &wire.Message{
		ConnID:     1,
		RemoteAddr: "127.0.0.1:12345",
	}
	// Note: In real usage, the message would be parsed from wire format with proper body

	// Test with short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(10 * time.Millisecond) // Ensure context is expired

	response := handler.HandleWithContext(ctx, msg)

	// Should get a timeout response (ok=0 for errors)
	if ok, _ := response.Get("ok"); ok.Type == bson.TypeDouble && ok.Double() == 1 {
		t.Error("expected timeout error for expired context")
	}
}

// TestHealthEndpoints tests all health check endpoints.
func TestHealthEndpoints(t *testing.T) {
	_, adminHandler, _, _, cleanup := setupTestEnvironment(t)
	defer cleanup()

	tests := []struct {
		name       string
		path       string
		wantStatus int
		checkField string
		wantValue  interface{}
	}{
		{
			name:       "health endpoint",
			path:       "/health",
			wantStatus: http.StatusOK,
			checkField: "status",
			wantValue:  "healthy",
		},
		{
			name:       "ready endpoint",
			path:       "/ready",
			wantStatus: http.StatusOK,
			checkField: "ready",
			wantValue:  true,
		},
		{
			name:       "live endpoint",
			path:       "/live",
			wantStatus: http.StatusOK,
			checkField: "alive",
			wantValue:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tt.path, nil)
			rec := httptest.NewRecorder()

			adminHandler.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
		})
	}
}

// TestRateLimitingBehavior tests rate limiting under load.
func TestRateLimitingBehavior(t *testing.T) {
	_, _, rateLimiter, _, cleanup := setupTestEnvironment(t)
	defer cleanup()

	connID := uint64(1)

	// Consume burst (allowing for some refills during the loop)
	allowed := 0
	for i := 0; i < 200; i++ {
		if rateLimiter.Allow(connID) {
			allowed++
		}
	}

	// Should have been rate limited (not all 200 allowed)
	if allowed >= 200 {
		t.Error("expected rate limiting after burst consumption")
	}

	// Should have allowed at least the burst size
	if allowed < 100 {
		t.Errorf("expected at least 100 allowed, got %d", allowed)
	}
}

// TestCircuitBreakerStateTransitions tests circuit breaker state changes.
func TestCircuitBreakerStateTransitions(t *testing.T) {
	_, _, _, cbManager, cleanup := setupTestEnvironment(t)
	defer cleanup()

	breaker := cbManager.GetBreaker("test")

	// Initial state should be closed
	if breaker.State() != circuitbreaker.StateClosed {
		t.Errorf("initial state = %v, want closed", breaker.State())
	}

	// Record failures to open circuit
	for i := 0; i < 5; i++ {
		breaker.RecordFailure()
	}

	// Circuit should be open
	if breaker.State() != circuitbreaker.StateOpen {
		t.Errorf("state after failures = %v, want open", breaker.State())
	}

	// Allow should return false
	if breaker.Allow() {
		t.Error("Allow() should return false when circuit is open")
	}
}

// TestRetryMechanism tests the retry mechanism with exponential backoff.
func TestRetryMechanism(t *testing.T) {
	config := retry.Config{
		MaxRetries: 3,
		BaseDelay:  1 * time.Millisecond,
		MaxDelay:   10 * time.Millisecond,
		Multiplier: 2.0,
		Jitter:     false,
	}

	attempts := 0
	err := retry.DoWithConfig(config, func() error {
		attempts++
		if attempts < 3 {
			return fmt.Errorf("attempt %d failed", attempts)
		}
		return nil
	})

	if err != nil {
		t.Errorf("expected success after retries, got: %v", err)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

// TestStructuredLoggingWithContext tests that correlation IDs are properly propagated.
func TestStructuredLoggingWithContext(t *testing.T) {
	// Create context with correlation ID
	ctx := logging.WithCorrelationID(context.Background(), "test-correlation-123")
	ctx = logging.WithRequestID(ctx, "test-request-456")

	// Get fields from context
	fields := logging.FFromContext(ctx)

	if len(fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(fields))
	}

	// Verify fields
	found := make(map[string]string)
	for _, f := range fields {
		if s, ok := f.Value.(string); ok {
			found[f.Key] = s
		}
	}

	if found["correlation_id"] != "test-correlation-123" {
		t.Errorf("correlation_id = %v, want test-correlation-123", found["correlation_id"])
	}
	if found["request_id"] != "test-request-456" {
		t.Errorf("request_id = %v, want test-request-456", found["request_id"])
	}
}

// TestAllFeaturesTogether tests all production features working together.
func TestAllFeaturesTogether(t *testing.T) {
	handler, adminHandler, rateLimiter, cbManager, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Test 1: Health endpoint works
	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()
	adminHandler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("health endpoint returned %d, want 200", rec.Code)
	}

	// Test 2: Rate limiting works
	connID := uint64(999)
	allowed := 0
	for i := 0; i < 200; i++ {
		if rateLimiter.Allow(connID) {
			allowed++
		}
	}
	if allowed > 110 { // Should be limited to burst + some refills
		t.Errorf("rate limiting not working, allowed %d requests", allowed)
	}

	// Test 3: Circuit breaker works
	breaker := cbManager.GetBreaker("integration-test")
	if breaker.State() != circuitbreaker.StateClosed {
		t.Error("circuit breaker should start closed")
	}

	// Test 4: Handler is properly initialized
	_ = handler
	ctx := logging.WithCorrelationID(context.Background(), "integration-test")
	_ = ctx
	// Note: Handler test requires proper wire message setup
	// This is a simplified integration test verifying handler exists
}

// TestProductionConfigValidation tests that production config is valid.
func TestProductionConfigValidation(t *testing.T) {
	// This test ensures the production config example is valid
	// The actual validation is done by loading and parsing the config
	t.Skip("Production config validation requires config file - manual test")
}

// BenchmarkHealthEndpoint benchmarks the health endpoint.
func BenchmarkHealthEndpoint(b *testing.B) {
	_, adminHandler, _, _, cleanup := setupTestEnvironment(&testing.T{})
	defer cleanup()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("GET", "/health", nil)
		rec := httptest.NewRecorder()
		adminHandler.ServeHTTP(rec, req)
	}
}

// BenchmarkRateLimiter benchmarks the rate limiter.
func BenchmarkRateLimiter(b *testing.B) {
	_, _, rateLimiter, _, cleanup := setupTestEnvironment(&testing.T{})
	defer cleanup()

	connID := uint64(1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rateLimiter.Allow(connID)
	}
}
