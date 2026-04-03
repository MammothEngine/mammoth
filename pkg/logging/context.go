package logging

import (
	"context"
	"crypto/rand"
	"encoding/hex"
)

// Context keys for request tracing.
type contextKey string

const (
	correlationIDKey contextKey = "correlation_id"
	requestIDKey     contextKey = "request_id"
)

// GenerateID generates a random hex ID of the specified length.
func GenerateID(length int) string {
	bytes := make([]byte, length/2)
	if _, err := rand.Read(bytes); err != nil {
		// Fallback to timestamp-based ID if crypto/rand fails
		return fallbackID()
	}
	return hex.EncodeToString(bytes)
}

// fallbackID returns a simple timestamp-based ID.
func fallbackID() string {
	return hex.EncodeToString([]byte("fallback"))
}

// WithCorrelationID adds a correlation ID to the context.
// If id is empty, a new ID will be generated.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	if id == "" {
		id = GenerateID(16)
	}
	return context.WithValue(ctx, correlationIDKey, id)
}

// GetCorrelationID retrieves the correlation ID from context.
// Returns empty string if not found.
func GetCorrelationID(ctx context.Context) string {
	if v := ctx.Value(correlationIDKey); v != nil {
		if id, ok := v.(string); ok {
			return id
		}
	}
	return ""
}

// WithRequestID adds a request ID to the context.
// If id is empty, a new ID will be generated.
func WithRequestID(ctx context.Context, id string) context.Context {
	if id == "" {
		id = GenerateID(12)
	}
	return context.WithValue(ctx, requestIDKey, id)
}

// GetRequestID retrieves the request ID from context.
// Returns empty string if not found.
func GetRequestID(ctx context.Context) string {
	if v := ctx.Value(requestIDKey); v != nil {
		if id, ok := v.(string); ok {
			return id
		}
	}
	return ""
}

// RequestContext creates a new context with both correlation ID and request ID.
// If correlationID is empty, a new one will be generated.
// If requestID is empty, a new one will be generated.
func RequestContext(parent context.Context, correlationID, requestID string) context.Context {
	ctx := parent
	if correlationID != "" {
		ctx = WithCorrelationID(ctx, correlationID)
	} else {
		ctx = WithCorrelationID(ctx, "")
	}
	if requestID != "" {
		ctx = WithRequestID(ctx, requestID)
	} else {
		ctx = WithRequestID(ctx, "")
	}
	return ctx
}

// FCorrelationID creates a Field for correlation ID.
func FCorrelationID(id string) Field {
	return Field{Key: "correlation_id", Value: id}
}

// FRequestID creates a Field for request ID.
func FRequestID(id string) Field {
	return Field{Key: "request_id", Value: id}
}

// FFromContext extracts correlation and request IDs from context and returns them as fields.
func FFromContext(ctx context.Context) []Field {
	fields := make([]Field, 0, 2)

	if cid := GetCorrelationID(ctx); cid != "" {
		fields = append(fields, FCorrelationID(cid))
	}
	if rid := GetRequestID(ctx); rid != "" {
		fields = append(fields, FRequestID(rid))
	}

	return fields
}

// LoggerWithContext returns a logger with correlation and request ID fields from context.
func LoggerWithContext(l *Logger, ctx context.Context) *Logger {
	fields := FFromContext(ctx)
	if len(fields) > 0 {
		return l.WithFields(fields...)
	}
	return l
}
