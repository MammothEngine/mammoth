# Mammoth Engine - Production Features

This document describes the production-ready features implemented in Mammoth Engine.

## Overview

Mammoth Engine includes several production-grade features for reliability, observability, and fault tolerance:

1. **Context & Query Timeout** - Request cancellation and timeout handling
2. **Health Check Endpoints** - Kubernetes-compatible health probes
3. **Rate Limiting** - Token bucket rate limiting
4. **Structured Logging** - Correlation IDs and request tracing
5. **Circuit Breaker** - Fault tolerance pattern
6. **Retry/Backoff** - Exponential backoff retry mechanism

---

## 1. Context & Query Timeout

### Description
All wire protocol commands support context cancellation and timeout. This prevents long-running queries from consuming resources indefinitely.

### Configuration
```toml
[server]
query-timeout = "30s"  # Default timeout for queries
```

### Environment Variables
- `MAMMOTH_QUERY_TIMEOUT` - Override query timeout

### Usage
```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

response := handler.HandleWithContext(ctx, msg)
```

---

## 2. Health Check Endpoints

### Description
Admin API provides three health check endpoints compatible with Kubernetes and other orchestrators:

- `/health` - General health status with engine stats
- `/ready` - Readiness probe (returns 503 if not ready)
- `/live` - Liveness probe (always returns 200 if process is running)

### Usage
```bash
# Health check
curl http://localhost:8080/health

# Readiness check
curl http://localhost:8080/ready

# Liveness check
curl http://localhost:8080/live
```

### Response Format
```json
{
  "ok": true,
  "data": {
    "status": "healthy",
    "timestamp": "2026-04-03T10:30:00Z",
    "version": "1.0.0",
    "uptime": "2h30m"
  }
}
```

---

## 3. Rate Limiting

### Description
Token bucket rate limiting with both per-connection and global limits to protect the server from overload.

### Configuration
```toml
[server.rate-limit]
enabled = true
requests-per-second = 1000    # Per-connection rate
burst = 100                   # Per-connection burst
per-connection = true         # Enable per-connection limiting
global-rate = 10000           # Global rate across all connections
global-burst = 1000           # Global burst
wait-timeout = "100ms"        # Max wait time for token
```

### Environment Variables
- `MAMMOTH_RATE_LIMIT_ENABLED`
- `MAMMOTH_RATE_LIMIT_RPS`
- `MAMMOTH_RATE_LIMIT_BURST`
- `MAMMOTH_RATE_LIMIT_PER_CONNECTION`
- `MAMMOTH_RATE_LIMIT_GLOBAL_RATE`
- `MAMMOTH_RATE_LIMIT_GLOBAL_BURST`
- `MAMMOTH_RATE_LIMIT_WAIT_TIMEOUT`

---

## 4. Structured Logging

### Description
JSON structured logging with correlation IDs and request tracing for distributed systems.

### Features
- Correlation ID propagation across requests
- Request ID generation
- Context-aware logging

### Usage
```go
// Create context with correlation ID
ctx := logging.WithCorrelationID(context.Background(), "")

// Extract logging fields from context
fields := logging.FFromContext(ctx)

// Create logger with context
logger := logging.LoggerWithContext(log, ctx)
```

### Log Format
```json
{
  "ts": "2026-04-03T10:30:00.123456789Z",
  "level": "info",
  "component": "wire",
  "correlation_id": "a1b2c3d4e5f6",
  "request_id": "abc123def456",
  "msg": "command executed"
}
```

---

## 5. Circuit Breaker

### Description
Circuit breaker pattern for fault tolerance. Prevents cascading failures by rejecting requests when the system is unhealthy.

### States
- **Closed** - Normal operation, requests pass through
- **Open** - Failure threshold reached, requests fail fast
- **Half-Open** - Testing if system has recovered

### Configuration
```toml
[server.circuit-breaker]
enabled = true
failure-threshold = 5      # Failures before opening
success-threshold = 3      # Successes needed to close
timeout = "30s"            # Timeout before half-open
max-requests = 1           # Max requests in half-open
```

### Environment Variables
- `MAMMOTH_CIRCUIT_BREAKER_ENABLED`
- `MAMMOTH_CIRCUIT_BREAKER_FAILURE_THRESHOLD`
- `MAMMOTH_CIRCUIT_BREAKER_SUCCESS_THRESHOLD`
- `MAMMOTH_CIRCUIT_BREAKER_TIMEOUT`
- `MAMMOTH_CIRCUIT_BREAKER_MAX_REQUESTS`

---

## 6. Retry/Backoff

### Description
Exponential backoff retry mechanism with jitter for resilient operations.

### Features
- Configurable max retries
- Exponential backoff with multiplier
- Jitter to prevent thundering herd
- Context cancellation support
- Custom retryable error function

### Usage
```go
config := retry.Config{
    MaxRetries: 3,
    BaseDelay:  100 * time.Millisecond,
    MaxDelay:   30 * time.Second,
    Multiplier: 2.0,
    Jitter:     true,
}

err := retry.DoWithConfig(config, func() error {
    // Your operation here
    return nil
})
```

---

## Configuration Priority

Configuration is applied in the following order (later overrides earlier):

1. Default values
2. Config file (TOML)
3. CLI flags
4. Environment variables

## Example Production Deployment

### Docker Compose
```yaml
version: '3.8'
services:
  mammoth:
    image: mammoth-engine:latest
    ports:
      - "27017:27017"
      - "8080:8080"
      - "9100:9100"
    environment:
      MAMMOTH_LOG_LEVEL: info
      MAMMOTH_QUERY_TIMEOUT: 30s
      MAMMOTH_RATE_LIMIT_ENABLED: "true"
      MAMMOTH_RATE_LIMIT_RPS: "1000"
      MAMMOTH_CIRCUIT_BREAKER_ENABLED: "true"
    volumes:
      - ./data:/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3
```

### Kubernetes
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mammoth-engine
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: mammoth
        image: mammoth-engine:latest
        ports:
        - containerPort: 27017
        - containerPort: 8080
        env:
        - name: MAMMOTH_LOG_LEVEL
          value: "info"
        livenessProbe:
          httpGet:
            path: /live
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5
```

---

## Graceful Shutdown

The server supports graceful shutdown with the following features:

- **In-flight request tracking** - Waits for active requests to complete
- **Connection draining** - Stops accepting new connections
- **Background job cleanup** - Stops compaction, replication, etc.
- **Shutdown hooks** - Custom cleanup functions
- **Configurable timeouts** - Drain timeout and force timeout

### Configuration
```go
config := shutdown.Config{
    DrainTimeout: 30 * time.Second,  // Wait for in-flight requests
    ForceTimeout: 60 * time.Second,  // Maximum shutdown time
}
```

### Usage
```go
// Create shutdown manager
shutdownMgr := shutdown.NewManager(config)

// Register custom shutdown hook
shutdownMgr.RegisterHook(func(ctx context.Context) error {
    // Custom cleanup logic
    return nil
})

// Register background job
shutdownMgr.RegisterBackgroundJob("compaction", compactionJob)

// Wrap requests for tracking
if !shutdownMgr.StartRequest() {
    return // Server is shutting down
}
defer shutdownMgr.EndRequest()

// Initiate graceful shutdown
ctx := context.Background()
if err := shutdownMgr.Shutdown(ctx); err != nil {
    log.Printf("shutdown error: %v", err)
}
```

---

## Monitoring

### Prometheus Metrics
Available at `http://localhost:9100/metrics`:

- `mammoth_commands_total` - Total commands processed
- `mammoth_command_duration_seconds` - Command latency histogram
- `mammoth_commands_errors_total` - Total command errors
- `mammoth_connections_active` - Active connections gauge
- `mammoth_connections_total` - Total connections counter

### Production Feature Metrics
- `mammoth_rate_limit_allowed_total` - Allowed requests
- `mammoth_rate_limit_denied_total` - Denied requests
- `mammoth_rate_limit_active` - Active rate limiters
- `mammoth_circuit_breaker_state_changes_total` - Circuit breaker state changes
- `mammoth_circuit_breaker_open` - Open circuit breakers count
- `mammoth_circuit_breaker_half_open` - Half-open circuit breakers count
- `mammoth_circuit_breaker_closed` - Closed circuit breakers count
- `mammoth_retry_attempts_total` - Retry attempts
- `mammoth_retry_success_total` - Successful retries
- `mammoth_retry_failure_total` - Failed retries
- `mammoth_query_timeouts_total` - Query timeouts

### Health Check Integration

Example Kubernetes probes:
```yaml
livenessProbe:
  httpGet:
    path: /live
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 10

readinessProbe:
  httpGet:
    path: /ready
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 5
```

---

## Testing

Run all tests:
```bash
go test ./pkg/... -count=1
```

Run integration tests:
```bash
go test ./pkg/integration/... -v
```

Run benchmarks:
```bash
go test ./pkg/... -bench=. -benchmem
```
