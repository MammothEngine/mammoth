// Package metrics provides production feature metrics collection.
package metrics

import (
	"sync"
	"sync/atomic"
)

// ProductionMetrics holds metrics for all production features.
type ProductionMetrics struct {
	// Rate limiting metrics
	RateLimitAllowed   *Counter
	RateLimitDenied    *Counter
	RateLimitActive    *Gauge

	// Circuit breaker metrics
	CBStateChanges *Counter
	CBOpenState    *Gauge
	CBHalfOpenState *Gauge
	CBClosedState  *Gauge

	// Retry metrics
	RetryAttempts   *Counter
	RetrySuccess    *Counter
	RetryFailure    *Counter

	// Query timeout metrics
	QueryTimeouts   *Counter

	// Connection metrics (additional)
	ActiveConnections *Gauge
	TotalConnections  *Counter

	mu sync.RWMutex
}

// NewProductionMetrics creates a new production metrics collector.
func NewProductionMetrics() *ProductionMetrics {
	return &ProductionMetrics{
		RateLimitAllowed:  NewCounter("mammoth_rate_limit_allowed_total"),
		RateLimitDenied:   NewCounter("mammoth_rate_limit_denied_total"),
		RateLimitActive:   NewGauge("mammoth_rate_limit_active"),
		CBStateChanges:    NewCounter("mammoth_circuit_breaker_state_changes_total"),
		CBOpenState:       NewGauge("mammoth_circuit_breaker_open"),
		CBHalfOpenState:   NewGauge("mammoth_circuit_breaker_half_open"),
		CBClosedState:     NewGauge("mammoth_circuit_breaker_closed"),
		RetryAttempts:     NewCounter("mammoth_retry_attempts_total"),
		RetrySuccess:      NewCounter("mammoth_retry_success_total"),
		RetryFailure:      NewCounter("mammoth_retry_failure_total"),
		QueryTimeouts:     NewCounter("mammoth_query_timeouts_total"),
		ActiveConnections: NewGauge("mammoth_connections_active"),
		TotalConnections:  NewCounter("mammoth_connections_total"),
	}
}

// RegisterToRegistry registers all production metrics to a registry.
func (pm *ProductionMetrics) RegisterToRegistry(reg *Registry) {
	reg.Register(pm.RateLimitAllowed)
	reg.Register(pm.RateLimitDenied)
	reg.Register(pm.RateLimitActive)
	reg.Register(pm.CBStateChanges)
	reg.Register(pm.CBOpenState)
	reg.Register(pm.CBHalfOpenState)
	reg.Register(pm.CBClosedState)
	reg.Register(pm.RetryAttempts)
	reg.Register(pm.RetrySuccess)
	reg.Register(pm.RetryFailure)
	reg.Register(pm.QueryTimeouts)
	reg.Register(pm.ActiveConnections)
	reg.Register(pm.TotalConnections)
}

// RecordRateLimitAllowed records an allowed request.
func (pm *ProductionMetrics) RecordRateLimitAllowed() {
	pm.RateLimitAllowed.Inc()
}

// RecordRateLimitDenied records a denied request.
func (pm *ProductionMetrics) RecordRateLimitDenied() {
	pm.RateLimitDenied.Inc()
}

// SetRateLimitActive sets the number of active rate limiters.
func (pm *ProductionMetrics) SetRateLimitActive(n int64) {
	pm.RateLimitActive.Set(n)
}

// RecordCBStateChange records a circuit breaker state change.
func (pm *ProductionMetrics) RecordCBStateChange() {
	pm.CBStateChanges.Inc()
}

// SetCBStates sets the counts for each circuit breaker state.
func (pm *ProductionMetrics) SetCBStates(open, halfOpen, closed int64) {
	pm.CBOpenState.Set(open)
	pm.CBHalfOpenState.Set(halfOpen)
	pm.CBClosedState.Set(closed)
}

// RecordRetryAttempt records a retry attempt.
func (pm *ProductionMetrics) RecordRetryAttempt() {
	pm.RetryAttempts.Inc()
}

// RecordRetrySuccess records a successful retry.
func (pm *ProductionMetrics) RecordRetrySuccess() {
	pm.RetrySuccess.Inc()
}

// RecordRetryFailure records a failed retry.
func (pm *ProductionMetrics) RecordRetryFailure() {
	pm.RetryFailure.Inc()
}

// RecordQueryTimeout records a query timeout.
func (pm *ProductionMetrics) RecordQueryTimeout() {
	pm.QueryTimeouts.Inc()
}

// IncActiveConnections increments active connections.
func (pm *ProductionMetrics) IncActiveConnections() {
	pm.ActiveConnections.Inc()
	pm.TotalConnections.Inc()
}

// DecActiveConnections decrements active connections.
func (pm *ProductionMetrics) DecActiveConnections() {
	pm.ActiveConnections.Dec()
}

// MetricsCollector is an interface for components that expose metrics.
type MetricsCollector interface {
	CollectMetrics() map[string]float64
}

// CollectorManager manages multiple metric collectors.
type CollectorManager struct {
	collectors map[string]MetricsCollector
	mu         sync.RWMutex
}

// NewCollectorManager creates a new collector manager.
func NewCollectorManager() *CollectorManager {
	return &CollectorManager{
		collectors: make(map[string]MetricsCollector),
	}
}

// RegisterCollector registers a metrics collector.
func (cm *CollectorManager) RegisterCollector(name string, c MetricsCollector) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.collectors[name] = c
}

// UnregisterCollector removes a metrics collector.
func (cm *CollectorManager) UnregisterCollector(name string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	delete(cm.collectors, name)
}

// CollectAll collects metrics from all registered collectors.
func (cm *CollectorManager) CollectAll() map[string]map[string]float64 {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	result := make(map[string]map[string]float64, len(cm.collectors))
	for name, c := range cm.collectors {
		result[name] = c.CollectMetrics()
	}
	return result
}

// SimpleGauge is a simple gauge implementation for external use.
type SimpleGauge struct {
	value atomic.Int64
}

// NewSimpleGauge creates a new simple gauge.
func NewSimpleGauge() *SimpleGauge {
	return &SimpleGauge{}
}

// Set sets the gauge value.
func (g *SimpleGauge) Set(v float64) {
	g.value.Store(int64(v * 1000000)) // Store with 6 decimal precision
}

// Get gets the gauge value.
func (g *SimpleGauge) Get() float64 {
	return float64(g.value.Load()) / 1000000
}

// Inc increments the gauge.
func (g *SimpleGauge) Inc() {
	g.value.Add(1000000)
}

// Dec decrements the gauge.
func (g *SimpleGauge) Dec() {
	g.value.Add(-1000000)
}

// SimpleCounter is a simple counter implementation.
type SimpleCounter struct {
	value atomic.Uint64
}

// NewSimpleCounter creates a new simple counter.
func NewSimpleCounter() *SimpleCounter {
	return &SimpleCounter{}
}

// Inc increments the counter.
func (c *SimpleCounter) Inc() {
	c.value.Add(1)
}

// Add adds a value to the counter.
func (c *SimpleCounter) Add(v uint64) {
	c.value.Add(v)
}

// Get gets the counter value.
func (c *SimpleCounter) Get() uint64 {
	return c.value.Load()
}
