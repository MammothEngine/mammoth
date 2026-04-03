// Package circuitbreaker implements the circuit breaker pattern for fault tolerance.
package circuitbreaker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// State represents the circuit breaker state.
type State int32

const (
	// StateClosed - normal operation, requests pass through.
	StateClosed State = iota
	// StateOpen - failure threshold reached, requests fail fast.
	StateOpen
	// StateHalfOpen - testing if the system has recovered.
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// ErrCircuitOpen is returned when the circuit breaker is open.
var ErrCircuitOpen = errors.New("circuit breaker is open")

// Config holds circuit breaker configuration.
type Config struct {
	// FailureThreshold is the number of failures before opening the circuit.
	FailureThreshold uint32
	// SuccessThreshold is the number of consecutive successes needed to close the circuit.
	SuccessThreshold uint32
	// Timeout is the duration to wait before transitioning from open to half-open.
	Timeout time.Duration
	// MaxRequests is the maximum number of requests allowed in half-open state.
	MaxRequests uint32
}

// DefaultConfig returns a default circuit breaker configuration.
func DefaultConfig() Config {
	return Config{
		FailureThreshold: 5,
		SuccessThreshold: 3,
		Timeout:          30 * time.Second,
		MaxRequests:      1,
	}
}

// Breaker implements the circuit breaker pattern.
type Breaker struct {
	config      Config
	state       atomic.Int32
	failures    atomic.Uint32
	successes   atomic.Uint32
	lastFailure atomic.Int64 // Unix timestamp
	halfOpenReqs atomic.Uint32

	// Callbacks
	onStateChange func(from, to State)
}

// New creates a new circuit breaker with the given configuration.
func New(config Config) *Breaker {
	return NewWithCallbacks(config, nil)
}

// NewWithCallbacks creates a new circuit breaker with state change callbacks.
func NewWithCallbacks(config Config, onStateChange func(from, to State)) *Breaker {
	b := &Breaker{
		config:        config,
		onStateChange: onStateChange,
	}
	b.state.Store(int32(StateClosed))
	return b
}

// State returns the current state of the circuit breaker.
func (b *Breaker) State() State {
	return State(b.state.Load())
}

// Allow checks if a request is allowed to proceed.
func (b *Breaker) Allow() bool {
	state := b.State()

	switch state {
	case StateClosed:
		return true
	case StateOpen:
		// Check if we should transition to half-open
		lastFailure := time.Unix(b.lastFailure.Load(), 0)
		if time.Since(lastFailure) > b.config.Timeout {
			// Only transition if we can allow at least one request
			if b.config.MaxRequests > 0 {
				b.transitionTo(StateHalfOpen)
				b.halfOpenReqs.Store(1) // Count the request we're about to allow
				return true
			}
			return false
		}
		return false
	case StateHalfOpen:
		// Allow limited requests in half-open state
		// Check before incrementing to avoid race conditions
		current := b.halfOpenReqs.Load()
		if current >= b.config.MaxRequests {
			return false
		}
		if b.halfOpenReqs.CompareAndSwap(current, current+1) {
			return true
		}
		// CAS failed, someone else got the slot
		return false
	default:
		return false
	}
}

// Execute runs the given function if the circuit allows it.
// Returns ErrCircuitOpen if the circuit is open.
func (b *Breaker) Execute(fn func() error) error {
	return b.ExecuteContext(context.Background(), fn)
}

// ExecuteContext runs the given function if the circuit allows it.
// Returns ErrCircuitOpen if the circuit is open.
// Respects context cancellation.
func (b *Breaker) ExecuteContext(ctx context.Context, fn func() error) error {
	// Check context cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if !b.Allow() {
		return ErrCircuitOpen
	}

	err := fn()
	b.RecordResult(err)
	return err
}

// RecordResult records the result of a request.
func (b *Breaker) RecordResult(err error) {
	if err != nil {
		b.RecordFailure()
	} else {
		b.RecordSuccess()
	}
}

// RecordFailure records a failure.
func (b *Breaker) RecordFailure() {
	b.failures.Add(1)
	b.successes.Store(0)
	b.lastFailure.Store(time.Now().Unix())

	// Check if we should open the circuit
	if b.failures.Load() >= b.config.FailureThreshold && b.State() == StateClosed {
		b.transitionTo(StateOpen)
	}
}

// RecordSuccess records a success.
func (b *Breaker) RecordSuccess() {
	b.failures.Store(0)

	state := b.State()
	if state == StateHalfOpen {
		// Increment successes in half-open state
		if b.successes.Add(1) >= b.config.SuccessThreshold {
			b.transitionTo(StateClosed)
			b.successes.Store(0)
		}
	}
}

// Reset resets the circuit breaker to closed state.
func (b *Breaker) Reset() {
	b.transitionTo(StateClosed)
	b.failures.Store(0)
	b.successes.Store(0)
	b.halfOpenReqs.Store(0)
}

// transitionTo transitions to a new state.
func (b *Breaker) transitionTo(newState State) {
	oldState := b.State()
	if oldState == newState {
		return
	}

	b.state.Store(int32(newState))

	// Reset counters on state change
	switch newState {
	case StateClosed:
		b.failures.Store(0)
		b.successes.Store(0)
	case StateOpen:
		b.successes.Store(0)
		b.halfOpenReqs.Store(0)
	case StateHalfOpen:
		b.successes.Store(0)
	}

	if b.onStateChange != nil {
		b.onStateChange(oldState, newState)
	}
}

// Metrics holds circuit breaker metrics.
type Metrics struct {
	State          State
	Failures       uint32
	Successes      uint32
	LastFailure    time.Time
	HalfOpenReqs   uint32
}

// GetMetrics returns current metrics.
func (b *Breaker) GetMetrics() Metrics {
	return Metrics{
		State:        b.State(),
		Failures:     b.failures.Load(),
		Successes:    b.successes.Load(),
		LastFailure:  time.Unix(b.lastFailure.Load(), 0),
		HalfOpenReqs: b.halfOpenReqs.Load(),
	}
}

// Manager manages multiple circuit breakers for different resources.
type Manager struct {
	config    Config
	breakers  map[string]*Breaker
	mu        sync.RWMutex
}

// NewManager creates a new circuit breaker manager.
func NewManager(config Config) *Manager {
	return &Manager{
		config:   config,
		breakers: make(map[string]*Breaker),
	}
}

// GetBreaker returns a circuit breaker for the given name.
// Creates one if it doesn't exist.
func (m *Manager) GetBreaker(name string) *Breaker {
	m.mu.RLock()
	b, exists := m.breakers[name]
	m.mu.RUnlock()

	if exists {
		return b
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if b, exists = m.breakers[name]; exists {
		return b
	}

	b = New(m.config)
	m.breakers[name] = b
	return b
}

// Execute runs a function with the named circuit breaker.
func (m *Manager) Execute(name string, fn func() error) error {
	return m.GetBreaker(name).Execute(fn)
}

// ExecuteContext runs a function with the named circuit breaker.
func (m *Manager) ExecuteContext(ctx context.Context, name string, fn func() error) error {
	return m.GetBreaker(name).ExecuteContext(ctx, fn)
}

// Reset resets all circuit breakers.
func (m *Manager) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, b := range m.breakers {
		b.Reset()
	}
}

// GetAllMetrics returns metrics for all circuit breakers.
func (m *Manager) GetAllMetrics() map[string]Metrics {
	m.mu.RLock()
	defer m.mu.RUnlock()

	metrics := make(map[string]Metrics, len(m.breakers))
	for name, b := range m.breakers {
		metrics[name] = b.GetMetrics()
	}
	return metrics
}

// Remove removes a circuit breaker.
func (m *Manager) Remove(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.breakers, name)
}
