// Package shutdown provides graceful shutdown management for the server.
package shutdown

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// Manager handles graceful shutdown of server components.
type Manager struct {
	mu sync.RWMutex

	// Configuration
	drainTimeout   time.Duration
	forceTimeout   time.Duration
	shutdownHooks  []Hook

	// State
	shuttingDown   atomic.Bool
	inFlightReqs   atomic.Int64
	wg             sync.WaitGroup

	// Background jobs
	bgJobs         map[string]Job
	bgJobsMu       sync.RWMutex

	// Channels
	shutdownCh     chan struct{}
	doneCh         chan struct{}
}

// Hook is a function called during shutdown.
type Hook func(ctx context.Context) error

// Job represents a background job that can be stopped.
type Job interface {
	Start() error
	Stop(ctx context.Context) error
}

// Config holds shutdown manager configuration.
type Config struct {
	// DrainTimeout is the maximum time to wait for in-flight requests.
	DrainTimeout time.Duration
	// ForceTimeout is the maximum time to wait before force shutdown.
	ForceTimeout time.Duration
}

// DefaultConfig returns default shutdown configuration.
func DefaultConfig() Config {
	return Config{
		DrainTimeout: 30 * time.Second,
		ForceTimeout: 60 * time.Second,
	}
}

// NewManager creates a new shutdown manager.
func NewManager(config Config) *Manager {
	return &Manager{
		drainTimeout:  config.DrainTimeout,
		forceTimeout:  config.ForceTimeout,
		shutdownHooks: make([]Hook, 0),
		bgJobs:        make(map[string]Job),
		shutdownCh:    make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
}

// RegisterHook registers a shutdown hook.
func (m *Manager) RegisterHook(hook Hook) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shutdownHooks = append(m.shutdownHooks, hook)
}

// RegisterBackgroundJob registers a background job.
func (m *Manager) RegisterBackgroundJob(name string, job Job) {
	m.bgJobsMu.Lock()
	defer m.bgJobsMu.Unlock()
	m.bgJobs[name] = job
}

// UnregisterBackgroundJob removes a background job.
func (m *Manager) UnregisterBackgroundJob(name string) {
	m.bgJobsMu.Lock()
	defer m.bgJobsMu.Unlock()
	delete(m.bgJobs, name)
}

// StartRequest marks the beginning of a request.
// Returns false if the server is shutting down.
func (m *Manager) StartRequest() bool {
	if m.shuttingDown.Load() {
		return false
	}
	m.inFlightReqs.Add(1)
	m.wg.Add(1)
	return true
}

// EndRequest marks the end of a request.
func (m *Manager) EndRequest() {
	m.inFlightReqs.Add(-1)
	m.wg.Done()
}

// InFlightRequests returns the number of in-flight requests.
func (m *Manager) InFlightRequests() int64 {
	return m.inFlightReqs.Load()
}

// IsShuttingDown returns true if shutdown has been initiated.
func (m *Manager) IsShuttingDown() bool {
	return m.shuttingDown.Load()
}

// Shutdown initiates graceful shutdown.
func (m *Manager) Shutdown(ctx context.Context) error {
	if !m.shuttingDown.CompareAndSwap(false, true) {
		return errors.New("shutdown already in progress")
	}

	close(m.shutdownCh)

	// Create a context with force timeout
	forceCtx, forceCancel := context.WithTimeout(ctx, m.forceTimeout)
	defer forceCancel()

	// Step 1: Stop accepting new requests
	// (This happens automatically via StartRequest returning false)

	// Step 2: Wait for in-flight requests with drain timeout
	drainCtx, drainCancel := context.WithTimeout(forceCtx, m.drainTimeout)
	defer drainCancel()

	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All requests completed
	case <-drainCtx.Done():
		// Drain timeout exceeded
	}

	// Step 3: Stop background jobs
	if err := m.stopBackgroundJobs(forceCtx); err != nil {
		return err
	}

	// Step 4: Execute shutdown hooks
	if err := m.executeHooks(forceCtx); err != nil {
		return err
	}

	close(m.doneCh)
	return nil
}

// stopBackgroundJobs stops all registered background jobs.
func (m *Manager) stopBackgroundJobs(ctx context.Context) error {
	m.bgJobsMu.RLock()
	jobs := make(map[string]Job, len(m.bgJobs))
	for name, job := range m.bgJobs {
		jobs[name] = job
	}
	m.bgJobsMu.RUnlock()

	var wg sync.WaitGroup
	errCh := make(chan error, len(jobs))

	for name, job := range jobs {
		wg.Add(1)
		go func(n string, j Job) {
			defer wg.Done()
			if err := j.Stop(ctx); err != nil {
				errCh <- errors.New("failed to stop job " + n + ": " + err.Error())
			}
		}(name, job)
	}

	wg.Wait()
	close(errCh)

	// Collect errors
	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// executeHooks executes all registered shutdown hooks.
func (m *Manager) executeHooks(ctx context.Context) error {
	m.mu.RLock()
	hooks := make([]Hook, len(m.shutdownHooks))
	copy(hooks, m.shutdownHooks)
	m.mu.RUnlock()

	var wg sync.WaitGroup
	errCh := make(chan error, len(hooks))

	for _, hook := range hooks {
		wg.Add(1)
		go func(h Hook) {
			defer wg.Done()
			if err := h(ctx); err != nil {
				errCh <- err
			}
		}(hook)
	}

	wg.Wait()
	close(errCh)

	// Collect errors
	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// Wait blocks until shutdown is complete.
func (m *Manager) Wait() {
	<-m.doneCh
}

// ShutdownCh returns a channel that is closed when shutdown is initiated.
func (m *Manager) ShutdownCh() <-chan struct{} {
	return m.shutdownCh
}

// RequestWrapper wraps a function with shutdown tracking.
func (m *Manager) RequestWrapper(fn func()) error {
	if !m.StartRequest() {
		return errors.New("server is shutting down")
	}
	defer m.EndRequest()
	fn()
	return nil
}

// ContextWrapper returns a context that is cancelled when shutdown is initiated.
func (m *Manager) ContextWrapper(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	go func() {
		select {
		case <-m.shutdownCh:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

// Stats holds shutdown manager statistics.
type Stats struct {
	InFlightRequests int64
	IsShuttingDown   bool
	BackgroundJobs   int
	RegisteredHooks  int
}

// GetStats returns current statistics.
func (m *Manager) GetStats() Stats {
	m.mu.RLock()
	hooks := len(m.shutdownHooks)
	m.mu.RUnlock()

	m.bgJobsMu.RLock()
	jobs := len(m.bgJobs)
	m.bgJobsMu.RUnlock()

	return Stats{
		InFlightRequests: m.inFlightReqs.Load(),
		IsShuttingDown:   m.shuttingDown.Load(),
		BackgroundJobs:   jobs,
		RegisteredHooks:  hooks,
	}
}
