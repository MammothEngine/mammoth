package shutdown

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewManager(t *testing.T) {
	config := DefaultConfig()
	m := NewManager(config)

	if m == nil {
		t.Fatal("expected non-nil Manager")
	}
	if m.drainTimeout != config.DrainTimeout {
		t.Errorf("drainTimeout = %v, want %v", m.drainTimeout, config.DrainTimeout)
	}
	if m.forceTimeout != config.ForceTimeout {
		t.Errorf("forceTimeout = %v, want %v", m.forceTimeout, config.ForceTimeout)
	}
}

func TestManager_StartEndRequest(t *testing.T) {
	m := NewManager(DefaultConfig())

	// Start request should succeed
	if !m.StartRequest() {
		t.Error("StartRequest() should return true")
	}

	if m.InFlightRequests() != 1 {
		t.Errorf("InFlightRequests() = %d, want 1", m.InFlightRequests())
	}

	// End request
	m.EndRequest()

	if m.InFlightRequests() != 0 {
		t.Errorf("InFlightRequests() = %d, want 0", m.InFlightRequests())
	}
}

func TestManager_StartRequest_DuringShutdown(t *testing.T) {
	m := NewManager(DefaultConfig())

	// Initiate shutdown
	m.shuttingDown.Store(true)

	// Start request should fail
	if m.StartRequest() {
		t.Error("StartRequest() should return false during shutdown")
	}
}

func TestManager_IsShuttingDown(t *testing.T) {
	m := NewManager(DefaultConfig())

	if m.IsShuttingDown() {
		t.Error("IsShuttingDown() should return false initially")
	}

	m.shuttingDown.Store(true)

	if !m.IsShuttingDown() {
		t.Error("IsShuttingDown() should return true after setting")
	}
}

func TestManager_RegisterHook(t *testing.T) {
	m := NewManager(DefaultConfig())

	hookCalled := false
	hook := func(ctx context.Context) error {
		hookCalled = true
		return nil
	}

	m.RegisterHook(hook)

	if len(m.shutdownHooks) != 1 {
		t.Errorf("expected 1 hook, got %d", len(m.shutdownHooks))
	}

	// Test hook execution
	ctx := context.Background()
	m.executeHooks(ctx)

	if !hookCalled {
		t.Error("hook should have been called")
	}
}

func TestManager_RegisterBackgroundJob(t *testing.T) {
	m := NewManager(DefaultConfig())

	job := &testJob{}
	m.RegisterBackgroundJob("test", job)

	if len(m.bgJobs) != 1 {
		t.Errorf("expected 1 job, got %d", len(m.bgJobs))
	}

	m.UnregisterBackgroundJob("test")

	if len(m.bgJobs) != 0 {
		t.Errorf("expected 0 jobs, got %d", len(m.bgJobs))
	}
}

func TestManager_Shutdown(t *testing.T) {
	m := NewManager(Config{
		DrainTimeout: 100 * time.Millisecond,
		ForceTimeout: 200 * time.Millisecond,
	})

	// Add a hook
	hookCalled := false
	m.RegisterHook(func(ctx context.Context) error {
		hookCalled = true
		return nil
	})

	// Add a background job
	job := &testJob{}
	m.RegisterBackgroundJob("test", job)

	// Start a request
	if !m.StartRequest() {
		t.Fatal("StartRequest() should succeed")
	}

	// End request after a delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		m.EndRequest()
	}()

	// Shutdown
	ctx := context.Background()
	err := m.Shutdown(ctx)

	if err != nil {
		t.Errorf("Shutdown() error = %v", err)
	}

	if !hookCalled {
		t.Error("hook should have been called during shutdown")
	}

	if !job.stopped.Load() {
		t.Error("job should have been stopped")
	}

	if !m.IsShuttingDown() {
		t.Error("IsShuttingDown() should return true after shutdown")
	}
}

func TestManager_Shutdown_AlreadyShuttingDown(t *testing.T) {
	m := NewManager(DefaultConfig())

	// First shutdown
	go m.Shutdown(context.Background())
	time.Sleep(10 * time.Millisecond)

	// Second shutdown should fail
	err := m.Shutdown(context.Background())
	if err == nil {
		t.Error("second Shutdown() should return error")
	}
}

func TestManager_Shutdown_WithDrainTimeout(t *testing.T) {
	m := NewManager(Config{
		DrainTimeout: 50 * time.Millisecond,
		ForceTimeout: 100 * time.Millisecond,
	})

	// Start a request that won't complete
	if !m.StartRequest() {
		t.Fatal("StartRequest() should succeed")
	}

	// Shutdown should timeout waiting for request
	ctx := context.Background()
	err := m.Shutdown(ctx)

	if err != nil {
		t.Errorf("Shutdown() error = %v", err)
	}

	// Request should still be counted
	if m.InFlightRequests() != 1 {
		t.Errorf("InFlightRequests() = %d, want 1", m.InFlightRequests())
	}
}

func TestManager_Shutdown_WithFailingHook(t *testing.T) {
	m := NewManager(DefaultConfig())

	// Add a failing hook
	m.RegisterHook(func(ctx context.Context) error {
		return errors.New("hook error")
	})

	ctx := context.Background()
	err := m.Shutdown(ctx)

	if err == nil {
		t.Error("Shutdown() should return error when hook fails")
	}
}

func TestManager_RequestWrapper(t *testing.T) {
	m := NewManager(DefaultConfig())

	called := false
	err := m.RequestWrapper(func() {
		called = true
	})

	if err != nil {
		t.Errorf("RequestWrapper() error = %v", err)
	}
	if !called {
		t.Error("function should have been called")
	}
	if m.InFlightRequests() != 0 {
		t.Errorf("InFlightRequests() = %d, want 0", m.InFlightRequests())
	}
}

func TestManager_RequestWrapper_DuringShutdown(t *testing.T) {
	m := NewManager(DefaultConfig())
	m.shuttingDown.Store(true)

	err := m.RequestWrapper(func() {})

	if err == nil {
		t.Error("RequestWrapper() should return error during shutdown")
	}
}

func TestManager_ContextWrapper(t *testing.T) {
	m := NewManager(DefaultConfig())

	ctx, cancel := m.ContextWrapper(context.Background())
	defer cancel()

	// Initiate shutdown
	go func() {
		m.Shutdown(context.Background())
	}()

	// Context should be cancelled
	select {
	case <-ctx.Done():
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Error("context should have been cancelled")
	}
}

func TestManager_GetStats(t *testing.T) {
	m := NewManager(DefaultConfig())

	// Add hooks and jobs
	m.RegisterHook(func(ctx context.Context) error { return nil })
	m.RegisterHook(func(ctx context.Context) error { return nil })
	m.RegisterBackgroundJob("job1", &testJob{})

	// Start a request
	m.StartRequest()

	stats := m.GetStats()

	if stats.InFlightRequests != 1 {
		t.Errorf("InFlightRequests = %d, want 1", stats.InFlightRequests)
	}
	if stats.IsShuttingDown {
		t.Error("IsShuttingDown should be false")
	}
	if stats.BackgroundJobs != 1 {
		t.Errorf("BackgroundJobs = %d, want 1", stats.BackgroundJobs)
	}
	if stats.RegisteredHooks != 2 {
		t.Errorf("RegisteredHooks = %d, want 2", stats.RegisteredHooks)
	}

	m.EndRequest()
}

func TestManager_ShutdownCh(t *testing.T) {
	m := NewManager(DefaultConfig())

	ch := m.ShutdownCh()

	// Initiate shutdown
	go m.Shutdown(context.Background())

	// Channel should be closed
	select {
	case <-ch:
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Error("shutdown channel should have been closed")
	}
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.DrainTimeout != 30*time.Second {
		t.Errorf("DrainTimeout = %v, want 30s", config.DrainTimeout)
	}
	if config.ForceTimeout != 60*time.Second {
		t.Errorf("ForceTimeout = %v, want 60s", config.ForceTimeout)
	}
}

// testJob is a test implementation of Job interface
type testJob struct {
	stopped atomic.Bool
}

func (j *testJob) Start() error {
	return nil
}

func (j *testJob) Stop(ctx context.Context) error {
	j.stopped.Store(true)
	return nil
}

func BenchmarkManager_StartEndRequest(b *testing.B) {
	m := NewManager(DefaultConfig())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.StartRequest()
		m.EndRequest()
	}
}
