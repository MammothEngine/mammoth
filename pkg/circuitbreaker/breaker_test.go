package circuitbreaker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

var testError = errors.New("test error")

func TestNew(t *testing.T) {
	config := DefaultConfig()
	b := New(config)

	if b.State() != StateClosed {
		t.Errorf("expected initial state closed, got %v", b.State())
	}
}

func TestBreaker_Allow_Closed(t *testing.T) {
	config := DefaultConfig()
	b := New(config)

	if !b.Allow() {
		t.Error("expected Allow() to return true in closed state")
	}
}

func TestBreaker_Allow_Open(t *testing.T) {
	config := Config{
		FailureThreshold: 1,
		Timeout:          1 * time.Hour, // Long timeout to stay open
	}
	b := New(config)

	// Trigger failure to open circuit
	b.RecordFailure()

	if b.State() != StateOpen {
		t.Fatalf("expected state open, got %v", b.State())
	}

	if b.Allow() {
		t.Error("expected Allow() to return false in open state")
	}
}

func TestBreaker_Allow_HalfOpen(t *testing.T) {
	config := Config{
		FailureThreshold: 1,
		Timeout:          1 * time.Millisecond,
		MaxRequests:      1,
	}
	b := New(config)

	// Trigger failure to open circuit
	b.RecordFailure()
	if b.State() != StateOpen {
		t.Fatal("expected state open")
	}

	// Wait for timeout
	time.Sleep(5 * time.Millisecond)

	// Should transition to half-open
	if !b.Allow() {
		t.Error("expected Allow() to return true in half-open state")
	}

	if b.State() != StateHalfOpen {
		t.Errorf("expected state half-open, got %v", b.State())
	}
}

func TestBreaker_Execute_Success(t *testing.T) {
	config := DefaultConfig()
	b := New(config)

	called := false
	err := b.Execute(func() error {
		called = true
		return nil
	})

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if !called {
		t.Error("expected function to be called")
	}
}

func TestBreaker_Execute_Failure(t *testing.T) {
	config := DefaultConfig()
	b := New(config)

	err := b.Execute(func() error {
		return testError
	})

	if err != testError {
		t.Errorf("expected test error, got %v", err)
	}

	if b.failures.Load() != 1 {
		t.Errorf("expected 1 failure, got %d", b.failures.Load())
	}
}

func TestBreaker_Execute_CircuitOpen(t *testing.T) {
	config := Config{
		FailureThreshold: 1,
		Timeout:          1 * time.Hour,
	}
	b := New(config)

	// Open the circuit
	b.RecordFailure()

	called := false
	err := b.Execute(func() error {
		called = true
		return nil
	})

	if err != ErrCircuitOpen {
		t.Errorf("expected ErrCircuitOpen, got %v", err)
	}
	if called {
		t.Error("expected function not to be called")
	}
}

func TestBreaker_ExecuteContext_Cancelled(t *testing.T) {
	config := DefaultConfig()
	b := New(config)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := b.ExecuteContext(ctx, func() error {
		return nil
	})

	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestBreaker_TransitionToClosed(t *testing.T) {
	config := Config{
		FailureThreshold: 1,
		SuccessThreshold: 2,
		Timeout:          1 * time.Millisecond,
		MaxRequests:      10,
	}
	b := New(config)

	// Open the circuit
	b.RecordFailure()
	if b.State() != StateOpen {
		t.Fatal("expected state open")
	}

	// Wait for timeout and trigger transition to half-open
	time.Sleep(5 * time.Millisecond)
	b.Allow() // Trigger transition to half-open
	if b.State() != StateHalfOpen {
		t.Fatalf("expected state half-open, got %v", b.State())
	}

	// Record successes to close circuit
	b.RecordSuccess()
	if b.successes.Load() != 1 {
		t.Errorf("expected 1 success, got %d", b.successes.Load())
	}

	b.RecordSuccess()
	if b.State() != StateClosed {
		t.Errorf("expected state closed after threshold, got %v", b.State())
	}
}

func TestBreaker_Reset(t *testing.T) {
	config := Config{
		FailureThreshold: 1,
		Timeout:          1 * time.Hour,
	}
	b := New(config)

	// Open the circuit
	b.RecordFailure()
	if b.State() != StateOpen {
		t.Fatal("expected state open")
	}

	b.Reset()

	if b.State() != StateClosed {
		t.Errorf("expected state closed after reset, got %v", b.State())
	}
	if b.failures.Load() != 0 {
		t.Errorf("expected 0 failures after reset, got %d", b.failures.Load())
	}
}

func TestBreaker_OnStateChange(t *testing.T) {
	config := Config{
		FailureThreshold: 1,
		Timeout:          1 * time.Millisecond,
		MaxRequests:      1,
	}

	var transitions []string
	b := NewWithCallbacks(config, func(from, to State) {
		transitions = append(transitions, from.String()+"->"+to.String())
	})

	// Open the circuit
	b.RecordFailure()
	if len(transitions) != 1 || transitions[0] != "closed->open" {
		t.Errorf("expected closed->open transition, got %v", transitions)
	}

	// Wait for half-open
	time.Sleep(5 * time.Millisecond)
	b.Allow()
	if len(transitions) != 2 || transitions[1] != "open->half-open" {
		t.Errorf("expected open->half-open transition, got %v", transitions)
	}
}

func TestBreaker_GetMetrics(t *testing.T) {
	config := DefaultConfig()
	b := New(config)

	b.RecordFailure()

	metrics := b.GetMetrics()
	if metrics.State != StateClosed {
		t.Errorf("expected state closed in metrics, got %v", metrics.State)
	}
	if metrics.Failures != 1 {
		t.Errorf("expected 1 failure in metrics, got %d", metrics.Failures)
	}
	if metrics.LastFailure.IsZero() {
		t.Error("expected LastFailure to be set")
	}
}

func TestManager_GetBreaker(t *testing.T) {
	config := DefaultConfig()
	m := NewManager(config)

	b1 := m.GetBreaker("test")
	b2 := m.GetBreaker("test")

	if b1 != b2 {
		t.Error("expected same breaker instance for same name")
	}

	b3 := m.GetBreaker("other")
	if b1 == b3 {
		t.Error("expected different breaker instance for different name")
	}
}

func TestManager_Execute(t *testing.T) {
	config := DefaultConfig()
	m := NewManager(config)

	called := false
	err := m.Execute("test", func() error {
		called = true
		return nil
	})

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if !called {
		t.Error("expected function to be called")
	}
}

func TestManager_Reset(t *testing.T) {
	config := Config{
		FailureThreshold: 1,
		Timeout:          1 * time.Hour,
	}
	m := NewManager(config)

	// Open the circuit
	m.GetBreaker("test").RecordFailure()

	m.Reset()

	if m.GetBreaker("test").State() != StateClosed {
		t.Error("expected state closed after reset")
	}
}

func TestManager_GetAllMetrics(t *testing.T) {
	config := DefaultConfig()
	m := NewManager(config)

	m.GetBreaker("test1").RecordFailure()
	m.GetBreaker("test2").RecordSuccess()

	metrics := m.GetAllMetrics()
	if len(metrics) != 2 {
		t.Errorf("expected 2 metrics, got %d", len(metrics))
	}

	if metrics["test1"].Failures != 1 {
		t.Errorf("expected test1 to have 1 failure, got %d", metrics["test1"].Failures)
	}
}

func TestManager_Remove(t *testing.T) {
	config := DefaultConfig()
	m := NewManager(config)

	b1 := m.GetBreaker("test")
	m.Remove("test")
	b2 := m.GetBreaker("test")

	if b1 == b2 {
		t.Error("expected new breaker instance after remove")
	}
}

func TestBreaker_Concurrent(t *testing.T) {
	config := Config{
		FailureThreshold: 100,
		MaxRequests:      100,
	}
	b := New(config)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.Allow()
		}()
	}
	wg.Wait()
}

func TestState_String(t *testing.T) {
	tests := []struct {
		state State
		want  string
	}{
		{StateClosed, "closed"},
		{StateOpen, "open"},
		{StateHalfOpen, "half-open"},
		{State(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("State(%d).String() = %v, want %v", tt.state, got, tt.want)
		}
	}
}

func TestBreaker_HalfOpenMaxRequests(t *testing.T) {
	config := Config{
		FailureThreshold: 1,
		Timeout:          1 * time.Millisecond,
		MaxRequests:      2,
	}
	b := New(config)

	// Open circuit
	b.RecordFailure()

	// Wait for half-open
	time.Sleep(5 * time.Millisecond)

	// Should allow MaxRequests
	if !b.Allow() {
		t.Error("expected first request to be allowed")
	}
	if !b.Allow() {
		t.Error("expected second request to be allowed")
	}
	if b.Allow() {
		t.Error("expected third request to be denied")
	}
}
