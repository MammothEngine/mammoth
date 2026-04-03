package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

var testError = errors.New("test error")

func TestDo_Success(t *testing.T) {
	called := 0
	err := Do(func() error {
		called++
		return nil
	})

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if called != 1 {
		t.Errorf("expected 1 call, got %d", called)
	}
}

func TestDo_MaxRetriesExceeded(t *testing.T) {
	config := Config{
		MaxRetries: 2,
		BaseDelay:  1 * time.Millisecond,
		Jitter:     false,
	}

	called := 0
	err := DoWithConfig(config, func() error {
		called++
		return testError
	})

	if !errors.Is(err, ErrMaxRetriesExceeded) {
		t.Errorf("expected ErrMaxRetriesExceeded, got %v", err)
	}
	if called != 3 { // initial + 2 retries
		t.Errorf("expected 3 calls, got %d", called)
	}
}

func TestDo_EventualSuccess(t *testing.T) {
	config := Config{
		MaxRetries: 3,
		BaseDelay:  1 * time.Millisecond,
		Jitter:     false,
	}

	called := 0
	err := DoWithConfig(config, func() error {
		called++
		if called < 3 {
			return testError
		}
		return nil
	})

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if called != 3 {
		t.Errorf("expected 3 calls, got %d", called)
	}
}

func TestDo_NoRetries(t *testing.T) {
	config := Config{
		MaxRetries: 0,
	}

	called := 0
	err := DoWithConfig(config, func() error {
		called++
		return testError
	})

	if err != testError {
		t.Errorf("expected test error, got %v", err)
	}
	if called != 1 {
		t.Errorf("expected 1 call, got %d", called)
	}
}

func TestDo_ContextCancellation(t *testing.T) {
	config := Config{
		MaxRetries: 3,
		BaseDelay:  1 * time.Hour, // Long delay to ensure cancellation happens first
		Jitter:     false,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	called := 0
	err := DoWithContext(ctx, config, func() error {
		called++
		return testError
	})

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestDo_ContextTimeout(t *testing.T) {
	config := Config{
		MaxRetries: 10,
		BaseDelay:  100 * time.Millisecond,
		Jitter:     false,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := DoWithContext(ctx, config, func() error {
		return testError
	})

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestDo_NonRetryableError(t *testing.T) {
	nonRetryableErr := errors.New("non-retryable")
	config := Config{
		MaxRetries: 3,
		BaseDelay:  1 * time.Millisecond,
		Jitter:     false,
		Retryable: func(err error) bool {
			return err != nonRetryableErr
		},
	}

	called := 0
	err := DoWithConfig(config, func() error {
		called++
		return nonRetryableErr
	})

	if err != nonRetryableErr {
		t.Errorf("expected non-retryable error, got %v", err)
	}
	if called != 1 {
		t.Errorf("expected 1 call, got %d", called)
	}
}

func TestDoWithResult_Success(t *testing.T) {
	result, err := DoWithResult(func() (string, error) {
		return "success", nil
	})

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if result != "success" {
		t.Errorf("expected 'success', got %q", result)
	}
}

func TestDoWithResult_MaxRetriesExceeded(t *testing.T) {
	config := Config{
		MaxRetries: 2,
		BaseDelay:  1 * time.Millisecond,
		Jitter:     false,
	}

	called := 0
	result, err := DoWithResultAndConfig(config, func() (string, error) {
		called++
		return "", testError
	})

	if !errors.Is(err, ErrMaxRetriesExceeded) {
		t.Errorf("expected ErrMaxRetriesExceeded, got %v", err)
	}
	if result != "" {
		t.Errorf("expected empty result, got %q", result)
	}
	if called != 3 {
		t.Errorf("expected 3 calls, got %d", called)
	}
}

func TestCalculateDelay(t *testing.T) {
	baseDelay := 100 * time.Millisecond

	// Without jitter
	config := Config{Jitter: false}
	delay := calculateDelay(baseDelay, config)
	if delay != baseDelay {
		t.Errorf("expected delay %v, got %v", baseDelay, delay)
	}

	// With jitter - should be within 25% of base
	config = Config{Jitter: true}
	for i := 0; i < 100; i++ {
		delay = calculateDelay(baseDelay, config)
		minDelay := time.Duration(float64(baseDelay) * 0.75)
		maxDelay := time.Duration(float64(baseDelay) * 1.25)
		if delay < minDelay || delay > maxDelay {
			t.Errorf("jittered delay %v out of range [%v, %v]", delay, minDelay, maxDelay)
		}
	}
}

func TestRetrier(t *testing.T) {
	r := New(DefaultConfig())

	called := 0
	err := r.Do(func() error {
		called++
		return nil
	})

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if called != 1 {
		t.Errorf("expected 1 call, got %d", called)
	}
}

func TestRetrier_WithMaxRetries(t *testing.T) {
	r := New(DefaultConfig()).WithMaxRetries(1)

	called := 0
	err := r.Do(func() error {
		called++
		return testError
	})

	if !errors.Is(err, ErrMaxRetriesExceeded) {
		t.Errorf("expected ErrMaxRetriesExceeded, got %v", err)
	}
	if called != 2 { // initial + 1 retry
		t.Errorf("expected 2 calls, got %d", called)
	}
}

func TestRetrier_WithBaseDelay(t *testing.T) {
	r := New(DefaultConfig()).WithBaseDelay(50 * time.Millisecond)

	if r.config.BaseDelay != 50*time.Millisecond {
		t.Errorf("expected base delay 50ms, got %v", r.config.BaseDelay)
	}
}

func TestRetrier_WithMaxDelay(t *testing.T) {
	r := New(DefaultConfig()).WithMaxDelay(10 * time.Second)

	if r.config.MaxDelay != 10*time.Second {
		t.Errorf("expected max delay 10s, got %v", r.config.MaxDelay)
	}
}

func TestRetrier_WithMultiplier(t *testing.T) {
	r := New(DefaultConfig()).WithMultiplier(3.0)

	if r.config.Multiplier != 3.0 {
		t.Errorf("expected multiplier 3.0, got %v", r.config.Multiplier)
	}
}

func TestRetrier_WithJitter(t *testing.T) {
	r := New(DefaultConfig()).WithJitter(false)

	if r.config.Jitter != false {
		t.Errorf("expected jitter false, got %v", r.config.Jitter)
	}
}

func TestRetrier_WithRetryable(t *testing.T) {
	retryable := func(err error) bool {
		return err == testError
	}
	r := New(DefaultConfig()).WithRetryable(retryable)

	if r.config.Retryable == nil {
		t.Error("expected retryable function to be set")
	}
}

func TestConfig_IsRetryable(t *testing.T) {
	config := DefaultConfig()

	// Nil error should not be retryable
	if config.IsRetryable(nil) {
		t.Error("nil error should not be retryable")
	}

	// Context canceled should not be retryable
	if config.IsRetryable(context.Canceled) {
		t.Error("context.Canceled should not be retryable")
	}

	// Context deadline exceeded should not be retryable
	if config.IsRetryable(context.DeadlineExceeded) {
		t.Error("context.DeadlineExceeded should not be retryable")
	}

	// Regular errors should be retryable
	if !config.IsRetryable(testError) {
		t.Error("regular error should be retryable")
	}

	// Custom retryable function
	config.Retryable = func(err error) bool {
		return err != testError
	}
	if config.IsRetryable(testError) {
		t.Error("testError should not be retryable with custom function")
	}
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.MaxRetries != 3 {
		t.Errorf("expected MaxRetries 3, got %d", config.MaxRetries)
	}
	if config.BaseDelay != 100*time.Millisecond {
		t.Errorf("expected BaseDelay 100ms, got %v", config.BaseDelay)
	}
	if config.MaxDelay != 30*time.Second {
		t.Errorf("expected MaxDelay 30s, got %v", config.MaxDelay)
	}
	if config.Multiplier != 2.0 {
		t.Errorf("expected Multiplier 2.0, got %v", config.Multiplier)
	}
	if !config.Jitter {
		t.Error("expected Jitter to be true")
	}
}

func TestExponentialBackoff(t *testing.T) {
	config := Config{
		MaxRetries: 5,
		BaseDelay:  10 * time.Millisecond,
		MaxDelay:   100 * time.Millisecond,
		Multiplier: 2.0,
		Jitter:     false,
	}

	start := time.Now()
	attempts := 0

	DoWithConfig(config, func() error {
		attempts++
		return testError
	})

	elapsed := time.Since(start)

	// Expected delays: 10ms, 20ms, 40ms, 80ms = 150ms total
	// Allow some tolerance for test execution time
	expectedMin := 140 * time.Millisecond
	if elapsed < expectedMin {
		t.Errorf("expected at least %v elapsed, got %v", expectedMin, elapsed)
	}
}

func BenchmarkDo(b *testing.B) {
	config := Config{
		MaxRetries: 0, // No retries for benchmark
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DoWithConfig(config, func() error {
			return nil
		})
	}
}
