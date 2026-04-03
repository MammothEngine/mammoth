// Package retry provides retry mechanisms with exponential backoff.
package retry

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

// ErrMaxRetriesExceeded is returned when max retries is exceeded.
var ErrMaxRetriesExceeded = errors.New("max retries exceeded")

// Config holds retry configuration.
type Config struct {
	// MaxRetries is the maximum number of retry attempts (0 = no retries).
	MaxRetries int
	// BaseDelay is the initial delay between retries.
	BaseDelay time.Duration
	// MaxDelay is the maximum delay between retries.
	MaxDelay time.Duration
	// Multiplier is the exponential backoff multiplier.
	Multiplier float64
	// Jitter adds randomization to prevent thundering herd.
	Jitter bool
	// Retryable determines if an error should be retried.
	// If nil, all errors are retried.
	Retryable func(error) bool
}

// DefaultConfig returns a default retry configuration.
func DefaultConfig() Config {
	return Config{
		MaxRetries: 3,
		BaseDelay:  100 * time.Millisecond,
		MaxDelay:   30 * time.Second,
		Multiplier: 2.0,
		Jitter:     true,
	}
}

// IsRetryable checks if an error is retryable.
func (c Config) IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if c.Retryable != nil {
		return c.Retryable(err)
	}
	return true
}

// Do executes the given function with retry logic.
func Do(fn func() error) error {
	return DoWithConfig(DefaultConfig(), fn)
}

// DoWithConfig executes the given function with custom retry configuration.
func DoWithConfig(config Config, fn func() error) error {
	return DoWithContext(context.Background(), config, fn)
}

// DoWithContext executes the given function with retry logic and context support.
func DoWithContext(ctx context.Context, config Config, fn func() error) error {
	if config.MaxRetries <= 0 {
		return fn()
	}

	var lastErr error
	delay := config.BaseDelay

	for attempt := 0; attempt <= config.MaxRetries; attempt++ {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := fn()
		if err == nil {
			return nil
		}

		lastErr = err

		// Check if we should retry this error
		if !config.IsRetryable(err) {
			return err
		}

		// Don't sleep after the last attempt
		if attempt < config.MaxRetries {
			sleepDuration := calculateDelay(delay, config)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(sleepDuration):
			}

			// Calculate next delay with exponential backoff
			delay = time.Duration(float64(delay) * config.Multiplier)
			if delay > config.MaxDelay {
				delay = config.MaxDelay
			}
		}
	}

	return errors.Join(ErrMaxRetriesExceeded, lastErr)
}

// DoWithResult executes the given function that returns a result with retry logic.
func DoWithResult[T any](fn func() (T, error)) (T, error) {
	return DoWithResultAndConfig(DefaultConfig(), fn)
}

// DoWithResultAndConfig executes the given function with custom retry configuration.
func DoWithResultAndConfig[T any](config Config, fn func() (T, error)) (T, error) {
	return DoWithResultAndContext(context.Background(), config, fn)
}

// DoWithResultAndContext executes the given function with retry logic and context support.
func DoWithResultAndContext[T any](ctx context.Context, config Config, fn func() (T, error)) (T, error) {
	var zero T
	if config.MaxRetries <= 0 {
		return fn()
	}

	var lastErr error
	delay := config.BaseDelay

	for attempt := 0; attempt <= config.MaxRetries; attempt++ {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		default:
		}

		result, err := fn()
		if err == nil {
			return result, nil
		}

		lastErr = err

		// Check if we should retry this error
		if !config.IsRetryable(err) {
			return zero, err
		}

		// Don't sleep after the last attempt
		if attempt < config.MaxRetries {
			sleepDuration := calculateDelay(delay, config)

			select {
			case <-ctx.Done():
				return zero, ctx.Err()
			case <-time.After(sleepDuration):
			}

			// Calculate next delay with exponential backoff
			delay = time.Duration(float64(delay) * config.Multiplier)
			if delay > config.MaxDelay {
				delay = config.MaxDelay
			}
		}
	}

	return zero, errors.Join(ErrMaxRetriesExceeded, lastErr)
}

// calculateDelay calculates the delay with optional jitter.
func calculateDelay(baseDelay time.Duration, config Config) time.Duration {
	if !config.Jitter {
		return baseDelay
	}

	// Add +/- 25% jitter
	jitter := float64(baseDelay) * 0.25
	jitteredDelay := float64(baseDelay) + (rand.Float64()*2-1)*jitter
	return time.Duration(jitteredDelay)
}

// Retrier is a configurable retry handler.
type Retrier struct {
	config Config
}

// New creates a new Retrier with the given configuration.
func New(config Config) *Retrier {
	return &Retrier{config: config}
}

// Do executes the given function with retry logic.
func (r *Retrier) Do(fn func() error) error {
	return DoWithConfig(r.config, fn)
}

// DoWithContext executes the given function with retry logic and context support.
func (r *Retrier) DoWithContext(ctx context.Context, fn func() error) error {
	return DoWithContext(ctx, r.config, fn)
}

// DoWithResult executes the given function that returns a result with retry logic.
// Note: Use DoWithResultAndConfig function directly for typed results.
func (r *Retrier) DoWithResult(fn func() (any, error)) (any, error) {
	return DoWithResultAndConfig(r.config, fn)
}

// DoWithResultAndContext executes the given function with retry logic and context support.
// Note: Use DoWithResultAndContext function directly for typed results.
func (r *Retrier) DoWithResultAndContext(ctx context.Context, fn func() (any, error)) (any, error) {
	return DoWithResultAndContext(ctx, r.config, fn)
}

// WithMaxRetries returns a new Retrier with updated MaxRetries.
func (r *Retrier) WithMaxRetries(maxRetries int) *Retrier {
	config := r.config
	config.MaxRetries = maxRetries
	return New(config)
}

// WithBaseDelay returns a new Retrier with updated BaseDelay.
func (r *Retrier) WithBaseDelay(baseDelay time.Duration) *Retrier {
	config := r.config
	config.BaseDelay = baseDelay
	return New(config)
}

// WithMaxDelay returns a new Retrier with updated MaxDelay.
func (r *Retrier) WithMaxDelay(maxDelay time.Duration) *Retrier {
	config := r.config
	config.MaxDelay = maxDelay
	return New(config)
}

// WithMultiplier returns a new Retrier with updated Multiplier.
func (r *Retrier) WithMultiplier(multiplier float64) *Retrier {
	config := r.config
	config.Multiplier = multiplier
	return New(config)
}

// WithJitter returns a new Retrier with updated Jitter.
func (r *Retrier) WithJitter(jitter bool) *Retrier {
	config := r.config
	config.Jitter = jitter
	return New(config)
}

// WithRetryable returns a new Retrier with custom retryable function.
func (r *Retrier) WithRetryable(retryable func(error) bool) *Retrier {
	config := r.config
	config.Retryable = retryable
	return New(config)
}
