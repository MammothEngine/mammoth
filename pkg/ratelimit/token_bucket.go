package ratelimit

import (
	"context"
	"sync"
	"time"
)

// TokenBucket implements a token bucket rate limiter.
type TokenBucket struct {
	mu        sync.Mutex
	rate      Rate
	burst     int
	tokens    float64
	lastUpdate time.Time
}

// NewTokenBucket creates a new token bucket rate limiter.
func NewTokenBucket(r Rate, burst int) *TokenBucket {
	return &TokenBucket{
		rate:       r,
		burst:      burst,
		tokens:     float64(burst),
		lastUpdate: time.Now(),
	}
}

// NewTokenBucketFromConfig creates a token bucket from config.
func NewTokenBucketFromConfig(cfg Config) *TokenBucket {
	if !cfg.Enabled {
		return nil
	}
	return NewTokenBucket(Rate(cfg.RequestsPerSecond), cfg.Burst)
}

// Allow checks if a single request is allowed.
func (tb *TokenBucket) Allow() bool {
	return tb.AllowN(1)
}

// AllowN checks if n requests are allowed.
func (tb *TokenBucket) AllowN(n int) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.advance(time.Now())

	if float64(n) <= tb.tokens {
		tb.tokens -= float64(n)
		return true
	}
	return false
}

// Wait blocks until a request is allowed or context is cancelled.
func (tb *TokenBucket) Wait(ctx context.Context) error {
	return tb.WaitN(ctx, 1)
}

// WaitN blocks until n requests are allowed or context is cancelled.
func (tb *TokenBucket) WaitN(ctx context.Context, n int) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	tb.mu.Lock()
	tb.advance(time.Now())

	// Check if we have enough tokens now
	if float64(n) <= tb.tokens {
		tb.tokens -= float64(n)
		tb.mu.Unlock()
		return nil
	}

	// Calculate wait time
	needed := float64(n) - tb.tokens
	waitTime := time.Duration(needed * float64(time.Second) / float64(tb.rate))
	tb.mu.Unlock()

	// Wait with context
	timer := time.NewTimer(waitTime)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		// Try again after waiting
		tb.mu.Lock()
		tb.advance(time.Now())
		if float64(n) <= tb.tokens {
			tb.tokens -= float64(n)
			tb.mu.Unlock()
			return nil
		}
		tb.mu.Unlock()
		return context.DeadlineExceeded
	}
}

// Reserve returns a reservation for future use.
func (tb *TokenBucket) Reserve() *Reservation {
	return tb.ReserveN(time.Now(), 1)
}

// ReserveN returns a reservation for n tokens.
func (tb *TokenBucket) ReserveN(now time.Time, n int) *Reservation {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.advance(now)

	if float64(n) > tb.tokens {
		return &Reservation{Ok: false}
	}

	tb.tokens -= float64(n)
	return &Reservation{
		Ok:        true,
		TimeToAct: now,
		Limit:     tb.rate,
	}
}

// Rate returns the current rate limit.
func (tb *TokenBucket) Rate() Rate {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return tb.rate
}

// Burst returns the current burst size.
func (tb *TokenBucket) Burst() int {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return tb.burst
}

// SetRate changes the rate limit.
func (tb *TokenBucket) SetRate(r Rate) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.rate = r
}

// SetBurst changes the burst size.
func (tb *TokenBucket) SetBurst(burst int) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.burst = burst
	if tb.tokens > float64(burst) {
		tb.tokens = float64(burst)
	}
}

// Tokens returns the current number of available tokens.
func (tb *TokenBucket) Tokens() float64 {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.advance(time.Now())
	return tb.tokens
}

// advance updates the token count based on elapsed time.
func (tb *TokenBucket) advance(now time.Time) {
	elapsed := now.Sub(tb.lastUpdate)
	tb.lastUpdate = now

	// Add tokens based on elapsed time
	tokensToAdd := float64(elapsed) * float64(tb.rate) / float64(time.Second)
	tb.tokens += tokensToAdd

	// Cap at burst size
	if tb.tokens > float64(tb.burst) {
		tb.tokens = float64(tb.burst)
	}
}
