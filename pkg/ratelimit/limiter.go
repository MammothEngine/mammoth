// Package ratelimit provides rate limiting for the wire protocol.
package ratelimit

import (
	"context"
	"time"
)

// Limiter is the interface for rate limiters.
type Limiter interface {
	// Allow checks if a request is allowed. Returns true if allowed, false if rate limited.
	Allow() bool

	// AllowN checks if n requests are allowed.
	AllowN(n int) bool

	// Wait blocks until a request is allowed or context is cancelled.
	Wait(ctx context.Context) error

	// WaitN blocks until n requests are allowed or context is cancelled.
	WaitN(ctx context.Context, n int) error

	// Reserve returns a reservation for future use.
	Reserve() *Reservation

	// Rate returns the current rate limit.
	Rate() Rate

	// Burst returns the current burst size.
	Burst() int
}

// Rate represents a rate limit (events per second).
type Rate float64

// RatePerSecond creates a rate from events per second.
func RatePerSecond(r float64) Rate {
	return Rate(r)
}

// RatePerMinute creates a rate from events per minute.
func RatePerMinute(r float64) Rate {
	return Rate(r / 60)
}

// Duration returns the time between events at this rate.
func (r Rate) Duration() time.Duration {
	if r == 0 {
		return time.Duration(1<<63 - 1) // Max duration
	}
	return time.Duration(float64(time.Second) / float64(r))
}

// Reservation represents a reserved slot in the rate limiter.
type Reservation struct {
	Ok        bool
	TimeToAct time.Time
	Limit     Rate
}

// Delay returns the duration to wait before the reservation can be used.
func (r *Reservation) Delay() time.Duration {
	return r.DelayFrom(time.Now())
}

// DelayFrom returns the duration to wait from the given time.
func (r *Reservation) DelayFrom(now time.Time) time.Duration {
	if !r.Ok {
		return time.Duration(1<<63 - 1) // Max duration
	}
	delay := r.TimeToAct.Sub(now)
	if delay < 0 {
		return 0
	}
	return delay
}

// Cancel cancels the reservation and returns tokens to the bucket.
func (r *Reservation) Cancel() {
	// Implementation depends on the limiter type
}

// Config holds rate limiter configuration.
type Config struct {
	// Enabled enables rate limiting.
	Enabled bool `json:"enabled"`

	// RequestsPerSecond is the rate limit.
	RequestsPerSecond float64 `json:"requestsPerSecond"`

	// Burst is the maximum burst size.
	Burst int `json:"burst"`

	// PerConnection enables per-connection rate limiting.
	PerConnection bool `json:"perConnection"`

	// GlobalRate limits total requests across all connections.
	GlobalRate float64 `json:"globalRate"`

	// GlobalBurst is the global burst size.
	GlobalBurst int `json:"globalBurst"`

	// WaitTimeout is the maximum time to wait for a token.
	WaitTimeout time.Duration `json:"waitTimeout"`
}

// DefaultConfig returns a default rate limiter configuration.
func DefaultConfig() Config {
	return Config{
		Enabled:           false,
		RequestsPerSecond: 1000,
		Burst:             100,
		PerConnection:     true,
		GlobalRate:        10000,
		GlobalBurst:       1000,
		WaitTimeout:       100 * time.Millisecond,
	}
}

// Unlimited returns a limiter with no restrictions.
func Unlimited() Limiter {
	return &unlimitedLimiter{}
}

type unlimitedLimiter struct{}

func (u *unlimitedLimiter) Allow() bool                      { return true }
func (u *unlimitedLimiter) AllowN(n int) bool                { return true }
func (u *unlimitedLimiter) Wait(ctx context.Context) error   { return nil }
func (u *unlimitedLimiter) WaitN(ctx context.Context, n int) error { return nil }
func (u *unlimitedLimiter) Reserve() *Reservation            { return &Reservation{Ok: true} }
func (u *unlimitedLimiter) Rate() Rate                       { return Rate(1<<63 - 1) }
func (u *unlimitedLimiter) Burst() int                       { return 1<<31 - 1 }
