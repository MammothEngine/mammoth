package ratelimit

import (
	"context"
	"testing"
	"time"
)

func TestTokenBucket_Allow(t *testing.T) {
	tb := NewTokenBucket(Rate(10), 5) // 10 req/sec, burst 5

	// Should allow burst
	for i := 0; i < 5; i++ {
		if !tb.Allow() {
			t.Errorf("expected allow at iteration %d", i)
		}
	}

	// Should deny after burst
	if tb.Allow() {
		t.Error("expected deny after burst")
	}

	// Wait for token refill
	time.Sleep(200 * time.Millisecond)

	if !tb.Allow() {
		t.Error("expected allow after refill")
	}
}

func TestTokenBucket_AllowN(t *testing.T) {
	tb := NewTokenBucket(Rate(10), 10)

	// Allow 5 at once
	if !tb.AllowN(5) {
		t.Error("expected AllowN(5) to succeed")
	}

	// Should have 5 left
	if !tb.AllowN(5) {
		t.Error("expected AllowN(5) to succeed with remaining tokens")
	}

	// Should deny
	if tb.AllowN(1) {
		t.Error("expected AllowN(1) to fail after burst")
	}
}

func TestTokenBucket_Wait(t *testing.T) {
	tb := NewTokenBucket(Rate(100), 1) // 100 req/sec, burst 1

	// Consume the burst
	if !tb.Allow() {
		t.Fatal("expected initial allow")
	}

	// Wait should succeed with enough timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := tb.Wait(ctx)
	if err != nil {
		t.Errorf("expected wait to succeed, got: %v", err)
	}
}

func TestTokenBucket_WaitTimeout(t *testing.T) {
	tb := NewTokenBucket(Rate(1), 0) // 1 req/sec, no burst

	// Consume any initial tokens
	time.Sleep(10 * time.Millisecond)

	// Wait with short timeout should fail
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := tb.Wait(ctx)
	if err != context.DeadlineExceeded && err != context.Canceled {
		t.Errorf("expected timeout, got: %v", err)
	}
}

func TestTokenBucket_SetRate(t *testing.T) {
	tb := NewTokenBucket(Rate(10), 5)

	if tb.Rate() != Rate(10) {
		t.Errorf("expected rate 10, got %v", tb.Rate())
	}

	tb.SetRate(Rate(20))

	if tb.Rate() != Rate(20) {
		t.Errorf("expected rate 20, got %v", tb.Rate())
	}
}

func TestTokenBucket_SetBurst(t *testing.T) {
	tb := NewTokenBucket(Rate(10), 5)

	if tb.Burst() != 5 {
		t.Errorf("expected burst 5, got %d", tb.Burst())
	}

	tb.SetBurst(10)

	if tb.Burst() != 10 {
		t.Errorf("expected burst 10, got %d", tb.Burst())
	}
}

func TestTokenBucket_Reserve(t *testing.T) {
	tb := NewTokenBucket(Rate(10), 5)

	// Reserve should succeed when tokens available
	r := tb.Reserve()
	if !r.Ok {
		t.Error("expected reservation to succeed")
	}

	// Should have 4 tokens left
	for i := 0; i < 4; i++ {
		if !tb.Allow() {
			t.Errorf("expected allow at iteration %d", i)
		}
	}

	// Should deny
	if tb.Allow() {
		t.Error("expected deny after reservations")
	}
}

func TestUnlimitedLimiter(t *testing.T) {
	ul := Unlimited()

	// Should always allow
	for i := 0; i < 1000; i++ {
		if !ul.Allow() {
			t.Error("unlimited limiter should always allow")
		}
	}

	// Should always allow N
	if !ul.AllowN(10000) {
		t.Error("unlimited limiter should always allow N")
	}

	// Wait should return immediately
	ctx := context.Background()
	if err := ul.Wait(ctx); err != nil {
		t.Errorf("unlimited limiter wait should succeed: %v", err)
	}

	// Reserve should always succeed
	r := ul.Reserve()
	if !r.Ok {
		t.Error("unlimited limiter reserve should succeed")
	}
}

func TestRate_Duration(t *testing.T) {
	tests := []struct {
		rate     Rate
		expected time.Duration
	}{
		{Rate(1), time.Second},
		{Rate(2), 500 * time.Millisecond},
		{Rate(10), 100 * time.Millisecond},
		{Rate(0), time.Duration(1<<63 - 1)},
	}

	for _, tt := range tests {
		d := tt.rate.Duration()
		if d != tt.expected {
			t.Errorf("Rate(%v).Duration() = %v, want %v", tt.rate, d, tt.expected)
		}
	}
}

func BenchmarkTokenBucket_Allow(b *testing.B) {
	tb := NewTokenBucket(Rate(1000000), 1000000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tb.Allow()
	}
}

func BenchmarkTokenBucket_AllowParallel(b *testing.B) {
	tb := NewTokenBucket(Rate(1000000), 1000000)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tb.Allow()
		}
	})
}
