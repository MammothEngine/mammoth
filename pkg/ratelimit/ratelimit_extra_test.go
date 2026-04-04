package ratelimit

import (
	"context"
	"testing"
	"time"
)

// TestRatePerSecond tests RatePerSecond helper
func TestRatePerSecond(t *testing.T) {
	r := RatePerSecond(10)
	if r != Rate(10) {
		t.Errorf("RatePerSecond(10) = %v, want %v", r, Rate(10))
	}
}

// TestRatePerMinute tests RatePerMinute helper
func TestRatePerMinute(t *testing.T) {
	r := RatePerMinute(60)
	if r != Rate(1) {
		t.Errorf("RatePerMinute(60) = %v, want %v", r, Rate(1))
	}
}

// TestRateDuration tests Rate.Duration method
func TestRateDuration(t *testing.T) {
	// Rate of 10 events per second = 100ms between events
	r := Rate(10)
	d := r.Duration()
	if d != 100*time.Millisecond {
		t.Errorf("Rate(10).Duration() = %v, want 100ms", d)
	}

	// Zero rate should return max duration
	r = Rate(0)
	d = r.Duration()
	if d != time.Duration(1<<63-1) {
		t.Errorf("Rate(0).Duration() = %v, want max duration", d)
	}
}

// TestReservationDelay tests Reservation.Delay method
func TestReservationDelay(t *testing.T) {
	r := &Reservation{
		Ok:        true,
		TimeToAct: time.Now().Add(100 * time.Millisecond),
		Limit:     Rate(10),
	}

	delay := r.Delay()
	// Delay should be approximately 100ms (with some tolerance)
	if delay < 50*time.Millisecond || delay > 200*time.Millisecond {
		t.Errorf("Delay() = %v, expected ~100ms", delay)
	}

	// DelayFrom with future reference time should return 0
	delayFrom := r.DelayFrom(time.Now().Add(1 * time.Hour))
	if delayFrom != 0 {
		t.Errorf("DelayFrom(future) = %v, want 0", delayFrom)
	}
}

// TestReservationDelayNotOK tests Delay when reservation is not OK
func TestReservationDelayNotOK(t *testing.T) {
	r := &Reservation{
		Ok:        false,
		TimeToAct: time.Now().Add(100 * time.Millisecond),
	}

	delay := r.Delay()
	// When not OK, Delay returns max duration
	if delay != time.Duration(1<<63-1) {
		t.Errorf("Delay() for !Ok reservation = %v, want max duration", delay)
	}
}

// TestReservationCancel tests Reservation.Cancel method
func TestReservationCancel(t *testing.T) {
	tb := NewTokenBucket(Rate(10), 5)

	// Get a reservation
	r := tb.Reserve()
	if r == nil {
		t.Fatal("expected reservation")
	}
	if !r.Ok {
		t.Fatal("expected OK reservation")
	}

	// Cancel it
	r.Cancel()

	// After cancel, should be able to get another reservation quickly
	r2 := tb.Reserve()
	if r2 == nil {
		t.Fatal("expected second reservation after cancel")
	}
	if r2.Ok {
		r2.Cancel()
	}
}

// TestDefaultConfig tests DefaultConfig function
func TestDefaultConfigFull(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Enabled {
		t.Error("default Enabled should be false")
	}
	if cfg.RequestsPerSecond != 1000 {
		t.Errorf("default RequestsPerSecond = %v, want 1000", cfg.RequestsPerSecond)
	}
	if cfg.Burst != 100 {
		t.Errorf("default Burst = %v, want 100", cfg.Burst)
	}
	if !cfg.PerConnection {
		t.Error("default PerConnection should be true")
	}
	if cfg.GlobalRate != 10000 {
		t.Errorf("default GlobalRate = %v, want 10000", cfg.GlobalRate)
	}
	if cfg.GlobalBurst != 1000 {
		t.Errorf("default GlobalBurst = %v, want 1000", cfg.GlobalBurst)
	}
	if cfg.WaitTimeout != 100*time.Millisecond {
		t.Errorf("default WaitTimeout = %v, want 100ms", cfg.WaitTimeout)
	}
}

// TestNewTokenBucketFromConfig tests NewTokenBucketFromConfig
func TestNewTokenBucketFromConfig(t *testing.T) {
	cfg := Config{
		Enabled:           true,
		RequestsPerSecond: 50,
		Burst:             20,
	}

	tb := NewTokenBucketFromConfig(cfg)
	if tb == nil {
		t.Fatal("expected non-nil TokenBucket")
	}

	// Should allow burst
	for i := 0; i < 20; i++ {
		if !tb.Allow() {
			t.Errorf("expected allow at iteration %d", i)
		}
	}

	// Should deny after burst
	if tb.Allow() {
		t.Error("expected deny after burst")
	}
}

// TestTokenBucketTokens tests Tokens method
func TestTokenBucketTokens(t *testing.T) {
	tb := NewTokenBucket(Rate(10), 5)

	// Initially should have full burst
	tokens := tb.Tokens()
	if tokens != 5 {
		t.Errorf("initial tokens = %v, want 5", tokens)
	}

	// Consume one
	tb.Allow()

	tokens = tb.Tokens()
	if tokens != 4 {
		t.Errorf("tokens after 1 consume = %v, want 4", tokens)
	}
}

// TestTokenBucketSetBurst tests SetBurst method
func TestTokenBucketSetBurst(t *testing.T) {
	tb := NewTokenBucket(Rate(10), 5)

	// Initially should have 5 tokens
	if tb.Tokens() != 5 {
		t.Errorf("initial tokens = %v, want 5", tb.Tokens())
	}

	// Increase burst
	tb.SetBurst(10)

	// Burst should be increased (tokens may refill)
	if tb.Burst() != 10 {
		t.Errorf("burst = %v after SetBurst(10), want 10", tb.Burst())
	}

	// Decrease burst
	tb.SetBurst(3)

	// Burst should be decreased
	if tb.Burst() != 3 {
		t.Errorf("burst = %v after SetBurst(3), want 3", tb.Burst())
	}

	// Tokens should be capped at new burst
	tokens := tb.Tokens()
	if tokens > 3 {
		t.Errorf("tokens = %v after SetBurst(3), should be <= 3", tokens)
	}
}

// TestTokenBucketReserveN tests ReserveN method
func TestTokenBucketReserveN(t *testing.T) {
	tb := NewTokenBucket(Rate(10), 10)
	now := time.Now()

	// Reserve 5
	r := tb.ReserveN(now, 5)
	if r == nil {
		t.Fatal("expected reservation")
	}
	if !r.Ok {
		t.Error("expected OK reservation for 5 tokens with burst 10")
	}

	// Reserve more than burst - should fail
	r2 := tb.ReserveN(now, 20)
	if r2 == nil {
		t.Fatal("expected non-nil reservation")
	}
	if r2.Ok {
		t.Error("expected !Ok reservation for 20 tokens with burst 10")
	}
}

// TestManagerGlobalLimiter tests GlobalLimiter and AllowN methods
func TestManagerGlobalLimiter(t *testing.T) {
	mgr := NewManager(Config{
		RequestsPerSecond: 1000,
		Burst:             100,
		GlobalRate:        1000,
		GlobalBurst:       100,
		Enabled:           true,
	})
	defer mgr.Close()

	// Get global limiter
	global := mgr.GlobalLimiter()
	if global == nil {
		t.Fatal("expected non-nil global limiter")
	}

	connID := uint64(1)

	// AllowN should work
	if !mgr.AllowN(connID, 1) {
		t.Error("expected AllowN to succeed")
	}

	// Exhaust burst
	for i := 0; i < 100; i++ {
		mgr.Allow(connID)
	}

	// Should deny now
	if mgr.AllowN(connID, 1) {
		t.Error("expected AllowN to fail after burst")
	}
}

// TestManagerGetLimiterAutoCreate tests GetLimiter auto-creation
func TestManagerGetLimiterAutoCreate(t *testing.T) {
	mgr := NewManager(Config{
		RequestsPerSecond: 100,
		Burst:             10,
		Enabled:           true,
		PerConnection:     true,
	})
	defer mgr.Close()

	// Get limiter for a key - should auto-create
	l1 := mgr.GetLimiter(1)
	if l1 == nil {
		t.Fatal("expected non-nil limiter")
	}

	// Get same key again - should return same limiter
	l2 := mgr.GetLimiter(1)
	if l1 != l2 {
		t.Error("expected same limiter instance for same key")
	}

	// Get different key - should return different limiter
	l3 := mgr.GetLimiter(2)
	if l3 == l1 {
		t.Error("expected different limiter for different key")
	}
}

// TestUnlimitedLimiterMethods tests Unlimited limiter's Rate and Burst methods
func TestUnlimitedLimiterMethods(t *testing.T) {
	ul := Unlimited()

	// Rate should return max value
	if ul.Rate() != Rate(1<<63-1) {
		t.Errorf("Unlimited.Rate() = %v, want max", ul.Rate())
	}

	// Burst should return max value
	if ul.Burst() != 1<<31-1 {
		t.Errorf("Unlimited.Burst() = %v, want max", ul.Burst())
	}

	// WaitN should always return nil
	ctx := context.Background()
	if err := ul.WaitN(ctx, 100); err != nil {
		t.Errorf("Unlimited.WaitN() should return nil, got %v", err)
	}
}



// TestManagerUpdateConfig tests Manager.UpdateConfig
func TestManagerUpdateConfig(t *testing.T) {
	mgr := NewManager(Config{
		RequestsPerSecond: 100,
		Burst:             10,
		Enabled:           true,
	})
	defer mgr.Close()

	// Update config - should not panic
	newConfig := Config{
		RequestsPerSecond: 200,
		Burst:             20,
		Enabled:           true,
	}
	mgr.UpdateConfig(newConfig)

	// Disable and verify behavior changes
	mgr.UpdateConfig(Config{Enabled: false})

	// After disabling, Allow should always return true (unlimited)
	if !mgr.Allow(1) {
		t.Error("Allow should return true when disabled")
	}
}


// TestTokenBucketWaitNTimeout tests TokenBucket.WaitN with timeout
func TestTokenBucketWaitNTimeout(t *testing.T) {
	tb := NewTokenBucket(Rate(1), 1)

	// Exhaust tokens
	tb.Allow()

	// Try to wait for more than burst
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	// Should timeout
	err := tb.WaitN(ctx, 10)
	if err == nil {
		t.Error("expected timeout error for n > burst")
	}
}

// TestReservationCancelNoOp tests Cancel when not implemented
func TestReservationCancelNoOp(t *testing.T) {
	// Create a reservation directly (not through limiter)
	r := &Reservation{
		Ok:        true,
		TimeToAct: time.Now(),
		Limit:     Rate(10),
	}

	// Cancel should not panic even though it has no limiter reference
	r.Cancel()
}

// TestManagerAllowNGlobal tests Manager.AllowN with global limiting
func TestManagerAllowNGlobal(t *testing.T) {
	mgr := NewManager(Config{
		RequestsPerSecond: 1000,
		Burst:             1000,
		GlobalRate:        10,
		GlobalBurst:       5,
		Enabled:           true,
		PerConnection:     false,
	})
	defer mgr.Close()

	connID := uint64(1)

	// Should be limited by global burst of 5
	for i := 0; i < 5; i++ {
		if !mgr.AllowN(connID, 1) {
			t.Errorf("expected allow at iteration %d", i)
		}
	}

	// Should deny now due to global limit
	if mgr.AllowN(connID, 1) {
		t.Error("expected deny after global burst exhausted")
	}
}
