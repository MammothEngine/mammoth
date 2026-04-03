package ratelimit

import (
	"context"
	"testing"
	"time"
)

func TestManager_Allow(t *testing.T) {
	cfg := Config{
		Enabled:           true,
		RequestsPerSecond: 100,
		Burst:             10,
		PerConnection:     true,
		GlobalRate:        1000,
		GlobalBurst:       100,
	}

	m := NewManager(cfg)
	defer m.Close()

	connID := uint64(1)

	// Should allow burst
	for i := 0; i < 10; i++ {
		if !m.Allow(connID) {
			t.Errorf("expected allow at iteration %d", i)
		}
	}

	// Should deny after burst
	if m.Allow(connID) {
		t.Error("expected deny after burst")
	}
}

func TestManager_GlobalLimit(t *testing.T) {
	cfg := Config{
		Enabled:           true,
		RequestsPerSecond: 1000, // High per-connection
		Burst:             1000,
		PerConnection:     true,
		GlobalRate:        5, // Low global limit
		GlobalBurst:       5,
	}

	m := NewManager(cfg)
	defer m.Close()

	// Exhaust global limit with different connections
	for i := uint64(0); i < 5; i++ {
		if !m.Allow(i) {
			t.Errorf("expected allow for conn %d", i)
		}
	}

	// Should deny due to global limit
	if m.Allow(100) {
		t.Error("expected deny due to global limit")
	}
}

func TestManager_PerConnectionDisabled(t *testing.T) {
	cfg := Config{
		Enabled:           true,
		RequestsPerSecond: 100,
		Burst:             10,
		PerConnection:     false, // Disabled
		GlobalRate:        100,
		GlobalBurst:       10,
	}

	m := NewManager(cfg)
	defer m.Close()

	// Should share global limiter
	limiter1 := m.GetLimiter(1)
	limiter2 := m.GetLimiter(2)

	if limiter1 != limiter2 {
		t.Error("expected same limiter when per-connection is disabled")
	}
}

func TestManager_Disabled(t *testing.T) {
	cfg := Config{
		Enabled: false,
	}

	m := NewManager(cfg)
	defer m.Close()

	// Should always allow when disabled
	for i := 0; i < 1000; i++ {
		if !m.Allow(uint64(i)) {
			t.Error("expected allow when disabled")
		}
	}
}

func TestManager_RemoveLimiter(t *testing.T) {
	cfg := Config{
		Enabled:           true,
		RequestsPerSecond: 100,
		Burst:             10,
		PerConnection:     true,
		GlobalRate:        1000,
		GlobalBurst:       100,
	}

	m := NewManager(cfg)
	defer m.Close()

	connID := uint64(1)

	// Create limiter
	m.GetLimiter(connID)

	if m.ActiveCount() != 1 {
		t.Errorf("expected 1 active limiter, got %d", m.ActiveCount())
	}

	// Remove limiter
	m.RemoveLimiter(connID)

	if m.ActiveCount() != 0 {
		t.Errorf("expected 0 active limiters, got %d", m.ActiveCount())
	}
}

func TestManager_Wait(t *testing.T) {
	cfg := Config{
		Enabled:           true,
		RequestsPerSecond: 100,
		Burst:             1,
		PerConnection:     true,
		GlobalRate:        1000,
		GlobalBurst:       100,
	}

	m := NewManager(cfg)
	defer m.Close()

	connID := uint64(1)

	// Consume burst
	m.Allow(connID)

	// Wait should succeed
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := m.Wait(ctx, connID)
	if err != nil {
		t.Errorf("expected wait to succeed: %v", err)
	}
}

func TestManager_UpdateConfig(t *testing.T) {
	cfg := Config{
		Enabled:           true,
		RequestsPerSecond: 100,
		Burst:             10,
		PerConnection:     true,
		GlobalRate:        1000,
		GlobalBurst:       100,
	}

	m := NewManager(cfg)
	defer m.Close()

	// Create a limiter
	limiter := m.GetLimiter(1).(*TokenBucket)

	if limiter.Rate() != Rate(100) {
		t.Errorf("expected rate 100, got %v", limiter.Rate())
	}

	// Update config
	newCfg := Config{
		Enabled:           true,
		RequestsPerSecond: 200,
		Burst:             20,
		PerConnection:     true,
		GlobalRate:        2000,
		GlobalBurst:       200,
	}
	m.UpdateConfig(newCfg)

	if limiter.Rate() != Rate(200) {
		t.Errorf("expected rate 200 after update, got %v", limiter.Rate())
	}

	if limiter.Burst() != 20 {
		t.Errorf("expected burst 20 after update, got %d", limiter.Burst())
	}
}

func BenchmarkManager_Allow(b *testing.B) {
	cfg := Config{
		Enabled:           true,
		RequestsPerSecond: 1000000,
		Burst:             1000000,
		PerConnection:     true,
		GlobalRate:        10000000,
		GlobalBurst:       1000000,
	}

	m := NewManager(cfg)
	defer m.Close()

	connID := uint64(1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Allow(connID)
	}
}

func BenchmarkManager_AllowParallel(b *testing.B) {
	cfg := Config{
		Enabled:           true,
		RequestsPerSecond: 1000000,
		Burst:             1000000,
		PerConnection:     true,
		GlobalRate:        10000000,
		GlobalBurst:       1000000,
	}

	m := NewManager(cfg)
	defer m.Close()

	b.RunParallel(func(pb *testing.PB) {
		connID := uint64(1)
		for pb.Next() {
			m.Allow(connID)
		}
	})
}
