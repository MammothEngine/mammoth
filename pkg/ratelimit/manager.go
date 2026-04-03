package ratelimit

import (
	"context"
	"sync"
	"sync/atomic"
)

// Manager manages rate limiters for connections.
type Manager struct {
	config        Config
	globalLimiter Limiter
	limiters      map[uint64]*TokenBucket
	mu            sync.RWMutex
	activeCount   atomic.Int64
}

// NewManager creates a new rate limiter manager.
func NewManager(cfg Config) *Manager {
	m := &Manager{
		config:   cfg,
		limiters: make(map[uint64]*TokenBucket),
	}

	if cfg.Enabled {
		m.globalLimiter = NewTokenBucket(Rate(cfg.GlobalRate), cfg.GlobalBurst)
	} else {
		m.globalLimiter = Unlimited()
	}

	return m
}

// GetLimiter returns the rate limiter for a connection.
// Returns nil if per-connection limiting is disabled.
func (m *Manager) GetLimiter(connID uint64) Limiter {
	if !m.config.Enabled {
		return Unlimited()
	}

	if !m.config.PerConnection {
		return m.globalLimiter
	}

	m.mu.RLock()
	limiter, exists := m.limiters[connID]
	m.mu.RUnlock()

	if exists {
		return limiter
	}

	// Create new limiter
	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if limiter, exists = m.limiters[connID]; exists {
		return limiter
	}

	limiter = NewTokenBucket(
		Rate(m.config.RequestsPerSecond),
		m.config.Burst,
	)
	m.limiters[connID] = limiter
	m.activeCount.Add(1)

	return limiter
}

// RemoveLimiter removes a connection's rate limiter.
func (m *Manager) RemoveLimiter(connID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.limiters[connID]; exists {
		delete(m.limiters, connID)
		m.activeCount.Add(-1)
	}
}

// GlobalLimiter returns the global rate limiter.
func (m *Manager) GlobalLimiter() Limiter {
	return m.globalLimiter
}

// Allow checks if a request is allowed for a connection.
// This checks both global and per-connection limits.
func (m *Manager) Allow(connID uint64) bool {
	if !m.config.Enabled {
		return true
	}

	// Check global limit first
	if !m.globalLimiter.Allow() {
		return false
	}

	// Check per-connection limit if enabled
	if m.config.PerConnection {
		limiter := m.GetLimiter(connID)
		if !limiter.Allow() {
			// Return token to global bucket since we consumed it
			return false
		}
	}

	return true
}

// AllowN checks if n requests are allowed for a connection.
func (m *Manager) AllowN(connID uint64, n int) bool {
	if !m.config.Enabled {
		return true
	}

	// Check global limit first
	if !m.globalLimiter.AllowN(n) {
		return false
	}

	// Check per-connection limit if enabled
	if m.config.PerConnection {
		limiter := m.GetLimiter(connID)
		if !limiter.AllowN(n) {
			return false
		}
	}

	return true
}

// Wait waits for a token with context timeout.
func (m *Manager) Wait(ctx context.Context, connID uint64) error {
	if !m.config.Enabled {
		return nil
	}

	// Check global limit
	if err := m.globalLimiter.Wait(ctx); err != nil {
		return err
	}

	// Check per-connection limit if enabled
	if m.config.PerConnection {
		limiter := m.GetLimiter(connID)
		if err := limiter.Wait(ctx); err != nil {
			return err
		}
	}

	return nil
}

// ActiveCount returns the number of active connection limiters.
func (m *Manager) ActiveCount() int64 {
	return m.activeCount.Load()
}

// UpdateConfig updates the rate limiter configuration.
func (m *Manager) UpdateConfig(cfg Config) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.config = cfg

	if cfg.Enabled {
		m.globalLimiter = NewTokenBucket(Rate(cfg.GlobalRate), cfg.GlobalBurst)
	} else {
		m.globalLimiter = Unlimited()
	}

	// Update existing limiters
	if cfg.PerConnection {
		for _, limiter := range m.limiters {
			limiter.SetRate(Rate(cfg.RequestsPerSecond))
			limiter.SetBurst(cfg.Burst)
		}
	}
}

// Close cleans up all limiters.
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.limiters = make(map[uint64]*TokenBucket)
	m.activeCount.Store(0)
}
