package wire

import (
	"context"
	"sync"

	"github.com/mammothengine/mammoth/pkg/shutdown"
)

// ShutdownManager wraps shutdown.Manager for wire server.
type ShutdownManager struct {
	*shutdown.Manager
	server *Server
}

// NewShutdownManager creates a new shutdown manager for the server.
func NewShutdownManager(server *Server) *ShutdownManager {
	config := shutdown.DefaultConfig()
	return NewShutdownManagerWithConfig(server, config)
}

// NewShutdownManagerWithConfig creates a shutdown manager with custom config.
func NewShutdownManagerWithConfig(server *Server, config shutdown.Config) *ShutdownManager {
	m := shutdown.NewManager(config)

	sm := &ShutdownManager{
		Manager: m,
		server:  server,
	}

	// Register default hooks
	sm.registerDefaultHooks()

	return sm
}

// registerDefaultHooks registers the default shutdown hooks.
func (sm *ShutdownManager) registerDefaultHooks() {
	// Hook to close the wire server
	sm.RegisterHook(func(ctx context.Context) error {
		if sm.server != nil {
			return sm.server.Close()
		}
		return nil
	})

	// Hook to close the handler
	sm.RegisterHook(func(ctx context.Context) error {
		if sm.server != nil && sm.server.handler != nil {
			sm.server.handler.Close()
		}
		return nil
	})
}

// GracefulShutdown initiates graceful shutdown of the server.
func (sm *ShutdownManager) GracefulShutdown(ctx context.Context) error {
	// Stop accepting new connections
	if sm.server != nil {
		sm.server.closed.Store(true)
	}

	// Execute shutdown manager shutdown
	return sm.Shutdown(ctx)
}

// WaitForShutdown blocks until shutdown is complete.
func (sm *ShutdownManager) WaitForShutdown() {
	sm.Wait()
}

// ServerWithShutdown wraps a Server with shutdown capabilities.
type ServerWithShutdown struct {
	*Server
	shutdownMgr *ShutdownManager
}

// NewServerWithShutdown creates a new server with shutdown management.
func NewServerWithShutdown(config ServerConfig) (*ServerWithShutdown, error) {
	server, err := NewServer(config)
	if err != nil {
		return nil, err
	}

	shutdownMgr := NewShutdownManager(server)

	return &ServerWithShutdown{
		Server:      server,
		shutdownMgr: shutdownMgr,
	}, nil
}

// Shutdown initiates graceful shutdown.
func (s *ServerWithShutdown) Shutdown(ctx context.Context) error {
	return s.shutdownMgr.GracefulShutdown(ctx)
}

// ShutdownManager returns the shutdown manager.
func (s *ServerWithShutdown) ShutdownManager() *ShutdownManager {
	return s.shutdownMgr
}

// ConnTracker tracks active connections for shutdown.
type ConnTracker struct {
	mu          sync.RWMutex
	connections map[uint64]struct{}
}

// NewConnTracker creates a new connection tracker.
func NewConnTracker() *ConnTracker {
	return &ConnTracker{
		connections: make(map[uint64]struct{}),
	}
}

// Add adds a connection.
func (ct *ConnTracker) Add(connID uint64) {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	ct.connections[connID] = struct{}{}
}

// Remove removes a connection.
func (ct *ConnTracker) Remove(connID uint64) {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	delete(ct.connections, connID)
}

// Count returns the number of active connections.
func (ct *ConnTracker) Count() int {
	ct.mu.RLock()
	defer ct.mu.RUnlock()
	return len(ct.connections)
}

// CloseAll closes all tracked connections.
func (ct *ConnTracker) CloseAll() {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	ct.connections = make(map[uint64]struct{})
}
