package wire

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mammothengine/mammoth/pkg/shutdown"
)

func TestConnTracker(t *testing.T) {
	t.Run("Add and Count", func(t *testing.T) {
		ct := NewConnTracker()

		if ct.Count() != 0 {
			t.Errorf("Initial count = %d, want 0", ct.Count())
		}

		// Add connections
		ct.Add(1)
		ct.Add(2)
		ct.Add(3)

		if ct.Count() != 3 {
			t.Errorf("Count after add = %d, want 3", ct.Count())
		}
	})

	t.Run("Remove", func(t *testing.T) {
		ct := NewConnTracker()

		ct.Add(1)
		ct.Add(2)
		ct.Remove(1)

		if ct.Count() != 1 {
			t.Errorf("Count after remove = %d, want 1", ct.Count())
		}

		// Remove non-existent
		ct.Remove(999)
		if ct.Count() != 1 {
			t.Errorf("Count after removing non-existent = %d, want 1", ct.Count())
		}
	})

	t.Run("CloseAll", func(t *testing.T) {
		ct := NewConnTracker()

		// Add some connection IDs
		ct.Add(1)
		ct.Add(2)
		ct.Add(3)

		if ct.Count() != 3 {
			t.Fatalf("Count before CloseAll = %d, want 3", ct.Count())
		}

		// Close all - clears all connections
		ct.CloseAll()

		// Count should be 0 after CloseAll
		if ct.Count() != 0 {
			t.Errorf("Count after CloseAll = %d, want 0", ct.Count())
		}
	})

	t.Run("Concurrent Access", func(t *testing.T) {
		ct := NewConnTracker()
		var wg sync.WaitGroup

		// Concurrent adds
		for i := uint64(0); i < 100; i++ {
			wg.Add(1)
			go func(n uint64) {
				defer wg.Done()
				ct.Add(n)
			}(i)
		}

		wg.Wait()

		if ct.Count() != 100 {
			t.Errorf("Count after concurrent adds = %d, want 100", ct.Count())
		}

		// Concurrent removes
		for i := uint64(0); i < 50; i++ {
			wg.Add(1)
			go func(n uint64) {
				defer wg.Done()
				ct.Remove(n)
			}(i)
		}

		wg.Wait()

		if ct.Count() != 50 {
			t.Errorf("Count after concurrent removes = %d, want 50", ct.Count())
		}
	})
}

// testJob is a mock background job for testing
type testJob struct {
	started bool
	stopped bool
	mu      sync.Mutex
}

func (j *testJob) Start() error {
	j.mu.Lock()
	j.started = true
	j.mu.Unlock()
	return nil
}

func (j *testJob) Stop(ctx context.Context) error {
	j.mu.Lock()
	j.stopped = true
	j.mu.Unlock()
	return nil
}

func TestShutdownManager(t *testing.T) {
	t.Run("Basic Shutdown", func(t *testing.T) {
		server, err := NewServer(ServerConfig{Addr: "127.0.0.1:0"})
		if err != nil {
			t.Fatalf("Failed to create server: %v", err)
		}

		mgr := NewShutdownManager(server)
		if mgr == nil {
			t.Fatal("NewShutdownManager returned nil")
		}

		// Start shutdown
		ctx := context.Background()
		err = mgr.Shutdown(ctx)
		if err != nil {
			t.Errorf("Shutdown error: %v", err)
		}
	})

	t.Run("RegisterHook", func(t *testing.T) {
		server, _ := NewServer(ServerConfig{Addr: "127.0.0.1:0"})
		mgr := NewShutdownManager(server)

		hookCalled := false
		mgr.RegisterHook(func(ctx context.Context) error {
			hookCalled = true
			return nil
		})

		// Trigger shutdown
		mgr.Shutdown(context.Background())

		// Hook should have been called
		if !hookCalled {
			t.Error("Hook was not called during shutdown")
		}
	})

	t.Run("RegisterBackgroundJob", func(t *testing.T) {
		server, _ := NewServer(ServerConfig{Addr: "127.0.0.1:0"})
		mgr := NewShutdownManager(server)

		job := &testJob{}
		mgr.RegisterBackgroundJob("test-job", job)

		// Start and shutdown
		mgr.Shutdown(context.Background())

		// Job should have been stopped
		if !job.stopped {
			t.Error("Background job was not stopped")
		}
	})

	t.Run("StartRequest_EndRequest", func(t *testing.T) {
		server, _ := NewServer(ServerConfig{Addr: "127.0.0.1:0"})
		mgr := NewShutdownManager(server)

		// Start a request
		if !mgr.StartRequest() {
			t.Error("StartRequest returned false before shutdown")
		}

		// End the request
		mgr.EndRequest()
	})
}

func TestNewShutdownManagerWithConfig(t *testing.T) {
	config := shutdown.Config{
		DrainTimeout: 500 * time.Millisecond,
		ForceTimeout: 1 * time.Second,
	}

	server, _ := NewServer(ServerConfig{Addr: "127.0.0.1:0"})
	mgr := NewShutdownManagerWithConfig(server, config)
	if mgr == nil {
		t.Fatal("NewShutdownManagerWithConfig returned nil")
	}
}
