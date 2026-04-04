package wire

import (
	"context"
	"testing"
	"time"
)

func TestServer_StartTime(t *testing.T) {
	server, err := NewServer(ServerConfig{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	startTime := server.StartTime()
	if startTime.IsZero() {
		t.Error("StartTime should not be zero")
	}

	// Should be recent
	if time.Since(startTime) > time.Minute {
		t.Error("StartTime should be recent")
	}
}

func TestServer_ConnCount(t *testing.T) {
	server, err := NewServer(ServerConfig{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Initial count should be 0
	if count := server.ConnCount(); count != 0 {
		t.Errorf("Initial ConnCount = %d, want 0", count)
	}
}

func TestNewServerWithShutdown(t *testing.T) {
	server, err := NewServerWithShutdown(ServerConfig{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("Failed to create server with shutdown: %v", err)
	}

	// Test ShutdownManager access
	mgr := server.ShutdownManager()
	if mgr == nil {
		t.Error("ShutdownManager should not be nil")
	}

	// Test Shutdown
	ctx := context.Background()
	err = server.Shutdown(ctx)
	if err != nil {
		t.Errorf("Shutdown error: %v", err)
	}
}

func TestShutdownManager_GracefulShutdown(t *testing.T) {
	server, _ := NewServer(ServerConfig{Addr: "127.0.0.1:0"})
	mgr := NewShutdownManager(server)

	ctx := context.Background()
	err := mgr.GracefulShutdown(ctx)
	if err != nil {
		t.Errorf("GracefulShutdown error: %v", err)
	}
}

func TestShutdownManager_WaitForShutdown(t *testing.T) {
	server, _ := NewServer(ServerConfig{Addr: "127.0.0.1:0"})
	mgr := NewShutdownManager(server)

	// WaitForShutdown should not block indefinitely if shutdown is triggered
	go func() {
		time.Sleep(100 * time.Millisecond)
		mgr.Shutdown(context.Background())
	}()

	done := make(chan bool)
	go func() {
		mgr.WaitForShutdown()
		done <- true
	}()

	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Error("WaitForShutdown did not return after shutdown")
	}
}
