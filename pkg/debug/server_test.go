package debug

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.Enabled != false {
		t.Errorf("Enabled = %v, want false", config.Enabled)
	}
	if config.Port != 6060 {
		t.Errorf("Port = %d, want 6060", config.Port)
	}
	if config.PathPrefix != "/debug" {
		t.Errorf("PathPrefix = %s, want /debug", config.PathPrefix)
	}
}

func TestNewServer(t *testing.T) {
	config := DefaultConfig()
	server := NewServer(config, nil, nil)

	if server == nil {
		t.Fatal("expected non-nil Server")
	}
	if server.config.Port != config.Port {
		t.Errorf("config.Port = %d, want %d", server.config.Port, config.Port)
	}
}

func TestServer_Start_Disabled(t *testing.T) {
	config := Config{
		Enabled:    false,
		Port:       6061,
		PathPrefix: "/debug",
	}
	server := NewServer(config, nil, nil)

	err := server.Start()
	if err != nil {
		t.Errorf("Start() error = %v, want nil when disabled", err)
	}

	// Server should be nil when disabled
	if server.server != nil {
		t.Error("server should be nil when disabled")
	}
}

func TestServer_Start_Enabled(t *testing.T) {
	config := Config{
		Enabled:    true,
		Port:       6062,
		PathPrefix: "/debug",
	}
	server := NewServer(config, nil, nil)

	err := server.Start()
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Give server time to start
	time.Sleep(50 * time.Millisecond)

	// Test that server is running
	if server.server == nil {
		t.Error("server should not be nil when enabled")
	}

	// Test health endpoint
	resp, err := http.Get("http://localhost:6062/debug/health")
	if err != nil {
		t.Fatalf("Failed to query health endpoint: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	// Cleanup
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	server.Stop(ctx)
}

func TestServer_Stop(t *testing.T) {
	config := Config{
		Enabled:    true,
		Port:       6063,
		PathPrefix: "/debug",
	}
	server := NewServer(config, nil, nil)

	// Start server
	err := server.Start()
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Stop server
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err = server.Stop(ctx)
	if err != nil {
		t.Errorf("Stop() error = %v", err)
	}
}

func TestServer_Stop_NotStarted(t *testing.T) {
	config := DefaultConfig()
	server := NewServer(config, nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := server.Stop(ctx)
	if err != nil {
		t.Errorf("Stop() error = %v, want nil when not started", err)
	}
}

func TestHandleVars(t *testing.T) {
	config := Config{
		Enabled:    true,
		Port:       6064,
		PathPrefix: "/debug",
	}
	server := NewServer(config, nil, nil)

	server.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		server.Stop(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	resp, err := http.Get("http://localhost:6064/debug/vars")
	if err != nil {
		t.Fatalf("Failed to query vars endpoint: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	contentType := resp.Header.Get("Content-Type")
	if !strings.Contains(contentType, "application/json") {
		t.Errorf("Content-Type = %s, want application/json", contentType)
	}

	body, _ := io.ReadAll(resp.Body)
	if len(body) == 0 {
		t.Error("vars endpoint returned empty body")
	}
}

func TestHandleHealth(t *testing.T) {
	config := Config{
		Enabled:    true,
		Port:       6065,
		PathPrefix: "/debug",
	}
	server := NewServer(config, nil, nil)

	server.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		server.Stop(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	resp, err := http.Get("http://localhost:6065/debug/health")
	if err != nil {
		t.Fatalf("Failed to query health endpoint: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	// Check that health response contains expected fields
	if !strings.Contains(bodyStr, "status") {
		t.Error("health response should contain 'status'")
	}
	if !strings.Contains(bodyStr, "timestamp") {
		t.Error("health response should contain 'timestamp'")
	}
	if !strings.Contains(bodyStr, "uptime") {
		t.Error("health response should contain 'uptime'")
	}
	if !strings.Contains(bodyStr, "goroutines") {
		t.Error("health response should contain 'goroutines'")
	}
	if !strings.Contains(bodyStr, "memory") {
		t.Error("health response should contain 'memory'")
	}
}

func TestHandleGC(t *testing.T) {
	config := Config{
		Enabled:    true,
		Port:       6066,
		PathPrefix: "/debug",
	}
	server := NewServer(config, nil, nil)

	server.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		server.Stop(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	resp, err := http.Get("http://localhost:6066/debug/gc")
	if err != nil {
		t.Fatalf("Failed to query gc endpoint: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "GC triggered") {
		t.Error("gc endpoint should return 'GC triggered'")
	}
}

func TestHandleStats(t *testing.T) {
	config := Config{
		Enabled:    true,
		Port:       6067,
		PathPrefix: "/debug",
	}
	server := NewServer(config, nil, nil)

	server.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		server.Stop(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	resp, err := http.Get("http://localhost:6067/debug/stats")
	if err != nil {
		t.Fatalf("Failed to query stats endpoint: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	contentType := resp.Header.Get("Content-Type")
	if !strings.Contains(contentType, "application/json") {
		t.Errorf("Content-Type = %s, want application/json", contentType)
	}

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	// Check that stats response contains expected sections
	if !strings.Contains(bodyStr, "runtime") {
		t.Error("stats response should contain 'runtime'")
	}
	if !strings.Contains(bodyStr, "memory") {
		t.Error("stats response should contain 'memory'")
	}
	if !strings.Contains(bodyStr, "gc") {
		t.Error("stats response should contain 'gc'")
	}
}

func TestHandleFreeMemory(t *testing.T) {
	config := Config{
		Enabled:    true,
		Port:       6068,
		PathPrefix: "/debug",
	}
	server := NewServer(config, nil, nil)

	server.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		server.Stop(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	resp, err := http.Get("http://localhost:6068/debug/freemem")
	if err != nil {
		t.Fatalf("Failed to query freemem endpoint: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "Memory freed") {
		t.Error("freemem endpoint should return 'Memory freed'")
	}
}

func TestGetProfileURL(t *testing.T) {
	config := Config{
		Enabled:    true,
		Port:       6069,
		PathPrefix: "/debug",
	}
	server := NewServer(config, nil, nil)

	tests := []struct {
		profile  ProfileType
		expected string
	}{
		{ProfileCPU, "http://localhost:6069/debug/pprof/profile"},
		{ProfileHeap, "http://localhost:6069/debug/pprof/heap"},
		{ProfileGoroutine, "http://localhost:6069/debug/pprof/goroutine"},
		{ProfileThread, "http://localhost:6069/debug/pprof/threadcreate"},
		{ProfileBlock, "http://localhost:6069/debug/pprof/block"},
		{ProfileMutex, "http://localhost:6069/debug/pprof/mutex"},
	}

	for _, tt := range tests {
		t.Run(string(tt.profile), func(t *testing.T) {
			url := server.GetProfileURL(tt.profile)
			if url != tt.expected {
				t.Errorf("GetProfileURL(%s) = %s, want %s", tt.profile, url, tt.expected)
			}
		})
	}
}

func TestExpVar(t *testing.T) {
	// Test int
	ExpVar("test_int", 42)

	// Test int64
	ExpVar("test_int64", int64(9223372036854775807))

	// Test float64
	ExpVar("test_float", 3.14)

	// Test string
	ExpVar("test_string", "hello")

	// Test complex type
	type customStruct struct {
		Name  string
		Value int
	}
	ExpVar("test_custom", customStruct{Name: "test", Value: 100})
}

func TestReadTCPTimeouts(t *testing.T) {
	readTimeout, writeTimeout := ReadTCPTimeouts()

	if readTimeout != 30*time.Second {
		t.Errorf("readTimeout = %v, want 30s", readTimeout)
	}
	if writeTimeout != 30*time.Second {
		t.Errorf("writeTimeout = %v, want 30s", writeTimeout)
	}
}

func TestParseBool(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"true", true},
		{"1", true},
		{"TRUE", true},
		{"True", true},
		{"false", false},
		{"0", false},
		{"FALSE", false},
		{"False", false},
		{"", false},
		{"invalid", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := ParseBool(tt.input)
			if result != tt.expected {
				t.Errorf("ParseBool(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestPprofEndpoints(t *testing.T) {
	config := Config{
		Enabled:    true,
		Port:       6070,
		PathPrefix: "/debug",
	}
	server := NewServer(config, nil, nil)

	server.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		server.Stop(ctx)
	}()

	time.Sleep(50 * time.Millisecond)

	// Test pprof index endpoint
	resp, err := http.Get("http://localhost:6070/debug/pprof/")
	if err != nil {
		t.Fatalf("Failed to query pprof index: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("pprof index StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	// Test heap profile endpoint
	resp, err = http.Get("http://localhost:6070/debug/pprof/heap?debug=1")
	if err != nil {
		t.Fatalf("Failed to query heap profile: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("heap profile StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func BenchmarkServer_StartStop(b *testing.B) {
	config := Config{
		Enabled:    true,
		Port:       6071,
		PathPrefix: "/debug",
	}

	for i := 0; i < b.N; i++ {
		server := NewServer(config, nil, nil)
		server.Start()
		time.Sleep(10 * time.Millisecond)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		server.Stop(ctx)
		cancel()
	}
}
