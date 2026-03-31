package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultConfigIsValid(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config should be valid: %v", err)
	}
	if cfg.Server.Port != 27017 {
		t.Errorf("default port = %d, want 27017", cfg.Server.Port)
	}
	if cfg.Server.Bind != "0.0.0.0" {
		t.Errorf("default bind = %q", cfg.Server.Bind)
	}
	if cfg.Server.DataDir != "./data" {
		t.Errorf("default data-dir = %q", cfg.Server.DataDir)
	}
}

func TestConfigValidationPort(t *testing.T) {
	cfg := DefaultConfig()

	cfg.Server.Port = 0
	if err := cfg.Validate(); err == nil {
		t.Error("port 0 should fail")
	}

	cfg.Server.Port = 70000
	if err := cfg.Validate(); err == nil {
		t.Error("port 70000 should fail")
	}

	cfg.Server.Port = 27017
	if err := cfg.Validate(); err != nil {
		t.Fatalf("port 27017 should pass: %v", err)
	}
}

func TestConfigValidationLogLevel(t *testing.T) {
	cfg := DefaultConfig()
	for _, level := range []string{"debug", "info", "warn", "error"} {
		cfg.Server.LogLevel = level
		if err := cfg.Validate(); err != nil {
			t.Errorf("level %q should pass", level)
		}
	}
	cfg.Server.LogLevel = "verbose"
	if err := cfg.Validate(); err == nil {
		t.Error("verbose should fail")
	}
}

func TestConfigValidationEmptyDataDir(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Server.DataDir = ""
	if err := cfg.Validate(); err == nil {
		t.Error("empty data-dir should fail")
	}
}

func TestConfigValidationTLS(t *testing.T) {
	cfg := DefaultConfig()

	cfg.Server.TLS.CertFile = "cert.pem"
	cfg.Server.TLS.KeyFile = ""
	if err := cfg.Validate(); err == nil {
		t.Error("cert without key should fail")
	}

	cfg.Server.TLS.CertFile = ""
	cfg.Server.TLS.KeyFile = "key.pem"
	if err := cfg.Validate(); err == nil {
		t.Error("key without cert should fail")
	}

	cfg.Server.TLS.CertFile = "cert.pem"
	cfg.Server.TLS.KeyFile = "key.pem"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("both cert+key should pass: %v", err)
	}
}

func TestLoadFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.conf")
	content := `
[server]
port = 28017
bind = "127.0.0.1"
data-dir = "/tmp/mammoth"
log-level = "debug"

[server.tls]
cert-file = "/certs/cert.pem"
key-file = "/certs/key.pem"

[server.admin]
port = 9090

[server.metrics]
port = 9200

[server.auth]
enabled = true

[engine]
memtable-size = 33554432
cache-size = 67108864
compaction-threads = 4
wal-sync-mode = "batch"

[replication]
enabled = true
node-id = 3
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if cfg.Server.Port != 28017 {
		t.Errorf("port = %d, want 28017", cfg.Server.Port)
	}
	if cfg.Server.Bind != "127.0.0.1" {
		t.Errorf("bind = %q, want 127.0.0.1", cfg.Server.Bind)
	}
	if cfg.Server.DataDir != "/tmp/mammoth" {
		t.Errorf("data-dir = %q", cfg.Server.DataDir)
	}
	if cfg.Server.LogLevel != "debug" {
		t.Errorf("log-level = %q", cfg.Server.LogLevel)
	}
	if cfg.Server.TLS.CertFile != "/certs/cert.pem" {
		t.Errorf("cert-file = %q", cfg.Server.TLS.CertFile)
	}
	if cfg.Server.TLS.KeyFile != "/certs/key.pem" {
		t.Errorf("key-file = %q", cfg.Server.TLS.KeyFile)
	}
	if cfg.Server.Admin.Port != 9090 {
		t.Errorf("admin-port = %d", cfg.Server.Admin.Port)
	}
	if cfg.Server.Metrics.Port != 9200 {
		t.Errorf("metrics-port = %d", cfg.Server.Metrics.Port)
	}
	if !cfg.Server.Auth.Enabled {
		t.Error("auth should be enabled")
	}
	if cfg.Engine.MemtableSize != 33554432 {
		t.Errorf("memtable-size = %d", cfg.Engine.MemtableSize)
	}
	if cfg.Engine.WALSyncMode != "batch" {
		t.Errorf("wal-sync-mode = %q", cfg.Engine.WALSyncMode)
	}
	if !cfg.Replication.Enabled {
		t.Error("replication should be enabled")
	}
	if cfg.Replication.NodeID != 3 {
		t.Errorf("node-id = %d", cfg.Replication.NodeID)
	}
}

func TestLoadFromFilePreservesDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "minimal.conf")
	content := `
[server]
port = 30000
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	// Overridden value
	if cfg.Server.Port != 30000 {
		t.Errorf("port = %d, want 30000", cfg.Server.Port)
	}

	// Defaults should be preserved
	if cfg.Server.Bind != "0.0.0.0" {
		t.Errorf("bind = %q, want 0.0.0.0 (default)", cfg.Server.Bind)
	}
	if cfg.Server.DataDir != "./data" {
		t.Errorf("data-dir = %q, want ./data (default)", cfg.Server.DataDir)
	}
	if cfg.Server.LogLevel != "info" {
		t.Errorf("log-level = %q, want info (default)", cfg.Server.LogLevel)
	}
	if cfg.Server.Auth.Enabled {
		t.Error("auth should default to false")
	}
	if cfg.Engine.WALSyncMode != "full" {
		t.Errorf("wal-sync-mode = %q, want full (default)", cfg.Engine.WALSyncMode)
	}
}

func TestLoadFromFileNotFound(t *testing.T) {
	_, err := LoadFromFile("/nonexistent/path.conf")
	if err == nil {
		t.Error("should fail for missing file")
	}
}

func TestApplyFlags(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ApplyFlags(map[string]string{
		"port":     "29017",
		"bind":     "10.0.0.1",
		"data-dir": "/data",
		"log-level": "warn",
		"auth":     "true",
		"metrics-port": "9200",
		"admin-port": "9090",
	})

	if cfg.Server.Port != 29017 {
		t.Errorf("port = %d", cfg.Server.Port)
	}
	if cfg.Server.Bind != "10.0.0.1" {
		t.Errorf("bind = %q", cfg.Server.Bind)
	}
	if cfg.Server.DataDir != "/data" {
		t.Errorf("data-dir = %q", cfg.Server.DataDir)
	}
	if cfg.Server.LogLevel != "warn" {
		t.Errorf("log-level = %q", cfg.Server.LogLevel)
	}
	if !cfg.Server.Auth.Enabled {
		t.Error("auth should be enabled")
	}
	if cfg.Server.Metrics.Port != 9200 {
		t.Errorf("metrics-port = %d", cfg.Server.Metrics.Port)
	}
	if cfg.Server.Admin.Port != 9090 {
		t.Errorf("admin-port = %d", cfg.Server.Admin.Port)
	}
}

func TestApplyFlagsUnknownIgnored(t *testing.T) {
	cfg := DefaultConfig()
	orig := cfg.Server.Port
	cfg.ApplyFlags(map[string]string{
		"unknown-flag": "value",
	})
	if cfg.Server.Port != orig {
		t.Error("unknown flag should not affect config")
	}
}

func TestConfigMergingPriority(t *testing.T) {
	// defaults → config file → CLI flags
	dir := t.TempDir()
	path := filepath.Join(dir, "test.conf")
	content := `
[server]
port = 28017
bind = "192.168.1.1"
log-level = "debug"
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Config file values applied
	if cfg.Server.Port != 28017 {
		t.Errorf("port from file = %d", cfg.Server.Port)
	}
	if cfg.Server.Bind != "192.168.1.1" {
		t.Errorf("bind from file = %q", cfg.Server.Bind)
	}
	if cfg.Server.LogLevel != "debug" {
		t.Errorf("log-level from file = %q", cfg.Server.LogLevel)
	}

	// CLI flags override config file
	cfg.ApplyFlags(map[string]string{
		"port": "30000",
	})
	if cfg.Server.Port != 30000 {
		t.Errorf("port after flags = %d, want 30000", cfg.Server.Port)
	}
	// Non-overridden values stay from config file
	if cfg.Server.Bind != "192.168.1.1" {
		t.Errorf("bind should be preserved = %q", cfg.Server.Bind)
	}
	if cfg.Server.LogLevel != "debug" {
		t.Errorf("log-level should be preserved = %q", cfg.Server.LogLevel)
	}
}

func TestApplyEnv(t *testing.T) {
	cfg := DefaultConfig()

	t.Setenv("MAMMOTH_PORT", "29999")
	t.Setenv("MAMMOTH_BIND", "10.0.0.5")
	t.Setenv("MAMMOTH_DATA_DIR", "/env-data")
	t.Setenv("MAMMOTH_LOG_LEVEL", "error")
	t.Setenv("MAMMOTH_AUTH", "true")

	cfg.ApplyEnv()

	if cfg.Server.Port != 29999 {
		t.Errorf("port = %d", cfg.Server.Port)
	}
	if cfg.Server.Bind != "10.0.0.5" {
		t.Errorf("bind = %q", cfg.Server.Bind)
	}
	if cfg.Server.DataDir != "/env-data" {
		t.Errorf("data-dir = %q", cfg.Server.DataDir)
	}
	if cfg.Server.LogLevel != "error" {
		t.Errorf("log-level = %q", cfg.Server.LogLevel)
	}
	if !cfg.Server.Auth.Enabled {
		t.Error("auth should be enabled")
	}
}

func TestApplyEnvAuth1(t *testing.T) {
	cfg := DefaultConfig()
	t.Setenv("MAMMOTH_AUTH", "1")
	cfg.ApplyEnv()
	if !cfg.Server.Auth.Enabled {
		t.Error("MAMMOTH_AUTH=1 should enable auth")
	}
}
