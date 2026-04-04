package config

import (
	"testing"
)

// TestApplyFlagsMore tests additional flags not covered in main test
func TestApplyFlagsMore(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ApplyFlags(map[string]string{
		"tls-cert-file":          "/certs/cert.pem",
		"tls-key-file":           "/certs/key.pem",
		"slow-query-threshold":   "500ms",
		"audit-enabled":          "true",
		"audit-path":             "/var/log/audit.log",
		"encryption-enabled":     "true",
		"encryption-key-file":    "/keys/mammoth.key",
		"sharding-enabled":       "true",
		"sharding-config-server": "config1:27019",
	})

	if cfg.Server.TLS.CertFile != "/certs/cert.pem" {
		t.Errorf("tls-cert-file = %q", cfg.Server.TLS.CertFile)
	}
	if cfg.Server.TLS.KeyFile != "/certs/key.pem" {
		t.Errorf("tls-key-file = %q", cfg.Server.TLS.KeyFile)
	}
	if cfg.Server.SlowQueryThreshold != 500000000 { // 500ms in nanoseconds
		t.Errorf("slow-query-threshold = %v", cfg.Server.SlowQueryThreshold)
	}
	if !cfg.Server.Audit.Enabled {
		t.Error("audit should be enabled")
	}
	if cfg.Server.Audit.Path != "/var/log/audit.log" {
		t.Errorf("audit-path = %q", cfg.Server.Audit.Path)
	}
	if !cfg.Engine.Encryption.Enabled {
		t.Error("encryption should be enabled")
	}
	if cfg.Engine.Encryption.KeyFile != "/keys/mammoth.key" {
		t.Errorf("encryption-key-file = %q", cfg.Engine.Encryption.KeyFile)
	}
	if !cfg.Sharding.Enabled {
		t.Error("sharding should be enabled")
	}
	if cfg.Sharding.ConfigServer != "config1:27019" {
		t.Errorf("sharding-config-server = %q", cfg.Sharding.ConfigServer)
	}
}

// TestApplyFlagsInvalidInt tests flags with invalid integer values
func TestApplyFlagsInvalidInt(t *testing.T) {
	cfg := DefaultConfig()
	origPort := cfg.Server.Port

	// Invalid port should not change config
	cfg.ApplyFlags(map[string]string{
		"port": "invalid",
	})
	if cfg.Server.Port != origPort {
		t.Error("invalid port should not change config")
	}
}

// TestApplyFlagsHealthPort tests health-port flag
func TestApplyFlagsHealthPort(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ApplyFlags(map[string]string{
		"health-port": "8080",
	})
	if cfg.Server.Admin.Port != 8080 {
		t.Errorf("health-port = %d", cfg.Server.Admin.Port)
	}
}

// TestApplyEnvMore tests additional environment variables
func TestApplyEnvMore(t *testing.T) {
	cfg := DefaultConfig()

	t.Setenv("MAMMOTH_TLS_CERT_FILE", "/env/cert.pem")
	t.Setenv("MAMMOTH_TLS_KEY_FILE", "/env/key.pem")
	t.Setenv("MAMMOTH_AUDIT_ENABLED", "1")
	t.Setenv("MAMMOTH_AUDIT_PATH", "/env/audit.log")
	t.Setenv("MAMMOTH_QUERY_TIMEOUT", "30s")
	t.Setenv("MAMMOTH_ENCRYPTION_ENABLED", "1")
	t.Setenv("MAMMOTH_ENCRYPTION_KEY_FILE", "/env/key.file")
	t.Setenv("MAMMOTH_SHARDING_ENABLED", "true")
	t.Setenv("MAMMOTH_SHARDING_CONFIG_SERVER", "config.example.com:27019")
	t.Setenv("MAMMOTH_RATE_LIMIT_ENABLED", "true")
	t.Setenv("MAMMOTH_RATE_LIMIT_RPS", "1000.5")
	t.Setenv("MAMMOTH_RATE_LIMIT_BURST", "200")
	t.Setenv("MAMMOTH_RATE_LIMIT_PER_CONNECTION", "true")
	t.Setenv("MAMMOTH_RATE_LIMIT_GLOBAL_RATE", "10000.5")
	t.Setenv("MAMMOTH_RATE_LIMIT_GLOBAL_BURST", "500")
	t.Setenv("MAMMOTH_RATE_LIMIT_WAIT_TIMEOUT", "5s")
	t.Setenv("MAMMOTH_CIRCUIT_BREAKER_ENABLED", "true")
	t.Setenv("MAMMOTH_CIRCUIT_BREAKER_FAILURE_THRESHOLD", "10")
	t.Setenv("MAMMOTH_CIRCUIT_BREAKER_SUCCESS_THRESHOLD", "5")
	t.Setenv("MAMMOTH_CIRCUIT_BREAKER_TIMEOUT", "10s")
	t.Setenv("MAMMOTH_CIRCUIT_BREAKER_MAX_REQUESTS", "100")

	cfg.ApplyEnv()

	if cfg.Server.TLS.CertFile != "/env/cert.pem" {
		t.Errorf("MAMMOTH_TLS_CERT_FILE = %q", cfg.Server.TLS.CertFile)
	}
	if cfg.Server.TLS.KeyFile != "/env/key.pem" {
		t.Errorf("MAMMOTH_TLS_KEY_FILE = %q", cfg.Server.TLS.KeyFile)
	}
	if !cfg.Server.Audit.Enabled {
		t.Error("MAMMOTH_AUDIT_ENABLED should enable audit")
	}
	if cfg.Server.Audit.Path != "/env/audit.log" {
		t.Errorf("MAMMOTH_AUDIT_PATH = %q", cfg.Server.Audit.Path)
	}
	if cfg.Server.QueryTimeout != 30000000000 { // 30s in nanoseconds
		t.Errorf("MAMMOTH_QUERY_TIMEOUT = %v", cfg.Server.QueryTimeout)
	}
	if !cfg.Engine.Encryption.Enabled {
		t.Error("MAMMOTH_ENCRYPTION_ENABLED should enable encryption")
	}
	if cfg.Engine.Encryption.KeyFile != "/env/key.file" {
		t.Errorf("MAMMOTH_ENCRYPTION_KEY_FILE = %q", cfg.Engine.Encryption.KeyFile)
	}
	if !cfg.Sharding.Enabled {
		t.Error("MAMMOTH_SHARDING_ENABLED should enable sharding")
	}
	if cfg.Sharding.ConfigServer != "config.example.com:27019" {
		t.Errorf("MAMMOTH_SHARDING_CONFIG_SERVER = %q", cfg.Sharding.ConfigServer)
	}
	if !cfg.Server.RateLimit.Enabled {
		t.Error("MAMMOTH_RATE_LIMIT_ENABLED should enable rate limit")
	}
	if cfg.Server.RateLimit.RequestsPerSecond != 1000.5 {
		t.Errorf("MAMMOTH_RATE_LIMIT_RPS = %f", cfg.Server.RateLimit.RequestsPerSecond)
	}
	if cfg.Server.RateLimit.Burst != 200 {
		t.Errorf("MAMMOTH_RATE_LIMIT_BURST = %d", cfg.Server.RateLimit.Burst)
	}
	if !cfg.Server.RateLimit.PerConnection {
		t.Error("MAMMOTH_RATE_LIMIT_PER_CONNECTION should be true")
	}
	if cfg.Server.RateLimit.GlobalRate != 10000.5 {
		t.Errorf("MAMMOTH_RATE_LIMIT_GLOBAL_RATE = %f", cfg.Server.RateLimit.GlobalRate)
	}
	if cfg.Server.RateLimit.GlobalBurst != 500 {
		t.Errorf("MAMMOTH_RATE_LIMIT_GLOBAL_BURST = %d", cfg.Server.RateLimit.GlobalBurst)
	}
	if cfg.Server.RateLimit.WaitTimeout != 5000000000 { // 5s in nanoseconds
		t.Errorf("MAMMOTH_RATE_LIMIT_WAIT_TIMEOUT = %v", cfg.Server.RateLimit.WaitTimeout)
	}
	if !cfg.Server.CircuitBreaker.Enabled {
		t.Error("MAMMOTH_CIRCUIT_BREAKER_ENABLED should enable circuit breaker")
	}
	if cfg.Server.CircuitBreaker.FailureThreshold != 10 {
		t.Errorf("MAMMOTH_CIRCUIT_BREAKER_FAILURE_THRESHOLD = %d", cfg.Server.CircuitBreaker.FailureThreshold)
	}
	if cfg.Server.CircuitBreaker.SuccessThreshold != 5 {
		t.Errorf("MAMMOTH_CIRCUIT_BREAKER_SUCCESS_THRESHOLD = %d", cfg.Server.CircuitBreaker.SuccessThreshold)
	}
	if cfg.Server.CircuitBreaker.Timeout != 10000000000 { // 10s in nanoseconds
		t.Errorf("MAMMOTH_CIRCUIT_BREAKER_TIMEOUT = %v", cfg.Server.CircuitBreaker.Timeout)
	}
	if cfg.Server.CircuitBreaker.MaxRequests != 100 {
		t.Errorf("MAMMOTH_CIRCUIT_BREAKER_MAX_REQUESTS = %d", cfg.Server.CircuitBreaker.MaxRequests)
	}
}

// TestApplyEnvInvalidValues tests environment variables with invalid values
func TestApplyEnvInvalidValues(t *testing.T) {
	cfg := DefaultConfig()
	origPort := cfg.Server.Port

	t.Setenv("MAMMOTH_PORT", "invalid")
	t.Setenv("MAMMOTH_RATE_LIMIT_RPS", "not-a-number")
	t.Setenv("MAMMOTH_RATE_LIMIT_BURST", "not-an-int")
	t.Setenv("MAMMOTH_QUERY_TIMEOUT", "invalid-duration")

	cfg.ApplyEnv()

	// Config should remain unchanged for invalid values
	if cfg.Server.Port != origPort {
		t.Error("invalid MAMMOTH_PORT should not change config")
	}
}

// TestValidateAdminPort tests admin port validation
func TestValidateAdminPort(t *testing.T) {
	cfg := DefaultConfig()

	cfg.Server.Admin.Port = -1
	if err := cfg.Validate(); err == nil {
		t.Error("negative admin port should fail")
	}

	cfg.Server.Admin.Port = 70000
	if err := cfg.Validate(); err == nil {
		t.Error("admin port > 65535 should fail")
	}

	cfg.Server.Admin.Port = 0
	if err := cfg.Validate(); err != nil {
		t.Errorf("admin port 0 should pass: %v", err)
	}
}

// TestValidateMetricsPort tests metrics port validation
func TestValidateMetricsPort(t *testing.T) {
	cfg := DefaultConfig()

	cfg.Server.Metrics.Port = -1
	if err := cfg.Validate(); err == nil {
		t.Error("negative metrics port should fail")
	}

	cfg.Server.Metrics.Port = 70000
	if err := cfg.Validate(); err == nil {
		t.Error("metrics port > 65535 should fail")
	}

	cfg.Server.Metrics.Port = 0
	if err := cfg.Validate(); err != nil {
		t.Errorf("metrics port 0 should pass: %v", err)
	}
}

// TestValidateAuditPath tests audit path validation
func TestValidateAuditPath(t *testing.T) {
	cfg := DefaultConfig()

	cfg.Server.Audit.Enabled = true
	cfg.Server.Audit.Path = ""
	if err := cfg.Validate(); err == nil {
		t.Error("enabled audit with empty path should fail")
	}

	cfg.Server.Audit.Path = "/var/log/audit.log"
	if err := cfg.Validate(); err != nil {
		t.Errorf("enabled audit with path should pass: %v", err)
	}
}

// TestValidateEncryptionKey tests encryption key validation
func TestValidateEncryptionKey(t *testing.T) {
	cfg := DefaultConfig()

	cfg.Engine.Encryption.Enabled = true
	cfg.Engine.Encryption.KeyFile = ""
	cfg.Engine.Encryption.KeyEnv = ""
	if err := cfg.Validate(); err == nil {
		t.Error("enabled encryption without key should fail")
	}

	cfg.Engine.Encryption.KeyFile = "/keys/mammoth.key"
	if err := cfg.Validate(); err != nil {
		t.Errorf("enabled encryption with key-file should pass: %v", err)
	}

	cfg.Engine.Encryption.KeyFile = ""
	cfg.Engine.Encryption.KeyEnv = "MAMMOTH_ENCRYPTION_KEY"
	if err := cfg.Validate(); err != nil {
		t.Errorf("enabled encryption with key-env should pass: %v", err)
	}
}

// TestValidateShardingConfigServer tests sharding config server validation
func TestValidateShardingConfigServer(t *testing.T) {
	cfg := DefaultConfig()

	cfg.Sharding.Enabled = true
	cfg.Sharding.ConfigServer = ""
	if err := cfg.Validate(); err == nil {
		t.Error("enabled sharding without config server should fail")
	}

	cfg.Sharding.ConfigServer = "config.example.com:27019"
	if err := cfg.Validate(); err != nil {
		t.Errorf("enabled sharding with config server should pass: %v", err)
	}
}

// TestParseInt64 tests parseInt64 function
func TestParseInt64(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
		wantErr  bool
	}{
		{"123", 123, false},
		{"0", 0, false},
		{"-456", -456, false},
		{"", 0, true},
		{"abc", 0, true},
		{"12.34", 12, false}, // fmt.Sscanf parses the integer part
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			n, err := parseInt64(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseInt64(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if n != tt.expected {
				t.Errorf("parseInt64(%q) = %d, want %d", tt.input, n, tt.expected)
			}
		})
	}
}
