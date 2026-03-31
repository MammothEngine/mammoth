package config

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// ServerConfig holds server-related configuration.
type ServerConfig struct {
	Port              int           `json:"port"`
	Bind              string        `json:"bind"`
	DataDir           string        `json:"dataDir"`
	LogLevel          string        `json:"logLevel"`
	TLS               TLSConfig     `json:"tls"`
	Admin             AdminConfig   `json:"admin"`
	Metrics           MetricsConfig `json:"metrics"`
	Auth              AuthConfig    `json:"auth"`
	Audit             AuditConfig   `json:"audit"`
	SlowQueryThreshold time.Duration `json:"slowQueryThreshold"`
}

// TLSConfig holds TLS configuration.
type TLSConfig struct {
	CertFile string `json:"certFile"`
	KeyFile  string `json:"keyFile"`
}

// AdminConfig holds admin UI configuration.
type AdminConfig struct {
	Port int `json:"port"`
}

// MetricsConfig holds metrics configuration.
type MetricsConfig struct {
	Port int `json:"port"`
}

// AuthConfig holds authentication configuration.
type AuthConfig struct {
	Enabled bool `json:"enabled"`
}

// AuditConfig holds audit logging configuration.
type AuditConfig struct {
	Enabled bool   `json:"enabled"`
	Path    string `json:"path"`
}

// EncryptionConfig holds encryption-at-rest configuration.
type EncryptionConfig struct {
	Enabled  bool   `json:"enabled"`
	KeyFile  string `json:"keyFile"`
	KeyEnv   string `json:"keyEnv"`
}

// EngineConfig holds storage engine configuration.
type EngineConfig struct {
	MemtableSize     int           `json:"memtableSize"`
	CacheSize        int           `json:"cacheSize"`
	CompactionThreads int          `json:"compactionThreads"`
	WALSyncMode      string        `json:"walSyncMode"`
	Encryption       EncryptionConfig `json:"encryption"`
	SlowQueryThreshold time.Duration `json:"slowQueryThreshold"`
}

// ReplConfig holds replication configuration.
type ReplConfig struct {
	Enabled bool     `json:"enabled"`
	NodeID  int      `json:"nodeId"`
	Peers   []string `json:"peers"`
}

// ShardingConfig holds sharding configuration.
type ShardingConfig struct {
	Enabled      bool     `json:"enabled"`
	ConfigServer string   `json:"configServer"`
	Shards       []string `json:"shards"`
	AutoSplit    bool     `json:"autoSplit"`
	BalancerOn   bool     `json:"balancerOn"`
}

// Config is the top-level configuration.
type Config struct {
	Server     ServerConfig `json:"server"`
	Engine     EngineConfig `json:"engine"`
	Replication ReplConfig  `json:"replication"`
	Sharding    ShardingConfig `json:"sharding"`
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Port:               27017,
			Bind:               "0.0.0.0",
			DataDir:            "./data",
			LogLevel:           "info",
			TLS:                TLSConfig{},
			Admin:              AdminConfig{Port: 8080},
			Metrics:            MetricsConfig{Port: 9100},
			Auth:               AuthConfig{},
			Audit:              AuditConfig{Path: "./audit.log"},
			SlowQueryThreshold: 100 * time.Millisecond,
		},
		Engine: EngineConfig{
			MemtableSize:     64 * 1024 * 1024, // 64MB
			CacheSize:        128 * 1024 * 1024, // 128MB
			CompactionThreads: 2,
			WALSyncMode:      "full",
			Encryption:       EncryptionConfig{},
			SlowQueryThreshold: 100 * time.Millisecond,
		},
		Replication: ReplConfig{},
		Sharding:    ShardingConfig{AutoSplit: true, BalancerOn: true},
	}
}

// LoadFromFile loads and parses a TOML config file.
func LoadFromFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	toml, err := ParseTOML(string(data))
	if err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}
	cfg := DefaultConfig()
	cfg.applyTOML(toml)
	return cfg, nil
}

func (c *Config) applyTOML(t TOMLConfig) {
	// Server
	c.Server.Port = t.GetInt("server.port", c.Server.Port)
	if v := t.GetString("server.bind"); v != "" {
		c.Server.Bind = v
	}
	if v := t.GetString("server.data-dir"); v != "" {
		c.Server.DataDir = v
	}
	if v := t.GetString("server.log-level"); v != "" {
		c.Server.LogLevel = v
	}
	if v := t.GetString("server.tls.cert-file"); v != "" {
		c.Server.TLS.CertFile = v
	}
	if v := t.GetString("server.tls.key-file"); v != "" {
		c.Server.TLS.KeyFile = v
	}
	c.Server.Admin.Port = t.GetInt("server.admin.port", c.Server.Admin.Port)
	c.Server.Metrics.Port = t.GetInt("server.metrics.port", c.Server.Metrics.Port)
	c.Server.Auth.Enabled = t.GetBool("server.auth.enabled", c.Server.Auth.Enabled)
	c.Server.Audit.Enabled = t.GetBool("server.audit.enabled", c.Server.Audit.Enabled)
	if v := t.GetString("server.audit.path"); v != "" {
		c.Server.Audit.Path = v
	}
	c.Server.SlowQueryThreshold = t.GetDuration("server.slow-query-threshold", c.Server.SlowQueryThreshold)

	// Engine
	c.Engine.MemtableSize = t.GetInt("engine.memtable-size", c.Engine.MemtableSize)
	c.Engine.CacheSize = t.GetInt("engine.cache-size", c.Engine.CacheSize)
	c.Engine.CompactionThreads = t.GetInt("engine.compaction-threads", c.Engine.CompactionThreads)
	if v := t.GetString("engine.wal-sync-mode"); v != "" {
		c.Engine.WALSyncMode = v
	}
	c.Engine.SlowQueryThreshold = t.GetDuration("engine.slow-query-threshold", c.Engine.SlowQueryThreshold)
	c.Engine.Encryption.Enabled = t.GetBool("engine.encryption.enabled", c.Engine.Encryption.Enabled)
	if v := t.GetString("engine.encryption.key-file"); v != "" {
		c.Engine.Encryption.KeyFile = v
	}
	if v := t.GetString("engine.encryption.key-env"); v != "" {
		c.Engine.Encryption.KeyEnv = v
	}

	// Replication
	c.Replication.Enabled = t.GetBool("replication.enabled", c.Replication.Enabled)
	c.Replication.NodeID = t.GetInt("replication.node-id", c.Replication.NodeID)

	// Sharding
	c.Sharding.Enabled = t.GetBool("sharding.enabled", c.Sharding.Enabled)
	if v := t.GetString("sharding.config-server"); v != "" {
		c.Sharding.ConfigServer = v
	}
	c.Sharding.AutoSplit = t.GetBool("sharding.auto-split", c.Sharding.AutoSplit)
	c.Sharding.BalancerOn = t.GetBool("sharding.balancer-on", c.Sharding.BalancerOn)
}

// ApplyFlags overrides config values from CLI flags.
// Flag names use dashes (e.g. "data-dir" → ServerConfig.DataDir).
func (c *Config) ApplyFlags(flags map[string]string) {
	if v, ok := flags["port"]; ok {
		if n, err := parseInt(v); err == nil {
			c.Server.Port = n
		}
	}
	if v, ok := flags["bind"]; ok {
		c.Server.Bind = v
	}
	if v, ok := flags["data-dir"]; ok {
		c.Server.DataDir = v
	}
	if v, ok := flags["log-level"]; ok {
		c.Server.LogLevel = v
	}
	if v, ok := flags["tls-cert-file"]; ok {
		c.Server.TLS.CertFile = v
	}
	if v, ok := flags["tls-key-file"]; ok {
		c.Server.TLS.KeyFile = v
	}
	if v, ok := flags["metrics-port"]; ok {
		if n, err := parseInt(v); err == nil {
			c.Server.Metrics.Port = n
		}
	}
	if v, ok := flags["health-port"]; ok {
		if n, err := parseInt(v); err == nil {
			c.Server.Admin.Port = n
		}
	}
	if v, ok := flags["auth"]; ok {
		c.Server.Auth.Enabled = v == "true"
	}
	if v, ok := flags["slow-query-threshold"]; ok {
		if d, err := time.ParseDuration(v); err == nil {
			c.Server.SlowQueryThreshold = d
		}
	}
	if v, ok := flags["admin-port"]; ok {
		if n, err := parseInt(v); err == nil {
			c.Server.Admin.Port = n
		}
	}
	if v, ok := flags["audit-enabled"]; ok {
		c.Server.Audit.Enabled = v == "true"
	}
	if v, ok := flags["audit-path"]; ok {
		c.Server.Audit.Path = v
	}
	if v, ok := flags["encryption-enabled"]; ok {
		c.Engine.Encryption.Enabled = v == "true"
	}
	if v, ok := flags["encryption-key-file"]; ok {
		c.Engine.Encryption.KeyFile = v
	}
	if v, ok := flags["sharding-enabled"]; ok {
		c.Sharding.Enabled = v == "true"
	}
	if v, ok := flags["sharding-config-server"]; ok {
		c.Sharding.ConfigServer = v
	}
}

// ApplyEnv overrides config values from environment variables.
// Env keys: MAMMOTH_PORT, MAMMOTH_DATA_DIR, MAMMOTH_LOG_LEVEL, etc.
func (c *Config) ApplyEnv() {
	env := func(key string) string { return os.Getenv(key) }
	if v := env("MAMMOTH_PORT"); v != "" {
		if n, err := parseInt(v); err == nil {
			c.Server.Port = n
		}
	}
	if v := env("MAMMOTH_BIND"); v != "" {
		c.Server.Bind = v
	}
	if v := env("MAMMOTH_DATA_DIR"); v != "" {
		c.Server.DataDir = v
	}
	if v := env("MAMMOTH_LOG_LEVEL"); v != "" {
		c.Server.LogLevel = v
	}
	if v := env("MAMMOTH_TLS_CERT_FILE"); v != "" {
		c.Server.TLS.CertFile = v
	}
	if v := env("MAMMOTH_TLS_KEY_FILE"); v != "" {
		c.Server.TLS.KeyFile = v
	}
	if v := env("MAMMOTH_AUTH"); v != "" {
		c.Server.Auth.Enabled = strings.EqualFold(v, "true") || v == "1"
	}
	if v := env("MAMMOTH_AUDIT_ENABLED"); v != "" {
		c.Server.Audit.Enabled = strings.EqualFold(v, "true") || v == "1"
	}
	if v := env("MAMMOTH_AUDIT_PATH"); v != "" {
		c.Server.Audit.Path = v
	}
	if v := env("MAMMOTH_ENCRYPTION_ENABLED"); v != "" {
		c.Engine.Encryption.Enabled = strings.EqualFold(v, "true") || v == "1"
	}
	if v := env("MAMMOTH_ENCRYPTION_KEY_FILE"); v != "" {
		c.Engine.Encryption.KeyFile = v
	}
	if v := env("MAMMOTH_SHARDING_ENABLED"); v != "" {
		c.Sharding.Enabled = strings.EqualFold(v, "true") || v == "1"
	}
	if v := env("MAMMOTH_SHARDING_CONFIG_SERVER"); v != "" {
		c.Sharding.ConfigServer = v
	}
}

// Validate checks the config for errors.
func (c *Config) Validate() error {
	if c.Server.Port < 1 || c.Server.Port > 65535 {
		return fmt.Errorf("config: server.port must be 1-65535, got %d", c.Server.Port)
	}
	if c.Server.DataDir == "" {
		return fmt.Errorf("config: server.data-dir is required")
	}
	validLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if !validLevels[c.Server.LogLevel] {
		return fmt.Errorf("config: server.log-level must be debug|info|warn|error, got %q", c.Server.LogLevel)
	}
	if (c.Server.TLS.CertFile != "") != (c.Server.TLS.KeyFile != "") {
		return fmt.Errorf("config: server.tls.cert-file and server.tls.key-file must both be set or both empty")
	}
	if c.Server.Admin.Port < 0 || c.Server.Admin.Port > 65535 {
		return fmt.Errorf("config: server.admin.port must be 0-65535")
	}
	if c.Server.Metrics.Port < 0 || c.Server.Metrics.Port > 65535 {
		return fmt.Errorf("config: server.metrics.port must be 0-65535")
	}
	if c.Server.Audit.Enabled && c.Server.Audit.Path == "" {
		return fmt.Errorf("config: server.audit.path is required when audit is enabled")
	}
	if c.Engine.Encryption.Enabled && c.Engine.Encryption.KeyFile == "" && c.Engine.Encryption.KeyEnv == "" {
		return fmt.Errorf("config: engine.encryption.key-file or engine.encryption.key-env is required when encryption is enabled")
	}
	if c.Sharding.Enabled && c.Sharding.ConfigServer == "" {
		return fmt.Errorf("config: sharding.config-server is required when sharding is enabled")
	}
	return nil
}

func parseInt(s string) (int, error) {
	n, err := parseInt64(s)
	return int(n), err
}

func parseInt64(s string) (int64, error) {
	var n int64
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}
