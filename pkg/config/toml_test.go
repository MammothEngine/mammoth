package config

import (
	"testing"
	"time"
)

func TestParseTOMLBasic(t *testing.T) {
	input := `
# Server config
[server]
port = 27017
bind = "0.0.0.0"
data-dir = "./data"
log-level = "info"
`
	cfg, err := ParseTOML(input)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.GetInt("server.port", 0) != 27017 {
		t.Errorf("server.port = %d, want 27017", cfg.GetInt("server.port", 0))
	}
	if cfg.GetString("server.bind") != "0.0.0.0" {
		t.Errorf("server.bind = %q, want 0.0.0.0", cfg.GetString("server.bind"))
	}
	if cfg.GetString("server.data-dir") != "./data" {
		t.Errorf("data-dir = %q", cfg.GetString("server.data-dir"))
	}
}

func TestParseTOMLNested(t *testing.T) {
	input := `
[server.tls]
cert-file = "/certs/cert.pem"
key-file = "/certs/key.pem"

[server.auth]
enabled = true
`
	cfg, err := ParseTOML(input)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.GetString("server.tls.cert-file") != "/certs/cert.pem" {
		t.Errorf("cert-file = %q", cfg.GetString("server.tls.cert-file"))
	}
	if !cfg.GetBool("server.auth.enabled", false) {
		t.Error("server.auth.enabled should be true")
	}
}

func TestParseTOMLTypes(t *testing.T) {
	input := `
[engine]
memtable-size = 67108864
cache-size = 134217728
compaction-threads = 2
wal-sync-mode = "full"
slow-query-threshold = "100ms"
`
	cfg, err := ParseTOML(input)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.GetInt("engine.memtable-size", 0) != 67108864 {
		t.Errorf("memtable-size = %d", cfg.GetInt("engine.memtable-size", 0))
	}
	if cfg.GetString("engine.wal-sync-mode") != "full" {
		t.Errorf("wal-sync-mode = %q", cfg.GetString("engine.wal-sync-mode"))
	}
	if cfg.GetDuration("engine.slow-query-threshold", 0) != 100*time.Millisecond {
		t.Errorf("slow-query-threshold = %v", cfg.GetDuration("engine.slow-query-threshold", 0))
	}
}

func TestParseTOMLEscapes(t *testing.T) {
	input := `path = "C:\\Users\\test\\data"
msg = "hello \"world\""
newline = "line1\nline2"
`
	cfg, err := ParseTOML(input)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.GetString("path") != `C:\Users\test\data` {
		t.Errorf("path = %q", cfg.GetString("path"))
	}
	if cfg.GetString("msg") != `hello "world"` {
		t.Errorf("msg = %q", cfg.GetString("msg"))
	}
	if cfg.GetString("newline") != "line1\nline2" {
		t.Errorf("newline = %q", cfg.GetString("newline"))
	}
}

func TestParseTOMLDefaults(t *testing.T) {
	cfg := make(TOMLConfig)
	if cfg.GetInt("missing", 42) != 42 {
		t.Errorf("default int")
	}
	if cfg.GetBool("missing", true) != true {
		t.Errorf("default bool")
	}
	if cfg.GetString("missing") != "" {
		t.Errorf("default string")
	}
	if cfg.GetDuration("missing", 5*time.Second) != 5*time.Second {
		t.Errorf("default duration")
	}
}

func TestParseTOMLComments(t *testing.T) {
	input := `
# Full line comment
port = 27017  # inline comment
bind = "0.0.0.0"
`
	cfg, err := ParseTOML(input)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.GetInt("port", 0) != 27017 {
		t.Errorf("port = %d", cfg.GetInt("port", 0))
	}
}
