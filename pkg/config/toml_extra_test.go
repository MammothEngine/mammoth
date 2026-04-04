package config

import (
	"testing"
	"time"
)

// TestGetFloat tests the GetFloat method
func TestGetFloat(t *testing.T) {
	input := `
[server.rate-limit]
requests-per-second = 1000.5
burst = 100
`
	cfg, err := ParseTOML(input)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Test existing float value
	if cfg.GetFloat("server.rate-limit.requests-per-second", 0) != 1000.5 {
		t.Errorf("requests-per-second = %f, want 1000.5", cfg.GetFloat("server.rate-limit.requests-per-second", 0))
	}

	// Test default value when key doesn't exist
	if cfg.GetFloat("missing.key", 42.5) != 42.5 {
		t.Errorf("default float = %f, want 42.5", cfg.GetFloat("missing.key", 42.5))
	}

	// Test converting int to float
	if cfg.GetFloat("server.rate-limit.burst", 0) != 100.0 {
		t.Errorf("burst as float = %f, want 100.0", cfg.GetFloat("server.rate-limit.burst", 0))
	}
}

// TestParseTOMLFloatValue tests parsing float values
func TestParseTOMLFloatValue(t *testing.T) {
	input := `
rate = 123.456
negative = -78.9
 scientific = 1e10
`
	cfg, err := ParseTOML(input)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if cfg.GetFloat("rate", 0) != 123.456 {
		t.Errorf("rate = %f", cfg.GetFloat("rate", 0))
	}
	if cfg.GetFloat("negative", 0) != -78.9 {
		t.Errorf("negative = %f", cfg.GetFloat("negative", 0))
	}
}

// TestParseTOMLInvalid tests invalid TOML parsing
func TestParseTOMLInvalid(t *testing.T) {
	// Missing closing bracket
	input := `[server
port = 27017`
	_, err := ParseTOML(input)
	if err == nil {
		t.Error("should fail for missing closing bracket")
	}
}

// TestParseTOMLDurationValues tests duration parsing
func TestParseTOMLDurationValues(t *testing.T) {
	tests := []struct {
		input    string
		expected time.Duration
	}{
		{`timeout = "100ms"`, 100 * time.Millisecond},
		{`timeout = "5s"`, 5 * time.Second},
		{`timeout = "1m"`, 1 * time.Minute},
		{`timeout = "1h"`, 1 * time.Hour},
		{`timeout = "1h30m"`, 90 * time.Minute},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			cfg, err := ParseTOML(tt.input)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if cfg.GetDuration("timeout", 0) != tt.expected {
				t.Errorf("timeout = %v, want %v", cfg.GetDuration("timeout", 0), tt.expected)
			}
		})
	}
}

// TestParseTOMLBooleanValues tests boolean parsing
func TestParseTOMLBooleanValues(t *testing.T) {
	input := `
enabled = true
disabled = false
`
	cfg, err := ParseTOML(input)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if !cfg.GetBool("enabled", false) {
		t.Error("enabled should be true")
	}
	if cfg.GetBool("disabled", true) {
		t.Error("disabled should be false")
	}
}

// TestParseTOMLEmptyString tests empty string value
func TestParseTOMLEmptyString(t *testing.T) {
	input := `empty = ""`
	cfg, err := ParseTOML(input)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if cfg.GetString("empty") != "" {
		t.Errorf("empty = %q, want empty string", cfg.GetString("empty"))
	}
}

// TestParseTOMLUnescapeString tests string unescaping
func TestParseTOMLUnescapeString(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{`path = "C:\\Users\\test"`, `C:\Users\test`},
		{`msg = "hello \"world\""`, `hello "world"`},
		{`tab = "col1\tcol2"`, "col1\tcol2"},
		{`newline = "line1\nline2"`, "line1\nline2"},
		{`carriage = "line1\rline2"`, "line1\rline2"},
		{`backslash = "path\\\\file"`, `path\\file`},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			cfg, err := ParseTOML(tt.input)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			// Extract key name from input
			var key string
			switch {
			case len(tt.input) > 5 && tt.input[:5] == "path ":
				key = "path"
			case len(tt.input) > 4 && tt.input[:4] == "msg ":
				key = "msg"
			case len(tt.input) > 4 && tt.input[:4] == "tab ":
				key = "tab"
			case len(tt.input) > 8 && tt.input[:8] == "newline ":
				key = "newline"
			case len(tt.input) > 10 && tt.input[:10] == "carriage ":
				key = "carriage"
			case len(tt.input) > 10 && tt.input[:10] == "backslash ":
				key = "backslash"
			}
			if key != "" {
				got := cfg.GetString(key)
				if got != tt.expected {
					t.Errorf("%s = %q, want %q", key, got, tt.expected)
				}
			}
		})
	}
}

// TestParseTOMLIntegerValues tests integer parsing
func TestParseTOMLIntegerValues(t *testing.T) {
	input := `
positive = 42
zero = 0
negative = -17
large = 9999999999
`
	cfg, err := ParseTOML(input)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if cfg.GetInt("positive", 0) != 42 {
		t.Errorf("positive = %d", cfg.GetInt("positive", 0))
	}
	if cfg.GetInt("zero", -1) != 0 {
		t.Errorf("zero = %d", cfg.GetInt("zero", -1))
	}
	if cfg.GetInt("negative", 0) != -17 {
		t.Errorf("negative = %d", cfg.GetInt("negative", 0))
	}
	if cfg.GetInt("large", 0) != 9999999999 {
		t.Errorf("large = %d", cfg.GetInt("large", 0))
	}
}

// TestParseTOMLBareString tests bare string values
func TestParseTOMLBareString(t *testing.T) {
	input := `
bare = simple_value
with_underscore = test_value_123
`
	cfg, err := ParseTOML(input)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if cfg.GetString("bare") != "simple_value" {
		t.Errorf("bare = %q", cfg.GetString("bare"))
	}
	if cfg.GetString("with_underscore") != "test_value_123" {
		t.Errorf("with_underscore = %q", cfg.GetString("with_underscore"))
	}
}

// TestParseTOMLWhitespace tests various whitespace handling
func TestParseTOMLWhitespace(t *testing.T) {
	input := `
	port = 27017
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
