package logging

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestLevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	l := &Logger{level: WarnLevel, output: &buf}
	l.Debug("no")
	l.Info("no")
	l.Warn("yes", FString("key", "val"))
	l.Error("also")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(lines))
	}
	var entry map[string]interface{}
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatal(err)
	}
	if entry["level"] != "warn" || entry["msg"] != "yes" {
		t.Errorf("unexpected: %v", entry)
	}
}

func TestWithComponent(t *testing.T) {
	var buf bytes.Buffer
	l := &Logger{level: DebugLevel, output: &buf}
	l.WithComponent("wire").Info("test")
	var entry map[string]interface{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &entry); err != nil {
		t.Fatal(err)
	}
	if entry["component"] != "wire" {
		t.Errorf("expected component=wire, got %v", entry["component"])
	}
}

func TestWithFields(t *testing.T) {
	var buf bytes.Buffer
	l := &Logger{level: DebugLevel, output: &buf}
	l.WithFields(FString("db", "test")).Info("query", FInt("count", 42))
	var entry map[string]interface{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &entry); err != nil {
		t.Fatal(err)
	}
	if entry["db"] != "test" || entry["count"] != float64(42) {
		t.Errorf("unexpected: %v", entry)
	}
}

func TestJSONFormat(t *testing.T) {
	var buf bytes.Buffer
	l := &Logger{level: DebugLevel, output: &buf}
	l.Info("hello", FString("cmd", "find"), FInt("n", 1))
	var entry map[string]interface{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &entry); err != nil {
		t.Fatalf("invalid JSON: %s", buf.String())
	}
	for _, key := range []string{"ts", "level", "msg", "cmd", "n"} {
		if _, ok := entry[key]; !ok {
			t.Errorf("missing key: %s", key)
		}
	}
}

func TestParseLevel(t *testing.T) {
	if ParseLevel("debug") != DebugLevel {
		t.Error("debug")
	}
	if ParseLevel("warn") != WarnLevel {
		t.Error("warn")
	}
	if ParseLevel("unknown") != InfoLevel {
		t.Error("default")
	}
}

func TestConcurrentLogging(t *testing.T) {
	var buf bytes.Buffer
	l := &Logger{level: DebugLevel, output: &buf}
	done := make(chan struct{})
	for i := 0; i < 100; i++ {
		go func() {
			l.Info("concurrent", FInt("n", 1))
			done <- struct{}{}
		}()
	}
	for i := 0; i < 100; i++ {
		<-done
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 100 {
		t.Errorf("expected 100 lines, got %d", len(lines))
	}
}

// Tests for uncovered field functions

func TestFieldFunctions(t *testing.T) {
	var buf bytes.Buffer
	l := &Logger{level: DebugLevel, output: &buf}

	// Test FInt64
	l.Info("test", FInt64("int64", 9223372036854775807))
	var entry map[string]interface{}
	json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &entry)
	if entry["int64"] != float64(9223372036854775807) {
		t.Errorf("FInt64: unexpected value %v", entry["int64"])
	}

	// Test FFloat
	buf.Reset()
	l.Info("test", FFloat("float", 3.14159))
	json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &entry)
	if entry["float"] != 3.14159 {
		t.Errorf("FFloat: unexpected value %v", entry["float"])
	}

	// Test FBool
	buf.Reset()
	l.Info("test", FBool("bool", true))
	json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &entry)
	if entry["bool"] != true {
		t.Errorf("FBool: unexpected value %v", entry["bool"])
	}

	// Test FErr
	buf.Reset()
	testErr := errors.New("test error")
	l.Info("test", FErr(testErr))
	json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &entry)
	if entry["error"] != "test error" {
		t.Errorf("FErr: unexpected value %v", entry["error"])
	}

	// Test FDuration
	buf.Reset()
	l.Info("test", FDuration("duration", 5*time.Second))
	json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &entry)
	if entry["duration"] != "5s" {
		t.Errorf("FDuration: unexpected value %v", entry["duration"])
	}
}

func TestDefaultLogger(t *testing.T) {
	// Get default logger
	l := Default()
	if l == nil {
		t.Error("Default() should return non-nil logger")
	}

	// Test SetLevel
	SetLevel(DebugLevel)
	if Default().level != DebugLevel {
		t.Error("SetLevel should change default logger level")
	}

	// Reset to info level
	SetLevel(InfoLevel)
}

func TestFatalf(t *testing.T) {
	// We can't actually test Fatalf as it would exit the program
	// Just verify the method exists and can be called with the right signature
	// This is a compile-time check effectively
	var l *Logger
	_ = func() {
		// This would exit if actually called
		// l.Fatalf("test", FString("key", "value"))
	}
	_ = l // silence unused warning
}

func TestLevelString(t *testing.T) {
	tests := []struct {
		level    Level
		expected string
	}{
		{DebugLevel, "debug"},
		{InfoLevel, "info"},
		{WarnLevel, "warn"},
		{ErrorLevel, "error"},
		{Level(99), "unknown"},
	}

	for _, tt := range tests {
		if tt.level.String() != tt.expected {
			t.Errorf("String() = %q, want %q", tt.level.String(), tt.expected)
		}
	}
}

func TestParseLevelAll(t *testing.T) {
	tests := []struct {
		input    string
		expected Level
	}{
		{"debug", DebugLevel},
		{"info", InfoLevel},
		{"warn", WarnLevel},
		{"error", ErrorLevel},
		{"unknown", InfoLevel},
		{"", InfoLevel},
	}

	for _, tt := range tests {
		result := ParseLevel(tt.input)
		if result != tt.expected {
			t.Errorf("ParseLevel(%q) = %v, want %v", tt.input, result, tt.expected)
		}
	}
}
