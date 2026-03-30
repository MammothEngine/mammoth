package logging

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestLogLevelFiltering(t *testing.T) {
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
