package audit

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestLogWritesValidJSONLines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.log")

	l, err := NewAuditLogger(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	entry := AuditEntry{
		Timestamp:  time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC),
		Operation:  "find",
		Database:   "testdb",
		Collection: "users",
		User:       "admin",
		RemoteAddr: "127.0.0.1",
		Duration:   "10ms",
	}
	if err := l.Log(entry); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	var got AuditEntry
	scanner := bufio.NewScanner(f)
	if !scanner.Scan() {
		t.Fatal("expected one line")
	}
	line := scanner.Text()
	if err := json.Unmarshal([]byte(line), &got); err != nil {
		t.Fatalf("invalid JSON: %s, err: %v", line, err)
	}
	if got.Operation != "find" {
		t.Errorf("op = %q, want %q", got.Operation, "find")
	}
	if got.Database != "testdb" {
		t.Errorf("db = %q, want %q", got.Database, "testdb")
	}
	if got.Collection != "users" {
		t.Errorf("coll = %q, want %q", got.Collection, "users")
	}
	if got.Duration != "10ms" {
		t.Errorf("duration = %q, want %q", got.Duration, "10ms")
	}
	if scanner.Scan() {
		t.Errorf("unexpected extra lines: %s", scanner.Text())
	}
}

func TestDisabledLoggerNoFile(t *testing.T) {
	l, err := NewAuditLogger("")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	if l.Enabled() {
		t.Error("disabled logger should not report enabled")
	}

	entry := AuditEntry{
		Timestamp: time.Now(),
		Operation: "insert",
		Database:  "testdb",
	}
	if err := l.Log(entry); err != nil {
		t.Errorf("disabled logger Log returned error: %v", err)
	}
}

func TestNilReceiverNoPanic(t *testing.T) {
	var l *AuditLogger
	if l.Enabled() {
		t.Error("nil logger should not be enabled")
	}
	if err := l.Log(AuditEntry{Operation: "test"}); err != nil {
		t.Errorf("nil logger Log returned error: %v", err)
	}
	l.LogOperation("insert", "db", "coll", time.Second)
	if err := l.Close(); err != nil {
		t.Errorf("nil logger Close returned error: %v", err)
	}
}

func TestConcurrentLog(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.log")

	l, err := NewAuditLogger(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			l.Log(AuditEntry{
				Timestamp: time.Now(),
				Operation: "write",
				Database:  "bench",
			})
		}()
	}
	wg.Wait()

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	count := 0
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		var entry AuditEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("invalid JSON on line %d: %v", count+1, err)
		}
		if entry.Operation != "write" {
			t.Errorf("op = %q, want %q", entry.Operation, "write")
		}
		count++
	}
	if count != n {
		t.Errorf("got %d lines, want %d", count, n)
	}
}

func TestLogOperation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.log")

	l, err := NewAuditLogger(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	before := time.Now()
	l.LogOperation("delete", "mydb", "orders", 25*time.Millisecond)
	after := time.Now()

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	var got AuditEntry
	scanner := bufio.NewScanner(f)
	if !scanner.Scan() {
		t.Fatal("expected one line")
	}
	if err := json.Unmarshal([]byte(scanner.Text()), &got); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	if got.Operation != "delete" {
		t.Errorf("op = %q, want %q", got.Operation, "delete")
	}
	if got.Database != "mydb" {
		t.Errorf("db = %q, want %q", got.Database, "mydb")
	}
	if got.Collection != "orders" {
		t.Errorf("coll = %q, want %q", got.Collection, "orders")
	}
	if got.Duration != "25ms" {
		t.Errorf("duration = %q, want %q", got.Duration, "25ms")
	}
	if got.Timestamp.Before(before) || got.Timestamp.After(after) {
		t.Errorf("timestamp %v not in [%v, %v]", got.Timestamp, before, after)
	}
}

func TestMultipleEntries(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.log")

	l, err := NewAuditLogger(path)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		l.LogOperation("op", "db", "", 0)
	}
	l.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Verify we can parse each line back.
	lines := splitNonEmpty(string(data))
	if len(lines) != 5 {
		t.Fatalf("expected 5 lines, got %d", len(lines))
	}
	for i, line := range lines {
		var entry AuditEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("line %d: invalid JSON: %v", i+1, err)
		}
	}
}

// splitNonEmpty splits s by newline and drops empty strings.
func splitNonEmpty(s string) []string {
	var result []string
	for _, line := range splitLines(s) {
		if line != "" {
			result = append(result, line)
		}
	}
	return result
}

// splitLines splits on \n.
func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}
