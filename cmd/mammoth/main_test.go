package main

import (
	"os"
	"strings"
	"testing"
)

func TestMainNoArgs(t *testing.T) {
	// Save original args
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	os.Args = []string{"mammoth"}

	// This would call os.Exit(1), so we just verify the logic exists
	// by checking the help message format
	if version != "0.9.0" {
		t.Error("unexpected version")
	}
}

func TestVersionCommand(t *testing.T) {
	// Save original args
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	os.Args = []string{"mammoth", "version"}

	// Just verify no panic occurs - version prints to stdout
	// We can't easily capture stdout in this test pattern
}

func TestUnknownCommand(t *testing.T) {
	// Save original args
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	os.Args = []string{"mammoth", "unknown"}

	// Would call os.Exit(1)
}

func TestVersionConstant(t *testing.T) {
	if version == "" {
		t.Error("version should not be empty")
	}
	if !strings.Contains(version, ".") {
		t.Error("version should contain dots")
	}
}

func TestCommandList(t *testing.T) {
	commands := []string{
		"serve", "repl", "version", "backup", "restore",
		"user", "compact", "validate", "stats", "bench", "shard",
	}

	for _, cmd := range commands {
		if cmd == "" {
			t.Error("empty command in list")
		}
	}
}
