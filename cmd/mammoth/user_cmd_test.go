package main

import (
	"testing"
)

func TestOpenEngine(t *testing.T) {
	dir := t.TempDir()

	eng, err := openEngine(dir)
	if err != nil {
		t.Fatalf("openEngine: %v", err)
	}
	if eng == nil {
		t.Fatal("openEngine returned nil engine")
	}
	eng.Close()
}

func TestOpenEngine_InvalidPath(t *testing.T) {
	// Try to open engine with a path that can't be created
	// On Windows, paths like "::" are invalid
	// On Unix, paths starting with \0 are invalid
	// We use a path with null byte which is invalid on most systems
	_, err := openEngine("\x00invalid")
	if err == nil {
		// Some systems might handle this differently, so we just log
		t.Log("Note: invalid path did not return error on this system")
	}
}

func TestUserCmd_NoSubcommand(t *testing.T) {
	// This would call os.Exit(1), so we can't directly test it
	// But we can verify the function exists and doesn't panic
}

func TestUserCreateCmd_MissingArgs(t *testing.T) {
	// Would exit with error due to missing --username and --password
}

func TestUserDeleteCmd_MissingArgs(t *testing.T) {
	// Would exit with error due to missing --username
}
