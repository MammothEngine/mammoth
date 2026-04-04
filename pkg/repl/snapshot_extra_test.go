package repl

import (
	"testing"
)

// Test DecodeSnapshotData with invalid JSON
func TestDecodeSnapshotData_InvalidJSON(t *testing.T) {
	_, err := DecodeSnapshotData([]byte("not valid json"))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

// Test DecodeSnapshotData with empty data
func TestDecodeSnapshotData_Empty(t *testing.T) {
	_, err := DecodeSnapshotData([]byte{})
	if err == nil {
		t.Error("expected error for empty data")
	}
}

// Test DecodeSnapshotData with partial data
func TestDecodeSnapshotData_Partial(t *testing.T) {
	// JSON that starts but doesn't complete
	_, err := DecodeSnapshotData([]byte(`{"kv": {`))
	if err == nil {
		t.Error("expected error for partial JSON")
	}
}
