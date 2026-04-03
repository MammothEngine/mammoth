package wire

import (
	"testing"
	"time"
)

func TestSlowQueryProfiler(t *testing.T) {
	p := NewSlowQueryProfiler(100 * time.Millisecond)
	p.Record("find", "test", 50*time.Millisecond)
	if len(p.Entries()) != 0 {
		t.Error("should not record fast query")
	}

	p.Record("find", "test", 200*time.Millisecond)
	p.Record("insert", "test", 150*time.Millisecond)
	entries := p.Entries()
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
}

func TestSlowQueryProfilerRingBuffer(t *testing.T) {
	p := NewSlowQueryProfiler(0) // record everything
	for i := 0; i < 150; i++ {
		p.Record("find", "test", time.Millisecond)
	}
	entries := p.Entries()
	if len(entries) != 100 {
		t.Errorf("expected 100 entries (ring capacity), got %d", len(entries))
	}
}

// Test SlowQueryProfiler Threshold
func TestSlowQueryProfiler_Threshold(t *testing.T) {
	p := NewSlowQueryProfiler(100 * time.Millisecond)

	// Test Threshold returns the correct value
	if p.Threshold() != 100*time.Millisecond {
		t.Errorf("expected threshold 100ms, got %v", p.Threshold())
	}

	// Test with different threshold
	p2 := NewSlowQueryProfiler(500 * time.Millisecond)
	if p2.Threshold() != 500*time.Millisecond {
		t.Errorf("expected threshold 500ms, got %v", p2.Threshold())
	}
}
