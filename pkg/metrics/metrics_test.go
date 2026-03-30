package metrics

import (
	"bytes"
	"strings"
	"testing"
)

func TestCounter(t *testing.T) {
	c := NewCounter("test_counter")
	c.Inc()
	c.Inc()
	c.Add(5)
	if c.Value() != 7 {
		t.Errorf("expected 7, got %d", c.Value())
	}
}

func TestGauge(t *testing.T) {
	g := NewGauge("test_gauge")
	g.Set(10)
	g.Inc()
	g.Dec()
	if g.Value() != 10 {
		t.Errorf("expected 10, got %d", g.Value())
	}
}

func TestHistogram(t *testing.T) {
	h := NewHistogram("test_hist", []float64{0.005, 0.01, 0.05, 0.1, 0.5, 1.0})
	h.Observe(0.003)
	h.Observe(0.007)
	h.Observe(0.1)
	h.Observe(2.0)
	if h.Count() != 4 {
		t.Errorf("expected 4, got %d", h.Count())
	}
	if h.BucketCount(0) != 1 {
		t.Errorf("bucket 0: expected 1, got %d", h.BucketCount(0))
	}
	if h.BucketCount(1) != 1 {
		t.Errorf("bucket 1: expected 1, got %d", h.BucketCount(1))
	}
	if h.BucketCount(len(h.Buckets())) != 1 {
		t.Errorf("overflow: expected 1, got %d", h.BucketCount(len(h.Buckets())))
	}
}

func TestExpoFormat(t *testing.T) {
	reg := NewRegistry()
	c := NewCounter("mammoth_engine_puts_total")
	c.Add(1423)
	reg.Register(c)

	g := NewGauge("mammoth_connections")
	g.Set(5)
	reg.Register(g)

	h := NewHistogram("mammoth_command_duration_seconds", []float64{0.005, 0.01})
	h.Observe(0.003)
	reg.Register(h)

	var buf bytes.Buffer
	WritePrometheus(&buf, reg.All())
	out := buf.String()

	if !strings.Contains(out, "# TYPE mammoth_engine_puts_total counter") {
		t.Error("missing counter TYPE")
	}
	if !strings.Contains(out, "mammoth_engine_puts_total 1423") {
		t.Error("missing counter value")
	}
	if !strings.Contains(out, "# TYPE mammoth_connections gauge") {
		t.Error("missing gauge TYPE")
	}
	if !strings.Contains(out, "# TYPE mammoth_command_duration_seconds histogram") {
		t.Error("missing histogram TYPE")
	}
	if !strings.Contains(out, "mammoth_command_duration_seconds_count 1") {
		t.Error("missing histogram count")
	}
}
