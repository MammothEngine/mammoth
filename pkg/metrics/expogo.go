package metrics

import (
	"fmt"
	"io"
	"math"
	"strings"
)

// WritePrometheus writes all metrics in Prometheus text exposition format.
func WritePrometheus(w io.Writer, metrics []Metric) {
	for _, m := range metrics {
		switch m.Type() {
		case "counter":
			writeCounter(w, m)
		case "gauge":
			writeGauge(w, m)
		case "histogram":
			writeHistogram(w, m)
		}
	}
}

func writeCounter(w io.Writer, m Metric) {
	c := m.(*Counter)
	fmt.Fprintf(w, "# TYPE %s counter\n", c.Name())
	fmt.Fprintf(w, "%s %d\n\n", c.Name(), c.Value())
}

func writeGauge(w io.Writer, m Metric) {
	g := m.(*Gauge)
	fmt.Fprintf(w, "# TYPE %s gauge\n", g.Name())
	fmt.Fprintf(w, "%s %d\n\n", g.Name(), g.Value())
}

func writeHistogram(w io.Writer, m Metric) {
	h := m.(*Histogram)
	buckets := h.Buckets()
	name := h.Name()

	fmt.Fprintf(w, "# TYPE %s histogram\n", name)

	var cumulative uint64
	for i, b := range buckets {
		cumulative += h.BucketCount(i)
		fmt.Fprintf(w, "%s_bucket{le=\"%s\"} %d\n", name, formatFloat(b), cumulative)
	}
	cumulative += h.BucketCount(len(buckets))
	fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n", name, cumulative)
	fmt.Fprintf(w, "%s_sum %s\n", name, formatFloat(h.Sum()))
	fmt.Fprintf(w, "%s_count %d\n\n", name, h.Count())
}

func formatFloat(v float64) string {
	if v == math.Inf(1) {
		return "+Inf"
	}
	s := fmt.Sprintf("%g", v)
	if !strings.Contains(s, ".") && !strings.Contains(s, "e") && !strings.Contains(s, "E") {
		s += ".0"
	}
	return s
}
