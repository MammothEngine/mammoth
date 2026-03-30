package metrics

import "sync/atomic"

// Histogram tracks distribution of values across predefined buckets.
type Histogram struct {
	name    string
	buckets []float64
	counts  []atomic.Uint64
	sum     atomic.Uint64 // fixed-point: sum * 1000
	count   atomic.Uint64
}

// NewHistogram creates a histogram with the given bucket boundaries.
func NewHistogram(name string, buckets []float64) *Histogram {
	return &Histogram{
		name:    name,
		buckets: buckets,
		counts:  make([]atomic.Uint64, len(buckets)+1),
	}
}

func (h *Histogram) Name() string { return h.name }
func (h *Histogram) Type() string { return "histogram" }

// Observe records a value.
func (h *Histogram) Observe(val float64) {
	h.count.Add(1)
	h.sum.Add(uint64(val * 1000))
	bucket := len(h.counts) - 1
	for i, b := range h.buckets {
		if val <= b {
			bucket = i
			break
		}
	}
	h.counts[bucket].Add(1)
}

// BucketCount returns the count in bucket i.
func (h *Histogram) BucketCount(i int) uint64 {
	if i < 0 || i >= len(h.counts) {
		return 0
	}
	return h.counts[i].Load()
}

// Sum returns the sum of observed values.
func (h *Histogram) Sum() float64 {
	return float64(h.sum.Load()) / 1000.0
}

// Count returns the total observation count.
func (h *Histogram) Count() uint64 { return h.count.Load() }

// Buckets returns the bucket boundaries.
func (h *Histogram) Buckets() []float64 { return h.buckets }
