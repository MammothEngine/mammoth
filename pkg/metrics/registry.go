package metrics

import "sync"

// Metric is the interface for all metric types.
type Metric interface {
	Name() string
	Type() string
}

// Registry holds named metrics.
type Registry struct {
	mu      sync.RWMutex
	metrics map[string]Metric
}

// NewRegistry creates a new metric registry.
func NewRegistry() *Registry {
	return &Registry{metrics: make(map[string]Metric)}
}

// Register adds a metric.
func (r *Registry) Register(m Metric) {
	r.mu.Lock()
	r.metrics[m.Name()] = m
	r.mu.Unlock()
}

// Get retrieves a metric by name.
func (r *Registry) Get(name string) Metric {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.metrics[name]
}

// All returns all registered metrics.
func (r *Registry) All() []Metric {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Metric, 0, len(r.metrics))
	for _, m := range r.metrics {
		out = append(out, m)
	}
	return out
}
