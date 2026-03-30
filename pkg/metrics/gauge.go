package metrics

import "sync/atomic"

// Gauge is an atomic value that can go up or down.
type Gauge struct {
	name  string
	value atomic.Int64
}

// NewGauge creates a new gauge.
func NewGauge(name string) *Gauge {
	return &Gauge{name: name}
}

func (g *Gauge) Name() string { return g.name }
func (g *Gauge) Type() string { return "gauge" }
func (g *Gauge) Set(v int64)  { g.value.Store(v) }
func (g *Gauge) Inc()         { g.value.Add(1) }
func (g *Gauge) Dec()         { g.value.Add(-1) }
func (g *Gauge) Value() int64 { return g.value.Load() }
