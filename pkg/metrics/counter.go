package metrics

import "sync/atomic"

// Counter is an atomic monotonically-increasing counter.
type Counter struct {
	name  string
	value atomic.Uint64
}

// NewCounter creates a new counter.
func NewCounter(name string) *Counter {
	return &Counter{name: name}
}

func (c *Counter) Name() string   { return c.name }
func (c *Counter) Type() string   { return "counter" }
func (c *Counter) Inc()            { c.value.Add(1) }
func (c *Counter) Add(n uint64)   { c.value.Add(n) }
func (c *Counter) Value() uint64  { return c.value.Load() }
