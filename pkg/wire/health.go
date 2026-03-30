package wire

import (
	"container/ring"
	"sync"
	"time"

	"github.com/mammothengine/mammoth/pkg/logging"
)

// SlowQueryProfiler tracks slow operations in a ring buffer.
type SlowQueryProfiler struct {
	mu       sync.Mutex
	buffer   *ring.Ring
	capacity int
	threshold time.Duration
	log       *logging.Logger
}

// NewSlowQueryProfiler creates a new slow query profiler.
func NewSlowQueryProfiler(threshold time.Duration) *SlowQueryProfiler {
	cap := 100
	return &SlowQueryProfiler{
		buffer:    ring.New(cap),
		capacity:  cap,
		threshold: threshold,
		log:       logging.Default().WithComponent("slowquery"),
	}
}

// SlowQueryEntry represents a recorded slow operation.
type SlowQueryEntry struct {
	Timestamp time.Time
	Command   string
	Database  string
	Duration  time.Duration
}

// Record checks if an operation exceeded the threshold and logs it.
func (p *SlowQueryProfiler) Record(cmd, db string, duration time.Duration) {
	if duration < p.threshold {
		return
	}
	p.mu.Lock()
	p.buffer.Value = SlowQueryEntry{
		Timestamp: time.Now(),
		Command:   cmd,
		Database:  db,
		Duration:  duration,
	}
	p.buffer = p.buffer.Next()
	p.mu.Unlock()

	p.log.Warn("slow query",
		logging.FString("cmd", cmd),
		logging.FString("db", db),
		logging.FDuration("duration", duration),
	)
}

// Entries returns all recorded slow query entries.
func (p *SlowQueryProfiler) Entries() []SlowQueryEntry {
	p.mu.Lock()
	defer p.mu.Unlock()
	var entries []SlowQueryEntry
	p.buffer.Do(func(v interface{}) {
		if e, ok := v.(SlowQueryEntry); ok {
			entries = append(entries, e)
		}
	})
	return entries
}

// Threshold returns the configured slow query threshold.
func (p *SlowQueryProfiler) Threshold() time.Duration { return p.threshold }
