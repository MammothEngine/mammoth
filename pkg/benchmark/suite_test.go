// Package benchmark provides comprehensive performance benchmarks.
package benchmark

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mammothengine/mammoth/pkg/circuitbreaker"
	"github.com/mammothengine/mammoth/pkg/metrics"
	"github.com/mammothengine/mammoth/pkg/ratelimit"
	"github.com/mammothengine/mammoth/pkg/retry"
	"github.com/mammothengine/mammoth/pkg/shutdown"
)

// BenchmarkCircuitBreaker_HeavyLoad tests under heavy concurrent load
func BenchmarkCircuitBreaker_HeavyLoad(b *testing.B) {
	cb := circuitbreaker.New(circuitbreaker.Config{
		FailureThreshold: 100,
		SuccessThreshold: 50,
		Timeout:          5 * time.Second,
		MaxRequests:      1000,
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cb.Execute(func() error {
				if rand.Intn(10) == 0 {
					return fmt.Errorf("error")
				}
				return nil
			})
		}
	})
}

// BenchmarkRateLimiter_Throughput tests throughput
func BenchmarkRateLimiter_Throughput(b *testing.B) {
	cfg := ratelimit.Config{
		Enabled:           true,
		RequestsPerSecond: 1000000,
		Burst:             100000,
		PerConnection:     true,
		GlobalRate:        5000000,
		GlobalBurst:       500000,
	}
	m := ratelimit.NewManager(cfg)
	defer m.Close()

	var allowed atomic.Int64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		connID := uint64(rand.Intn(100))
		for pb.Next() {
			if m.Allow(connID) {
				allowed.Add(1)
			}
		}
	})

	b.ReportMetric(float64(allowed.Load())/b.Elapsed().Seconds(), "ops/sec")
}

// BenchmarkRetry_WithFailures tests with failure scenarios
func BenchmarkRetry_WithFailures(b *testing.B) {
	cfg := retry.Config{
		MaxRetries: 3,
		BaseDelay:  1 * time.Millisecond,
		MaxDelay:   10 * time.Millisecond,
		Multiplier: 2.0,
		Jitter:     true,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		retry.DoWithConfig(cfg, func() error {
			if i%10 == 0 {
				return fmt.Errorf("error")
			}
			return nil
		})
	}
}

// BenchmarkShutdown_Drain tests graceful shutdown
func BenchmarkShutdown_Drain(b *testing.B) {
	for _, connections := range []int{10, 100} {
		b.Run(fmt.Sprintf("conn_%d", connections), func(b *testing.B) {
			mgr := shutdown.NewManager(shutdown.Config{
				DrainTimeout: 1 * time.Second,
				ForceTimeout: 2 * time.Second,
			})

			var wg sync.WaitGroup
			var completed atomic.Int64

			for i := 0; i < connections; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for mgr.StartRequest() {
						completed.Add(1)
						time.Sleep(time.Millisecond)
						mgr.EndRequest()
					}
				}()
			}

			b.ResetTimer()
			mgr.Shutdown(context.Background())
			wg.Wait()

			b.ReportMetric(float64(completed.Load()), "completed")
		})
	}
}

// BenchmarkMetrics_Collection tests metrics overhead
func BenchmarkMetrics_Collection(b *testing.B) {
	pm := metrics.NewProductionMetrics()

	b.Run("counter", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			pm.RecordRateLimitAllowed()
		}
	})

	b.Run("gauge", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			pm.SetRateLimitActive(int64(i % 100))
		}
	})
}

// BenchmarkScalability tests scaling with goroutines
func BenchmarkScalability(b *testing.B) {
	goroutines := []int{1, 2, 4, 8, 16, 32, 64}

	for _, n := range goroutines {
		b.Run(fmt.Sprintf("goroutines_%d", n), func(b *testing.B) {
			cfg := ratelimit.Config{
				Enabled:           true,
				RequestsPerSecond: 10000000,
				Burst:             10000000,
			}
			m := ratelimit.NewManager(cfg)
			defer m.Close()

			var wg sync.WaitGroup
			opsPerGoroutine := b.N / n
			if opsPerGoroutine == 0 {
				opsPerGoroutine = 1
			}

			b.ResetTimer()
			start := time.Now()

			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					connID := uint64(id)
					for j := 0; j < opsPerGoroutine; j++ {
						m.Allow(connID)
					}
				}(i)
			}

			wg.Wait()
			elapsed := time.Since(start)

			opsPerSec := float64(b.N) / elapsed.Seconds()
			b.ReportMetric(opsPerSec/1000000, "mops/sec")
		})
	}
}

// BenchmarkMemory_Allocation tracks allocations
func BenchmarkMemory_Allocation(b *testing.B) {
	b.Run("circuit_breaker", func(b *testing.B) {
		cb := circuitbreaker.New(circuitbreaker.DefaultConfig())
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cb.Execute(func() error { return nil })
		}
	})

	b.Run("rate_limiter", func(b *testing.B) {
		cfg := ratelimit.Config{
			Enabled:           true,
			RequestsPerSecond: 1000000,
			Burst:             1000000,
			PerConnection:     true,
			GlobalRate:        10000000,
			GlobalBurst:       1000000,
		}
		m := ratelimit.NewManager(cfg)
		defer m.Close()
		connID := uint64(1)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.Allow(connID)
		}
	})

	b.Run("retry", func(b *testing.B) {
		cfg := retry.Config{MaxRetries: 0}

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			retry.DoWithConfig(cfg, func() error { return nil })
		}
	})

	b.Run("shutdown", func(b *testing.B) {
		mgr := shutdown.NewManager(shutdown.DefaultConfig())

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if mgr.StartRequest() {
				mgr.EndRequest()
			}
		}
	})
}

// BenchmarkComparison compares all features
func BenchmarkComparison(b *testing.B) {
	b.Run("CircuitBreaker", func(b *testing.B) {
		cb := circuitbreaker.New(circuitbreaker.DefaultConfig())
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cb.Allow()
		}
	})

	b.Run("RateLimiter", func(b *testing.B) {
		cfg := ratelimit.Config{
			Enabled:           true,
			RequestsPerSecond: 1000000,
			Burst:             1000000,
		}
		m := ratelimit.NewManager(cfg)
		defer m.Close()
		connID := uint64(1)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m.Allow(connID)
		}
	})

	b.Run("Retry", func(b *testing.B) {
		cfg := retry.Config{MaxRetries: 0}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			retry.DoWithConfig(cfg, func() error { return nil })
		}
	})

	b.Run("Shutdown", func(b *testing.B) {
		mgr := shutdown.NewManager(shutdown.DefaultConfig())
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if mgr.StartRequest() {
				mgr.EndRequest()
			}
		}
	})

	b.Run("Metrics", func(b *testing.B) {
		pm := metrics.NewProductionMetrics()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			pm.RecordRateLimitAllowed()
		}
	})
}

// BenchmarkAllFeatures together
func BenchmarkAllFeatures(b *testing.B) {
	// Setup all components
	cb := circuitbreaker.New(circuitbreaker.DefaultConfig())
	rlCfg := ratelimit.Config{
		Enabled:           true,
		RequestsPerSecond: 1000000,
		Burst:             1000000,
		PerConnection:     true,
		GlobalRate:        10000000,
		GlobalBurst:       1000000,
	}
	rlManager := ratelimit.NewManager(rlCfg)
	defer rlManager.Close()

	retryCfg := retry.Config{MaxRetries: 0}
	shutdownMgr := shutdown.NewManager(shutdown.DefaultConfig())
	pm := metrics.NewProductionMetrics()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		connID := uint64(1)
		for pb.Next() {
			if !shutdownMgr.StartRequest() {
				return
			}

			if rlManager.Allow(connID) {
				cb.Execute(func() error {
					return retry.DoWithConfig(retryCfg, func() error {
						pm.RecordRateLimitAllowed()
						return nil
					})
				})
			}

			shutdownMgr.EndRequest()
		}
	})
}

// BenchmarkLatencyPercentiles measures latency distribution
func BenchmarkLatencyPercentiles(b *testing.B) {
	cb := circuitbreaker.New(circuitbreaker.DefaultConfig())

	// Collect latencies
	var mu sync.Mutex
	var latencies []time.Duration

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		cb.Execute(func() error { return nil })
		elapsed := time.Since(start)

		mu.Lock()
		latencies = append(latencies, elapsed)
		mu.Unlock()
	}

	// Report percentiles if we have data
	if len(latencies) > 0 {
		// Simple sort
		for i := 0; i < len(latencies); i++ {
			for j := i + 1; j < len(latencies); j++ {
				if latencies[i] > latencies[j] {
					latencies[i], latencies[j] = latencies[j], latencies[i]
				}
			}
		}

		p50 := latencies[len(latencies)*50/100]
		p95 := latencies[len(latencies)*95/100]
		p99 := latencies[len(latencies)*99/100]

		b.ReportMetric(float64(p50)/float64(time.Nanosecond), "p50_ns")
		b.ReportMetric(float64(p95)/float64(time.Nanosecond), "p95_ns")
		b.ReportMetric(float64(p99)/float64(time.Nanosecond), "p99_ns")
	}
}

// BenchmarkGoroutineCount tests with different GOMAXPROCS
func BenchmarkGoroutineCount(b *testing.B) {
	cpus := []int{1, 2, 4, 8, runtime.NumCPU()}

	for _, n := range cpus {
		b.Run(fmt.Sprintf("cpu_%d", n), func(b *testing.B) {
			oldMaxProcs := runtime.GOMAXPROCS(n)
			defer runtime.GOMAXPROCS(oldMaxProcs)

			cb := circuitbreaker.New(circuitbreaker.DefaultConfig())

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					cb.Execute(func() error { return nil })
				}
			})

			b.ReportMetric(float64(n), "gomaxprocs")
		})
	}
}
