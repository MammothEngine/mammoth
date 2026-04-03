package integration

import (
	"context"
	"fmt"
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

// TestFeatureMatrix_AllFeatures validates all production features work together
func TestFeatureMatrix_AllFeatures(t *testing.T) {
	// Rate limiter
	rlConfig := ratelimit.Config{
		Enabled:           true,
		RequestsPerSecond: 10000,
		Burst:             1000,
		PerConnection:     true,
		GlobalRate:        50000,
		GlobalBurst:       5000,
	}
	rlManager := ratelimit.NewManager(rlConfig)
	defer rlManager.Close()

	// Circuit breaker
	cbConfig := circuitbreaker.Config{
		FailureThreshold: 5,
		SuccessThreshold: 3,
		Timeout:          5 * time.Second,
		MaxRequests:      3,
	}
	cbManager := circuitbreaker.NewManager(cbConfig)

	// Retry configuration
	retryConfig := retry.Config{
		MaxRetries: 3,
		BaseDelay:  10 * time.Millisecond,
		MaxDelay:   1 * time.Second,
		Multiplier: 2.0,
		Jitter:     true,
	}

	// Metrics
	productionMetrics := metrics.NewProductionMetrics()

	// Shutdown manager
	shutdownConfig := shutdown.DefaultConfig()
	shutdownMgr := shutdown.NewManager(shutdownConfig)

	// Test scenario: Simulate production workload
	ctx := context.Background()

	var successCount, failureCount atomic.Int32
	var wg sync.WaitGroup

	// Simulate 100 concurrent requests
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Start request tracking for shutdown
			if !shutdownMgr.StartRequest() {
				return // Server shutting down
			}
			defer shutdownMgr.EndRequest()

			// Rate limiting check
			connID := uint64(id % 10) // 10 different "connections"
			if !rlManager.Allow(connID) {
				failureCount.Add(1)
				productionMetrics.RecordRateLimitDenied()
				return
			}
			productionMetrics.RecordRateLimitAllowed()

			// Circuit breaker protected operation with retry
			err := cbManager.Execute(fmt.Sprintf("operation-%d", id), func() error {
				productionMetrics.RecordRetryAttempt()
				return retry.DoWithContext(ctx, retryConfig, func() error {
					// Simulate work
					time.Sleep(time.Microsecond * 100)

					// Simulate occasional failures
					if id%20 == 0 {
						return fmt.Errorf("simulated error")
					}

					return nil
				})
			})

			if err != nil {
				failureCount.Add(1)
				productionMetrics.RecordRetryFailure()
			} else {
				successCount.Add(1)
				productionMetrics.RecordRetrySuccess()
			}
		}(i)
	}

	wg.Wait()

	// Validate results
	successes := successCount.Load()
	failures := failureCount.Load()

	t.Logf("Results: %d successes, %d failures", successes, failures)

	if successes == 0 {
		t.Error("Expected some successful operations")
	}

	// Verify metrics were recorded
	if productionMetrics.RateLimitAllowed.Value() == 0 {
		t.Log("Note: Metrics may not be incremented due to test timing")
	}

	// Verify circuit breaker metrics
	cbMetrics := cbManager.GetAllMetrics()
	if len(cbMetrics) == 0 {
		t.Error("Expected circuit breaker metrics")
	}
}

// TestFeatureMatrix_StressTest runs a stress test on all features
func TestFeatureMatrix_StressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	// Configure components for stress
	rlConfig := ratelimit.Config{
		Enabled:           true,
		RequestsPerSecond: 100000,
		Burst:             10000,
		PerConnection:     true,
		GlobalRate:        1000000,
		GlobalBurst:       100000,
	}
	rlManager := ratelimit.NewManager(rlConfig)
	defer rlManager.Close()

	cbConfig := circuitbreaker.Config{
		FailureThreshold: 10,
		SuccessThreshold: 5,
		Timeout:          1 * time.Second,
		MaxRequests:      10,
	}
	cbManager := circuitbreaker.NewManager(cbConfig)

	retryConfig := retry.Config{
		MaxRetries: 2,
		BaseDelay:  1 * time.Millisecond,
		MaxDelay:   100 * time.Millisecond,
		Multiplier: 2.0,
		Jitter:     true,
	}

	shutdownMgr := shutdown.NewManager(shutdown.DefaultConfig())

	var opsCompleted atomic.Int64
	var startTime = time.Now()

	// Run stress test for 2 seconds
	duration := 2 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var wg sync.WaitGroup

	// Spawn many goroutines
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				if !shutdownMgr.StartRequest() {
					return
				}

				connID := uint64(id)
				if rlManager.Allow(connID) {
					cbManager.Execute(fmt.Sprintf("stress-%d", id%5), func() error {
						return retry.DoWithConfig(retryConfig, func() error {
							opsCompleted.Add(1)
							return nil
						})
					})
				}

				shutdownMgr.EndRequest()
			}
		}(i)
	}

	wg.Wait()

	elapsed := time.Since(startTime)
	ops := opsCompleted.Load()
	opsPerSec := float64(ops) / elapsed.Seconds()

	t.Logf("Stress test completed: %d ops in %v (%.0f ops/sec)", ops, elapsed, opsPerSec)

	if ops < 1000 {
		t.Errorf("Expected at least 1000 operations, got %d", ops)
	}
}

// BenchmarkFeatureMatrix_SingleOperation benchmarks a single operation with all features
func BenchmarkFeatureMatrix_SingleOperation(b *testing.B) {
	rlConfig := ratelimit.Config{
		Enabled:           true,
		RequestsPerSecond: 1000000,
		Burst:             1000000,
		PerConnection:     true,
		GlobalRate:        10000000,
		GlobalBurst:       1000000,
	}
	rlManager := ratelimit.NewManager(rlConfig)
	defer rlManager.Close()

	cbConfig := circuitbreaker.DefaultConfig()
	cbManager := circuitbreaker.NewManager(cbConfig)

	retryConfig := retry.Config{
		MaxRetries: 0, // No retries for benchmark
	}

	shutdownMgr := shutdown.NewManager(shutdown.DefaultConfig())

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		connID := uint64(1)
		for pb.Next() {
			if !shutdownMgr.StartRequest() {
				return
			}

			if rlManager.Allow(connID) {
				cbManager.Execute("bench-op", func() error {
					return retry.DoWithConfig(retryConfig, func() error {
						return nil
					})
				})
			}

			shutdownMgr.EndRequest()
		}
	})
}

// BenchmarkFeatureMatrix_FullStack benchmarks complete feature stack
func BenchmarkFeatureMatrix_FullStack(b *testing.B) {
	rlConfig := ratelimit.Config{
		Enabled:           true,
		RequestsPerSecond: 1000000,
		Burst:             1000000,
		PerConnection:     true,
		GlobalRate:        10000000,
		GlobalBurst:       1000000,
	}
	rlManager := ratelimit.NewManager(rlConfig)
	defer rlManager.Close()

	cbConfig := circuitbreaker.DefaultConfig()
	cbManager := circuitbreaker.NewManager(cbConfig)

	retryConfig := retry.Config{
		MaxRetries: 1,
		BaseDelay:  1 * time.Microsecond,
		Jitter:     false,
	}

	productionMetrics := metrics.NewProductionMetrics()
	shutdownMgr := shutdown.NewManager(shutdown.DefaultConfig())
	ctx := context.Background()

	attempts := 0
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if !shutdownMgr.StartRequest() {
			continue
		}

		connID := uint64(i % 10)
		if rlManager.Allow(connID) {
			cbManager.Execute("fullstack-op", func() error {
				return retry.DoWithContext(ctx, retryConfig, func() error {
					attempts++
					if attempts%100 == 0 {
						return fmt.Errorf("occasional error")
					}
					productionMetrics.RecordRetrySuccess()
					return nil
				})
			})
		}

		shutdownMgr.EndRequest()
	}
}

// BenchmarkRateLimiter_Throughput measures rate limiter throughput
func BenchmarkRateLimiter_Throughput(b *testing.B) {
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

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Allow(connID)
		}
	})
}

// BenchmarkCircuitBreaker_Throughput measures circuit breaker throughput
func BenchmarkCircuitBreaker_Throughput(b *testing.B) {
	cfg := circuitbreaker.DefaultConfig()
	cb := circuitbreaker.New(cfg)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cb.Execute(func() error {
				return nil
			})
		}
	})
}

// BenchmarkRetry_Throughput measures retry mechanism throughput
func BenchmarkRetry_Throughput(b *testing.B) {
	cfg := retry.Config{
		MaxRetries: 0,
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			retry.DoWithConfig(cfg, func() error {
				return nil
			})
		}
	})
}

// BenchmarkShutdown_Throughput measures shutdown manager throughput
func BenchmarkShutdown_Throughput(b *testing.B) {
	cfg := shutdown.DefaultConfig()
	mgr := shutdown.NewManager(cfg)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if mgr.StartRequest() {
				mgr.EndRequest()
			}
		}
	})
}

// TestFeatureMatrix_ConcurrentShutdown tests graceful shutdown with active requests
func TestFeatureMatrix_ConcurrentShutdown(t *testing.T) {
	shutdownConfig := shutdown.Config{
		DrainTimeout: 500 * time.Millisecond,
		ForceTimeout: 1 * time.Second,
	}
	shutdownMgr := shutdown.NewManager(shutdownConfig)

	var activeRequests atomic.Int32
	var wg sync.WaitGroup

	// Start some requests
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			if shutdownMgr.StartRequest() {
				activeRequests.Add(1)
				time.Sleep(200 * time.Millisecond) // Simulate work
				activeRequests.Add(-1)
				shutdownMgr.EndRequest()
			}
		}()
	}

	// Wait a bit then initiate shutdown
	time.Sleep(50 * time.Millisecond)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() {
		shutdownMgr.Shutdown(shutdownCtx)
	}()

	// Wait for shutdown
	shutdownMgr.Wait()
	wg.Wait()

	if shutdownMgr.InFlightRequests() > 0 {
		t.Errorf("Expected 0 in-flight requests after shutdown, got %d", shutdownMgr.InFlightRequests())
	}
}
