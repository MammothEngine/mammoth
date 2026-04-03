package metrics

import (
	"testing"
)

func TestNewProductionMetrics(t *testing.T) {
	pm := NewProductionMetrics()

	if pm == nil {
		t.Fatal("expected non-nil ProductionMetrics")
	}
	if pm.RateLimitAllowed == nil {
		t.Error("expected RateLimitAllowed to be initialized")
	}
	if pm.CBStateChanges == nil {
		t.Error("expected CBStateChanges to be initialized")
	}
}

func TestProductionMetrics_RegisterToRegistry(t *testing.T) {
	pm := NewProductionMetrics()
	reg := NewRegistry()

	pm.RegisterToRegistry(reg)

	// Check that metrics are registered
	if len(reg.metrics) != 13 {
		t.Errorf("expected 13 metrics, got %d", len(reg.metrics))
	}
}

func TestProductionMetrics_RateLimiting(t *testing.T) {
	pm := NewProductionMetrics()

	pm.RecordRateLimitAllowed()
	pm.RecordRateLimitAllowed()
	pm.RecordRateLimitDenied()
	pm.SetRateLimitActive(5)

	if pm.RateLimitAllowed.Value() != 2 {
		t.Errorf("RateLimitAllowed = %d, want 2", pm.RateLimitAllowed.Value())
	}
	if pm.RateLimitDenied.Value() != 1 {
		t.Errorf("RateLimitDenied = %d, want 1", pm.RateLimitDenied.Value())
	}
	if pm.RateLimitActive.Value() != 5 {
		t.Errorf("RateLimitActive = %d, want 5", pm.RateLimitActive.Value())
	}
}

func TestProductionMetrics_CircuitBreaker(t *testing.T) {
	pm := NewProductionMetrics()

	pm.RecordCBStateChange()
	pm.RecordCBStateChange()
	pm.SetCBStates(1, 2, 3)

	if pm.CBStateChanges.Value() != 2 {
		t.Errorf("CBStateChanges = %d, want 2", pm.CBStateChanges.Value())
	}
	if pm.CBOpenState.Value() != 1 {
		t.Errorf("CBOpenState = %d, want 1", pm.CBOpenState.Value())
	}
	if pm.CBHalfOpenState.Value() != 2 {
		t.Errorf("CBHalfOpenState = %d, want 2", pm.CBHalfOpenState.Value())
	}
	if pm.CBClosedState.Value() != 3 {
		t.Errorf("CBClosedState = %d, want 3", pm.CBClosedState.Value())
	}
}

func TestProductionMetrics_Retry(t *testing.T) {
	pm := NewProductionMetrics()

	pm.RecordRetryAttempt()
	pm.RecordRetryAttempt()
	pm.RecordRetryAttempt()
	pm.RecordRetrySuccess()
	pm.RecordRetryFailure()

	if pm.RetryAttempts.Value() != 3 {
		t.Errorf("RetryAttempts = %d, want 3", pm.RetryAttempts.Value())
	}
	if pm.RetrySuccess.Value() != 1 {
		t.Errorf("RetrySuccess = %d, want 1", pm.RetrySuccess.Value())
	}
	if pm.RetryFailure.Value() != 1 {
		t.Errorf("RetryFailure = %d, want 1", pm.RetryFailure.Value())
	}
}

func TestProductionMetrics_QueryTimeouts(t *testing.T) {
	pm := NewProductionMetrics()

	pm.RecordQueryTimeout()
	pm.RecordQueryTimeout()

	if pm.QueryTimeouts.Value() != 2 {
		t.Errorf("QueryTimeouts = %d, want 2", pm.QueryTimeouts.Value())
	}
}

func TestProductionMetrics_Connections(t *testing.T) {
	pm := NewProductionMetrics()

	pm.IncActiveConnections()
	pm.IncActiveConnections()
	pm.IncActiveConnections()
	pm.DecActiveConnections()

	if pm.ActiveConnections.Value() != 2 {
		t.Errorf("ActiveConnections = %d, want 2", pm.ActiveConnections.Value())
	}
	if pm.TotalConnections.Value() != 3 {
		t.Errorf("TotalConnections = %d, want 3", pm.TotalConnections.Value())
	}
}

type testMetricsCollector struct {
	value float64
}

func (t *testMetricsCollector) CollectMetrics() map[string]float64 {
	return map[string]float64{
		"value": t.value,
	}
}

func TestCollectorManager(t *testing.T) {
	cm := NewCollectorManager()

	collector := &testMetricsCollector{value: 42}

	cm.RegisterCollector("test", collector)

	if len(cm.collectors) != 1 {
		t.Errorf("expected 1 collector, got %d", len(cm.collectors))
	}

	cm.UnregisterCollector("test")

	if len(cm.collectors) != 0 {
		t.Errorf("expected 0 collectors, got %d", len(cm.collectors))
	}
}

func TestSimpleGauge(t *testing.T) {
	g := NewSimpleGauge()

	g.Set(42.5)
	if v := g.Get(); v != 42.5 {
		t.Errorf("Get() = %v, want 42.5", v)
	}

	g.Inc()
	if v := g.Get(); v != 43.5 {
		t.Errorf("Get() after Inc = %v, want 43.5", v)
	}

	g.Dec()
	if v := g.Get(); v != 42.5 {
		t.Errorf("Get() after Dec = %v, want 42.5", v)
	}
}

func TestSimpleCounter(t *testing.T) {
	c := NewSimpleCounter()

	c.Inc()
	c.Inc()
	c.Add(3)

	if v := c.Get(); v != 5 {
		t.Errorf("Get() = %d, want 5", v)
	}
}

func BenchmarkProductionMetricsRecord(b *testing.B) {
	pm := NewProductionMetrics()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pm.RecordRateLimitAllowed()
	}
}
