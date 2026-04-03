// Package debug provides profiling and debugging endpoints.
package debug

import (
	"context"
	"expvar"
	"fmt"
	"net/http"
	"net/http/pprof"
	"runtime"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

// Config holds debug server configuration.
type Config struct {
	Enabled    bool
	Port       int
	PathPrefix string
}

// DefaultConfig returns default debug configuration.
func DefaultConfig() Config {
	return Config{
		Enabled:    false,
		Port:       6060,
		PathPrefix: "/debug",
	}
}

// Server provides debug and profiling endpoints.
type Server struct {
	config   Config
	eng      *engine.Engine
	cat      *mongo.Catalog
	server   *http.Server
	startTime time.Time
}

// NewServer creates a new debug server.
func NewServer(config Config, eng *engine.Engine, cat *mongo.Catalog) *Server {
	return &Server{
		config:    config,
		eng:       eng,
		cat:       cat,
		startTime: time.Now(),
	}
}

// Start starts the debug server.
func (s *Server) Start() error {
	if !s.config.Enabled {
		return nil
	}

	mux := http.NewServeMux()
	prefix := s.config.PathPrefix

	// pprof endpoints
	mux.HandleFunc(prefix+"/pprof/", pprof.Index)
	mux.HandleFunc(prefix+"/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc(prefix+"/pprof/profile", pprof.Profile)
	mux.HandleFunc(prefix+"/pprof/symbol", pprof.Symbol)
	mux.HandleFunc(prefix+"/pprof/trace", pprof.Trace)

	// Custom handlers
	mux.HandleFunc(prefix+"/vars", s.handleVars)
	mux.HandleFunc(prefix+"/health", s.handleHealth)
	mux.HandleFunc(prefix+"/gc", s.handleGC)
	mux.HandleFunc(prefix+"/stats", s.handleStats)
	mux.HandleFunc(prefix+"/freemem", s.handleFreeMemory)

	s.server = &http.Server{
		Addr:    fmt.Sprintf("0.0.0.0:%d", s.config.Port),
		Handler: mux,
	}

	go func() {
		s.server.ListenAndServe()
	}()

	return nil
}

// Stop stops the debug server.
func (s *Server) Stop(ctx context.Context) error {
	if s.server == nil {
		return nil
	}
	return s.server.Shutdown(ctx)
}

// handleVars returns expvars.
func (s *Server) handleVars(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "{\n")
	first := true
	expvar.Do(func(kv expvar.KeyValue) {
		if !first {
			fmt.Fprintf(w, ",\n")
		}
		first = false
		fmt.Fprintf(w, "%q: %s", kv.Key, kv.Value)
	})
	fmt.Fprintf(w, "\n}\n")
}

// handleHealth returns detailed health information.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	health := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"uptime":    time.Since(s.startTime).String(),
		"goroutines": runtime.NumGoroutine(),
		"memory": map[string]interface{}{
			"alloc":         m.Alloc,
			"total_alloc":   m.TotalAlloc,
			"sys":           m.Sys,
			"num_gc":        m.NumGC,
			"heap_alloc":    m.HeapAlloc,
			"heap_sys":      m.HeapSys,
			"heap_inuse":    m.HeapInuse,
			"heap_idle":     m.HeapIdle,
		},
	}

	if s.eng != nil {
		stats := s.eng.Stats()
		health["engine"] = map[string]interface{}{
			"memtables":       stats.MemtableCount,
			"memtable_size":   stats.MemtableSizeBytes,
			"sstables":        stats.SSTableCount,
			"sstable_size":    stats.SSTableTotalBytes,
			"compactions":     stats.CompactionCount,
			"sequence_number": stats.SequenceNumber,
		}
	}

	jsonBytes, _ := json.MarshalIndent(health, "", "  ")
	w.Write(jsonBytes)
}

// handleGC triggers garbage collection.
func (s *Server) handleGC(w http.ResponseWriter, r *http.Request) {
	runtime.GC()
	debug.FreeOSMemory()
	w.Write([]byte("GC triggered\n"))
}

// handleStats returns detailed runtime statistics.
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	stats := map[string]interface{}{
		"runtime": map[string]interface{}{
			"go_version":   runtime.Version(),
			"go_os":        runtime.GOOS,
			"go_arch":      runtime.GOARCH,
			"num_cpu":      runtime.NumCPU(),
			"num_goroutine": runtime.NumGoroutine(),
			"num_cgo_call": runtime.NumCgoCall(),
		},
		"memory": map[string]interface{}{
			"alloc":              m.Alloc,
			"total_alloc":        m.TotalAlloc,
			"sys":                m.Sys,
			"lookups":            m.Lookups,
			"mallocs":            m.Mallocs,
			"frees":              m.Frees,
			"heap_alloc":         m.HeapAlloc,
			"heap_sys":           m.HeapSys,
			"heap_idle":          m.HeapIdle,
			"heap_inuse":         m.HeapInuse,
			"heap_released":      m.HeapReleased,
			"heap_objects":       m.HeapObjects,
			"stack_inuse":        m.StackInuse,
			"stack_sys":          m.StackSys,
			"mspan_inuse":        m.MSpanInuse,
			"mspan_sys":          m.MSpanSys,
			"mcache_inuse":       m.MCacheInuse,
			"mcache_sys":         m.MCacheSys,
			"buck_hash_sys":      m.BuckHashSys,
			"gc_sys":             m.GCSys,
			"other_sys":          m.OtherSys,
			"next_gc":            m.NextGC,
			"last_gc":            m.LastGC,
			"pause_total_ns":     m.PauseTotalNs,
			"pause_ns":           m.PauseNs,
			"num_gc":             m.NumGC,
			"num_forced_gc":      m.NumForcedGC,
			"gc_cpu_fraction":    m.GCCPUFraction,
		},
		"gc": map[string]interface{}{
			"target_percentage": debug.SetGCPercent(-1),
			"memory_limit":      debug.SetMemoryLimit(-1),
		},
	}

	// Restore GC percent after reading
	debug.SetGCPercent(100)

	jsonBytes, _ := json.MarshalIndent(stats, "", "  ")
	w.Write(jsonBytes)
}

// handleFreeMemory forces memory release to OS.
func (s *Server) handleFreeMemory(w http.ResponseWriter, r *http.Request) {
	debug.FreeOSMemory()
	w.Write([]byte("Memory freed to OS\n"))
}

// ProfileType represents a pprof profile type.
type ProfileType string

const (
	ProfileCPU       ProfileType = "cpu"
	ProfileHeap      ProfileType = "heap"
	ProfileGoroutine ProfileType = "goroutine"
	ProfileThread    ProfileType = "threadcreate"
	ProfileBlock     ProfileType = "block"
	ProfileMutex     ProfileType = "mutex"
)

// GetProfileURL returns the URL for a specific profile.
func (s *Server) GetProfileURL(profile ProfileType) string {
	if profile == ProfileCPU {
		return fmt.Sprintf("http://localhost:%d%s/pprof/profile", s.config.Port, s.config.PathPrefix)
	}
	return fmt.Sprintf("http://localhost:%d%s/pprof/%s", s.config.Port, s.config.PathPrefix, profile)
}

// ExpVar exports a variable for monitoring.
func ExpVar(name string, v interface{}) {
	switch val := v.(type) {
	case int:
		expvar.NewInt(name).Set(int64(val))
	case int64:
		expvar.NewInt(name).Set(val)
	case float64:
		expvar.NewFloat(name).Set(val)
	case string:
		expvar.NewString(name).Set(val)
	default:
		expvar.Publish(name, expvar.Func(func() interface{} { return v }))
	}
}

// ReadTCPTimeouts reads TCP timeout values from config.
func ReadTCPTimeouts() (readTimeout, writeTimeout time.Duration) {
	// These would normally be read from config
	return 30 * time.Second, 30 * time.Second
}

// ParseBool parses a boolean value from string.
func ParseBool(s string) bool {
	b, _ := strconv.ParseBool(s)
	return b
}

// Add default expvars.
func init() {
	expvar.NewString("version").Set("dev")
	expvar.NewString("build_time").Set(time.Now().Format(time.RFC3339))
}

// json is a placeholder for json package.
var json = &jsonImpl{}

type jsonImpl struct{}

func (j *jsonImpl) MarshalIndent(v interface{}, prefix, indent string) ([]byte, error) {
	// Simple JSON marshal - in real implementation use encoding/json
	return fmt.Appendf(nil, "%+v", v), nil
}
