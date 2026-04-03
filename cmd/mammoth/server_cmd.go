package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mammothengine/mammoth/pkg/admin"
	"github.com/mammothengine/mammoth/pkg/auth"
	"github.com/mammothengine/mammoth/pkg/config"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/logging"
	"github.com/mammothengine/mammoth/pkg/metrics"
	"github.com/mammothengine/mammoth/pkg/mongo"
	"github.com/mammothengine/mammoth/pkg/ratelimit"
	"github.com/mammothengine/mammoth/pkg/wire"
)

func serveCmd(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	port := fs.Int("port", 27017, "listen port")
	bind := fs.String("bind", "0.0.0.0", "bind address")
	dataDir := fs.String("data-dir", "./data", "data directory")
	logLevel := fs.String("log-level", "info", "log level (debug|info|warn|error)")
	tlsCertFile := fs.String("tls-cert-file", "", "TLS certificate file")
	tlsKeyFile := fs.String("tls-key-file", "", "TLS private key file")
	metricsPort := fs.Int("metrics-port", 9100, "Prometheus metrics HTTP port")
	authEnabled := fs.Bool("auth", false, "enable authentication")
	healthPort := fs.Int("health-port", 8080, "health check HTTP port")
	slowQueryThreshold := fs.Duration("slow-query-threshold", 100*time.Millisecond, "slow query log threshold")
	adminPort := fs.Int("admin-port", 8081, "admin web UI HTTP port")
	configFile := fs.String("config", "", "config file path (TOML)")
	fs.Parse(args)

	// Build config: defaults → config file → CLI flags → env vars
	cfg := config.DefaultConfig()
	if *configFile != "" {
		loaded, err := config.LoadFromFile(*configFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
			os.Exit(1)
		}
		cfg = loaded
	}
	cfg.ApplyFlags(map[string]string{
		"port":                  fmt.Sprintf("%d", *port),
		"bind":                  *bind,
		"data-dir":              *dataDir,
		"log-level":             *logLevel,
		"tls-cert-file":         *tlsCertFile,
		"tls-key-file":          *tlsKeyFile,
		"metrics-port":          fmt.Sprintf("%d", *metricsPort),
		"health-port":           fmt.Sprintf("%d", *healthPort),
		"auth":                  fmt.Sprintf("%v", *authEnabled),
		"slow-query-threshold":  slowQueryThreshold.String(),
		"admin-port":            fmt.Sprintf("%d", *adminPort),
	})
	cfg.ApplyEnv()

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Config error: %v\n", err)
		os.Exit(1)
	}

	// Setup structured logging
	logging.SetLevel(logging.ParseLevel(cfg.Server.LogLevel))
	log := logging.Default().WithComponent("server")

	// Open engine
	opts := engine.DefaultOptions(cfg.Server.DataDir)
	eng, err := engine.Open(opts)
	if err != nil {
		log.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	// Create catalog
	cat := mongo.NewCatalog(eng)

	// Create auth manager
	userStore := auth.NewUserStore(eng)
	authMgr := auth.NewAuthManager(userStore, cfg.Server.Auth.Enabled)

	// Create wire handler
	handler := wire.NewHandler(eng, cat, authMgr)

	// Setup rate limiting
	rateLimitMgr := ratelimit.NewManager(ratelimit.Config{
		Enabled:           cfg.Server.RateLimit.Enabled,
		RequestsPerSecond: cfg.Server.RateLimit.RequestsPerSecond,
		Burst:             cfg.Server.RateLimit.Burst,
		PerConnection:     cfg.Server.RateLimit.PerConnection,
		GlobalRate:        cfg.Server.RateLimit.GlobalRate,
		GlobalBurst:       cfg.Server.RateLimit.GlobalBurst,
		WaitTimeout:       cfg.Server.RateLimit.WaitTimeout,
	})
	handler.WithRateLimiter(rateLimitMgr)

	// Setup metrics
	metricsReg := metrics.NewRegistry()
	enginePuts := metrics.NewCounter("mammoth_engine_puts_total")
	engineGets := metrics.NewCounter("mammoth_engine_gets_total")
	engineDeletes := metrics.NewCounter("mammoth_engine_deletes_total")
	engineScans := metrics.NewCounter("mammoth_engine_scans_total")
	metricsReg.Register(enginePuts)
	metricsReg.Register(engineGets)
	metricsReg.Register(engineDeletes)
	metricsReg.Register(engineScans)

	totalConns := metrics.NewCounter("mammoth_connections_total")
	rejectedConns := metrics.NewCounter("mammoth_connections_rejected_total")
	activeConns := metrics.NewGauge("mammoth_connections_active")
	metricsReg.Register(totalConns)
	metricsReg.Register(rejectedConns)
	metricsReg.Register(activeConns)

	commandDuration := metrics.NewHistogram("mammoth_command_duration_seconds",
		[]float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0})
	totalCommands := metrics.NewCounter("mammoth_commands_total")
	errorCommands := metrics.NewCounter("mammoth_commands_errors_total")
	metricsReg.Register(commandDuration)
	metricsReg.Register(totalCommands)
	metricsReg.Register(errorCommands)

	handler.WithMetrics(&wire.HandlerMetrics{
		CommandDuration: commandDuration,
		TotalCommands:   totalCommands,
		Errors:          errorCommands,
	})
	handler.WithSlowQueryProfiler(wire.NewSlowQueryProfiler(cfg.Server.SlowQueryThreshold))

	addr := fmt.Sprintf("%s:%d", cfg.Server.Bind, cfg.Server.Port)
	srv, err := wire.NewServer(wire.ServerConfig{
		Addr:        addr,
		Handler:     handler,
		TLSCertFile: cfg.Server.TLS.CertFile,
		TLSKeyFile:  cfg.Server.TLS.KeyFile,
		Metrics: &wire.ServerMetrics{
			TotalConns:    totalConns,
			RejectedConns: rejectedConns,
			ActiveConns:   activeConns,
		},
	})
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	handler.SetConnCountFn(srv.ConnCount)

	log.Info("Mammoth Engine starting",
		logging.FString("version", version),
		logging.FString("addr", srv.Addr()),
		logging.FBool("auth", cfg.Server.Auth.Enabled),
		logging.FBool("tls", cfg.Server.TLS.CertFile != ""),
		logging.FString("config", func() string {
			if *configFile != "" {
				return *configFile
			}
			return "(none)"
		}()),
	)

	// Start metrics HTTP server
	metricsSrv := &http.Server{
		Addr:    fmt.Sprintf("0.0.0.0:%d", cfg.Server.Metrics.Port),
		Handler: metrics.Handler(metricsReg),
	}
	go func() {
		log.Info("Metrics endpoint", logging.FString("addr", fmt.Sprintf("0.0.0.0:%d/metrics", cfg.Server.Metrics.Port)))
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Warn("Metrics server error", logging.FErr(err))
		}
	}()

	// Start health HTTP server
	healthSrv := &http.Server{
		Addr: fmt.Sprintf("0.0.0.0:%d", cfg.Server.Admin.Port),
	}
	healthSrv.Handler = &healthServer{
		startTime: time.Now(),
		version:   version,
	}
	go func() {
		log.Info("Health endpoint", logging.FString("addr", fmt.Sprintf("0.0.0.0:%d/health", cfg.Server.Admin.Port)))
		if err := healthSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Warn("Health server error", logging.FErr(err))
		}
	}()

	// Start admin web UI server
	adminHandler := admin.NewAPIHandler(eng, cat, authMgr, version)
	adminSrv := &http.Server{
		Addr:    fmt.Sprintf("0.0.0.0:%d", cfg.Server.Admin.Port),
		Handler: adminHandler,
	}
	go func() {
		log.Info("Admin UI", logging.FString("addr", fmt.Sprintf("0.0.0.0:%d", cfg.Server.Admin.Port)))
		if err := adminSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Warn("Admin server error", logging.FErr(err))
		}
	}()

	// Hot-reload via SIGHUP
	hupCh := make(chan os.Signal, 1)
	signal.Notify(hupCh, syscall.SIGHUP)
	go func() {
		for range hupCh {
			if *configFile == "" {
				continue
			}
			loaded, err := config.LoadFromFile(*configFile)
			if err != nil {
				log.Warn("Hot-reload failed", logging.FErr(err))
				continue
			}
			logging.SetLevel(logging.ParseLevel(loaded.Server.LogLevel))
			log.Info("Hot-reloaded config",
				logging.FString("log-level", loaded.Server.LogLevel),
				logging.FDuration("slow-query-threshold", loaded.Server.SlowQueryThreshold),
			)
		}
	}()

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := srv.Serve(); err != nil {
			log.Warn("Server error", logging.FErr(err))
		}
	}()

	<-sigCh
	log.Info("Shutting down...")
	metricsSrv.Close()
	healthSrv.Close()
	adminSrv.Close()
	if err := srv.Close(); err != nil {
		log.Warn("Error closing server", logging.FErr(err))
	}
	log.Info("Server stopped.")
}
