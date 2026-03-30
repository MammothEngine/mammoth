package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mammothengine/mammoth/pkg/auth"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/logging"
	"github.com/mammothengine/mammoth/pkg/metrics"
	"github.com/mammothengine/mammoth/pkg/mongo"
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
	fs.Parse(args)

	// Setup structured logging
	logging.SetLevel(logging.ParseLevel(*logLevel))
	log := logging.Default().WithComponent("server")

	// Open engine
	opts := engine.DefaultOptions(*dataDir)
	eng, err := engine.Open(opts)
	if err != nil {
		log.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	// Create catalog
	cat := mongo.NewCatalog(eng)

	// Create auth manager
	userStore := auth.NewUserStore(eng)
	authMgr := auth.NewAuthManager(userStore, *authEnabled)

	// Create wire handler
	handler := wire.NewHandler(eng, cat, authMgr)

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
	handler.WithSlowQueryProfiler(wire.NewSlowQueryProfiler(*slowQueryThreshold))

	addr := fmt.Sprintf("%s:%d", *bind, *port)
	srv, err := wire.NewServer(wire.ServerConfig{
		Addr:        addr,
		Handler:     handler,
		TLSCertFile: *tlsCertFile,
		TLSKeyFile:  *tlsKeyFile,
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
		logging.FBool("auth", *authEnabled),
		logging.FBool("tls", *tlsCertFile != ""),
	)

	// Start metrics HTTP server
	metricsSrv := &http.Server{
		Addr:    fmt.Sprintf("0.0.0.0:%d", *metricsPort),
		Handler: metrics.Handler(metricsReg),
	}
	go func() {
		log.Info("Metrics endpoint", logging.FString("addr", fmt.Sprintf("0.0.0.0:%d/metrics", *metricsPort)))
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Warn("Metrics server error", logging.FErr(err))
		}
	}()

	// Start health HTTP server
	healthSrv := &http.Server{
		Addr: fmt.Sprintf("0.0.0.0:%d", *healthPort),
	}
	healthSrv.Handler = &healthServer{
		startTime: time.Now(),
		version:   version,
	}
	go func() {
		log.Info("Health endpoint", logging.FString("addr", fmt.Sprintf("0.0.0.0:%d/health", *healthPort)))
		if err := healthSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Warn("Health server error", logging.FErr(err))
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
	if err := srv.Close(); err != nil {
		log.Warn("Error closing server", logging.FErr(err))
	}
	log.Info("Server stopped.")
}
