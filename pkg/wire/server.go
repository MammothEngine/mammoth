package wire

import (
	"crypto/tls"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mammothengine/mammoth/pkg/logging"
	"github.com/mammothengine/mammoth/pkg/metrics"
)

// Server is a MongoDB-compatible wire protocol server.
type Server struct {
	listener  net.Listener
	handler   *Handler
	addr      string
	closed    atomic.Bool
	wg        sync.WaitGroup
	cfg       ServerConfig
	connCount atomic.Int64
	connID    atomic.Uint64
	totalConns  *metrics.Counter
	rejectedConns *metrics.Counter
	log       *logging.Logger
	startTime time.Time
}

// ServerConfig configures the wire server.
type ServerConfig struct {
	Addr           string   // bind address (default "0.0.0.0:27017")
	Handler        *Handler
	MaxConnections int      // max concurrent connections (0 = unlimited)
	TLSCertFile    string   // path to TLS certificate file
	TLSKeyFile     string   // path to TLS private key file
	ConnTimeout    time.Duration // connection idle timeout (0 = no timeout)
	Metrics        *ServerMetrics
}

// ServerMetrics holds server-level metrics.
type ServerMetrics struct {
	TotalConns     *metrics.Counter
	RejectedConns  *metrics.Counter
	ActiveConns    *metrics.Gauge
}

// NewServer creates a new wire protocol server.
func NewServer(config ServerConfig) (*Server, error) {
	addr := config.Addr
	if addr == "" {
		addr = "0.0.0.0:27017"
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	// Wrap with TLS if configured
	if config.TLSCertFile != "" && config.TLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(config.TLSCertFile, config.TLSKeyFile)
		if err != nil {
			ln.Close()
			return nil, err
		}
		ln = tls.NewListener(ln, &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		})
	}

	s := &Server{
		listener:  ln,
		handler:   config.Handler,
		addr:      ln.Addr().String(),
		cfg:       config,
		startTime: time.Now(),
		log:       logging.Default().WithComponent("server"),
	}

	if config.Metrics != nil {
		s.totalConns = config.Metrics.TotalConns
		s.rejectedConns = config.Metrics.RejectedConns
	}

	return s, nil
}

// Addr returns the actual listen address.
func (s *Server) Addr() string { return s.addr }

// StartTime returns when the server was created.
func (s *Server) StartTime() time.Time { return s.startTime }

// ConnCount returns the current connection count.
func (s *Server) ConnCount() int64 { return s.connCount.Load() }

// Serve starts accepting connections.
func (s *Server) Serve() error {
	for !s.closed.Load() {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.closed.Load() {
				return nil
			}
			s.log.Warn("accept error", logging.FErr(err))
			continue
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConn(conn)
		}()
	}
	return nil
}

// Close gracefully shuts down the server.
func (s *Server) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	err := s.listener.Close()
	s.wg.Wait()
	if s.handler != nil {
		s.handler.Close()
	}
	return err
}

func (s *Server) handleConn(conn net.Conn) {
	// Check connection limit
	if s.cfg.MaxConnections > 0 {
		if s.connCount.Load() >= int64(s.cfg.MaxConnections) {
			if s.rejectedConns != nil {
				s.rejectedConns.Inc()
			}
			conn.Close()
			return
		}
	}

	s.connCount.Add(1)
	if s.totalConns != nil {
		s.totalConns.Inc()
	}
	if s.cfg.Metrics != nil && s.cfg.Metrics.ActiveConns != nil {
		s.cfg.Metrics.ActiveConns.Inc()
	}

	defer func() {
		s.connCount.Add(-1)
		if s.cfg.Metrics != nil && s.cfg.Metrics.ActiveConns != nil {
			s.cfg.Metrics.ActiveConns.Dec()
		}
	}()

	defer conn.Close()

	// Set connection timeout
	if s.cfg.ConnTimeout > 0 {
		conn.SetReadDeadline(time.Now().Add(s.cfg.ConnTimeout))
	}

	remoteAddr := conn.RemoteAddr().String()
	cid := s.connID.Add(1)

	// Clean up auth session on disconnect
	defer func() {
		if s.handler.authMgr != nil {
			s.handler.authMgr.RemoveSession(cid)
		}
	}()

	for !s.closed.Load() {
		// Reset deadline before each read
		if s.cfg.ConnTimeout > 0 {
			conn.SetReadDeadline(time.Now().Add(s.cfg.ConnTimeout))
		}

		msg, err := ReadMessage(conn)
		if err != nil {
			if !s.closed.Load() {
				// Client disconnected or error
			}
			return
		}
		if msg == nil || msg.Msg == nil {
			continue
		}

		msg.RemoteAddr = remoteAddr
		msg.ConnID = cid
		response := s.handler.Handle(msg)
		if response != nil {
			if err := WriteMessage(conn, msg.Header.RequestID, 0, response); err != nil {
				return
			}
		}
	}
}
