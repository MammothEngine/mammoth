package wire

import (
	"log"
	"net"
	"sync"
	"sync/atomic"
)

// Server is a MongoDB-compatible wire protocol server.
type Server struct {
	listener net.Listener
	handler  *Handler
	addr     string
	closed   atomic.Bool
	wg       sync.WaitGroup
}

// ServerConfig configures the wire server.
type ServerConfig struct {
	Addr    string // bind address (default "0.0.0.0:27017")
	Handler *Handler
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

	return &Server{
		listener: ln,
		handler:  config.Handler,
		addr:     ln.Addr().String(),
	}, nil
}

// Addr returns the actual listen address.
func (s *Server) Addr() string {
	return s.addr
}

// Serve starts accepting connections.
func (s *Server) Serve() error {
	for !s.closed.Load() {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.closed.Load() {
				return nil
			}
			log.Printf("accept error: %v", err)
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
	defer conn.Close()
	remoteAddr := conn.RemoteAddr().String()

	for !s.closed.Load() {
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
		response := s.handler.Handle(msg)
		if response != nil {
			if err := WriteMessage(conn, msg.Header.RequestID, 0, response); err != nil {
				return
			}
		}
	}
}
