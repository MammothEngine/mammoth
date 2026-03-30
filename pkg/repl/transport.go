package repl

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// TCPTransport sends RPCs to peers over TCP.
type TCPTransport struct {
	mu        sync.Mutex
	localAddr string
	conns     map[uint64]net.Conn // peer ID -> connection
	peers     map[uint64]string   // peer ID -> address
	handler   func(req *RPCRequest) (*RPCResponse, error)
}

// NewTCPTransport creates a TCP transport.
func NewTCPTransport(localAddr string) *TCPTransport {
	return &TCPTransport{
		localAddr: localAddr,
		conns:     make(map[uint64]net.Conn),
		peers:     make(map[uint64]string),
	}
}

// SetHandler sets the RPC handler for incoming connections.
func (t *TCPTransport) SetHandler(fn func(req *RPCRequest) (*RPCResponse, error)) {
	t.handler = fn
}

// AddPeer registers a peer address.
func (t *TCPTransport) AddPeer(id uint64, addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[id] = addr
}

// SendRPC sends an RPC to a peer.
func (t *TCPTransport) SendRPC(to uint64, req *RPCRequest) (*RPCResponse, error) {
	t.mu.Lock()
	addr, ok := t.peers[to]
	if !ok {
		t.mu.Unlock()
		return nil, fmt.Errorf("repl: unknown peer %d", to)
	}

	conn, hasConn := t.conns[to]
	t.mu.Unlock()

	var err error
	if !hasConn {
		conn, err = net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			return nil, fmt.Errorf("repl: dial peer %d: %w", to, err)
		}
		t.mu.Lock()
		t.conns[to] = conn
		t.mu.Unlock()
	}

	// Send request
	data := EncodeRPC(req)
	if _, err := conn.Write(data); err != nil {
		t.mu.Lock()
		delete(t.conns, to)
		t.mu.Unlock()
		conn.Close()
		return nil, fmt.Errorf("repl: write to peer %d: %w", to, err)
	}

	// Read response
	respData, err := readResponse(conn)
	if err != nil {
		t.mu.Lock()
		delete(t.conns, to)
		t.mu.Unlock()
		return nil, fmt.Errorf("repl: read from peer %d: %w", to, err)
	}

	return DecodeRPCResponse(respData)
}

// Listen starts accepting connections.
func (t *TCPTransport) Listen() error {
	ln, err := net.Listen("tcp", t.localAddr)
	if err != nil {
		return fmt.Errorf("repl: listen: %w", err)
	}

	go t.acceptLoop(ln)
	return nil
}

func (t *TCPTransport) acceptLoop(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go t.handleConn(conn)
	}
}

func (t *TCPTransport) handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		reqData, err := readRequest(conn)
		if err != nil {
			if err == io.EOF {
				return
			}
			return
		}

		req, err := DecodeRPC(reqData)
		if err != nil {
			return
		}

		if t.handler != nil {
			resp, err := t.handler(req)
			if err != nil {
				return
			}
			respData := EncodeRPCResponse(resp)
			if _, err := conn.Write(respData); err != nil {
				return
			}
		}
	}
}

// Close closes all connections.
func (t *TCPTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, conn := range t.conns {
		conn.Close()
		delete(t.conns, id)
	}
}

func readRequest(conn net.Conn) ([]byte, error) {
	// Read header: type(1) + from(8) + len(4) = 13 bytes
	header := make([]byte, 13)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, err
	}
	plen := int(uint32(header[9])<<24 | uint32(header[10])<<16 | uint32(header[11])<<8 | uint32(header[12]))
	payload := make([]byte, 13+plen)
	copy(payload, header)
	if _, err := io.ReadFull(conn, payload[13:]); err != nil {
		return nil, err
	}
	return payload, nil
}

func readResponse(conn net.Conn) ([]byte, error) {
	// Read header: type(1) + len(4) = 5 bytes
	header := make([]byte, 5)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, err
	}
	plen := int(uint32(header[1])<<24 | uint32(header[2])<<16 | uint32(header[3])<<8 | uint32(header[4]))
	payload := make([]byte, 5+plen)
	copy(payload, header)
	if _, err := io.ReadFull(conn, payload[5:]); err != nil {
		return nil, err
	}
	return payload, nil
}
