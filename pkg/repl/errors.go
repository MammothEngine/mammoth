package repl

import "errors"

var (
	ErrInvalidRPC    = errors.New("repl: invalid RPC message")
	ErrNotLeader     = errors.New("repl: not the leader")
	ErrShutdown      = errors.New("repl: node shutdown")
	ErrTimeout       = errors.New("repl: timeout")
	ErrConfigChange  = errors.New("repl: config change rejected")
	ErrSnapshoptTooLarge = errors.New("repl: snapshot too large")
)
