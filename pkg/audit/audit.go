package audit

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

// AuditEntry represents a single audit log record.
type AuditEntry struct {
	Timestamp  time.Time `json:"ts"`
	Operation  string    `json:"op"`
	Database   string    `json:"db"`
	Collection string    `json:"coll,omitempty"`
	User       string    `json:"user,omitempty"`
	RemoteAddr string    `json:"remote,omitempty"`
	Details    any       `json:"details,omitempty"`
	Duration   string    `json:"duration,omitempty"`
}

// AuditLogger writes structured audit records as JSON Lines to a file.
// A nil or disabled logger silently discards all Log calls.
type AuditLogger struct {
	mu      sync.Mutex
	file    *os.File
	enc     *json.Encoder
	enabled bool
}

// NewAuditLogger creates an AuditLogger that appends JSON Lines to path.
// If path is empty, it returns a disabled logger that discards all writes.
func NewAuditLogger(path string) (*AuditLogger, error) {
	if path == "" {
		return &AuditLogger{}, nil
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	return &AuditLogger{
		file:    f,
		enc:     json.NewEncoder(f),
		enabled: true,
	}, nil
}

// Log writes one audit entry as a JSON line to the underlying file.
// It is a no-op when the logger is disabled or the receiver is nil.
func (l *AuditLogger) Log(entry AuditEntry) error {
	if l == nil || !l.enabled {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if err := l.enc.Encode(entry); err != nil {
		return err
	}
	// json.Encoder.Encode already appends a newline, so just flush.
	return l.file.Sync()
}

// LogOperation is a convenience that builds an AuditEntry from the given
// parameters and writes it. Timestamp is set to time.Now().
func (l *AuditLogger) LogOperation(op, db, coll string, duration time.Duration) {
	_ = l.Log(AuditEntry{
		Timestamp:  time.Now(),
		Operation:  op,
		Database:   db,
		Collection: coll,
		Duration:   duration.String(),
	})
}

// Enabled reports whether the logger will actually write to a file.
func (l *AuditLogger) Enabled() bool {
	if l == nil {
		return false
	}
	return l.enabled
}

// Close flushes and closes the underlying file. It is safe to call Close
// on a disabled logger.
func (l *AuditLogger) Close() error {
	if l == nil || !l.enabled {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	err := l.file.Close()
	l.file = nil
	l.enc = nil
	l.enabled = false
	return err
}
