package logging

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// Level represents log severity.
type Level int

const (
	DebugLevel Level = iota
	InfoLevel
	WarnLevel
	ErrorLevel
)

func (l Level) String() string {
	switch l {
	case DebugLevel:
		return "debug"
	case InfoLevel:
		return "info"
	case WarnLevel:
		return "warn"
	case ErrorLevel:
		return "error"
	default:
		return "unknown"
	}
}

// ParseLevel parses a level string.
func ParseLevel(s string) Level {
	switch s {
	case "debug":
		return DebugLevel
	case "info":
		return InfoLevel
	case "warn":
		return WarnLevel
	case "error":
		return ErrorLevel
	default:
		return InfoLevel
	}
}

// Field is a structured key-value pair for log entries.
type Field struct {
	Key   string
	Value any
}

// Field constructors.
func FString(key, val string) Field                { return Field{key, val} }
func FInt(key string, val int) Field               { return Field{key, val} }
func FInt64(key string, val int64) Field           { return Field{key, val} }
func FFloat(key string, val float64) Field         { return Field{key, val} }
func FBool(key string, val bool) Field             { return Field{key, val} }
func FErr(val error) Field                         { return Field{"error", val.Error()} }
func FDuration(key string, val time.Duration) Field { return Field{key, val.String()} }

// Logger writes structured JSON log lines.
type Logger struct {
	level     Level
	component string
	fields    []Field
	output    io.Writer
	mu        sync.Mutex
}

var defaultLogger = &Logger{
	level:  InfoLevel,
	output: os.Stderr,
}

// Default returns the global logger.
func Default() *Logger { return defaultLogger }

// SetLevel sets the global log level.
func SetLevel(l Level) {
	defaultLogger.mu.Lock()
	defaultLogger.level = l
	defaultLogger.mu.Unlock()
}

// WithComponent returns a child logger with a component tag.
func (l *Logger) WithComponent(name string) *Logger {
	return &Logger{
		level:     l.level,
		component: name,
		fields:    copyFields(l.fields),
		output:    l.output,
	}
}

// WithFields returns a child logger with additional fields.
func (l *Logger) WithFields(fields ...Field) *Logger {
	merged := make([]Field, 0, len(l.fields)+len(fields))
	merged = append(merged, l.fields...)
	merged = append(merged, fields...)
	return &Logger{
		level:  l.level,
		fields: merged,
		output: l.output,
	}
}

func copyFields(f []Field) []Field {
	if len(f) == 0 {
		return nil
	}
	cp := make([]Field, len(f))
	copy(cp, f)
	return cp
}

// Debug logs at debug level.
func (l *Logger) Debug(msg string, fields ...Field) { l.log(DebugLevel, msg, fields...) }

// Info logs at info level.
func (l *Logger) Info(msg string, fields ...Field) { l.log(InfoLevel, msg, fields...) }

// Warn logs at warn level.
func (l *Logger) Warn(msg string, fields ...Field) { l.log(WarnLevel, msg, fields...) }

// Error logs at error level.
func (l *Logger) Error(msg string, fields ...Field) { l.log(ErrorLevel, msg, fields...) }

// Fatalf logs at error level and exits.
func (l *Logger) Fatalf(format string, args ...interface{}) {
	l.log(ErrorLevel, fmt.Sprintf(format, args...))
	os.Exit(1)
}

func (l *Logger) log(level Level, msg string, fields ...Field) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if level < l.level {
		return
	}

	entry := make(map[string]any, 4+len(l.fields)+len(fields))
	entry["ts"] = time.Now().UTC().Format(time.RFC3339Nano)
	entry["level"] = level.String()
	if l.component != "" {
		entry["component"] = l.component
	}
	entry["msg"] = msg

	for _, f := range l.fields {
		entry[f.Key] = f.Value
	}
	for _, f := range fields {
		entry[f.Key] = f.Value
	}

	data, _ := json.Marshal(entry)
	fmt.Fprintln(l.output, string(data))
}
