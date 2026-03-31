package config

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// TOMLValue holds a parsed TOML value.
type TOMLValue struct {
	Type  string // "string", "int", "bool", "duration"
	Raw   string
	Int   int64
	Bool  bool
	Dur   time.Duration
}

// TOMLConfig is a flat map of "section.key" or "section.subsection.key" → value.
type TOMLConfig map[string]TOMLValue

// ParseTOML parses a minimal TOML subset:
//   - key = value pairs (string, int, bool, duration)
//   - [section] and [section.subsection] groups
//   - # line comments
//   - double-quoted strings with \" \\ \n escapes
func ParseTOML(input string) (TOMLConfig, error) {
	cfg := make(TOMLConfig)
	scanner := bufio.NewScanner(strings.NewReader(input))
	var section string
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Section header: [name] or [name.sub]
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.TrimSpace(line[1 : len(line)-1])
			continue
		}

		// Key = value
		key, rawVal, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("toml: line %d: expected key = value", lineNum)
		}
		key = strings.TrimSpace(key)
		rawVal = strings.TrimSpace(rawVal)

		// Strip inline comment for unquoted values
		if !strings.HasPrefix(rawVal, `"`) {
			if idx := strings.Index(rawVal, "#"); idx >= 0 {
				rawVal = strings.TrimSpace(rawVal[:idx])
			}
		}

		fullKey := key
		if section != "" {
			fullKey = section + "." + key
		}

		val, err := parseValue(rawVal)
		if err != nil {
			return nil, fmt.Errorf("toml: line %d: %w", lineNum, err)
		}
		cfg[fullKey] = val
	}

	return cfg, scanner.Err()
}

func parseValue(raw string) (TOMLValue, error) {
	// Quoted string
	if strings.HasPrefix(raw, `"`) && strings.HasSuffix(raw, `"`) {
		s, err := unescapeString(raw[1 : len(raw)-1])
		return TOMLValue{Type: "string", Raw: s}, err
	}

	// Bool
	if raw == "true" {
		return TOMLValue{Type: "bool", Raw: raw, Bool: true}, nil
	}
	if raw == "false" {
		return TOMLValue{Type: "bool", Raw: raw, Bool: false}, nil
	}

	// Duration (must end with a duration unit)
	if isDuration(raw) {
		d, err := time.ParseDuration(raw)
		if err == nil {
			return TOMLValue{Type: "duration", Raw: raw, Dur: d}, nil
		}
	}

	// Int
	if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return TOMLValue{Type: "int", Raw: raw, Int: n}, nil
	}

	// Fallback: bare string
	return TOMLValue{Type: "string", Raw: raw}, nil
}

func isDuration(s string) bool {
	if len(s) < 2 {
		return false
	}
	suffix := s[len(s)-1]
	return suffix == 's' || suffix == 'm' || suffix == 'h' || s[len(s)-2] == 'm' && s[len(s)-1] == 's'
}

func unescapeString(s string) (string, error) {
	var b strings.Builder
	b.Grow(len(s))
	i := 0
	for i < len(s) {
		if s[i] == '\\' && i+1 < len(s) {
			switch s[i+1] {
			case '"':
				b.WriteByte('"')
			case '\\':
				b.WriteByte('\\')
			case 'n':
				b.WriteByte('\n')
			case 't':
				b.WriteByte('\t')
			default:
				b.WriteByte('\\')
				b.WriteByte(s[i+1])
			}
			i += 2
		} else {
			b.WriteByte(s[i])
			i++
		}
	}
	return b.String(), nil
}

// GetString returns the string value for key, or empty string.
func (c TOMLConfig) GetString(key string) string {
	v, ok := c[key]
	if !ok {
		return ""
	}
	return v.Raw
}

// GetInt returns the int value for key, or defaultVal.
func (c TOMLConfig) GetInt(key string, defaultVal int) int {
	v, ok := c[key]
	if !ok || v.Type != "int" {
		return defaultVal
	}
	return int(v.Int)
}

// GetBool returns the bool value for key, or defaultVal.
func (c TOMLConfig) GetBool(key string, defaultVal bool) bool {
	v, ok := c[key]
	if !ok || v.Type != "bool" {
		return defaultVal
	}
	return v.Bool
}

// GetDuration returns the duration value for key, or defaultVal.
// Supports both bare durations (100ms) and quoted durations ("100ms").
func (c TOMLConfig) GetDuration(key string, defaultVal time.Duration) time.Duration {
	v, ok := c[key]
	if !ok {
		return defaultVal
	}
	if v.Type == "duration" {
		return v.Dur
	}
	// Try parsing string as duration (for quoted values like "100ms")
	if d, err := time.ParseDuration(v.Raw); err == nil {
		return d
	}
	return defaultVal
}
