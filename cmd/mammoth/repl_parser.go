package main

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// parseDocument parses a JSON-like document string into a BSON document.
// Supports: {key: value, key: value, ...}
func parseDocument(s string) (*bson.Document, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return bson.NewDocument(), nil
	}
	if s[0] != '{' || s[len(s)-1] != '}' {
		return nil, fmt.Errorf("expected {...}, got %q", s)
	}
	s = s[1 : len(s)-1] // strip braces

	doc := bson.NewDocument()
	pos := 0
	for pos < len(s) {
		// Skip whitespace
		pos = skipWS(s, pos)
		if pos >= len(s) {
			break
		}

		// Parse key
		key, newPos, err := parseKey(s, pos)
		if err != nil {
			return nil, err
		}
		pos = newPos

		// Skip ':'
		pos = skipWS(s, pos)
		if pos >= len(s) || s[pos] != ':' {
			return nil, fmt.Errorf("expected ':' at position %d", pos)
		}
		pos++

		// Parse value
		val, newPos, err := parseValue(s, pos)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", key, err)
		}
		pos = newPos

		doc.Set(key, val)

		// Skip comma
		pos = skipWS(s, pos)
		if pos < len(s) && s[pos] == ',' {
			pos++
		}
	}
	return doc, nil
}

func skipWS(s string, pos int) int {
	for pos < len(s) && unicode.IsSpace(rune(s[pos])) {
		pos++
	}
	return pos
}

func parseKey(s string, pos int) (string, int, error) {
	pos = skipWS(s, pos)
	if pos >= len(s) {
		return "", pos, fmt.Errorf("expected key")
	}

	// Quoted key
	if s[pos] == '"' {
		end := strings.IndexByte(s[pos+1:], '"')
		if end < 0 {
			return "", pos, fmt.Errorf("unterminated string key")
		}
		return s[pos+1 : pos+1+end], pos + 2 + end, nil
	}

	// Unquoted key (until : or , or })
	end := pos
	for end < len(s) && s[end] != ':' && s[end] != ',' && s[end] != '}' && !unicode.IsSpace(rune(s[end])) {
		end++
	}
	return s[pos:end], end, nil
}

func parseValue(s string, pos int) (bson.Value, int, error) {
	pos = skipWS(s, pos)
	if pos >= len(s) {
		return bson.Value{}, pos, fmt.Errorf("expected value")
	}

	switch s[pos] {
	case '"': // String
		end := strings.IndexByte(s[pos+1:], '"')
		if end < 0 {
			return bson.Value{}, pos, fmt.Errorf("unterminated string")
		}
		return bson.VString(s[pos+1 : pos+1+end]), pos + 2 + end, nil

	case '{': // Nested document
		end, err := findMatchingBrace(s, pos)
		if err != nil {
			return bson.Value{}, pos, err
		}
		doc, err := parseDocument(s[pos : end+1])
		if err != nil {
			return bson.Value{}, pos, err
		}
		return bson.VDoc(doc), end + 1, nil

	case '[': // Array
		end, err := findMatchingBracket(s, pos)
		if err != nil {
			return bson.Value{}, pos, err
		}
		arr, err := parseArray(s[pos+1 : end])
		if err != nil {
			return bson.Value{}, pos, err
		}
		return bson.VArray(arr), end + 1, nil

	case 'n': // null
		if pos+4 <= len(s) && s[pos:pos+4] == "null" {
			return bson.VNull(), pos + 4, nil
		}

	case 't': // true
		if pos+4 <= len(s) && s[pos:pos+4] == "true" {
			return bson.VBool(true), pos + 4, nil
		}

	case 'f': // false
		if pos+5 <= len(s) && s[pos:pos+5] == "false" {
			return bson.VBool(false), pos + 5, nil
		}

	case 'O': // ObjectId("...")
		if pos+9 < len(s) && s[pos:pos+9] == "ObjectId(" {
			return parseObjectID(s, pos + 9)
		}
	}

	// Number
	if s[pos] == '-' || (s[pos] >= '0' && s[pos] <= '9') {
		return parseNumber(s, pos)
	}

	// Dollar-prefixed operator (like $gt, $set)
	if s[pos] == '$' {
		end := pos
		for end < len(s) && s[end] != ':' && s[end] != ',' && s[end] != '}' && !unicode.IsSpace(rune(s[end])) {
			end++
		}
		return bson.VString(s[pos:end]), end, nil
	}

	return bson.Value{}, pos, fmt.Errorf("unexpected character %q at position %d", s[pos], pos)
}

func parseNumber(s string, pos int) (bson.Value, int, error) {
	end := pos
	hasDot := false
	if s[end] == '-' {
		end++
	}
	for end < len(s) && ((s[end] >= '0' && s[end] <= '9') || s[end] == '.' || s[end] == 'e' || s[end] == 'E' || s[end] == '+' || (s[end] == '-' && end > pos)) {
		if s[end] == '.' {
			hasDot = true
		}
		end++
	}
	numStr := s[pos:end]
	if hasDot {
		f, err := strconv.ParseFloat(numStr, 64)
		if err != nil {
			return bson.Value{}, pos, err
		}
		return bson.VDouble(f), end, nil
	}
	i, err := strconv.ParseInt(numStr, 10, 64)
	if err != nil {
		return bson.Value{}, pos, err
	}
	if i > 2147483647 || i < -2147483648 {
		return bson.VInt64(i), end, nil
	}
	return bson.VInt32(int32(i)), end, nil
}

func parseObjectID(s string, pos int) (bson.Value, int, error) {
	if pos >= len(s) || s[pos] != '"' {
		return bson.Value{}, pos, fmt.Errorf("expected \" in ObjectId()")
	}
	end := strings.IndexByte(s[pos+1:], '"')
	if end < 0 {
		return bson.Value{}, pos, fmt.Errorf("unterminated ObjectId string")
	}
	hexStr := s[pos+1 : pos+1+end]
	oid, err := bson.ParseObjectID(hexStr)
	if err != nil {
		return bson.Value{}, pos, err
	}
	return bson.VObjectID(oid), pos + 2 + end + 1, nil // skip ), end quote, and closing paren
}

func parseArray(s string) (bson.Array, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return bson.Array{}, nil
	}

	var arr bson.Array
	pos := 0
	for pos < len(s) {
		pos = skipWS(s, pos)
		if pos >= len(s) {
			break
		}

		val, newPos, err := parseValue(s, pos)
		if err != nil {
			return nil, err
		}
		arr = append(arr, val)
		pos = newPos

		pos = skipWS(s, pos)
		if pos < len(s) && s[pos] == ',' {
			pos++
		}
	}
	return arr, nil
}

func findMatchingBrace(s string, start int) (int, error) {
	depth := 0
	for i := start; i < len(s); i++ {
		switch s[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return i, nil
			}
		case '"':
			end := strings.IndexByte(s[i+1:], '"')
			if end < 0 {
				return 0, fmt.Errorf("unterminated string")
			}
			i += end + 1
		}
	}
	return 0, fmt.Errorf("unmatched {")
}

func findMatchingBracket(s string, start int) (int, error) {
	depth := 0
	for i := start; i < len(s); i++ {
		switch s[i] {
		case '[':
			depth++
		case ']':
			depth--
			if depth == 0 {
				return i, nil
			}
		case '"':
			end := strings.IndexByte(s[i+1:], '"')
			if end < 0 {
				return 0, fmt.Errorf("unterminated string")
			}
			i += end + 1
		}
	}
	return 0, fmt.Errorf("unmatched [")
}
