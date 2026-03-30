package bson

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

var (
	errInvalidDocument = errors.New("invalid BSON document")
	errBufferTooShort  = errors.New("buffer too short")
)

// Decode parses BSON bytes into a Document.
func Decode(data []byte) (*Document, error) {
	if len(data) < 5 {
		return nil, errBufferTooShort
	}

	size := int(binary.LittleEndian.Uint32(data))
	if len(data) < size {
		return nil, fmt.Errorf("document size %d exceeds buffer %d: %w", size, len(data), errBufferTooShort)
	}
	if data[size-1] != 0x00 {
		return nil, errors.New("document not null-terminated")
	}

	doc := NewDocument()
	pos := 4

	for pos < size-1 {
		if pos >= len(data) {
			return nil, errInvalidDocument
		}

		typ := BSONType(data[pos])
		pos++

		key, err := readCString(data, &pos)
		if err != nil {
			return nil, err
		}

		val, err := decodeValue(data, &pos, typ)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", key, err)
		}

		doc.Set(key, val)
	}

	return doc, nil
}

func decodeValue(data []byte, pos *int, typ BSONType) (Value, error) {
	switch typ {
	case TypeDouble:
		if *pos+8 > len(data) {
			return Value{}, errBufferTooShort
		}
		bits := binary.LittleEndian.Uint64(data[*pos:])
		*pos += 8
		return VDouble(float64fromBits(bits)), nil

	case TypeString:
		s, err := readString(data, pos)
		if err != nil {
			return Value{}, err
		}
		return VString(s), nil

	case TypeDocument:
		inner, err := decodeInlineDocument(data, pos)
		if err != nil {
			return Value{}, err
		}
		return Value{Type: TypeDocument, value: inner}, nil

	case TypeArray:
		arr, err := decodeArray(data, pos)
		if err != nil {
			return Value{}, err
		}
		return Value{Type: TypeArray, value: arr}, nil

	case TypeBinary:
		b, err := decodeBinary(data, pos)
		if err != nil {
			return Value{}, err
		}
		return Value{Type: TypeBinary, value: b}, nil

	case TypeUndefined:
		return Value{Type: TypeUndefined, value: nil}, nil

	case TypeObjectID:
		if *pos+12 > len(data) {
			return Value{}, errBufferTooShort
		}
		var id ObjectID
		copy(id[:], data[*pos:*pos+12])
		*pos += 12
		return Value{Type: TypeObjectID, value: id}, nil

	case TypeBoolean:
		if *pos+1 > len(data) {
			return Value{}, errBufferTooShort
		}
		v := data[*pos] != 0
		*pos++
		return VBool(v), nil

	case TypeDateTime:
		if *pos+8 > len(data) {
			return Value{}, errBufferTooShort
		}
		ms := int64(binary.LittleEndian.Uint64(data[*pos:]))
		*pos += 8
		return VDateTime(ms), nil

	case TypeNull:
		return VNull(), nil

	case TypeRegex:
		pattern, err := readCString(data, pos)
		if err != nil {
			return Value{}, err
		}
		options, err := readCString(data, pos)
		if err != nil {
			return Value{}, err
		}
		return Value{Type: TypeRegex, value: Regex{Pattern: pattern, Options: options}}, nil

	case TypeDBPointer:
		ns, err := readString(data, pos)
		if err != nil {
			return Value{}, err
		}
		if *pos+12 > len(data) {
			return Value{}, errBufferTooShort
		}
		var id ObjectID
		copy(id[:], data[*pos:*pos+12])
		*pos += 12
		return Value{Type: TypeDBPointer, value: DBPointer{Namespace: ns, ID: id}}, nil

	case TypeJavaScript:
		s, err := readString(data, pos)
		if err != nil {
			return Value{}, err
		}
		return Value{Type: TypeJavaScript, value: s}, nil

	case TypeSymbol:
		s, err := readString(data, pos)
		if err != nil {
			return Value{}, err
		}
		return Value{Type: TypeSymbol, value: s}, nil

	case TypeCodeScope:
		if *pos+4 > len(data) {
			return Value{}, errBufferTooShort
		}
		totalLen := int(binary.LittleEndian.Uint32(data[*pos:]))
		*pos += 4
		endPos := *pos - 4 + totalLen

		code, err := readString(data, pos)
		if err != nil {
			return Value{}, err
		}
		scope, err := decodeInlineDocument(data, pos)
		if err != nil {
			return Value{}, err
		}
		*pos = endPos
		return Value{Type: TypeCodeScope, value: CodeWithScope{Code: code, Scope: scope}}, nil

	case TypeInt32:
		if *pos+4 > len(data) {
			return Value{}, errBufferTooShort
		}
		v := int32(binary.LittleEndian.Uint32(data[*pos:]))
		*pos += 4
		return VInt32(v), nil

	case TypeTimestamp:
		if *pos+8 > len(data) {
			return Value{}, errBufferTooShort
		}
		v := binary.LittleEndian.Uint64(data[*pos:])
		*pos += 8
		return Value{Type: TypeTimestamp, value: v}, nil

	case TypeInt64:
		if *pos+8 > len(data) {
			return Value{}, errBufferTooShort
		}
		v := int64(binary.LittleEndian.Uint64(data[*pos:]))
		*pos += 8
		return VInt64(v), nil

	case TypeDecimal128:
		if *pos+16 > len(data) {
			return Value{}, errBufferTooShort
		}
		var d [16]byte
		copy(d[:], data[*pos:*pos+16])
		*pos += 16
		return Value{Type: TypeDecimal128, value: d}, nil

	case TypeMinKey:
		return Value{Type: TypeMinKey, value: nil}, nil

	case TypeMaxKey:
		return Value{Type: TypeMaxKey, value: nil}, nil

	default:
		return Value{}, fmt.Errorf("unknown BSON type: 0x%02x", typ)
	}
}

func decodeInlineDocument(data []byte, pos *int) (*Document, error) {
	if *pos+4 > len(data) {
		return nil, errBufferTooShort
	}
	size := int(binary.LittleEndian.Uint32(data[*pos:]))
	if *pos+size > len(data) {
		return nil, errBufferTooShort
	}
	doc, err := Decode(data[*pos : *pos+size])
	if err != nil {
		return nil, err
	}
	*pos += size
	return doc, nil
}

func decodeArray(data []byte, pos *int) (Array, error) {
	if *pos+4 > len(data) {
		return nil, errBufferTooShort
	}
	size := int(binary.LittleEndian.Uint32(data[*pos:]))
	endPos := *pos + size
	if endPos > len(data) {
		return nil, errBufferTooShort
	}

	var arr Array
	p := *pos + 4

	for p < endPos-1 {
		typ := BSONType(data[p])
		p++

		// Read key (numeric string index)
		_, err := readCString(data, &p)
		if err != nil {
			return nil, err
		}

		val, err := decodeValue(data, &p, typ)
		if err != nil {
			return nil, err
		}
		arr = append(arr, val)
	}

	*pos = endPos
	return arr, nil
}

func decodeBinary(data []byte, pos *int) (Binary, error) {
	if *pos+4 > len(data) {
		return Binary{}, errBufferTooShort
	}
	length := int(binary.LittleEndian.Uint32(data[*pos:]))
	*pos += 4
	if *pos+1 > len(data) {
		return Binary{}, errBufferTooShort
	}
	subtype := BinarySubtype(data[*pos])
	*pos++
	if *pos+length > len(data) {
		return Binary{}, errBufferTooShort
	}
	b := Binary{Subtype: subtype, Data: make([]byte, length)}
	copy(b.Data, data[*pos:*pos+length])
	*pos += length
	return b, nil
}

func readCString(data []byte, pos *int) (string, error) {
	end := *pos
	for end < len(data) && data[end] != 0x00 {
		end++
	}
	if end >= len(data) {
		return "", errInvalidDocument
	}
	s := string(data[*pos:end])
	*pos = end + 1
	return s, nil
}

func readString(data []byte, pos *int) (string, error) {
	if *pos+4 > len(data) {
		return "", errBufferTooShort
	}
	length := int(binary.LittleEndian.Uint32(data[*pos:]))
	*pos += 4
	if *pos+length > len(data) {
		return "", errBufferTooShort
	}
	// length includes null terminator
	s := string(data[*pos : *pos+length-1])
	*pos += length
	return s, nil
}

func float64fromBits(b uint64) float64 {
	return math.Float64frombits(b)
}
