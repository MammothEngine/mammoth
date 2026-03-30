package bson

import "encoding/binary"

// RawDocument wraps raw BSON bytes for zero-copy field access.
type RawDocument []byte

// Length returns the document's declared byte length.
func (r RawDocument) Length() int {
	if len(r) < 4 {
		return 0
	}
	return int(binary.LittleEndian.Uint32(r))
}

// Lookup finds a field by key without full decode.
func (r RawDocument) Lookup(key string) (Value, bool, error) {
	if len(r) < 5 {
		return Value{}, false, nil
	}

	size := int(binary.LittleEndian.Uint32(r))
	if size > len(r) {
		return Value{}, false, nil
	}

	pos := 4
	for pos < size-1 {
		typ := BSONType(r[pos])
		pos++

		// Read key
		k, err := readCString(r, &pos)
		if err != nil {
			return Value{}, false, err
		}

		if k == key {
			val, err := decodeValue(r, &pos, typ)
			return val, true, err
		}

		// Skip value
		if err := skipValue(r, &pos, typ); err != nil {
			return Value{}, false, err
		}
	}

	return Value{}, false, nil
}

// Values iterates all fields returning keys and raw type codes.
func (r RawDocument) Values() []RawElement {
	if len(r) < 5 {
		return nil
	}

	size := int(binary.LittleEndian.Uint32(r))
	pos := 4
	var elems []RawElement

	for pos < size-1 {
		typ := BSONType(r[pos])
		pos++
		key, err := readCString(r, &pos)
		if err != nil {
			break
		}
		valueStart := pos
		if err := skipValue(r, &pos, typ); err != nil {
			break
		}
		elems = append(elems, RawElement{
			Key:   key,
			Type:  typ,
			Value: r[valueStart:pos],
		})
	}

	return elems
}

// RawElement represents a single element in a RawDocument.
type RawElement struct {
	Key   string
	Type  BSONType
	Value []byte
}

func skipValue(data []byte, pos *int, typ BSONType) error {
	switch typ {
	case TypeDouble:
		*pos += 8
	case TypeString:
		if *pos+4 > len(data) {
			return errBufferTooShort
		}
		length := int(binary.LittleEndian.Uint32(data[*pos:]))
		*pos += 4 + length
	case TypeDocument, TypeArray:
		if *pos+4 > len(data) {
			return errBufferTooShort
		}
		size := int(binary.LittleEndian.Uint32(data[*pos:]))
		*pos += size
	case TypeBinary:
		if *pos+4 > len(data) {
			return errBufferTooShort
		}
		length := int(binary.LittleEndian.Uint32(data[*pos:]))
		*pos += 4 + 1 + length
	case TypeObjectID:
		*pos += 12
	case TypeBoolean:
		*pos++
	case TypeDateTime:
		*pos += 8
	case TypeTimestamp:
		*pos += 8
	case TypeNull, TypeUndefined, TypeMinKey, TypeMaxKey:
		// no payload
	case TypeRegex:
		for *pos < len(data) && data[*pos] != 0x00 {
			*pos++
		}
		*pos++ // null
		for *pos < len(data) && data[*pos] != 0x00 {
			*pos++
		}
		*pos++
	case TypeDBPointer:
		if *pos+4 > len(data) {
			return errBufferTooShort
		}
		length := int(binary.LittleEndian.Uint32(data[*pos:]))
		*pos += 4 + length + 12
	case TypeJavaScript, TypeSymbol:
		if *pos+4 > len(data) {
			return errBufferTooShort
		}
		length := int(binary.LittleEndian.Uint32(data[*pos:]))
		*pos += 4 + length
	case TypeCodeScope:
		if *pos+4 > len(data) {
			return errBufferTooShort
		}
		size := int(binary.LittleEndian.Uint32(data[*pos:]))
		*pos += size
	case TypeInt32:
		*pos += 4
	case TypeInt64:
		*pos += 8
	case TypeDecimal128:
		*pos += 16
	default:
		return errInvalidDocument
	}
	return nil
}
