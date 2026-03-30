package bson

import (
	"encoding/binary"
	"math"
)

// Encode serializes a Document to BSON bytes.
func Encode(doc *Document) []byte {
	// Pre-calculate size
	size := 4 // int32 size
	for _, e := range doc.elements {
		size += elementSize(e)
	}
	size++ // null terminator

	buf := make([]byte, 0, size)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(size))

	for _, e := range doc.elements {
		buf = appendElement(buf, e)
	}
	buf = append(buf, 0x00)

	// Fix size
	binary.LittleEndian.PutUint32(buf, uint32(len(buf)))
	return buf
}

func elementSize(e Element) int {
	size := 1 + len(e.Key) + 1 // type + key + null
	size += valueSize(e.Value)
	return size
}

func valueSize(v Value) int {
	switch v.Type {
	case TypeDouble:
		return 8
	case TypeString:
		return 4 + len(v.value.(string)) + 1
	case TypeDocument:
		return len(Encode(v.value.(*Document)))
	case TypeArray:
		arr := v.value.(Array)
		return arraySize(arr)
	case TypeBinary:
		b := v.value.(Binary)
		return 4 + 1 + len(b.Data)
	case TypeUndefined:
		return 0
	case TypeObjectID:
		return 12
	case TypeBoolean:
		return 1
	case TypeDateTime:
		return 8
	case TypeNull:
		return 0
	case TypeRegex:
		r := v.value.(Regex)
		return len(r.Pattern) + 1 + len(r.Options) + 1
	case TypeDBPointer:
		dp := v.value.(DBPointer)
		return 4 + len(dp.Namespace) + 1 + 12
	case TypeJavaScript:
		return 4 + len(v.value.(string)) + 1
	case TypeSymbol:
		return 4 + len(v.value.(string)) + 1
	case TypeCodeScope:
		cs := v.value.(CodeWithScope)
		scope := Encode(cs.Scope)
		return 4 + len(cs.Code) + 1 + len(scope)
	case TypeInt32:
		return 4
	case TypeTimestamp:
		return 8
	case TypeInt64:
		return 8
	case TypeDecimal128:
		return 16
	case TypeMinKey, TypeMaxKey:
		return 0
	}
	return 0
}

func arraySize(arr Array) int {
	size := 4 // int32 size
	for i, v := range arr {
		key := uitoa(uint(i))
		size += 1 + len(key) + 1 + valueSize(v)
	}
	size++ // null
	return size
}

func appendElement(buf []byte, e Element) []byte {
	buf = append(buf, byte(e.Value.Type))
	buf = append(buf, e.Key...)
	buf = append(buf, 0x00)
	buf = appendValue(buf, e.Value)
	return buf
}

func appendValue(buf []byte, v Value) []byte {
	switch v.Type {
	case TypeDouble:
		var b [8]byte
		math.Float64bits(v.value.(float64))
		binary.LittleEndian.PutUint64(b[:], math.Float64bits(v.value.(float64)))
		buf = append(buf, b[:]...)
	case TypeString:
		s := v.value.(string)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(s)+1))
		buf = append(buf, s...)
		buf = append(buf, 0x00)
	case TypeDocument:
		enc := Encode(v.value.(*Document))
		buf = append(buf, enc...)
	case TypeArray:
		buf = appendArray(buf, v.value.(Array))
	case TypeBinary:
		b := v.value.(Binary)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(b.Data)))
		buf = append(buf, byte(b.Subtype))
		buf = append(buf, b.Data...)
	case TypeObjectID:
		oid := v.value.(ObjectID)
		buf = append(buf, oid[:]...)
	case TypeBoolean:
		if v.value.(bool) {
			buf = append(buf, 0x01)
		} else {
			buf = append(buf, 0x00)
		}
	case TypeDateTime:
		buf = binary.LittleEndian.AppendUint64(buf, uint64(v.value.(int64)))
	case TypeTimestamp:
		buf = binary.LittleEndian.AppendUint64(buf, v.value.(uint64))
	case TypeNull:
		// no payload
	case TypeRegex:
		r := v.value.(Regex)
		buf = append(buf, r.Pattern...)
		buf = append(buf, 0x00)
		buf = append(buf, r.Options...)
		buf = append(buf, 0x00)
	case TypeDBPointer:
		dp := v.value.(DBPointer)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(dp.Namespace)+1))
		buf = append(buf, dp.Namespace...)
		buf = append(buf, 0x00)
		buf = append(buf, dp.ID[:]...)
	case TypeJavaScript:
		s := v.value.(string)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(s)+1))
		buf = append(buf, s...)
		buf = append(buf, 0x00)
	case TypeSymbol:
		s := v.value.(string)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(s)+1))
		buf = append(buf, s...)
		buf = append(buf, 0x00)
	case TypeCodeScope:
		cs := v.value.(CodeWithScope)
		scopeBytes := Encode(cs.Scope)
		totalLen := 4 + len(cs.Code) + 1 + len(scopeBytes)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(totalLen))
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(cs.Code)+1))
		buf = append(buf, cs.Code...)
		buf = append(buf, 0x00)
		buf = append(buf, scopeBytes...)
	case TypeInt32:
		buf = binary.LittleEndian.AppendUint32(buf, uint32(v.value.(int32)))
	case TypeInt64:
		buf = binary.LittleEndian.AppendUint64(buf, uint64(v.value.(int64)))
	case TypeDecimal128:
		// Store as 16 raw bytes
		d := v.value.([16]byte)
		buf = append(buf, d[:]...)
	case TypeMinKey, TypeMaxKey:
		// no payload
	}
	return buf
}

func appendArray(buf []byte, arr Array) []byte {
	// Calculate size
	size := 4 // int32
	for i, v := range arr {
		key := uitoa(uint(i))
		size += 1 + len(key) + 1 + valueSize(v)
	}
	size++

	start := len(buf)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(size))
	for i, v := range arr {
		buf = append(buf, byte(v.Type))
		buf = append(buf, uitoa(uint(i))...)
		buf = append(buf, 0x00)
		buf = appendValue(buf, v)
	}
	buf = append(buf, 0x00)

	// Fix size
	binary.LittleEndian.PutUint32(buf[start:], uint32(len(buf)-start))
	return buf
}

func uitoa(v uint) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
