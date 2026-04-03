package bson

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"testing"
	"time"
)

func TestDecode_EmptyBuffer(t *testing.T) {
	_, err := Decode([]byte{})
	if err != errBufferTooShort {
		t.Errorf("expected errBufferTooShort, got %v", err)
	}

	_, err = Decode([]byte{0x00, 0x00, 0x00})
	if err != errBufferTooShort {
		t.Errorf("expected errBufferTooShort for 3 bytes, got %v", err)
	}
}

func TestDecode_InvalidSize(t *testing.T) {
	// Buffer with size larger than actual data
	data := make([]byte, 8)
	binary.LittleEndian.PutUint32(data, 100) // Claims 100 bytes but only has 8
	data[4] = 0x00 // Null terminator

	_, err := Decode(data)
	if err == nil || !errors.Is(err, errBufferTooShort) {
		t.Errorf("expected errBufferTooShort for oversized document, got %v", err)
	}
}

func TestDecode_NotNullTerminated(t *testing.T) {
	// Document that doesn't end with null
	data := make([]byte, 8)
	binary.LittleEndian.PutUint32(data, 8)
	data[7] = 0xFF // Not null

	_, err := Decode(data)
	if err == nil {
		t.Error("expected error for non-null terminated document")
	}
}

func TestDecode_EmptyDocument(t *testing.T) {
	// Minimal valid document: size(4) + null(1) = 5 bytes, but minimum is 5 with size=5
	data := []byte{0x05, 0x00, 0x00, 0x00, 0x00}

	doc, err := Decode(data)
	if err != nil {
		t.Fatalf("failed to decode empty document: %v", err)
	}
	if doc.Len() != 0 {
		t.Errorf("expected empty document, got %d elements", doc.Len())
	}
}

func TestDecode_TruncatedDocument(t *testing.T) {
	// Document with size indicating more data than available during element parsing
	data := []byte{
		0x10, 0x00, 0x00, 0x00, // size = 16
		0x02, // type = string
		// Missing key and value
	}

	_, err := Decode(data)
	if err == nil {
		t.Error("expected error for truncated document")
	}
}

func TestDecode_AllTypes(t *testing.T) {
	doc := NewDocument()
	doc.Set("double", VDouble(3.14))
	doc.Set("string", VString("hello"))
	doc.Set("document", VDoc(NewDocument()))
	doc.Set("array", VArray(A()))
	doc.Set("binary", VBinary(BinaryGeneric, []byte{1, 2, 3}))
	doc.Set("objectid", VObjectID(NewObjectID()))
	doc.Set("bool", VBool(true))
	doc.Set("datetime", VDateTime(time.Now().UnixMilli()))
	doc.Set("null", VNull())
	doc.Set("regex", VRegex("test", "i"))
	doc.Set("javascript", VJavaScript("function() {}"))
	doc.Set("symbol", VSymbol("sym"))
	doc.Set("int32", VInt32(42))
	doc.Set("timestamp", VTimestamp(1234567890))
	doc.Set("int64", VInt64(9876543210))
	doc.Set("minkey", VMinKey())
	doc.Set("maxkey", VMaxKey())

	encoded := Encode(doc)
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	if decoded.Len() != doc.Len() {
		t.Errorf("expected %d elements, got %d", doc.Len(), decoded.Len())
	}
}

func TestDecode_NestedDocument(t *testing.T) {
	inner := NewDocument()
	inner.Set("value", VInt32(42))

	outer := NewDocument()
	outer.Set("nested", VDoc(inner))

	encoded := Encode(outer)
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("failed to decode nested: %v", err)
	}

	val, ok := decoded.Get("nested")
	if !ok {
		t.Fatal("nested field not found")
	}
	if val.Type != TypeDocument {
		t.Errorf("expected TypeDocument, got %v", val.Type)
	}

	nestedDoc := val.DocumentValue()
	innerVal, ok := nestedDoc.Get("value")
	if !ok || innerVal.Int32() != 42 {
		t.Error("nested value incorrect")
	}
}

func TestDecode_Array(t *testing.T) {
	arr := A(VInt32(1), VInt32(2), VInt32(3))

	doc := NewDocument()
	doc.Set("items", VArray(arr))

	encoded := Encode(doc)
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("failed to decode array: %v", err)
	}

	val, ok := decoded.Get("items")
	if !ok {
		t.Fatal("array field not found")
	}
	if val.Type != TypeArray {
		t.Errorf("expected TypeArray, got %v", val.Type)
	}

	decodedArr := val.ArrayValue()
	if len(decodedArr) != 3 {
		t.Errorf("expected 3 items, got %d", len(decodedArr))
	}
}

func TestDecode_UnknownType(t *testing.T) {
	// Create a document with an unknown type
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, uint32(0)) // placeholder for size
	buf.WriteByte(0xFF)                               // unknown type
	buf.WriteString("key")
	buf.WriteByte(0x00) // null terminator for key
	buf.WriteByte(0x00) // value
	buf.WriteByte(0x00) // document terminator

	data := buf.Bytes()
	binary.LittleEndian.PutUint32(data, uint32(len(data)))

	_, err := Decode(data)
	if err == nil {
		t.Error("expected error for unknown type")
	}
}

func TestDecode_BinaryTypes(t *testing.T) {
	subtypes := []BinarySubtype{
		BinaryGeneric,
		BinaryFunction,
		BinaryOld,
		BinaryUUIDOld,
		BinaryUUID,
		BinaryMD5,
		BinaryEncrypted,
		BinaryUser,
	}

	for _, subtype := range subtypes {
		doc := NewDocument()
		doc.Set("bin", VBinary(subtype, []byte{1, 2, 3}))

		encoded := Encode(doc)
		decoded, err := Decode(encoded)
		if err != nil {
			t.Fatalf("failed to decode binary subtype %d: %v", subtype, err)
		}

		val, _ := decoded.Get("bin")
		bin := val.Binary()
		if bin.Subtype != subtype {
			t.Errorf("subtype %d: expected %d, got %d", subtype, subtype, bin.Subtype)
		}
	}
}

func TestDecode_CodeWithScope(t *testing.T) {
	scope := NewDocument()
	scope.Set("x", VInt32(10))

	code := CodeWithScope{
		Code:  "function() { return x; }",
		Scope: scope,
	}

	doc := NewDocument()
	doc.Set("code", Value{Type: TypeCodeScope, value: code})

	encoded := Encode(doc)
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("failed to decode code with scope: %v", err)
	}

	val, _ := decoded.Get("code")
	decodedCode := val.CodeScope()
	if decodedCode.Code != code.Code {
		t.Errorf("code mismatch: expected %q, got %q", code.Code, decodedCode.Code)
	}
}

func TestDecode_TruncatedTypes(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "truncated double",
			data: []byte{0x0D, 0x00, 0x00, 0x00, 0x01, 'x', 0x00, 0x00, 0x00},
		},
		{
			name: "truncated string length",
			data: []byte{0x08, 0x00, 0x00, 0x00, 0x02, 'x', 0x00},
		},
		{
			name: "truncated objectid",
			data: []byte{0x0D, 0x00, 0x00, 0x00, 0x07, 'x', 0x00, 0x01, 0x02, 0x03},
		},
		{
			name: "truncated boolean",
			data: []byte{0x08, 0x00, 0x00, 0x00, 0x08, 'x', 0x00},
		},
		{
			name: "truncated datetime",
			data: []byte{0x0D, 0x00, 0x00, 0x00, 0x09, 'x', 0x00, 0x00},
		},
		{
			name: "truncated regex pattern",
			data: []byte{0x09, 0x00, 0x00, 0x00, 0x0B, 'x', 0x00, 'a', 'b'},
		},
		{
			name: "truncated int32",
			data: []byte{0x09, 0x00, 0x00, 0x00, 0x10, 'x', 0x00, 0x00},
		},
		{
			name: "truncated int64",
			data: []byte{0x0D, 0x00, 0x00, 0x00, 0x12, 'x', 0x00, 0x00, 0x00},
		},
		{
			name: "truncated timestamp",
			data: []byte{0x0D, 0x00, 0x00, 0x00, 0x11, 'x', 0x00, 0x00},
		},
		{
			name: "truncated decimal128",
			data: []byte{0x15, 0x00, 0x00, 0x00, 0x13, 'x', 0x00, 0x00, 0x00},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Need to add proper size and null terminator for some cases
			_, err := Decode(tc.data)
			if err == nil {
				t.Error("expected error for truncated data")
			}
		})
	}
}

func TestReadCString_Truncated(t *testing.T) {
	data := []byte{'h', 'e', 'l', 'l', 'o'} // No null terminator
	pos := 0
	_, err := readCString(data, &pos)
	if err != errInvalidDocument {
		t.Errorf("expected errInvalidDocument, got %v", err)
	}
}

func TestReadString_Truncated(t *testing.T) {
	data := []byte{0x10, 0x00, 0x00, 0x00} // Claims 16 bytes but no data
	pos := 0
	_, err := readString(data, &pos)
	if err != errBufferTooShort {
		t.Errorf("expected errBufferTooShort, got %v", err)
	}
}

func TestDecodeValue_BufferTooShort(t *testing.T) {
	tests := []struct {
		name     string
		typ      BSONType
		data     []byte
		expected error
	}{
		{
			name:     "double too short",
			typ:      TypeDouble,
			data:     []byte{0x00, 0x00, 0x00},
			expected: errBufferTooShort,
		},
		{
			name:     "string too short for length",
			typ:      TypeString,
			data:     []byte{0x00},
			expected: errBufferTooShort,
		},
		{
			name:     "objectid too short",
			typ:      TypeObjectID,
			data:     []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			expected: errBufferTooShort,
		},
		{
			name:     "boolean too short",
			typ:      TypeBoolean,
			data:     []byte{},
			expected: errBufferTooShort,
		},
		{
			name:     "datetime too short",
			typ:      TypeDateTime,
			data:     []byte{0x00, 0x00, 0x00},
			expected: errBufferTooShort,
		},
		{
			name:     "int32 too short",
			typ:      TypeInt32,
			data:     []byte{0x00, 0x00},
			expected: errBufferTooShort,
		},
		{
			name:     "int64 too short",
			typ:      TypeInt64,
			data:     []byte{0x00, 0x00, 0x00, 0x00},
			expected: errBufferTooShort,
		},
		{
			name:     "timestamp too short",
			typ:      TypeTimestamp,
			data:     []byte{0x00, 0x00, 0x00},
			expected: errBufferTooShort,
		},
		{
			name:     "decimal128 too short",
			typ:      TypeDecimal128,
			data:     []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			expected: errBufferTooShort,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pos := 0
			_, err := decodeValue(tc.data, &pos, tc.typ)
			if err != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, err)
			}
		})
	}
}

func TestDecodeInlineDocument_Truncated(t *testing.T) {
	data := []byte{0x10, 0x00, 0x00, 0x00} // Claims 16 bytes
	pos := 0
	_, err := decodeInlineDocument(data, &pos)
	if err != errBufferTooShort {
		t.Errorf("expected errBufferTooShort, got %v", err)
	}
}

func TestDecodeArray_Truncated(t *testing.T) {
	data := []byte{0x10, 0x00, 0x00, 0x00} // Claims 16 bytes
	pos := 0
	_, err := decodeArray(data, &pos)
	if err != errBufferTooShort {
		t.Errorf("expected errBufferTooShort, got %v", err)
	}
}

func TestDecodeBinary_Truncated(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "no length",
			data: []byte{},
		},
		{
			name: "no subtype",
			data: []byte{0x04, 0x00, 0x00, 0x00},
		},
		{
			name: "truncated data",
			data: []byte{0x10, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pos := 0
			_, err := decodeBinary(tc.data, &pos)
			if err != errBufferTooShort {
				t.Errorf("expected errBufferTooShort, got %v", err)
			}
		})
	}
}

func TestDecode_DBPointerTruncated(t *testing.T) {
	// DBPointer needs: 4 bytes length + string + 12 bytes ObjectID
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, uint32(5)) // length including null
	buf.WriteString("ns")
	buf.WriteByte(0x00)
	buf.Write([]byte{0x01, 0x02, 0x03}) // Only 3 bytes of ObjectID

	data := buf.Bytes()
	pos := 0
	_, err := decodeValue(data, &pos, TypeDBPointer)
	if err != errBufferTooShort {
		t.Errorf("expected errBufferTooShort, got %v", err)
	}
}

func TestDecode_CodeWithScopeTruncated(t *testing.T) {
	// CodeWithScope needs: 4 bytes total length + string + document
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, uint32(20)) // Claims 20 bytes
	binary.Write(buf, binary.LittleEndian, uint32(5))  // Code length
	buf.WriteString("x")
	buf.WriteByte(0x00)
	// Missing scope document

	data := buf.Bytes()
	pos := 0
	_, err := decodeValue(data, &pos, TypeCodeScope)
	if err == nil {
		t.Error("expected error for truncated code with scope")
	}
}

func TestFloat64fromBits(t *testing.T) {
	original := 3.14159
	bits := math.Float64bits(original)
	result := float64fromBits(bits)
	if result != original {
		t.Errorf("expected %v, got %v", original, result)
	}
}

func TestDecode_InvalidPosition(t *testing.T) {
	// Test when pos goes beyond data length during key parsing
	data := []byte{
		0x08, 0x00, 0x00, 0x00, // size = 8
		0x02,                   // type = string
		// No key, will hit end of buffer
	}

	_, err := Decode(data)
	if err == nil {
		t.Error("expected error when position exceeds buffer")
	}
}
