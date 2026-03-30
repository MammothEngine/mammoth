package bson

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestEncode_EmptyDocument(t *testing.T) {
	doc := NewDocument()
	data := Encode(doc)

	// An empty BSON document is 4 bytes (size) + 1 byte (null terminator) = 5
	if len(data) != 5 {
		t.Fatalf("expected 5 bytes for empty doc, got %d", len(data))
	}

	// First 4 bytes are the document size in little-endian
	size := int(binary.LittleEndian.Uint32(data))
	if size != 5 {
		t.Fatalf("expected size field=5, got %d", size)
	}

	// Last byte must be null terminator
	if data[4] != 0x00 {
		t.Fatalf("expected null terminator, got 0x%02x", data[4])
	}

	// Decode should round-trip cleanly
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("decode empty doc: %v", err)
	}
	if decoded.Len() != 0 {
		t.Fatalf("expected 0 elements, got %d", decoded.Len())
	}
}

func TestEncode_SizeAccuracy(t *testing.T) {
	doc := D(
		"name", VString("test"),
		"count", VInt32(99),
	)
	data := Encode(doc)

	// The size encoded in the first 4 bytes must equal len(data)
	size := int(binary.LittleEndian.Uint32(data))
	if size != len(data) {
		t.Fatalf("size field %d != actual length %d", size, len(data))
	}

	// Re-encode and verify identical size
	data2 := Encode(doc)
	size2 := int(binary.LittleEndian.Uint32(data2))
	if size2 != len(data2) {
		t.Fatalf("re-encoded size field %d != actual length %d", size2, len(data2))
	}
	if size != size2 {
		t.Fatalf("encode is not deterministic: %d vs %d", size, size2)
	}
}

func TestEncode_NestedDocuments(t *testing.T) {
	// Build a 3-level nested structure: outer -> mid -> inner
	inner := D("leaf", VString("value"))
	mid := D("child", VDoc(inner))
	outer := D("root", VDoc(mid))

	data := Encode(outer)
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	// Navigate all three levels
	v, ok := decoded.Get("root")
	if !ok || v.Type != TypeDocument {
		t.Fatal("missing root")
	}
	rootDoc := v.DocumentValue()

	v, ok = rootDoc.Get("child")
	if !ok || v.Type != TypeDocument {
		t.Fatal("missing child")
	}
	midDoc := v.DocumentValue()

	v, ok = midDoc.Get("leaf")
	if !ok || v.String() != "value" {
		t.Fatalf("leaf mismatch: %v", v)
	}
}

func TestEncode_ArrayEncoding(t *testing.T) {
	// Array with mixed types: int32, string, bool, null, double
	arr := A(VInt32(1), VString("two"), VBool(true), VNull(), VDouble(5.5))
	doc := D("items", VArray(arr))

	data := Encode(doc)
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	v, ok := decoded.Get("items")
	if !ok || v.Type != TypeArray {
		t.Fatal("missing items array")
	}
	got := v.ArrayValue()
	if len(got) != 5 {
		t.Fatalf("expected 5 array elements, got %d", len(got))
	}
	if got[0].Int32() != 1 {
		t.Fatalf("arr[0] mismatch: %v", got[0])
	}
	if got[1].String() != "two" {
		t.Fatalf("arr[1] mismatch: %v", got[1])
	}
	if !got[2].Boolean() {
		t.Fatalf("arr[2] mismatch: %v", got[2])
	}
	if !got[3].IsNull() {
		t.Fatalf("arr[3] mismatch: expected null")
	}
	if got[4].Double() != 5.5 {
		t.Fatalf("arr[4] mismatch: %v", got[4])
	}
}

func TestEncode_AllTypes(t *testing.T) {
	var oid ObjectID
	copy(oid[:], []byte("ABCDEFGHIJKL"))

	var dec128 [16]byte
	copy(dec128[:], []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10})

	doc := D(
		"double", VDouble(3.14),
		"string", VString("hello"),
		"doc", VDoc(D("inner", VInt32(1))),
		"array", VArray(A(VString("a"), VString("b"))),
		"binary", VBinary(BinaryUUID, []byte{0xDE, 0xAD, 0xBE, 0xEF}),
		"oid", VObjectID(oid),
		"bool_true", VBool(true),
		"bool_false", VBool(false),
		"datetime", VDateTime(1609459200000),
		"null", VNull(),
		"regex", VRegex("abc", "im"),
		"js", VJavaScript("var x = 1"),
		"symbol", VSymbol("sym"),
		"int32", VInt32(42),
		"timestamp", VTimestamp(12345),
		"int64", VInt64(9876543210),
		"decimal", Value{Type: TypeDecimal128, value: dec128},
		"minkey", VMinKey(),
		"maxkey", VMaxKey(),
	)

	data := Encode(doc)
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	assertDouble := func(key string, want float64) {
		v, ok := decoded.Get(key)
		if !ok || v.Double() != want {
			t.Fatalf("%s: expected %f, got %v", key, want, v)
		}
	}
	assertString := func(key string, want string) {
		v, ok := decoded.Get(key)
		if !ok || v.String() != want {
			t.Fatalf("%s: expected %q, got %v", key, want, v)
		}
	}

	assertDouble("double", 3.14)
	assertString("string", "hello")

	v, _ := decoded.Get("doc")
	inner := v.DocumentValue()
	if iv, _ := inner.Get("inner"); iv.Int32() != 1 {
		t.Fatalf("doc.inner mismatch")
	}

	v, _ = decoded.Get("array")
	arr := v.ArrayValue()
	if len(arr) != 2 || arr[0].String() != "a" || arr[1].String() != "b" {
		t.Fatalf("array mismatch: %v", arr)
	}

	v, _ = decoded.Get("binary")
	b := v.Binary()
	if b.Subtype != BinaryUUID || !bytes.Equal(b.Data, []byte{0xDE, 0xAD, 0xBE, 0xEF}) {
		t.Fatalf("binary mismatch")
	}

	v, _ = decoded.Get("oid")
	gotOID := v.ObjectID()
	if !bytes.Equal(gotOID[:], oid[:]) {
		t.Fatalf("oid mismatch")
	}

	if v, _ := decoded.Get("bool_true"); !v.Boolean() {
		t.Fatal("bool_true mismatch")
	}
	if v, _ := decoded.Get("bool_false"); v.Boolean() {
		t.Fatal("bool_false mismatch")
	}

	if v, _ := decoded.Get("datetime"); v.DateTime() != 1609459200000 {
		t.Fatalf("datetime mismatch")
	}
	if v, _ := decoded.Get("null"); !v.IsNull() {
		t.Fatal("null mismatch")
	}

	v, _ = decoded.Get("regex")
	r := v.Regex()
	if r.Pattern != "abc" || r.Options != "im" {
		t.Fatalf("regex mismatch: %v", r)
	}

	if v, _ := decoded.Get("js"); v.JavaScriptCode() != "var x = 1" {
		t.Fatalf("js mismatch: %q", v.JavaScriptCode())
	}
	if v, _ := decoded.Get("symbol"); v.Symbol() != "sym" {
		t.Fatalf("symbol mismatch: %q", v.Symbol())
	}

	if v, _ := decoded.Get("int32"); v.Int32() != 42 {
		t.Fatalf("int32 mismatch")
	}
	if v, _ := decoded.Get("timestamp"); v.Timestamp() != 12345 {
		t.Fatalf("timestamp mismatch")
	}
	if v, _ := decoded.Get("int64"); v.Int64() != 9876543210 {
		t.Fatalf("int64 mismatch")
	}

	v, _ = decoded.Get("decimal")
	gotDec := v.Interface().([16]byte)
	if !bytes.Equal(gotDec[:], dec128[:]) {
		t.Fatalf("decimal128 mismatch")
	}

	if v, _ := decoded.Get("minkey"); v.Type != TypeMinKey {
		t.Fatal("minkey type mismatch")
	}
	if v, _ := decoded.Get("maxkey"); v.Type != TypeMaxKey {
		t.Fatal("maxkey type mismatch")
	}
}
