package bson

import (
	"bytes"
	"testing"
)

func TestDocumentCRUD(t *testing.T) {
	doc := NewDocument()

	// Set and Get
	doc.Set("name", VString("test"))
	v, ok := doc.Get("name")
	if !ok || v.String() != "test" {
		t.Fatalf("expected name=test, got %v, ok=%v", v, ok)
	}

	// Update
	doc.Set("name", VString("updated"))
	v, ok = doc.Get("name")
	if !ok || v.String() != "updated" {
		t.Fatalf("expected name=updated")
	}

	// Has
	if !doc.Has("name") {
		t.Fatal("expected Has(name)=true")
	}
	if doc.Has("missing") {
		t.Fatal("expected Has(missing)=false")
	}

	// Len
	if doc.Len() != 1 {
		t.Fatalf("expected Len=1, got %d", doc.Len())
	}

	// Delete
	doc.Delete("name")
	if doc.Has("name") {
		t.Fatal("expected name deleted")
	}
	if doc.Len() != 0 {
		t.Fatalf("expected Len=0, got %d", doc.Len())
	}

	// Multiple keys preserve order
	doc.Set("b", VInt32(2))
	doc.Set("a", VInt32(1))
	doc.Set("c", VInt32(3))
	keys := doc.Keys()
	if len(keys) != 3 || keys[0] != "b" || keys[1] != "a" || keys[2] != "c" {
		t.Fatalf("expected insertion order [b,a,c], got %v", keys)
	}
}

func TestArrayOperations(t *testing.T) {
	arr := A(VInt32(1), VInt32(2), VInt32(3))
	if len(arr) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(arr))
	}
	if arr[0].Int32() != 1 || arr[1].Int32() != 2 || arr[2].Int32() != 3 {
		t.Fatalf("unexpected array values: %v", arr)
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	doc := NewDocument()
	doc.Set("double", VDouble(3.14))
	doc.Set("string", VString("hello"))
	doc.Set("int32", VInt32(42))
	doc.Set("int64", VInt64(12345678901234))
	doc.Set("bool", VBool(true))
	doc.Set("null", VNull())
	doc.Set("oid", VObjectID(NewObjectID()))

	data := Encode(doc)
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	// Verify
	if v, _ := decoded.Get("double"); v.Double() != 3.14 {
		t.Fatalf("double mismatch: %v", v.Double())
	}
	if v, _ := decoded.Get("string"); v.String() != "hello" {
		t.Fatalf("string mismatch: %v", v.String())
	}
	if v, _ := decoded.Get("int32"); v.Int32() != 42 {
		t.Fatalf("int32 mismatch: %v", v.Int32())
	}
	if v, _ := decoded.Get("int64"); v.Int64() != 12345678901234 {
		t.Fatalf("int64 mismatch: %v", v.Int64())
	}
	if v, _ := decoded.Get("bool"); !v.Boolean() {
		t.Fatalf("bool mismatch")
	}
	if v, _ := decoded.Get("null"); !v.IsNull() {
		t.Fatalf("null mismatch")
	}
}

func TestEncodeDecodeNestedDocument(t *testing.T) {
	inner := NewDocument()
	inner.Set("x", VInt32(1))

	doc := NewDocument()
	doc.Set("nested", VDoc(inner))
	doc.Set("name", VString("test"))

	data := Encode(doc)
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	v, ok := decoded.Get("nested")
	if !ok || v.Type != TypeDocument {
		t.Fatal("nested doc not found")
	}
	nested := v.DocumentValue()
	if iv, _ := nested.Get("x"); iv.Int32() != 1 {
		t.Fatalf("nested.x mismatch: %v", iv)
	}
}

func TestEncodeDecodeArray(t *testing.T) {
	doc := NewDocument()
	doc.Set("arr", VArray(A(VInt32(10), VInt32(20), VInt32(30))))

	data := Encode(doc)
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	v, ok := decoded.Get("arr")
	if !ok || v.Type != TypeArray {
		t.Fatal("array not found")
	}
	arr := v.ArrayValue()
	if len(arr) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(arr))
	}
	if arr[0].Int32() != 10 || arr[1].Int32() != 20 || arr[2].Int32() != 30 {
		t.Fatalf("array values mismatch: %v", arr)
	}
}

func TestEncodeDecodeBinary(t *testing.T) {
	doc := NewDocument()
	doc.Set("bin", VBinary(BinaryGeneric, []byte{1, 2, 3, 4, 5}))

	data := Encode(doc)
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	v, _ := decoded.Get("bin")
	b := v.Binary()
	if b.Subtype != BinaryGeneric || !bytes.Equal(b.Data, []byte{1, 2, 3, 4, 5}) {
		t.Fatalf("binary mismatch: subtype=%d data=%v", b.Subtype, b.Data)
	}
}

func TestEncodeDecodeDateTime(t *testing.T) {
	doc := NewDocument()
	doc.Set("ts", VDateTime(1609459200000)) // 2021-01-01

	data := Encode(doc)
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	v, _ := decoded.Get("ts")
	if v.DateTime() != 1609459200000 {
		t.Fatalf("datetime mismatch: %v", v.DateTime())
	}
}

func TestEncodeDecodeRegex(t *testing.T) {
	doc := NewDocument()
	doc.Set("re", VRegex("pattern", "i"))

	data := Encode(doc)
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	v, _ := decoded.Get("re")
	r := v.Regex()
	if r.Pattern != "pattern" || r.Options != "i" {
		t.Fatalf("regex mismatch: %v", r)
	}
}

func TestDecodeMalformed(t *testing.T) {
	// Too short
	_, err := Decode([]byte{1, 2, 3})
	if err == nil {
		t.Fatal("expected error for short buffer")
	}

	// Wrong size
	_, err = Decode([]byte{100, 0, 0, 0, 0})
	if err == nil {
		t.Fatal("expected error for wrong size")
	}
}

func TestConstructors(t *testing.T) {
	d := D("a", VInt32(1), "b", VString("hello"))
	if d.Len() != 2 {
		t.Fatalf("expected 2, got %d", d.Len())
	}

	m := M(map[string]Value{"x": VInt32(42)})
	if m.Len() != 1 {
		t.Fatalf("expected 1, got %d", m.Len())
	}

	arr := A(VInt32(1), VInt32(2))
	if len(arr) != 2 {
		t.Fatalf("expected 2, got %d", len(arr))
	}
}
