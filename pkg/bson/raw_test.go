package bson

import (
	"testing"
)

func TestRawDocument_Lookup(t *testing.T) {
	doc := NewDocument()
	doc.Set("name", VString("test"))
	doc.Set("age", VInt32(42))
	doc.Set("active", VBool(true))

	data := Encode(doc)
	raw := RawDocument(data)

	v, ok, err := raw.Lookup("name")
	if err != nil || !ok {
		t.Fatalf("lookup name: ok=%v err=%v", ok, err)
	}
	if v.String() != "test" {
		t.Fatalf("name mismatch: got %q", v.String())
	}

	v, ok, err = raw.Lookup("age")
	if err != nil || !ok {
		t.Fatalf("lookup age: ok=%v err=%v", ok, err)
	}
	if v.Int32() != 42 {
		t.Fatalf("age mismatch: got %d", v.Int32())
	}

	v, ok, err = raw.Lookup("active")
	if err != nil || !ok {
		t.Fatalf("lookup active: ok=%v err=%v", ok, err)
	}
	if !v.Boolean() {
		t.Fatal("active should be true")
	}
}

func TestRawDocument_Values(t *testing.T) {
	doc := NewDocument()
	doc.Set("a", VInt32(1))
	doc.Set("b", VString("hello"))
	doc.Set("c", VBool(false))

	raw := RawDocument(Encode(doc))
	elems := raw.Values()

	if len(elems) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(elems))
	}
	if elems[0].Key != "a" || elems[0].Type != TypeInt32 {
		t.Fatalf("element 0 mismatch: key=%q type=%v", elems[0].Key, elems[0].Type)
	}
	if elems[1].Key != "b" || elems[1].Type != TypeString {
		t.Fatalf("element 1 mismatch: key=%q type=%v", elems[1].Key, elems[1].Type)
	}
	if elems[2].Key != "c" || elems[2].Type != TypeBoolean {
		t.Fatalf("element 2 mismatch: key=%q type=%v", elems[2].Key, elems[2].Type)
	}
}

func TestRawDocument_Length(t *testing.T) {
	doc := NewDocument()
	doc.Set("x", VInt32(1))

	data := Encode(doc)
	raw := RawDocument(data)

	if raw.Length() != len(data) {
		t.Fatalf("length mismatch: %d vs %d", raw.Length(), len(data))
	}

	// Empty/nil raw document should return 0 length.
	if RawDocument(nil).Length() != 0 {
		t.Fatal("nil RawDocument should have length 0")
	}
	if RawDocument([]byte{0x01, 0x00}).Length() != 0 {
		t.Fatal("truncated RawDocument (< 4 bytes) should have length 0")
	}
}

func TestRawDocument_LookupNotFound(t *testing.T) {
	doc := NewDocument()
	doc.Set("exists", VInt32(10))

	raw := RawDocument(Encode(doc))

	_, ok, err := raw.Lookup("missing")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("expected ok=false for missing key")
	}
}

func TestRawDocument_DecodeRawRoundtrip(t *testing.T) {
	// Build a document with multiple types, encode it, then access via RawDocument.
	inner := NewDocument()
	inner.Set("val", VDouble(3.14))

	doc := NewDocument()
	doc.Set("greeting", VString("hello"))
	doc.Set("count", VInt64(999))
	doc.Set("inner", VDoc(inner))
	doc.Set("flag", VBool(true))
	doc.Set("nothing", VNull())

	data := Encode(doc)
	raw := RawDocument(data)

	// Verify each field through Lookup.
	v, ok, err := raw.Lookup("greeting")
	if err != nil || !ok || v.String() != "hello" {
		t.Fatalf("greeting: ok=%v err=%v val=%q", ok, err, v.String())
	}

	v, ok, err = raw.Lookup("count")
	if err != nil || !ok || v.Int64() != 999 {
		t.Fatalf("count: ok=%v err=%v val=%d", ok, err, v.Int64())
	}

	v, ok, err = raw.Lookup("flag")
	if err != nil || !ok || !v.Boolean() {
		t.Fatalf("flag: ok=%v err=%v val=%v", ok, err, v.Boolean())
	}

	v, ok, err = raw.Lookup("nothing")
	if err != nil || !ok || !v.IsNull() {
		t.Fatalf("nothing: ok=%v err=%v type=%v", ok, err, v.Type)
	}

	v, ok, err = raw.Lookup("inner")
	if err != nil || !ok {
		t.Fatalf("inner: ok=%v err=%v", ok, err)
	}
	sub := v.DocumentValue()
	if sub == nil {
		t.Fatal("inner document is nil")
	}
	sv, sok := sub.Get("val")
	if !sok || sv.Double() != 3.14 {
		t.Fatalf("inner.val: ok=%v val=%v", sok, sv.Double())
	}

	// Also verify all elements appear via Values.
	elems := raw.Values()
	if len(elems) != 5 {
		t.Fatalf("expected 5 elements, got %d", len(elems))
	}
}

func TestRawDocument_InvalidData(t *testing.T) {
	// Truncated: too short for any element.
	raw := RawDocument([]byte{})
	_, ok, _ := raw.Lookup("x")
	if ok {
		t.Fatal("empty raw should not find key")
	}
	if raw.Values() != nil {
		t.Fatal("empty raw Values should be nil")
	}

	// Declared size exceeds actual buffer.
	raw = RawDocument([]byte{0xFF, 0x00, 0x00, 0x00, 0x00})
	_, ok, _ = raw.Lookup("x")
	if ok {
		t.Fatal("oversized raw should not find key")
	}

	// Valid header but truncated mid-element.
	raw = RawDocument([]byte{0x0A, 0x00, 0x00, 0x00, 0x02, 'k', 0x00})
	_, ok, _ = raw.Lookup("k")
	if ok {
		t.Fatal("truncated element should not be found")
	}
}
