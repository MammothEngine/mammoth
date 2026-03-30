package bson

import "testing"

func TestRawDocumentLookup(t *testing.T) {
	doc := NewDocument()
	doc.Set("name", VString("test"))
	doc.Set("age", VInt32(42))
	doc.Set("active", VBool(true))

	data := Encode(doc)
	raw := RawDocument(data)

	// Lookup existing key
	v, ok, err := raw.Lookup("name")
	if err != nil || !ok {
		t.Fatalf("lookup name: ok=%v, err=%v", ok, err)
	}
	if v.String() != "test" {
		t.Fatalf("name mismatch: %v", v)
	}

	// Lookup another key
	v, ok, err = raw.Lookup("age")
	if err != nil || !ok {
		t.Fatalf("lookup age: ok=%v, err=%v", ok, err)
	}
	if v.Int32() != 42 {
		t.Fatalf("age mismatch: %v", v)
	}

	// Lookup missing key
	_, ok, err = raw.Lookup("missing")
	if err != nil || ok {
		t.Fatalf("lookup missing: ok=%v, err=%v", ok, err)
	}
}

func TestRawDocumentValues(t *testing.T) {
	doc := NewDocument()
	doc.Set("a", VInt32(1))
	doc.Set("b", VString("hello"))

	data := Encode(doc)
	raw := RawDocument(data)

	elems := raw.Values()
	if len(elems) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(elems))
	}
	if elems[0].Key != "a" || elems[0].Type != TypeInt32 {
		t.Fatalf("first element mismatch: %v", elems[0])
	}
	if elems[1].Key != "b" || elems[1].Type != TypeString {
		t.Fatalf("second element mismatch: %v", elems[1])
	}
}

func TestRawDocumentLength(t *testing.T) {
	doc := NewDocument()
	doc.Set("x", VInt32(1))

	data := Encode(doc)
	raw := RawDocument(data)

	if raw.Length() != len(data) {
		t.Fatalf("length mismatch: %d vs %d", raw.Length(), len(data))
	}
}

func TestRawDocumentEmpty(t *testing.T) {
	raw := RawDocument([]byte{})
	if raw.Length() != 0 {
		t.Fatalf("expected 0 length")
	}
	_, ok, _ := raw.Lookup("anything")
	if ok {
		t.Fatal("expected no results for empty raw doc")
	}
}
