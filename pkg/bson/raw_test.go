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

// TestSkipValue_AllTypes comprehensively tests skipValue for all BSON types
func TestSkipValue_AllTypes(t *testing.T) {
	tests := []struct {
		name     string
		makeDoc  func() *Document
		key      string
		valType  BSONType
	}{
		{
			name: "double",
			makeDoc: func() *Document {
				d := NewDocument()
				d.Set("v", VDouble(3.14))
				return d
			},
			key: "v", valType: TypeDouble,
		},
		{
			name: "string",
			makeDoc: func() *Document {
				d := NewDocument()
				d.Set("v", VString("hello world"))
				return d
			},
			key: "v", valType: TypeString,
		},
		{
			name: "document",
			makeDoc: func() *Document {
				d := NewDocument()
				inner := NewDocument()
				inner.Set("x", VInt32(1))
				d.Set("v", VDoc(inner))
				return d
			},
			key: "v", valType: TypeDocument,
		},
		{
			name: "array",
			makeDoc: func() *Document {
				d := NewDocument()
				d.Set("v", VArray(A(VInt32(1), VInt32(2))))
				return d
			},
			key: "v", valType: TypeArray,
		},
		{
			name: "binary",
			makeDoc: func() *Document {
				d := NewDocument()
				d.Set("v", VBinary(BinaryGeneric, []byte{1, 2, 3, 4, 5}))
				return d
			},
			key: "v", valType: TypeBinary,
		},
		{
			name: "undefined",
			makeDoc: func() *Document {
				d := NewDocument()
				d.Set("a", VInt32(1))
				d.Set("v", Value{Type: TypeUndefined, value: nil})
				d.Set("b", VInt32(2))
				return d
			},
			key: "v", valType: TypeUndefined,
		},
		{
			name: "objectid",
			makeDoc: func() *Document {
				d := NewDocument()
				d.Set("v", VObjectID(NewObjectID()))
				return d
			},
			key: "v", valType: TypeObjectID,
		},
		{
			name: "boolean_true",
			makeDoc: func() *Document {
				d := NewDocument()
				d.Set("v", VBool(true))
				return d
			},
			key: "v", valType: TypeBoolean,
		},
		{
			name: "boolean_false",
			makeDoc: func() *Document {
				d := NewDocument()
				d.Set("v", VBool(false))
				return d
			},
			key: "v", valType: TypeBoolean,
		},
		{
			name: "datetime",
			makeDoc: func() *Document {
				d := NewDocument()
				d.Set("v", VDateTime(1234567890000))
				return d
			},
			key: "v", valType: TypeDateTime,
		},
		{
			name: "null",
			makeDoc: func() *Document {
				d := NewDocument()
				d.Set("a", VInt32(1))
				d.Set("v", VNull())
				d.Set("b", VInt32(2))
				return d
			},
			key: "v", valType: TypeNull,
		},
		{
			name: "regex",
			makeDoc: func() *Document {
				d := NewDocument()
				d.Set("v", VRegex("pattern.*", "im"))
				return d
			},
			key: "v", valType: TypeRegex,
		},
		{
			name: "dbpointer",
			makeDoc: func() *Document {
				d := NewDocument()
				d.Set("v", Value{Type: TypeDBPointer, value: DBPointer{Namespace: "test.ns", ID: NewObjectID()}})
				return d
			},
			key: "v", valType: TypeDBPointer,
		},
		{
			name: "javascript",
			makeDoc: func() *Document {
				d := NewDocument()
				d.Set("v", VJavaScript("function() { return 1; }"))
				return d
			},
			key: "v", valType: TypeJavaScript,
		},
		{
			name: "symbol",
			makeDoc: func() *Document {
				d := NewDocument()
				d.Set("v", VSymbol("symbol_value"))
				return d
			},
			key: "v", valType: TypeSymbol,
		},
		{
			name: "code_with_scope",
			makeDoc: func() *Document {
				d := NewDocument()
				scope := NewDocument()
				scope.Set("x", VInt32(10))
				d.Set("v", Value{Type: TypeCodeScope, value: CodeWithScope{Code: "function() {}", Scope: scope}})
				return d
			},
			key: "v", valType: TypeCodeScope,
		},
		{
			name: "int32",
			makeDoc: func() *Document {
				d := NewDocument()
				d.Set("v", VInt32(42))
				return d
			},
			key: "v", valType: TypeInt32,
		},
		{
			name: "timestamp",
			makeDoc: func() *Document {
				d := NewDocument()
				d.Set("v", VTimestamp(1234567890))
				return d
			},
			key: "v", valType: TypeTimestamp,
		},
		{
			name: "int64",
			makeDoc: func() *Document {
				d := NewDocument()
				d.Set("v", VInt64(9876543210))
				return d
			},
			key: "v", valType: TypeInt64,
		},
		{
			name: "minkey",
			makeDoc: func() *Document {
				d := NewDocument()
				d.Set("a", VInt32(1))
				d.Set("v", VMinKey())
				d.Set("b", VInt32(2))
				return d
			},
			key: "v", valType: TypeMinKey,
		},
		{
			name: "maxkey",
			makeDoc: func() *Document {
				d := NewDocument()
				d.Set("a", VInt32(1))
				d.Set("v", VMaxKey())
				d.Set("b", VInt32(2))
				return d
			},
			key: "v", valType: TypeMaxKey,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			doc := tc.makeDoc()
			data := Encode(doc)
			raw := RawDocument(data)

			// This will call skipValue internally for the types we're not retrieving
			val, found, err := raw.Lookup(tc.key)
			if !found {
				t.Fatalf("key %s not found", tc.key)
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if val.Type != tc.valType {
				t.Errorf("expected type %v, got %v", tc.valType, val.Type)
			}
		})
	}
}

// TestSkipValue_TruncatedCases tests skipValue with truncated data
func TestSkipValue_TruncatedCases(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		typ  BSONType
	}{
		{"string_no_length", []byte{}, TypeString},
		{"string_short_length", []byte{0x10, 0x00}, TypeString},
		{"doc_no_size", []byte{}, TypeDocument},
		{"doc_short_size", []byte{0x10, 0x00}, TypeDocument},
		{"array_no_size", []byte{}, TypeArray},
		{"array_short_size", []byte{0x10, 0x00}, TypeArray},
		{"binary_no_length", []byte{}, TypeBinary},
		{"dbpointer_no_length", []byte{}, TypeDBPointer},
		{"javascript_no_length", []byte{}, TypeJavaScript},
		{"symbol_no_length", []byte{}, TypeSymbol},
		{"codewscope_no_length", []byte{}, TypeCodeScope},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pos := 0
			err := skipValue(tc.data, &pos, tc.typ)
			if err == nil {
				t.Error("expected error for truncated data")
			}
		})
	}
}

// TestSkipValue_UnknownType tests skipValue with an unknown type
func TestSkipValue_UnknownType(t *testing.T) {
	// Use a type that's not defined (e.g., 0x20 which is between defined types)
	data := []byte{0x01, 0x02, 0x03}
	pos := 0
	err := skipValue(data, &pos, 0x20)
	if err != errInvalidDocument {
		t.Errorf("expected errInvalidDocument, got %v", err)
	}
}

// TestRawDocument_SkipThroughElements tests that Values() properly skips through various types
func TestRawDocument_SkipThroughElements(t *testing.T) {
	doc := NewDocument()
	doc.Set("int1", VInt32(1))
	doc.Set("str", VString("hello"))
	doc.Set("dbl", VDouble(3.14))
	doc.Set("doc", VDoc(NewDocument()))
	doc.Set("arr", VArray(A(VInt32(1))))
	doc.Set("bin", VBinary(BinaryGeneric, []byte{1, 2, 3}))
	doc.Set("oid", VObjectID(NewObjectID()))
	doc.Set("bool", VBool(true))
	doc.Set("date", VDateTime(12345))
	doc.Set("null", VNull())
	doc.Set("regex", VRegex("p", "i"))
	doc.Set("js", VJavaScript("code"))
	doc.Set("sym", VSymbol("sym"))
	doc.Set("i64", VInt64(64))
	doc.Set("ts", VTimestamp(100))
	doc.Set("i32", VInt32(32))
	doc.Set("min", VMinKey())
	doc.Set("max", VMaxKey())

	data := Encode(doc)
	raw := RawDocument(data)

	elems := raw.Values()
	if len(elems) != 18 {
		t.Errorf("expected 18 elements, got %d", len(elems))
	}

	// Verify we can access them all by key
	keys := []string{"int1", "str", "dbl", "doc", "arr", "bin", "oid", "bool", "date",
		"null", "regex", "js", "sym", "i64", "ts", "i32", "min", "max"}
	for _, key := range keys {
		_, found, err := raw.Lookup(key)
		if err != nil {
			t.Errorf("error looking up %s: %v", key, err)
		}
		if !found {
			t.Errorf("key %s not found", key)
		}
	}
}
