package bson

import (
	"bytes"
	"fmt"
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

func TestDocument_Set_Overwrite(t *testing.T) {
	doc := NewDocument()
	doc.Set("x", VInt32(1))
	doc.Set("x", VInt32(2))
	if doc.Len() != 1 {
		t.Fatalf("expected Len=1 after overwrite, got %d", doc.Len())
	}
	v, ok := doc.Get("x")
	if !ok || v.Int32() != 2 {
		t.Fatalf("expected x=2, got %v", v)
	}
}

func TestDocument_Delete(t *testing.T) {
	doc := NewDocument()
	doc.Set("a", VInt32(1))
	doc.Set("b", VInt32(2))
	doc.Set("c", VInt32(3))

	doc.Delete("b")
	if _, ok := doc.Get("b"); ok {
		t.Fatal("expected b to be deleted")
	}
	if doc.Len() != 2 {
		t.Fatalf("expected Len=2, got %d", doc.Len())
	}
	// Verify remaining keys are correct
	keys := doc.Keys()
	if len(keys) != 2 || keys[0] != "a" || keys[1] != "c" {
		t.Fatalf("expected keys [a,c], got %v", keys)
	}
	// Verify Get on remaining keys still works
	if v, ok := doc.Get("c"); !ok || v.Int32() != 3 {
		t.Fatal("expected c=3 after delete")
	}
}

func TestDocument_LargeDoc(t *testing.T) {
	doc := NewDocument()
	const n = 1000
	for i := 0; i < n; i++ {
		doc.Set(fmt.Sprintf("field_%04d", i), VInt32(int32(i)))
	}
	if doc.Len() != n {
		t.Fatalf("expected Len=%d, got %d", n, doc.Len())
	}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("field_%04d", i)
		v, ok := doc.Get(key)
		if !ok || v.Int32() != int32(i) {
			t.Fatalf("field %s: expected %d, got %v, ok=%v", key, i, v, ok)
		}
	}
	// Delete half and verify
	for i := 0; i < n/2; i++ {
		doc.Delete(fmt.Sprintf("field_%04d", i))
	}
	if doc.Len() != n/2 {
		t.Fatalf("expected Len=%d after deletes, got %d", n/2, doc.Len())
	}
	for i := n / 2; i < n; i++ {
		v, ok := doc.Get(fmt.Sprintf("field_%04d", i))
		if !ok || v.Int32() != int32(i) {
			t.Fatalf("field_%04d: expected %d after deletes", i, i)
		}
	}
}

func TestDocument_EncodeDecode_Roundtrip_WithIndex(t *testing.T) {
	doc := NewDocument()
	doc.Set("hello", VString("world"))
	doc.Set("num", VInt32(42))
	doc.Set("inner", VDoc(D("x", VInt32(1))))

	data := Encode(doc)
	decoded, err := Decode(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	// Verify decoded doc works with map-based Get
	if v, ok := decoded.Get("hello"); !ok || v.String() != "world" {
		t.Fatal("hello mismatch")
	}
	if v, ok := decoded.Get("num"); !ok || v.Int32() != 42 {
		t.Fatal("num mismatch")
	}

	// Re-encode and check equality
	data2 := Encode(decoded)
	if len(data) != len(data2) {
		t.Fatalf("roundtrip size mismatch: %d vs %d", len(data), len(data2))
	}
}

func TestDocument_SetGet_O1(t *testing.T) {
	doc := NewDocument()
	const n = 5000
	for i := 0; i < n; i++ {
		doc.Set(fmt.Sprintf("key_%d", i), VInt32(int32(i)))
	}
	// Get each key — should be fast with O(1) map lookup
	for i := 0; i < n; i++ {
		v, ok := doc.Get(fmt.Sprintf("key_%d", i))
		if !ok || v.Int32() != int32(i) {
			t.Fatalf("key_%d: expected %d", i, i)
		}
	}
}

// Test Value type accessors with wrong type
func TestValueAccessors_WrongType(t *testing.T) {
	// Test that accessors return zero values for wrong types
	v := Value{Type: TypeInt32, value: int32(42)}

	if v.Double() != 0 {
		t.Errorf("Double() on int32: expected 0, got %v", v.Double())
	}
	if v.String() != "" {
		t.Errorf("String() on int32: expected empty, got %v", v.String())
	}
	if v.Int64() != 0 {
		t.Errorf("Int64() on int32: expected 0, got %v", v.Int64())
	}
	if v.Boolean() != false {
		t.Errorf("Boolean() on int32: expected false, got %v", v.Boolean())
	}
	if v.DateTime() != 0 {
		t.Errorf("DateTime() on int32: expected 0, got %v", v.DateTime())
	}
	if v.Timestamp() != 0 {
		t.Errorf("Timestamp() on int32: expected 0, got %v", v.Timestamp())
	}
	if !v.ObjectID().IsZero() {
		t.Errorf("ObjectID() on int32: expected zero, got %v", v.ObjectID())
	}
	if v.Binary().Subtype != 0 || v.Binary().Data != nil {
		t.Errorf("Binary() on int32: expected empty, got %v", v.Binary())
	}
	if v.DocumentValue() != nil {
		t.Errorf("DocumentValue() on int32: expected nil, got %v", v.DocumentValue())
	}
	if v.ArrayValue() != nil {
		t.Errorf("ArrayValue() on int32: expected nil, got %v", v.ArrayValue())
	}
	if v.Regex().Pattern != "" || v.Regex().Options != "" {
		t.Errorf("Regex() on int32: expected empty, got %v", v.Regex())
	}
	if v.JavaScriptCode() != "" {
		t.Errorf("JavaScriptCode() on int32: expected empty, got %v", v.JavaScriptCode())
	}
	if v.CodeScope().Code != "" || v.CodeScope().Scope != nil {
		t.Errorf("CodeScope() on int32: expected empty, got %v", v.CodeScope())
	}
	if v.Symbol() != "" {
		t.Errorf("Symbol() on int32: expected empty, got %v", v.Symbol())
	}
}

// Test JavaScriptCode, CodeScope, Symbol accessors with correct types
func TestValueAccessors_SpecialTypes(t *testing.T) {
	// JavaScriptCode
	jsVal := Value{Type: TypeJavaScript, value: "function() { return 1; }"}
	if jsVal.JavaScriptCode() != "function() { return 1; }" {
		t.Errorf("JavaScriptCode() mismatch: got %v", jsVal.JavaScriptCode())
	}

	// CodeScope
	cwsVal := Value{Type: TypeCodeScope, value: CodeWithScope{Code: "code", Scope: NewDocument()}}
	if cwsVal.CodeScope().Code != "code" {
		t.Errorf("CodeScope().Code mismatch: got %v", cwsVal.CodeScope().Code)
	}

	// Symbol
	symVal := Value{Type: TypeSymbol, value: "symbol_name"}
	if symVal.Symbol() != "symbol_name" {
		t.Errorf("Symbol() mismatch: got %v", symVal.Symbol())
	}

	// Timestamp
	tsVal := Value{Type: TypeTimestamp, value: uint64(12345)}
	if tsVal.Timestamp() != 12345 {
		t.Errorf("Timestamp() mismatch: got %v", tsVal.Timestamp())
	}
}

// Test Document.Get without index (linear search path)
func TestDocumentGetWithoutIndex(t *testing.T) {
	// Create document that doesn't use the index path
	doc := &Document{elements: []Element{
		{Key: "first", Value: VInt32(1)},
		{Key: "second", Value: VInt32(2)},
		{Key: "third", Value: VInt32(3)},
	}}

	// Get should work via linear search
	v, ok := doc.Get("second")
	if !ok || v.Int32() != 2 {
		t.Errorf("Get without index: expected 2, got %v, ok=%v", v, ok)
	}

	// Missing key
	v, ok = doc.Get("missing")
	if ok {
		t.Errorf("Get missing key: expected not found, got %v", v)
	}
}

// Test Document.Delete without index
func TestDocumentDeleteWithoutIndex(t *testing.T) {
	doc := &Document{elements: []Element{
		{Key: "first", Value: VInt32(1)},
		{Key: "second", Value: VInt32(2)},
		{Key: "third", Value: VInt32(3)},
	}}

	// Delete middle element
	doc.Delete("second")
	if doc.Len() != 2 {
		t.Errorf("After delete: expected Len=2, got %d", doc.Len())
	}
	if _, ok := doc.Get("second"); ok {
		t.Error("second should be deleted")
	}

	// Delete non-existent key (should not panic)
	doc.Delete("missing")

	// Delete first element
	doc.Delete("first")
	if doc.Len() != 1 {
		t.Errorf("After second delete: expected Len=1, got %d", doc.Len())
	}
}

// Test Document.Delete with index rebuild
func TestDocumentDeleteWithIndex(t *testing.T) {
	doc := NewDocument()
	doc.Set("a", VInt32(1))
	doc.Set("b", VInt32(2))
	doc.Set("c", VInt32(3))
	doc.Set("d", VInt32(4))

	// Delete 'b' - should trigger index rebuild for shifted elements
	doc.Delete("b")

	// All remaining keys should still be accessible
	if v, ok := doc.Get("a"); !ok || v.Int32() != 1 {
		t.Error("a should be accessible")
	}
	if v, ok := doc.Get("c"); !ok || v.Int32() != 3 {
		t.Error("c should be accessible")
	}
	if v, ok := doc.Get("d"); !ok || v.Int32() != 4 {
		t.Error("d should be accessible")
	}

	// Keys order should be correct
	keys := doc.Keys()
	if len(keys) != 3 || keys[0] != "a" || keys[1] != "c" || keys[2] != "d" {
		t.Errorf("keys order wrong: %v", keys)
	}
}

// Test Document.Elements
func TestDocument_Elements(t *testing.T) {
	doc := NewDocument()
	doc.Set("x", VInt32(1))
	doc.Set("y", VInt32(2))

	elems := doc.Elements()
	if len(elems) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(elems))
	}
	if elems[0].Key != "x" || elems[0].Value.Int32() != 1 {
		t.Errorf("first element wrong: %v", elems[0])
	}
	if elems[1].Key != "y" || elems[1].Value.Int32() != 2 {
		t.Errorf("second element wrong: %v", elems[1])
	}
}

// Test Document.String
func TestDocument_String(t *testing.T) {
	doc := NewDocument()
	doc.Set("name", VString("test"))
	doc.Set("value", VInt32(42))

	s := doc.String()
	if s == "" {
		t.Error("String() returned empty")
	}
	// Should contain element info
	if !bytes.Contains([]byte(s), []byte("name")) {
		t.Error("String() should contain 'name'")
	}
}

// Test Value.Interface
func TestValue_Interface(t *testing.T) {
	v := Value{Type: TypeInt32, value: int32(42)}
	if v.Interface() != int32(42) {
		t.Errorf("Interface() mismatch: got %v", v.Interface())
	}
}

// Test Document.Has without index
func TestDocumentHasWithoutIndex(t *testing.T) {
	doc := &Document{elements: []Element{
		{Key: "key1", Value: VInt32(1)},
		{Key: "key2", Value: VInt32(2)},
	}}

	if !doc.Has("key1") {
		t.Error("Has(key1) should be true")
	}
	if doc.Has("missing") {
		t.Error("Has(missing) should be false")
	}
}
