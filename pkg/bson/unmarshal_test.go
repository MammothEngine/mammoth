package bson

import (
	"reflect"
	"testing"
	"time"
)

type simpleStruct struct {
	Name  string `bson:"name"`
	Age   int32  `bson:"age"`
	Email string `bson:"email,omitempty"`
}

type allTypesStruct struct {
	StrField     string            `bson:"str"`
	IntField     int               `bson:"int"`
	Int8Field    int8              `bson:"int8"`
	Int16Field   int16             `bson:"int16"`
	Int32Field   int32             `bson:"int32"`
	Int64Field   int64             `bson:"int64"`
	UintField    uint              `bson:"uint"`
	Uint8Field   uint8             `bson:"uint8"`
	Uint16Field  uint16            `bson:"uint16"`
	Uint32Field  uint32            `bson:"uint32"`
	Uint64Field  uint64            `bson:"uint64"`
	Float32Field float32           `bson:"float32"`
	Float64Field float64           `bson:"float64"`
	BoolField    bool              `bson:"bool"`
	TimeField    time.Time         `bson:"time"`
	OIDField     ObjectID          `bson:"oid"`
	BytesField   []byte            `bson:"bytes"`
	SliceField   []int32           `bson:"slice"`
	MapField     map[string]int32  `bson:"map"`
	NestedField  simpleStruct      `bson:"nested"`
	PtrField     *string           `bson:"ptr"`
	IfaceField   interface{}       `bson:"iface"`
}

func TestUnmarshal_Basic(t *testing.T) {
	doc := NewDocument()
	doc.Set("name", VString("Alice"))
	doc.Set("age", VInt32(30))

	var s simpleStruct
	err := Unmarshal(doc, &s)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if s.Name != "Alice" {
		t.Errorf("name: expected Alice, got %s", s.Name)
	}
	if s.Age != 30 {
		t.Errorf("age: expected 30, got %d", s.Age)
	}
}

func TestUnmarshal_NonPointer(t *testing.T) {
	doc := NewDocument()
	var s simpleStruct
	err := Unmarshal(doc, s)
	if err == nil {
		t.Error("expected error for non-pointer")
	}
}

func TestUnmarshal_NilPointer(t *testing.T) {
	doc := NewDocument()
	var s *simpleStruct
	err := Unmarshal(doc, s)
	if err == nil {
		t.Error("expected error for nil pointer")
	}
}

func TestUnmarshal_NonStruct(t *testing.T) {
	doc := NewDocument()
	var i int
	err := Unmarshal(doc, &i)
	if err == nil {
		t.Error("expected error for non-struct pointer")
	}
}

func TestUnmarshal_AllTypes(t *testing.T) {
	doc := NewDocument()
	doc.Set("str", VString("hello"))
	doc.Set("int", VInt32(42))
	doc.Set("int8", VInt32(8))
	doc.Set("int16", VInt32(16))
	doc.Set("int32", VInt32(32))
	doc.Set("int64", VInt64(64))
	doc.Set("uint", VInt32(42))
	doc.Set("uint8", VInt32(8))
	doc.Set("uint16", VInt32(16))
	doc.Set("uint32", VInt32(32))
	doc.Set("uint64", VInt64(64))
	doc.Set("float32", VDouble(3.14))
	doc.Set("float64", VDouble(6.28))
	doc.Set("bool", VBool(true))
	doc.Set("time", VDateTime(time.Now().UnixMilli()))
	doc.Set("oid", VObjectID(NewObjectID()))
	doc.Set("bytes", VBinary(BinaryGeneric, []byte{1, 2, 3}))
	doc.Set("slice", VArray(A(VInt32(1), VInt32(2), VInt32(3))))

	inner := NewDocument()
	inner.Set("name", VString("Bob"))
	inner.Set("age", VInt32(25))
	doc.Set("nested", VDoc(inner))

	var s allTypesStruct
	err := Unmarshal(doc, &s)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	// Check key fields
	if s.StrField != "hello" {
		t.Errorf("str: expected hello, got %s", s.StrField)
	}
	if s.IntField != 42 {
		t.Errorf("int: expected 42, got %d", s.IntField)
	}
	if s.Int32Field != 32 {
		t.Errorf("int32: expected 32, got %d", s.Int32Field)
	}
	if s.Int64Field != 64 {
		t.Errorf("int64: expected 64, got %d", s.Int64Field)
	}
	if !s.BoolField {
		t.Error("bool: expected true")
	}
}

func TestUnmarshal_PointerFields(t *testing.T) {
	type ptrStruct struct {
		StrPtr *string `bson:"str"`
		IntPtr *int32  `bson:"int"`
	}

	doc := NewDocument()
	doc.Set("str", VString("test"))
	doc.Set("int", VInt32(42))

	var s ptrStruct
	err := Unmarshal(doc, &s)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if s.StrPtr == nil || *s.StrPtr != "test" {
		t.Error("str ptr not set correctly")
	}
	if s.IntPtr == nil || *s.IntPtr != 42 {
		t.Error("int ptr not set correctly")
	}
}

func TestUnmarshal_PointerField_NilOnNull(t *testing.T) {
	// Note: When unmarshaling, null values cause fields to be skipped (not modified)
	// This is the current behavior of the unmarshal implementation
	type ptrStruct struct {
		StrPtr *string `bson:"str"`
	}

	doc := NewDocument()
	doc.Set("str", VNull())

	var s ptrStruct
	s.StrPtr = new(string)
	*s.StrPtr = "original"

	err := Unmarshal(doc, &s)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	// Null values are skipped, so original value is preserved
	if s.StrPtr == nil || *s.StrPtr != "original" {
		t.Error("expected original value to be preserved for null value")
	}
}

func TestUnmarshal_Time(t *testing.T) {
	type timeStruct struct {
		Created time.Time `bson:"created"`
	}

	now := time.Now().Truncate(time.Millisecond)
	doc := NewDocument()
	doc.Set("created", VDateTime(now.UnixMilli()))

	var s timeStruct
	err := Unmarshal(doc, &s)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if !s.Created.Equal(now) {
		t.Errorf("time mismatch: expected %v, got %v", now, s.Created)
	}
}

func TestUnmarshal_Time_WrongType(t *testing.T) {
	type timeStruct struct {
		Created time.Time `bson:"created"`
	}

	doc := NewDocument()
	doc.Set("created", VString("not a time"))

	var s timeStruct
	err := Unmarshal(doc, &s)
	if err == nil {
		t.Error("expected error for wrong type")
	}
}

func TestUnmarshal_ObjectID(t *testing.T) {
	type oidStruct struct {
		ID ObjectID `bson:"_id"`
	}

	oid := NewObjectID()
	doc := NewDocument()
	doc.Set("_id", VObjectID(oid))

	var s oidStruct
	err := Unmarshal(doc, &s)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if !s.ID.Equal(oid) {
		t.Errorf("objectid mismatch: expected %v, got %v", oid, s.ID)
	}
}

func TestUnmarshal_ObjectID_WrongType(t *testing.T) {
	type oidStruct struct {
		ID ObjectID `bson:"_id"`
	}

	doc := NewDocument()
	doc.Set("_id", VString("not an oid"))

	var s oidStruct
	err := Unmarshal(doc, &s)
	if err == nil {
		t.Error("expected error for wrong type")
	}
}

func TestUnmarshal_ByteSlice(t *testing.T) {
	type bytesStruct struct {
		Data []byte `bson:"data"`
	}

	doc := NewDocument()
	doc.Set("data", VBinary(BinaryGeneric, []byte{1, 2, 3}))

	var s bytesStruct
	err := Unmarshal(doc, &s)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if !reflect.DeepEqual(s.Data, []byte{1, 2, 3}) {
		t.Errorf("bytes mismatch: expected [1 2 3], got %v", s.Data)
	}
}

func TestUnmarshal_SliceFromBSON(t *testing.T) {
	type sliceStruct struct {
		Items []int32 `bson:"items"`
	}

	doc := NewDocument()
	doc.Set("items", VArray(A(VInt32(1), VInt32(2), VInt32(3))))

	var s sliceStruct
	err := Unmarshal(doc, &s)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if len(s.Items) != 3 || s.Items[0] != 1 || s.Items[1] != 2 || s.Items[2] != 3 {
		t.Errorf("slice mismatch: expected [1 2 3], got %v", s.Items)
	}
}

func TestUnmarshal_Map(t *testing.T) {
	type mapStruct struct {
		Data map[string]int32 `bson:"data"`
	}

	inner := NewDocument()
	inner.Set("a", VInt32(1))
	inner.Set("b", VInt32(2))

	doc := NewDocument()
	doc.Set("data", VDoc(inner))

	var s mapStruct
	err := Unmarshal(doc, &s)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if len(s.Data) != 2 || s.Data["a"] != 1 || s.Data["b"] != 2 {
		t.Errorf("map mismatch: expected map[a:1 b:2], got %v", s.Data)
	}
}

func TestUnmarshal_NestedStructFromBSON(t *testing.T) {
	type innerStruct struct {
		Value int32 `bson:"value"`
	}
	type outerStruct struct {
		Inner innerStruct `bson:"inner"`
	}

	innerDoc := NewDocument()
	innerDoc.Set("value", VInt32(42))

	doc := NewDocument()
	doc.Set("inner", VDoc(innerDoc))

	var s outerStruct
	err := Unmarshal(doc, &s)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if s.Inner.Value != 42 {
		t.Errorf("nested value: expected 42, got %d", s.Inner.Value)
	}
}

func TestUnmarshal_Interface(t *testing.T) {
	type ifaceStruct struct {
		Value interface{} `bson:"value"`
	}

	doc := NewDocument()
	doc.Set("value", VString("hello"))

	var s ifaceStruct
	err := Unmarshal(doc, &s)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if s.Value != "hello" {
		t.Errorf("interface value: expected hello, got %v", s.Value)
	}
}

func TestUnmarshal_MissingFields(t *testing.T) {
	doc := NewDocument()
	doc.Set("name", VString("Alice"))
	// age is missing

	var s simpleStruct
	s.Age = 25 // Should not be modified

	err := Unmarshal(doc, &s)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if s.Name != "Alice" {
		t.Errorf("name: expected Alice, got %s", s.Name)
	}
	if s.Age != 25 {
		t.Errorf("age: expected unchanged 25, got %d", s.Age)
	}
}

func TestUnmarshal_NullValue(t *testing.T) {
	doc := NewDocument()
	doc.Set("name", VNull())

	var s simpleStruct
	s.Name = "original"

	err := Unmarshal(doc, &s)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if s.Name != "original" {
		t.Errorf("name: expected unchanged 'original', got %s", s.Name)
	}
}

func TestUnmarshal_UnexportedField(t *testing.T) {
	type unexportedStruct struct {
		Public  string `bson:"public"`
		private string `bson:"private"`
	}

	doc := NewDocument()
	doc.Set("public", VString("pub"))
	doc.Set("private", VString("priv"))

	var s unexportedStruct
	err := Unmarshal(doc, &s)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if s.Public != "pub" {
		t.Errorf("public: expected pub, got %s", s.Public)
	}
	if s.private != "" {
		t.Errorf("private: expected empty, got %s", s.private)
	}
}

func TestUnmarshal_TagOptions(t *testing.T) {
	type tagStruct struct {
		FieldName string `bson:"field_name"`
		OmitEmpty string `bson:",omitempty"`
	}

	doc := NewDocument()
	doc.Set("field_name", VString("value"))
	doc.Set("OmitEmpty", VString("omit_value"))

	var s tagStruct
	err := Unmarshal(doc, &s)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if s.FieldName != "value" {
		t.Errorf("field_name: expected value, got %s", s.FieldName)
	}
	if s.OmitEmpty != "omit_value" {
		t.Errorf("OmitEmpty: expected omit_value, got %s", s.OmitEmpty)
	}
}

func TestUnmarshal_TypeConversions(t *testing.T) {
	t.Run("int32_to_float64", func(t *testing.T) {
		type testStruct struct {
			F float64 `bson:"f"`
		}
		doc := NewDocument()
		doc.Set("f", VInt32(42))
		var s testStruct
		err := Unmarshal(doc, &s)
		if err != nil {
			t.Fatalf("unmarshal failed: %v", err)
		}
		if s.F != 42.0 {
			t.Errorf("expected 42.0, got %f", s.F)
		}
	})

	t.Run("int64_to_float64", func(t *testing.T) {
		type testStruct struct {
			F float64 `bson:"f"`
		}
		doc := NewDocument()
		doc.Set("f", VInt64(42))
		var s testStruct
		err := Unmarshal(doc, &s)
		if err != nil {
			t.Fatalf("unmarshal failed: %v", err)
		}
		if s.F != 42.0 {
			t.Errorf("expected 42.0, got %f", s.F)
		}
	})

	t.Run("double_to_int", func(t *testing.T) {
		type testStruct struct {
			I int `bson:"i"`
		}
		doc := NewDocument()
		doc.Set("i", VDouble(42.9))
		var s testStruct
		err := Unmarshal(doc, &s)
		if err != nil {
			t.Fatalf("unmarshal failed: %v", err)
		}
		if s.I != 42 {
			t.Errorf("expected 42, got %d", s.I)
		}
	})
}

func TestUnmarshal_InvalidConversions(t *testing.T) {
	t.Run("string_to_int", func(t *testing.T) {
		type testStruct struct {
			I int `bson:"i"`
		}
		doc := NewDocument()
		doc.Set("i", VString("not a number"))
		var s testStruct
		err := Unmarshal(doc, &s)
		if err == nil {
			t.Error("expected error for invalid conversion")
		}
	})

	t.Run("document_to_int", func(t *testing.T) {
		type testStruct struct {
			I int `bson:"i"`
		}
		doc := NewDocument()
		doc.Set("i", VDoc(NewDocument()))
		var s testStruct
		err := Unmarshal(doc, &s)
		if err == nil {
			t.Error("expected error for invalid conversion")
		}
	})
}

func TestUnmarshal_EmbeddedPointer(t *testing.T) {
	type Inner struct {
		Value int32 `bson:"value"`
	}
	type Outer struct {
		Inner *Inner `bson:"inner"`
	}

	innerDoc := NewDocument()
	innerDoc.Set("value", VInt32(42))

	doc := NewDocument()
	doc.Set("inner", VDoc(innerDoc))

	var s Outer
	err := Unmarshal(doc, &s)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if s.Inner == nil || s.Inner.Value != 42 {
		t.Error("embedded pointer not set correctly")
	}
}

func TestUnmarshal_ArrayToSlice(t *testing.T) {
	type sliceStr struct {
		Strings []string `bson:"strings"`
	}

	doc := NewDocument()
	doc.Set("strings", VArray(A(VString("a"), VString("b"), VString("c"))))

	var s sliceStr
	err := Unmarshal(doc, &s)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if len(s.Strings) != 3 || s.Strings[0] != "a" || s.Strings[1] != "b" || s.Strings[2] != "c" {
		t.Errorf("string slice mismatch: got %v", s.Strings)
	}
}

func TestSetField_UnsupportedType(t *testing.T) {
	type complexStruct struct {
		C complex128 `bson:"c"`
	}

	doc := NewDocument()
	doc.Set("c", VDouble(1.0))

	var s complexStruct
	err := Unmarshal(doc, &s)
	if err == nil {
		t.Error("expected error for unsupported type")
	}
}
