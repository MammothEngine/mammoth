package bson

import (
	"testing"
)

func TestValue_Constructors(t *testing.T) {
	tests := []struct {
		name string
		val  Value
		want BSONType
	}{
		{"double", VDouble(3.14), TypeDouble},
		{"string", VString("hello"), TypeString},
		{"document", VDoc(NewDocument()), TypeDocument},
		{"array", VArray(A()), TypeArray},
		{"binary", VBinary(BinaryGeneric, []byte{1, 2}), TypeBinary},
		{"objectid", VObjectID(ObjectID{}), TypeObjectID},
		{"bool true", VBool(true), TypeBoolean},
		{"bool false", VBool(false), TypeBoolean},
		{"datetime", VDateTime(1234567890), TypeDateTime},
		{"null", VNull(), TypeNull},
		{"regex", VRegex("pat", "i"), TypeRegex},
		{"int32", VInt32(42), TypeInt32},
		{"int64", VInt64(999), TypeInt64},
		{"timestamp", VTimestamp(100), TypeTimestamp},
	}
	for _, tt := range tests {
		if got := tt.val.Type; got != tt.want {
			t.Errorf("%s: Type() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestValue_Accessors(t *testing.T) {
	oid := ObjectID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c}
	doc := D("x", VInt32(1))

	if v := VDouble(2.5).Double(); v != 2.5 {
		t.Errorf("Double() = %v, want 2.5", v)
	}
	if v := VString("abc").String(); v != "abc" {
		t.Errorf("String() = %v, want abc", v)
	}
	if v := VInt32(7).Int32(); v != 7 {
		t.Errorf("Int32() = %v, want 7", v)
	}
	if v := VInt64(100).Int64(); v != 100 {
		t.Errorf("Int64() = %v, want 100", v)
	}
	if v := VBool(true).Boolean(); v != true {
		t.Errorf("Boolean() = %v, want true", v)
	}
	if v := VDateTime(9999).DateTime(); v != 9999 {
		t.Errorf("DateTime() = %v, want 9999", v)
	}
	if v := VObjectID(oid).ObjectID(); v != oid {
		t.Errorf("ObjectID() = %v, want %v", v, oid)
	}
	if v := VBinary(BinaryUUID, []byte{0xFF}).Binary(); v.Subtype != BinaryUUID || len(v.Data) != 1 || v.Data[0] != 0xFF {
		t.Errorf("Binary() = %+v, unexpected", v)
	}
	if d := VDoc(doc).DocumentValue(); d == nil {
		t.Error("DocumentValue() = nil, want non-nil")
	} else if val, _ := d.Get("x"); val.Int32() != 1 {
		t.Errorf("DocumentValue().Get(\"x\") = %v, want 1", val.Int32())
	}
	if a := VArray(A(VInt32(10))).ArrayValue(); len(a) != 1 || a[0].Int32() != 10 {
		t.Errorf("ArrayValue() = %v, unexpected", a)
	}
	if r := VRegex("p", "m").Regex(); r.Pattern != "p" || r.Options != "m" {
		t.Errorf("Regex() = %+v, unexpected", r)
	}
	if !VNull().IsNull() {
		t.Error("IsNull() = false, want true for null value")
	}
}

func TestValue_CompareValues_AllTypes(t *testing.T) {
	// Same-type comparisons
	if cmp := CompareValues(VInt32(1), VInt32(2)); cmp != -1 {
		t.Errorf("CompareValues(int32 1, int32 2) = %d, want -1", cmp)
	}
	if cmp := CompareValues(VInt32(2), VInt32(2)); cmp != 0 {
		t.Errorf("CompareValues(int32 2, int32 2) = %d, want 0", cmp)
	}
	if cmp := CompareValues(VDouble(5.0), VDouble(3.0)); cmp != 1 {
		t.Errorf("CompareValues(double 5, double 3) = %d, want 1", cmp)
	}
	if cmp := CompareValues(VString("a"), VString("b")); cmp != -1 {
		t.Errorf("CompareValues(string a, string b) = %d, want -1", cmp)
	}
	if cmp := CompareValues(VBool(false), VBool(true)); cmp != -1 {
		t.Errorf("CompareValues(bool false, bool true) = %d, want -1", cmp)
	}
	if cmp := CompareValues(VNull(), VNull()); cmp != 0 {
		t.Errorf("CompareValues(null, null) = %d, want 0", cmp)
	}

	// Cross-type: numbers compare as equal order group
	if cmp := CompareValues(VInt32(5), VDouble(5.0)); cmp != 0 {
		t.Errorf("CompareValues(int32 5, double 5.0) = %d, want 0", cmp)
	}
	if cmp := CompareValues(VInt64(1), VDouble(0.5)); cmp != 1 {
		t.Errorf("CompareValues(int64 1, double 0.5) = %d, want 1", cmp)
	}

	// Cross-type ordering: null < numbers < string < document < array < ...
	if cmp := CompareValues(VNull(), VInt32(0)); cmp != -1 {
		t.Errorf("CompareValues(null, int32) = %d, want -1", cmp)
	}
	if cmp := CompareValues(VInt32(1), VString("a")); cmp != -1 {
		t.Errorf("CompareValues(int32, string) = %d, want -1", cmp)
	}
	if cmp := CompareValues(VString("x"), VBool(false)); cmp != -1 {
		t.Errorf("CompareValues(string, bool) = %d, want -1", cmp)
	}
	if cmp := CompareValues(VDateTime(0), VRegex("a", "")); cmp != -1 {
		t.Errorf("CompareValues(datetime, regex) = %d, want -1", cmp)
	}
}

func TestValue_ZeroValues(t *testing.T) {
	// A string-typed value should return zero defaults from other accessors
	v := VString("test")
	if v.Double() != 0 {
		t.Errorf("Double() on string = %v, want 0", v.Double())
	}
	if v.Int32() != 0 {
		t.Errorf("Int32() on string = %v, want 0", v.Int32())
	}
	if v.Int64() != 0 {
		t.Errorf("Int64() on string = %v, want 0", v.Int64())
	}
	if v.Boolean() != false {
		t.Errorf("Boolean() on string = %v, want false", v.Boolean())
	}
	if v.DateTime() != 0 {
		t.Errorf("DateTime() on string = %v, want 0", v.DateTime())
	}
	if v.ObjectID() != (ObjectID{}) {
		t.Errorf("ObjectID() on string = %v, want zero ObjectID", v.ObjectID())
	}
	if b := v.Binary(); b.Subtype != 0 || b.Data != nil {
		t.Errorf("Binary() on string = %+v, want zero Binary", b)
	}
	if v.DocumentValue() != nil {
		t.Errorf("DocumentValue() on string = %v, want nil", v.DocumentValue())
	}
	if v.ArrayValue() != nil {
		t.Errorf("ArrayValue() on string = %v, want nil", v.ArrayValue())
	}
	if v.Regex() != (Regex{}) {
		t.Errorf("Regex() on string = %+v, want zero Regex", v.Regex())
	}
	if v.IsNull() {
		t.Error("IsNull() on string = true, want false")
	}
}
