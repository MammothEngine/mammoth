package bson

import "testing"

func TestCompareTypeOrder(t *testing.T) {
	tests := []struct {
		a, b Value
		want int
	}{
		{VMinKey(), VNull(), -1},
		{VNull(), VInt32(0), -1},
		{VInt32(1), VInt32(2), -1},
		{VInt32(1), VInt32(1), 0},
		{VInt32(2), VInt32(1), 1},
		{VDouble(1.5), VDouble(2.5), -1},
		{VInt32(1), VDouble(1.0), 0},
		{VString("a"), VString("b"), -1},
		{VString("b"), VString("a"), 1},
		{VString("a"), VString("a"), 0},
		{VBool(false), VBool(true), -1},
		{VBool(true), VBool(false), 1},
		{VMaxKey(), VMinKey(), 1},
	}

	for _, tt := range tests {
		got := CompareValues(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("CompareValues(%v, %v) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestCompareCrossType(t *testing.T) {
	// Null < Numbers < String < Object < Array < BinData < ObjectId < Boolean < Date
	if CompareValues(VNull(), VInt32(0)) >= 0 {
		t.Error("null should be < number")
	}
	if CompareValues(VInt32(0), VString("")) >= 0 {
		t.Error("number should be < string")
	}
	if CompareValues(VString(""), VBool(false)) >= 0 {
		t.Error("string should be < boolean")
	}
	if CompareValues(VBool(false), VDateTime(0)) >= 0 {
		t.Error("boolean should be < datetime")
	}
}

func TestCompareDocuments(t *testing.T) {
	d1 := NewDocument()
	d1.Set("a", VInt32(1))
	d1.Set("b", VInt32(2))

	d2 := NewDocument()
	d2.Set("a", VInt32(1))
	d2.Set("b", VInt32(3))

	if CompareDocuments(d1, d2) >= 0 {
		t.Error("d1 should be < d2")
	}
}

func TestCompareArrays(t *testing.T) {
	a1 := A(VInt32(1), VInt32(2))
	a2 := A(VInt32(1), VInt32(3))

	if CompareValues(Value{Type: TypeArray, value: a1}, Value{Type: TypeArray, value: a2}) >= 0 {
		t.Error("a1 should be < a2")
	}
}
