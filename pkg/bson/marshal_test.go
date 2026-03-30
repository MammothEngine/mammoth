package bson

import (
	"testing"
	"time"
)

// --- test types ---

type basicTypesStruct struct {
	Name   string  `bson:"name"`
	Age    int32   `bson:"age"`
	Height int64   `bson:"height"`
	Score  float64 `bson:"score"`
	Active bool    `bson:"active"`
}

type sliceStruct struct {
	Tags  []string `bson:"tags"`
	Bytes []byte   `bson:"bytes"`
}

type innerStruct struct {
	X int32 `bson:"x"`
	Y int32 `bson:"y"`
}

type nestedWrapper struct {
	Label string     `bson:"label"`
	Inner innerStruct `bson:"inner"`
}

type timeAndOIDStruct struct {
	ID      ObjectID  `bson:"_id"`
	Created time.Time `bson:"created"`
}

type bsonTagStruct struct {
	RealName string `bson:"real_name"`
	Age      int32  `bson:"age"`
	Untagged string
}

type omitemptyStruct struct {
	Name     string `bson:"name"`
	Skipped  string `bson:",omitempty"`
	Present  string `bson:",omitempty"`
}

type noTagStruct struct {
	Name string
	Age  int32
}

// --- Marshal tests ---

func TestMarshal_BasicTypes(t *testing.T) {
	s := basicTypesStruct{
		Name:   "Alice",
		Age:    30,
		Height: 180,
		Score:  95.5,
		Active: true,
	}

	doc, err := Marshal(s)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	if v, _ := doc.Get("name"); v.String() != "Alice" {
		t.Errorf("name: got %q, want %q", v.String(), "Alice")
	}
	if v, _ := doc.Get("age"); v.Int32() != 30 {
		t.Errorf("age: got %d, want %d", v.Int32(), 30)
	}
	if v, _ := doc.Get("height"); v.Int64() != 180 {
		t.Errorf("height: got %d, want %d", v.Int64(), 180)
	}
	if v, _ := doc.Get("score"); v.Double() != 95.5 {
		t.Errorf("score: got %f, want %f", v.Double(), 95.5)
	}
	if v, _ := doc.Get("active"); !v.Boolean() {
		t.Error("active: got false, want true")
	}
}

func TestMarshal_NestedStruct(t *testing.T) {
	s := nestedWrapper{
		Label: "point",
		Inner: innerStruct{X: 3, Y: 7},
	}

	doc, err := Marshal(s)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	if v, _ := doc.Get("label"); v.String() != "point" {
		t.Errorf("label: got %q, want %q", v.String(), "point")
	}

	v, ok := doc.Get("inner")
	if !ok || v.Type != TypeDocument {
		t.Fatalf("inner: expected document, got type=%v ok=%v", v.Type, ok)
	}
	inner := v.DocumentValue()
	if xv, _ := inner.Get("x"); xv.Int32() != 3 {
		t.Errorf("inner.x: got %d, want 3", xv.Int32())
	}
	if yv, _ := inner.Get("y"); yv.Int32() != 7 {
		t.Errorf("inner.y: got %d, want 7", yv.Int32())
	}
}

func TestMarshal_Slice(t *testing.T) {
	s := sliceStruct{
		Tags:  []string{"go", "bson", "test"},
		Bytes: []byte{0xDE, 0xAD, 0xBE, 0xEF},
	}

	doc, err := Marshal(s)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	v, ok := doc.Get("tags")
	if !ok || v.Type != TypeArray {
		t.Fatalf("tags: expected array, got type=%v ok=%v", v.Type, ok)
	}
	arr := v.ArrayValue()
	if len(arr) != 3 {
		t.Fatalf("tags: expected 3 elements, got %d", len(arr))
	}
	if arr[0].String() != "go" || arr[1].String() != "bson" || arr[2].String() != "test" {
		t.Errorf("tags: got %v, want [go bson test]", arr)
	}

	bv, _ := doc.Get("bytes")
	if bv.Type != TypeBinary {
		t.Fatalf("bytes: expected TypeBinary, got %v", bv.Type)
	}
	bin := bv.Binary()
	if len(bin.Data) != 4 || bin.Data[0] != 0xDE || bin.Data[3] != 0xEF {
		t.Errorf("bytes: got %v, want [DE AD BE EF]", bin.Data)
	}
}

func TestMarshal_TimeAndObjectID(t *testing.T) {
	oid := NewObjectID()
	ts := time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC)

	s := timeAndOIDStruct{ID: oid, Created: ts}
	doc, err := Marshal(s)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	if v, _ := doc.Get("_id"); v.Type != TypeObjectID || v.ObjectID() != oid {
		t.Errorf("_id: got %v, want %v", v.ObjectID(), oid)
	}
	if v, _ := doc.Get("created"); v.Type != TypeDateTime {
		t.Errorf("created: expected TypeDateTime, got %v", v.Type)
	}
	if v, _ := doc.Get("created"); v.DateTime() != ts.UnixMilli() {
		t.Errorf("created: got %d, want %d", v.DateTime(), ts.UnixMilli())
	}
}

// --- Unmarshal tests ---

func TestUnmarshal_BasicTypes(t *testing.T) {
	doc := NewDocument()
	doc.Set("name", VString("Bob"))
	doc.Set("age", VInt32(25))
	doc.Set("height", VInt64(175))
	doc.Set("score", VDouble(88.0))
	doc.Set("active", VBool(false))

	var s basicTypesStruct
	if err := Unmarshal(doc, &s); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if s.Name != "Bob" {
		t.Errorf("name: got %q, want %q", s.Name, "Bob")
	}
	if s.Age != 25 {
		t.Errorf("age: got %d, want %d", s.Age, 25)
	}
	if s.Height != 175 {
		t.Errorf("height: got %d, want %d", s.Height, 175)
	}
	if s.Score != 88.0 {
		t.Errorf("score: got %f, want %f", s.Score, 88.0)
	}
	if s.Active {
		t.Error("active: got true, want false")
	}
}

func TestUnmarshal_NestedStruct(t *testing.T) {
	inner := NewDocument()
	inner.Set("x", VInt32(10))
	inner.Set("y", VInt32(20))

	doc := NewDocument()
	doc.Set("label", VString("nested"))
	doc.Set("inner", VDoc(inner))

	var s nestedWrapper
	if err := Unmarshal(doc, &s); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if s.Label != "nested" {
		t.Errorf("label: got %q, want %q", s.Label, "nested")
	}
	if s.Inner.X != 10 || s.Inner.Y != 20 {
		t.Errorf("inner: got %+v, want {X:10 Y:20}", s.Inner)
	}
}

func TestUnmarshal_Slice(t *testing.T) {
	doc := NewDocument()
	doc.Set("tags", VArray(A(VString("a"), VString("b"))))
	doc.Set("bytes", VBinary(BinaryGeneric, []byte{0x01, 0x02}))

	var s sliceStruct
	if err := Unmarshal(doc, &s); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if len(s.Tags) != 2 || s.Tags[0] != "a" || s.Tags[1] != "b" {
		t.Errorf("tags: got %v, want [a b]", s.Tags)
	}
	if len(s.Bytes) != 2 || s.Bytes[0] != 0x01 || s.Bytes[1] != 0x02 {
		t.Errorf("bytes: got %v, want [01 02]", s.Bytes)
	}
}

func TestUnmarshal_TimeAndObjectID(t *testing.T) {
	oid := NewObjectID()
	ts := time.Date(2025, 3, 1, 12, 0, 0, 0, time.UTC)

	doc := NewDocument()
	doc.Set("_id", VObjectID(oid))
	doc.Set("created", VDateTime(ts.UnixMilli()))

	var s timeAndOIDStruct
	if err := Unmarshal(doc, &s); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if s.ID != oid {
		t.Errorf("_id: got %v, want %v", s.ID, oid)
	}
	if !s.Created.Equal(ts) {
		t.Errorf("created: got %v, want %v", s.Created, ts)
	}
}

// --- Roundtrip ---

func TestMarshal_Roundtrip(t *testing.T) {
	oid := NewObjectID()
	original := timeAndOIDStruct{
		ID:      oid,
		Created: time.Date(2024, 12, 25, 0, 0, 0, 0, time.UTC),
	}

	doc, err := Marshal(original)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var decoded timeAndOIDStruct
	if err := Unmarshal(doc, &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if decoded.ID != original.ID {
		t.Errorf("ID: got %v, want %v", decoded.ID, original.ID)
	}
	if !decoded.Created.Equal(original.Created) {
		t.Errorf("Created: got %v, want %v", decoded.Created, original.Created)
	}

	doc2, err := Marshal(decoded)
	if err != nil {
		t.Fatalf("second Marshal error: %v", err)
	}
	if doc2.Len() != doc.Len() {
		t.Errorf("doc lengths differ: first=%d, second=%d", doc.Len(), doc2.Len())
	}
}

func TestMarshal_Roundtrip_Nested(t *testing.T) {
	original := nestedWrapper{
		Label: "rt",
		Inner: innerStruct{X: 100, Y: 200},
	}

	doc, err := Marshal(original)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var decoded nestedWrapper
	if err := Unmarshal(doc, &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if decoded.Label != original.Label {
		t.Errorf("label: got %q, want %q", decoded.Label, original.Label)
	}
	if decoded.Inner != original.Inner {
		t.Errorf("inner: got %+v, want %+v", decoded.Inner, original.Inner)
	}
}

// --- Error cases ---

func TestMarshal_ErrorCases(t *testing.T) {
	// Passing a non-struct (int) should error.
	_, err := Marshal(42)
	if err == nil {
		t.Error("expected error when marshaling int, got nil")
	}

	// Passing a non-struct (string) should error.
	_, err = Marshal("hello")
	if err == nil {
		t.Error("expected error when marshaling string, got nil")
	}
}

func TestUnmarshal_ErrorCases(t *testing.T) {
	// Non-pointer should error.
	err := Unmarshal(NewDocument(), basicTypesStruct{})
	if err == nil {
		t.Error("expected error for non-pointer target, got nil")
	}

	// nil interface should error.
	err = Unmarshal(NewDocument(), nil)
	if err == nil {
		t.Error("expected error for nil target, got nil")
	}

	// Pointer to non-struct should error.
	var x int32
	err = Unmarshal(NewDocument(), &x)
	if err == nil {
		t.Error("expected error for *int32 target, got nil")
	}
}

func TestMarshal_NilPointer(t *testing.T) {
	type ptrStruct struct {
		Name *string `bson:"name"`
	}
	s := ptrStruct{Name: nil}

	doc, err := Marshal(s)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}
	if v, ok := doc.Get("name"); !ok || !v.IsNull() {
		t.Errorf("expected null for nil pointer, got %v ok=%v", v, ok)
	}
}

// --- BSON tag support ---

func TestMarshal_BSONTags(t *testing.T) {
	s := bsonTagStruct{
		RealName: "Tagged",
		Age:      42,
		Untagged: "no_tag",
	}

	doc, err := Marshal(s)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	// The struct uses `bson:"real_name"` so the key must be "real_name".
	if _, ok := doc.Get("real_name"); !ok {
		t.Error("expected key 'real_name' from bson tag")
	}
	if v, _ := doc.Get("real_name"); v.String() != "Tagged" {
		t.Errorf("real_name: got %q, want %q", v.String(), "Tagged")
	}
	if v, _ := doc.Get("age"); v.Int32() != 42 {
		t.Errorf("age: got %d, want 42", v.Int32())
	}
	// Untagged field should use the Go field name.
	if v, _ := doc.Get("Untagged"); v.String() != "no_tag" {
		t.Errorf("Untagged: got %q, want %q", v.String(), "no_tag")
	}
}

func TestUnmarshal_BSONTags(t *testing.T) {
	doc := NewDocument()
	doc.Set("real_name", VString("FromDoc"))
	doc.Set("age", VInt32(55))
	doc.Set("Untagged", VString("val"))

	var s bsonTagStruct
	if err := Unmarshal(doc, &s); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if s.RealName != "FromDoc" {
		t.Errorf("RealName: got %q, want %q", s.RealName, "FromDoc")
	}
	if s.Age != 55 {
		t.Errorf("Age: got %d, want %d", s.Age, 55)
	}
	if s.Untagged != "val" {
		t.Errorf("Untagged: got %q, want %q", s.Untagged, "val")
	}
}

func TestMarshal_OmitEmpty(t *testing.T) {
	s := omitemptyStruct{
		Name:    "test",
		Skipped: "",
		Present: "here",
	}

	doc, err := Marshal(s)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	if doc.Has("Skipped") {
		t.Error("Skipped should be omitted when empty")
	}
	if !doc.Has("name") {
		t.Error("name should always be present")
	}
	if !doc.Has("Present") {
		t.Error("Present should be included when non-empty")
	}
}

func TestMarshal_NoTags(t *testing.T) {
	s := noTagStruct{Name: "plain", Age: 20}

	doc, err := Marshal(s)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	// Without bson tags, Go field names are used as keys.
	if v, _ := doc.Get("Name"); v.String() != "plain" {
		t.Errorf("Name: got %q, want %q", v.String(), "plain")
	}
	if v, _ := doc.Get("Age"); v.Int32() != 20 {
		t.Errorf("Age: got %d, want 20", v.Int32())
	}
}
