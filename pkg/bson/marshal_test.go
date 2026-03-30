package bson

import (
	"testing"
	"time"
)

type testStruct struct {
	Name    string    `bson:"name"`
	Age     int32     `bson:"age"`
	Score   float64   `bson:"score"`
	Active  bool      `bson:"active"`
	Created time.Time `bson:"created"`
	ID      ObjectID  `bson:"_id"`
}

type nestedStruct struct {
	Inner testStruct `bson:"inner"`
}

type omitemptyStruct struct {
	Name  string `bson:"name"`
	Empty string `bson:",omitempty"`
}

func TestMarshalBasic(t *testing.T) {
	s := testStruct{
		Name:    "Alice",
		Age:     30,
		Score:   95.5,
		Active:  true,
		Created: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		ID:      NewObjectID(),
	}

	doc, err := Marshal(s)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	if v, _ := doc.Get("name"); v.String() != "Alice" {
		t.Fatalf("name mismatch: %v", v)
	}
	if v, _ := doc.Get("age"); v.Int32() != 30 {
		t.Fatalf("age mismatch: %v", v)
	}
	if v, _ := doc.Get("score"); v.Double() != 95.5 {
		t.Fatalf("score mismatch: %v", v)
	}
	if v, _ := doc.Get("active"); !v.Boolean() {
		t.Fatalf("active mismatch")
	}
}

func TestUnmarshalBasic(t *testing.T) {
	doc := NewDocument()
	doc.Set("name", VString("Bob"))
	doc.Set("age", VInt32(25))
	doc.Set("score", VDouble(88.0))
	doc.Set("active", VBool(false))
	doc.Set("created", VDateTime(time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC).UnixMilli()))
	doc.Set("_id", VObjectID(NewObjectID()))

	var s testStruct
	if err := Unmarshal(doc, &s); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if s.Name != "Bob" {
		t.Fatalf("name mismatch: %s", s.Name)
	}
	if s.Age != 25 {
		t.Fatalf("age mismatch: %d", s.Age)
	}
	if s.Score != 88.0 {
		t.Fatalf("score mismatch: %f", s.Score)
	}
	if s.Active {
		t.Fatalf("active mismatch")
	}
}

func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	original := testStruct{
		Name:    "Charlie",
		Age:     35,
		Score:   99.9,
		Active:  true,
		Created: time.Date(2024, 3, 15, 12, 0, 0, 0, time.UTC),
		ID:      NewObjectID(),
	}

	doc, err := Marshal(original)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	var decoded testStruct
	if err := Unmarshal(doc, &decoded); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if decoded.Name != original.Name {
		t.Fatalf("name: got %s, want %s", decoded.Name, original.Name)
	}
	if decoded.Age != original.Age {
		t.Fatalf("age: got %d, want %d", decoded.Age, original.Age)
	}
	if decoded.Score != original.Score {
		t.Fatalf("score: got %f, want %f", decoded.Score, original.Score)
	}
	if decoded.Active != original.Active {
		t.Fatalf("active: got %v, want %v", decoded.Active, original.Active)
	}
}

func TestMarshalNested(t *testing.T) {
	s := nestedStruct{
		Inner: testStruct{
			Name: "Nested",
			Age:  10,
		},
	}

	doc, err := Marshal(s)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	v, ok := doc.Get("inner")
	if !ok || v.Type != TypeDocument {
		t.Fatal("inner doc not found")
	}
	inner := v.DocumentValue()
	if nv, _ := inner.Get("name"); nv.String() != "Nested" {
		t.Fatalf("inner name mismatch: %v", nv)
	}
}
