package mongo

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

func TestProjection_IncludeMode(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("_id", bson.VInt32(1))
	doc.Set("name", bson.VString("alice"))
	doc.Set("age", bson.VInt32(30))
	doc.Set("email", bson.VString("alice@example.com"))

	proj := bson.NewDocument()
	proj.Set("name", bson.VInt32(1))
	proj.Set("age", bson.VInt32(1))

	result := ApplyProjection(doc, proj)
	keys := result.Keys()
	if len(keys) != 3 { // _id + name + age
		t.Errorf("include mode: got %d fields, want 3", len(keys))
	}
	if _, ok := result.Get("_id"); !ok {
		t.Error("include mode should include _id by default")
	}
	if _, ok := result.Get("name"); !ok {
		t.Error("include mode should include name")
	}
	if _, ok := result.Get("email"); ok {
		t.Error("include mode should not include email")
	}
}

func TestProjection_ExcludeMode(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("_id", bson.VInt32(1))
	doc.Set("name", bson.VString("alice"))
	doc.Set("age", bson.VInt32(30))
	doc.Set("email", bson.VString("alice@example.com"))

	proj := bson.NewDocument()
	proj.Set("age", bson.VInt32(0))
	proj.Set("email", bson.VInt32(0))

	result := ApplyProjection(doc, proj)
	if _, ok := result.Get("age"); ok {
		t.Error("exclude mode should not include age")
	}
	if _, ok := result.Get("name"); !ok {
		t.Error("exclude mode should include name")
	}
	if _, ok := result.Get("_id"); !ok {
		t.Error("exclude mode should include _id")
	}
}

func TestProjection_ExcludeID(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("_id", bson.VInt32(1))
	doc.Set("name", bson.VString("alice"))

	proj := bson.NewDocument()
	proj.Set("_id", bson.VInt32(0))
	proj.Set("name", bson.VInt32(1))

	result := ApplyProjection(doc, proj)
	if _, ok := result.Get("_id"); ok {
		t.Error("projection with _id:0 should exclude _id")
	}
	if _, ok := result.Get("name"); !ok {
		t.Error("projection should include name")
	}
}

func TestProjection_Empty(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("a", bson.VInt32(1))
	doc.Set("b", bson.VInt32(2))

	result := ApplyProjection(doc, nil)
	if result.Len() != 2 {
		t.Errorf("empty projection should return all fields, got %d", result.Len())
	}

	result2 := ApplyProjection(doc, bson.NewDocument())
	if result2.Len() != 2 {
		t.Errorf("empty doc projection should return all fields, got %d", result2.Len())
	}
}
