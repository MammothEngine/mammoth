package mongo

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

func setupCollection(t *testing.T) (*engine.Engine, *Catalog, *Collection) {
	t.Helper()
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	cat := NewCatalog(eng)
	cat.CreateDatabase("testdb")
	cat.CreateCollection("testdb", "testcoll")
	coll := NewCollection("testdb", "testcoll", eng, cat)
	return eng, cat, coll
}

func TestCollection_InsertOne(t *testing.T) {
	eng, _, coll := setupCollection(t)
	defer eng.Close()

	doc := bson.NewDocument()
	doc.Set("name", bson.VString("alice"))
	doc.Set("age", bson.VInt32(30))

	if err := coll.InsertOne(doc); err != nil {
		t.Fatalf("InsertOne: %v", err)
	}

	// Verify _id was set
	if _, ok := doc.Get("_id"); !ok {
		t.Error("_id should be set after InsertOne")
	}
}

func TestCollection_InsertMany(t *testing.T) {
	eng, _, coll := setupCollection(t)
	defer eng.Close()

	docs := []*bson.Document{
		func() *bson.Document { d := bson.NewDocument(); d.Set("i", bson.VInt32(0)); return d }(),
		func() *bson.Document { d := bson.NewDocument(); d.Set("i", bson.VInt32(1)); return d }(),
		func() *bson.Document { d := bson.NewDocument(); d.Set("i", bson.VInt32(2)); return d }(),
	}

	if err := coll.InsertMany(docs); err != nil {
		t.Fatalf("InsertMany: %v", err)
	}

	count, _ := coll.Count()
	if count != 3 {
		t.Errorf("Count = %d, want 3", count)
	}
}

func TestCollection_FindOne(t *testing.T) {
	eng, _, coll := setupCollection(t)
	defer eng.Close()

	doc := bson.NewDocument()
	doc.Set("name", bson.VString("bob"))
	coll.InsertOne(doc)

	idVal, _ := doc.Get("_id")
	found, err := coll.FindOne(idVal.ObjectID())
	if err != nil {
		t.Fatalf("FindOne: %v", err)
	}
	if v, _ := found.Get("name"); v.String() != "bob" {
		t.Errorf("FindOne name = %q, want bob", v.String())
	}
}

func TestCollection_DeleteOne(t *testing.T) {
	eng, _, coll := setupCollection(t)
	defer eng.Close()

	doc := bson.NewDocument()
	doc.Set("name", bson.VString("charlie"))
	coll.InsertOne(doc)

	idVal, _ := doc.Get("_id")
	if err := coll.DeleteOne(idVal.ObjectID()); err != nil {
		t.Fatalf("DeleteOne: %v", err)
	}

	count, _ := coll.Count()
	if count != 0 {
		t.Errorf("Count after delete = %d, want 0", count)
	}
}

func TestCollection_ReplaceOne(t *testing.T) {
	eng, _, coll := setupCollection(t)
	defer eng.Close()

	doc := bson.NewDocument()
	doc.Set("name", bson.VString("dave"))
	doc.Set("age", bson.VInt32(25))
	coll.InsertOne(doc)

	idVal, _ := doc.Get("_id")
	replacement := bson.NewDocument()
	replacement.Set("name", bson.VString("david"))
	replacement.Set("age", bson.VInt32(26))

	if err := coll.ReplaceOne(idVal.ObjectID(), replacement); err != nil {
		t.Fatalf("ReplaceOne: %v", err)
	}

	found, _ := coll.FindOne(idVal.ObjectID())
	if v, _ := found.Get("name"); v.String() != "david" {
		t.Errorf("ReplaceOne name = %q, want david", v.String())
	}
}

func TestCollection_ScanAll(t *testing.T) {
	eng, _, coll := setupCollection(t)
	defer eng.Close()

	for i := 0; i < 5; i++ {
		doc := bson.NewDocument()
		doc.Set("i", bson.VInt32(int32(i)))
		coll.InsertOne(doc)
	}

	var count int
	coll.ScanAll(func(_ []byte, doc *bson.Document) bool {
		count++
		return true
	})
	if count != 5 {
		t.Errorf("ScanAll count = %d, want 5", count)
	}
}

func TestCollection_Count(t *testing.T) {
	eng, _, coll := setupCollection(t)
	defer eng.Close()

	for i := 0; i < 10; i++ {
		doc := bson.NewDocument()
		doc.Set("i", bson.VInt32(int32(i)))
		coll.InsertOne(doc)
	}

	count, err := coll.Count()
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 10 {
		t.Errorf("Count = %d, want 10", count)
	}
}

func TestCollection_StringID(t *testing.T) {
	eng, _, coll := setupCollection(t)
	defer eng.Close()

	doc := bson.NewDocument()
	doc.Set("_id", bson.VString("my_custom_id"))
	doc.Set("name", bson.VString("test"))
	if err := coll.InsertOne(doc); err != nil {
		t.Fatalf("InsertOne with string _id: %v", err)
	}

	count, _ := coll.Count()
	if count != 1 {
		t.Errorf("Count with string _id = %d, want 1", count)
	}

	// Verify _id wasn't overwritten
	if v, _ := doc.Get("_id"); v.String() != "my_custom_id" {
		t.Errorf("_id = %q, want my_custom_id", v.String())
	}
}

func TestCollection_IntID(t *testing.T) {
	eng, _, coll := setupCollection(t)
	defer eng.Close()

	doc := bson.NewDocument()
	doc.Set("_id", bson.VInt32(42))
	doc.Set("name", bson.VString("test"))
	if err := coll.InsertOne(doc); err != nil {
		t.Fatalf("InsertOne with int _id: %v", err)
	}

	count, _ := coll.Count()
	if count != 1 {
		t.Errorf("Count with int _id = %d, want 1", count)
	}
}

func TestCollection_AutoID(t *testing.T) {
	eng, _, coll := setupCollection(t)
	defer eng.Close()

	doc := bson.NewDocument()
	doc.Set("name", bson.VString("auto"))
	if err := coll.InsertOne(doc); err != nil {
		t.Fatalf("InsertOne auto _id: %v", err)
	}

	idVal, ok := doc.Get("_id")
	if !ok || idVal.Type != bson.TypeObjectID {
		t.Error("auto-generated _id should be ObjectID")
	}
}

func TestCollection_MixedID(t *testing.T) {
	eng, _, coll := setupCollection(t)
	defer eng.Close()

	// String ID
	doc1 := bson.NewDocument()
	doc1.Set("_id", bson.VString("str_id"))
	doc1.Set("type", bson.VString("string"))
	coll.InsertOne(doc1)

	// ObjectID
	doc2 := bson.NewDocument()
	doc2.Set("type", bson.VString("objectid"))
	coll.InsertOne(doc2)

	// Int ID
	doc3 := bson.NewDocument()
	doc3.Set("_id", bson.VInt32(123))
	doc3.Set("type", bson.VString("int"))
	coll.InsertOne(doc3)

	count, _ := coll.Count()
	if count != 3 {
		t.Errorf("Count mixed IDs = %d, want 3", count)
	}
}
