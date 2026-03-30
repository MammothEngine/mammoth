package mongo

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

func setupIndexTest(t *testing.T) (*engine.Engine, *Catalog, *IndexCatalog) {
	t.Helper()
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	cat := NewCatalog(eng)
	cat.CreateDatabase("testdb")
	cat.CreateCollection("testdb", "testcoll")
	ic := NewIndexCatalog(eng, cat)
	return eng, cat, ic
}

func TestIndexCatalog_CreateIndex(t *testing.T) {
	eng, _, ic := setupIndexTest(t)
	defer eng.Close()

	spec := IndexSpec{
		Name: "name_idx",
		Key: []IndexKey{
			{Field: "name"},
		},
	}

	if err := ic.CreateIndex("testdb", "testcoll", spec); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	indexes, _ := ic.ListIndexes("testdb", "testcoll")
	if len(indexes) != 1 {
		t.Errorf("ListIndexes count = %d, want 1", len(indexes))
	}
	if indexes[0].Name != "name_idx" {
		t.Errorf("index name = %q, want name_idx", indexes[0].Name)
	}
}

func TestIndexCatalog_DropIndex(t *testing.T) {
	eng, _, ic := setupIndexTest(t)
	defer eng.Close()

	spec := IndexSpec{
		Name: "temp_idx",
		Key: []IndexKey{{Field: "field1"}},
	}
	ic.CreateIndex("testdb", "testcoll", spec)

	if err := ic.DropIndex("testdb", "testcoll", "temp_idx"); err != nil {
		t.Fatalf("DropIndex: %v", err)
	}

	indexes, _ := ic.ListIndexes("testdb", "testcoll")
	if len(indexes) != 0 {
		t.Errorf("ListIndexes after drop = %d, want 0", len(indexes))
	}
}

func TestIndexCatalog_ListIndexes(t *testing.T) {
	eng, _, ic := setupIndexTest(t)
	defer eng.Close()

	spec1 := IndexSpec{Name: "idx1", Key: []IndexKey{{Field: "a"}}}
	spec2 := IndexSpec{Name: "idx2", Key: []IndexKey{{Field: "b"}}}

	ic.CreateIndex("testdb", "testcoll", spec1)
	ic.CreateIndex("testdb", "testcoll", spec2)

	indexes, _ := ic.ListIndexes("testdb", "testcoll")
	if len(indexes) != 2 {
		t.Errorf("ListIndexes = %d, want 2", len(indexes))
	}
}

func TestIndexCatalog_UniqueConstraint(t *testing.T) {
	eng, _, ic := setupIndexTest(t)
	defer eng.Close()

	spec := IndexSpec{
		Name:   "unique_name",
		Key:    []IndexKey{{Field: "name"}},
		Unique: true,
	}
	ic.CreateIndex("testdb", "testcoll", spec)

	doc1 := bson.NewDocument()
	doc1.Set("name", bson.VString("alice"))
	doc1.Set("_id", bson.VObjectID(bson.NewObjectID()))

	if err := ic.OnDocumentInsert("testdb", "testcoll", doc1); err != nil {
		t.Fatalf("OnDocumentInsert first: %v", err)
	}

	// Duplicate should fail
	doc2 := bson.NewDocument()
	doc2.Set("name", bson.VString("alice"))
	doc2.Set("_id", bson.VObjectID(bson.NewObjectID()))

	if err := ic.OnDocumentInsert("testdb", "testcoll", doc2); err != ErrDuplicateKey {
		t.Errorf("OnDocumentInsert duplicate = %v, want ErrDuplicateKey", err)
	}
}

func TestIndexCatalog_OnDocumentInsertDeleteUpdate(t *testing.T) {
	eng, _, ic := setupIndexTest(t)
	defer eng.Close()

	spec := IndexSpec{
		Name: "score_idx",
		Key:  []IndexKey{{Field: "score"}},
	}
	ic.CreateIndex("testdb", "testcoll", spec)

	oid := bson.NewObjectID()
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(oid))
	doc.Set("score", bson.VInt32(100))

	// Insert
	if err := ic.OnDocumentInsert("testdb", "testcoll", doc); err != nil {
		t.Fatalf("OnDocumentInsert: %v", err)
	}

	// Update
	newDoc := bson.NewDocument()
	newDoc.Set("_id", bson.VObjectID(oid))
	newDoc.Set("score", bson.VInt32(200))

	if err := ic.OnDocumentUpdate("testdb", "testcoll", doc, newDoc); err != nil {
		t.Fatalf("OnDocumentUpdate: %v", err)
	}

	// Delete
	if err := ic.OnDocumentDelete("testdb", "testcoll", newDoc); err != nil {
		t.Fatalf("OnDocumentDelete: %v", err)
	}
}

func TestIndexCatalog_DuplicateIndex(t *testing.T) {
	eng, _, ic := setupIndexTest(t)
	defer eng.Close()

	spec := IndexSpec{Name: "dup_idx", Key: []IndexKey{{Field: "x"}}}
	ic.CreateIndex("testdb", "testcoll", spec)

	if err := ic.CreateIndex("testdb", "testcoll", spec); err != ErrNamespaceExists {
		t.Errorf("duplicate CreateIndex = %v, want ErrNamespaceExists", err)
	}
}
