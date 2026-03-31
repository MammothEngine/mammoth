package mongo

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

func setupHashIndexTest(t *testing.T) (*engine.Engine, *HashIndex) {
	t.Helper()
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { eng.Close() })

	spec := &IndexSpec{
		Name: "email_hash",
		Key:  []IndexKey{{Field: "email", Hashed: true}},
	}
	return eng, NewHashIndex("testdb", "users", spec, eng)
}

func TestHashIndex_AddAndLookup(t *testing.T) {
	_, hi := setupHashIndexTest(t)

	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("email", bson.VString("alice@example.com"))

	if err := hi.AddEntry(doc); err != nil {
		t.Fatalf("AddEntry: %v", err)
	}

	ids := hi.LookupEqual(bson.VString("alice@example.com"))
	if len(ids) != 1 {
		t.Fatalf("LookupEqual count = %d, want 1", len(ids))
	}

	// Verify non-matching lookup returns empty
	ids2 := hi.LookupEqual(bson.VString("bob@example.com"))
	if len(ids2) != 0 {
		t.Errorf("LookupEqual for non-existent = %d, want 0", len(ids2))
	}
}

func TestHashIndex_RemoveEntry(t *testing.T) {
	_, hi := setupHashIndexTest(t)

	oid := bson.NewObjectID()
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(oid))
	doc.Set("email", bson.VString("carol@example.com"))

	hi.AddEntry(doc)

	ids := hi.LookupEqual(bson.VString("carol@example.com"))
	if len(ids) != 1 {
		t.Fatalf("before remove: count = %d, want 1", len(ids))
	}

	if err := hi.RemoveEntry(doc); err != nil {
		t.Fatalf("RemoveEntry: %v", err)
	}

	ids = hi.LookupEqual(bson.VString("carol@example.com"))
	if len(ids) != 0 {
		t.Errorf("after remove: count = %d, want 0", len(ids))
	}
}

func TestHashIndex_MultipleDocuments(t *testing.T) {
	eng, hi := setupHashIndexTest(t)
	_ = eng

	for _, email := range []string{"a@test.com", "b@test.com", "a@test.com"} {
		doc := bson.NewDocument()
		doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
		doc.Set("email", bson.VString(email))
		hi.AddEntry(doc)
	}

	// "a@test.com" appears twice
	ids := hi.LookupEqual(bson.VString("a@test.com"))
	if len(ids) != 2 {
		t.Errorf("LookupEqual a@test.com = %d, want 2", len(ids))
	}

	// "b@test.com" appears once
	ids = hi.LookupEqual(bson.VString("b@test.com"))
	if len(ids) != 1 {
		t.Errorf("LookupEqual b@test.com = %d, want 1", len(ids))
	}
}

func TestHashIndex_Sparse(t *testing.T) {
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	spec := &IndexSpec{
		Name:   "opt_hash",
		Key:    []IndexKey{{Field: "optional", Hashed: true}},
		Sparse: true,
	}
	hi := NewHashIndex("testdb", "sparse_coll", spec, eng)

	// Document without the indexed field
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("name", bson.VString("no-field"))

	if err := hi.AddEntry(doc); err != nil {
		t.Fatalf("AddEntry (sparse miss): %v", err)
	}

	// Should not be indexed
	ids := hi.LookupEqual(bson.VNull())
	if len(ids) != 0 {
		t.Errorf("sparse: LookupEqual null = %d, want 0", len(ids))
	}

	// Document with the field
	doc2 := bson.NewDocument()
	doc2.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc2.Set("optional", bson.VString("present"))
	hi.AddEntry(doc2)

	ids = hi.LookupEqual(bson.VString("present"))
	if len(ids) != 1 {
		t.Errorf("sparse: LookupEqual present = %d, want 1", len(ids))
	}
}
