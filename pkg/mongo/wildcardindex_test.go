package mongo

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

func setupWildcardIndexTest(t *testing.T) (*engine.Engine, *WildcardIndex) {
	t.Helper()
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { eng.Close() })

	spec := &IndexSpec{
		Name: "wildcard_all",
		Key:  []IndexKey{{Field: "$**"}},
	}
	return eng, NewWildcardIndex("testdb", "docs", spec, eng)
}

func TestWildcardIndex_AddAndLookup(t *testing.T) {
	eng, wi := setupWildcardIndexTest(t)
	_ = eng

	oid := bson.NewObjectID()
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(oid))
	doc.Set("name", bson.VString("alice"))
	doc.Set("age", bson.VInt32(30))

	if err := wi.AddEntry(doc); err != nil {
		t.Fatalf("AddEntry: %v", err)
	}

	// Lookup by name
	ids := wi.LookupField("name", bson.VString("alice"))
	if len(ids) != 1 {
		t.Fatalf("LookupField name = %d, want 1", len(ids))
	}

	// Lookup by age
	ids = wi.LookupField("age", bson.VInt32(30))
	if len(ids) != 1 {
		t.Errorf("LookupField age = %d, want 1", len(ids))
	}

	// Non-matching value
	ids = wi.LookupField("name", bson.VString("bob"))
	if len(ids) != 0 {
		t.Errorf("LookupField name=bob = %d, want 0", len(ids))
	}
}

func TestWildcardIndex_RemoveEntry(t *testing.T) {
	_, wi := setupWildcardIndexTest(t)

	oid := bson.NewObjectID()
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(oid))
	doc.Set("title", bson.VString("hello"))

	wi.AddEntry(doc)

	ids := wi.LookupField("title", bson.VString("hello"))
	if len(ids) != 1 {
		t.Fatalf("before remove: count = %d, want 1", len(ids))
	}

	wi.RemoveEntry(doc)

	ids = wi.LookupField("title", bson.VString("hello"))
	if len(ids) != 0 {
		t.Errorf("after remove: count = %d, want 0", len(ids))
	}
}

func TestWildcardIndex_NestedFields(t *testing.T) {
	_, wi := setupWildcardIndexTest(t)

	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))

	inner := bson.NewDocument()
	inner.Set("city", bson.VString("Istanbul"))
	inner.Set("zip", bson.VString("34000"))
	doc.Set("address", bson.VDoc(inner))

	wi.AddEntry(doc)

	// Lookup nested field
	ids := wi.LookupField("address.city", bson.VString("Istanbul"))
	if len(ids) != 1 {
		t.Errorf("LookupField address.city = %d, want 1", len(ids))
	}

	ids = wi.LookupField("address.zip", bson.VString("34000"))
	if len(ids) != 1 {
		t.Errorf("LookupField address.zip = %d, want 1", len(ids))
	}
}

func TestWildcardIndex_SkipsID(t *testing.T) {
	eng, wi := setupWildcardIndexTest(t)
	_ = eng

	doc := bson.NewDocument()
	oid := bson.NewObjectID()
	doc.Set("_id", bson.VObjectID(oid))
	doc.Set("value", bson.VInt32(42))

	wi.AddEntry(doc)

	// _id should NOT be indexed as a wildcard field
	prefix := wi.wcIndexPrefix()
	count := 0
	eng.Scan(prefix, func(_, _ []byte) bool {
		count++
		return true
	})

	// Only "value" should be indexed (not "_id")
	if count != 1 {
		t.Errorf("wildcard entry count = %d, want 1 (only 'value', not '_id')", count)
	}
}
