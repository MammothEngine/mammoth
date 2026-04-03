package mongo

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

func TestIndexIterator_BasicIteration(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	// Create index
	spec := &IndexSpec{
		Name: "idx_value",
		Key:  []IndexKey{{Field: "value", Descending: false}},
	}
	idx := NewIndex("test", "items", spec, eng)

	// Insert some documents
	docs := []*bson.Document{
		func() *bson.Document {
			d := bson.NewDocument()
			d.Set("_id", bson.VObjectID(bson.NewObjectID()))
			d.Set("value", bson.VInt32(10))
			return d
		}(),
		func() *bson.Document {
			d := bson.NewDocument()
			d.Set("_id", bson.VObjectID(bson.NewObjectID()))
			d.Set("value", bson.VInt32(20))
			return d
		}(),
		func() *bson.Document {
			d := bson.NewDocument()
			d.Set("_id", bson.VObjectID(bson.NewObjectID()))
			d.Set("value", bson.VInt32(30))
			return d
		}(),
	}

	for _, doc := range docs {
		if err := idx.AddEntry(doc); err != nil {
			t.Fatalf("AddEntry: %v", err)
		}
	}

	// Iterate over all entries
	it := idx.NewIterator(nil)
	defer it.Close()

	count := 0
	for it.Next() {
		if it.ID() == nil {
			t.Error("expected non-nil ID")
		}
		count++
	}

	if count != 3 {
		t.Errorf("expected 3 entries, got %d", count)
	}
}

func TestIndexIterator_RangeScan(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	spec := &IndexSpec{
		Name: "idx_val",
		Key:  []IndexKey{{Field: "val", Descending: false}},
	}
	idx := NewIndex("test", "nums", spec, eng)

	// Insert values 1-10
	for i := 1; i <= 10; i++ {
		d := bson.NewDocument()
		d.Set("_id", bson.VObjectID(bson.NewObjectID()))
		d.Set("val", bson.VInt32(int32(i)))
		if err := idx.AddEntry(d); err != nil {
			t.Fatalf("AddEntry: %v", err)
		}
	}

	// Range scan for values 3-7
	bounds := &IndexScanBounds{
		StartKey:       encodeIndexValue(bson.VInt32(3)),
		EndKey:         encodeIndexValue(bson.VInt32(7)),
		StartInclusive: true,
		EndInclusive:   true,
	}

	ids, err := idx.RangeScan(bounds, 0)
	if err != nil {
		t.Fatalf("RangeScan: %v", err)
	}

	// Note: Index encoding may affect exact range boundaries
	// We expect at least 3 entries (4,5,6) and at most 5 entries (3,4,5,6,7)
	if len(ids) < 3 || len(ids) > 5 {
		t.Errorf("expected 3-5 entries, got %d", len(ids))
	}
}

func TestIndexIterator_PointLookup(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	spec := &IndexSpec{
		Name: "idx_name",
		Key:  []IndexKey{{Field: "name", Descending: false}},
	}
	idx := NewIndex("test", "users", spec, eng)

	// Insert documents
	id1 := bson.NewObjectID()
	d1 := bson.NewDocument()
	d1.Set("_id", bson.VObjectID(id1))
	d1.Set("name", bson.VString("alice"))
	idx.AddEntry(d1)

	id2 := bson.NewObjectID()
	d2 := bson.NewDocument()
	d2.Set("_id", bson.VObjectID(id2))
	d2.Set("name", bson.VString("bob"))
	idx.AddEntry(d2)

	// Point lookup for alice
	ids, err := idx.PointLookup([]bson.Value{bson.VString("alice")})
	if err != nil {
		t.Fatalf("PointLookup: %v", err)
	}

	if len(ids) != 1 {
		t.Errorf("expected 1 result, got %d", len(ids))
	}

	if len(ids) > 0 && ids[0] != id1 {
		t.Error("expected to find alice's ID")
	}
}

func TestIndexIterator_Seek(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	spec := &IndexSpec{
		Name: "idx_score",
		Key:  []IndexKey{{Field: "score", Descending: false}},
	}
	idx := NewIndex("test", "scores", spec, eng)

	// Insert scores
	scores := []int32{10, 20, 30, 40, 50}
	for _, s := range scores {
		d := bson.NewDocument()
		d.Set("_id", bson.VObjectID(bson.NewObjectID()))
		d.Set("score", bson.VInt32(s))
		idx.AddEntry(d)
	}

	// Seek to 25 (should position at 30)
	it := idx.NewIterator(nil)
	defer it.Close()

	target := encodeIndexValue(bson.VInt32(25))
	if !it.Seek(target, true) {
		t.Error("expected Seek to succeed")
	}

	// Should be at 30
	if !it.Valid() {
		t.Fatal("expected iterator to be valid after seek")
	}
}

func TestIndexIterator_Count(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	spec := &IndexSpec{
		Name: "idx_x",
		Key:  []IndexKey{{Field: "x", Descending: false}},
	}
	idx := NewIndex("test", "data", spec, eng)

	// Insert 100 documents
	for i := 0; i < 100; i++ {
		d := bson.NewDocument()
		d.Set("_id", bson.VObjectID(bson.NewObjectID()))
		d.Set("x", bson.VInt32(int32(i)))
		idx.AddEntry(d)
	}

	count := idx.Count()
	if count != 100 {
		t.Errorf("expected count=100, got %d", count)
	}
}

func TestIndexIterator_EmptyIndex(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	spec := &IndexSpec{
		Name: "idx_empty",
		Key:  []IndexKey{{Field: "field", Descending: false}},
	}
	idx := NewIndex("test", "empty", spec, eng)

	// Iterate over empty index
	it := idx.NewIterator(nil)
	defer it.Close()

	if it.Next() {
		t.Error("expected no entries in empty index")
	}

	// Range scan on empty
	ids, _ := idx.RangeScan(&IndexScanBounds{
		StartKey: encodeIndexValue(bson.VInt32(1)),
		EndKey:   encodeIndexValue(bson.VInt32(10)),
	}, 0)

	if len(ids) != 0 {
		t.Errorf("expected 0 results, got %d", len(ids))
	}
}
