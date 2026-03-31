package mongo

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

func int32Val(v int32) bson.Value { return bson.VInt32(v) }

func byIDAsc(a, b *bson.Document) bool {
	av, _ := a.Get("_id")
	bv, _ := b.Get("_id")
	return av.Int32() < bv.Int32()
}

func byIDDesc(a, b *bson.Document) bool {
	av, _ := a.Get("_id")
	bv, _ := b.Get("_id")
	return av.Int32() > bv.Int32()
}

func collectIDs(docs []*bson.Document) []int32 {
	ids := make([]int32, len(docs))
	for i, d := range docs {
		v, _ := d.Get("_id")
		ids[i] = v.Int32()
	}
	return ids
}

func TestSortSmallInMemory(t *testing.T) {
	s := NewExternalSorter(0, byIDAsc)
	defer s.Close()

	docs := []*bson.Document{
		bson.D("_id", bson.VInt32(3), "name", bson.VString("charlie")),
		bson.D("_id", bson.VInt32(1), "name", bson.VString("alice")),
		bson.D("_id", bson.VInt32(2), "name", bson.VString("bob")),
	}
	for _, d := range docs {
		if err := s.Add(d); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	result, err := s.Sort()
	if err != nil {
		t.Fatalf("Sort: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 docs, got %d", len(result))
	}
	ids := collectIDs(result)
	if ids[0] != 1 || ids[1] != 2 || ids[2] != 3 {
		t.Fatalf("expected [1 2 3], got %v", ids)
	}
}

func TestSortExceedsMemLimit(t *testing.T) {
	// Very small memory limit to force multiple runs.
	s := NewExternalSorter(64, byIDAsc)
	defer s.Close()

	for i := 50; i >= 1; i-- {
		doc := bson.D("_id", bson.VInt32(int32(i)), "v", bson.VString("x"))
		if err := s.Add(doc); err != nil {
			t.Fatalf("Add(%d): %v", i, err)
		}
	}

	result, err := s.Sort()
	if err != nil {
		t.Fatalf("Sort: %v", err)
	}
	if len(result) != 50 {
		t.Fatalf("expected 50 docs, got %d", len(result))
	}
	for i, d := range result {
		v, _ := d.Get("_id")
		if v.Int32() != int32(i+1) {
			t.Fatalf("position %d: expected _id %d, got %d", i, i+1, v.Int32())
		}
	}
}

func TestSortDescending(t *testing.T) {
	s := NewExternalSorter(0, byIDDesc)
	defer s.Close()

	for _, id := range []int32{5, 1, 9, 3, 7} {
		if err := s.Add(bson.D("_id", bson.VInt32(id))); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	result, err := s.Sort()
	if err != nil {
		t.Fatalf("Sort: %v", err)
	}
	ids := collectIDs(result)
	if ids[0] != 9 || ids[len(ids)-1] != 1 {
		t.Fatalf("expected descending order, got %v", ids)
	}
}

func TestSortEmpty(t *testing.T) {
	s := NewExternalSorter(0, byIDAsc)
	defer s.Close()

	result, err := s.Sort()
	if err != nil {
		t.Fatalf("Sort: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 docs, got %d", len(result))
	}
}

func TestSortPreservesFields(t *testing.T) {
	s := NewExternalSorter(0, byIDAsc)
	defer s.Close()

	if err := s.Add(bson.D("_id", bson.VInt32(2), "name", bson.VString("bob"))); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := s.Add(bson.D("_id", bson.VInt32(1), "name", bson.VString("alice"))); err != nil {
		t.Fatalf("Add: %v", err)
	}

	result, err := s.Sort()
	if err != nil {
		t.Fatalf("Sort: %v", err)
	}
	name, _ := result[0].Get("name")
	if name.String() != "alice" {
		t.Fatalf("expected name=alice for first doc, got %s", name.String())
	}
}
