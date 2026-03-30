package mongo

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

func TestCappedEnforceDocLimit(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "mammoth_capped_test")
	os.RemoveAll(dir)
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	defer os.RemoveAll(dir)

	cat := NewCatalog(eng)
	_ = cat.EnsureDatabase("testdb")
	_ = cat.CreateCollectionWithInfo("testdb", "logs", CollectionInfo{
		DB:      "testdb",
		Name:    "logs",
		Capped:  true,
		MaxDocs: 3,
	})

	coll := NewCollection("testdb", "logs", eng, cat)
	capped := NewCappedCollection("testdb", "logs", eng, cat, 0, 3)

	// Insert 5 docs
	for i := 0; i < 5; i++ {
		doc := bson.D("_id", bson.VInt32(int32(i)), "msg", bson.VString("log entry"))
		coll.InsertOne(doc)
	}

	// Enforce limit
	removed, err := capped.EnforceLimits()
	if err != nil {
		t.Fatal(err)
	}
	if removed != 2 {
		t.Errorf("expected 2 docs removed, got %d", removed)
	}

	// Verify count
	count, _ := coll.Count()
	if count != 3 {
		t.Errorf("expected 3 docs remaining, got %d", count)
	}
}

func TestCappedIsCapped(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "mammoth_capped_check")
	os.RemoveAll(dir)
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	defer os.RemoveAll(dir)

	cat := NewCatalog(eng)
	_ = cat.EnsureDatabase("testdb")
	_ = cat.CreateCollectionWithInfo("testdb", "capped_coll", CollectionInfo{
		DB:      "testdb",
		Name:    "capped_coll",
		Capped:  true,
		MaxSize: 1024,
	})
	_ = cat.CreateCollection("testdb", "normal_coll")

	if !IsCapped(cat, "testdb", "capped_coll") {
		t.Error("capped_coll should be capped")
	}
	if IsCapped(cat, "testdb", "normal_coll") {
		t.Error("normal_coll should not be capped")
	}
}
