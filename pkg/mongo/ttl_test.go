package mongo

import (
	"testing"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

func TestTTLIndex(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	_ = cat.EnsureCollection("test", "logs")
	indexCat := NewIndexCatalog(eng, cat)
	coll := NewCollection("test", "logs", eng, cat)

	// Insert old doc (2 hours ago)
	oldDoc := bson.NewDocument()
	oldDoc.Set("msg", bson.VString("old"))
	oldDoc.Set("createdAt", bson.VDateTime(time.Now().Add(-2 * time.Hour).UnixMilli()))
	if err := coll.InsertOne(oldDoc); err != nil {
		t.Fatal(err)
	}

	// Insert new doc (now)
	newDoc := bson.NewDocument()
	newDoc.Set("msg", bson.VString("new"))
	newDoc.Set("createdAt", bson.VDateTime(time.Now().UnixMilli()))
	if err := coll.InsertOne(newDoc); err != nil {
		t.Fatal(err)
	}

	// Create TTL index
	err = indexCat.CreateIndex("test", "logs", IndexSpec{
		Name:               "ttl_idx",
		Key:                []IndexKey{{Field: "createdAt"}},
		ExpireAfterSeconds: 3600,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Simulate TTL expiration: delete old doc by _id
	oldID, _ := oldDoc.Get("_id")
	newID, _ := newDoc.Get("_id")

	if err := coll.DeleteOne(oldID.ObjectID()); err != nil {
		t.Fatal(err)
	}
	indexCat.OnDocumentDelete("test", "logs", oldDoc)

	// Verify old doc is gone
	_, err = coll.FindOne(oldID.ObjectID())
	if err == nil {
		t.Error("expected old document to be expired")
	}

	// Verify new doc still exists
	_, err = coll.FindOne(newID.ObjectID())
	if err != nil {
		t.Error("expected new document to still exist")
	}
}

func TestTTLWorker_StartStop(t *testing.T) {
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	indexCat := NewIndexCatalog(eng, cat)

	worker := NewTTLWorker(eng, cat, indexCat)

	// Start should not panic
	worker.Start()

	// Stop should not panic
	worker.Stop()

	// Stop again should be safe (no-op)
	worker.Stop()
}

func TestTTLWorker_ExpireDocs(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	indexCat := NewIndexCatalog(eng, cat)

	// Create database and collection
	if err := cat.CreateDatabase("testdb"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	if err := cat.CreateCollection("testdb", "events"); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	// Create TTL index
	ttlIndex := IndexSpec{
		Name:               "created_at_ttl",
		Key:                []IndexKey{{Field: "created_at"}},
		ExpireAfterSeconds: 1, // 1 second TTL
	}
	if err := indexCat.CreateIndex("testdb", "events", ttlIndex); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	// Insert a document with old timestamp
	oldTime := time.Now().Add(-2 * time.Second).UnixMilli()
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("created_at", bson.VDateTime(oldTime))
	doc.Set("data", bson.VString("test"))

	coll := NewCollection("testdb", "events", eng, cat)
	if err := coll.InsertOne(doc); err != nil {
		t.Fatalf("InsertOne: %v", err)
	}

	// Verify document exists by counting
	count, _ := coll.Count()
	if count != 1 {
		t.Fatalf("Expected 1 doc, got %d", count)
	}

	// Run TTL worker
	worker := NewTTLWorker(eng, cat, indexCat)
	worker.expireDocs()

	// Document should be expired
	count, _ = coll.Count()
	if count != 0 {
		t.Errorf("Expected 0 docs after TTL, got %d", count)
	}
}

func TestTTLWorker_NoExpireRecentDocs(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	indexCat := NewIndexCatalog(eng, cat)

	// Create database and collection
	if err := cat.CreateDatabase("testdb"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	if err := cat.CreateCollection("testdb", "events"); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	// Create TTL index with long TTL
	ttlIndex := IndexSpec{
		Name:               "created_at_ttl",
		Key:                []IndexKey{{Field: "created_at"}},
		ExpireAfterSeconds: 3600, // 1 hour TTL
	}
	if err := indexCat.CreateIndex("testdb", "events", ttlIndex); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	// Insert a document with recent timestamp
	recentTime := time.Now().UnixMilli()
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("created_at", bson.VDateTime(recentTime))
	doc.Set("data", bson.VString("test"))

	coll := NewCollection("testdb", "events", eng, cat)
	if err := coll.InsertOne(doc); err != nil {
		t.Fatalf("InsertOne: %v", err)
	}

	// Run TTL worker
	worker := NewTTLWorker(eng, cat, indexCat)
	worker.expireDocs()

	// Document should NOT be expired (TTL is 1 hour)
	count, _ := coll.Count()
	if count != 1 {
		t.Errorf("Expected 1 doc (not expired), got %d", count)
	}
}

func TestTTLWorker_NoTTLIndex(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	indexCat := NewIndexCatalog(eng, cat)

	// Create database and collection WITHOUT TTL index
	if err := cat.CreateDatabase("testdb"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	if err := cat.CreateCollection("testdb", "events"); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	// Insert a document
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("data", bson.VString("test"))

	coll := NewCollection("testdb", "events", eng, cat)
	if err := coll.InsertOne(doc); err != nil {
		t.Fatalf("InsertOne: %v", err)
	}

	// Run TTL worker
	worker := NewTTLWorker(eng, cat, indexCat)
	worker.expireDocs()

	// Document should still exist
	count, _ := coll.Count()
	if count != 1 {
		t.Errorf("Expected 1 doc (no TTL index), got %d", count)
	}
}

func TestTTLWorker_NonDateTimeField(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	indexCat := NewIndexCatalog(eng, cat)

	// Create database and collection
	if err := cat.CreateDatabase("testdb"); err != nil {
		t.Fatalf("CreateDatabase: %v", err)
	}
	if err := cat.CreateCollection("testdb", "events"); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	// Create TTL index
	ttlIndex := IndexSpec{
		Name:               "created_at_ttl",
		Key:                []IndexKey{{Field: "created_at"}},
		ExpireAfterSeconds: 1,
	}
	if err := indexCat.CreateIndex("testdb", "events", ttlIndex); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	// Insert a document with non-DateTime field
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("created_at", bson.VString("not a date"))
	doc.Set("data", bson.VString("test"))

	coll := NewCollection("testdb", "events", eng, cat)
	if err := coll.InsertOne(doc); err != nil {
		t.Fatalf("InsertOne: %v", err)
	}

	// Run TTL worker
	worker := NewTTLWorker(eng, cat, indexCat)
	worker.expireDocs()

	// Document should still exist (field is not DateTime)
	count, _ := coll.Count()
	if count != 1 {
		t.Errorf("Expected 1 doc (non-DateTime field), got %d", count)
	}
}
