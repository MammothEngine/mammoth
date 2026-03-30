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
