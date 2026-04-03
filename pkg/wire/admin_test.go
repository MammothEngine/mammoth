package wire

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

func TestHandleListDatabases(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	// Ensure a database exists
	h.cat.EnsureCollection("mydb", "test")

	body := bson.NewDocument()
	body.Set("listDatabases", bson.VInt32(1))
	body.Set("$db", bson.VString("admin"))
	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}

	resp := h.Handle(msg)
	if ok, _ := resp.Get("ok"); ok.Double() != 1.0 {
		t.Errorf("listDatabases ok = %v, want 1.0", ok.Double())
	}
	databasesVal, ok := resp.Get("databases")
	if !ok || databasesVal.Type != bson.TypeArray {
		t.Error("listDatabases should include databases array")
	}
}

func TestHandleListCollections(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	h.cat.EnsureCollection("testdb", "users")
	h.cat.EnsureCollection("testdb", "orders")

	body := bson.NewDocument()
	body.Set("listCollections", bson.VInt32(1))
	body.Set("$db", bson.VString("testdb"))
	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}

	resp := h.Handle(msg)
	cursorVal, ok := resp.Get("cursor")
	if !ok || cursorVal.Type != bson.TypeDocument {
		t.Fatal("listCollections should have cursor")
	}
	cursorDoc := cursorVal.DocumentValue()
	batchVal, _ := cursorDoc.Get("firstBatch")
	if batchVal.Type != bson.TypeArray || len(batchVal.ArrayValue()) != 2 {
		t.Errorf("listCollections batch = %d, want 2", len(batchVal.ArrayValue()))
	}
}

func TestHandleCreate(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	body := bson.NewDocument()
	body.Set("create", bson.VString("newcoll"))
	body.Set("$db", bson.VString("testdb"))
	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}

	resp := h.Handle(msg)
	if ok, _ := resp.Get("ok"); ok.Double() != 1.0 {
		t.Errorf("create ok = %v, want 1.0", ok.Double())
	}

	// Verify collection exists
	colls, _ := h.cat.ListCollections("testdb")
	found := false
	for _, c := range colls {
		if c.Name == "newcoll" {
			found = true
		}
	}
	if !found {
		t.Error("newcoll should exist after create")
	}
}

func TestHandleDrop(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	h.cat.EnsureCollection("testdb", "tempcoll")

	body := bson.NewDocument()
	body.Set("drop", bson.VString("tempcoll"))
	body.Set("$db", bson.VString("testdb"))
	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}

	resp := h.Handle(msg)
	if ok, _ := resp.Get("ok"); ok.Double() != 1.0 {
		t.Errorf("drop ok = %v, want 1.0", ok.Double())
	}

	// Verify collection is gone
	colls, _ := h.cat.ListCollections("testdb")
	for _, c := range colls {
		if c.Name == "tempcoll" {
			t.Error("tempcoll should not exist after drop")
		}
	}
}

func TestHandleDropDatabase(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	h.cat.EnsureCollection("tempdb", "coll1")

	body := bson.NewDocument()
	body.Set("dropDatabase", bson.VInt32(1))
	body.Set("$db", bson.VString("tempdb"))
	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}

	resp := h.Handle(msg)
	if ok, _ := resp.Get("ok"); ok.Double() != 1.0 {
		t.Errorf("dropDatabase ok = %v, want 1.0", ok.Double())
	}
	if dropped, _ := resp.Get("dropped"); dropped.String() != "tempdb" {
		t.Errorf("dropDatabase dropped = %q, want tempdb", dropped.String())
	}
}

func TestHandleServerStatus(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	body := bson.NewDocument()
	body.Set("serverStatus", bson.VInt32(1))
	body.Set("$db", bson.VString("admin"))
	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}

	resp := h.Handle(msg)
	if ok, _ := resp.Get("ok"); ok.Double() != 1.0 {
		t.Errorf("serverStatus ok = %v, want 1.0", ok.Double())
	}
	if v, _ := resp.Get("version"); v.String() != "7.0.0" {
		t.Errorf("serverStatus version = %q, want 7.0.0", v.String())
	}
}

func TestHandleCreateIndexes(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	h.cat.EnsureCollection("testdb", "testcoll")

	// Create index spec
	keyDoc := bson.NewDocument()
	keyDoc.Set("name", bson.VInt32(1))

	idxDoc := bson.NewDocument()
	idxDoc.Set("name", bson.VString("name_idx"))
	idxDoc.Set("key", bson.VDoc(keyDoc))
	idxDoc.Set("unique", bson.VBool(true))

	indexes := bson.A(bson.VDoc(idxDoc))

	body := bson.NewDocument()
	body.Set("createIndexes", bson.VString("testcoll"))
	body.Set("$db", bson.VString("testdb"))
	body.Set("indexes", bson.VArray(indexes))

	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}

	resp := h.Handle(msg)
	if ok, _ := resp.Get("ok"); ok.Double() != 1.0 {
		t.Errorf("createIndexes ok = %v, want 1.0", ok.Double())
	}
}

func TestHandleDropIndexes(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	h.cat.EnsureCollection("testdb", "testcoll")

	body := bson.NewDocument()
	body.Set("dropIndexes", bson.VString("testcoll"))
	body.Set("$db", bson.VString("testdb"))
	body.Set("index", bson.VString("name_idx"))

	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}

	resp := h.Handle(msg)
	// May fail if index doesn't exist, but should execute
	_, hasOk := resp.Get("ok")
	if !hasOk {
		t.Error("dropIndexes should return ok field")
	}
}

func TestHandleListIndexes(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	h.cat.EnsureCollection("testdb", "testcoll")

	body := bson.NewDocument()
	body.Set("listIndexes", bson.VString("testcoll"))
	body.Set("$db", bson.VString("testdb"))

	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}

	resp := h.Handle(msg)
	if ok, _ := resp.Get("ok"); ok.Double() != 1.0 {
		t.Errorf("listIndexes ok = %v, want 1.0", ok.Double())
	}

	cursorVal, ok := resp.Get("cursor")
	if !ok || cursorVal.Type != bson.TypeDocument {
		t.Fatal("listIndexes should have cursor")
	}
}

func TestHandleCollMod(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	h.cat.EnsureCollection("testdb", "testcoll")

	body := bson.NewDocument()
	body.Set("collMod", bson.VString("testcoll"))
	body.Set("$db", bson.VString("testdb"))

	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}

	resp := h.Handle(msg)
	if ok, _ := resp.Get("ok"); ok.Double() != 1.0 {
		t.Errorf("collMod ok = %v, want 1.0", ok.Double())
	}
}

func TestHandleCollMod_WithValidator(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	h.cat.EnsureCollection("testdb", "validatedcoll")

	// Create validator document
	validatorDoc := bson.NewDocument()
	validatorDoc.Set("name", bson.VDoc(bson.D("$exists", bson.VBool(true))))

	body := bson.NewDocument()
	body.Set("collMod", bson.VString("validatedcoll"))
	body.Set("$db", bson.VString("testdb"))
	body.Set("validator", bson.VDoc(validatorDoc))
	body.Set("validationLevel", bson.VString("strict"))
	body.Set("validationAction", bson.VString("error"))

	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}

	resp := h.Handle(msg)
	if ok, _ := resp.Get("ok"); ok.Double() != 1.0 {
		t.Errorf("collMod with validator ok = %v, want 1.0", ok.Double())
	}
}

func TestHandleCollMod_IndexParams(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	h.cat.EnsureCollection("testdb", "testcoll")

	body := bson.NewDocument()
	body.Set("collMod", bson.VString("testcoll"))
	body.Set("$db", bson.VString("testdb"))
	body.Set("index", bson.VDoc(bson.D(
		"keyPattern", bson.VDoc(bson.D("name", bson.VInt32(1))),
		"expireAfterSeconds", bson.VInt32(3600),
	)))

	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}

	resp := h.Handle(msg)
	// May succeed or fail depending on implementation, but should not panic
	_, hasOk := resp.Get("ok")
	if !hasOk {
		t.Error("collMod with index should return ok field")
	}
}

func TestHandleCollMod_NoCollection(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	body := bson.NewDocument()
	body.Set("collMod", bson.VString("nonexistentcoll"))
	body.Set("$db", bson.VString("testdb"))

	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}

	resp := h.Handle(msg)
	// Should fail since collection doesn't exist
	okVal, _ := resp.Get("ok")
	if okVal.Double() == 1.0 {
		t.Error("collMod on non-existent collection should fail")
	}
}

func TestHandleCollMod_NoDB(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	body := bson.NewDocument()
	body.Set("collMod", bson.VString("testcoll"))
	// Missing $db

	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}

	resp := h.Handle(msg)
	// Should fail since no database specified
	okVal, _ := resp.Get("ok")
	if okVal.Double() == 1.0 {
		t.Error("collMod without $db should fail")
	}
}

func TestHandleCollStats(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	// Insert a document using the proper namespace encoding
	h.cat.EnsureCollection("testdb", "testcoll")
	doc := bson.NewDocument()
	doc.Set("_id", bson.VInt32(1))
	doc.Set("name", bson.VString("test"))
	// Use proper namespace prefix encoding
	prefix := mongo.EncodeNamespacePrefix("testdb", "testcoll")
	key := append(prefix, []byte("1")...)
	h.engine.Put(key, bson.Encode(doc))

	body := bson.NewDocument()
	body.Set("collStats", bson.VString("testcoll"))
	body.Set("$db", bson.VString("testdb"))

	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}

	resp := h.Handle(msg)
	if ok, _ := resp.Get("ok"); ok.Double() != 1.0 {
		t.Errorf("collStats ok = %v, want 1.0", ok.Double())
	}

	// Verify ns field
	if ns, _ := resp.Get("ns"); ns.String() != "testdb.testcoll" {
		t.Errorf("collStats ns = %s, want testdb.testcoll", ns.String())
	}
}

func TestHandleDbStats(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	// Insert documents
	h.cat.EnsureCollection("testdb", "coll1")
	h.cat.EnsureCollection("testdb", "coll2")

	body := bson.NewDocument()
	body.Set("dbStats", bson.VInt32(1))
	body.Set("$db", bson.VString("testdb"))

	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}

	resp := h.Handle(msg)
	if ok, _ := resp.Get("ok"); ok.Double() != 1.0 {
		t.Errorf("dbStats ok = %v, want 1.0", ok.Double())
	}

	// Verify stats fields
	if collections, _ := resp.Get("collections"); collections.Int32() != 2 {
		t.Errorf("dbStats collections = %d, want 2", collections.Int32())
	}
}

func TestHandleCreate_Capped(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	body := bson.NewDocument()
	body.Set("create", bson.VString("cappedcoll"))
	body.Set("$db", bson.VString("testdb"))
	body.Set("capped", bson.VBool(true))
	body.Set("size", bson.VInt64(10000))
	body.Set("max", bson.VInt64(100))

	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}

	resp := h.Handle(msg)
	if ok, _ := resp.Get("ok"); ok.Double() != 1.0 {
		t.Errorf("create (capped) ok = %v, want 1.0", ok.Double())
	}
}

func TestHandleListCollections_NoDB(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	body := bson.NewDocument()
	body.Set("listCollections", bson.VInt32(1))
	// Missing $db

	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}

	resp := h.Handle(msg)
	if ok, _ := resp.Get("ok"); ok.Double() != 0.0 {
		t.Errorf("listCollections (no db) ok = %v, want 0.0", ok.Double())
	}
}

func TestHandleDrop_NoDB(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	body := bson.NewDocument()
	body.Set("drop", bson.VString("coll"))
	// Missing $db

	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}

	resp := h.Handle(msg)
	if ok, _ := resp.Get("ok"); ok.Double() != 0.0 {
		t.Errorf("drop (no db) ok = %v, want 0.0", ok.Double())
	}
}
