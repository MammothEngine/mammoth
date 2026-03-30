package wire

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

func makeFAMMsg(db, coll string, pairs ...interface{}) *Message {
	body := bson.NewDocument()
	body.Set("findAndModify", bson.VString(coll))
	body.Set("$db", bson.VString(db))
	for i := 0; i+1 < len(pairs); i += 2 {
		body.Set(pairs[i].(string), pairs[i+1].(bson.Value))
	}
	return &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}
}

func TestFindAndModify_Update(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	// Insert a document
	h.Handle(makeInsertMsg("test", "users", bson.D("name", bson.VString("Alice"), "age", bson.VInt32(30))))

	// findAndModify: update Alice's age
	query := bson.NewDocument()
	query.Set("name", bson.VString("Alice"))
	update := bson.NewDocument()
	update.Set("$set", bson.VDoc(bson.D("age", bson.VInt32(31))))

	resp := h.Handle(makeFAMMsg("test", "users",
		"query", bson.VDoc(query),
		"update", bson.VDoc(update),
	))
	if ok, _ := resp.Get("ok"); ok.Double() != 1 {
		t.Fatalf("expected ok=1, got %v", resp)
	}
	val, _ := resp.Get("value")
	if val.Type != bson.TypeDocument {
		t.Fatalf("expected value to be document, got %v", val.Type)
	}
	oldDoc := val.DocumentValue()
	if age, _ := oldDoc.Get("age"); age.Int32() != 30 {
		t.Fatalf("expected old doc age=30, got %v", age)
	}
}

func TestFindAndModify_Remove(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	h.Handle(makeInsertMsg("test", "users", bson.D("name", bson.VString("Bob"))))

	query := bson.NewDocument()
	query.Set("name", bson.VString("Bob"))

	resp := h.Handle(makeFAMMsg("test", "users",
		"query", bson.VDoc(query),
		"remove", bson.VBool(true),
	))
	if ok, _ := resp.Get("ok"); ok.Double() != 1 {
		t.Fatalf("expected ok=1, got %v", resp)
	}
	val, _ := resp.Get("value")
	if val.Type != bson.TypeDocument {
		t.Fatalf("expected value to be document, got %v", val.Type)
	}

	// Verify doc is gone
	findResp := h.Handle(makeFindMsg("test", "users", query))
	cursorDoc := getCursorDoc(t, findResp)
	batch := getCursorBatch(t, cursorDoc, "firstBatch")
	if len(batch) != 0 {
		t.Fatalf("expected 0 docs after remove, got %d", len(batch))
	}
}

func TestFindAndModify_Upsert(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	query := bson.NewDocument()
	query.Set("name", bson.VString("Charlie"))
	update := bson.NewDocument()
	update.Set("$set", bson.VDoc(bson.D("status", bson.VString("active"))))

	resp := h.Handle(makeFAMMsg("test", "users",
		"query", bson.VDoc(query),
		"update", bson.VDoc(update),
		"upsert", bson.VBool(true),
		"new", bson.VBool(true),
	))
	if ok, _ := resp.Get("ok"); ok.Double() != 1 {
		t.Fatalf("expected ok=1, got %v", resp)
	}
	val, _ := resp.Get("value")
	if val.Type != bson.TypeDocument {
		t.Fatalf("expected value to be document, got %v", val.Type)
	}
	newDoc := val.DocumentValue()
	if name, _ := newDoc.Get("name"); name.String() != "Charlie" {
		t.Fatalf("expected name=Charlie, got %v", name)
	}
	if status, _ := newDoc.Get("status"); status.String() != "active" {
		t.Fatalf("expected status=active, got %v", status)
	}
}

func TestFindAndModify_ReturnNew(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	h.Handle(makeInsertMsg("test", "users", bson.D("name", bson.VString("Dave"), "age", bson.VInt32(25))))

	query := bson.NewDocument()
	query.Set("name", bson.VString("Dave"))
	update := bson.NewDocument()
	update.Set("$set", bson.VDoc(bson.D("age", bson.VInt32(26))))

	resp := h.Handle(makeFAMMsg("test", "users",
		"query", bson.VDoc(query),
		"update", bson.VDoc(update),
		"new", bson.VBool(true),
	))
	val, _ := resp.Get("value")
	if val.Type != bson.TypeDocument {
		t.Fatalf("expected value to be document, got %v", val.Type)
	}
	newDoc := val.DocumentValue()
	if age, _ := newDoc.Get("age"); age.Int32() != 26 {
		t.Fatalf("expected new doc age=26, got %v", age)
	}
}

func TestFindAndModify_NoMatch(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	query := bson.NewDocument()
	query.Set("name", bson.VString("Nobody"))
	update := bson.NewDocument()
	update.Set("$set", bson.VDoc(bson.D("age", bson.VInt32(99))))

	resp := h.Handle(makeFAMMsg("test", "users",
		"query", bson.VDoc(query),
		"update", bson.VDoc(update),
	))
	if ok, _ := resp.Get("ok"); ok.Double() != 1 {
		t.Fatalf("expected ok=1, got %v", resp)
	}
	val, _ := resp.Get("value")
	if !val.IsNull() {
		t.Fatalf("expected null value for no match, got %v", val)
	}
}
