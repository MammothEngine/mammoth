package wire

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

func makeInsertMsg(db, coll string, docs ...*bson.Document) *Message {
	body := bson.NewDocument()
	body.Set("insert", bson.VString(coll))
	body.Set("$db", bson.VString(db))

	var arr bson.Array
	for _, d := range docs {
		arr = append(arr, bson.VDoc(d))
	}
	body.Set("documents", bson.VArray(arr))

	return &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}
}

func makeFindMsg(db, coll string, filter *bson.Document) *Message {
	body := bson.NewDocument()
	body.Set("find", bson.VString(coll))
	body.Set("$db", bson.VString(db))
	if filter != nil {
		body.Set("filter", bson.VDoc(filter))
	}
	return &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}
}

func getCursorDoc(t *testing.T, resp *bson.Document) *bson.Document {
	t.Helper()
	v, ok := resp.Get("cursor")
	if !ok || v.Type != bson.TypeDocument {
		t.Fatal("response missing cursor document")
	}
	return v.DocumentValue()
}

func getCursorBatch(t *testing.T, cursorDoc *bson.Document, key string) bson.Array {
	t.Helper()
	v, ok := cursorDoc.Get(key)
	if !ok || v.Type != bson.TypeArray {
		t.Fatalf("cursor missing %q array", key)
	}
	return v.ArrayValue()
}

func TestHandleInsert(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	doc := bson.NewDocument()
	doc.Set("name", bson.VString("alice"))
	doc.Set("age", bson.VInt32(30))

	msg := makeInsertMsg("testdb", "users", doc)
	resp := h.Handle(msg)

	if ok, _ := resp.Get("ok"); ok.Double() != 1.0 {
		t.Errorf("insert ok = %v, want 1.0", ok.Double())
	}
	if n, _ := resp.Get("n"); n.Int32() != 1 {
		t.Errorf("insert n = %d, want 1", n.Int32())
	}
}

func TestHandleInsertMany(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	var docs []*bson.Document
	for i := 0; i < 3; i++ {
		d := bson.NewDocument()
		d.Set("i", bson.VInt32(int32(i)))
		docs = append(docs, d)
	}

	msg := makeInsertMsg("testdb", "items", docs...)
	resp := h.Handle(msg)

	if ok, _ := resp.Get("ok"); ok.Double() != 1.0 {
		t.Errorf("insertMany ok = %v, want 1.0", ok.Double())
	}
	if n, _ := resp.Get("n"); n.Int32() != 3 {
		t.Errorf("insertMany n = %d, want 3", n.Int32())
	}
}

func TestHandleFind(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	doc := bson.NewDocument()
	doc.Set("name", bson.VString("alice"))
	h.Handle(makeInsertMsg("testdb", "users", doc))

	msg := makeFindMsg("testdb", "users", nil)
	resp := h.Handle(msg)

	cursorDoc := getCursorDoc(t, resp)
	batch := getCursorBatch(t, cursorDoc, "firstBatch")
	if len(batch) != 1 {
		t.Fatalf("find batch length = %d, want 1", len(batch))
	}
}

func TestHandleFindWithFilter(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	d1 := bson.NewDocument()
	d1.Set("name", bson.VString("alice"))
	d1.Set("age", bson.VInt32(25))
	h.Handle(makeInsertMsg("testdb", "users", d1))

	d2 := bson.NewDocument()
	d2.Set("name", bson.VString("bob"))
	d2.Set("age", bson.VInt32(30))
	h.Handle(makeInsertMsg("testdb", "users", d2))

	filter := bson.NewDocument()
	filter.Set("name", bson.VString("alice"))
	msg := makeFindMsg("testdb", "users", filter)
	resp := h.Handle(msg)

	cursorDoc := getCursorDoc(t, resp)
	batch := getCursorBatch(t, cursorDoc, "firstBatch")
	if len(batch) != 1 {
		t.Fatalf("filtered find: batch length = %d, want 1", len(batch))
	}
}

func TestHandleFindWithLimit(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	for i := 0; i < 5; i++ {
		d := bson.NewDocument()
		d.Set("i", bson.VInt32(int32(i)))
		h.Handle(makeInsertMsg("testdb", "nums", d))
	}

	body := bson.NewDocument()
	body.Set("find", bson.VString("nums"))
	body.Set("$db", bson.VString("testdb"))
	body.Set("limit", bson.VInt32(2))
	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}

	resp := h.Handle(msg)
	cursorDoc := getCursorDoc(t, resp)
	batch := getCursorBatch(t, cursorDoc, "firstBatch")
	if len(batch) != 2 {
		t.Errorf("limit 2: batch length = %d, want 2", len(batch))
	}
}

func TestHandleUpdate(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	doc := bson.NewDocument()
	doc.Set("name", bson.VString("charlie"))
	doc.Set("age", bson.VInt32(20))
	h.Handle(makeInsertMsg("testdb", "users", doc))

	q := bson.NewDocument()
	q.Set("name", bson.VString("charlie"))

	setDoc := bson.NewDocument()
	setDoc.Set("age", bson.VInt32(21))
	u := bson.NewDocument()
	u.Set("$set", bson.VDoc(setDoc))

	updateEntry := bson.NewDocument()
	updateEntry.Set("q", bson.VDoc(q))
	updateEntry.Set("u", bson.VDoc(u))

	body := bson.NewDocument()
	body.Set("update", bson.VString("users"))
	body.Set("$db", bson.VString("testdb"))
	body.Set("updates", bson.VArray(bson.A(bson.VDoc(updateEntry))))
	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}

	resp := h.Handle(msg)
	if n, _ := resp.Get("n"); n.Int32() != 1 {
		t.Errorf("update matched n = %d, want 1", n.Int32())
	}
}

func TestHandleDelete(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	doc := bson.NewDocument()
	doc.Set("name", bson.VString("dave"))
	h.Handle(makeInsertMsg("testdb", "users", doc))

	q := bson.NewDocument()
	q.Set("name", bson.VString("dave"))

	delEntry := bson.NewDocument()
	delEntry.Set("q", bson.VDoc(q))

	body := bson.NewDocument()
	body.Set("delete", bson.VString("users"))
	body.Set("$db", bson.VString("testdb"))
	body.Set("deletes", bson.VArray(bson.A(bson.VDoc(delEntry))))
	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}

	resp := h.Handle(msg)
	if n, _ := resp.Get("n"); n.Int32() != 1 {
		t.Errorf("delete n = %d, want 1", n.Int32())
	}
}

func TestHandleGetMore(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	for i := 0; i < 5; i++ {
		d := bson.NewDocument()
		d.Set("i", bson.VInt32(int32(i)))
		h.Handle(makeInsertMsg("testdb", "nums", d))
	}

	body := bson.NewDocument()
	body.Set("find", bson.VString("nums"))
	body.Set("$db", bson.VString("testdb"))
	body.Set("batchSize", bson.VInt32(2))
	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}

	resp := h.Handle(msg)
	cursorDoc := getCursorDoc(t, resp)
	cursorIDVal, _ := cursorDoc.Get("id")
	var cursorID int64
	if cursorIDVal.Type == bson.TypeInt64 {
		cursorID = cursorIDVal.Int64()
	}

	if cursorID == 0 {
		t.Fatal("cursor should have non-zero ID when more results remain")
	}

	getMoreBody := bson.NewDocument()
	getMoreBody.Set("getMore", bson.VInt64(cursorID))
	getMoreBody.Set("collection", bson.VString("nums"))
	getMoreBody.Set("batchSize", bson.VInt32(2))
	getMoreMsg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: getMoreBody}}},
	}

	resp2 := h.Handle(getMoreMsg)
	cursorDoc2 := getCursorDoc(t, resp2)
	nextBatch := getCursorBatch(t, cursorDoc2, "nextBatch")
	if len(nextBatch) != 2 {
		t.Errorf("getMore batch length = %d, want 2", len(nextBatch))
	}
}

func TestHandleKillCursors(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	for i := 0; i < 5; i++ {
		d := bson.NewDocument()
		d.Set("i", bson.VInt32(int32(i)))
		h.Handle(makeInsertMsg("testdb", "nums", d))
	}

	body := bson.NewDocument()
	body.Set("find", bson.VString("nums"))
	body.Set("$db", bson.VString("testdb"))
	body.Set("batchSize", bson.VInt32(1))
	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}
	resp := h.Handle(msg)
	cursorDoc := getCursorDoc(t, resp)
	cursorIDVal, _ := cursorDoc.Get("id")

	killBody := bson.NewDocument()
	killBody.Set("killCursors", bson.VString("nums"))
	killBody.Set("cursors", bson.VArray(bson.A(cursorIDVal)))
	killMsg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: killBody}}},
	}

	resp2 := h.Handle(killMsg)
	if ok, _ := resp2.Get("ok"); ok.Double() != 1.0 {
		t.Errorf("killCursors ok = %v, want 1.0", ok.Double())
	}
}
