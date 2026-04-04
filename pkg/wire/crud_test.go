package wire

import (
	"fmt"
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

func TestHandle_Find_SkipLimit(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	// Insert 5 docs
	for i := 0; i < 5; i++ {
		d := bson.NewDocument()
		d.Set("val", bson.VInt32(int32(i)))
		h.Handle(makeInsertMsg("testdb", "nums", d))
	}

	// skip=2, limit=2 → should return 2 docs (skip first 2, take next 2)
	body := bson.NewDocument()
	body.Set("find", bson.VString("nums"))
	body.Set("$db", bson.VString("testdb"))
	body.Set("skip", bson.VInt32(2))
	body.Set("limit", bson.VInt32(2))
	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}

	resp := h.Handle(msg)
	cursorDoc := getCursorDoc(t, resp)
	batch := getCursorBatch(t, cursorDoc, "firstBatch")
	if len(batch) != 2 {
		t.Fatalf("skip=2,limit=2: expected 2 docs, got %d", len(batch))
	}
}

func TestHandle_Update_Upsert(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	// Update with upsert=true on non-existent doc
	q := bson.NewDocument()
	q.Set("name", bson.VString("alice"))

	setDoc := bson.NewDocument()
	setDoc.Set("age", bson.VInt32(25))
	u := bson.NewDocument()
	u.Set("$set", bson.VDoc(setDoc))

	updateEntry := bson.NewDocument()
	updateEntry.Set("q", bson.VDoc(q))
	updateEntry.Set("u", bson.VDoc(u))
	updateEntry.Set("upsert", bson.VBool(true))

	body := bson.NewDocument()
	body.Set("update", bson.VString("users"))
	body.Set("$db", bson.VString("testdb"))
	body.Set("updates", bson.VArray(bson.A(bson.VDoc(updateEntry))))
	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}

	resp := h.Handle(msg)
	// Should have n=0 matched, nModified=0, upserted with _id
	if n, _ := resp.Get("n"); n.Int32() != 0 {
		t.Errorf("upsert n = %d, want 0", n.Int32())
	}
	if nMod, _ := resp.Get("nModified"); nMod.Int32() != 0 {
		t.Errorf("upsert nModified = %d, want 0", nMod.Int32())
	}
	upsertedVal, ok := resp.Get("upserted")
	if !ok || upsertedVal.Type != bson.TypeArray {
		t.Fatal("expected upserted array in response")
	}

	// Verify the document was actually inserted
	findResp := h.Handle(makeFindMsg("testdb", "users", q))
	cursorDoc := getCursorDoc(t, findResp)
	batch := getCursorBatch(t, cursorDoc, "firstBatch")
	if len(batch) != 1 {
		t.Fatalf("upserted doc not found, batch len = %d", len(batch))
	}
}

func TestHandle_Update_Upsert_WithExisting(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	// Insert a doc
	doc := bson.NewDocument()
	doc.Set("name", bson.VString("bob"))
	doc.Set("age", bson.VInt32(30))
	h.Handle(makeInsertMsg("testdb", "users", doc))

	// Update with upsert=true on existing doc
	q := bson.NewDocument()
	q.Set("name", bson.VString("bob"))

	setDoc := bson.NewDocument()
	setDoc.Set("age", bson.VInt32(31))
	u := bson.NewDocument()
	u.Set("$set", bson.VDoc(setDoc))

	updateEntry := bson.NewDocument()
	updateEntry.Set("q", bson.VDoc(q))
	updateEntry.Set("u", bson.VDoc(u))
	updateEntry.Set("upsert", bson.VBool(true))

	body := bson.NewDocument()
	body.Set("update", bson.VString("users"))
	body.Set("$db", bson.VString("testdb"))
	body.Set("updates", bson.VArray(bson.A(bson.VDoc(updateEntry))))
	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}

	resp := h.Handle(msg)
	// Should match existing, no upsert
	if n, _ := resp.Get("n"); n.Int32() != 1 {
		t.Errorf("matched n = %d, want 1", n.Int32())
	}
	if _, ok := resp.Get("upserted"); ok {
		t.Error("should not have upserted field when match exists")
	}
}

func TestHandle_Find_UsesIndex(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	// Insert docs
	for i := 0; i < 5; i++ {
		d := bson.NewDocument()
		d.Set("status", bson.VString("active"))
		d.Set("val", bson.VInt32(int32(i)))
		h.Handle(makeInsertMsg("testdb", "items", d))
	}
	for i := 5; i < 10; i++ {
		d := bson.NewDocument()
		d.Set("status", bson.VString("inactive"))
		d.Set("val", bson.VInt32(int32(i)))
		h.Handle(makeInsertMsg("testdb", "items", d))
	}

	// Create index on status
	idxBody := bson.NewDocument()
	idxBody.Set("createIndexes", bson.VString("items"))
	idxBody.Set("$db", bson.VString("testdb"))

	idxKey := bson.NewDocument()
	idxKey.Set("status", bson.VInt32(1))
	idxSpec := bson.NewDocument()
	idxSpec.Set("name", bson.VString("status_idx"))
	idxSpec.Set("key", bson.VDoc(idxKey))
	idxBody.Set("indexes", bson.VArray(bson.A(bson.VDoc(idxSpec))))
	h.Handle(&Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: idxBody}}},
	})

	// Find with indexed field
	filter := bson.NewDocument()
	filter.Set("status", bson.VString("active"))
	resp := h.Handle(makeFindMsg("testdb", "items", filter))

	cursorDoc := getCursorDoc(t, resp)
	batch := getCursorBatch(t, cursorDoc, "firstBatch")
	if len(batch) != 5 {
		t.Errorf("indexed find: expected 5 active docs, got %d", len(batch))
	}
}

func TestHandle_Find_FallsBackToScan(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	// Insert docs without creating any index
	for i := 0; i < 3; i++ {
		d := bson.NewDocument()
		d.Set("x", bson.VInt32(int32(i)))
		h.Handle(makeInsertMsg("testdb", "nums", d))
	}

	// Find with filter should still work via full scan
	filter := bson.NewDocument()
	filter.Set("x", bson.VInt32(1))
	resp := h.Handle(makeFindMsg("testdb", "nums", filter))

	cursorDoc := getCursorDoc(t, resp)
	batch := getCursorBatch(t, cursorDoc, "firstBatch")
	if len(batch) != 1 {
		t.Errorf("full scan find: expected 1 doc, got %d", len(batch))
	}
}

func TestHandle_Distinct(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	// Insert docs with different status values
	statuses := []string{"active", "active", "inactive", "pending", "active", "inactive"}
	for i, status := range statuses {
		d := bson.NewDocument()
		d.Set("_id", bson.VInt32(int32(i)))
		d.Set("status", bson.VString(status))
		d.Set("score", bson.VInt32(int32(i*10)))
		h.Handle(makeInsertMsg("testdb", "items", d))
	}

	// Test distinct on status field
	body := bson.NewDocument()
	body.Set("distinct", bson.VString("items"))
	body.Set("$db", bson.VString("testdb"))
	body.Set("key", bson.VString("status"))

	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}

	resp := h.Handle(msg)
	if ok, _ := resp.Get("ok"); ok.Double() != 1.0 {
		t.Errorf("distinct ok = %v, want 1.0", ok.Double())
	}

	values, ok := resp.Get("values")
	if !ok || values.Type != bson.TypeArray {
		t.Fatal("distinct should return values array")
	}

	// Should have 3 distinct values: active, inactive, pending
	arr := values.ArrayValue()
	if len(arr) != 3 {
		t.Errorf("expected 3 distinct values, got %d", len(arr))
	}
}

func TestHandle_Distinct_WithFilter(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	// Insert docs
	for i := 0; i < 10; i++ {
		d := bson.NewDocument()
		d.Set("_id", bson.VInt32(int32(i)))
		d.Set("category", bson.VString(fmt.Sprintf("cat%d", i%3)))
		d.Set("active", bson.VBool(i%2 == 0))
		h.Handle(makeInsertMsg("testdb", "prods", d))
	}

	// Test distinct with query filter
	query := bson.NewDocument()
	query.Set("active", bson.VBool(true))

	body := bson.NewDocument()
	body.Set("distinct", bson.VString("prods"))
	body.Set("$db", bson.VString("testdb"))
	body.Set("key", bson.VString("category"))
	body.Set("query", bson.VDoc(query))

	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}

	resp := h.Handle(msg)
	if ok, _ := resp.Get("ok"); ok.Double() != 1.0 {
		t.Errorf("distinct with filter ok = %v, want 1.0", ok.Double())
	}
}

func TestHandle_Distinct_NoCollection(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	body := bson.NewDocument()
	body.Set("distinct", bson.VString(""))
	body.Set("$db", bson.VString("testdb"))
	body.Set("key", bson.VString("status"))

	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}

	resp := h.Handle(msg)
	if ok, _ := resp.Get("ok"); ok.Double() != 0.0 {
		t.Errorf("distinct (no collection) ok = %v, want 0.0", ok.Double())
	}
}

func TestHandle_Distinct_NoKey(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	body := bson.NewDocument()
	body.Set("distinct", bson.VString("items"))
	body.Set("$db", bson.VString("testdb"))
	// Missing key

	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}

	resp := h.Handle(msg)
	if ok, _ := resp.Get("ok"); ok.Double() != 0.0 {
		t.Errorf("distinct (no key) ok = %v, want 0.0", ok.Double())
	}
}

func TestHandle_Distinct_WithArrays(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	// Insert docs with array fields
	d1 := bson.NewDocument()
	d1.Set("_id", bson.VInt32(1))
	d1.Set("tags", bson.VArray(bson.A(bson.VString("a"), bson.VString("b"), bson.VString("c"))))
	h.Handle(makeInsertMsg("testdb", "tagged", d1))

	d2 := bson.NewDocument()
	d2.Set("_id", bson.VInt32(2))
	d2.Set("tags", bson.VArray(bson.A(bson.VString("b"), bson.VString("c"), bson.VString("d"))))
	h.Handle(makeInsertMsg("testdb", "tagged", d2))

	// Test distinct on array field - should flatten arrays
	body := bson.NewDocument()
	body.Set("distinct", bson.VString("tagged"))
	body.Set("$db", bson.VString("testdb"))
	body.Set("key", bson.VString("tags"))

	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}

	resp := h.Handle(msg)
	values, _ := resp.Get("values")
	arr := values.ArrayValue()

	// Should have 4 distinct values: a, b, c, d
	if len(arr) != 4 {
		t.Errorf("expected 4 distinct values from arrays, got %d", len(arr))
	}
}
