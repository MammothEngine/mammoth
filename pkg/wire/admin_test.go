package wire

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
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
