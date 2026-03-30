package wire

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

func makeExplainMsg(db, coll string, filter *bson.Document) *Message {
	findBody := bson.NewDocument()
	findBody.Set("find", bson.VString(coll))
	findBody.Set("$db", bson.VString(db))
	if filter != nil {
		findBody.Set("filter", bson.VDoc(filter))
	}

	body := bson.NewDocument()
	body.Set("explain", bson.VDoc(findBody))
	body.Set("$db", bson.VString(db))
	return &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	}
}

func TestExplain_CollScan(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	// Insert documents
	h.Handle(makeInsertMsg("test", "users",
		bson.D("name", bson.VString("Alice"), "age", bson.VInt32(30)),
		bson.D("name", bson.VString("Bob"), "age", bson.VInt32(25)),
	))

	filter := bson.NewDocument()
	filter.Set("age", bson.VInt32(30))

	resp := h.Handle(makeExplainMsg("test", "users", filter))
	if ok, _ := resp.Get("ok"); ok.Double() != 1 {
		t.Fatalf("expected ok=1, got %v", resp)
	}

	planner, _ := resp.Get("queryPlanner")
	if planner.Type != bson.TypeDocument {
		t.Fatal("expected queryPlanner document")
	}
	winningPlan, _ := planner.DocumentValue().Get("winningPlan")
	if winningPlan.Type != bson.TypeDocument {
		t.Fatal("expected winningPlan document")
	}
	stage, _ := winningPlan.DocumentValue().Get("stage")
	if stage.String() != "COLLSCAN" {
		t.Fatalf("expected COLLSCAN, got %s", stage.String())
	}

	stats, _ := resp.Get("executionStats")
	if stats.Type != bson.TypeDocument {
		t.Fatal("expected executionStats document")
	}
	nReturned, _ := stats.DocumentValue().Get("nReturned")
	if nReturned.Int32() != 1 {
		t.Fatalf("expected nReturned=1, got %d", nReturned.Int32())
	}
}

func TestExplain_IXSCAN(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	// Insert a document first to create the collection
	h.Handle(makeInsertMsg("test", "users",
		bson.D("name", bson.VString("Alice"), "age", bson.VInt32(30)),
	))

	// Create index
	idxBody := bson.NewDocument()
	idxBody.Set("createIndexes", bson.VString("users"))
	idxBody.Set("$db", bson.VString("test"))
	idxKey := bson.NewDocument()
	idxKey.Set("name", bson.VInt32(1))
	idxBody.Set("indexes", bson.VArray(bson.Array{
		bson.VDoc(bson.D("name", bson.VString("name_idx"), "key", bson.VDoc(idxKey))),
	}))
	idxResp := h.Handle(&Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: idxBody}}},
	})
	if ok, _ := idxResp.Get("ok"); ok.Double() != 1 {
		t.Fatalf("createIndexes failed: %v", idxResp)
	}

	filter := bson.NewDocument()
	filter.Set("name", bson.VString("Alice"))

	resp := h.Handle(makeExplainMsg("test", "users", filter))
	if ok, _ := resp.Get("ok"); ok.Double() != 1 {
		t.Fatalf("explain failed: %v", resp)
	}

	planner, _ := resp.Get("queryPlanner")
	if planner.Type != bson.TypeDocument {
		t.Fatal("expected queryPlanner document")
	}
	winningPlan, _ := planner.DocumentValue().Get("winningPlan")
	if winningPlan.Type != bson.TypeDocument {
		t.Fatal("expected winningPlan document")
	}
	stage, _ := winningPlan.DocumentValue().Get("stage")
	if stage.String() != "IXSCAN" {
		t.Fatalf("expected IXSCAN, got %s", stage.String())
	}
}
