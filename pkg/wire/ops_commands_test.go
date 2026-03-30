package wire

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

func TestCurrentOp(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	// Track an operation
	h.opTracker = newOpTracker()
	id := h.opTracker.begin("find", "test.users")

	body := bson.NewDocument()
	body.Set("currentOp", bson.VInt32(1))
	body.Set("$db", bson.VString("admin"))
	resp := h.Handle(&Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	})
	if ok, _ := resp.Get("ok"); ok.Double() != 1 {
		t.Fatalf("expected ok=1, got %v", resp)
	}

	// Should have the tracked op
	inprog, _ := resp.Get("inprog")
	if inprog.Type != bson.TypeArray {
		t.Fatal("expected inprog array")
	}
	arr := inprog.ArrayValue()
	if len(arr) != 1 {
		t.Fatalf("expected 1 op, got %d", len(arr))
	}

	h.opTracker.end(id)
}

func TestKillOp(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	h.opTracker = newOpTracker()
	id := h.opTracker.begin("find", "test.users")

	body := bson.NewDocument()
	body.Set("killOp", bson.VInt32(1))
	body.Set("op", bson.VInt64(int64(id)))
	body.Set("$db", bson.VString("admin"))
	resp := h.Handle(&Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{{Kind: 0, Body: body}}},
	})
	if ok, _ := resp.Get("ok"); ok.Double() != 1 {
		t.Fatalf("expected ok=1, got %v", resp)
	}

	info, _ := resp.Get("info")
	if info.Type != bson.TypeString {
		t.Fatal("expected info string")
	}
	if info.String() == "" {
		t.Fatal("expected non-empty info")
	}
}
