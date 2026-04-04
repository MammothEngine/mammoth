package wire

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

func TestHandleKillOp(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, _ := engine.Open(opts)
	defer eng.Close()

	h := NewHandler(eng, nil, nil)

	// Test killing a non-existent operation
	req := bson.D("op", bson.VInt64(99999))
	resp := h.handleKillOp(req)

	ok, _ := resp.Get("ok")
	if ok.Double() != 1.0 {
		t.Errorf("expected ok=1.0, got %v", ok.Double())
	}

	// Test killing with missing op field
	req2 := bson.NewDocument()
	resp2 := h.handleKillOp(req2)

	ok2, _ := resp2.Get("ok")
	if ok2.Double() != 0.0 {
		t.Errorf("expected ok=0.0 for missing op, got %v", ok2.Double())
	}
}
