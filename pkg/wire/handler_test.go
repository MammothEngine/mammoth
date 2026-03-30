package wire

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

func setupTestHandler(t *testing.T) (*Handler, *engine.Engine) {
	t.Helper()
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	cat := mongo.NewCatalog(eng)
	return NewHandler(eng, cat, nil), eng
}

func makeMessage(cmd string, pairs ...string) *Message {
	body := bson.NewDocument()
	body.Set(cmd, bson.VInt32(1))
	for i := 0; i < len(pairs); i += 2 {
		body.Set(pairs[i], bson.VString(pairs[i+1]))
	}
	return &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg: &OPMsg{
			Sections: []Section{
				{Kind: 0, Body: body},
			},
		},
	}
}

func TestHandler_Dispatch(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	msg := makeMessage("ping")
	resp := h.Handle(msg)
	if ok, _ := resp.Get("ok"); ok.Double() != 1.0 {
		t.Errorf("ping ok = %v, want 1.0", ok.Double())
	}
}

func TestHandler_UnknownCommand(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	msg := makeMessage("unknownCmd")
	resp := h.Handle(msg)
	if ok, _ := resp.Get("ok"); ok.Double() != 0.0 {
		t.Errorf("unknown cmd ok = %v, want 0.0", ok.Double())
	}
	if code, _ := resp.Get("code"); code.Int32() != CodeCommandNotFound {
		t.Errorf("unknown cmd code = %d, want %d", code.Int32(), CodeCommandNotFound)
	}
}

func TestHandler_EmptyMessage(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg:    &OPMsg{Sections: []Section{}},
	}
	resp := h.Handle(msg)
	if ok, _ := resp.Get("ok"); ok.Double() != 0.0 {
		t.Errorf("empty msg ok = %v, want 0.0", ok.Double())
	}
}

func TestHandler_NilMessage(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	msg := &Message{}
	resp := h.Handle(msg)
	if ok, _ := resp.Get("ok"); ok.Double() != 0.0 {
		t.Errorf("nil msg ok = %v, want 0.0", ok.Double())
	}
}
