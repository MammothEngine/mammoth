package wire

import (
	"errors"
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

func TestCodeName(t *testing.T) {
	tests := []struct {
		code     int32
		expected string
	}{
		{0, "UnknownError"}, // 0 is not mapped, falls through to default
		{CodeBadValue, "BadValue"},
		{CodeNamespaceNotFound, "NamespaceNotFound"},
		{CodeNamespaceExists, "NamespaceExists"},
		{CodeCommandNotFound, "CommandNotFound"},
		{CodeDuplicateKey, "DuplicateKey"},
		{999, "UnknownError"},
	}

	for _, tc := range tests {
		result := codeName(tc.code)
		if result != tc.expected {
			t.Errorf("codeName(%d) = %q, want %q", tc.code, result, tc.expected)
		}
	}
}

func TestMongoErrToCode(t *testing.T) {
	tests := []struct {
		err      error
		expected int32
	}{
		{nil, 0},
		{mongo.ErrNamespaceNotFound, CodeNamespaceNotFound},
		{mongo.ErrNamespaceExists, CodeNamespaceExists},
		// Note: mongo.ErrDuplicateKey message is "mongo: duplicate key"
		// which contains "duplicate" so it maps to CodeNamespaceExists (48)
		{mongo.ErrDuplicateKey, CodeNamespaceExists},
		{errors.New("not found"), CodeNamespaceNotFound},
		{errors.New("already exists"), CodeNamespaceExists},
		{errors.New("some random error"), CodeInternalError},
	}

	for _, tc := range tests {
		result := mongoErrToCode(tc.err)
		if result != tc.expected {
			t.Errorf("mongoErrToCode(%v) = %d, want %d", tc.err, result, tc.expected)
		}
	}
}

func TestGetStringFromBody(t *testing.T) {
	body := bson.D("key", bson.VString("value"))
	result := getStringFromBody(body, "key")
	if result != "value" {
		t.Errorf("getStringFromBody = %q, want %q", result, "value")
	}

	// Missing key
	result = getStringFromBody(body, "missing")
	if result != "" {
		t.Errorf("getStringFromBody missing = %q, want empty", result)
	}
}

func TestGetArrayFromBody(t *testing.T) {
	arr := bson.A(bson.VString("item1"), bson.VString("item2"))
	body := bson.D("items", bson.VArray(arr))

	result := getArrayFromBody(body, "items")
	if result == nil || len(result) != 2 {
		t.Errorf("getArrayFromBody = %v, want array of 2", result)
	}

	// Missing key
	result = getArrayFromBody(body, "missing")
	if result != nil {
		t.Errorf("getArrayFromBody missing = %v, want nil", result)
	}
}

