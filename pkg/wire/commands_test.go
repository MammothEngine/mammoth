package wire

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

func TestHandleHello(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	msg := makeMessage("hello")
	resp := h.Handle(msg)

	// Check required fields
	if ok, _ := resp.Get("ok"); ok.Double() != 1.0 {
		t.Errorf("ok = %v, want 1.0", ok.Double())
	}
	if v, _ := resp.Get("isWritablePrimary"); !v.Boolean() {
		t.Error("isWritablePrimary should be true")
	}
	if v, _ := resp.Get("ismaster"); !v.Boolean() {
		t.Error("ismaster should be true")
	}
	if v, _ := resp.Get("maxWireVersion"); v.Int32() != 17 {
		t.Errorf("maxWireVersion = %d, want 17", v.Int32())
	}
	if v, _ := resp.Get("minWireVersion"); v.Int32() != 0 {
		t.Errorf("minWireVersion = %d, want 0", v.Int32())
	}
	if _, ok := resp.Get("topologyVersion"); !ok {
		t.Error("hello should include topologyVersion")
	}
	if v, _ := resp.Get("logicalSessionTimeoutMinutes"); v.Int32() != 30 {
		t.Errorf("logicalSessionTimeoutMinutes = %d, want 30", v.Int32())
	}
	if _, ok := resp.Get("connectionId"); !ok {
		t.Error("hello should include connectionId")
	}
}

func TestHandlePing(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	msg := makeMessage("ping")
	resp := h.Handle(msg)
	if ok, _ := resp.Get("ok"); ok.Double() != 1.0 {
		t.Errorf("ping ok = %v, want 1.0", ok.Double())
	}
}

func TestHandleBuildInfo(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	msg := makeMessage("buildInfo")
	resp := h.Handle(msg)
	if v, _ := resp.Get("version"); v.String() != "7.0.0" {
		t.Errorf("version = %q, want 7.0.0", v.String())
	}
}

func TestHandleWhatsmyuri(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	msg := makeMessage("whatsmyuri")
	msg.RemoteAddr = "192.168.1.100:12345"
	resp := h.Handle(msg)
	if v, _ := resp.Get("you"); v.String() != "192.168.1.100:12345" {
		t.Errorf("you = %q, want 192.168.1.100:12345", v.String())
	}
}

func TestHandleConnectionStatus(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	msg := makeMessage("connectionStatus")
	resp := h.Handle(msg)
	if ok, _ := resp.Get("ok"); ok.Double() != 1.0 {
		t.Errorf("connectionStatus ok = %v, want 1.0", ok.Double())
	}
	authInfo, ok := resp.Get("authInfo")
	if !ok || authInfo.Type != bson.TypeDocument {
		t.Error("connectionStatus should include authInfo document")
	}
}

func TestHandleStartSession(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	msg := makeMessage("startSession")
	resp := h.Handle(msg)
	if ok, _ := resp.Get("ok"); ok.Double() != 1.0 {
		t.Errorf("startSession ok = %v, want 1.0", ok.Double())
	}

	// Should include id document with an ObjectID
	idVal, ok := resp.Get("id")
	if !ok || idVal.Type != bson.TypeDocument {
		t.Fatal("startSession should return id document")
	}
	idDoc := idVal.DocumentValue()
	if oidVal, ok := idDoc.Get("id"); !ok || oidVal.Type != bson.TypeObjectID {
		t.Errorf("startSession id document should contain ObjectID, got type %v", oidVal.Type)
	}
}

func TestHandleGetCmdLineOpts(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	msg := makeMessage("getCmdLineOpts")
	resp := h.Handle(msg)
	if ok, _ := resp.Get("ok"); ok.Double() != 1.0 {
		t.Errorf("getCmdLineOpts ok = %v, want 1.0", ok.Double())
	}

	// Should include argv and parsed
	if _, ok := resp.Get("argv"); !ok {
		t.Error("getCmdLineOpts should return argv")
	}
	parsed, ok := resp.Get("parsed")
	if !ok || parsed.Type != bson.TypeDocument {
		t.Error("getCmdLineOpts should return parsed document")
	}
}
