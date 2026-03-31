package wire

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

func TestHandleCreateRole(t *testing.T) {
	h := &Handler{}

	// Test successful role creation
	body := bson.D(
		"createRole", bson.VString("admin"),
		"$db", bson.VString("test"),
	)
	result := h.handleCreateRole(body)

	if result == nil {
		t.Fatal("expected non-nil result")
	}

	okVal, _ := result.Get("ok")
	if okVal.Double() != 1.0 {
		t.Errorf("expected ok=1.0, got %v", okVal.Double())
	}

	role, _ := result.Get("role")
	if role.String() != "admin" {
		t.Errorf("expected role 'admin', got %s", role.String())
	}

	db, _ := result.Get("db")
	if db.String() != "test" {
		t.Errorf("expected db 'test', got %s", db.String())
	}

	// Test missing role name
	body2 := bson.NewDocument()
	result2 := h.handleCreateRole(body2)

	okVal2, _ := result2.Get("ok")
	if okVal2.Double() != 0.0 {
		t.Error("expected error for missing role name")
	}
}

func TestHandleUpdateRole(t *testing.T) {
	h := &Handler{}

	// Test successful update
	body := bson.D("updateRole", bson.VString("admin"))
	result := h.handleUpdateRole(body)

	if result == nil {
		t.Fatal("expected non-nil result")
	}

	okVal, _ := result.Get("ok")
	if okVal.Double() != 1.0 {
		t.Errorf("expected ok=1.0, got %v", okVal.Double())
	}

	// Test missing role name
	body2 := bson.NewDocument()
	result2 := h.handleUpdateRole(body2)

	okVal2, _ := result2.Get("ok")
	if okVal2.Double() != 0.0 {
		t.Error("expected error for missing role name")
	}
}

func TestHandleDropRole(t *testing.T) {
	h := &Handler{}

	// Test successful drop
	body := bson.D(
		"dropRole", bson.VString("admin"),
		"$db", bson.VString("test"),
	)
	result := h.handleDropRole(body)

	if result == nil {
		t.Fatal("expected non-nil result")
	}

	okVal, _ := result.Get("ok")
	if okVal.Double() != 1.0 {
		t.Errorf("expected ok=1.0, got %v", okVal.Double())
	}

	// Test missing role name
	body2 := bson.NewDocument()
	result2 := h.handleDropRole(body2)

	okVal2, _ := result2.Get("ok")
	if okVal2.Double() != 0.0 {
		t.Error("expected error for missing role name")
	}
}

func TestHandleRolesInfo(t *testing.T) {
	h := &Handler{}

	body := bson.NewDocument()
	result := h.handleRolesInfo(body)

	if result == nil {
		t.Fatal("expected non-nil result")
	}

	okVal, _ := result.Get("ok")
	if okVal.Double() != 1.0 {
		t.Errorf("expected ok=1.0, got %v", okVal.Double())
	}

	roles, _ := result.Get("roles")
	if roles.Type != bson.TypeArray {
		t.Error("expected roles to be an array")
	}
}
