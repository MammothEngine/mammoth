package auth

import "testing"

func TestAuthManagerDisabled(t *testing.T) {
	am := NewAuthManager(nil, false)
	if !am.IsAuthenticated(1) {
		t.Error("auth disabled: should always be authenticated")
	}
	if !am.CheckPermission(1, ActionFind, Resource{"test", "users"}) {
		t.Error("auth disabled: should allow everything")
	}
}

func TestAuthManagerEnabled(t *testing.T) {
	am := NewAuthManager(nil, true)
	if am.IsAuthenticated(1) {
		t.Error("auth enabled, no session: should not be authenticated")
	}

	am.GetOrCreateSession(1)
	am.MarkAuthenticated(1, "testuser", "admin")
	if !am.IsAuthenticated(1) {
		t.Error("after mark: should be authenticated")
	}

	username, authDB, ok := am.GetUser(1)
	if !ok || username != "testuser" || authDB != "admin" {
		t.Errorf("unexpected user: %s %s %v", username, authDB, ok)
	}
}

func TestAuthManagerRemoveSession(t *testing.T) {
	am := NewAuthManager(nil, true)
	am.GetOrCreateSession(1)
	am.MarkAuthenticated(1, "user", "admin")
	am.RemoveSession(1)
	if am.IsAuthenticated(1) {
		t.Error("after remove: should not be authenticated")
	}
}

func TestAuthManagerCheckPermission(t *testing.T) {
	am := NewAuthManager(nil, true)
	s := am.GetOrCreateSession(1)
	s.Authenticated = true
	s.Roles = []RoleRef{{DB: "admin", Name: "readWrite"}}

	if !am.CheckPermission(1, ActionInsert, Resource{"test", "users"}) {
		t.Error("readWrite should allow insert")
	}
	if am.CheckPermission(1, ActionCreateUser, Resource{"test", "users"}) {
		t.Error("readWrite should not allow createUser")
	}
}

func TestAuthManager_Enabled(t *testing.T) {
	am := NewAuthManager(nil, true)
	if !am.Enabled() {
		t.Error("Enabled() should return true")
	}

	am2 := NewAuthManager(nil, false)
	if am2.Enabled() {
		t.Error("Enabled() should return false")
	}
}

func TestAuthManager_UserStore(t *testing.T) {
	store := &UserStore{}
	am := NewAuthManager(store, true)
	if am.UserStore() != store {
		t.Error("UserStore() should return the store")
	}
}

func TestAuthManager_GetSession(t *testing.T) {
	am := NewAuthManager(nil, true)

	// Non-existent session
	if s := am.GetSession(1); s != nil {
		t.Error("GetSession non-existent should return nil")
	}

	// Create session
	am.GetOrCreateSession(1)

	// Get existing session
	s := am.GetSession(1)
	if s == nil {
		t.Error("GetSession should return session")
	}
	if s.ConnID != 1 {
		t.Errorf("ConnID = %d, want 1", s.ConnID)
	}
}

func TestAuthManager_SCRAMSession(t *testing.T) {
	am := NewAuthManager(nil, true)

	// Get non-existent SCRAM session
	if scram := am.GetSCRAMSession(1); scram != nil {
		t.Error("GetSCRAMSession non-existent should return nil")
	}

	// Set SCRAM session
	scram := &SCRAMSession{}
	am.SetSCRAMSession(1, scram)

	// Get SCRAM session
	got := am.GetSCRAMSession(1)
	if got == nil {
		t.Fatal("GetSCRAMSession should return session")
	}
	if got.Username() != "" {
		t.Error("Username should be empty")
	}
}

func TestAuthManager_SetRoles(t *testing.T) {
	am := NewAuthManager(nil, true)

	// Set roles on non-existent session (should not panic)
	am.SetRoles(1, []RoleRef{{DB: "admin", Name: "read"}})

	// Create session and set roles
	am.GetOrCreateSession(1)
	am.SetRoles(1, []RoleRef{{DB: "admin", Name: "readWrite"}})

	s := am.GetSession(1)
	if len(s.Roles) != 1 {
		t.Errorf("Roles count = %d, want 1", len(s.Roles))
	}
}
