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
