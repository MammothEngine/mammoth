package auth

import (
	"os"
	"testing"

	"github.com/mammothengine/mammoth/pkg/engine"
)

func setupUserStore(t *testing.T) (*UserStore, *engine.Engine, func()) {
	dir := t.TempDir()
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	store := NewUserStore(eng)
	return store, eng, func() {
		eng.Close()
		os.RemoveAll(dir)
	}
}

func TestDeriveAndVerifyKeys(t *testing.T) {
	salt := GenerateSalt()
	storedKey, serverKey := DeriveKeys("mypassword", salt, 10000)
	if len(storedKey) != 32 {
		t.Errorf("stored key len: %d", len(storedKey))
	}
	if len(serverKey) != 32 {
		t.Errorf("server key len: %d", len(serverKey))
	}
	// Same password and salt should give same keys
	sk2, svk2 := DeriveKeys("mypassword", salt, 10000)
	if string(storedKey) != string(sk2) {
		t.Error("stored key mismatch")
	}
	if string(serverKey) != string(svk2) {
		t.Error("server key mismatch")
	}
}

func TestUserStore_CreateUser(t *testing.T) {
	store, _, cleanup := setupUserStore(t)
	defer cleanup()

	// Create user
	err := store.CreateUser("testuser", "admin", "password123")
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	// Duplicate user should fail
	err = store.CreateUser("testuser", "admin", "password123")
	if err == nil {
		t.Error("CreateUser duplicate should fail")
	}
}

func TestUserStore_GetUser(t *testing.T) {
	store, _, cleanup := setupUserStore(t)
	defer cleanup()

	// Create user
	if err := store.CreateUser("alice", "admin", "password123"); err != nil {
		t.Fatal(err)
	}

	// Get user
	user, err := store.GetUser("alice")
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}
	if user.Username != "alice" {
		t.Errorf("username = %q, want alice", user.Username)
	}
	if user.AuthDB != "admin" {
		t.Errorf("authDB = %q, want admin", user.AuthDB)
	}

	// Non-existent user
	_, err = store.GetUser("bob")
	if err == nil {
		t.Error("GetUser non-existent should fail")
	}
}

func TestUserStore_UpdatePassword(t *testing.T) {
	store, _, cleanup := setupUserStore(t)
	defer cleanup()

	// Create user
	if err := store.CreateUser("testuser", "admin", "oldpassword"); err != nil {
		t.Fatal(err)
	}

	// Update password
	err := store.UpdatePassword("testuser", "admin", "newpassword")
	if err != nil {
		t.Fatalf("UpdatePassword: %v", err)
	}

	// Non-existent user should fail
	err = store.UpdatePassword("nonexistent", "admin", "password")
	if err == nil {
		t.Error("UpdatePassword non-existent should fail")
	}
}

func TestUserStore_DropUser(t *testing.T) {
	store, _, cleanup := setupUserStore(t)
	defer cleanup()

	// Create user
	if err := store.CreateUser("testuser", "admin", "password123"); err != nil {
		t.Fatal(err)
	}

	// Drop user
	err := store.DropUser("testuser", "admin")
	if err != nil {
		t.Fatalf("DropUser: %v", err)
	}

	// Verify user is gone
	_, err = store.GetUser("testuser")
	if err == nil {
		t.Error("user should not exist after drop")
	}

	// Drop non-existent should fail
	err = store.DropUser("nonexistent", "admin")
	if err == nil {
		t.Error("DropUser non-existent should fail")
	}
}

func TestUserStore_GetUsersInDB(t *testing.T) {
	store, _, cleanup := setupUserStore(t)
	defer cleanup()

	// Create users in different databases
	if err := store.CreateUser("user1", "admin", "pass1"); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateUser("user2", "admin", "pass2"); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateUser("user3", "test", "pass3"); err != nil {
		t.Fatal(err)
	}

	// Get users in admin db
	users, err := store.GetUsersInDB("admin")
	if err != nil {
		t.Fatalf("GetUsersInDB: %v", err)
	}
	if len(users) != 2 {
		t.Errorf("admin users = %d, want 2", len(users))
	}

	// Get users in test db
	users, err = store.GetUsersInDB("test")
	if err != nil {
		t.Fatalf("GetUsersInDB: %v", err)
	}
	if len(users) != 1 {
		t.Errorf("test users = %d, want 1", len(users))
	}
}
