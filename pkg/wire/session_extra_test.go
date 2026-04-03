package wire

import (
	"testing"
)

func TestSessionManager_GetOrCreate(t *testing.T) {
	sm := NewSessionManager()

	// Create new session
	session := sm.GetOrCreate(1)
	if session == nil {
		t.Fatal("GetOrCreate returned nil")
	}

	// Get existing session
	session2 := sm.GetOrCreate(1)
	if session2 != session {
		t.Error("GetOrCreate returned different session for same ID")
	}
}

func TestSessionManager_Get(t *testing.T) {
	sm := NewSessionManager()

	// Get non-existing - should return nil
	session := sm.Get(999)
	if session != nil {
		t.Error("Get returned non-nil for non-existing session")
	}

	// Create and get
	sm.GetOrCreate(1)
	session = sm.Get(1)
	if session == nil {
		t.Error("Get returned nil for existing session")
	}
}
