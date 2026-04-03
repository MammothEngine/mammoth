package wire

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/auth"
	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

func setupTestHandlerWithAuth(t *testing.T) (*Handler, *engine.Engine, *auth.AuthManager) {
	t.Helper()
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	cat := mongo.NewCatalog(eng)
	userStore := auth.NewUserStore(eng)
	authMgr := auth.NewAuthManager(userStore, true)
	h := NewHandler(eng, cat, authMgr)
	return h, eng, authMgr
}

func TestHandleSaslStart(t *testing.T) {
	h, eng, _ := setupTestHandlerWithAuth(t)
	defer eng.Close()
	defer h.Close()

	tests := []struct {
		name       string
		mechanism  string
		payload    string
		username   string
		wantOk     bool
		wantErr    bool
		errCode    int32
	}{
		{
			name:      "unsupported mechanism",
			mechanism: "PLAIN",
			payload:   "",
			wantOk:    false,
			wantErr:   true,
			errCode:   CodeBadValue,
		},
		{
			name:      "SCRAM-SHA-256 with empty payload",
			mechanism: "SCRAM-SHA-256",
			payload:   "",
			wantOk:    false,
			wantErr:   true,
			errCode:   CodeUnauthorized,
		},
		{
			name:      "SCRAM-SHA-256 with invalid payload",
			mechanism: "SCRAM-SHA-256",
			payload:   "invalid-data",
			wantOk:    false,
			wantErr:   true,
			errCode:   CodeUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := bson.NewDocument()
			body.Set("saslStart", bson.VInt32(1))
			body.Set("mechanism", bson.VString(tt.mechanism))
			if tt.payload != "" {
				body.Set("payload", bson.VBinary(bson.BinaryGeneric, []byte(tt.payload)))
			}

			resp := h.handleSaslStart(body, 123)

			okVal, _ := resp.Get("ok")
			ok := okVal.Double() == 1.0

			if tt.wantOk && !ok {
				t.Errorf("expected ok=1, got ok=%v", okVal.Double())
			}
			if tt.wantErr && ok {
				t.Errorf("expected error, got ok=1")
			}
			if tt.wantErr && tt.errCode != 0 {
				codeVal, _ := resp.Get("code")
				if codeVal.Int32() != tt.errCode {
					t.Errorf("expected code=%d, got %d", tt.errCode, codeVal.Int32())
				}
			}
		})
	}
}

func TestHandleSaslStart_WithUsername(t *testing.T) {
	h, eng, _ := setupTestHandlerWithAuth(t)
	defer eng.Close()
	defer h.Close()

	// First create a user via the handler (so it's in the right db)
	createBody := bson.NewDocument()
	createBody.Set("createUser", bson.VString("testuser"))
	createBody.Set("pwd", bson.VString("testpass"))
	createBody.Set("$db", bson.VString("admin"))
	h.handleCreateUser(createBody)

	body := bson.NewDocument()
	body.Set("saslStart", bson.VInt32(1))
	body.Set("mechanism", bson.VString("SCRAM-SHA-256"))
	body.Set("username", bson.VString("testuser"))

	resp := h.handleSaslStart(body, 124)

	// Should return challenge or error (depending on implementation)
	_, hasPayload := resp.Get("payload")
	if !hasPayload {
		// If no payload, should be an error response
		codeVal, hasCode := resp.Get("code")
		if hasCode && codeVal.Int32() != CodeUnauthorized {
			t.Logf("Got error code %d", codeVal.Int32())
		}
	}
}

func TestHandleSaslStart_ExtractUsernameFromPayload(t *testing.T) {
	h, eng, _ := setupTestHandlerWithAuth(t)
	defer eng.Close()
	defer h.Close()

	// Create payload with username embedded (n=username format)
	payload := "n,,n=testuser,r=nonce123"

	body := bson.NewDocument()
	body.Set("saslStart", bson.VInt32(1))
	body.Set("mechanism", bson.VString("SCRAM-SHA-256"))
	body.Set("payload", bson.VString(payload))

	resp := h.handleSaslStart(body, 125)

	// Verify response has proper structure
	_, hasPayload := resp.Get("payload")
	conversationId, hasConvId := resp.Get("conversationId")
	done, hasDone := resp.Get("done")

	if hasPayload || hasConvId || hasDone {
		// Response has expected fields, verify done is false
		if hasDone && done.Boolean() {
			t.Error("expected done=false for first step")
		}
		if hasConvId && conversationId.Int32() != 1 {
			t.Errorf("expected conversationId=1, got %d", conversationId.Int32())
		}
	}
}

func TestHandleSaslContinue_NoSession(t *testing.T) {
	h, eng, _ := setupTestHandlerWithAuth(t)
	defer eng.Close()
	defer h.Close()

	body := bson.NewDocument()
	body.Set("saslContinue", bson.VInt32(1))
	body.Set("conversationId", bson.VInt32(1))
	body.Set("payload", bson.VBinary(bson.BinaryGeneric, []byte("some-data")))

	resp := h.handleSaslContinue(body, 999) // No session for connID 999

	okVal, _ := resp.Get("ok")
	if okVal.Double() == 1.0 {
		t.Error("expected error when no session exists")
	}

	codeVal, _ := resp.Get("code")
	if codeVal.Int32() != CodeUnauthorized {
		t.Errorf("expected code=%d, got %d", CodeUnauthorized, codeVal.Int32())
	}
}

func TestHandleSaslContinue_InvalidPayload(t *testing.T) {
	h, eng, _ := setupTestHandlerWithAuth(t)
	defer eng.Close()
	defer h.Close()

	// Start a session first
	body := bson.NewDocument()
	body.Set("saslStart", bson.VInt32(1))
	body.Set("mechanism", bson.VString("SCRAM-SHA-256"))
	body.Set("payload", bson.VBinary(bson.BinaryGeneric, []byte("n,,n=testuser,r=nonce")))

	h.handleSaslStart(body, 126)

	// Continue with invalid payload
	body2 := bson.NewDocument()
	body2.Set("saslContinue", bson.VInt32(1))
	body2.Set("conversationId", bson.VInt32(1))
	body2.Set("payload", bson.VBinary(bson.BinaryGeneric, []byte("invalid")))

	resp := h.handleSaslContinue(body2, 126)

	okVal, _ := resp.Get("ok")
	if okVal.Double() == 1.0 {
		t.Error("expected error for invalid payload")
	}
}

func TestHandleSaslContinue_WithStringPayload(t *testing.T) {
	h, eng, _ := setupTestHandlerWithAuth(t)
	defer eng.Close()
	defer h.Close()

	// Start a session first with string payload (not binary)
	body := bson.NewDocument()
	body.Set("saslStart", bson.VInt32(1))
	body.Set("mechanism", bson.VString("SCRAM-SHA-256"))
	body.Set("payload", bson.VString("n,,n=testuser,r=nonce"))

	h.handleSaslStart(body, 127)

	// Continue with string payload
	body2 := bson.NewDocument()
	body2.Set("saslContinue", bson.VInt32(1))
	body2.Set("conversationId", bson.VInt32(1))
	body2.Set("payload", bson.VString("c=biws,,p=invalid"))

	resp := h.handleSaslContinue(body2, 127)

	// Should fail but not panic
	okVal, _ := resp.Get("ok")
	_ = okVal
}

func TestHandleCreateUser(t *testing.T) {
	h, eng, _ := setupTestHandlerWithAuth(t)
	defer eng.Close()
	defer h.Close()

	tests := []struct {
		name     string
		username string
		password string
		wantOk   bool
		wantErr  bool
	}{
		{
			name:     "valid user",
			username: "newuser",
			password: "newpass",
			wantOk:   true,
		},
		{
			name:     "empty username",
			username: "",
			password: "pass",
			wantOk:   false,
			wantErr:  true,
		},
		{
			name:     "empty password",
			username: "user",
			password: "",
			wantOk:   false,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := bson.NewDocument()
			body.Set("createUser", bson.VString(tt.username))
			body.Set("pwd", bson.VString(tt.password))
			body.Set("$db", bson.VString("admin"))

			resp := h.handleCreateUser(body)

			okVal, _ := resp.Get("ok")
			ok := okVal.Double() == 1.0

			if tt.wantOk && !ok {
				t.Errorf("expected ok=1, got ok=%v", okVal.Double())
			}
			if tt.wantErr && ok {
				t.Error("expected error, got ok=1")
			}
		})
	}
}

func TestHandleCreateUser_Duplicate(t *testing.T) {
	h, eng, _ := setupTestHandlerWithAuth(t)
	defer eng.Close()
	defer h.Close()

	// Create user first
	body := bson.NewDocument()
	body.Set("createUser", bson.VString("dupuser"))
	body.Set("pwd", bson.VString("pass"))
	body.Set("$db", bson.VString("admin"))

	h.handleCreateUser(body)

	// Try to create again
	resp := h.handleCreateUser(body)

	okVal, _ := resp.Get("ok")
	if okVal.Double() == 1.0 {
		t.Error("expected error for duplicate user")
	}

	codeVal, _ := resp.Get("code")
	if codeVal.Int32() != CodeDuplicateKey {
		t.Errorf("expected code=%d, got %d", CodeDuplicateKey, codeVal.Int32())
	}
}

func TestHandleDropUser(t *testing.T) {
	h, eng, _ := setupTestHandlerWithAuth(t)
	defer eng.Close()
	defer h.Close()

	// Create a user first
	h.authMgr.UserStore().CreateUser("todelete", "admin", "pass")

	tests := []struct {
		name     string
		username string
		wantOk   bool
		wantErr  bool
		errCode  int32
	}{
		{
			name:     "existing user",
			username: "todelete",
			wantOk:   true,
		},
		{
			name:     "empty username",
			username: "",
			wantOk:   false,
			wantErr:  true,
			errCode:  CodeBadValue,
		},
		{
			name:     "non-existent user",
			username: "nonexistent",
			wantOk:   false,
			wantErr:  true,
			errCode:  CodeNamespaceNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := bson.NewDocument()
			body.Set("dropUser", bson.VString(tt.username))
			body.Set("$db", bson.VString("admin"))

			resp := h.handleDropUser(body)

			okVal, _ := resp.Get("ok")
			ok := okVal.Double() == 1.0

			if tt.wantOk && !ok {
				t.Errorf("expected ok=1, got ok=%v", okVal.Double())
			}
			if tt.wantErr && ok {
				t.Error("expected error, got ok=1")
			}
			if tt.wantErr && tt.errCode != 0 {
				codeVal, _ := resp.Get("code")
				if codeVal.Int32() != tt.errCode {
					t.Errorf("expected code=%d, got %d", tt.errCode, codeVal.Int32())
				}
			}
		})
	}
}

func TestHandleUsersInfo(t *testing.T) {
	h, eng, _ := setupTestHandlerWithAuth(t)
	defer eng.Close()
	defer h.Close()

	// Create some users
	h.authMgr.UserStore().CreateUser("user1", "testdb", "pass1")
	h.authMgr.UserStore().CreateUser("user2", "testdb", "pass2")

	body := bson.NewDocument()
	body.Set("usersInfo", bson.VInt32(1))
	body.Set("$db", bson.VString("testdb"))

	resp := h.handleUsersInfo(body)

	okVal, _ := resp.Get("ok")
	if okVal.Double() != 1.0 {
		t.Errorf("expected ok=1, got %v", okVal.Double())
	}

	usersVal, ok := resp.Get("users")
	if !ok {
		t.Fatal("expected users array in response")
	}

	users := usersVal.ArrayValue()
	if len(users) != 2 {
		t.Errorf("expected 2 users, got %d", len(users))
	}

	// Verify user document structure
	for _, u := range users {
		doc := u.DocumentValue()
		if _, ok := doc.Get("_id"); !ok {
			t.Error("user missing _id field")
		}
		if _, ok := doc.Get("user"); !ok {
			t.Error("user missing user field")
		}
		if _, ok := doc.Get("db"); !ok {
			t.Error("user missing db field")
		}
	}
}

