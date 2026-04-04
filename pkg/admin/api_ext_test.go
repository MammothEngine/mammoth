package admin

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mammothengine/mammoth/pkg/mongo"
)

func TestHandleCreateCollection_Validation(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "empty name",
			body:       `{"name": ""}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing name field",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid JSON",
			body:       `{invalid json`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := doReq(t, h, "POST", "/api/v1/databases/testdb/collections", tc.body)
			if rec.Code != tc.wantStatus {
				t.Errorf("expected status %d, got %d", tc.wantStatus, rec.Code)
			}
		})
	}
}

func TestHandleInsertDocument_Validation(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	h.cat.EnsureDatabase("testdb")
	h.cat.EnsureCollection("testdb", "testcoll")

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "invalid JSON",
			body:       `{invalid json`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing document field",
			body:       `{"other": "value"}`,
			wantStatus: http.StatusOK, // Empty document is accepted
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := doReq(t, h, "POST", "/api/v1/databases/testdb/collections/testcoll/documents", tc.body)
			if rec.Code != tc.wantStatus {
				t.Errorf("expected status %d, got %d", tc.wantStatus, rec.Code)
			}
		})
	}
}

func TestHandleReady_Healthy(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	rec := doReq(t, h, "GET", "/ready", "")

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if ready, ok := resp["ready"].(bool); !ok || !ready {
		t.Errorf("expected ready=true, got %v", resp["ready"])
	}
}

func TestHandleReady_Unhealthy(t *testing.T) {
	// Create handler with nil engine
	h := &APIHandler{
		engine:  nil,
		cat:     nil,
		authMgr: nil,
		version: "test",
		router:  NewRouter(),
	}
	h.router.Handle("GET", "/ready", h.handleReady)

	rec := doReq(t, h, "GET", "/ready", "")

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status 503, got %d", rec.Code)
	}
}

func TestHandleListUsers_WithDB(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	// Create a test user
	h.authMgr.UserStore().CreateUser("testuser", "testdb", "password123")

	rec := doReq(t, h, "GET", "/api/v1/users?db=testdb", "")

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	// Should have users array
	data := resp["data"].(map[string]any)
	if _, ok := data["users"]; !ok {
		t.Error("expected users in response data")
	}
}

func TestHandleListUsers_NoDB(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	// Test listing users without db filter (should return empty)
	rec := doReq(t, h, "GET", "/api/v1/users", "")

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestHandleCreateUser_Validation(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "missing username",
			body:       `{"password": "pass123"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing password",
			body:       `{"username": "user1"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty username",
			body:       `{"username": "", "password": "pass123"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty password",
			body:       `{"username": "user1", "password": ""}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := doReq(t, h, "POST", "/api/v1/users", tc.body)
			if rec.Code != tc.wantStatus {
				t.Errorf("expected status %d, got %d", tc.wantStatus, rec.Code)
			}
		})
	}
}

func TestHandleCreateIndex_Validation(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	h.cat.EnsureDatabase("testdb")
	h.cat.EnsureCollection("testdb", "testcoll")

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "missing name",
			body:       `{"key": [{"field": "name", "descending": false}]}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing key",
			body:       `{"name": "idx1"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := doReq(t, h, "POST", "/api/v1/databases/testdb/collections/testcoll/indexes", tc.body)
			if rec.Code != tc.wantStatus {
				t.Errorf("expected status %d, got %d", tc.wantStatus, rec.Code)
			}
		})
	}
}

func TestHandleListDocuments_Pagination(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	h.cat.EnsureDatabase("testdb")
	h.cat.EnsureCollection("testdb", "testcoll")

	// Insert some documents
	prefix := mongo.EncodeNamespacePrefix("testdb", "testcoll")
	for i := range 25 {
		doc := map[string]any{"_id": i, "value": i}
		docBytes, _ := json.Marshal(doc)
		key := append(prefix, fmt.Appendf(nil, "%d", i)...)
		h.engine.Put(key, docBytes)
	}

	tests := []struct {
		name     string
		query    string
		expected int
	}{
		{
			name:     "default limit (20)",
			query:    "",
			expected: 20,
		},
		{
			name:     "custom limit",
			query:    "?limit=5",
			expected: 5,
		},
		{
			name:     "with skip",
			query:    "?skip=20&limit=10",
			expected: 5, // Only 5 docs left after skipping 20
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := "/api/v1/databases/testdb/collections/testcoll/documents" + tc.query
			rec := doReq(t, h, "GET", path, "")

			if rec.Code != http.StatusOK {
				t.Errorf("expected status 200, got %d", rec.Code)
			}

			var resp map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("failed to parse response: %v", err)
			}

			data := resp["data"].(map[string]any)
			count := int(data["count"].(float64))
			if count != tc.expected {
				t.Errorf("expected %d documents, got %d", tc.expected, count)
			}
		})
	}
}

func TestHandleDeleteDocuments(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	h.cat.EnsureDatabase("testdb")
	h.cat.EnsureCollection("testdb", "testcoll")

	// Insert some documents
	prefix := mongo.EncodeNamespacePrefix("testdb", "testcoll")
	for i := range 5 {
		doc := map[string]any{"_id": i, "value": i}
		docBytes, _ := json.Marshal(doc)
		key := append(prefix, fmt.Appendf(nil, "%d", i)...)
		h.engine.Put(key, docBytes)
	}

	// Test missing filter
	rec := doReq(t, h, "DELETE", "/api/v1/databases/testdb/collections/testcoll/documents", "")
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400 for missing filter, got %d", rec.Code)
	}

	// Test with filter
	rec2 := doReq(t, h, "DELETE", "/api/v1/databases/testdb/collections/testcoll/documents?filter=all", "")
	if rec2.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec2.Code)
	}

	var resp map[string]any
	if err := json.Unmarshal(rec2.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	data := resp["data"].(map[string]any)
	if _, ok := data["deleted"]; !ok {
		t.Error("expected deleted count in response")
	}
}

func TestWriteJSON(t *testing.T) {
	rec := httptest.NewRecorder()
	writeJSON(rec, http.StatusCreated, map[string]any{"test": "value"})

	if rec.Code != http.StatusCreated {
		t.Errorf("expected status %d, got %d", http.StatusCreated, rec.Code)
	}

	contentType := rec.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("expected Content-Type application/json, got %s", contentType)
	}
}

func TestWriteError(t *testing.T) {
	rec := httptest.NewRecorder()
	writeError(rec, http.StatusBadRequest, "test error")

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}

	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if ok, _ := resp["ok"].(bool); ok {
		t.Error("expected ok=false")
	}

	if errMsg, _ := resp["error"].(string); errMsg != "test error" {
		t.Errorf("expected error='test error', got %v", errMsg)
	}
}

func TestServeHTTP_CORS(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	// Test OPTIONS request
	req := httptest.NewRequest("OPTIONS", "/api/v1/status", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Errorf("expected status 204 for OPTIONS, got %d", rec.Code)
	}

	// Check CORS headers
	if origin := rec.Header().Get("Access-Control-Allow-Origin"); origin != "*" {
		t.Errorf("expected CORS origin *, got %s", origin)
	}
}

func TestHandleDropCollection_NotFound(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	// Drop non-existent collection
	rec := doReq(t, h, "DELETE", "/api/v1/databases/testdb/collections/nonexistent", "")

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", rec.Code)
	}
}

func TestHandleDropIndex_NotFound(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	h.cat.EnsureDatabase("testdb")
	h.cat.EnsureCollection("testdb", "testcoll")

	// Drop non-existent index
	rec := doReq(t, h, "DELETE", "/api/v1/databases/testdb/collections/testcoll/indexes/nonexistent", "")

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", rec.Code)
	}
}

func TestHandleDeleteUser_NotFound(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	// Delete non-existent user
	rec := doReq(t, h, "DELETE", "/api/v1/users/nonexistent", "")

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", rec.Code)
	}
}

func TestHandleHealth(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	rec := doReq(t, h, "GET", "/health", "")

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if ok, _ := resp["ok"].(bool); !ok {
		t.Error("expected ok=true")
	}
}

func TestHandleLive(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	rec := doReq(t, h, "GET", "/live", "")

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	data := resp["data"].(map[string]any)
	if alive, _ := data["alive"].(bool); !alive {
		t.Error("expected alive=true")
	}
}

// --- Conflict and Error Path Tests ---

func TestHandleCreateCollection_Duplicate(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	// Create collection first
	h.cat.EnsureDatabase("testdb")
	h.cat.CreateCollection("testdb", "testcoll")

	// Try to create again - should get conflict
	rec := doReq(t, h, "POST", "/api/v1/databases/testdb/collections", `{"name": "testcoll"}`)

	if rec.Code != http.StatusConflict {
		t.Errorf("expected status 409 for duplicate collection, got %d", rec.Code)
	}
}

func TestHandleCreateIndex_Duplicate(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	h.cat.EnsureDatabase("testdb")
	h.cat.EnsureCollection("testdb", "testcoll")

	// Create index first
	spec := mongo.IndexSpec{
		Name: "idx1",
		Key:  []mongo.IndexKey{{Field: "name", Descending: false}},
	}
	h.indexCat.CreateIndex("testdb", "testcoll", spec)

	// Try to create again - should get conflict
	rec := doReq(t, h, "POST", "/api/v1/databases/testdb/collections/testcoll/indexes", `{"name": "idx1", "key": [{"field": "name"}]}`)

	if rec.Code != http.StatusConflict {
		t.Errorf("expected status 409 for duplicate index, got %d", rec.Code)
	}
}

func TestHandleCreateUser_Duplicate(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	// Create user first
	h.authMgr.UserStore().CreateUser("testuser", "admin", "password123")

	// Try to create again - should get conflict
	rec := doReq(t, h, "POST", "/api/v1/users", `{"username": "testuser", "password": "newpass", "db": "admin"}`)

	if rec.Code != http.StatusConflict {
		t.Errorf("expected status 409 for duplicate user, got %d", rec.Code)
	}
}

func TestHandleInsertDocument_EngineError(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	h.cat.EnsureDatabase("testdb")
	h.cat.EnsureCollection("testdb", "testcoll")

	// Close engine to cause errors
	h.engine.Close()

	// Try to insert - should get error
	rec := doReq(t, h, "POST", "/api/v1/databases/testdb/collections/testcoll/documents", `{"document": {"name": "test"}}`)

	// Should return 500 due to engine being closed
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500 for closed engine, got %d", rec.Code)
	}
}

func TestHandleDeleteDocuments_WithDeleteError(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	h.cat.EnsureDatabase("testdb")
	h.cat.EnsureCollection("testdb", "testcoll")

	// Insert a document first
	prefix := mongo.EncodeNamespacePrefix("testdb", "testcoll")
	key := append(prefix, []byte("testkey")...)
	h.engine.Put(key, []byte(`{"_id": "test"}`))

	// Close engine to cause delete errors
	h.engine.Close()

	// Try to delete - error path should be exercised
	rec := doReq(t, h, "DELETE", "/api/v1/databases/testdb/collections/testcoll/documents?filter=all", "")

	// Even with errors, handler returns 200 with count (errors are silently ignored in loop)
	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}
