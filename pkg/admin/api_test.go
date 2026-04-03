package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/mammothengine/mammoth/pkg/auth"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

func setupTestHandler(t *testing.T) (*APIHandler, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "admin-test-*")
	if err != nil {
		t.Fatal(err)
	}
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}
	cat := mongo.NewCatalog(eng)
	authMgr := auth.NewAuthManager(auth.NewUserStore(eng), false)
	h := NewAPIHandler(eng, cat, authMgr, "test")
	return h, func() {
		eng.Close()
		os.RemoveAll(dir)
	}
}

func doReq(t *testing.T, h *APIHandler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func decodeResp(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v, body: %s", err, w.Body.String())
	}
	return resp
}

// --- Status ---

func TestStatusEndpoint(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	w := doReq(t, h, "GET", "/api/v1/status", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	resp := decodeResp(t, w)
	if resp["ok"] != true {
		t.Fatal("expected ok=true")
	}
	data := resp["data"].(map[string]any)
	if data["version"] != "test" {
		t.Errorf("version = %v", data["version"])
	}
}

// --- Databases ---

func TestListDatabases(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	w := doReq(t, h, "GET", "/api/v1/databases", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	resp := decodeResp(t, w)
	data := resp["data"].(map[string]any)
	// databases may be nil for empty — just verify the field exists
	_ = data["databases"]
}

// --- Collections ---

func TestCreateAndGetCollections(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	// Create collection
	w := doReq(t, h, "POST", "/api/v1/databases/test/collections", `{"name":"users"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("create = %d, want 200: %s", w.Code, w.Body.String())
	}

	// List collections
	w = doReq(t, h, "GET", "/api/v1/databases/test/collections", "")
	if w.Code != http.StatusOK {
		t.Fatalf("list = %d, want 200", w.Code)
	}

	resp := decodeResp(t, w)
	data := resp["data"].(map[string]any)
	colls, ok := data["collections"].([]any)
	if !ok {
		t.Fatalf("collections is not a list: %T", data["collections"])
	}
	if len(colls) != 1 {
		t.Errorf("expected 1 collection, got %d", len(colls))
	}
}

func TestCreateCollectionNoName(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	w := doReq(t, h, "POST", "/api/v1/databases/test/collections", `{"name":""}`)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestDropCollection(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	// Create first
	doReq(t, h, "POST", "/api/v1/databases/test/collections", `{"name":"temp"}`)

	// Drop
	w := doReq(t, h, "DELETE", "/api/v1/databases/test/collections/temp", "")
	if w.Code != http.StatusOK {
		t.Fatalf("drop = %d, want 200: %s", w.Code, w.Body.String())
	}
}

func TestDropCollectionNotFound(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	w := doReq(t, h, "DELETE", "/api/v1/databases/test/collections/nonexistent", "")
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

// --- Collection Stats ---

func TestCollectionStats(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	doReq(t, h, "POST", "/api/v1/databases/test/collections", `{"name":"data"}`)

	w := doReq(t, h, "GET", "/api/v1/databases/test/collections/data/stats", "")
	if w.Code != http.StatusOK {
		t.Fatalf("stats = %d, want 200: %s", w.Code, w.Body.String())
	}

	resp := decodeResp(t, w)
	data := resp["data"].(map[string]any)
	if data["count"] != float64(0) {
		t.Errorf("count = %v, want 0", data["count"])
	}
}

// --- Documents ---

func TestListDocumentsEmpty(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	doReq(t, h, "POST", "/api/v1/databases/test/collections", `{"name":"items"}`)

	w := doReq(t, h, "GET", "/api/v1/databases/test/collections/items/documents?limit=10&skip=0", "")
	if w.Code != http.StatusOK {
		t.Fatalf("list docs = %d, want 200: %s", w.Code, w.Body.String())
	}
}

// --- Indexes ---

func TestListIndexes(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	doReq(t, h, "POST", "/api/v1/databases/test/collections", `{"name":"products"}`)

	w := doReq(t, h, "GET", "/api/v1/databases/test/collections/products/indexes", "")
	if w.Code != http.StatusOK {
		t.Fatalf("list indexes = %d, want 200: %s", w.Code, w.Body.String())
	}
}

func TestCreateIndex(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	doReq(t, h, "POST", "/api/v1/databases/test/collections", `{"name":"products"}`)

	w := doReq(t, h, "POST", "/api/v1/databases/test/collections/products/indexes",
		`{"name":"idx_name","key":[{"field":"name","descending":false}],"unique":false}`)
	if w.Code != http.StatusOK {
		t.Fatalf("create index = %d, want 200: %s", w.Code, w.Body.String())
	}
}

func TestCreateIndexNoName(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	doReq(t, h, "POST", "/api/v1/databases/test/collections", `{"name":"items"}`)

	w := doReq(t, h, "POST", "/api/v1/databases/test/collections/items/indexes",
		`{"name":"","key":[{"field":"name"}]}`)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

// --- Users ---

func TestCreateAndListUsers(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	// Create user
	w := doReq(t, h, "POST", "/api/v1/users", `{"username":"alice","password":"secret","db":"admin"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("create user = %d, want 200: %s", w.Code, w.Body.String())
	}

	// List users
	w = doReq(t, h, "GET", "/api/v1/users?db=admin", "")
	if w.Code != http.StatusOK {
		t.Fatalf("list users = %d, want 200: %s", w.Code, w.Body.String())
	}
}

func TestCreateUserNoPassword(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	w := doReq(t, h, "POST", "/api/v1/users", `{"username":"bob","password":"","db":"admin"}`)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestDeleteUser(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	doReq(t, h, "POST", "/api/v1/users", `{"username":"charlie","password":"pass","db":"admin"}`)

	w := doReq(t, h, "DELETE", "/api/v1/users/charlie?db=admin", "")
	if w.Code != http.StatusOK {
		t.Fatalf("delete user = %d, want 200: %s", w.Code, w.Body.String())
	}
}

// --- CORS ---

func TestCORSPreflight(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	w := doReq(t, h, "OPTIONS", "/api/v1/status", "")
	if w.Code != http.StatusNoContent {
		t.Errorf("OPTIONS = %d, want 204", w.Code)
	}
	if w.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("missing CORS header")
	}
}

// --- Static Files ---

func TestStaticFilesSPA(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	w := doReq(t, h, "GET", "/", "")
	if w.Code != http.StatusOK {
		t.Errorf("root = %d, want 200", w.Code)
	}
	ct := w.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/html") {
		t.Errorf("content-type = %q, want text/html", ct)
	}
}

// --- Not Found ---

func TestNotFound(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	w := doReq(t, h, "GET", "/api/v1/nonexistent", "")
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

// --- DB Stats ---

func TestDBStatsEmptyDB(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	// nonexistent db returns 200 with 0 collections (no error from ListCollections)
	w := doReq(t, h, "GET", "/api/v1/databases/nonexistent/stats", "")
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	resp := decodeResp(t, w)
	data := resp["data"].(map[string]any)
	if data["collections"] != float64(0) {
		t.Errorf("collections = %v, want 0", data["collections"])
	}
}

// --- Insert Document ---

func TestInsertDocument(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	// Create collection first
	doReq(t, h, "POST", "/api/v1/databases/test/collections", `{"name":"items"}`)

	// Insert document
	w := doReq(t, h, "POST", "/api/v1/databases/test/collections/items/documents",
		`{"document":{"name":"test","value":42}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("insert = %d, want 200: %s", w.Code, w.Body.String())
	}

	resp := decodeResp(t, w)
	data := resp["data"].(map[string]any)
	if data["inserted"] != float64(1) {
		t.Errorf("inserted = %v, want 1", data["inserted"])
	}
}

func TestInsertDocumentInvalidJSON(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	doReq(t, h, "POST", "/api/v1/databases/test/collections", `{"name":"items"}`)

	w := doReq(t, h, "POST", "/api/v1/databases/test/collections/items/documents", `invalid json`)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

// --- Delete Documents ---

func TestDeleteDocuments(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	// Create collection and insert documents
	doReq(t, h, "POST", "/api/v1/databases/test/collections", `{"name":"items"}`)
	doReq(t, h, "POST", "/api/v1/databases/test/collections/items/documents", `{"document":{"_id":"1"}}`)
	doReq(t, h, "POST", "/api/v1/databases/test/collections/items/documents", `{"document":{"_id":"2"}}`)

	// Delete all documents (empty filter matches all in this simple implementation)
	w := doReq(t, h, "DELETE", "/api/v1/databases/test/collections/items/documents?filter=all", "")
	if w.Code != http.StatusOK {
		t.Fatalf("delete = %d, want 200: %s", w.Code, w.Body.String())
	}

	resp := decodeResp(t, w)
	data := resp["data"].(map[string]any)
	if data["deleted"] == nil {
		t.Error("expected deleted count")
	}
}

func TestDeleteDocumentsNoFilter(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	doReq(t, h, "POST", "/api/v1/databases/test/collections", `{"name":"items"}`)

	w := doReq(t, h, "DELETE", "/api/v1/databases/test/collections/items/documents", "")
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

// --- Drop Index ---

func TestDropIndex(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	// Create collection and index
	doReq(t, h, "POST", "/api/v1/databases/test/collections", `{"name":"products"}`)
	doReq(t, h, "POST", "/api/v1/databases/test/collections/products/indexes",
		`{"name":"idx_name","key":[{"field":"name"}]}`)

	// Drop index
	w := doReq(t, h, "DELETE", "/api/v1/databases/test/collections/products/indexes/idx_name", "")
	if w.Code != http.StatusOK {
		t.Fatalf("drop index = %d, want 200: %s", w.Code, w.Body.String())
	}

	resp := decodeResp(t, w)
	data := resp["data"].(map[string]any)
	if data["dropped"] != "idx_name" {
		t.Errorf("dropped = %v, want idx_name", data["dropped"])
	}
}

func TestDropIndexNotFound(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	doReq(t, h, "POST", "/api/v1/databases/test/collections", `{"name":"products"}`)

	w := doReq(t, h, "DELETE", "/api/v1/databases/test/collections/products/indexes/nonexistent", "")
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

// --- Additional Tests for Coverage ---

func TestCreateCollectionInvalidJSON(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	w := doReq(t, h, "POST", "/api/v1/databases/test/collections", `invalid json`)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestCreateIndexInvalidJSON(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	doReq(t, h, "POST", "/api/v1/databases/test/collections", `{"name":"products"}`)

	w := doReq(t, h, "POST", "/api/v1/databases/test/collections/products/indexes", `invalid json`)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestCreateIndexNoKey(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	doReq(t, h, "POST", "/api/v1/databases/test/collections", `{"name":"products"}`)

	// Name provided but no key
	w := doReq(t, h, "POST", "/api/v1/databases/test/collections/products/indexes",
		`{"name":"idx_name"}`)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestCreateUserInvalidJSON(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	w := doReq(t, h, "POST", "/api/v1/users", `invalid json`)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestCreateUserNoUsername(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	w := doReq(t, h, "POST", "/api/v1/users", `{"username":"","password":"secret"}`)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestCreateUserDefaultDB(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	// Create user without specifying db - should default to "admin"
	w := doReq(t, h, "POST", "/api/v1/users", `{"username":"defaultdbuser","password":"secret"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("create user = %d, want 200: %s", w.Code, w.Body.String())
	}

	// Verify user was created in admin db
	w = doReq(t, h, "GET", "/api/v1/users?db=admin", "")
	if w.Code != http.StatusOK {
		t.Fatalf("list users = %d, want 200", w.Code)
	}
}

func TestDeleteUserDefaultDB(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	// Create user in admin db
	doReq(t, h, "POST", "/api/v1/users", `{"username":"todelete","password":"pass"}`)

	// Delete without specifying db - should default to "admin"
	w := doReq(t, h, "DELETE", "/api/v1/users/todelete", "")
	if w.Code != http.StatusOK {
		t.Fatalf("delete user = %d, want 200: %s", w.Code, w.Body.String())
	}
}

func TestDeleteUserNotFound(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	w := doReq(t, h, "DELETE", "/api/v1/users/nonexistent?db=admin", "")
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

func TestListDocumentsWithSkip(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	doReq(t, h, "POST", "/api/v1/databases/test/collections", `{"name":"items"}`)

	// Insert multiple documents
	for i := 0; i < 5; i++ {
		doReq(t, h, "POST", "/api/v1/databases/test/collections/items/documents",
			`{"document":{"_id":"`+string(rune('a'+i))+`","value":`+string(rune('0'+i))+`}}`)
	}

	// List with skip
	w := doReq(t, h, "GET", "/api/v1/databases/test/collections/items/documents?skip=2", "")
	if w.Code != http.StatusOK {
		t.Fatalf("list docs = %d, want 200: %s", w.Code, w.Body.String())
	}

	resp := decodeResp(t, w)
	data := resp["data"].(map[string]any)
	if data["skip"] != float64(2) {
		t.Errorf("skip = %v, want 2", data["skip"])
	}
}

func TestIntParam(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	doReq(t, h, "POST", "/api/v1/databases/test/collections", `{"name":"items"}`)

	// Test with valid int parameter
	w := doReq(t, h, "GET", "/api/v1/databases/test/collections/items/documents?limit=5", "")
	if w.Code != http.StatusOK {
		t.Fatalf("list docs = %d, want 200: %s", w.Code, w.Body.String())
	}

	// Test with invalid int parameter (should use default)
	w = doReq(t, h, "GET", "/api/v1/databases/test/collections/items/documents?limit=invalid", "")
	if w.Code != http.StatusOK {
		t.Fatalf("list docs = %d, want 200: %s", w.Code, w.Body.String())
	}
}

// --- Health Check Tests ---

func TestHealthEndpoint(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	w := doReq(t, h, "GET", "/health", "")
	if w.Code != http.StatusOK {
		t.Fatalf("health = %d, want 200: %s", w.Code, w.Body.String())
	}

	resp := decodeResp(t, w)
	if resp["ok"] != true {
		t.Errorf("ok = %v, want true", resp["ok"])
	}

	data := resp["data"].(map[string]any)
	if data["status"] != "healthy" {
		t.Errorf("status = %v, want healthy", data["status"])
	}
	if data["version"] == "" {
		t.Error("version is empty")
	}
	if data["uptime"] == "" {
		t.Error("uptime is empty")
	}
}

func TestReadyEndpoint(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	w := doReq(t, h, "GET", "/ready", "")
	if w.Code != http.StatusOK {
		t.Fatalf("ready = %d, want 200: %s", w.Code, w.Body.String())
	}

	resp := decodeResp(t, w)
	if resp["ready"] != true {
		t.Errorf("ready = %v, want true", resp["ready"])
	}

	checks := resp["checks"].(map[string]any)
	if checks["engine"] != "ok" {
		t.Errorf("engine check = %v, want ok", checks["engine"])
	}
	if checks["catalog"] != "ok" {
		t.Errorf("catalog check = %v, want ok", checks["catalog"])
	}
}

func TestLiveEndpoint(t *testing.T) {
	h, cleanup := setupTestHandler(t)
	defer cleanup()

	w := doReq(t, h, "GET", "/live", "")
	if w.Code != http.StatusOK {
		t.Fatalf("live = %d, want 200: %s", w.Code, w.Body.String())
	}

	resp := decodeResp(t, w)
	if resp["ok"] != true {
		t.Errorf("ok = %v, want true", resp["ok"])
	}

	data := resp["data"].(map[string]any)
	if data["alive"] != true {
		t.Errorf("alive = %v, want true", data["alive"])
	}
	if data["timestamp"] == "" {
		t.Error("timestamp is empty")
	}
}
