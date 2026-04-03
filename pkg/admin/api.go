package admin

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/mammothengine/mammoth/pkg/auth"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

// APIHandler serves REST API and static admin UI files.
type APIHandler struct {
	engine   *engine.Engine
	cat      *mongo.Catalog
	indexCat *mongo.IndexCatalog
	authMgr  *auth.AuthManager
	started  time.Time
	version  string
	router   *Router
}

// NewAPIHandler creates a new admin API handler.
func NewAPIHandler(eng *engine.Engine, cat *mongo.Catalog, authMgr *auth.AuthManager, version string) *APIHandler {
	h := &APIHandler{
		engine:   eng,
		cat:      cat,
		indexCat: mongo.NewIndexCatalog(eng, cat),
		authMgr:  authMgr,
		started:  time.Now(),
		version:  version,
		router:   NewRouter(),
	}
	h.registerRoutes()
	return h
}

func (h *APIHandler) registerRoutes() {
	r := h.router

	// Status
	r.Handle("GET", "/api/v1/status", h.handleStatus)

	// Databases
	r.Handle("GET", "/api/v1/databases", h.handleListDatabases)
	r.Handle("GET", "/api/v1/databases/:db/stats", h.handleDBStats)
	r.Handle("GET", "/api/v1/databases/:db/collections", h.handleListCollections)
	r.Handle("POST", "/api/v1/databases/:db/collections", h.handleCreateCollection)
	r.Handle("DELETE", "/api/v1/databases/:db/collections/:coll", h.handleDropCollection)
	r.Handle("GET", "/api/v1/databases/:db/collections/:coll/stats", h.handleCollStats)

	// Documents
	r.Handle("GET", "/api/v1/databases/:db/collections/:coll/documents", h.handleListDocuments)
	r.Handle("POST", "/api/v1/databases/:db/collections/:coll/documents", h.handleInsertDocument)
	r.Handle("DELETE", "/api/v1/databases/:db/collections/:coll/documents", h.handleDeleteDocuments)

	// Indexes
	r.Handle("GET", "/api/v1/databases/:db/collections/:coll/indexes", h.handleListIndexes)
	r.Handle("POST", "/api/v1/databases/:db/collections/:coll/indexes", h.handleCreateIndex)
	r.Handle("DELETE", "/api/v1/databases/:db/collections/:coll/indexes/:name", h.handleDropIndex)

	// Users
	r.Handle("GET", "/api/v1/users", h.handleListUsers)
	r.Handle("POST", "/api/v1/users", h.handleCreateUser)
	r.Handle("DELETE", "/api/v1/users/:username", h.handleDeleteUser)

	// Health checks
	r.Handle("GET", "/health", h.handleHealth)
	r.Handle("GET", "/ready", h.handleReady)
	r.Handle("GET", "/live", h.handleLive)
}

// ServeHTTP routes requests to API or static files.
func (h *APIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// CORS headers
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// API routes (including health checks)
	if strings.HasPrefix(r.URL.Path, "/api/v1") ||
		strings.HasPrefix(r.URL.Path, "/health") ||
		strings.HasPrefix(r.URL.Path, "/ready") ||
		strings.HasPrefix(r.URL.Path, "/live") {
		h.router.ServeHTTP(w, r)
		return
	}

	// Static files (SPA fallback)
	serveStaticFn(w, r)
}

// --- Status ---

func (h *APIHandler) handleStatus(w http.ResponseWriter, r *http.Request, _ map[string]string) {
	stats := h.engine.Stats()
	writeOK(w, map[string]any{
		"version": h.version,
		"uptime":  time.Since(h.started).String(),
		"engine": map[string]any{
			"memtables":      stats.MemtableCount,
			"memtableSize":   stats.MemtableSizeBytes,
			"sstables":       stats.SSTableCount,
			"sstableSize":    stats.SSTableTotalBytes,
			"compactions":    stats.CompactionCount,
			"sequenceNumber": stats.SequenceNumber,
		},
		"operations": map[string]any{
			"puts":    stats.PutCount,
			"gets":    stats.GetCount,
			"deletes": stats.DeleteCount,
			"scans":   stats.ScanCount,
		},
	})
}

// --- Databases ---

func (h *APIHandler) handleListDatabases(w http.ResponseWriter, r *http.Request, _ map[string]string) {
	dbs, err := h.cat.ListDatabases()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeOK(w, map[string]any{"databases": dbs})
}

func (h *APIHandler) handleDBStats(w http.ResponseWriter, r *http.Request, p map[string]string) {
	dbName := p["db"]
	colls, err := h.cat.ListCollections(dbName)
	if err != nil {
		writeError(w, http.StatusNotFound, "database not found")
		return
	}
	writeOK(w, map[string]any{
		"db":          dbName,
		"collections": len(colls),
	})
}

func (h *APIHandler) handleListCollections(w http.ResponseWriter, r *http.Request, p map[string]string) {
	colls, err := h.cat.ListCollections(p["db"])
	if err != nil {
		writeError(w, http.StatusNotFound, "database not found")
		return
	}
	writeOK(w, map[string]any{"collections": colls})
}

func (h *APIHandler) handleCreateCollection(w http.ResponseWriter, r *http.Request, p map[string]string) {
	var body struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if body.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	// Ensure database exists
	if err := h.cat.EnsureDatabase(p["db"]); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := h.cat.CreateCollection(p["db"], body.Name); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeOK(w, map[string]any{"created": body.Name})
}

func (h *APIHandler) handleDropCollection(w http.ResponseWriter, r *http.Request, p map[string]string) {
	if err := h.cat.DropCollection(p["db"], p["coll"]); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeOK(w, map[string]any{"dropped": p["coll"]})
}

func (h *APIHandler) handleCollStats(w http.ResponseWriter, r *http.Request, p map[string]string) {
	dbName, collName := p["db"], p["coll"]
	prefix := mongo.EncodeNamespacePrefix(dbName, collName)

	docCount := 0
	var dataSize int64
	it := h.engine.NewPrefixIterator(prefix)
	for it.Next() {
		docCount++
		dataSize += int64(len(it.Value()))
	}
	it.Close()

	indexes, _ := h.indexCat.ListIndexes(dbName, collName)

	writeOK(w, map[string]any{
		"db":         dbName,
		"collection": collName,
		"count":      docCount,
		"size":       dataSize,
		"indexes":    len(indexes),
	})
}

// --- Documents ---

func (h *APIHandler) handleListDocuments(w http.ResponseWriter, r *http.Request, p map[string]string) {
	dbName, collName := p["db"], p["coll"]
	limit := intParam(r, "limit", 20)
	skip := intParam(r, "skip", 0)

	prefix := mongo.EncodeNamespacePrefix(dbName, collName)
	var docs []json.RawMessage
	skipped := 0

	it := h.engine.NewPrefixIterator(prefix)
	defer it.Close()

	for it.Next() {
		if skipped < skip {
			skipped++
			continue
		}
		if len(docs) >= limit {
			break
		}
		docs = append(docs, json.RawMessage(it.Value()))
	}

	if docs == nil {
		docs = []json.RawMessage{}
	}
	writeOK(w, map[string]any{
		"documents": docs,
		"count":     len(docs),
		"skip":      skip,
		"limit":     limit,
	})
}

func (h *APIHandler) handleInsertDocument(w http.ResponseWriter, r *http.Request, p map[string]string) {
	dbName, collName := p["db"], p["coll"]

	var body struct {
		Document json.RawMessage `json:"document"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}

	if err := h.cat.EnsureDatabase(dbName); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := h.cat.EnsureCollection(dbName, collName); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Generate key using sequence number
	key := mongo.EncodeNamespacePrefix(dbName, collName)
	key = append(key, fmt.Appendf(nil, "%d", h.engine.Stats().SequenceNumber+1)...)
	if err := h.engine.Put(key, body.Document); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeOK(w, map[string]any{"inserted": 1})
}

func (h *APIHandler) handleDeleteDocuments(w http.ResponseWriter, r *http.Request, p map[string]string) {
	dbName, collName := p["db"], p["coll"]
	filter := r.URL.Query().Get("filter")

	if filter == "" {
		writeError(w, http.StatusBadRequest, "filter query parameter required")
		return
	}

	// Simple prefix-based delete
	prefix := mongo.EncodeNamespacePrefix(dbName, collName)
	deleted := 0

	it := h.engine.NewPrefixIterator(prefix)
	defer it.Close()

	// Collect keys to delete
	var keys [][]byte
	for it.Next() {
		keys = append(keys, append([]byte(nil), it.Key()...))
	}

	for _, key := range keys {
		if err := h.engine.Delete(key); err != nil {
			continue
		}
		deleted++
	}

	writeOK(w, map[string]any{"deleted": deleted})
}

// --- Indexes ---

func (h *APIHandler) handleListIndexes(w http.ResponseWriter, r *http.Request, p map[string]string) {
	indexes, err := h.indexCat.ListIndexes(p["db"], p["coll"])
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeOK(w, map[string]any{"indexes": indexes})
}

func (h *APIHandler) handleCreateIndex(w http.ResponseWriter, r *http.Request, p map[string]string) {
	var spec mongo.IndexSpec
	if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if spec.Name == "" || len(spec.Key) == 0 {
		writeError(w, http.StatusBadRequest, "name and key are required")
		return
	}
	if err := h.indexCat.CreateIndex(p["db"], p["coll"], spec); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeOK(w, map[string]any{"created": spec.Name})
}

func (h *APIHandler) handleDropIndex(w http.ResponseWriter, r *http.Request, p map[string]string) {
	if err := h.indexCat.DropIndex(p["db"], p["coll"], p["name"]); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeOK(w, map[string]any{"dropped": p["name"]})
}

// --- Users ---

func (h *APIHandler) handleListUsers(w http.ResponseWriter, r *http.Request, _ map[string]string) {
	db := r.URL.Query().Get("db")
	var users any
	var err error
	if db != "" {
		users, err = h.authMgr.UserStore().GetUsersInDB(db)
	} else {
		users = []struct{}{}
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeOK(w, map[string]any{"users": users})
}

func (h *APIHandler) handleCreateUser(w http.ResponseWriter, r *http.Request, _ map[string]string) {
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
		DB       string `json:"db"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if body.Username == "" || body.Password == "" {
		writeError(w, http.StatusBadRequest, "username and password are required")
		return
	}
	if body.DB == "" {
		body.DB = "admin"
	}
	if err := h.authMgr.UserStore().CreateUser(body.Username, body.DB, body.Password); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeOK(w, map[string]any{"created": body.Username})
}

func (h *APIHandler) handleDeleteUser(w http.ResponseWriter, r *http.Request, p map[string]string) {
	db := r.URL.Query().Get("db")
	if db == "" {
		db = "admin"
	}
	if err := h.authMgr.UserStore().DropUser(p["username"], db); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeOK(w, map[string]any{"deleted": p["username"]})
}

// --- Helpers ---

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeOK(w http.ResponseWriter, data any) {
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":   true,
		"data": data,
	})
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{
		"ok":    false,
		"error": msg,
	})
}

func intParam(r *http.Request, name string, defaultVal int) int {
	s := r.URL.Query().Get(name)
	if s == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return defaultVal
	}
	return n
}

// serveStaticFn is the function used to serve static files.
// It defaults to a placeholder; ui.go overrides it via init().
var serveStaticFn = defaultServeStatic

func defaultServeStatic(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte("<h1>Admin UI</h1><p>Static files not embedded.</p>"))
}

// --- Health Checks ---

// handleHealth returns general health status.
func (h *APIHandler) handleHealth(w http.ResponseWriter, r *http.Request, _ map[string]string) {
	status := map[string]any{
		"status":    "healthy",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"version":   h.version,
		"uptime":    time.Since(h.started).String(),
	}

	// Check engine health
	if h.engine != nil {
		stats := h.engine.Stats()
		status["engine"] = map[string]any{
			"healthy":        true,
			"memtables":      stats.MemtableCount,
			"sstables":       stats.SSTableCount,
		}
	}

	writeOK(w, status)
}

// handleReady returns readiness status (can accept traffic).
func (h *APIHandler) handleReady(w http.ResponseWriter, r *http.Request, _ map[string]string) {
	ready := true
	checks := make(map[string]any)

	// Check if engine is available
	if h.engine == nil {
		ready = false
		checks["engine"] = "not initialized"
	} else {
		checks["engine"] = "ok"
	}

	// Check if catalog is available
	if h.cat == nil {
		ready = false
		checks["catalog"] = "not initialized"
	} else {
		checks["catalog"] = "ok"
	}

	status := http.StatusOK
	if !ready {
		status = http.StatusServiceUnavailable
	}

	writeJSON(w, status, map[string]any{
		"ready":   ready,
		"checks":  checks,
		"version": h.version,
	})
}

// handleLive returns liveness status (is the process running).
func (h *APIHandler) handleLive(w http.ResponseWriter, r *http.Request, _ map[string]string) {
	// Simple liveness check - if we can respond, we're alive
	writeOK(w, map[string]any{
		"alive":     true,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}
