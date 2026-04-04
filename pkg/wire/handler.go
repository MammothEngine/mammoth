package wire

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mammothengine/mammoth/pkg/audit"
	"github.com/mammothengine/mammoth/pkg/auth"
	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/circuitbreaker"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/logging"
	"github.com/mammothengine/mammoth/pkg/metrics"
	"github.com/mammothengine/mammoth/pkg/mongo"
	"github.com/mammothengine/mammoth/pkg/ratelimit"
)

// MongoDB error codes
const (
	CodeInternalError     int32 = 1
	CodeBadValue          int32 = 2
	CodeUnauthorized      int32 = 13
	CodeNamespaceNotFound int32 = 26
	CodeNamespaceExists   int32 = 48
	CodeCommandNotFound   int32 = 59
	CodeDuplicateKey      int32 = 110
)

// HandlerMetrics holds metrics for the wire handler.
type HandlerMetrics struct {
	CommandDuration *metrics.Histogram
	TotalCommands   *metrics.Counter
	Errors          *metrics.Counter
}

// Handler dispatches wire protocol commands.
type Handler struct {
	engine           *engine.Engine
	cat              *mongo.Catalog
	indexCat         *mongo.IndexCatalog
	cursor           *mongo.CursorManager
	processID        bson.ObjectID
	connID           atomic.Uint64
	authMgr          *auth.AuthManager
	audit            *audit.AuditLogger
	metrics          *HandlerMetrics
	slowQuery        *SlowQueryProfiler
	startTime        time.Time
	log              *logging.Logger
	connCountFn      func() int64
	oplog            *mongo.Oplog
	changeStreamMgr  *mongo.ChangeStreamManager
	opTracker        *opTracker
	sessionMgr       *SessionManager
	rateLimiter      *ratelimit.Manager
	circuitBreaker   *circuitbreaker.Manager
}

// NewHandler creates a new command handler.
func NewHandler(eng *engine.Engine, cat *mongo.Catalog, authMgr *auth.AuthManager) *Handler {
	return &Handler{
		engine:          eng,
		cat:             cat,
		indexCat:        mongo.NewIndexCatalog(eng, cat),
		cursor:          mongo.NewCursorManager(),
		processID:       bson.NewObjectID(),
		authMgr:         authMgr,
		startTime:       time.Now(),
		log:             logging.Default().WithComponent("wire"),
		oplog:           mongo.NewOplog(eng),
		changeStreamMgr: mongo.NewChangeStreamManager(eng),
		opTracker:       newOpTracker(),
		sessionMgr:      NewSessionManager(),
	}
}

// SetSessionManager sets the session manager for the handler.
func (h *Handler) SetSessionManager(sm *SessionManager) {
	h.sessionMgr = sm
}

// txWrapper wraps a Transaction to match engineOps interface.
type txWrapper struct {
	tx *engine.Transaction
}

func (w *txWrapper) Get(key []byte) ([]byte, error) {
	return w.tx.Get(key)
}

func (w *txWrapper) Put(key, value []byte) error {
	w.tx.Put(key, value)
	return nil
}

func (w *txWrapper) Delete(key []byte) {
	w.tx.Delete(key)
}

// engineWrapper wraps an Engine to match engineOps interface.
type engineWrapper struct {
	eng *engine.Engine
}

func (w *engineWrapper) Get(key []byte) ([]byte, error) {
	return w.eng.Get(key)
}

func (w *engineWrapper) Put(key, value []byte) error {
	return w.eng.Put(key, value)
}

func (w *engineWrapper) Delete(key []byte) {
	w.eng.Delete(key)
}

// getEngine returns the engine or active transaction for the connection.
func (h *Handler) getEngine(connID uint64) engineOps {
	if h.sessionMgr.IsInTransaction(connID) {
		return &txWrapper{tx: h.sessionMgr.GetTransaction(connID)}
	}
	return &engineWrapper{eng: h.engine}
}

// engineOps is the interface for engine operations (Engine or Transaction).
type engineOps interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte)
}

func (h *Handler) WithMetrics(m *HandlerMetrics) *Handler {
	h.metrics = m
	return h
}

// WithSlowQueryProfiler sets the slow query profiler.
func (h *Handler) WithSlowQueryProfiler(p *SlowQueryProfiler) *Handler {
	h.slowQuery = p
	return h
}

// WithAudit sets the audit logger for the handler.
func (h *Handler) WithAudit(a *audit.AuditLogger) *Handler {
	h.audit = a
	return h
}

// WithRateLimiter sets the rate limiter manager.
func (h *Handler) WithRateLimiter(rm *ratelimit.Manager) *Handler {
	h.rateLimiter = rm
	return h
}

// WithCircuitBreaker sets the circuit breaker manager.
func (h *Handler) WithCircuitBreaker(cb *circuitbreaker.Manager) *Handler {
	h.circuitBreaker = cb
	return h
}

// SetConnCountFn sets the function to get current connection count.
func (h *Handler) SetConnCountFn(fn func() int64) {
	h.connCountFn = fn
}

// StartTime returns when the handler was created.
func (h *Handler) StartTime() time.Time { return h.startTime }

// Close cleans up handler resources.
func (h *Handler) Close() {
	if h.cursor != nil {
		h.cursor.Close()
	}
}

// Handle processes an incoming message and returns a response document.
func (h *Handler) Handle(msg *Message) *bson.Document {
	return h.HandleWithContext(context.Background(), msg)
}

// HandleWithContext processes a message with context (supports timeout/cancellation).
func (h *Handler) HandleWithContext(ctx context.Context, msg *Message) *bson.Document {
	cmd := msg.Command()
	body := msg.Body()

	if cmd == "" || body == nil {
		return errResponseWithCode("unknown", "empty command", CodeBadValue)
	}

	// Setup request context with correlation and request IDs
	if logging.GetCorrelationID(ctx) == "" {
		ctx = logging.WithCorrelationID(ctx, "")
	}
	if logging.GetRequestID(ctx) == "" {
		ctx = logging.WithRequestID(ctx, "")
	}

	// Create a logger with request context
	reqLog := logging.LoggerWithContext(h.log, ctx)
	reqLog.Debug("command", logging.FString("cmd", cmd), logging.FString("remote", msg.RemoteAddr))

	start := time.Now()

	// Auth check
	if !publicCommands[cmd] && h.authMgr != nil && h.authMgr.Enabled() {
		if !h.authMgr.IsAuthenticated(msg.ConnID) {
			return errResponseWithCode(cmd, "not authenticated", CodeUnauthorized)
		}

		// RBAC check
		action := auth.CommandToAction(cmd)
		db := extractDB(body)
		coll := extractCollection(body)
		if !h.authMgr.CheckPermission(msg.ConnID, action, auth.Resource{DB: db, Collection: coll}) {
			return errResponseWithCode(cmd, "not authorized", CodeUnauthorized)
		}
	}

	// Rate limiting check
	if h.rateLimiter != nil && !h.rateLimiter.Allow(msg.ConnID) {
		return errResponseWithCode(cmd, "rate limit exceeded", 16500) // ExceededTimeLimit error code
	}

	// Circuit breaker check
	var response *bson.Document
	if h.circuitBreaker != nil {
		err := h.circuitBreaker.ExecuteContext(ctx, "wire", func() error {
			response = h.executeCommand(ctx, cmd, body, msg)
			if ok, _ := response.Get("ok"); ok.Type == bson.TypeDouble && ok.Double() == 0 {
				if errmsg, ok := response.Get("errmsg"); ok && errmsg.Type == bson.TypeString {
					return errors.New(errmsg.String())
				}
				return errors.New("command failed")
			}
			return nil
		})
		if err == circuitbreaker.ErrCircuitOpen {
			return errResponseWithCode(cmd, "service temporarily unavailable", 89) // NetworkTimeout error code
		}
	} else {
		response = h.executeCommand(ctx, cmd, body, msg)
	}

	// Record metrics
	duration := time.Since(start)
	if h.metrics != nil {
		h.metrics.TotalCommands.Inc()
		h.metrics.CommandDuration.Observe(duration.Seconds())
		if response != nil {
			if ok, _ := response.Get("ok"); ok.Type == bson.TypeDouble && ok.Double() == 0 {
				h.metrics.Errors.Inc()
			}
		}
	}
	if h.slowQuery != nil {
		h.slowQuery.Record(cmd, extractDB(body), duration)
	}

	// Audit logging for write operations
	if h.audit != nil {
		db := extractDB(body)
		coll := extractCollection(body)
		switch cmd {
		case "insert", "update", "delete", "create", "drop", "createIndexes", "dropIndexes",
			"dropDatabase", "createUser", "dropUser", "createRole", "dropRole", "updateRole",
			"findAndModify":
			h.audit.LogOperation(cmd, db, coll, duration)
		}
	}

	return response
}

// executeCommand executes a single command and returns the response.
func (h *Handler) executeCommand(ctx context.Context, cmd string, body *bson.Document, msg *Message) *bson.Document {
	switch cmd {
	case "hello", "isMaster", "ismaster":
		return h.handleHello()
	case "ping":
		return h.handlePing()
	case "buildInfo", "buildinfo":
		return h.handleBuildInfo()
	case "whatsmyuri":
		return h.handleWhatsmyuri(msg)
	case "getCmdLineOpts":
		return h.handleGetCmdLineOpts()
	case "listDatabases":
		return h.handleListDatabases()
	case "listCollections":
		return h.handleListCollections(body)
	case "create":
		return h.handleCreate(body)
	case "drop":
		return h.handleDrop(body)
	case "collMod":
		return h.handleCollMod(body)
	case "find":
		return h.handleFind(body)
	case "insert":
		return h.handleInsert(msg, body)
	case "update":
		return h.handleUpdate(msg, body)
	case "delete":
		return h.handleDelete(msg, body)
	case "getMore":
		return h.handleGetMore(body)
	case "killCursors":
		return h.handleKillCursors(body)
	case "createIndexes":
		return h.handleCreateIndexes(body)
	case "dropIndexes":
		return h.handleDropIndexes(body)
	case "listIndexes":
		return h.handleListIndexes(body)
	case "serverStatus":
		return h.handleServerStatus()
	case "startSession":
		return h.handleStartSession()
	case "startTransaction":
		return h.handleStartTransaction(body, msg.ConnID)
	case "commitTransaction":
		return h.handleCommitTransaction(msg.ConnID)
	case "abortTransaction":
		return h.handleAbortTransaction(msg.ConnID)
	case "endSessions":
		return okDoc()
	case "connectionStatus":
		return h.handleConnectionStatus(msg.ConnID)
	case "dropDatabase":
		return h.handleDropDatabase(body)
	case "aggregate":
		return h.handleAggregate(body)
	case "count":
		return h.handleCount(body)
	case "findAndModify":
		return h.handleFindAndModify(body)
	case "distinct":
		return h.handleDistinct(body)
	case "explain":
		return h.handleExplain(body)
	case "currentOp":
		return h.handleCurrentOp(body)
	case "killOp":
		return h.handleKillOp(body)
	case "collStats":
		return h.handleCollStats(body)
	case "dbStats":
		return h.handleDbStats(body)
	case "saslStart":
		return h.handleSaslStart(body, msg.ConnID)
	case "saslContinue":
		return h.handleSaslContinue(body, msg.ConnID)
	case "createUser":
		return h.handleCreateUser(body)
	case "dropUser":
		return h.handleDropUser(body)
	case "usersInfo":
		return h.handleUsersInfo(body)
	case "createRole":
		return h.handleCreateRole(body)
	case "updateRole":
		return h.handleUpdateRole(body)
	case "dropRole":
		return h.handleDropRole(body)
	case "rolesInfo":
		return h.handleRolesInfo(body)
	default:
		return errResponseWithCode(cmd, "unknown command", CodeCommandNotFound)
	}
}

// --- Helper methods ---

func okDoc() *bson.Document {
	doc := bson.NewDocument()
	doc.Set("ok", bson.VDouble(1.0))
	return doc
}

func errResponseWithCode(cmd string, msg string, code int32) *bson.Document {
	doc := bson.NewDocument()
	doc.Set("ok", bson.VDouble(0.0))
	doc.Set("errmsg", bson.VString(msg))
	doc.Set("code", bson.VInt32(code))
	doc.Set("codeName", bson.VString(codeName(code)))
	return doc
}

func codeName(code int32) string {
	switch code {
	case CodeInternalError:
		return "InternalError"
	case CodeBadValue:
		return "BadValue"
	case CodeUnauthorized:
		return "Unauthorized"
	case CodeNamespaceNotFound:
		return "NamespaceNotFound"
	case CodeNamespaceExists:
		return "NamespaceExists"
	case CodeCommandNotFound:
		return "CommandNotFound"
	case CodeDuplicateKey:
		return "DuplicateKey"
	default:
		return "UnknownError"
	}
}

func mongoErrToCode(err error) int32 {
	if err == nil {
		return 0
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "not found"):
		return CodeNamespaceNotFound
	case strings.Contains(msg, "already exists") || strings.Contains(msg, "duplicate"):
		return CodeNamespaceExists
	default:
		return CodeInternalError
	}
}

func getStringFromBody(body *bson.Document, key string) string {
	if v, ok := body.Get(key); ok && v.Type == bson.TypeString {
		return v.String()
	}
	return ""
}

func getDocFromBody(body *bson.Document, key string) *bson.Document {
	if v, ok := body.Get(key); ok && v.Type == bson.TypeDocument {
		return v.DocumentValue()
	}
	return nil
}

func getArrayFromBody(body *bson.Document, key string) bson.Array {
	if v, ok := body.Get(key); ok && v.Type == bson.TypeArray {
		return v.ArrayValue()
	}
	return nil
}

func getInt32FromBody(body *bson.Document, key string) int32 {
	if v, ok := body.Get(key); ok && v.Type == bson.TypeInt32 {
		return v.Int32()
	}
	return 0
}
