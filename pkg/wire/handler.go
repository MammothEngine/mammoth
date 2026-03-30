package wire

import (
	"strings"
	"sync/atomic"
	"time"

	"github.com/mammothengine/mammoth/pkg/auth"
	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/logging"
	"github.com/mammothengine/mammoth/pkg/metrics"
	"github.com/mammothengine/mammoth/pkg/mongo"
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
	engine    *engine.Engine
	cat       *mongo.Catalog
	indexCat  *mongo.IndexCatalog
	cursor    *mongo.CursorManager
	reqID     atomic.Uint64
	processID bson.ObjectID
	connID    atomic.Uint64
	authMgr   *auth.AuthManager
	metrics   *HandlerMetrics
	slowQuery *SlowQueryProfiler
	startTime time.Time
	log       *logging.Logger
	connCountFn func() int64
}

// NewHandler creates a new command handler.
func NewHandler(eng *engine.Engine, cat *mongo.Catalog, authMgr *auth.AuthManager) *Handler {
	return &Handler{
		engine:    eng,
		cat:       cat,
		indexCat:  mongo.NewIndexCatalog(eng, cat),
		cursor:    mongo.NewCursorManager(),
		processID: bson.NewObjectID(),
		authMgr:   authMgr,
		startTime: time.Now(),
		log:       logging.Default().WithComponent("wire"),
	}
}

// WithMetrics sets handler metrics.
func (h *Handler) WithMetrics(m *HandlerMetrics) *Handler {
	h.metrics = m
	return h
}

// WithSlowQueryProfiler sets the slow query profiler.
func (h *Handler) WithSlowQueryProfiler(p *SlowQueryProfiler) *Handler {
	h.slowQuery = p
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
	cmd := msg.Command()
	body := msg.Body()

	if cmd == "" || body == nil {
		return errResponseWithCode("unknown", "empty command", CodeBadValue)
	}

	h.log.Debug("command", logging.FString("cmd", cmd), logging.FString("remote", msg.RemoteAddr))

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

	var response *bson.Document
	switch cmd {
	case "hello", "isMaster", "ismaster":
		response = h.handleHello()
	case "ping":
		response = h.handlePing()
	case "buildInfo", "buildinfo":
		response = h.handleBuildInfo()
	case "whatsmyuri":
		response = h.handleWhatsmyuri(msg)
	case "getCmdLineOpts":
		response = h.handleGetCmdLineOpts()
	case "listDatabases":
		response = h.handleListDatabases()
	case "listCollections":
		response = h.handleListCollections(body)
	case "create":
		response = h.handleCreate(body)
	case "drop":
		response = h.handleDrop(body)
	case "find":
		response = h.handleFind(body)
	case "insert":
		response = h.handleInsert(body)
	case "update":
		response = h.handleUpdate(body)
	case "delete":
		response = h.handleDelete(body)
	case "getMore":
		response = h.handleGetMore(body)
	case "killCursors":
		response = h.handleKillCursors(body)
	case "createIndexes":
		response = h.handleCreateIndexes(body)
	case "dropIndexes":
		response = h.handleDropIndexes(body)
	case "listIndexes":
		response = h.handleListIndexes(body)
	case "serverStatus":
		response = h.handleServerStatus()
	case "startSession":
		response = h.handleStartSession()
	case "endSessions":
		response = okDoc()
	case "connectionStatus":
		response = h.handleConnectionStatus(msg.ConnID)
	case "dropDatabase":
		response = h.handleDropDatabase(body)
	case "aggregate":
		response = h.handleAggregate(body)
	case "count":
		response = h.handleCount(body)
	case "saslStart":
		response = h.handleSaslStart(body, msg.ConnID)
	case "saslContinue":
		response = h.handleSaslContinue(body, msg.ConnID)
	case "createUser":
		response = h.handleCreateUser(body)
	case "dropUser":
		response = h.handleDropUser(body)
	case "usersInfo":
		response = h.handleUsersInfo(body)
	case "createRole":
		response = h.handleCreateRole(body)
	case "updateRole":
		response = h.handleUpdateRole(body)
	case "dropRole":
		response = h.handleDropRole(body)
	case "rolesInfo":
		response = h.handleRolesInfo(body)
	default:
		response = errResponseWithCode(cmd, "unknown command", CodeCommandNotFound)
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

	return response
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
