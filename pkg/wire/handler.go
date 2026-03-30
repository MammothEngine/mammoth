package wire

import (
	"log"
	"strings"
	"sync/atomic"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

// MongoDB error codes
const (
	CodeInternalError     int32 = 1
	CodeBadValue          int32 = 2
	CodeNamespaceNotFound int32 = 26
	CodeNamespaceExists   int32 = 48
	CodeCommandNotFound   int32 = 59
	CodeDuplicateKey      int32 = 110
)

// Handler dispatches wire protocol commands.
type Handler struct {
	engine    *engine.Engine
	cat       *mongo.Catalog
	indexCat  *mongo.IndexCatalog
	cursor    *mongo.CursorManager
	reqID     atomic.Uint64
	processID bson.ObjectID
	connID    atomic.Uint64
}

// NewHandler creates a new command handler.
func NewHandler(eng *engine.Engine, cat *mongo.Catalog) *Handler {
	return &Handler{
		engine:    eng,
		cat:       cat,
		indexCat:  mongo.NewIndexCatalog(eng, cat),
		cursor:    mongo.NewCursorManager(),
		processID: bson.NewObjectID(),
	}
}

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

	log.Printf("cmd: %s", cmd)

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
	case "find":
		return h.handleFind(body)
	case "insert":
		return h.handleInsert(body)
	case "update":
		return h.handleUpdate(body)
	case "delete":
		return h.handleDelete(body)
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
	case "endSessions":
		return okDoc()
	case "connectionStatus":
		return h.handleConnectionStatus()
	case "dropDatabase":
		return h.handleDropDatabase(body)
	case "aggregate":
		return h.handleAggregate(body)
	case "count":
		return h.handleCount(body)
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
