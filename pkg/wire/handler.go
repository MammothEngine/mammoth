package wire

import (
	"log"
	"sync/atomic"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

// Handler dispatches wire protocol commands.
type Handler struct {
	engine *engine.Engine
	cat    *mongo.Catalog
	cursor *mongo.CursorManager
	reqID  atomic.Uint64
}

// NewHandler creates a new command handler.
func NewHandler(eng *engine.Engine, cat *mongo.Catalog) *Handler {
	return &Handler{
		engine: eng,
		cat:    cat,
		cursor: mongo.NewCursorManager(),
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
		return h.errResponse("unknown", "empty command")
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
		return h.handleWhatsmyuri()
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
	default:
		return h.errResponse(cmd, "unknown command")
	}
}

// --- Helper methods ---

func okDoc() *bson.Document {
	doc := bson.NewDocument()
	doc.Set("ok", bson.VDouble(1.0))
	return doc
}

func (h *Handler) errResponse(cmd, msg string) *bson.Document {
	doc := bson.NewDocument()
	doc.Set("ok", bson.VDouble(0.0))
	doc.Set("errmsg", bson.VString(msg))
	doc.Set("code", bson.VInt32(0))
	doc.Set("codeName", bson.VString("UnknownError"))
	return doc
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
