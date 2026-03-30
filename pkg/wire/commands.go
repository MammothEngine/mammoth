package wire

import (
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
)

func (h *Handler) handleHello() *bson.Document {
	doc := okDoc()
	doc.Set("isWritablePrimary", bson.VBool(true))
	doc.Set("ismaster", bson.VBool(true))
	doc.Set("maxBsonObjectSize", bson.VInt32(16 * 1024 * 1024))
	doc.Set("maxMessageSizeBytes", bson.VInt32(48000000))
	doc.Set("maxWriteBatchSize", bson.VInt32(100000))
	doc.Set("localTime", bson.VDateTime(time.Now().UnixMilli()))
	doc.Set("minWireVersion", bson.VInt32(0))
	doc.Set("maxWireVersion", bson.VInt32(17))
	doc.Set("readOnly", bson.VBool(false))

	// topologyVersion
	tv := bson.NewDocument()
	tv.Set("processId", bson.VObjectID(h.processID))
	tv.Set("counter", bson.VInt64(0))
	doc.Set("topologyVersion", bson.VDoc(tv))

	doc.Set("logicalSessionTimeoutMinutes", bson.VInt32(30))
	doc.Set("connectionId", bson.VInt64(int64(h.connID.Add(1))))

	doc.Set("ok", bson.VDouble(1.0))
	return doc
}

func (h *Handler) handleStartSession() *bson.Document {
	doc := okDoc()
	idDoc := bson.NewDocument()
	idDoc.Set("id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("id", bson.VDoc(idDoc))
	return doc
}

func (h *Handler) handleConnectionStatus() *bson.Document {
	doc := okDoc()
	authInfo := bson.NewDocument()
	authInfo.Set("authenticatedUsers", bson.VArray(bson.Array{}))
	authInfo.Set("authenticatedUserRoles", bson.VArray(bson.Array{}))
	doc.Set("authInfo", bson.VDoc(authInfo))
	return doc
}

func (h *Handler) handlePing() *bson.Document {
	return okDoc()
}

func (h *Handler) handleBuildInfo() *bson.Document {
	doc := okDoc()
	doc.Set("version", bson.VString("7.0.0"))
	doc.Set("gitVersion", bson.VString("mammoth-engine-0.2.0"))
	doc.Set("modules", bson.VArray(bson.Array{}))
	doc.Set("sysInfo", bson.VString("Mammoth Engine - Pure Go MongoDB-compatible server"))
	doc.Set("versionArray", bson.VArray(bson.A(
		bson.VInt32(7), bson.VInt32(0), bson.VInt32(0), bson.VInt32(0),
	)))
	doc.Set("bits", bson.VInt32(int32(64)))
	doc.Set("debug", bson.VBool(false))
	doc.Set("maxBsonObjectSize", bson.VInt32(16*1024*1024))
	doc.Set("ok", bson.VDouble(1.0))
	return doc
}

func (h *Handler) handleWhatsmyuri(msg *Message) *bson.Document {
	doc := okDoc()
	addr := "127.0.0.1"
	if msg != nil && msg.RemoteAddr != "" {
		addr = msg.RemoteAddr
	}
	doc.Set("you", bson.VString(addr))
	doc.Set("ok", bson.VDouble(1.0))
	return doc
}

func (h *Handler) handleGetCmdLineOpts() *bson.Document {
	doc := okDoc()
	argv := bson.A(bson.VString("mammoth"), bson.VString("serve"))
	doc.Set("argv", bson.VArray(argv))
	doc.Set("parsed", bson.VDoc(bson.NewDocument()))
	doc.Set("ok", bson.VDouble(1.0))
	return doc
}
