package wire

import (
	"encoding/base64"
	"strings"

	"github.com/mammothengine/mammoth/pkg/auth"
	"github.com/mammothengine/mammoth/pkg/bson"
)

// Public commands that don't require authentication.
var publicCommands = map[string]bool{
	"hello": true, "isMaster": true, "ismaster": true,
	"ping": true, "buildInfo": true, "buildinfo": true,
	"whatsmyuri": true, "getCmdLineOpts": true,
	"saslStart": true, "saslContinue": true,
	"connectionStatus": true,
}

func (h *Handler) handleSaslStart(body *bson.Document, connID uint64) *bson.Document {
	mechanism := getStringFromBody(body, "mechanism")
	if mechanism != "SCRAM-SHA-256" {
		return errResponseWithCode("saslStart", "unsupported mechanism: "+mechanism, CodeBadValue)
	}

	var dataStr string
	if v, ok := body.Get("payload"); ok {
		if v.Type == bson.TypeBinary {
			dataStr = string(v.Binary().Data)
		} else if v.Type == bson.TypeString {
			dataStr = v.String()
		}
	}

	username := getStringFromBody(body, "username")
	if username == "" {
		for _, part := range strings.Split(dataStr, ",") {
			if strings.HasPrefix(part, "n=") && !strings.HasPrefix(part, "n,,") {
				username = part[2:]
			}
		}
	}

	session, result, err := auth.StartSCRAM(h.authMgr.UserStore(), username, dataStr)
	if err != nil {
		return errResponseWithCode("saslStart", err.Error(), CodeUnauthorized)
	}

	h.authMgr.SetSCRAMSession(connID, session)

	doc := okDoc()
	doc.Set("conversationId", bson.VInt32(1))
	doc.Set("done", bson.VBool(false))
	doc.Set("payload", bson.VBinary(bson.BinaryGeneric, []byte(result.Data)))
	return doc
}

func (h *Handler) handleSaslContinue(body *bson.Document, connID uint64) *bson.Document {
	var dataStr string
	if v, ok := body.Get("payload"); ok {
		if v.Type == bson.TypeBinary {
			dataStr = string(v.Binary().Data)
		} else if v.Type == bson.TypeString {
			dataStr = v.String()
		}
	}

	scram := h.authMgr.GetSCRAMSession(connID)
	if scram == nil {
		return errResponseWithCode("saslContinue", "no active SCRAM session", CodeUnauthorized)
	}

	result, err := scram.Continue(dataStr)
	if err != nil {
		h.authMgr.RemoveSession(connID)
		return errResponseWithCode("saslContinue", err.Error(), CodeUnauthorized)
	}

	if result.Done {
		h.authMgr.MarkAuthenticated(connID, scram.Username(), "admin")
		h.authMgr.SetRoles(connID, []auth.RoleRef{{DB: "admin", Name: "root"}})
	}

	doc := okDoc()
	doc.Set("conversationId", bson.VInt32(1))
	doc.Set("done", bson.VBool(result.Done))
	doc.Set("payload", bson.VBinary(bson.BinaryGeneric, []byte(result.Data)))
	return doc
}

func (h *Handler) handleCreateUser(body *bson.Document) *bson.Document {
	db := extractDB(body)
	username := getStringFromBody(body, "createUser")
	password := getStringFromBody(body, "pwd")
	if username == "" || password == "" {
		return errResponseWithCode("createUser", "username and password required", CodeBadValue)
	}
	if err := h.authMgr.UserStore().CreateUser(username, db, password); err != nil {
		return errResponseWithCode("createUser", err.Error(), CodeDuplicateKey)
	}
	return okDoc()
}

func (h *Handler) handleDropUser(body *bson.Document) *bson.Document {
	db := extractDB(body)
	username := getStringFromBody(body, "dropUser")
	if username == "" {
		return errResponseWithCode("dropUser", "username required", CodeBadValue)
	}
	if err := h.authMgr.UserStore().DropUser(username, db); err != nil {
		return errResponseWithCode("dropUser", err.Error(), CodeNamespaceNotFound)
	}
	return okDoc()
}

func (h *Handler) handleUsersInfo(body *bson.Document) *bson.Document {
	db := extractDB(body)
	users, err := h.authMgr.UserStore().GetUsersInDB(db)
	if err != nil {
		return errResponseWithCode("usersInfo", err.Error(), CodeInternalError)
	}

	var usersArr bson.Array
	for _, u := range users {
		udoc := bson.NewDocument()
		udoc.Set("_id", bson.VString(db + "." + u.Username))
		udoc.Set("user", bson.VString(u.Username))
		udoc.Set("db", bson.VString(db))
		udoc.Set("createdAt", bson.VDateTime(u.CreatedAt * 1000))
		usersArr = append(usersArr, bson.VDoc(udoc))
	}

	doc := okDoc()
	doc.Set("users", bson.VArray(usersArr))
	return doc
}

// decodeSASLData decodes base64 SASL data.
func decodeSASLData(data string) string {
	decoded, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return data
	}
	return string(decoded)
}
