package wire

import (
	"github.com/mammothengine/mammoth/pkg/bson"
)

func (h *Handler) handleCreateRole(body *bson.Document) *bson.Document {
	db := extractDB(body)
	roleName := getStringFromBody(body, "createRole")
	if roleName == "" {
		return errResponseWithCode("createRole", "role name required", CodeBadValue)
	}
	// Store a minimal role record — for now, accept and ack
	// Full implementation would serialize privileges to the catalog
	doc := okDoc()
	doc.Set("role", bson.VString(roleName))
	doc.Set("db", bson.VString(db))
	return doc
}

func (h *Handler) handleUpdateRole(body *bson.Document) *bson.Document {
	roleName := getStringFromBody(body, "updateRole")
	if roleName == "" {
		return errResponseWithCode("updateRole", "role name required", CodeBadValue)
	}
	return okDoc()
}

func (h *Handler) handleDropRole(body *bson.Document) *bson.Document {
	db := extractDB(body)
	roleName := getStringFromBody(body, "dropRole")
	if roleName == "" {
		return errResponseWithCode("dropRole", "role name required", CodeBadValue)
	}
	_ = db
	return okDoc()
}

func (h *Handler) handleRolesInfo(body *bson.Document) *bson.Document {
	doc := okDoc()
	doc.Set("roles", bson.VArray(bson.Array{}))
	return doc
}
