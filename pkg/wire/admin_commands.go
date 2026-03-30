package wire

import (
	"github.com/mammothengine/mammoth/pkg/bson"
)

func (h *Handler) handleListDatabases() *bson.Document {
	dbs, err := h.cat.ListDatabases()
	if err != nil {
		return h.errResponse("listDatabases", err.Error())
	}

	var dbDocs bson.Array
	for _, db := range dbs {
		entry := bson.NewDocument()
		entry.Set("name", bson.VString(db.Name))
		entry.Set("sizeOnDisk", bson.VInt64(0))
		entry.Set("empty", bson.VBool(false))
		dbDocs = append(dbDocs, bson.VDoc(entry))
	}

	doc := okDoc()
	doc.Set("databases", bson.VArray(dbDocs))
	doc.Set("totalSize", bson.VInt64(0))
	doc.Set("ok", bson.VDouble(1.0))
	return doc
}

func (h *Handler) handleListCollections(body *bson.Document) *bson.Document {
	db := extractDB(body)
	if db == "" {
		return h.errResponse("listCollections", "database name required")
	}

	colls, err := h.cat.ListCollections(db)
	if err != nil {
		return h.errResponse("listCollections", err.Error())
	}

	var firstBatch bson.Array
	for _, c := range colls {
		entry := bson.NewDocument()
		entry.Set("name", bson.VString(c.Name))
		entry.Set("type", bson.VString("collection"))
		entry.Set("options", bson.VDoc(bson.NewDocument()))
		firstBatch = append(firstBatch, bson.VDoc(entry))
	}

	cursorDoc := bson.NewDocument()
	cursorDoc.Set("firstBatch", bson.VArray(firstBatch))
	cursorDoc.Set("id", bson.VInt64(0))
	cursorDoc.Set("ns", bson.VString(db+".$cmd.listCollections"))

	doc := okDoc()
	doc.Set("cursor", bson.VDoc(cursorDoc))
	return doc
}

func (h *Handler) handleCreate(body *bson.Document) *bson.Document {
	db := extractDB(body)
	collName := getStringFromBody(body, "create")
	if db == "" || collName == "" {
		return h.errResponse("create", "collection name required")
	}
	if err := h.cat.EnsureCollection(db, collName); err != nil {
		return h.errResponse("create", err.Error())
	}
	return okDoc()
}

func (h *Handler) handleDrop(body *bson.Document) *bson.Document {
	db := extractDB(body)
	collName := getStringFromBody(body, "drop")
	if db == "" || collName == "" {
		return h.errResponse("drop", "database and collection name required")
	}
	if err := h.cat.DropCollection(db, collName); err != nil {
		return h.errResponse("drop", err.Error())
	}
	doc := okDoc()
	doc.Set("ns", bson.VString(db+"."+collName))
	doc.Set("nIndexesWas", bson.VInt32(1))
	return doc
}

func (h *Handler) handleCreateIndexes(body *bson.Document) *bson.Document {
	return okDoc()
}

func (h *Handler) handleDropIndexes(body *bson.Document) *bson.Document {
	return okDoc()
}

func (h *Handler) handleListIndexes(body *bson.Document) *bson.Document {
	db := extractDB(body)
	collName := extractCollection(body)

	idIdx := bson.NewDocument()
	idIdxKey := bson.NewDocument()
	idIdxKey.Set("_id", bson.VInt32(1))
	idIdx.Set("v", bson.VInt32(2))
	idIdx.Set("key", bson.VDoc(idIdxKey))
	idIdx.Set("name", bson.VString("_id_"))
	idIdx.Set("ns", bson.VString(db+"."+collName))

	var firstBatch bson.Array
	firstBatch = append(firstBatch, bson.VDoc(idIdx))

	cursorDoc := bson.NewDocument()
	cursorDoc.Set("firstBatch", bson.VArray(firstBatch))
	cursorDoc.Set("id", bson.VInt64(0))
	cursorDoc.Set("ns", bson.VString(db+"."+collName))

	doc := okDoc()
	doc.Set("cursor", bson.VDoc(cursorDoc))
	return doc
}

func (h *Handler) handleServerStatus() *bson.Document {
	doc := okDoc()
	doc.Set("host", bson.VString("mammoth"))
	doc.Set("version", bson.VString("7.0.0"))
	doc.Set("ok", bson.VDouble(1.0))
	return doc
}
