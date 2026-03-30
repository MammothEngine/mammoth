package wire

import (
	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

func (h *Handler) handleListDatabases() *bson.Document {
	dbs, err := h.cat.ListDatabases()
	if err != nil {
		return errResponseWithCode("listDatabases", err.Error(), CodeInternalError)
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
		return errResponseWithCode("listCollections", "database name required", CodeBadValue)
	}

	colls, err := h.cat.ListCollections(db)
	if err != nil {
		return errResponseWithCode("listCollections", err.Error(), CodeInternalError)
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
		return errResponseWithCode("create", "collection name required", CodeBadValue)
	}
	if err := h.cat.EnsureCollection(db, collName); err != nil {
		return errResponseWithCode("create", err.Error(), mongoErrToCode(err))
	}
	return okDoc()
}

func (h *Handler) handleDrop(body *bson.Document) *bson.Document {
	db := extractDB(body)
	collName := getStringFromBody(body, "drop")
	if db == "" || collName == "" {
		return errResponseWithCode("drop", "database and collection name required", CodeBadValue)
	}
	if err := h.cat.DropCollection(db, collName); err != nil {
		return errResponseWithCode("drop", err.Error(), mongoErrToCode(err))
	}
	doc := okDoc()
	doc.Set("ns", bson.VString(db+"."+collName))
	doc.Set("nIndexesWas", bson.VInt32(1))
	return doc
}

func (h *Handler) handleCreateIndexes(body *bson.Document) *bson.Document {
	db := extractDB(body)
	collName := extractCollection(body)

	indexes := getArrayFromBody(body, "indexes")
	var created int32
	for _, v := range indexes {
		if v.Type != bson.TypeDocument {
			continue
		}
		idxDoc := v.DocumentValue()

		// Extract index name
		name := ""
		if n, ok := idxDoc.Get("name"); ok && n.Type == bson.TypeString {
			name = n.String()
		}
		if name == "" {
			continue
		}

		// Extract key spec
		keyDoc := getDocFromBody(idxDoc, "key")
		if keyDoc == nil {
			continue
		}

		var keys []mongo.IndexKey
		for _, e := range keyDoc.Elements() {
			descending := false
			if e.Value.Type == bson.TypeInt32 && e.Value.Int32() == -1 {
				descending = true
			}
			keys = append(keys, mongo.IndexKey{Field: e.Key, Descending: descending})
		}

		// Extract unique flag
		unique := false
		if u, ok := idxDoc.Get("unique"); ok && u.Type == bson.TypeBoolean {
			unique = u.Boolean()
		}

		spec := mongo.IndexSpec{
			Name:   name,
			Key:    keys,
			Unique: unique,
		}

		if err := h.indexCat.CreateIndex(db, collName, spec); err != nil {
			return errResponseWithCode("createIndexes", err.Error(), mongoErrToCode(err))
		}
		created++
	}

	doc := okDoc()
	doc.Set("numIndexesBefore", bson.VInt32(0))
	doc.Set("numIndexesAfter", bson.VInt32(created))
	return doc
}

func (h *Handler) handleDropIndexes(body *bson.Document) *bson.Document {
	db := extractDB(body)
	collName := extractCollection(body)
	indexName := getStringFromBody(body, "index")

	if indexName == "" {
		return errResponseWithCode("dropIndexes", "index name required", CodeBadValue)
	}

	if err := h.indexCat.DropIndex(db, collName, indexName); err != nil {
		return errResponseWithCode("dropIndexes", err.Error(), mongoErrToCode(err))
	}

	return okDoc()
}

func (h *Handler) handleListIndexes(body *bson.Document) *bson.Document {
	db := extractDB(body)
	collName := extractCollection(body)

	var firstBatch bson.Array

	// Always include _id index
	idIdx := bson.NewDocument()
	idIdxKey := bson.NewDocument()
	idIdxKey.Set("_id", bson.VInt32(1))
	idIdx.Set("v", bson.VInt32(2))
	idIdx.Set("key", bson.VDoc(idIdxKey))
	idIdx.Set("name", bson.VString("_id_"))
	idIdx.Set("ns", bson.VString(db+"."+collName))
	firstBatch = append(firstBatch, bson.VDoc(idIdx))

	// Get real indexes from catalog
	if h.indexCat != nil {
		indexes, err := h.indexCat.ListIndexes(db, collName)
		if err == nil {
			for _, spec := range indexes {
				idxDoc := bson.NewDocument()
				idxDoc.Set("v", bson.VInt32(2))
				keyDoc := bson.NewDocument()
				for _, k := range spec.Key {
					if k.Descending {
						keyDoc.Set(k.Field, bson.VInt32(-1))
					} else {
						keyDoc.Set(k.Field, bson.VInt32(1))
					}
				}
				idxDoc.Set("key", bson.VDoc(keyDoc))
				idxDoc.Set("name", bson.VString(spec.Name))
				idxDoc.Set("ns", bson.VString(db + "." + collName))
				if spec.Unique {
					idxDoc.Set("unique", bson.VBool(true))
				}
				firstBatch = append(firstBatch, bson.VDoc(idxDoc))
			}
		}
	}

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

func (h *Handler) handleDropDatabase(body *bson.Document) *bson.Document {
	db := extractDB(body)
	if db == "" {
		return errResponseWithCode("dropDatabase", "database name required", CodeBadValue)
	}
	if err := h.cat.DropDatabase(db); err != nil {
		return errResponseWithCode("dropDatabase", err.Error(), mongoErrToCode(err))
	}
	doc := okDoc()
	doc.Set("dropped", bson.VString(db))
	return doc
}
