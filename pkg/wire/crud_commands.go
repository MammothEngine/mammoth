package wire

import (
	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

func (h *Handler) handleFind(body *bson.Document) *bson.Document {
	collName := extractCollection(body)
	db := extractDB(body)
	if collName == "" {
		return h.errResponse("find", "collection name required")
	}

	_ = h.cat.EnsureCollection(db, collName)

	filter := getDocFromBody(body, "filter")
	if filter == nil {
		filter = bson.NewDocument()
	}

	projection := getDocFromBody(body, "projection")
	limit := getInt32FromBody(body, "limit")
	skip := getInt32FromBody(body, "skip")
	batchSize := getInt32FromBody(body, "batchSize")
	if batchSize <= 0 {
		batchSize = 101
	}

	matcher := mongo.NewMatcher(filter)
	var docs []*bson.Document
	prefix := mongo.EncodeNamespacePrefix(db, collName)

	h.engine.Scan(prefix, func(key, value []byte) bool {
		if limit > 0 && int32(len(docs)) >= limit {
			return false
		}
		doc, err := bson.Decode(value)
		if err != nil {
			return true
		}
		if matcher.Match(doc) {
			if skip > 0 {
				skip--
				return true
			}
			if projection != nil && projection.Len() > 0 {
				doc = mongo.ApplyProjection(doc, projection)
			}
			docs = append(docs, doc)
		}
		return true
	})

	cursor := h.cursor.Register(db+"."+collName, docs, int(batchSize))
	firstBatch := cursor.GetBatch(int(batchSize))

	cursorDoc := bson.NewDocument()
	cursorDoc.Set("firstBatch", bson.VArray(docsToValues(firstBatch)))
	cursorDoc.Set("id", bson.VInt64(int64(cursor.ID())))
	cursorDoc.Set("ns", bson.VString(db + "." + collName))

	doc := okDoc()
	doc.Set("cursor", bson.VDoc(cursorDoc))
	return doc
}

func (h *Handler) handleInsert(body *bson.Document) *bson.Document {
	collName := extractCollection(body)
	db := extractDB(body)
	if collName == "" {
		return h.errResponse("insert", "collection name required")
	}

	_ = h.cat.EnsureCollection(db, collName)
	coll := mongo.NewCollection(db, collName, h.engine, h.cat)

	var docs []*bson.Document
	if arr := getArrayFromBody(body, "documents"); arr != nil {
		for _, v := range arr {
			if v.Type == bson.TypeDocument {
				docs = append(docs, v.DocumentValue())
			}
		}
	}

	ids, err := coll.InsertMany(docs)
	if err != nil {
		return h.errResponse("insert", err.Error())
	}

	doc := okDoc()
	doc.Set("n", bson.VInt32(int32(len(ids))))
	return doc
}

func (h *Handler) handleUpdate(body *bson.Document) *bson.Document {
	collName := extractCollection(body)
	db := extractDB(body)
	if collName == "" {
		return h.errResponse("update", "collection name required")
	}

	_ = h.cat.EnsureCollection(db, collName)
	coll := mongo.NewCollection(db, collName, h.engine, h.cat)

	updates := getArrayFromBody(body, "updates")
	var matched, modified int32

	for _, u := range updates {
		if u.Type != bson.TypeDocument {
			continue
		}
		updateDoc := u.DocumentValue()

		filter := getDocFromBody(updateDoc, "q")
		update := getDocFromBody(updateDoc, "u")
		if filter == nil || update == nil {
			continue
		}

		multi := false
		if v, ok := updateDoc.Get("multi"); ok && v.Type == bson.TypeBoolean {
			multi = v.Boolean()
		}

		matcher := mongo.NewMatcher(filter)
		prefix := mongo.EncodeNamespacePrefix(db, collName)

		h.engine.Scan(prefix, func(key, value []byte) bool {
			if !multi && matched > 0 {
				return false
			}
			doc, err := bson.Decode(value)
			if err != nil {
				return true
			}
			if !matcher.Match(doc) {
				return true
			}
			matched++
			newDoc := mongo.ApplyUpdate(doc, update)
			// Preserve _id
			if idVal, ok := doc.Get("_id"); ok {
				newDoc.Set("_id", idVal)
				if err := coll.ReplaceOne(idVal.ObjectID(), newDoc); err == nil {
					modified++
				}
			}
			return true
		})
	}

	doc := okDoc()
	doc.Set("n", bson.VInt32(matched))
	doc.Set("nModified", bson.VInt32(modified))
	doc.Set("ok", bson.VDouble(1.0))
	return doc
}

func (h *Handler) handleDelete(body *bson.Document) *bson.Document {
	collName := extractCollection(body)
	db := extractDB(body)
	if collName == "" {
		return h.errResponse("delete", "collection name required")
	}

	_ = h.cat.EnsureCollection(db, collName)

	deletes := getArrayFromBody(body, "deletes")
	var deleted int32

	for _, d := range deletes {
		if d.Type != bson.TypeDocument {
			continue
		}
		delDoc := d.DocumentValue()
		filter := getDocFromBody(delDoc, "q")
		if filter == nil {
			continue
		}

		matcher := mongo.NewMatcher(filter)
		prefix := mongo.EncodeNamespacePrefix(db, collName)

		var keys [][]byte
		h.engine.Scan(prefix, func(key, value []byte) bool {
			doc, err := bson.Decode(value)
			if err != nil {
				return true
			}
			if matcher.Match(doc) {
				keys = append(keys, append([]byte{}, key...))
			}
			return true
		})

		for _, k := range keys {
			if err := h.engine.Delete(k); err == nil {
				deleted++
			}
		}
	}

	doc := okDoc()
	doc.Set("n", bson.VInt32(deleted))
	return doc
}

// --- Helpers ---

func extractCollection(body *bson.Document) string {
	keys := body.Keys()
	if len(keys) < 1 {
		return ""
	}
	if v, ok := body.Get(keys[0]); ok && v.Type == bson.TypeString {
		return v.String()
	}
	return ""
}

func extractDB(body *bson.Document) string {
	return getStringFromBody(body, "$db")
}

func docsToValues(docs []*bson.Document) bson.Array {
	arr := make(bson.Array, len(docs))
	for i, d := range docs {
		arr[i] = bson.VDoc(d)
	}
	return arr
}
