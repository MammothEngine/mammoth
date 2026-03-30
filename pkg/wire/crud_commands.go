package wire

import (
	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

func (h *Handler) handleFind(body *bson.Document) *bson.Document {
	collName := extractCollection(body)
	db := extractDB(body)
	if collName == "" {
		return errResponseWithCode("find", "collection name required", CodeBadValue)
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

	// Try index-driven lookup first
	if spec, prefixKey, ok := h.indexCat.FindBestIndex(db, collName, filter); ok && spec != nil {
		ids := mongo.LookupByPrefix(h.engine, prefixKey)
		for _, id := range ids {
			docKey := mongo.EncodeDocumentKey(db, collName, id)
			val, err := h.engine.Get(docKey)
			if err != nil {
				continue
			}
			doc, err := bson.Decode(val)
			if err != nil {
				continue
			}
			if matcher.Match(doc) {
				if skip > 0 {
					skip--
					continue
				}
				if projection != nil && projection.Len() > 0 {
					doc = mongo.ApplyProjection(doc, projection)
				}
				docs = append(docs, doc)
				if limit > 0 && int32(len(docs)) >= limit {
					break
				}
			}
		}
	} else {
		// Full scan fallback
		h.engine.Scan(prefix, func(key, value []byte) bool {
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
				if limit > 0 && int32(len(docs)) >= limit {
					return false
				}
			}
			return true
		})
	}

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
		return errResponseWithCode("insert", "collection name required", CodeBadValue)
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

	err := coll.InsertMany(docs)
	if err != nil {
		return errResponseWithCode("insert", err.Error(), mongoErrToCode(err))
	}

	doc := okDoc()
	doc.Set("n", bson.VInt32(int32(len(docs))))
	return doc
}

func (h *Handler) handleUpdate(body *bson.Document) *bson.Document {
	collName := extractCollection(body)
	db := extractDB(body)
	if collName == "" {
		return errResponseWithCode("update", "collection name required", CodeBadValue)
	}

	_ = h.cat.EnsureCollection(db, collName)
	coll := mongo.NewCollection(db, collName, h.engine, h.cat)

	updates := getArrayFromBody(body, "updates")
	var matched, modified int32
	var upsertedIDs []bson.Value

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
		upsert := false
		if v, ok := updateDoc.Get("upsert"); ok && v.Type == bson.TypeBoolean {
			upsert = v.Boolean()
		}

		matcher := mongo.NewMatcher(filter)
		prefix := mongo.EncodeNamespacePrefix(db, collName)

		// Collect matching docs first to avoid deadlock (Scan holds read lock, Put needs write lock)
		type matchEntry struct {
			key []byte
			doc *bson.Document
		}
		var matches []matchEntry
		h.engine.Scan(prefix, func(key, value []byte) bool {
			if !multi && len(matches) > 0 {
				return false
			}
			doc, err := bson.Decode(value)
			if err != nil {
				return true
			}
			if !matcher.Match(doc) {
				return true
			}
			matches = append(matches, matchEntry{
				key: append([]byte{}, key...),
				doc: doc,
			})
			return true
		})

		if len(matches) > 0 {
			for _, m := range matches {
				matched++
				newDoc := mongo.ApplyUpdate(m.doc, update)
				// Preserve _id
				if idVal, ok := m.doc.Get("_id"); ok {
					newDoc.Set("_id", idVal)
					if err := coll.ReplaceByKey(m.key, newDoc); err == nil {
						modified++
					}
				}
			}
		} else if upsert {
			// No match — create new document from filter + update
			newDoc := bson.NewDocument()
			// Copy equality fields from filter
			for _, e := range filter.Elements() {
				if e.Key != "_id" && e.Value.Type != bson.TypeDocument {
					newDoc.Set(e.Key, e.Value)
				}
			}
			newDoc = mongo.ApplyUpdate(newDoc, update)
			// Generate _id if not present
			if _, ok := newDoc.Get("_id"); !ok {
				newDoc.Set("_id", bson.VObjectID(bson.NewObjectID()))
			}
			if err := coll.InsertOne(newDoc); err == nil {
				if idVal, ok := newDoc.Get("_id"); ok {
					upsertedIDs = append(upsertedIDs, idVal)
				}
			}
		}
	}

	doc := okDoc()
	doc.Set("n", bson.VInt32(matched))
	doc.Set("nModified", bson.VInt32(modified))
	if len(upsertedIDs) > 0 {
		var upsertedArr bson.Array
		for i, id := range upsertedIDs {
			entry := bson.NewDocument()
			entry.Set("index", bson.VInt32(int32(i)))
			entry.Set("_id", id)
			upsertedArr = append(upsertedArr, bson.VDoc(entry))
		}
		doc.Set("upserted", bson.VArray(upsertedArr))
	}
	doc.Set("ok", bson.VDouble(1.0))
	return doc
}

func (h *Handler) handleDelete(body *bson.Document) *bson.Document {
	collName := extractCollection(body)
	db := extractDB(body)
	if collName == "" {
		return errResponseWithCode("delete", "collection name required", CodeBadValue)
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
