package wire

import (
	"fmt"

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

func (h *Handler) handleInsert(msg *Message, body *bson.Document) *bson.Document {
	collName := extractCollection(body)
	db := extractDB(body)
	if db == "" {
		return errResponseWithCode("insert", "database name required", CodeBadValue)
	}
	if collName == "" {
		return errResponseWithCode("insert", "collection name required", CodeBadValue)
	}

	_ = h.cat.EnsureCollection(db, collName)
	coll := mongo.NewCollection(db, collName, h.engine, h.cat)

	var docs []*bson.Document

	// First try to get documents from the body (legacy format)
	if arr := getArrayFromBody(body, "documents"); arr != nil {
		for _, v := range arr {
			if v.Type == bson.TypeDocument {
				docs = append(docs, v.DocumentValue())
			}
		}
	}

	// Also check for documents in kind-1 sections (OP_MSG document sequence)
	if len(docs) == 0 {
		if rawDocs := msg.GetDocumentSequence("documents"); rawDocs != nil {
			for _, raw := range rawDocs {
				if doc, err := bson.Decode(raw); err == nil {
					docs = append(docs, doc)
				}
			}
		}
	}


	// Validate documents against collection schema
	if validator, err := h.cat.GetValidator(db, collName); err == nil && validator != nil {
		for _, doc := range docs {
			if verr := validator.ValidateDocument(doc); verr != nil {
				if validator.Action == mongo.ValidationError {
					return errResponseWithCode("insert", "Document failed validation: "+verr.Error(), CodeBadValue)
				}
			}
		}
	}

	err := coll.InsertMany(docs)
	if err != nil {
		return errResponseWithCode("insert", err.Error(), mongoErrToCode(err))
	}

	// Index maintenance: update indexes for each inserted document
	for _, d := range docs {
		h.indexCat.OnDocumentInsert(db, collName, d)
		h.oplogWriteInsert(db, collName, d)
	}

	// Capped collection: enforce limits after insert
	if capped, maxSize, maxDocs := mongo.GetCappedInfo(h.cat, db, collName); capped {
		cc := mongo.NewCappedCollection(db, collName, h.engine, h.cat, maxSize, maxDocs)
		cc.EnforceLimits()
	}

	doc := okDoc()
	doc.Set("n", bson.VInt32(int32(len(docs))))
	return doc
}

func (h *Handler) handleUpdate(msg *Message, body *bson.Document) *bson.Document {
	collName := extractCollection(body)
	db := extractDB(body)
	if collName == "" {
		return errResponseWithCode("update", "collection name required", CodeBadValue)
	}

	_ = h.cat.EnsureCollection(db, collName)
	coll := mongo.NewCollection(db, collName, h.engine, h.cat)

	// Get updates from body or document sequence
	updates := getArrayFromBody(body, "updates")
	if len(updates) == 0 {
		if rawDocs := msg.GetDocumentSequence("updates"); rawDocs != nil {
			for _, raw := range rawDocs {
				if doc, err := bson.Decode(raw); err == nil {
					updates = append(updates, bson.VDoc(doc))
				}
			}
		}
	}
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
				oldDoc := m.doc
				newDoc := mongo.ApplyUpdate(oldDoc, update, false)
				// Preserve _id
				if idVal, ok := oldDoc.Get("_id"); ok {
					newDoc.Set("_id", idVal)

					// Validate updated document
					if validator, verr := h.cat.GetValidator(db, collName); verr == nil && validator != nil {
						if vErr := validator.ValidateDocument(newDoc); vErr != nil {
							if validator.Action == mongo.ValidationError {
								continue // skip invalid document
							}
						}
					}

					if err := coll.ReplaceByKey(m.key, newDoc); err == nil {
						modified++
						// Index maintenance: remove old entries, add new ones
						h.indexCat.OnDocumentUpdate(db, collName, oldDoc, newDoc)
						h.oplogWriteUpdate(db, collName, newDoc)
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
			newDoc = mongo.ApplyUpdate(newDoc, update, true)
			// Generate _id if not present
			if _, ok := newDoc.Get("_id"); !ok {
				newDoc.Set("_id", bson.VObjectID(bson.NewObjectID()))
			}
			if err := coll.InsertOne(newDoc); err == nil {
				if idVal, ok := newDoc.Get("_id"); ok {
					upsertedIDs = append(upsertedIDs, idVal)
				}
				// Index maintenance: add entries for upserted document
				h.indexCat.OnDocumentInsert(db, collName, newDoc)
				h.oplogWriteInsert(db, collName, newDoc)
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

func (h *Handler) handleDelete(msg *Message, body *bson.Document) *bson.Document {
	collName := extractCollection(body)
	db := extractDB(body)
	if collName == "" {
		return errResponseWithCode("delete", "collection name required", CodeBadValue)
	}

	// Capped collections don't allow explicit deletes
	if mongo.IsCapped(h.cat, db, collName) {
		return errResponseWithCode("delete", "cannot delete from capped collection", CodeBadValue)
	}

	_ = h.cat.EnsureCollection(db, collName)

	// Get deletes from body or document sequence
	deletes := getArrayFromBody(body, "deletes")
	if len(deletes) == 0 {
		if rawDocs := msg.GetDocumentSequence("deletes"); rawDocs != nil {
			for _, raw := range rawDocs {
				if doc, err := bson.Decode(raw); err == nil {
					deletes = append(deletes, bson.VDoc(doc))
				}
			}
		}
	}
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

		// Check limit: 0 = delete all matching, 1 = delete first matching
		limit := getInt32FromBody(delDoc, "limit")

		matcher := mongo.NewMatcher(filter)
		prefix := mongo.EncodeNamespacePrefix(db, collName)

		var keys [][]byte
		var deletedDocs []*bson.Document
		h.engine.Scan(prefix, func(key, value []byte) bool {
			doc, err := bson.Decode(value)
			if err != nil {
				return true
			}
			if matcher.Match(doc) {
				keys = append(keys, append([]byte{}, key...))
				deletedDocs = append(deletedDocs, doc)
				// If limit is 1, stop after first match
				if limit == 1 {
					return false
				}
			}
			return true
		})

		for i, k := range keys {
			if err := h.engine.Delete(k); err == nil {
				deleted++
				// Index maintenance: remove index entries for deleted document
				h.indexCat.OnDocumentDelete(db, collName, deletedDocs[i])
				h.oplogWriteDelete(db, collName, deletedDocs[i])
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

func (h *Handler) handleDistinct(body *bson.Document) *bson.Document {
	collName := extractCollection(body)
	db := extractDB(body)
	if collName == "" {
		return errResponseWithCode("distinct", "collection name required", CodeBadValue)
	}

	key := getStringFromBody(body, "key")
	if key == "" {
		return errResponseWithCode("distinct", "key required", CodeBadValue)
	}

	_ = h.cat.EnsureCollection(db, collName)

	filter := getDocFromBody(body, "query")
	if filter == nil {
		filter = bson.NewDocument()
	}
	matcher := mongo.NewMatcher(filter)

	prefix := mongo.EncodeNamespacePrefix(db, collName)
	seen := make(map[string]bool)
	var values bson.Array

	h.engine.Scan(prefix, func(_, value []byte) bool {
		doc, err := bson.Decode(value)
		if err != nil {
			return true
		}
		if !matcher.Match(doc) {
			return true
		}

		v, found := mongo.ResolveField(doc, key)
		if !found {
			return true
		}

		// Flatten arrays
		if v.Type == bson.TypeArray {
			for _, elem := range v.ArrayValue() {
				key := distinctKey(elem)
				if !seen[key] {
					seen[key] = true
					values = append(values, elem)
				}
			}
		} else {
			key := distinctKey(v)
			if !seen[key] {
				seen[key] = true
				values = append(values, v)
			}
		}
		return true
	})

	doc := okDoc()
	doc.Set("values", bson.VArray(values))
	return doc
}

func distinctKey(v bson.Value) string {
	switch v.Type {
	case bson.TypeString:
		return "s:" + v.String()
	case bson.TypeInt32:
		return fmt.Sprintf("i:%d", v.Int32())
	case bson.TypeInt64:
		return fmt.Sprintf("l:%d", v.Int64())
	case bson.TypeDouble:
		return fmt.Sprintf("d:%v", v.Double())
	case bson.TypeBoolean:
		return fmt.Sprintf("b:%v", v.Boolean())
	case bson.TypeNull:
		return "n:"
	case bson.TypeObjectID:
		return "o:" + string(v.ObjectID().Bytes())
	default:
		return fmt.Sprintf("%d:%v", v.Type, v.Interface())
	}
}
