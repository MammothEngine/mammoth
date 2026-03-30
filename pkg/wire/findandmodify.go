package wire

import (
	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

func (h *Handler) handleFindAndModify(body *bson.Document) *bson.Document {
	collName := extractCollection(body)
	db := extractDB(body)
	if collName == "" {
		return errResponseWithCode("findAndModify", "collection name required", CodeBadValue)
	}

	_ = h.cat.EnsureCollection(db, collName)
	coll := mongo.NewCollection(db, collName, h.engine, h.cat)

	query := getDocFromBody(body, "query")
	if query == nil {
		query = bson.NewDocument()
	}
	sortDoc := getDocFromBody(body, "sort")
	update := getDocFromBody(body, "update")
	fields := getDocFromBody(body, "fields")

	remove := false
	if v, ok := body.Get("remove"); ok && v.Type == bson.TypeBoolean {
		remove = v.Boolean()
	}
	upsert := false
	if v, ok := body.Get("upsert"); ok && v.Type == bson.TypeBoolean {
		upsert = v.Boolean()
	}
	returnNew := false
	if v, ok := body.Get("new"); ok && v.Type == bson.TypeBoolean {
		returnNew = v.Boolean()
	}

	if remove && update != nil {
		return errResponseWithCode("findAndModify", "cannot specify both remove and update", CodeBadValue)
	}
	if !remove && update == nil {
		return errResponseWithCode("findAndModify", "must specify update or remove", CodeBadValue)
	}

	matcher := mongo.NewMatcher(query)
	prefix := mongo.EncodeNamespacePrefix(db, collName)

	// Collect matching docs
	type matchEntry struct {
		key []byte
		doc *bson.Document
	}
	var matches []matchEntry
	h.engine.Scan(prefix, func(key, value []byte) bool {
		doc, err := bson.Decode(value)
		if err != nil {
			return true
		}
		if matcher.Match(doc) {
			matches = append(matches, matchEntry{
				key: append([]byte{}, key...),
				doc: doc,
			})
		}
		return true
	})

	// Apply sort if specified
	if sortDoc != nil && sortDoc.Len() > 0 && len(matches) > 1 {
		for i := 1; i < len(matches); i++ {
			for j := i; j > 0; j-- {
				if compareDocs(matches[j-1].doc, matches[j].doc, sortDoc) > 0 {
					matches[j-1], matches[j] = matches[j], matches[j-1]
				}
			}
		}
	}

	result := okDoc()

	if len(matches) > 0 {
		entry := matches[0]

		if remove {
			// Delete the document
			if err := coll.DeleteByKey(entry.key); err == nil {
				h.indexCat.OnDocumentDelete(db, collName, entry.doc)
				h.oplogWriteDelete(db, collName, entry.doc)
			}
			returnedDoc := entry.doc
			if fields != nil && fields.Len() > 0 {
				returnedDoc = mongo.ApplyProjection(returnedDoc, fields)
			}
			result.Set("value", bson.VDoc(returnedDoc))
			result.Set("lastErrorObject", bson.VDoc(bson.NewDocument()))
			return result
		}

		// Update
		oldDoc := entry.doc
		newDoc := mongo.ApplyUpdate(oldDoc, update, false)
		if idVal, ok := oldDoc.Get("_id"); ok {
			newDoc.Set("_id", idVal)
			if err := coll.ReplaceByKey(entry.key, newDoc); err == nil {
				h.indexCat.OnDocumentUpdate(db, collName, oldDoc, newDoc)
				h.oplogWriteUpdate(db, collName, newDoc)
			}
		}

		returnedDoc := oldDoc
		if returnNew {
			returnedDoc = newDoc
		}
		if fields != nil && fields.Len() > 0 {
			returnedDoc = mongo.ApplyProjection(returnedDoc, fields)
		}
		result.Set("value", bson.VDoc(returnedDoc))

	} else if upsert && !remove {
		// No match — create new document
		newDoc := bson.NewDocument()
		for _, e := range query.Elements() {
			if e.Key != "_id" && e.Value.Type != bson.TypeDocument {
				newDoc.Set(e.Key, e.Value)
			}
		}
		newDoc = mongo.ApplyUpdate(newDoc, update, true)
		if _, ok := newDoc.Get("_id"); !ok {
			newDoc.Set("_id", bson.VObjectID(bson.NewObjectID()))
		}
		if err := coll.InsertOne(newDoc); err == nil {
			h.indexCat.OnDocumentInsert(db, collName, newDoc)
			h.oplogWriteInsert(db, collName, newDoc)
		}

		returnedDoc := newDoc
		if !returnNew {
			// upserted but new:false returns null (no old doc)
			returnedDoc = nil
		}
		if returnedDoc != nil && fields != nil && fields.Len() > 0 {
			returnedDoc = mongo.ApplyProjection(returnedDoc, fields)
		}
		if returnedDoc != nil {
			result.Set("value", bson.VDoc(returnedDoc))
		} else {
			result.Set("value", bson.VNull())
		}

		upsertedDoc := bson.NewDocument()
		upsertedDoc.Set("updatedExisting", bson.VBool(false))
		if idVal, ok := newDoc.Get("_id"); ok {
			upsertedDoc.Set("upserted", idVal)
		}
		result.Set("lastErrorObject", bson.VDoc(upsertedDoc))
		return result
	} else {
		result.Set("value", bson.VNull())
	}

	result.Set("lastErrorObject", bson.VDoc(bson.NewDocument()))
	return result
}
