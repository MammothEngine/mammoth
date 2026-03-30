package wire

import (
	"sort"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

func (h *Handler) handleAggregate(body *bson.Document) *bson.Document {
	db := getStringFromBody(body, "$db")
	collName := extractCollection(body)
	if db == "" {
		return errResponseWithCode("aggregate", "database name required", CodeBadValue)
	}

	pipeline := getArrayFromBody(body, "pipeline")
	if pipeline == nil {
		return errResponseWithCode("aggregate", "pipeline required", CodeBadValue)
	}

	_ = h.cat.EnsureCollection(db, collName)

	// Start with all documents in collection
	var docs []*bson.Document
	prefix := mongo.EncodeNamespacePrefix(db, collName)
	h.engine.Scan(prefix, func(_, value []byte) bool {
		doc, err := bson.Decode(value)
		if err != nil {
			return true
		}
		docs = append(docs, doc)
		return true
	})

	// Process pipeline stages
	for _, stage := range pipeline {
		if stage.Type != bson.TypeDocument {
			continue
		}
		stageDoc := stage.DocumentValue()
		keys := stageDoc.Keys()
		if len(keys) == 0 {
			continue
		}
		stageName := keys[0]
		stageVal, _ := stageDoc.Get(stageName)

		switch stageName {
		case "$match":
			docs = stageMatch(docs, stageVal)
		case "$group":
			docs = stageGroup(docs, stageVal)
		case "$sort":
			docs = stageSort(docs, stageVal)
		case "$count":
			docs = stageCount(docs, stageVal)
		case "$limit":
			docs = stageLimit(docs, stageVal)
		case "$skip":
			docs = stageSkip(docs, stageVal)
		case "$project":
			docs = stageProject(docs, stageVal)
		case "$unwind":
			docs = stageUnwind(docs, stageVal)
		case "$lookup":
			docs = stageLookup(docs, stageVal, h.engine, h.cat, db)
		case "$addFields", "$set":
			docs = stageAddFields(docs, stageVal)
		case "$changeStream":
			docs = h.handleChangeStream(db, collName, stageVal)
		}
	}

	// Return via cursor
	cursor := h.cursor.Register(db+"."+collName, docs, 101)
	firstBatch := cursor.GetBatch(101)

	cursorDoc := bson.NewDocument()
	cursorDoc.Set("firstBatch", bson.VArray(docsToValues(firstBatch)))
	cursorDoc.Set("id", bson.VInt64(int64(cursor.ID())))
	cursorDoc.Set("ns", bson.VString(db + "." + collName))

	doc := okDoc()
	doc.Set("cursor", bson.VDoc(cursorDoc))
	return doc
}

func (h *Handler) handleCount(body *bson.Document) *bson.Document {
	db := getStringFromBody(body, "$db")
	collName := extractCollection(body)
	if db == "" || collName == "" {
		return errResponseWithCode("count", "collection name required", CodeBadValue)
	}

	filter := getDocFromBody(body, "filter")
	matcher := mongo.NewMatcher(filter)

	prefix := mongo.EncodeNamespacePrefix(db, collName)
	var n int32
	h.engine.Scan(prefix, func(_, value []byte) bool {
		doc, err := bson.Decode(value)
		if err != nil {
			return true
		}
		if matcher.Match(doc) {
			n++
		}
		return true
	})

	doc := okDoc()
	doc.Set("n", bson.VInt32(n))
	return doc
}

// --- Pipeline stages ---

func stageMatch(docs []*bson.Document, stageVal bson.Value) []*bson.Document {
	if stageVal.Type != bson.TypeDocument {
		return docs
	}
	matcher := mongo.NewMatcher(stageVal.DocumentValue())
	var result []*bson.Document
	for _, doc := range docs {
		if matcher.Match(doc) {
			result = append(result, doc)
		}
	}
	return result
}

func stageGroup(docs []*bson.Document, stageVal bson.Value) []*bson.Document {
	if stageVal.Type != bson.TypeDocument {
		return docs
	}
	spec := stageVal.DocumentValue()

	// Get _id expression
	idExpr, hasID := spec.Get("_id")
	if !hasID {
		return docs
	}

	// Group by _id value
	type group struct {
		id   bson.Value
		docs []*bson.Document
	}
	var groups []group

	for _, doc := range docs {
		var groupKey bson.Value
		if idExpr.Type == bson.TypeDocument {
			// Field reference like "$field"
			idDoc := idExpr.DocumentValue()
			if idDoc.Len() > 0 {
				firstKey := idDoc.Keys()[0]
				firstVal, _ := idDoc.Get(firstKey)
				if firstVal.Type == bson.TypeString && len(firstVal.String()) > 0 && firstVal.String()[0] == '$' {
					fieldName := firstVal.String()[1:]
					if v, ok := doc.Get(fieldName); ok {
						groupKey = v
					} else {
						groupKey = bson.VNull()
					}
				}
			}
		} else {
			groupKey = idExpr
		}

		// Find existing group
		found := false
		for i := range groups {
			if bson.CompareValues(groups[i].id, groupKey) == 0 {
				groups[i].docs = append(groups[i].docs, doc)
				found = true
				break
			}
		}
		if !found {
			groups = append(groups, group{id: groupKey, docs: []*bson.Document{doc}})
		}
	}

	var result []*bson.Document
	for _, g := range groups {
		outDoc := bson.NewDocument()
		outDoc.Set("_id", g.id)

		// Process accumulators
		for _, e := range spec.Elements() {
			if e.Key == "_id" {
				continue
			}
			if e.Value.Type != bson.TypeDocument {
				continue
			}
			accDoc := e.Value.DocumentValue()
			accKeys := accDoc.Keys()
			if len(accKeys) == 0 {
				continue
			}
			accOp := accKeys[0]
			accVal, _ := accDoc.Get(accOp)

			switch accOp {
			case "$sum":
				outDoc.Set(e.Key, accumulateSum(g.docs, accVal))
			case "$count":
				outDoc.Set(e.Key, bson.VInt32(int32(len(g.docs))))
			case "$avg":
				outDoc.Set(e.Key, accumulateAvg(g.docs, accVal))
			case "$min":
				outDoc.Set(e.Key, accumulateMin(g.docs, accVal))
			case "$max":
				outDoc.Set(e.Key, accumulateMax(g.docs, accVal))
			case "$first":
				outDoc.Set(e.Key, accumulateFirst(g.docs, accVal))
			case "$last":
				outDoc.Set(e.Key, accumulateLast(g.docs, accVal))
			case "$push":
				outDoc.Set(e.Key, accumulatePush(g.docs, accVal))
			case "$addToSet":
				outDoc.Set(e.Key, accumulateAddToSet(g.docs, accVal))
			}
		}

		result = append(result, outDoc)
	}
	return result
}

func accumulateSum(docs []*bson.Document, accVal bson.Value) bson.Value {
	if accVal.Type == bson.TypeInt32 && accVal.Int32() == 1 {
		// Count
		return bson.VInt32(int32(len(docs)))
	}
	if accVal.Type == bson.TypeString && len(accVal.String()) > 0 && accVal.String()[0] == '$' {
		fieldName := accVal.String()[1:]
		var total float64
		for _, doc := range docs {
			if v, ok := doc.Get(fieldName); ok {
				switch v.Type {
				case bson.TypeInt32:
					total += float64(v.Int32())
				case bson.TypeInt64:
					total += float64(v.Int64())
				case bson.TypeDouble:
					total += v.Double()
				}
			}
		}
		return bson.VDouble(total)
	}
	return bson.VInt32(int32(len(docs)))
}

func stageSort(docs []*bson.Document, stageVal bson.Value) []*bson.Document {
	if stageVal.Type != bson.TypeDocument {
		return docs
	}
	sortSpec := stageVal.DocumentValue()
	sort.Slice(docs, func(i, j int) bool {
		return compareDocs(docs[i], docs[j], sortSpec) < 0
	})
	return docs
}

func compareDocs(a, b *bson.Document, sortSpec *bson.Document) int {
	for _, e := range sortSpec.Elements() {
		field := e.Key
		ascending := true
		if e.Value.Type == bson.TypeInt32 && e.Value.Int32() == -1 {
			ascending = false
		}

		aVal, aOk := a.Get(field)
		bVal, bOk := b.Get(field)

		if !aOk && !bOk {
			continue
		}
		if !aOk {
			if ascending {
				return 1
			}
			return -1
		}
		if !bOk {
			if ascending {
				return -1
			}
			return 1
		}

		cmp := bson.CompareValues(aVal, bVal)
		if cmp == 0 {
			continue
		}
		if !ascending {
			cmp = -cmp
		}
		return cmp
	}
	return 0
}

func stageCount(docs []*bson.Document, stageVal bson.Value) []*bson.Document {
	if stageVal.Type != bson.TypeString {
		return docs
	}
	fieldName := stageVal.String()
	result := bson.NewDocument()
	result.Set("_id", bson.VNull())
	result.Set(fieldName, bson.VInt32(int32(len(docs))))
	return []*bson.Document{result}
}

func stageLimit(docs []*bson.Document, stageVal bson.Value) []*bson.Document {
	var limit int
	if stageVal.Type == bson.TypeInt32 {
		limit = int(stageVal.Int32())
	} else if stageVal.Type == bson.TypeInt64 {
		limit = int(stageVal.Int64())
	}
	if limit <= 0 {
		return docs
	}
	if limit > len(docs) {
		limit = len(docs)
	}
	return docs[:limit]
}

func stageSkip(docs []*bson.Document, stageVal bson.Value) []*bson.Document {
	var skip int
	if stageVal.Type == bson.TypeInt32 {
		skip = int(stageVal.Int32())
	} else if stageVal.Type == bson.TypeInt64 {
		skip = int(stageVal.Int64())
	}
	if skip <= 0 {
		return docs
	}
	if skip >= len(docs) {
		return nil
	}
	return docs[skip:]
}

