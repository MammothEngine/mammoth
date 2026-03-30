package wire

import (
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

func (h *Handler) handleExplain(body *bson.Document) *bson.Document {
	cmd := getDocFromBody(body, "explain")
	if cmd == nil {
		return errResponseWithCode("explain", "explain requires a command document", CodeBadValue)
	}

	// Extract the actual command type
	keys := cmd.Keys()
	if len(keys) == 0 {
		return errResponseWithCode("explain", "empty command", CodeBadValue)
	}
	cmdType := keys[0]

	db := extractDB(body)
	if db == "" {
		db = getStringFromBody(cmd, "$db")
	}
	collName := extractCollection(cmd)

	if collName == "" {
		collName = getStringFromBody(cmd, cmdType)
	}

	filter := getDocFromBody(cmd, "filter")
	if filter == nil {
		filter = bson.NewDocument()
	}
	sortDoc := getDocFromBody(cmd, "sort")

	start := time.Now()

	// Build query planner info
	parsedQuery := buildParsedQuery(filter)

	var winningPlan *bson.Document
	var rejectedPlans bson.Array
	var totalKeysExamined int32
	var totalDocsExamined int32
	var nReturned int32

	// Try to find best index
	if spec, prefixKey, ok := h.indexCat.FindBestIndex(db, collName, filter); ok && spec != nil {
		// Index scan plan
		ids := mongo.LookupByPrefix(h.engine, prefixKey)
		totalKeysExamined = int32(len(ids))

		matcher := mongo.NewMatcher(filter)
		for _, id := range ids {
			docKey := mongo.EncodeDocumentKey(db, collName, id)
			val, err := h.engine.Get(docKey)
			if err != nil {
				continue
			}
			totalDocsExamined++
			doc, err := bson.Decode(val)
			if err != nil {
				continue
			}
			if matcher.Match(doc) {
				nReturned++
			}
		}

		winningPlan = bson.NewDocument()
		winningPlan.Set("stage", bson.VString("IXSCAN"))
		winningPlan.Set("indexName", bson.VString(spec.Name))
		winningPlan.Set("direction", bson.VString("forward"))

		// Check if sort can be served by index
		if sortDoc != nil && sortDoc.Len() > 0 {
			if isSortCoveredByIndex(spec, sortDoc) {
				winningPlan.Set("sortPattern", bson.VDoc(sortDoc))
			}
		}
	} else {
		// Collection scan plan
		prefix := mongo.EncodeNamespacePrefix(db, collName)
		matcher := mongo.NewMatcher(filter)
		h.engine.Scan(prefix, func(_, value []byte) bool {
			totalDocsExamined++
			doc, err := bson.Decode(value)
			if err != nil {
				return true
			}
			if matcher.Match(doc) {
				nReturned++
			}
			return true
		})

		winningPlan = bson.NewDocument()
		winningPlan.Set("stage", bson.VString("COLLSCAN"))
		winningPlan.Set("direction", bson.VString("forward"))
	}

	elapsed := time.Since(start)

	// Build response
	plannerDoc := bson.NewDocument()
	plannerDoc.Set("plannerVersion", bson.VInt32(1))
	plannerDoc.Set("namespace", bson.VString(db + "." + collName))
	plannerDoc.Set("indexFilterSet", bson.VBool(false))
	plannerDoc.Set("parsedQuery", bson.VDoc(parsedQuery))
	plannerDoc.Set("winningPlan", bson.VDoc(winningPlan))
	plannerDoc.Set("rejectedPlans", bson.VArray(rejectedPlans))

	statsDoc := bson.NewDocument()
	statsDoc.Set("executionSuccess", bson.VBool(true))
	statsDoc.Set("nReturned", bson.VInt32(nReturned))
	statsDoc.Set("executionTimeMillis", bson.VInt64(elapsed.Milliseconds()))
	statsDoc.Set("totalKeysExamined", bson.VInt32(totalKeysExamined))
	statsDoc.Set("totalDocsExamined", bson.VInt32(totalDocsExamined))

	result := okDoc()
	result.Set("queryPlanner", bson.VDoc(plannerDoc))
	result.Set("executionStats", bson.VDoc(statsDoc))
	result.Set("serverInfo", bson.VDoc(bson.NewDocument()))
	return result
}

// buildParsedQuery constructs a parsed query representation from a filter.
func buildParsedQuery(filter *bson.Document) *bson.Document {
	if filter == nil || filter.Len() == 0 {
		return bson.NewDocument()
	}

	result := bson.NewDocument()
	for _, e := range filter.Elements() {
		if e.Value.Type == bson.TypeDocument {
			// Already an operator expression
			result.Set(e.Key, e.Value)
		} else {
			// Implicit $eq
			eqDoc := bson.NewDocument()
			eqDoc.Set("$eq", e.Value)
			result.Set(e.Key, bson.VDoc(eqDoc))
		}
	}
	return result
}

// isSortCoveredByIndex checks if the sort can be served by the index without extra sorting.
func isSortCoveredByIndex(spec *mongo.IndexSpec, sortDoc *bson.Document) bool {
	if len(spec.Key) < sortDoc.Len() {
		return false
	}
	sortKeys := sortDoc.Keys()
	for i, sk := range sortKeys {
		if i >= len(spec.Key) {
			return false
		}
		sortVal, _ := sortDoc.Get(sk)
		descending := sortVal.Type == bson.TypeInt32 && sortVal.Int32() == -1
		if spec.Key[i].Field != sk || spec.Key[i].Descending != descending {
			return false
		}
	}
	return true
}
