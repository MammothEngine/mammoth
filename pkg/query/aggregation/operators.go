package aggregation

import (
	"context"
	"fmt"
	"sort"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

// ==================== $match stage ====================

// matchStage filters documents.
type matchStage struct {
	filter *bson.Document
}

func newMatchStage(value interface{}) (*matchStage, error) {
	filter, err := toDocument(value)
	if err != nil {
		return nil, fmt.Errorf("$match requires a document: %w", err)
	}
	return &matchStage{filter: filter}, nil
}

func (s *matchStage) Name() string {
	return "$match"
}

func (s *matchStage) Process(ctx context.Context, input Iterator) (Iterator, error) {
	matcher := mongo.NewMatcher(s.filter)
	return &FilterIterator{
		source: input,
		predicate: func(doc *bson.Document) (bool, error) {
			return matcher.Match(doc), nil
		},
	}, nil
}

// ==================== $project stage ====================

// projectStage reshapes documents.
type projectStage struct {
	projection map[string]interface{}
	inclusive  bool // true = include only specified fields
}

func newProjectStage(value interface{}) (*projectStage, error) {
	proj, err := toMap(value)
	if err != nil {
		return nil, fmt.Errorf("$project requires a document: %w", err)
	}

	// Determine if inclusive or exclusive
	inclusive := false
	for k, v := range proj {
		if k != "_id" {
			if val, ok := v.(int); ok && val == 1 {
				inclusive = true
				break
			}
		}
	}

	return &projectStage{projection: proj, inclusive: inclusive}, nil
}

func (s *projectStage) Name() string {
	return "$project"
}

func (s *projectStage) Process(ctx context.Context, input Iterator) (Iterator, error) {
	return &TransformIterator{
		source: input,
		transform: func(doc *bson.Document) (*bson.Document, error) {
			return applyProjection(doc, s.projection, s.inclusive), nil
		},
	}, nil
}

func applyProjection(doc *bson.Document, projection map[string]interface{}, inclusive bool) *bson.Document {
	result := bson.NewDocument()

	// Always include _id unless explicitly excluded
	includeID := true
	if idVal, ok := projection["_id"]; ok {
		if v, ok := idVal.(int); ok && v == 0 {
			includeID = false
		}
	}

	if inclusive {
		// Include only specified fields
		for field, val := range projection {
			if field == "_id" {
				continue
			}
			if v, ok := val.(int); ok && v == 1 {
				if fieldVal, found := doc.Get(field); found {
					result.Set(field, fieldVal)
				}
			} else if _, ok := val.(map[string]interface{}); ok {
				// Expression projection
				result.Set(field, evaluateExpression(val, doc))
			}
		}
		if includeID {
			if idVal, ok := doc.Get("_id"); ok {
				result.Set("_id", idVal)
			}
		}
	} else {
		// Exclude specified fields
		for _, elem := range doc.Elements() {
			if _, exclude := projection[elem.Key]; !exclude {
				result.Set(elem.Key, elem.Value)
			}
		}
		if !includeID {
			result.Delete("_id")
		}
	}

	return result
}

// ==================== $group stage ====================

// groupStage groups documents and applies accumulators.
type groupStage struct {
	idExpr      interface{} // _id expression
	accumulators map[string]Accumulator
}

// Accumulator represents an aggregation accumulator.
type Accumulator struct {
	Operator string
	Expr     interface{}
}

func newGroupStage(value interface{}) (*groupStage, error) {
	groupSpec, err := toMap(value)
	if err != nil {
		return nil, fmt.Errorf("$group requires a document: %w", err)
	}

	idExpr, ok := groupSpec["_id"]
	if !ok {
		return nil, fmt.Errorf("$group requires _id field")
	}

	accumulators := make(map[string]Accumulator)
	for field, val := range groupSpec {
		if field == "_id" {
			continue
		}
		accSpec, ok := val.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("accumulator for %s must be an operator document", field)
		}
		// Find the operator (should be only one key like "$sum", "$avg")
		for op, expr := range accSpec {
			accumulators[field] = Accumulator{Operator: op, Expr: expr}
			break
		}
	}

	return &groupStage{
		idExpr:       idExpr,
		accumulators: accumulators,
	}, nil
}

func (s *groupStage) Name() string {
	return "$group"
}

func (s *groupStage) Process(ctx context.Context, input Iterator) (Iterator, error) {
	// Materialize all input and group
	groups := make(map[string]*groupResult)

	for {
		doc, err := input.Next()
		if err != nil {
			return nil, err
		}
		if doc == nil {
			break
		}

		// Compute group key
		key := computeGroupKey(s.idExpr, doc)

		grp, ok := groups[key]
		if !ok {
			grp = &groupResult{
				id:    evaluateIDExpression(s.idExpr, doc),
				accs:  make(map[string]*accumulatorState),
			}
			for field, acc := range s.accumulators {
				grp.accs[field] = newAccumulator(acc.Operator)
			}
			groups[key] = grp
		}

		// Update accumulators
		for field, acc := range s.accumulators {
			value := evaluateExpression(acc.Expr, doc)
			grp.accs[field].add(value)
		}
	}

	// Build result documents
	results := make([]*bson.Document, 0, len(groups))
	for _, grp := range groups {
		result := bson.NewDocument()
		result.Set("_id", grp.id)
		for field, acc := range grp.accs {
			result.Set(field, acc.result())
		}
		results = append(results, result)
	}

	return newSliceIterator(results), nil
}

type groupResult struct {
	id   bson.Value
	accs map[string]*accumulatorState
}

type accumulatorState struct {
	op     string
	values []interface{}
	count  int64
	sum    float64
	min    interface{}
	max    interface{}
}

func newAccumulator(op string) *accumulatorState {
	return &accumulatorState{op: op, values: make([]interface{}, 0)}
}

func (a *accumulatorState) add(val interface{}) {
	switch a.op {
	case "$sum", "$avg":
		a.sum += toFloat64(val)
		a.count++
	case "$min":
		if a.min == nil || compareValues(val, a.min) < 0 {
			a.min = val
		}
	case "$max":
		if a.max == nil || compareValues(val, a.max) > 0 {
			a.max = val
		}
	case "$first":
		if a.count == 0 {
			a.min = val
		}
		a.count++
	case "$last":
		a.min = val
		a.count++
	case "$push":
		a.values = append(a.values, val)
	}
}

func (a *accumulatorState) result() bson.Value {
	switch a.op {
	case "$sum":
		return bson.VDouble(a.sum)
	case "$avg":
		if a.count > 0 {
			return bson.VDouble(a.sum / float64(a.count))
		}
		return bson.VNull()
	case "$min":
		return toBSONValue(a.min)
	case "$max":
		return toBSONValue(a.max)
	case "$first":
		return toBSONValue(a.min)
	case "$last":
		return toBSONValue(a.min)
	case "$push":
		arr := make(bson.Array, len(a.values))
		for i, v := range a.values {
			arr[i] = toBSONValue(v)
		}
		return bson.VArray(arr)
	default:
		return bson.VNull()
	}
}

// ==================== $sort stage ====================

// sortStage sorts documents.
type sortStage struct {
	sortSpec map[string]interface{}
}

func newSortStage(value interface{}) (*sortStage, error) {
	spec, err := toMap(value)
	if err != nil {
		return nil, fmt.Errorf("$sort requires a document: %w", err)
	}
	return &sortStage{sortSpec: spec}, nil
}

func (s *sortStage) Name() string {
	return "$sort"
}

func (s *sortStage) Process(ctx context.Context, input Iterator) (Iterator, error) {
	// Materialize all documents for sorting
	var docs []*bson.Document
	for {
		doc, err := input.Next()
		if err != nil {
			return nil, err
		}
		if doc == nil {
			break
		}
		docs = append(docs, doc)
	}

	// Sort documents
	sort.Slice(docs, func(i, j int) bool {
		for field, dir := range s.sortSpec {
			ascending := true
			if d, ok := dir.(int); ok && d == -1 {
				ascending = false
			}

			vi, fi := docs[i].Get(field)
			vj, fj := docs[j].Get(field)

			if !fi && !fj {
				continue
			}
			if !fi {
				return true // null sorts first
			}
			if !fj {
				return false
			}

			cmp := bson.CompareValues(vi, vj)
			if ascending {
				return cmp < 0
			}
			return cmp > 0
		}
		return false
	})

	return newSliceIterator(docs), nil
}

// ==================== $limit stage ====================

// limitStage limits the number of documents.
type limitStage struct {
	limit int64
}

func newLimitStage(value interface{}) (*limitStage, error) {
	limit, ok := toInt64(value)
	if !ok || limit < 0 {
		return nil, fmt.Errorf("$limit requires a non-negative integer")
	}
	return &limitStage{limit: limit}, nil
}

func (s *limitStage) Name() string {
	return "$limit"
}

func (s *limitStage) Process(ctx context.Context, input Iterator) (Iterator, error) {
	return newLimitIterator(input, s.limit), nil
}

// ==================== $skip stage ====================

// skipStage skips the first N documents.
type skipStage struct {
	skip int64
}

func newSkipStage(value interface{}) (*skipStage, error) {
	skip, ok := toInt64(value)
	if !ok || skip < 0 {
		return nil, fmt.Errorf("$skip requires a non-negative integer")
	}
	return &skipStage{skip: skip}, nil
}

func (s *skipStage) Name() string {
	return "$skip"
}

func (s *skipStage) Process(ctx context.Context, input Iterator) (Iterator, error) {
	return newSkipIterator(input, s.skip), nil
}

// ==================== $unwind stage ====================

// unwindStage deconstructs arrays.
type unwindStage struct {
	field string
}

func newUnwindStage(value interface{}) (*unwindStage, error) {
	field, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("$unwind requires a string field path")
	}
	// Remove $ prefix if present
	if len(field) > 0 && field[0] == '$' {
		field = field[1:]
	}
	return &unwindStage{field: field}, nil
}

func (s *unwindStage) Name() string {
	return "$unwind"
}

func (s *unwindStage) Process(ctx context.Context, input Iterator) (Iterator, error) {
	var results []*bson.Document

	for {
		doc, err := input.Next()
		if err != nil {
			return nil, err
		}
		if doc == nil {
			break
		}

		val, ok := doc.Get(s.field)
		if !ok || val.Type != bson.TypeArray {
			// Field missing or not array - include document as-is
			results = append(results, doc)
			continue
		}

		arr := val.ArrayValue()
		for _, elem := range arr {
			// Create new document with unwound field
			newDoc := bson.NewDocument()
			for _, e := range doc.Elements() {
				if e.Key != s.field {
					newDoc.Set(e.Key, e.Value)
				}
			}
			newDoc.Set(s.field, elem)
			results = append(results, newDoc)
		}
	}

	return newSliceIterator(results), nil
}

// ==================== $lookup stage ====================

// lookupStage performs left outer join.
type lookupStage struct {
	from         string
	localField   string
	foreignField string
	as           string
}

func newLookupStage(value interface{}) (*lookupStage, error) {
	spec, err := toMap(value)
	if err != nil {
		return nil, fmt.Errorf("$lookup requires a document: %w", err)
	}

	stage := &lookupStage{}
	if v, ok := spec["from"].(string); ok {
		stage.from = v
	}
	if v, ok := spec["localField"].(string); ok {
		stage.localField = v
	}
	if v, ok := spec["foreignField"].(string); ok {
		stage.foreignField = v
	}
	if v, ok := spec["as"].(string); ok {
		stage.as = v
	}

	if stage.from == "" || stage.localField == "" || stage.foreignField == "" || stage.as == "" {
		return nil, fmt.Errorf("$lookup requires from, localField, foreignField, and as")
	}

	return stage, nil
}

func (s *lookupStage) Name() string {
	return "$lookup"
}

func (s *lookupStage) Process(ctx context.Context, input Iterator) (Iterator, error) {
	// For now, return input unchanged - full implementation would need collection access
	// This is a placeholder - full lookup requires database context
	return input, nil
}

// ==================== Helper functions ====================

func toDocument(v interface{}) (*bson.Document, error) {
	switch val := v.(type) {
	case *bson.Document:
		return val, nil
	case map[string]interface{}:
		doc := bson.NewDocument()
		for k, v := range val {
			doc.Set(k, toBSONValue(v))
		}
		return doc, nil
	default:
		return nil, fmt.Errorf("expected document, got %T", v)
	}
}

func toMap(v interface{}) (map[string]interface{}, error) {
	switch val := v.(type) {
	case map[string]interface{}:
		return val, nil
	case *bson.Document:
		m := make(map[string]interface{})
		for _, e := range val.Elements() {
			m[e.Key] = toInterface(e.Value)
		}
		return m, nil
	default:
		return nil, fmt.Errorf("expected map, got %T", v)
	}
}

func toInt64(v interface{}) (int64, bool) {
	switch val := v.(type) {
	case int:
		return int64(val), true
	case int32:
		return int64(val), true
	case int64:
		return val, true
	case float64:
		return int64(val), true
	default:
		return 0, false
	}
}

func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case int:
		return float64(val)
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	case float64:
		return val
	case bson.Value:
		switch val.Type {
		case bson.TypeInt32:
			return float64(val.Int32())
		case bson.TypeInt64:
			return float64(val.Int64())
		case bson.TypeDouble:
			return val.Double()
		default:
			return 0
		}
	default:
		return 0
	}
}

func toBSONValue(v interface{}) bson.Value {
	switch val := v.(type) {
	case nil:
		return bson.VNull()
	case bool:
		return bson.VBool(val)
	case int:
		return bson.VInt32(int32(val))
	case int32:
		return bson.VInt32(val)
	case int64:
		return bson.VInt64(val)
	case float64:
		return bson.VDouble(val)
	case string:
		return bson.VString(val)
	case bson.Value:
		return val
	default:
		return bson.VNull()
	}
}

func toInterface(v bson.Value) interface{} {
	switch v.Type {
	case bson.TypeNull:
		return nil
	case bson.TypeBoolean:
		return v.Boolean()
	case bson.TypeInt32:
		return v.Int32()
	case bson.TypeInt64:
		return v.Int64()
	case bson.TypeDouble:
		return v.Double()
	case bson.TypeString:
		return v.String()
	default:
		return v.Interface()
	}
}

func computeGroupKey(idExpr interface{}, doc *bson.Document) string {
	val := evaluateExpression(idExpr, doc)
	return fmt.Sprintf("%v", toInterface(val))
}

func evaluateIDExpression(idExpr interface{}, doc *bson.Document) bson.Value {
	if idExpr == nil {
		return bson.VNull()
	}
	if s, ok := idExpr.(string); ok && s == "$null" {
		return bson.VNull()
	}
	return evaluateExpression(idExpr, doc)
}

func evaluateExpression(expr interface{}, doc *bson.Document) bson.Value {
	switch val := expr.(type) {
	case string:
		// Field reference: "$field"
		if len(val) > 0 && val[0] == '$' {
			field := val[1:]
			if v, ok := doc.Get(field); ok {
				return v
			}
			return bson.VNull()
		}
		return bson.VString(val)
	case int:
		return bson.VInt32(int32(val))
	case int32:
		return bson.VInt32(val)
	case int64:
		return bson.VInt64(val)
	case float64:
		return bson.VDouble(val)
	case bool:
		return bson.VBool(val)
	case map[string]interface{}:
		// Operator expression
		for op, operand := range val {
			switch op {
			case "$add":
				return evaluateAdd(operand, doc)
			case "$multiply":
				return evaluateMultiply(operand, doc)
			case "$subtract":
				return evaluateSubtract(operand, doc)
			case "$divide":
				return evaluateDivide(operand, doc)
			default:
				return bson.VNull()
			}
		}
	}
	return bson.VNull()
}

func evaluateAdd(operand interface{}, doc *bson.Document) bson.Value {
	if arr, ok := operand.([]interface{}); ok {
		sum := 0.0
		for _, v := range arr {
			sum += toFloat64(toInterface(evaluateExpression(v, doc)))
		}
		return bson.VDouble(sum)
	}
	return bson.VNull()
}

func evaluateMultiply(operand interface{}, doc *bson.Document) bson.Value {
	if arr, ok := operand.([]interface{}); ok && len(arr) >= 2 {
		prod := toFloat64(toInterface(evaluateExpression(arr[0], doc)))
		for i := 1; i < len(arr); i++ {
			prod *= toFloat64(toInterface(evaluateExpression(arr[i], doc)))
		}
		return bson.VDouble(prod)
	}
	return bson.VNull()
}

func evaluateSubtract(operand interface{}, doc *bson.Document) bson.Value {
	if arr, ok := operand.([]interface{}); ok && len(arr) == 2 {
		a := toFloat64(toInterface(evaluateExpression(arr[0], doc)))
		b := toFloat64(toInterface(evaluateExpression(arr[1], doc)))
		return bson.VDouble(a - b)
	}
	return bson.VNull()
}

func evaluateDivide(operand interface{}, doc *bson.Document) bson.Value {
	if arr, ok := operand.([]interface{}); ok && len(arr) == 2 {
		a := toFloat64(toInterface(evaluateExpression(arr[0], doc)))
		b := toFloat64(toInterface(evaluateExpression(arr[1], doc)))
		if b == 0 {
			return bson.VNull()
		}
		return bson.VDouble(a / b)
	}
	return bson.VNull()
}

func compareValues(a, b interface{}) int {
	fa := toFloat64(a)
	fb := toFloat64(b)
	if fa < fb {
		return -1
	}
	if fa > fb {
		return 1
	}
	return 0
}
