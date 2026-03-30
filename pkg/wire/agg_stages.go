package wire

import (
	"strings"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

// evaluateExpr evaluates an aggregation expression against a document.
// Supports:
//   - Field reference: "$fieldName" -> doc.Get(field)
//   - Literal: non-string value -> the value itself
//   - Object: { $add: [...], $subtract: [...], ... } -> computed
func evaluateExpr(expr bson.Value, doc *bson.Document) bson.Value {
	if expr.Type == bson.TypeString {
		s := expr.String()
		if len(s) > 0 && s[0] == '$' {
			fieldName := s[1:]
			if v, ok := mongo.ResolveField(doc, fieldName); ok {
				return v
			}
			return bson.VNull()
		}
		return expr
	}
	if expr.Type == bson.TypeDocument {
		obj := expr.DocumentValue()
		if obj.Len() == 0 {
			return bson.VNull()
		}
		keys := obj.Keys()
		op := keys[0]
		opVal, _ := obj.Get(op)

		switch op {
		case "$add":
			return exprAdd(opVal, doc)
		case "$subtract":
			return exprSubtract(opVal, doc)
		case "$multiply":
			return exprMultiply(opVal, doc)
		case "$divide":
			return exprDivide(opVal, doc)
		case "$concat":
			return exprConcat(opVal, doc)
		case "$toLower":
			return exprToLower(opVal, doc)
		case "$toUpper":
			return exprToUpper(opVal, doc)
		case "$substr":
			return exprSubstr(opVal, doc)
		case "$cond":
			return exprCond(opVal, doc)
		case "$ifNull":
			return exprIfNull(opVal, doc)
		default:
			// Treat as literal document
			return expr
		}
	}
	return expr
}

// --- Expression operators ---

func exprAdd(opVal bson.Value, doc *bson.Document) bson.Value {
	arr := opVal.ArrayValue()
	var total float64
	for _, v := range arr {
		ev := evaluateExpr(v, doc)
		total += toFloat64(ev)
	}
	return bson.VDouble(total)
}

func exprSubtract(opVal bson.Value, doc *bson.Document) bson.Value {
	arr := opVal.ArrayValue()
	if len(arr) < 2 {
		return bson.VInt32(0)
	}
	a := toFloat64(evaluateExpr(arr[0], doc))
	b := toFloat64(evaluateExpr(arr[1], doc))
	return bson.VDouble(a - b)
}

func exprMultiply(opVal bson.Value, doc *bson.Document) bson.Value {
	arr := opVal.ArrayValue()
	var total float64 = 1
	for _, v := range arr {
		ev := evaluateExpr(v, doc)
		total *= toFloat64(ev)
	}
	return bson.VDouble(total)
}

func exprDivide(opVal bson.Value, doc *bson.Document) bson.Value {
	arr := opVal.ArrayValue()
	if len(arr) < 2 {
		return bson.VNull()
	}
	a := toFloat64(evaluateExpr(arr[0], doc))
	b := toFloat64(evaluateExpr(arr[1], doc))
	if b == 0 {
		return bson.VNull()
	}
	return bson.VDouble(a / b)
}

func exprConcat(opVal bson.Value, doc *bson.Document) bson.Value {
	arr := opVal.ArrayValue()
	var sb strings.Builder
	for _, v := range arr {
		ev := evaluateExpr(v, doc)
		if ev.Type == bson.TypeString {
			sb.WriteString(ev.String())
		} else if ev.Type != bson.TypeNull {
			sb.WriteString(valueToString(ev))
		}
	}
	return bson.VString(sb.String())
}

func exprToLower(opVal bson.Value, doc *bson.Document) bson.Value {
	ev := evaluateExpr(opVal, doc)
	if ev.Type == bson.TypeString {
		return bson.VString(strings.ToLower(ev.String()))
	}
	return bson.VNull()
}

func exprToUpper(opVal bson.Value, doc *bson.Document) bson.Value {
	ev := evaluateExpr(opVal, doc)
	if ev.Type == bson.TypeString {
		return bson.VString(strings.ToUpper(ev.String()))
	}
	return bson.VNull()
}

func exprSubstr(opVal bson.Value, doc *bson.Document) bson.Value {
	arr := opVal.ArrayValue()
	if len(arr) < 3 {
		return bson.VString("")
	}
	s := evaluateExpr(arr[0], doc)
	if s.Type != bson.TypeString {
		return bson.VString("")
	}
	start := int(toFloat64(evaluateExpr(arr[1], doc)))
	length := int(toFloat64(evaluateExpr(arr[2], doc)))
	str := s.String()
	if start < 0 {
		start = 0
	}
	if start > len(str) {
		return bson.VString("")
	}
	end := start + length
	if end > len(str) {
		end = len(str)
	}
	return bson.VString(str[start:end])
}

func exprCond(opVal bson.Value, doc *bson.Document) bson.Value {
	arr := opVal.ArrayValue()
	if len(arr) < 3 {
		return bson.VNull()
	}
	condVal := evaluateExpr(arr[0], doc)
	if isTruthy(condVal) {
		return evaluateExpr(arr[1], doc)
	}
	return evaluateExpr(arr[2], doc)
}

func exprIfNull(opVal bson.Value, doc *bson.Document) bson.Value {
	arr := opVal.ArrayValue()
	if len(arr) < 2 {
		return bson.VNull()
	}
	ev := evaluateExpr(arr[0], doc)
	if ev.Type != bson.TypeNull {
		return ev
	}
	return evaluateExpr(arr[1], doc)
}

// --- Helper functions ---

func toFloat64(v bson.Value) float64 {
	switch v.Type {
	case bson.TypeInt32:
		return float64(v.Int32())
	case bson.TypeInt64:
		return float64(v.Int64())
	case bson.TypeDouble:
		return v.Double()
	case bson.TypeBoolean:
		if v.Boolean() {
			return 1
		}
		return 0
	}
	return 0
}

func isTruthy(v bson.Value) bool {
	switch v.Type {
	case bson.TypeNull:
		return false
	case bson.TypeBoolean:
		return v.Boolean()
	case bson.TypeInt32:
		return v.Int32() != 0
	case bson.TypeInt64:
		return v.Int64() != 0
	case bson.TypeDouble:
		return v.Double() != 0
	case bson.TypeString:
		return len(v.String()) > 0
	default:
		return true
	}
}

func valueToString(v bson.Value) string {
	switch v.Type {
	case bson.TypeInt32:
		return int32ToStr(v.Int32())
	case bson.TypeInt64:
		return int64ToStr(v.Int64())
	case bson.TypeDouble:
		return float64ToStr(v.Double())
	case bson.TypeBoolean:
		if v.Boolean() {
			return "true"
		}
		return "false"
	default:
		return ""
	}
}

// int32ToStr converts int32 to string without strconv dependency.
func int32ToStr(n int32) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [12]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

func int64ToStr(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [22]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

func float64ToStr(f float64) string {
	// Simple conversion: use int64 for integer part
	if f == float64(int64(f)) {
		return int64ToStr(int64(f))
	}
	// Fallback: approximate
	intPart := int64(f)
	frac := f - float64(intPart)
	if frac < 0 {
		frac = -frac
	}
	// Up to 6 decimal places
	frac *= 1e6
	fracInt := int64(frac + 0.5)
	s := int64ToStr(intPart) + "." + int64ToStr(fracInt)
	return s
}

// --- Pipeline stages ---

// stageProject handles $project aggregation stage.
// Supports inclusion/exclusion mode, dot notation, and simple expressions.
func stageProject(docs []*bson.Document, stageVal bson.Value) []*bson.Document {
	if stageVal.Type != bson.TypeDocument {
		return docs
	}
	spec := stageVal.DocumentValue()

	// Determine mode: inclusion or exclusion
	// _id is special: defaults to inclusion unless explicitly set to 0
	inclusion := true
	idExcluded := false
	for _, e := range spec.Elements() {
		if e.Key == "_id" {
			if e.Value.Type == bson.TypeInt32 && e.Value.Int32() == 0 {
				idExcluded = true
			}
			continue
		}
		if e.Value.Type == bson.TypeInt32 && e.Value.Int32() == 0 {
			inclusion = false
			break
		}
		if e.Value.Type == bson.TypeBoolean && !e.Value.Boolean() {
			inclusion = false
			break
		}
	}

	result := make([]*bson.Document, 0, len(docs))
	for _, doc := range docs {
		out := bson.NewDocument()

		if inclusion {
			// Inclusion mode: only include specified fields
			includeID := !idExcluded
			for _, e := range spec.Elements() {
				if e.Key == "_id" {
					if !idExcluded {
						if v, ok := doc.Get("_id"); ok {
							out.Set("_id", v)
						}
					}
					continue
				}
				// Value is expression or 1
				if e.Value.Type == bson.TypeInt32 && e.Value.Int32() == 1 {
					if v, ok := mongo.ResolveField(doc, e.Key); ok {
						setNestedValue(out, e.Key, v)
					}
				} else if e.Value.Type == bson.TypeBoolean && e.Value.Boolean() {
					if v, ok := mongo.ResolveField(doc, e.Key); ok {
						setNestedValue(out, e.Key, v)
					}
				} else {
					// Expression
					val := evaluateExpr(e.Value, doc)
					setNestedValue(out, e.Key, val)
				}
			}
			// Include _id by default if not explicitly handled
			if includeID {
				if _, ok := out.Get("_id"); !ok {
					if v, ok := doc.Get("_id"); ok {
						out.Set("_id", v)
					}
				}
			}
		} else {
			// Exclusion mode: include everything except excluded fields
			for _, e := range doc.Elements() {
				excluded := false
				for _, se := range spec.Elements() {
					if se.Key == e.Key {
						if se.Value.Type == bson.TypeInt32 && se.Value.Int32() == 0 {
							excluded = true
						} else if se.Value.Type == bson.TypeBoolean && !se.Value.Boolean() {
							excluded = true
						}
						break
					}
				}
				if !excluded {
					out.Set(e.Key, e.Value)
				}
			}
		}

		result = append(result, out)
	}
	return result
}

// stageUnwind handles $unwind aggregation stage.
// Supports basic string form and object form with preserveNullAndEmptyArrays.
func stageUnwind(docs []*bson.Document, stageVal bson.Value) []*bson.Document {
	var path string
	preserve := false

	if stageVal.Type == bson.TypeString {
		s := stageVal.String()
		if len(s) > 0 && s[0] == '$' {
			path = s[1:]
		} else {
			return docs
		}
	} else if stageVal.Type == bson.TypeDocument {
		obj := stageVal.DocumentValue()
		if p, ok := obj.Get("path"); ok && p.Type == bson.TypeString {
			s := p.String()
			if len(s) > 0 && s[0] == '$' {
				path = s[1:]
			}
		}
		if pr, ok := obj.Get("preserveNullAndEmptyArrays"); ok && pr.Type == bson.TypeBoolean {
			preserve = pr.Boolean()
		}
		if path == "" {
			return docs
		}
	} else {
		return docs
	}

	var result []*bson.Document
	for _, doc := range docs {
		fieldVal, found := mongo.ResolveField(doc, path)

		if !found || fieldVal.Type != bson.TypeArray {
			if preserve {
				result = append(result, doc)
			}
			continue
		}

		arr := fieldVal.ArrayValue()
		if len(arr) == 0 {
			if preserve {
				// Output doc without the field
				out := cloneDoc(doc)
				unsetNested(out, path)
				result = append(result, out)
			}
			continue
		}

		for _, elem := range arr {
			out := cloneDoc(doc)
			setNestedValue(out, path, elem)
			result = append(result, out)
		}
	}
	return result
}

// stageLookup handles $lookup aggregation stage (left outer join).
func stageLookup(docs []*bson.Document, stageVal bson.Value, eng interface {
	Scan(prefix []byte, fn func(key, value []byte) bool) error
	Get(key []byte) ([]byte, error)
}, cat *mongo.Catalog, db string) []*bson.Document {
	if stageVal.Type != bson.TypeDocument {
		return docs
	}
	spec := stageVal.DocumentValue()

	fromColl, _ := spec.Get("from")
	localField, _ := spec.Get("localField")
	foreignField, _ := spec.Get("foreignField")
	asField, _ := spec.Get("as")

	if fromColl.Type != bson.TypeString || localField.Type != bson.TypeString ||
		foreignField.Type != bson.TypeString || asField.Type != bson.TypeString {
		return docs
	}

	fromName := fromColl.String()
	localF := localField.String()
	foreignF := foreignField.String()
	asF := asField.String()

	// Load all docs from the "from" collection
	var fromDocs []*bson.Document
	prefix := mongo.EncodeNamespacePrefix(db, fromName)
	eng.Scan(prefix, func(_, value []byte) bool {
		doc, err := bson.Decode(value)
		if err != nil {
			return true
		}
		fromDocs = append(fromDocs, doc)
		return true
	})

	result := make([]*bson.Document, 0, len(docs))
	for _, doc := range docs {
		out := cloneDoc(doc)

		localVal, _ := mongo.ResolveField(doc, localF)
		var matched bson.Array

		for _, fromDoc := range fromDocs {
			foreignVal, _ := mongo.ResolveField(fromDoc, foreignF)
			if bson.CompareValues(localVal, foreignVal) == 0 {
				matched = append(matched, bson.VDoc(fromDoc))
			}
		}

		if matched == nil {
			matched = bson.Array{}
		}
		setNestedValue(out, asF, bson.VArray(matched))
		result = append(result, out)
	}
	return result
}

// stageAddFields handles $addFields / $set aggregation stage.
func stageAddFields(docs []*bson.Document, stageVal bson.Value) []*bson.Document {
	if stageVal.Type != bson.TypeDocument {
		return docs
	}
	spec := stageVal.DocumentValue()

	result := make([]*bson.Document, 0, len(docs))
	for _, doc := range docs {
		out := cloneDoc(doc)
		for _, e := range spec.Elements() {
			val := evaluateExpr(e.Value, doc)
			setNestedValue(out, e.Key, val)
		}
		result = append(result, out)
	}
	return result
}

// --- Additional $group accumulators ---

// accumulateAvg computes the average of a field across grouped documents.
func accumulateAvg(docs []*bson.Document, accVal bson.Value) bson.Value {
	if accVal.Type == bson.TypeString && len(accVal.String()) > 0 && accVal.String()[0] == '$' {
		fieldName := accVal.String()[1:]
		var total float64
		var count int
		for _, doc := range docs {
			if v, ok := doc.Get(fieldName); ok {
				total += toFloat64(v)
				count++
			}
		}
		if count == 0 {
			return bson.VNull()
		}
		return bson.VDouble(total / float64(count))
	}
	return bson.VNull()
}

// accumulateMin computes the minimum value of a field.
func accumulateMin(docs []*bson.Document, accVal bson.Value) bson.Value {
	if accVal.Type == bson.TypeString && len(accVal.String()) > 0 && accVal.String()[0] == '$' {
		fieldName := accVal.String()[1:]
		var min bson.Value
		found := false
		for _, doc := range docs {
			if v, ok := doc.Get(fieldName); ok {
				if !found || bson.CompareValues(v, min) < 0 {
					min = v
					found = true
				}
			}
		}
		if !found {
			return bson.VNull()
		}
		return min
	}
	return bson.VNull()
}

// accumulateMax computes the maximum value of a field.
func accumulateMax(docs []*bson.Document, accVal bson.Value) bson.Value {
	if accVal.Type == bson.TypeString && len(accVal.String()) > 0 && accVal.String()[0] == '$' {
		fieldName := accVal.String()[1:]
		var max bson.Value
		found := false
		for _, doc := range docs {
			if v, ok := doc.Get(fieldName); ok {
				if !found || bson.CompareValues(v, max) > 0 {
					max = v
					found = true
				}
			}
		}
		if !found {
			return bson.VNull()
		}
		return max
	}
	return bson.VNull()
}

// accumulateFirst returns the first document's value for a field.
func accumulateFirst(docs []*bson.Document, accVal bson.Value) bson.Value {
	if len(docs) == 0 {
		return bson.VNull()
	}
	if accVal.Type == bson.TypeString && len(accVal.String()) > 0 && accVal.String()[0] == '$' {
		fieldName := accVal.String()[1:]
		if v, ok := docs[0].Get(fieldName); ok {
			return v
		}
	}
	return bson.VNull()
}

// accumulateLast returns the last document's value for a field.
func accumulateLast(docs []*bson.Document, accVal bson.Value) bson.Value {
	if len(docs) == 0 {
		return bson.VNull()
	}
	if accVal.Type == bson.TypeString && len(accVal.String()) > 0 && accVal.String()[0] == '$' {
		fieldName := accVal.String()[1:]
		if v, ok := docs[len(docs)-1].Get(fieldName); ok {
			return v
		}
	}
	return bson.VNull()
}

// accumulatePush collects all values of a field into an array.
func accumulatePush(docs []*bson.Document, accVal bson.Value) bson.Value {
	if accVal.Type == bson.TypeString && len(accVal.String()) > 0 && accVal.String()[0] == '$' {
		fieldName := accVal.String()[1:]
		var arr bson.Array
		for _, doc := range docs {
			if v, ok := doc.Get(fieldName); ok {
				arr = append(arr, v)
			}
		}
		return bson.VArray(arr)
	}
	return bson.VArray(nil)
}

// accumulateAddToSet collects unique values of a field into an array.
func accumulateAddToSet(docs []*bson.Document, accVal bson.Value) bson.Value {
	if accVal.Type == bson.TypeString && len(accVal.String()) > 0 && accVal.String()[0] == '$' {
		fieldName := accVal.String()[1:]
		var arr bson.Array
		for _, doc := range docs {
			if v, ok := doc.Get(fieldName); ok {
				exists := false
				for _, existing := range arr {
					if bson.CompareValues(existing, v) == 0 {
						exists = true
						break
					}
				}
				if !exists {
					arr = append(arr, v)
				}
			}
		}
		return bson.VArray(arr)
	}
	return bson.VArray(nil)
}

// --- Document helpers ---

func cloneDoc(doc *bson.Document) *bson.Document {
	out := bson.NewDocument()
	for _, e := range doc.Elements() {
		out.Set(e.Key, cloneVal(e.Value))
	}
	return out
}

func cloneVal(v bson.Value) bson.Value {
	switch v.Type {
	case bson.TypeDocument:
		return bson.VDoc(cloneDoc(v.DocumentValue()))
	case bson.TypeArray:
		arr := v.ArrayValue()
		clone := make(bson.Array, len(arr))
		for i, elem := range arr {
			clone[i] = cloneVal(elem)
		}
		return bson.VArray(clone)
	default:
		return v
	}
}

func setNestedValue(doc *bson.Document, path string, val bson.Value) {
	parts := strings.SplitN(path, ".", 2)
	if len(parts) == 1 {
		doc.Set(parts[0], val)
		return
	}
	v, ok := doc.Get(parts[0])
	if !ok || v.Type != bson.TypeDocument {
		sub := bson.NewDocument()
		setNestedValue(sub, parts[1], val)
		doc.Set(parts[0], bson.VDoc(sub))
		return
	}
	setNestedValue(v.DocumentValue(), parts[1], val)
}

func unsetNested(doc *bson.Document, path string) {
	parts := strings.SplitN(path, ".", 2)
	if len(parts) == 1 {
		doc.Delete(parts[0])
		return
	}
	v, ok := doc.Get(parts[0])
	if !ok || v.Type != bson.TypeDocument {
		return
	}
	unsetNested(v.DocumentValue(), parts[1])
}
