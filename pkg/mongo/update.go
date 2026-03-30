package mongo

import (
	"strings"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// ApplyUpdate applies update operators to a document, returning the modified doc.
// The original document is not mutated; a new document is returned.
func ApplyUpdate(doc *bson.Document, update *bson.Document) *bson.Document {
	result := cloneDocument(doc)
	for _, e := range update.Elements() {
		switch e.Key {
		case "$set":
			applySet(result, e.Value.DocumentValue())
		case "$unset":
			applyUnset(result, e.Value.DocumentValue())
		case "$inc":
			applyInc(result, e.Value.DocumentValue())
		case "$mul":
			applyMul(result, e.Value.DocumentValue())
		case "$min":
			applyMin(result, e.Value.DocumentValue())
		case "$max":
			applyMax(result, e.Value.DocumentValue())
		case "$rename":
			applyRename(result, e.Value.DocumentValue())
		case "$currentDate":
			applyCurrentDate(result, e.Value.DocumentValue())
		case "$push":
			applyPush(result, e.Value.DocumentValue())
		case "$pop":
			applyPop(result, e.Value.DocumentValue())
		case "$addToSet":
			applyAddToSet(result, e.Value.DocumentValue())
		case "$pull":
			applyPull(result, e.Value.DocumentValue())
		default:
			// Unknown operator: skip
		}
	}
	return result
}

func cloneDocument(doc *bson.Document) *bson.Document {
	clone := bson.NewDocument()
	for _, e := range doc.Elements() {
		clone.Set(e.Key, cloneValue(e.Value))
	}
	return clone
}

func cloneValue(v bson.Value) bson.Value {
	switch v.Type {
	case bson.TypeDocument:
		return bson.VDoc(cloneDocument(v.DocumentValue()))
	case bson.TypeArray:
		arr := v.ArrayValue()
		clone := make(bson.Array, len(arr))
		for i, elem := range arr {
			clone[i] = cloneValue(elem)
		}
		return bson.VArray(clone)
	default:
		return v
	}
}

// --- Update operators ---

func applySet(doc *bson.Document, fields *bson.Document) {
	for _, e := range fields.Elements() {
		setNestedField(doc, e.Key, cloneValue(e.Value))
	}
}

func applyUnset(doc *bson.Document, fields *bson.Document) {
	for _, e := range fields.Elements() {
		unsetNestedField(doc, e.Key)
	}
}

func applyInc(doc *bson.Document, fields *bson.Document) {
	for _, e := range fields.Elements() {
		cur, found := resolveField(doc, e.Key)
		var delta float64
		switch e.Value.Type {
		case bson.TypeInt32:
			delta = float64(e.Value.Int32())
		case bson.TypeInt64:
			delta = float64(e.Value.Int64())
		case bson.TypeDouble:
			delta = e.Value.Double()
		}
		var newVal bson.Value
		if !found || cur.Type == bson.TypeNull {
			newVal = e.Value
		} else {
			switch cur.Type {
			case bson.TypeInt32:
				newVal = bson.VInt32(cur.Int32() + int32(delta))
			case bson.TypeInt64:
				newVal = bson.VInt64(cur.Int64() + int64(delta))
			case bson.TypeDouble:
				newVal = bson.VDouble(cur.Double() + delta)
			default:
				newVal = bson.VDouble(delta)
			}
		}
		setNestedField(doc, e.Key, newVal)
	}
}

func applyMul(doc *bson.Document, fields *bson.Document) {
	for _, e := range fields.Elements() {
		cur, found := resolveField(doc, e.Key)
		var factor float64
		switch e.Value.Type {
		case bson.TypeInt32:
			factor = float64(e.Value.Int32())
		case bson.TypeInt64:
			factor = float64(e.Value.Int64())
		case bson.TypeDouble:
			factor = e.Value.Double()
		}
		var newVal bson.Value
		if !found || cur.Type == bson.TypeNull {
			newVal = bson.VInt32(0)
		} else {
			switch cur.Type {
			case bson.TypeInt32:
				newVal = bson.VInt32(cur.Int32() * int32(factor))
			case bson.TypeInt64:
				newVal = bson.VInt64(cur.Int64() * int64(factor))
			case bson.TypeDouble:
				newVal = bson.VDouble(cur.Double() * factor)
			default:
				newVal = bson.VDouble(0)
			}
		}
		setNestedField(doc, e.Key, newVal)
	}
}

func applyMin(doc *bson.Document, fields *bson.Document) {
	for _, e := range fields.Elements() {
		cur, found := resolveField(doc, e.Key)
		if !found || bson.CompareValues(e.Value, cur) < 0 {
			setNestedField(doc, e.Key, cloneValue(e.Value))
		}
	}
}

func applyMax(doc *bson.Document, fields *bson.Document) {
	for _, e := range fields.Elements() {
		cur, found := resolveField(doc, e.Key)
		if !found || bson.CompareValues(e.Value, cur) > 0 {
			setNestedField(doc, e.Key, cloneValue(e.Value))
		}
	}
}

func applyRename(doc *bson.Document, fields *bson.Document) {
	for _, e := range fields.Elements() {
		if e.Value.Type != bson.TypeString {
			continue
		}
		oldPath := e.Key
		newPath := e.Value.String()
		val, found := resolveField(doc, oldPath)
		if found {
			unsetNestedField(doc, oldPath)
			setNestedField(doc, newPath, val)
		}
	}
}

func applyCurrentDate(doc *bson.Document, fields *bson.Document) {
	now := time.Now().UnixMilli()
	for _, e := range fields.Elements() {
		var val bson.Value
		if e.Value.Type == bson.TypeBoolean && e.Value.Boolean() {
			val = bson.VDateTime(now)
		} else if e.Value.Type == bson.TypeDocument {
			tsDoc := e.Value.DocumentValue()
			if v, ok := tsDoc.Get("$type"); ok && v.String() == "timestamp" {
				val = bson.VTimestamp(uint64(now))
			} else {
				val = bson.VDateTime(now)
			}
		} else {
			val = bson.VDateTime(now)
		}
		setNestedField(doc, e.Key, val)
	}
}

func applyPush(doc *bson.Document, fields *bson.Document) {
	for _, e := range fields.Elements() {
		cur, found := resolveField(doc, e.Key)
		var arr bson.Array
		if found && cur.Type == bson.TypeArray {
			arr = cur.ArrayValue()
		} else {
			arr = bson.Array{}
		}

		// Check for $each modifier
		if e.Value.Type == bson.TypeDocument {
			pushDoc := e.Value.DocumentValue()
			if eachVal, ok := pushDoc.Get("$each"); ok {
				eachArr := eachVal.ArrayValue()
				for _, v := range eachArr {
					arr = append(arr, cloneValue(v))
				}
				// Handle $slice
				if sliceVal, ok := pushDoc.Get("$slice"); ok {
					slice := int(sliceVal.Int32())
					if slice >= 0 && slice < len(arr) {
						arr = arr[:slice]
					} else if slice < 0 && -slice < len(arr) {
						arr = arr[len(arr)+slice:]
					}
				}
				setNestedField(doc, e.Key, bson.VArray(arr))
				continue
			}
		}
		arr = append(arr, cloneValue(e.Value))
		setNestedField(doc, e.Key, bson.VArray(arr))
	}
}

func applyPop(doc *bson.Document, fields *bson.Document) {
	for _, e := range fields.Elements() {
		cur, found := resolveField(doc, e.Key)
		if !found || cur.Type != bson.TypeArray {
			continue
		}
		arr := cur.ArrayValue()
		if len(arr) == 0 {
			continue
		}
		if e.Value.Int32() == 1 {
			// Remove last
			arr = arr[:len(arr)-1]
		} else {
			// Remove first
			arr = arr[1:]
		}
		setNestedField(doc, e.Key, bson.VArray(arr))
	}
}

func applyAddToSet(doc *bson.Document, fields *bson.Document) {
	for _, e := range fields.Elements() {
		cur, found := resolveField(doc, e.Key)
		var arr bson.Array
		if found && cur.Type == bson.TypeArray {
			arr = cur.ArrayValue()
		} else {
			arr = bson.Array{}
		}

		valsToAdd := bson.Array{e.Value}
		if e.Value.Type == bson.TypeDocument {
			if eachVal, ok := e.Value.DocumentValue().Get("$each"); ok {
				valsToAdd = eachVal.ArrayValue()
			}
		}

		for _, v := range valsToAdd {
			exists := false
			for _, existing := range arr {
				if bson.CompareValues(existing, v) == 0 {
					exists = true
					break
				}
			}
			if !exists {
				arr = append(arr, cloneValue(v))
			}
		}
		setNestedField(doc, e.Key, bson.VArray(arr))
	}
}

func applyPull(doc *bson.Document, fields *bson.Document) {
	for _, e := range fields.Elements() {
		cur, found := resolveField(doc, e.Key)
		if !found || cur.Type != bson.TypeArray {
			continue
		}
		arr := cur.ArrayValue()
		var newArr bson.Array
		for _, v := range arr {
			if e.Value.Type == bson.TypeDocument {
				// Use matcher for condition
				m := NewMatcher(e.Value.DocumentValue())
				tempDoc := bson.NewDocument()
				tempDoc.Set("v", v)
				if m.Match(tempDoc) {
					continue
				}
			} else {
				if bson.CompareValues(v, e.Value) == 0 {
					continue
				}
			}
			newArr = append(newArr, v)
		}
		setNestedField(doc, e.Key, bson.VArray(newArr))
	}
}

// --- Nested field helpers ---

func setNestedField(doc *bson.Document, path string, val bson.Value) {
	parts := strings.SplitN(path, ".", 2)
	if len(parts) == 1 {
		doc.Set(parts[0], val)
		return
	}
	v, ok := doc.Get(parts[0])
	if !ok || v.Type != bson.TypeDocument {
		// Create intermediate document
		sub := bson.NewDocument()
		setNestedField(sub, parts[1], val)
		doc.Set(parts[0], bson.VDoc(sub))
		return
	}
	sub := v.DocumentValue()
	// Mutate in place is fine since we cloned
	setNestedField(sub, parts[1], val)
}

func unsetNestedField(doc *bson.Document, path string) {
	parts := strings.SplitN(path, ".", 2)
	if len(parts) == 1 {
		doc.Delete(parts[0])
		return
	}
	v, ok := doc.Get(parts[0])
	if !ok || v.Type != bson.TypeDocument {
		return
	}
	unsetNestedField(v.DocumentValue(), parts[1])
}
