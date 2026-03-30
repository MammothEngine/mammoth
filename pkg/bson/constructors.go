package bson

// D creates a Document from alternating key-value pairs.
func D(pairs ...interface{}) *Document {
	doc := NewDocument()
	for i := 0; i+1 < len(pairs); i += 2 {
		key := pairs[i].(string)
		val := pairs[i+1].(Value)
		doc.Set(key, val)
	}
	return doc
}

// M creates a Document from a map[string]Value.
func M(m map[string]Value) *Document {
	doc := NewDocument()
	for k, v := range m {
		doc.Set(k, v)
	}
	return doc
}

// A creates an Array from Values.
func A(vals ...Value) Array {
	return Array(vals)
}

// Value constructors

// VDouble creates a Double value.
func VDouble(v float64) Value {
	return Value{Type: TypeDouble, value: v}
}

// VString creates a String value.
func VString(v string) Value {
	return Value{Type: TypeString, value: v}
}

// VDoc creates a Document value.
func VDoc(d *Document) Value {
	return Value{Type: TypeDocument, value: d}
}

// VArray creates an Array value.
func VArray(a Array) Value {
	return Value{Type: TypeArray, value: a}
}

// VBinary creates a Binary value.
func VBinary(subtype BinarySubtype, data []byte) Value {
	return Value{Type: TypeBinary, value: Binary{Subtype: subtype, Data: data}}
}

// VBool creates a Boolean value.
func VBool(v bool) Value {
	return Value{Type: TypeBoolean, value: v}
}

// VDateTime creates a DateTime value.
func VDateTime(ms int64) Value {
	return Value{Type: TypeDateTime, value: ms}
}

// VInt32 creates an Int32 value.
func VInt32(v int32) Value {
	return Value{Type: TypeInt32, value: v}
}

// VInt64 creates an Int64 value.
func VInt64(v int64) Value {
	return Value{Type: TypeInt64, value: v}
}

// VNull creates a Null value.
func VNull() Value {
	return Value{Type: TypeNull, value: nil}
}

// VObjectID creates an ObjectID value.
func VObjectID(id ObjectID) Value {
	return Value{Type: TypeObjectID, value: id}
}

// VRegex creates a Regex value.
func VRegex(pattern, options string) Value {
	return Value{Type: TypeRegex, value: Regex{Pattern: pattern, Options: options}}
}

// VTimestamp creates a Timestamp value.
func VTimestamp(v uint64) Value {
	return Value{Type: TypeTimestamp, value: v}
}

// VSymbol creates a Symbol value.
func VSymbol(v string) Value {
	return Value{Type: TypeSymbol, value: v}
}

// VJavaScript creates a JavaScript value.
func VJavaScript(code string) Value {
	return Value{Type: TypeJavaScript, value: code}
}

// VMinKey creates a MinKey value.
func VMinKey() Value {
	return Value{Type: TypeMinKey, value: nil}
}

// VMaxKey creates a MaxKey value.
func VMaxKey() Value {
	return Value{Type: TypeMaxKey, value: nil}
}
