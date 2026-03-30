package bson

import "fmt"

// Element represents a key-value pair in a BSON document.
type Element struct {
	Key   string
	Value Value
}

// Document represents an ordered BSON document.
type Document struct {
	elements []Element
	index    map[string]int
}

// NewDocument creates an empty BSON document.
func NewDocument() *Document {
	return &Document{index: make(map[string]int)}
}

// Set adds or updates a key-value pair.
func (d *Document) Set(key string, value Value) {
	if d.index != nil {
		if i, ok := d.index[key]; ok {
			d.elements[i].Value = value
			return
		}
	}
	d.elements = append(d.elements, Element{Key: key, Value: value})
	if d.index != nil {
		d.index[key] = len(d.elements) - 1
	}
}

// Get returns the value for a key.
func (d *Document) Get(key string) (Value, bool) {
	if d.index != nil {
		i, ok := d.index[key]
		if !ok {
			return Value{}, false
		}
		return d.elements[i].Value, true
	}
	for _, e := range d.elements {
		if e.Key == key {
			return e.Value, true
		}
	}
	return Value{}, false
}

// Delete removes a key from the document.
func (d *Document) Delete(key string) {
	if d.index == nil {
		for i := range d.elements {
			if d.elements[i].Key == key {
				d.elements = append(d.elements[:i], d.elements[i+1:]...)
				return
			}
		}
		return
	}
	i, ok := d.index[key]
	if !ok {
		return
	}
	delete(d.index, key)
	d.elements = append(d.elements[:i], d.elements[i+1:]...)
	// Rebuild index for shifted elements
	for j := i; j < len(d.elements); j++ {
		d.index[d.elements[j].Key] = j
	}
}

// Keys returns all keys in order.
func (d *Document) Keys() []string {
	keys := make([]string, len(d.elements))
	for i, e := range d.elements {
		keys[i] = e.Key
	}
	return keys
}

// Elements returns all elements.
func (d *Document) Elements() []Element {
	return d.elements
}

// Len returns the number of elements.
func (d *Document) Len() int {
	return len(d.elements)
}

// Has checks if a key exists.
func (d *Document) Has(key string) bool {
	if d.index != nil {
		_, ok := d.index[key]
		return ok
	}
	for _, e := range d.elements {
		if e.Key == key {
			return true
		}
	}
	return false
}

// Value represents a BSON value with its type.
type Value struct {
	Type  BSONType
	value interface{}
}

// Double returns the value as float64.
func (v Value) Double() float64 {
	if v.Type == TypeDouble {
		return v.value.(float64)
	}
	return 0
}

// String returns the value as string.
func (v Value) String() string {
	if v.Type == TypeString {
		return v.value.(string)
	}
	return ""
}

// Int32 returns the value as int32.
func (v Value) Int32() int32 {
	if v.Type == TypeInt32 {
		return v.value.(int32)
	}
	return 0
}

// Int64 returns the value as int64.
func (v Value) Int64() int64 {
	if v.Type == TypeInt64 {
		return v.value.(int64)
	}
	return 0
}

// Boolean returns the value as bool.
func (v Value) Boolean() bool {
	if v.Type == TypeBoolean {
		return v.value.(bool)
	}
	return false
}

// DateTime returns the value as int64 (milliseconds since epoch).
func (v Value) DateTime() int64 {
	if v.Type == TypeDateTime {
		return v.value.(int64)
	}
	return 0
}

// Timestamp returns the value as uint64.
func (v Value) Timestamp() uint64 {
	if v.Type == TypeTimestamp {
		return v.value.(uint64)
	}
	return 0
}

// ObjectID returns the value as ObjectID.
func (v Value) ObjectID() ObjectID {
	if v.Type == TypeObjectID {
		return v.value.(ObjectID)
	}
	return ObjectID{}
}

// Binary returns the value as Binary.
func (v Value) Binary() Binary {
	if v.Type == TypeBinary {
		return v.value.(Binary)
	}
	return Binary{}
}

// DocumentValue returns the value as *Document.
func (v Value) DocumentValue() *Document {
	if v.Type == TypeDocument {
		return v.value.(*Document)
	}
	return nil
}

// ArrayValue returns the value as Array.
func (v Value) ArrayValue() Array {
	if v.Type == TypeArray {
		return v.value.(Array)
	}
	return nil
}

// Regex returns the value as Regex.
func (v Value) Regex() Regex {
	if v.Type == TypeRegex {
		return v.value.(Regex)
	}
	return Regex{}
}

// JavaScriptCode returns the value as string.
func (v Value) JavaScriptCode() string {
	if v.Type == TypeJavaScript {
		return v.value.(string)
	}
	return ""
}

// CodeScope returns the value as CodeWithScope.
func (v Value) CodeScope() CodeWithScope {
	if v.Type == TypeCodeScope {
		return v.value.(CodeWithScope)
	}
	return CodeWithScope{}
}

// Symbol returns the value as string.
func (v Value) Symbol() string {
	if v.Type == TypeSymbol {
		return v.value.(string)
	}
	return ""
}

// Interface returns the raw interface{} value.
func (v Value) Interface() interface{} {
	return v.value
}

// IsNull returns true if the value is null.
func (v Value) IsNull() bool {
	return v.Type == TypeNull
}

// Binary holds BSON Binary data.
type Binary struct {
	Subtype BinarySubtype
	Data    []byte
}

// Regex holds a BSON Regex value.
type Regex struct {
	Pattern string
	Options string
}

// CodeWithScope holds a BSON JavaScript with scope value.
type CodeWithScope struct {
	Code  string
	Scope *Document
}

// DBPointer holds a BSON DBPointer value.
type DBPointer struct {
	Namespace string
	ID        ObjectID
}

// Array is a slice of Values.
type Array []Value

// String returns a human-readable representation.
func (d *Document) String() string {
	return fmt.Sprintf("%v", d.elements)
}
