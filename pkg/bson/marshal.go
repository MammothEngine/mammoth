package bson

import (
	"fmt"
	"reflect"
	"time"
)

// Marshal converts a Go struct to a BSON Document.
func Marshal(v interface{}) (*Document, error) {
	return marshalValue(reflect.ValueOf(v))
}

func marshalValue(v reflect.Value) (*Document, error) {
	// Dereference pointers
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return NewDocument(), nil
		}
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("bson: expected struct, got %s", v.Kind())
	}

	doc := NewDocument()
	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldValue := v.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		name := field.Tag.Get("bson")
		if name == "" {
			name = field.Name
		}

		// Handle omitempty
		omitempty := false
		if len(name) > 0 && name[len(name)-1] == ',' {
			// Parse tag options
		}
		if name == ",omitempty" {
			name = field.Name
			omitempty = true
		}

		if omitempty && isEmptyValue(fieldValue) {
			continue
		}

		bsonVal, err := goValueToBSON(fieldValue)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", name, err)
		}

		doc.Set(name, bsonVal)
	}

	return doc, nil
}

func goValueToBSON(v reflect.Value) (Value, error) {
	// Dereference pointers
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return VNull(), nil
		}
		v = v.Elem()
	}

	// Check for special types
	if v.Type() == reflect.TypeOf(ObjectID{}) {
		return Value{Type: TypeObjectID, value: v.Interface().(ObjectID)}, nil
	}
	if v.Type() == reflect.TypeOf(time.Time{}) {
		t := v.Interface().(time.Time)
		return VDateTime(t.UnixMilli()), nil
	}

	switch v.Kind() {
	case reflect.Bool:
		return VBool(v.Bool()), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		return VInt32(int32(v.Int())), nil
	case reflect.Int64:
		return VInt64(v.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return VInt32(int32(v.Uint())), nil
	case reflect.Uint64:
		return VInt64(int64(v.Uint())), nil
	case reflect.Float32, reflect.Float64:
		return VDouble(v.Float()), nil
	case reflect.String:
		return VString(v.String()), nil
	case reflect.Slice:
		if v.IsNil() {
			return VNull(), nil
		}
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return Value{Type: TypeBinary, value: Binary{Subtype: BinaryGeneric, Data: v.Bytes()}}, nil
		}
		arr := make(Array, v.Len())
		for i := 0; i < v.Len(); i++ {
			elem, err := goValueToBSON(v.Index(i))
			if err != nil {
				return Value{}, err
			}
			arr[i] = elem
		}
		return Value{Type: TypeArray, value: arr}, nil
	case reflect.Map:
		if v.IsNil() {
			return VNull(), nil
		}
		doc := NewDocument()
		iter := v.MapRange()
		for iter.Next() {
			key := iter.Key().String()
			elem, err := goValueToBSON(iter.Value())
			if err != nil {
				return Value{}, err
			}
			doc.Set(key, elem)
		}
		return Value{Type: TypeDocument, value: doc}, nil
	case reflect.Struct:
		doc, err := marshalValue(v)
		if err != nil {
			return Value{}, err
		}
		return Value{Type: TypeDocument, value: doc}, nil
	case reflect.Interface:
		if v.IsNil() {
			return VNull(), nil
		}
		return goValueToBSON(v.Elem())
	}

	return VNull(), nil
}

func isEmptyValue(v reflect.Value) bool {
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return true
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.String:
		return v.String() == ""
	case reflect.Slice, reflect.Map:
		return v.IsNil() || v.Len() == 0
	case reflect.Ptr:
		return v.IsNil()
	}
	return false
}
