package bson

import (
	"fmt"
	"reflect"
	"time"
)

// Unmarshal decodes a BSON Document into a Go struct.
// v must be a pointer to a struct.
func Unmarshal(doc *Document, v interface{}) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("bson: expected non-nil pointer, got %s", rv.Kind())
	}

	rv = rv.Elem()
	if rv.Kind() != reflect.Struct {
		return fmt.Errorf("bson: expected struct pointer, got %s", rv.Kind())
	}

	rt := rv.Type()
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		if !field.IsExported() {
			continue
		}

		name := field.Tag.Get("bson")
		if name == "" || name == ",omitempty" {
			name = field.Name
		}

		val, ok := doc.Get(name)
		if !ok || val.IsNull() {
			continue
		}

		fv := rv.Field(i)
		if err := setField(fv, val); err != nil {
			return fmt.Errorf("field %q: %w", name, err)
		}
	}

	return nil
}

func setField(fv reflect.Value, val Value) error {
	// Dereference pointers
	if fv.Kind() == reflect.Ptr {
		if val.IsNull() {
			fv.Set(reflect.Zero(fv.Type()))
			return nil
		}
		if fv.IsNil() {
			fv.Set(reflect.New(fv.Type().Elem()))
		}
		fv = fv.Elem()
	}

	// Special types
	if fv.Type() == reflect.TypeOf(ObjectID{}) {
		if val.Type == TypeObjectID {
			fv.Set(reflect.ValueOf(val.ObjectID()))
			return nil
		}
		return fmt.Errorf("cannot assign %v to ObjectID", val.Type)
	}
	if fv.Type() == reflect.TypeOf(time.Time{}) {
		if val.Type == TypeDateTime {
			fv.Set(reflect.ValueOf(time.UnixMilli(val.DateTime())))
			return nil
		}
		return fmt.Errorf("cannot assign %v to time.Time", val.Type)
	}

	switch fv.Kind() {
	case reflect.Bool:
		if val.Type == TypeBoolean {
			fv.SetBool(val.Boolean())
			return nil
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch val.Type {
		case TypeInt32:
			fv.SetInt(int64(val.Int32()))
			return nil
		case TypeInt64:
			fv.SetInt(val.Int64())
			return nil
		case TypeDouble:
			fv.SetInt(int64(val.Double()))
			return nil
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		switch val.Type {
		case TypeInt32:
			fv.SetUint(uint64(val.Int32()))
			return nil
		case TypeInt64:
			fv.SetUint(uint64(val.Int64()))
			return nil
		case TypeDouble:
			fv.SetUint(uint64(val.Double()))
			return nil
		}
	case reflect.Float32, reflect.Float64:
		switch val.Type {
		case TypeDouble:
			fv.SetFloat(val.Double())
			return nil
		case TypeInt32:
			fv.SetFloat(float64(val.Int32()))
			return nil
		case TypeInt64:
			fv.SetFloat(float64(val.Int64()))
			return nil
		}
	case reflect.String:
		if val.Type == TypeString {
			fv.SetString(val.String())
			return nil
		}
	case reflect.Slice:
		if fv.Type().Elem().Kind() == reflect.Uint8 && val.Type == TypeBinary {
			fv.SetBytes(val.Binary().Data)
			return nil
		}
		if val.Type == TypeArray {
			arr := val.ArrayValue()
			slice := reflect.MakeSlice(fv.Type(), len(arr), len(arr))
			for i, elem := range arr {
				if err := setField(slice.Index(i), elem); err != nil {
					return err
				}
			}
			fv.Set(slice)
			return nil
		}
	case reflect.Map:
		if val.Type == TypeDocument {
			innerDoc := val.DocumentValue()
			mapVal := reflect.MakeMapWithSize(fv.Type(), innerDoc.Len())
			for _, e := range innerDoc.Elements() {
				keyVal := reflect.ValueOf(e.Key)
				fieldType := fv.Type().Elem()
				fieldPtr := reflect.New(fieldType)
				if err := setField(fieldPtr.Elem(), e.Value); err != nil {
					return err
				}
				mapVal.SetMapIndex(keyVal, fieldPtr.Elem())
			}
			fv.Set(mapVal)
			return nil
		}
	case reflect.Struct:
		if val.Type == TypeDocument {
			return Unmarshal(val.DocumentValue(), fv.Addr().Interface())
		}
	case reflect.Interface:
		fv.Set(reflect.ValueOf(val.Interface()))
		return nil
	}

	return fmt.Errorf("cannot assign %v to %s", val.Type, fv.Type())
}
