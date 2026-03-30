package mongo

import (
	"regexp"
	"strings"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// Validator holds JSON Schema validation rules for a collection.
type Validator struct {
	Schema    *bson.Document `json:"schema"`
	Action    string         `json:"action"`    // "error" (default) or "warn"
	Level     string         `json:"level"`     // "strict" (default) or "moderate"
}

// ValidationAction constants
const (
	ValidationError = "error"
	ValidationWarn  = "warn"
)

// ValidationLevel constants
const (
	ValidationStrict   = "strict"
	ValidationModerate = "moderate"
)

// ValidateDocument validates a document against the schema.
// Returns nil if valid, or an error description if invalid.
func (v *Validator) ValidateDocument(doc *bson.Document) error {
	if v.Schema == nil {
		return nil
	}
	return validateSchema(doc, v.Schema)
}

func validateSchema(doc *bson.Document, schema *bson.Document) error {
	// Check bsonType
	if bt, ok := schema.Get("bsonType"); ok && bt.Type == bson.TypeString {
		if !checkBsonType(doc, bt.String()) {
			return errValidation("bsonType", "expected "+bt.String())
		}
	}

	// Check required fields
	if req, ok := schema.Get("required"); ok && req.Type == bson.TypeArray {
		for _, r := range req.ArrayValue() {
			if r.Type == bson.TypeString {
				if _, found := doc.Get(r.String()); !found {
					return errValidation("required", "missing required field: "+r.String())
				}
			}
		}
	}

	// Check properties
	if props, ok := schema.Get("properties"); ok && props.Type == bson.TypeDocument {
		for _, e := range props.DocumentValue().Elements() {
			fieldName := e.Key
			fieldSchema := e.Value
			if fieldSchema.Type != bson.TypeDocument {
				continue
			}
			fieldVal, fieldFound := doc.Get(fieldName)
			if !fieldFound {
				continue // missing optional fields are OK
			}
			if err := validateField(fieldName, fieldVal, fieldSchema.DocumentValue()); err != nil {
				return err
			}
		}
	}

	return nil
}

func validateField(name string, val bson.Value, schema *bson.Document) error {
	// Check bsonType
	if bt, ok := schema.Get("bsonType"); ok && bt.Type == bson.TypeString {
		if !checkValueBsonType(val, bt.String()) {
			return errValidation("bsonType", name+": expected "+bt.String())
		}
	}

	// Check enum
	if enum, ok := schema.Get("enum"); ok && enum.Type == bson.TypeArray {
		found := false
		for _, e := range enum.ArrayValue() {
			if bson.CompareValues(val, e) == 0 {
				found = true
				break
			}
		}
		if !found {
			return errValidation("enum", name+": value not in enum")
		}
	}

	// String validations
	if val.Type == bson.TypeString {
		s := val.String()
		if ml, ok := schema.Get("minLength"); ok {
			if minLen := toInt(ml); len(s) < minLen {
				return errValidation("minLength", name+": string too short")
			}
		}
		if mxl, ok := schema.Get("maxLength"); ok {
			if maxLen := toInt(mxl); len(s) > maxLen {
				return errValidation("maxLength", name+": string too long")
			}
		}
		if pat, ok := schema.Get("pattern"); ok && pat.Type == bson.TypeString {
			re, err := regexp.Compile(pat.String())
			if err == nil && !re.MatchString(s) {
				return errValidation("pattern", name+": does not match pattern")
			}
		}
	}

	// Number validations
	if val.Type == bson.TypeInt32 || val.Type == bson.TypeInt64 || val.Type == bson.TypeDouble {
		f := toFloat64(val)
		if min, ok := schema.Get("minimum"); ok {
			if f < toFloat64(min) {
				return errValidation("minimum", name+": value below minimum")
			}
		}
		if max, ok := schema.Get("maximum"); ok {
			if f > toFloat64(max) {
				return errValidation("maximum", name+": value above maximum")
			}
		}
		if emin, ok := schema.Get("exclusiveMinimum"); ok {
			if f <= toFloat64(emin) {
				return errValidation("exclusiveMinimum", name+": value not above exclusive minimum")
			}
		}
		if emax, ok := schema.Get("exclusiveMaximum"); ok {
			if f >= toFloat64(emax) {
				return errValidation("exclusiveMaximum", name+": value not below exclusive maximum")
			}
		}
	}

	// Array validations
	if val.Type == bson.TypeArray {
		arr := val.ArrayValue()
		if minItems, ok := schema.Get("minItems"); ok {
			if len(arr) < toInt(minItems) {
				return errValidation("minItems", name+": array too small")
			}
		}
		if maxItems, ok := schema.Get("maxItems"); ok {
			if len(arr) > toInt(maxItems) {
				return errValidation("maxItems", name+": array too large")
			}
		}
		// items: validate each element
		if items, ok := schema.Get("items"); ok && items.Type == bson.TypeDocument {
			for i, elem := range arr {
				if err := validateField(name+"[]", elem, items.DocumentValue()); err != nil {
					return errValidation("items", name+"["+int32ToStr(int32(i))+"]: "+err.Error())
				}
			}
		}
	}

	// Object validations (nested)
	if val.Type == bson.TypeDocument {
		nestedDoc := val.DocumentValue()
		if props, ok := schema.Get("properties"); ok && props.Type == bson.TypeDocument {
			for _, e := range props.DocumentValue().Elements() {
				nestedVal, nestedFound := nestedDoc.Get(e.Key)
				if !nestedFound {
					continue
				}
				if e.Value.Type == bson.TypeDocument {
					if err := validateField(name+"."+e.Key, nestedVal, e.Value.DocumentValue()); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

func checkBsonType(doc *bson.Document, typeName string) bool {
	// Documents are type "object"
	return typeName == "object"
}

func checkValueBsonType(val bson.Value, typeName string) bool {
	switch typeName {
	case "string":
		return val.Type == bson.TypeString
	case "int":
		return val.Type == bson.TypeInt32
	case "long":
		return val.Type == bson.TypeInt64
	case "double", "number":
		return val.Type == bson.TypeDouble || val.Type == bson.TypeInt32 || val.Type == bson.TypeInt64
	case "bool":
		return val.Type == bson.TypeBoolean
	case "object":
		return val.Type == bson.TypeDocument
	case "array":
		return val.Type == bson.TypeArray
	case "null":
		return val.Type == bson.TypeNull
	case "objectId":
		return val.Type == bson.TypeObjectID
	case "date":
		return val.Type == bson.TypeDateTime
	default:
		return true
	}
}

func toInt(v bson.Value) int {
	switch v.Type {
	case bson.TypeInt32:
		return int(v.Int32())
	case bson.TypeInt64:
		return int(v.Int64())
	case bson.TypeDouble:
		return int(v.Double())
	default:
		return 0
	}
}

func toFloat64(v bson.Value) float64 {
	switch v.Type {
	case bson.TypeInt32:
		return float64(v.Int32())
	case bson.TypeInt64:
		return float64(v.Int64())
	case bson.TypeDouble:
		return v.Double()
	default:
		return 0
	}
}

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

type validationError struct {
	keyword string
	message string
}

func (e validationError) Error() string {
	return e.message
}

func errValidation(keyword, msg string) error {
	return validationError{keyword: keyword, message: msg}
}

// IsValidationError checks if an error is a validation error.
func IsValidationError(err error) bool {
	_, ok := err.(validationError)
	return ok
}

// ParseValidator parses validation options from a create/collMod command.
func ParseValidator(body *bson.Document) (*Validator, error) {
	v := &Validator{
		Action: ValidationError,
		Level:  ValidationStrict,
	}

	if vs, ok := body.Get("validator"); ok && vs.Type == bson.TypeDocument {
		v.Schema = vs.DocumentValue()
	}

	if action, ok := body.Get("validationAction"); ok && action.Type == bson.TypeString {
		a := action.String()
		if a == ValidationWarn || a == ValidationError {
			v.Action = a
		}
	}

	if level, ok := body.Get("validationLevel"); ok && level.Type == bson.TypeString {
		l := level.String()
		if l == ValidationStrict || l == ValidationModerate {
			v.Level = l
		}
	}

	return v, nil
}

// resolveField is an alias for ResolveField used in validation.
func resolveFieldPath(doc *bson.Document, path string) (bson.Value, bool) {
	parts := strings.SplitN(path, ".", 2)
	v, ok := doc.Get(parts[0])
	if !ok {
		return bson.Value{}, false
	}
	if len(parts) == 1 {
		return v, true
	}
	if v.Type == bson.TypeDocument {
		return resolveFieldPath(v.DocumentValue(), parts[1])
	}
	return bson.Value{}, false
}
