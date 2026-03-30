package mongo

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

func testSchema() *bson.Document {
	return bson.D(
		"bsonType", bson.VString("object"),
		"required", bson.VArray(bson.A(bson.VString("name"), bson.VString("email"))),
		"properties", bson.VDoc(bson.D(
			"name", bson.VDoc(bson.D("bsonType", bson.VString("string"), "minLength", bson.VInt32(1))),
			"email", bson.VDoc(bson.D("bsonType", bson.VString("string"), "pattern", bson.VString("^[^@]+@[^@]+$"))),
			"age", bson.VDoc(bson.D("bsonType", bson.VString("int"), "minimum", bson.VInt32(0), "maximum", bson.VInt32(200))),
			"tags", bson.VDoc(bson.D("bsonType", bson.VString("array"), "items", bson.VDoc(bson.D("bsonType", bson.VString("string"))))),
		)),
	)
}

func TestValidationValidDoc(t *testing.T) {
	v := &Validator{Schema: testSchema(), Action: ValidationError, Level: ValidationStrict}

	doc := bson.D(
		"_id", bson.VInt32(1),
		"name", bson.VString("Alice"),
		"email", bson.VString("alice@example.com"),
		"age", bson.VInt32(30),
	)

	if err := v.ValidateDocument(doc); err != nil {
		t.Errorf("valid document should pass: %v", err)
	}
}

func TestValidationMissingRequired(t *testing.T) {
	v := &Validator{Schema: testSchema(), Action: ValidationError, Level: ValidationStrict}

	doc := bson.D(
		"_id", bson.VInt32(1),
		"name", bson.VString("Alice"),
		// missing email
	)

	if err := v.ValidateDocument(doc); err == nil {
		t.Error("expected validation error for missing required field")
	}
}

func TestValidationWrongType(t *testing.T) {
	v := &Validator{Schema: testSchema(), Action: ValidationError, Level: ValidationStrict}

	doc := bson.D(
		"_id", bson.VInt32(1),
		"name", bson.VInt32(42), // should be string
		"email", bson.VString("test@test.com"),
	)

	if err := v.ValidateDocument(doc); err == nil {
		t.Error("expected validation error for wrong type")
	}
}

func TestValidationPatternMismatch(t *testing.T) {
	v := &Validator{Schema: testSchema(), Action: ValidationError, Level: ValidationStrict}

	doc := bson.D(
		"_id", bson.VInt32(1),
		"name", bson.VString("Alice"),
		"email", bson.VString("not-an-email"), // doesn't match pattern
	)

	if err := v.ValidateDocument(doc); err == nil {
		t.Error("expected validation error for pattern mismatch")
	}
}

func TestValidationRangeViolation(t *testing.T) {
	v := &Validator{Schema: testSchema(), Action: ValidationError, Level: ValidationStrict}

	doc := bson.D(
		"_id", bson.VInt32(1),
		"name", bson.VString("Alice"),
		"email", bson.VString("alice@example.com"),
		"age", bson.VInt32(250), // above maximum of 200
	)

	if err := v.ValidateDocument(doc); err == nil {
		t.Error("expected validation error for range violation")
	}
}

func TestValidationMinLengthViolation(t *testing.T) {
	v := &Validator{Schema: testSchema(), Action: ValidationError, Level: ValidationStrict}

	doc := bson.D(
		"_id", bson.VInt32(1),
		"name", bson.VString(""), // minLength is 1
		"email", bson.VString("test@test.com"),
	)

	if err := v.ValidateDocument(doc); err == nil {
		t.Error("expected validation error for minLength")
	}
}

func TestValidationArrayItems(t *testing.T) {
	v := &Validator{Schema: testSchema(), Action: ValidationError, Level: ValidationStrict}

	// Valid array
	doc1 := bson.D(
		"_id", bson.VInt32(1),
		"name", bson.VString("Alice"),
		"email", bson.VString("a@b.com"),
		"tags", bson.VArray(bson.A(bson.VString("go"), bson.VString("db"))),
	)
	if err := v.ValidateDocument(doc1); err != nil {
		t.Errorf("valid array doc should pass: %v", err)
	}

	// Invalid array items
	doc2 := bson.D(
		"_id", bson.VInt32(2),
		"name", bson.VString("Bob"),
		"email", bson.VString("b@c.com"),
		"tags", bson.VArray(bson.A(bson.VInt32(123))), // should be strings
	)
	if err := v.ValidateDocument(doc2); err == nil {
		t.Error("expected validation error for wrong array item type")
	}
}

func TestValidationNoSchema(t *testing.T) {
	v := &Validator{Schema: nil, Action: ValidationError, Level: ValidationStrict}

	doc := bson.D("_id", bson.VInt32(1), "anything", bson.VString("goes"))
	if err := v.ValidateDocument(doc); err != nil {
		t.Error("no schema should mean no validation")
	}
}

func TestIsValidationError(t *testing.T) {
	err := errValidation("test", "test message")
	if !IsValidationError(err) {
		t.Error("should be a validation error")
	}
}

func TestParseValidator(t *testing.T) {
	body := bson.D(
		"create", bson.VString("users"),
		"validator", bson.VDoc(bson.D(
			"bsonType", bson.VString("object"),
			"required", bson.VArray(bson.A(bson.VString("name"))),
		)),
		"validationAction", bson.VString("warn"),
		"validationLevel", bson.VString("moderate"),
	)

	v, err := ParseValidator(body)
	if err != nil {
		t.Fatal(err)
	}
	if v.Action != ValidationWarn {
		t.Errorf("expected action=warn, got %s", v.Action)
	}
	if v.Level != ValidationModerate {
		t.Errorf("expected level=moderate, got %s", v.Level)
	}
	if v.Schema == nil {
		t.Error("expected schema to be set")
	}
}
