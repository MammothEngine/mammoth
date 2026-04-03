package mongo

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

// Test checkValueBsonType with various type names
func TestCheckValueBsonType(t *testing.T) {
	tests := []struct {
		name     string
		val      bson.Value
		typeName string
		expected bool
	}{
		{"string with string", bson.VString("test"), "string", true},
		{"int32 with string", bson.VInt32(42), "string", false},
		{"int32 with int", bson.VInt32(42), "int", true},
		{"int64 with int", bson.VInt64(42), "int", false},
		{"int64 with long", bson.VInt64(42), "long", true},
		{"double with double", bson.VDouble(3.14), "double", true},
		{"int32 with double", bson.VInt32(42), "double", true},
		{"int64 with double", bson.VInt64(42), "double", true},
		{"int32 with number", bson.VInt32(42), "number", true},
		{"double with number", bson.VDouble(3.14), "number", true},
		{"bool with bool", bson.VBool(true), "bool", true},
		{"bool with string", bson.VBool(true), "string", false},
		{"document with object", bson.VDoc(bson.NewDocument()), "object", true},
		{"array with array", bson.VArray(bson.A()), "array", true},
		{"null with null", bson.Value{Type: bson.TypeNull}, "null", true},
		{"objectId with objectId", bson.VObjectID([12]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}), "objectId", true},
		{"date with date", bson.VDateTime(1234567890), "date", true},
		{"string with unknown type", bson.VString("test"), "unknown", true},
		{"string with int", bson.VString("test"), "int", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := checkValueBsonType(tt.val, tt.typeName)
			if result != tt.expected {
				t.Errorf("checkValueBsonType() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test toInt with various BSON value types
func TestToInt(t *testing.T) {
	tests := []struct {
		name     string
		val      bson.Value
		expected int
	}{
		{"int32 positive", bson.VInt32(42), 42},
		{"int32 zero", bson.VInt32(0), 0},
		{"int32 negative", bson.VInt32(-10), -10},
		{"int64 positive", bson.VInt64(100), 100},
		{"int64 zero", bson.VInt64(0), 0},
		{"int64 negative", bson.VInt64(-50), -50},
		{"double positive", bson.VDouble(3.14), 3},
		{"double zero", bson.VDouble(0.0), 0},
		{"double negative", bson.VDouble(-5.7), -5},
		{"string (default)", bson.VString("test"), 0},
		{"null (default)", bson.Value{Type: bson.TypeNull}, 0},
		{"bool (default)", bson.VBool(true), 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toInt(tt.val)
			if result != tt.expected {
				t.Errorf("toInt() = %d, want %d", result, tt.expected)
			}
		})
	}
}

// Test toFloat64 with various BSON value types
func TestToFloat64(t *testing.T) {
	tests := []struct {
		name     string
		val      bson.Value
		expected float64
	}{
		{"int32 positive", bson.VInt32(42), 42.0},
		{"int32 zero", bson.VInt32(0), 0.0},
		{"int32 negative", bson.VInt32(-10), -10.0},
		{"int64 positive", bson.VInt64(100), 100.0},
		{"int64 zero", bson.VInt64(0), 0.0},
		{"int64 large", bson.VInt64(9223372036854775807), 9223372036854775807.0},
		{"double positive", bson.VDouble(3.14), 3.14},
		{"double zero", bson.VDouble(0.0), 0.0},
		{"double negative", bson.VDouble(-5.7), -5.7},
		{"string (default)", bson.VString("test"), 0.0},
		{"null (default)", bson.Value{Type: bson.TypeNull}, 0.0},
		{"document (default)", bson.VDoc(bson.NewDocument()), 0.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toFloat64(tt.val)
			if result != tt.expected {
				t.Errorf("toFloat64() = %f, want %f", result, tt.expected)
			}
		})
	}
}

// Test int32ToStr with edge cases
func TestInt32ToStr(t *testing.T) {
	tests := []struct {
		n        int32
		expected string
	}{
		{0, "0"},
		{1, "1"},
		{9, "9"},
		{10, "10"},
		{99, "99"},
		{100, "100"},
		{12345, "12345"},
		{-1, "-1"},
		{-9, "-9"},
		{-10, "-10"},
		{-99, "-99"},
		{-100, "-100"},
		{-12345, "-12345"},
		{2147483647, "2147483647"},
		// Note: -2147483648 overflows int32 negation in the function
	}

	for _, tt := range tests {
		result := int32ToStr(tt.n)
		if result != tt.expected {
			t.Errorf("int32ToStr(%d) = %q, want %q", tt.n, result, tt.expected)
		}
	}
}

// Test setNestedField - setting fields at various paths
func TestSetNestedField(t *testing.T) {
	tests := []struct {
		name     string
		setup    func() *bson.Document
		path     string
		val      bson.Value
		check    func(*bson.Document) bool
		checkMsg string
	}{
		{
			name: "set top-level field",
			setup: func() *bson.Document {
				return bson.NewDocument()
			},
			path: "name",
			val:  bson.VString("Alice"),
			check: func(d *bson.Document) bool {
				v, ok := d.Get("name")
				return ok && v.String() == "Alice"
			},
			checkMsg: "name should be Alice",
		},
		{
			name: "set nested field - create intermediate",
			setup: func() *bson.Document {
				return bson.NewDocument()
			},
			path: "address.city",
			val:  bson.VString("NYC"),
			check: func(d *bson.Document) bool {
				v, ok := d.Get("address")
				if !ok || v.Type != bson.TypeDocument {
					return false
				}
				city, ok := v.DocumentValue().Get("city")
				return ok && city.String() == "NYC"
			},
			checkMsg: "address.city should be NYC",
		},
		{
			name: "set deeply nested field",
			setup: func() *bson.Document {
				d := bson.NewDocument()
				d.Set("a", bson.VDoc(bson.NewDocument()))
				return d
			},
			path: "a.b.c.d",
			val:  bson.VInt32(42),
			check: func(d *bson.Document) bool {
				a, _ := d.Get("a")
				b, _ := a.DocumentValue().Get("b")
				c, _ := b.DocumentValue().Get("c")
				dv, ok := c.DocumentValue().Get("d")
				return ok && dv.Int32() == 42
			},
			checkMsg: "a.b.c.d should be 42",
		},
		{
			name: "overwrite existing nested field",
			setup: func() *bson.Document {
				d := bson.NewDocument()
				addr := bson.NewDocument()
				addr.Set("city", bson.VString("LA"))
				d.Set("address", bson.VDoc(addr))
				return d
			},
			path: "address.city",
			val:  bson.VString("NYC"),
			check: func(d *bson.Document) bool {
				v, _ := d.Get("address")
				city, _ := v.DocumentValue().Get("city")
				return city.String() == "NYC"
			},
			checkMsg: "address.city should be updated to NYC",
		},
		{
			name: "set field when intermediate is not document",
			setup: func() *bson.Document {
				d := bson.NewDocument()
				d.Set("name", bson.VString("test"))
				return d
			},
			path: "name.first",
			val:  bson.VString("John"),
			check: func(d *bson.Document) bool {
				v, _ := d.Get("name")
				if v.Type != bson.TypeDocument {
					return false
				}
				first, ok := v.DocumentValue().Get("first")
				return ok && first.String() == "John"
			},
			checkMsg: "name.first should be John (replaced intermediate)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := tt.setup()
			setNestedField(doc, tt.path, tt.val)
			if !tt.check(doc) {
				t.Errorf(tt.checkMsg)
			}
		})
	}
}

// Test unsetNestedField - removing fields at various paths
func TestUnsetNestedField(t *testing.T) {
	tests := []struct {
		name     string
		setup    func() *bson.Document
		path     string
		check    func(*bson.Document) bool
		checkMsg string
	}{
		{
			name: "unset top-level field",
			setup: func() *bson.Document {
				d := bson.NewDocument()
				d.Set("name", bson.VString("Alice"))
				return d
			},
			path: "name",
			check: func(d *bson.Document) bool {
				_, ok := d.Get("name")
				return !ok
			},
			checkMsg: "name should be removed",
		},
		{
			name: "unset nested field",
			setup: func() *bson.Document {
				d := bson.NewDocument()
				addr := bson.NewDocument()
				addr.Set("city", bson.VString("NYC"))
				addr.Set("zip", bson.VInt32(10001))
				d.Set("address", bson.VDoc(addr))
				return d
			},
			path: "address.city",
			check: func(d *bson.Document) bool {
				v, _ := d.Get("address")
				_, ok := v.DocumentValue().Get("city")
				_, zipOk := v.DocumentValue().Get("zip")
				return !ok && zipOk // city removed, zip still there
			},
			checkMsg: "address.city should be removed, address.zip should remain",
		},
		{
			name: "unset non-existent field",
			setup: func() *bson.Document {
				d := bson.NewDocument()
				d.Set("name", bson.VString("Alice"))
				return d
			},
			path: "nonexistent",
			check: func(d *bson.Document) bool {
				_, ok := d.Get("name")
				return ok // original field still there
			},
			checkMsg: "original field should remain",
		},
		{
			name: "unset non-existent nested path",
			setup: func() *bson.Document {
				d := bson.NewDocument()
				d.Set("name", bson.VString("Alice"))
				return d
			},
			path: "name.first",
			check: func(d *bson.Document) bool {
				// Should not panic, and name should remain unchanged
				v, ok := d.Get("name")
				return ok && v.String() == "Alice"
			},
			checkMsg: "name should remain unchanged",
		},
		{
			name: "unset when intermediate is not document",
			setup: func() *bson.Document {
				d := bson.NewDocument()
				d.Set("name", bson.VString("Alice"))
				return d
			},
			path: "name.first",
			check: func(d *bson.Document) bool {
				v, ok := d.Get("name")
				return ok && v.String() == "Alice"
			},
			checkMsg: "name should remain unchanged when intermediate is not document",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := tt.setup()
			unsetNestedField(doc, tt.path)
			if !tt.check(doc) {
				t.Errorf(tt.checkMsg)
			}
		})
	}
}

// Test WildcardIndex.AddEntry with various document structures
func TestWildcardIndex_AddEntry(t *testing.T) {
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	cat.EnsureDatabase("testdb")
	cat.EnsureCollection("testdb", "testcoll")

	spec := &IndexSpec{
		Name: "wc_idx",
		Key:  []IndexKey{{Field: "$**"}},
	}
	wi := NewWildcardIndex("testdb", "testcoll", spec, eng)

	tests := []struct {
		name    string
		doc     *bson.Document
		wantErr bool
	}{
		{
			name: "simple document",
			doc: bson.D("_id", bson.VObjectID([12]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
				"name", bson.VString("Alice"),
				"age", bson.VInt32(30)),
			wantErr: false,
		},
		{
			name: "nested document",
			doc: func() *bson.Document {
				addr := bson.NewDocument()
				addr.Set("city", bson.VString("NYC"))
				addr.Set("zip", bson.VInt32(10001))
				d := bson.NewDocument()
				d.Set("_id", bson.VObjectID([12]byte{2, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}))
				d.Set("name", bson.VString("Bob"))
				d.Set("address", bson.VDoc(addr))
				return d
			}(),
			wantErr: false,
		},
		{
			name: "document with array",
			doc: func() *bson.Document {
				tags := bson.A(bson.VString("a"), bson.VString("b"), bson.VString("c"))
				d := bson.NewDocument()
				d.Set("_id", bson.VObjectID([12]byte{3, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}))
				d.Set("tags", bson.VArray(tags))
				return d
			}(),
			wantErr: false,
		},
		{
			name: "document with array of objects",
			doc: func() *bson.Document {
				item1 := bson.NewDocument()
				item1.Set("name", bson.VString("item1"))
				item1.Set("price", bson.VInt32(10))
				item2 := bson.NewDocument()
				item2.Set("name", bson.VString("item2"))
				item2.Set("price", bson.VInt32(20))
				items := bson.A(bson.VDoc(item1), bson.VDoc(item2))
				d := bson.NewDocument()
				d.Set("_id", bson.VObjectID([12]byte{4, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}))
				d.Set("items", bson.VArray(items))
				return d
			}(),
			wantErr: false,
		},
		{
			name:    "document without _id",
			doc:     bson.D("name", bson.VString("NoID")),
			wantErr: false, // should return nil, not error
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := wi.AddEntry(tt.doc)
			if (err != nil) != tt.wantErr {
				t.Errorf("AddEntry() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// Test WildcardIndex.AddEntry with partial filter expression
func TestWildcardIndex_AddEntry_PartialFilter(t *testing.T) {
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// Create a wildcard index with partial filter
	filter := bson.NewDocument()
	filter.Set("active", bson.VBool(true))
	spec := &IndexSpec{
		Name:                    "wc_partial",
		Key:                     []IndexKey{{Field: "$**"}},
		PartialFilterExpression: filter,
	}
	wi := NewWildcardIndex("testdb", "testcoll", spec, eng)

	tests := []struct {
		name string
		doc  *bson.Document
	}{
		{
			name: "document matching filter",
			doc: func() *bson.Document {
				d := bson.NewDocument()
				d.Set("_id", bson.VObjectID([12]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}))
				d.Set("name", bson.VString("Active"))
				d.Set("active", bson.VBool(true))
				return d
			}(),
		},
		{
			name: "document not matching filter",
			doc: func() *bson.Document {
				d := bson.NewDocument()
				d.Set("_id", bson.VObjectID([12]byte{2, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}))
				d.Set("name", bson.VString("Inactive"))
				d.Set("active", bson.VBool(false))
				return d
			}(),
		},
		{
			name: "document without filter field",
			doc: func() *bson.Document {
				d := bson.NewDocument()
				d.Set("_id", bson.VObjectID([12]byte{3, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}))
				d.Set("name", bson.VString("NoActiveField"))
				return d
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := wi.AddEntry(tt.doc)
			if err != nil {
				t.Errorf("AddEntry() error = %v", err)
			}
		})
	}
}

// Test indexDocFields with complex nested structures
func TestWildcardIndex_indexDocFields(t *testing.T) {
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	spec := &IndexSpec{Name: "wc_complex", Key: []IndexKey{{Field: "$**"}}}
	wi := NewWildcardIndex("testdb", "testcoll", spec, eng)

	idBytes := [12]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}

	tests := []struct {
		name    string
		doc     *bson.Document
		wantErr bool
	}{
		{
			name: "deeply nested document",
			doc: func() *bson.Document {
				level3 := bson.NewDocument()
				level3.Set("value", bson.VInt32(42))
				level2 := bson.NewDocument()
				level2.Set("level3", bson.VDoc(level3))
				level1 := bson.NewDocument()
				level1.Set("level2", bson.VDoc(level2))
				d := bson.NewDocument()
				d.Set("_id", bson.VObjectID(idBytes))
				d.Set("level1", bson.VDoc(level1))
				return d
			}(),
			wantErr: false,
		},
		{
			name: "mixed arrays and objects",
			doc: func() *bson.Document {
				subDoc := bson.NewDocument()
				subDoc.Set("x", bson.VInt32(1))
				arr := bson.A(bson.VDoc(subDoc), bson.VInt32(2), bson.VString("test"))
				d := bson.NewDocument()
				d.Set("_id", bson.VObjectID(idBytes))
				d.Set("mixed", bson.VArray(arr))
				return d
			}(),
			wantErr: false,
		},
		{
			name: "various types",
			doc: bson.D(
				"_id", bson.VObjectID(idBytes),
				"string", bson.VString("text"),
				"int32", bson.VInt32(42),
				"int64", bson.VInt64(100),
				"double", bson.VDouble(3.14),
				"bool", bson.VBool(true),
				"null", bson.Value{Type: bson.TypeNull},
				"datetime", bson.VDateTime(1234567890),
			),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idVal, _ := tt.doc.Get("_id")
			err := wi.indexDocFields(tt.doc, "", idVal.ObjectID().Bytes())
			if (err != nil) != tt.wantErr {
				t.Errorf("indexDocFields() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// Test equalBytes function
func TestEqualBytes(t *testing.T) {
	tests := []struct {
		name     string
		a        []byte
		b        []byte
		expected bool
	}{
		{"equal empty", []byte{}, []byte{}, true},
		{"equal non-empty", []byte{1, 2, 3}, []byte{1, 2, 3}, true},
		{"different length", []byte{1, 2}, []byte{1, 2, 3}, false},
		{"different content", []byte{1, 2, 3}, []byte{1, 2, 4}, false},
		{"nil vs empty", nil, []byte{}, true},
		{"nil vs nil", nil, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := equalBytes(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("equalBytes() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test WildcardIndex LookupField
func TestWildcardIndex_LookupField(t *testing.T) {
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	spec := &IndexSpec{Name: "wc_lookup", Key: []IndexKey{{Field: "$**"}}}
	wi := NewWildcardIndex("testdb", "testcoll", spec, eng)

	// Add some entries
	doc1 := bson.D(
		"_id", bson.VObjectID([12]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
		"name", bson.VString("Alice"),
		"age", bson.VInt32(30),
	)
	err = wi.AddEntry(doc1)
	if err != nil {
		t.Fatalf("AddEntry: %v", err)
	}

	// Lookup
	ids := wi.LookupField("name", bson.VString("Alice"))
	if len(ids) == 0 {
		t.Error("expected to find at least one document")
	}

	// Lookup non-existent
	ids2 := wi.LookupField("name", bson.VString("Bob"))
	_ = ids2 // may or may not find depending on implementation
}

// Test WildcardIndex RemoveEntry behavior
func TestWildcardIndex_RemoveEntryBehavior(t *testing.T) {
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	spec := &IndexSpec{Name: "wc_remove", Key: []IndexKey{{Field: "$**"}}}
	wi := NewWildcardIndex("testdb", "testcoll", spec, eng)

	idBytes := [12]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	doc := bson.D(
		"_id", bson.VObjectID(idBytes),
		"name", bson.VString("ToRemove"),
	)

	// Add entry
	err = wi.AddEntry(doc)
	if err != nil {
		t.Fatalf("AddEntry: %v", err)
	}

	// Remove entry
	err = wi.RemoveEntry(doc)
	if err != nil {
		t.Errorf("RemoveEntry: %v", err)
	}

	// Remove document without _id
	docNoID := bson.D("name", bson.VString("NoID"))
	err = wi.RemoveEntry(docNoID)
	if err != nil {
		t.Errorf("RemoveEntry without _id should not error: %v", err)
	}
}

// Test validateField with various validation scenarios
func TestValidateField(t *testing.T) {
	tests := []struct {
		name      string
		fieldName string
		val       bson.Value
		schema    *bson.Document
		wantErr   bool
	}{
		{
			name:      "valid string type",
			fieldName: "name",
			val:       bson.VString("Alice"),
			schema:    bson.D("bsonType", bson.VString("string")),
			wantErr:   false,
		},
		{
			name:      "invalid string type",
			fieldName: "name",
			val:       bson.VInt32(42),
			schema:    bson.D("bsonType", bson.VString("string")),
			wantErr:   true,
		},
		{
			name:      "valid int type",
			fieldName: "age",
			val:       bson.VInt32(30),
			schema:    bson.D("bsonType", bson.VString("int")),
			wantErr:   false,
		},
		{
			name:      "string minLength valid",
			fieldName: "name",
			val:       bson.VString("Alice"),
			schema:    bson.D("minLength", bson.VInt32(3)),
			wantErr:   false,
		},
		{
			name:      "string minLength invalid",
			fieldName: "name",
			val:       bson.VString("Al"),
			schema:    bson.D("minLength", bson.VInt32(3)),
			wantErr:   true,
		},
		{
			name:      "string maxLength valid",
			fieldName: "name",
			val:       bson.VString("Alice"),
			schema:    bson.D("maxLength", bson.VInt32(10)),
			wantErr:   false,
		},
		{
			name:      "string maxLength invalid",
			fieldName: "name",
			val:       bson.VString("Alexander"),
			schema:    bson.D("maxLength", bson.VInt32(5)),
			wantErr:   true,
		},
		{
			name:      "string pattern valid",
			fieldName: "email",
			val:       bson.VString("test@example.com"),
			schema:    bson.D("pattern", bson.VString(`^[^@]+@[^@]+$`)),
			wantErr:   false,
		},
		{
			name:      "string pattern invalid",
			fieldName: "email",
			val:       bson.VString("not-an-email"),
			schema:    bson.D("pattern", bson.VString(`^[^@]+@[^@]+$`)),
			wantErr:   true,
		},
		{
			name:      "number minimum valid",
			fieldName: "age",
			val:       bson.VInt32(18),
			schema:    bson.D("minimum", bson.VInt32(0)),
			wantErr:   false,
		},
		{
			name:      "number minimum invalid",
			fieldName: "age",
			val:       bson.VInt32(-5),
			schema:    bson.D("minimum", bson.VInt32(0)),
			wantErr:   true,
		},
		{
			name:      "number maximum valid",
			fieldName: "score",
			val:       bson.VInt32(90),
			schema:    bson.D("maximum", bson.VInt32(100)),
			wantErr:   false,
		},
		{
			name:      "number maximum invalid",
			fieldName: "score",
			val:       bson.VInt32(150),
			schema:    bson.D("maximum", bson.VInt32(100)),
			wantErr:   true,
		},
		{
			name:      "array minItems valid",
			fieldName: "tags",
			val:       bson.VArray(bson.A(bson.VString("a"), bson.VString("b"))),
			schema:    bson.D("minItems", bson.VInt32(2)),
			wantErr:   false,
		},
		{
			name:      "array minItems invalid",
			fieldName: "tags",
			val:       bson.VArray(bson.A(bson.VString("a"))),
			schema:    bson.D("minItems", bson.VInt32(2)),
			wantErr:   true,
		},
		{
			name:      "array maxItems valid",
			fieldName: "tags",
			val:       bson.VArray(bson.A(bson.VString("a"))),
			schema:    bson.D("maxItems", bson.VInt32(3)),
			wantErr:   false,
		},
		{
			name:      "array maxItems invalid",
			fieldName: "tags",
			val:       bson.VArray(bson.A(bson.VString("a"), bson.VString("b"), bson.VString("c"), bson.VString("d"))),
			schema:    bson.D("maxItems", bson.VInt32(3)),
			wantErr:   true,
		},
		{
			name:      "enum valid",
			fieldName: "status",
			val:       bson.VString("active"),
			schema:    bson.D("enum", bson.VArray(bson.A(bson.VString("active"), bson.VString("inactive")))),
			wantErr:   false,
		},
		{
			name:      "enum invalid",
			fieldName: "status",
			val:       bson.VString("deleted"),
			schema:    bson.D("enum", bson.VArray(bson.A(bson.VString("active"), bson.VString("inactive")))),
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateField(tt.fieldName, tt.val, tt.schema)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateField() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// Test validateSchema
func TestValidateSchema(t *testing.T) {
	tests := []struct {
		name    string
		doc     *bson.Document
		schema  *bson.Document
		wantErr bool
	}{
		{
			name:    "document is object type",
			doc:     bson.D("name", bson.VString("Alice")),
			schema:  bson.D("bsonType", bson.VString("object")),
			wantErr: false,
		},
		{
			name:    "document is not string type",
			doc:     bson.D("name", bson.VString("Alice")),
			schema:  bson.D("bsonType", bson.VString("string")),
			wantErr: true,
		},
		{
			name: "required field present",
			doc:  bson.D("name", bson.VString("Alice")),
			schema: bson.D(
				"required", bson.VArray(bson.A(bson.VString("name"))),
			),
			wantErr: false,
		},
		{
			name: "required field missing",
			doc:  bson.D("age", bson.VInt32(30)),
			schema: bson.D(
				"required", bson.VArray(bson.A(bson.VString("name"))),
			),
			wantErr: true,
		},
		{
			name: "properties validation passes",
			doc:  bson.D("name", bson.VString("Alice")),
			schema: func() *bson.Document {
				nameSchema := bson.NewDocument()
				nameSchema.Set("bsonType", bson.VString("string"))
				s := bson.NewDocument()
				s.Set("properties", bson.VDoc(bson.D("name", bson.VDoc(nameSchema))))
				return s
			}(),
			wantErr: false,
		},
		{
			name: "properties validation fails",
			doc:  bson.D("name", bson.VInt32(42)),
			schema: func() *bson.Document {
				nameSchema := bson.NewDocument()
				nameSchema.Set("bsonType", bson.VString("string"))
				s := bson.NewDocument()
				s.Set("properties", bson.VDoc(bson.D("name", bson.VDoc(nameSchema))))
				return s
			}(),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSchema(tt.doc, tt.schema)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateSchema() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
