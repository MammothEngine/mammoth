package mongo

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// Test OnDocumentInsert with wildcard index
func TestIndexCatalog_OnDocumentInsert_Wildcard(t *testing.T) {
	eng, _, ic := setupIndexTest(t)
	defer eng.Close()

	spec := IndexSpec{
		Name: "wildcard_idx",
		Key:  []IndexKey{{Field: "$**"}},
	}
	if err := ic.CreateIndex("testdb", "testcoll", spec); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("name", bson.VString("Alice"))
	doc.Set("age", bson.VInt32(30))
	doc.Set("nested", bson.VDoc(bson.D("x", bson.VInt32(1))))

	if err := ic.OnDocumentInsert("testdb", "testcoll", doc); err != nil {
		t.Fatalf("OnDocumentInsert wildcard: %v", err)
	}
}

// Test OnDocumentInsert with text index
func TestIndexCatalog_OnDocumentInsert_Text(t *testing.T) {
	eng, _, ic := setupIndexTest(t)
	defer eng.Close()

	spec := IndexSpec{
		Name:      "text_idx",
		Key:       []IndexKey{{Field: "content"}},
		IndexType: "text",
	}
	if err := ic.CreateIndex("testdb", "testcoll", spec); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("content", bson.VString("hello world test document"))

	if err := ic.OnDocumentInsert("testdb", "testcoll", doc); err != nil {
		t.Fatalf("OnDocumentInsert text: %v", err)
	}
}

// Test OnDocumentInsert with geo index
func TestIndexCatalog_OnDocumentInsert_Geo(t *testing.T) {
	eng, _, ic := setupIndexTest(t)
	defer eng.Close()

	spec := IndexSpec{
		Name:      "geo_idx",
		Key:       []IndexKey{{Field: "location"}},
		IndexType: "2dsphere",
	}
	if err := ic.CreateIndex("testdb", "testcoll", spec); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	// Create GeoJSON Point document
	coords := bson.NewDocument()
	coords.Set("type", bson.VString("Point"))
	coords.Set("coordinates", bson.VArray(bson.A(bson.VDouble(40.7128), bson.VDouble(-74.0060))))

	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("location", bson.VDoc(coords))

	if err := ic.OnDocumentInsert("testdb", "testcoll", doc); err != nil {
		t.Fatalf("OnDocumentInsert geo: %v", err)
	}
}

// Test OnDocumentInsert with hash index
func TestIndexCatalog_OnDocumentInsert_Hash(t *testing.T) {
	eng, _, ic := setupIndexTest(t)
	defer eng.Close()

	spec := IndexSpec{
		Name: "hash_idx",
		Key:  []IndexKey{{Field: "email", Hashed: true}},
	}
	if err := ic.CreateIndex("testdb", "testcoll", spec); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("email", bson.VString("test@example.com"))

	if err := ic.OnDocumentInsert("testdb", "testcoll", doc); err != nil {
		t.Fatalf("OnDocumentInsert hash: %v", err)
	}
}

// Test OnDocumentDelete with different index types
func TestIndexCatalog_OnDocumentDelete_Types(t *testing.T) {
	eng, _, ic := setupIndexTest(t)
	defer eng.Close()

	// Create multiple index types
	indexes := []IndexSpec{
		{Name: "wildcard_idx", Key: []IndexKey{{Field: "$**"}}},
		{Name: "text_idx", Key: []IndexKey{{Field: "content"}}, IndexType: "text"},
		{Name: "hash_idx", Key: []IndexKey{{Field: "email", Hashed: true}}},
	}

	for _, spec := range indexes {
		if err := ic.CreateIndex("testdb", "testcoll", spec); err != nil {
			t.Fatalf("CreateIndex %s: %v", spec.Name, err)
		}
	}

	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("email", bson.VString("test@example.com"))
	doc.Set("content", bson.VString("test content"))

	if err := ic.OnDocumentDelete("testdb", "testcoll", doc); err != nil {
		t.Fatalf("OnDocumentDelete: %v", err)
	}
}

// Test OnDocumentUpdate with different index types
func TestIndexCatalog_OnDocumentUpdate_Types(t *testing.T) {
	eng, _, ic := setupIndexTest(t)
	defer eng.Close()

	// Create multiple index types
	indexes := []IndexSpec{
		{Name: "wildcard_idx", Key: []IndexKey{{Field: "$**"}}},
		{Name: "text_idx", Key: []IndexKey{{Field: "content"}}, IndexType: "text"},
		{Name: "hash_idx", Key: []IndexKey{{Field: "email", Hashed: true}}},
	}

	for _, spec := range indexes {
		if err := ic.CreateIndex("testdb", "testcoll", spec); err != nil {
			t.Fatalf("CreateIndex %s: %v", spec.Name, err)
		}
	}

	oid := bson.NewObjectID()
	oldDoc := bson.NewDocument()
	oldDoc.Set("_id", bson.VObjectID(oid))
	oldDoc.Set("email", bson.VString("old@example.com"))
	oldDoc.Set("content", bson.VString("old content"))

	newDoc := bson.NewDocument()
	newDoc.Set("_id", bson.VObjectID(oid))
	newDoc.Set("email", bson.VString("new@example.com"))
	newDoc.Set("content", bson.VString("new content"))

	if err := ic.OnDocumentUpdate("testdb", "testcoll", oldDoc, newDoc); err != nil {
		t.Fatalf("OnDocumentUpdate: %v", err)
	}
}

// Test OnDocumentInsert with no indexes
func TestIndexCatalog_OnDocumentInsert_NoIndexes(t *testing.T) {
	eng, _, ic := setupIndexTest(t)
	defer eng.Close()

	// No indexes created
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("name", bson.VString("Alice"))

	if err := ic.OnDocumentInsert("testdb", "testcoll", doc); err != nil {
		t.Fatalf("OnDocumentInsert without indexes: %v", err)
	}
}

// Test OnDocumentInsert to non-existent collection
func TestIndexCatalog_OnDocumentInsert_InvalidCollection(t *testing.T) {
	eng, _, ic := setupIndexTest(t)
	defer eng.Close()

	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("name", bson.VString("Alice"))

	// Try to insert into a non-existent collection
	if err := ic.OnDocumentInsert("testdb", "nonexistent", doc); err != nil {
		// Expected - collection doesn't exist, ListIndexes will fail
		t.Logf("Expected error for non-existent collection: %v", err)
	}
}

// Test OnDocumentUpdate with geo index
func TestIndexCatalog_OnDocumentUpdate_Geo(t *testing.T) {
	eng, _, ic := setupIndexTest(t)
	defer eng.Close()

	spec := IndexSpec{
		Name:      "geo_idx",
		Key:       []IndexKey{{Field: "location"}},
		IndexType: "2dsphere",
	}
	if err := ic.CreateIndex("testdb", "testcoll", spec); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	oid := bson.NewObjectID()

	// Old location
	oldCoords := bson.NewDocument()
	oldCoords.Set("type", bson.VString("Point"))
	oldCoords.Set("coordinates", bson.VArray(bson.A(bson.VDouble(0.0), bson.VDouble(0.0))))
	oldDoc := bson.NewDocument()
	oldDoc.Set("_id", bson.VObjectID(oid))
	oldDoc.Set("location", bson.VDoc(oldCoords))

	// New location
	newCoords := bson.NewDocument()
	newCoords.Set("type", bson.VString("Point"))
	newCoords.Set("coordinates", bson.VArray(bson.A(bson.VDouble(40.7128), bson.VDouble(-74.0060))))
	newDoc := bson.NewDocument()
	newDoc.Set("_id", bson.VObjectID(oid))
	newDoc.Set("location", bson.VDoc(newCoords))

	if err := ic.OnDocumentUpdate("testdb", "testcoll", oldDoc, newDoc); err != nil {
		t.Fatalf("OnDocumentUpdate geo: %v", err)
	}
}

// Test isHashIndex function
func TestIsHashIndex(t *testing.T) {
	tests := []struct {
		name     string
		spec     *IndexSpec
		expected bool
	}{
		{
			name:     "Hashed key",
			spec:     &IndexSpec{Key: []IndexKey{{Field: "email", Hashed: true}}},
			expected: true,
		},
		{
			name:     "Regular key",
			spec:     &IndexSpec{Key: []IndexKey{{Field: "email"}}},
			expected: false,
		},
		{
			name:     "Mixed keys",
			spec:     &IndexSpec{Key: []IndexKey{{Field: "a"}, {Field: "b", Hashed: true}}},
			expected: true,
		},
		{
			name:     "Empty keys",
			spec:     &IndexSpec{Key: []IndexKey{}},
			expected: false,
		},
		{
			name:     "Nil keys",
			spec:     &IndexSpec{},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := isHashIndex(tc.spec)
			if result != tc.expected {
				t.Errorf("isHashIndex() = %v, want %v", result, tc.expected)
			}
		})
	}
}

// Test isWildcardIndex function
func TestIsWildcardIndex(t *testing.T) {
	tests := []struct {
		name     string
		spec     *IndexSpec
		expected bool
	}{
		{
			name:     "Wildcard $**",
			spec:     &IndexSpec{Key: []IndexKey{{Field: "$**"}}},
			expected: true,
		},
		{
			name:     "Regular field",
			spec:     &IndexSpec{Key: []IndexKey{{Field: "name"}}},
			expected: false,
		},
		{
			name:     "Mixed",
			spec:     &IndexSpec{Key: []IndexKey{{Field: "name"}, {Field: "$**"}}},
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := isWildcardIndex(tc.spec)
			if result != tc.expected {
				t.Errorf("isWildcardIndex() = %v, want %v", result, tc.expected)
			}
		})
	}
}

// Test isTextIndex function
func TestIsTextIndex(t *testing.T) {
	tests := []struct {
		name     string
		spec     *IndexSpec
		expected bool
	}{
		{
			name:     "Text type",
			spec:     &IndexSpec{Key: []IndexKey{{Field: "content"}}, IndexType: "text"},
			expected: true,
		},
		{
			name:     "Regular type",
			spec:     &IndexSpec{Key: []IndexKey{{Field: "name"}}},
			expected: false,
		},
		{
			name:     "2dsphere type",
			spec:     &IndexSpec{Key: []IndexKey{{Field: "loc"}}, IndexType: "2dsphere"},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := isTextIndex(tc.spec)
			if result != tc.expected {
				t.Errorf("isTextIndex() = %v, want %v", result, tc.expected)
			}
		})
	}
}

// Test isGeoIndex function
func TestIsGeoIndex(t *testing.T) {
	tests := []struct {
		name     string
		spec     *IndexSpec
		expected bool
	}{
		{
			name:     "2dsphere type",
			spec:     &IndexSpec{Key: []IndexKey{{Field: "loc"}}, IndexType: "2dsphere"},
			expected: true,
		},
		{
			name:     "Regular type",
			spec:     &IndexSpec{Key: []IndexKey{{Field: "name"}}},
			expected: false,
		},
		{
			name:     "Text type",
			spec:     &IndexSpec{Key: []IndexKey{{Field: "content"}}, IndexType: "text"},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := isGeoIndex(tc.spec)
			if result != tc.expected {
				t.Errorf("isGeoIndex() = %v, want %v", result, tc.expected)
			}
		})
	}
}
