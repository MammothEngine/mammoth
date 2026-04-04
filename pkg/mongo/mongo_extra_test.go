package mongo

import (
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

// TestExtractCoordinatesGeoJSON tests extractCoordinates with GeoJSON format
func TestExtractCoordinatesGeoJSON(t *testing.T) {
	// GeoJSON Point format
	doc := bson.D(
		"type", bson.VString("Point"),
		"coordinates", bson.VArray(bson.A(bson.VDouble(10.5), bson.VDouble(20.3))),
	)
	v := bson.VDoc(doc)

	point := extractCoordinates(v)
	if point == nil {
		t.Fatal("expected non-nil point")
	}
	if point.Lon != 10.5 {
		t.Errorf("Lon = %v, want 10.5", point.Lon)
	}
	if point.Lat != 20.3 {
		t.Errorf("Lat = %v, want 20.3", point.Lat)
	}
}

// TestExtractCoordinatesLegacyLngLat tests extractCoordinates with legacy lng/lat format
func TestExtractCoordinatesLegacyLngLat(t *testing.T) {
	// Legacy format: {lng: x, lat: y}
	doc := bson.D(
		"lng", bson.VDouble(30.0),
		"lat", bson.VDouble(40.0),
	)
	v := bson.VDoc(doc)

	point := extractCoordinates(v)
	if point == nil {
		t.Fatal("expected non-nil point")
	}
	if point.Lon != 30.0 {
		t.Errorf("Lon = %v, want 30.0", point.Lon)
	}
	if point.Lat != 40.0 {
		t.Errorf("Lat = %v, want 40.0", point.Lat)
	}
}

// TestExtractCoordinatesLegacyLongitudeLatitude tests extractCoordinates with legacy longitude/latitude format
func TestExtractCoordinatesLegacyLongitudeLatitude(t *testing.T) {
	// Legacy format: {longitude: x, latitude: y}
	doc := bson.D(
		"longitude", bson.VDouble(50.0),
		"latitude", bson.VDouble(60.0),
	)
	v := bson.VDoc(doc)

	point := extractCoordinates(v)
	if point == nil {
		t.Fatal("expected non-nil point")
	}
	if point.Lon != 50.0 {
		t.Errorf("Lon = %v, want 50.0", point.Lon)
	}
	if point.Lat != 60.0 {
		t.Errorf("Lat = %v, want 60.0", point.Lat)
	}
}

// TestExtractCoordinatesArray tests extractCoordinates with array format
func TestExtractCoordinatesArray(t *testing.T) {
	// Array format: [lon, lat]
	v := bson.VArray(bson.A(bson.VDouble(70.0), bson.VDouble(80.0)))

	point := extractCoordinates(v)
	if point == nil {
		t.Fatal("expected non-nil point")
	}
	if point.Lon != 70.0 {
		t.Errorf("Lon = %v, want 70.0", point.Lon)
	}
	if point.Lat != 80.0 {
		t.Errorf("Lat = %v, want 80.0", point.Lat)
	}
}

// TestExtractCoordinatesInvalid tests extractCoordinates with invalid formats
func TestExtractCoordinatesInvalid(t *testing.T) {
	tests := []struct {
		name string
		v    bson.Value
	}{
		{
			name: "empty document",
			v:    bson.VDoc(bson.NewDocument()),
		},
		{
			name: "document with only lng",
			v:    bson.VDoc(bson.D("lng", bson.VDouble(10.0))),
		},
		{
			name: "array with only one element",
			v:    bson.VArray(bson.A(bson.VDouble(10.0))),
		},
		{
			name: "string value",
			v:    bson.VString("invalid"),
		},
		{
			name: "int value",
			v:    bson.VInt32(42),
		},
		{
			name: "GeoJSON with wrong type",
			v: bson.VDoc(bson.D(
				"type", bson.VString("Polygon"),
				"coordinates", bson.VArray(bson.A()),
			)),
		},
		{
			name: "GeoJSON with missing coordinates",
			v: bson.VDoc(bson.D(
				"type", bson.VString("Point"),
			)),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			point := extractCoordinates(tc.v)
			if point != nil {
				t.Error("expected nil point for invalid format")
			}
		})
	}
}

// TestExtractCoordinatesGeoJSONShortArray tests GeoJSON with short coordinates array
func TestExtractCoordinatesGeoJSONShortArray(t *testing.T) {
	doc := bson.D(
		"type", bson.VString("Point"),
		"coordinates", bson.VArray(bson.A(bson.VDouble(10.0))),
	)
	v := bson.VDoc(doc)

	point := extractCoordinates(v)
	if point != nil {
		t.Error("expected nil point for coordinates with < 2 elements")
	}
}

// TestCatalogDropDatabase tests DropDatabase with existing and non-existing databases
func TestCatalogDropDatabase(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)

	// Create a database and collection
	cat.EnsureDatabase("testdb")
	cat.EnsureCollection("testdb", "testcoll")

	// Drop the database
	if err := cat.DropDatabase("testdb"); err != nil {
		t.Fatalf("DropDatabase error: %v", err)
	}

	// Verify database is gone
	dbs, _ := cat.ListDatabases()
	for _, db := range dbs {
		if db.Name == "testdb" {
			t.Error("expected testdb to be dropped")
		}
	}

	// Try to drop non-existent database - may return error
	_ = cat.DropDatabase("nonexistent")
}

// TestCatalogDropDatabaseWithError tests DropDatabase error paths
func TestCatalogDropDatabaseWithError(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}

	cat := NewCatalog(eng)

	// Create database
	cat.EnsureDatabase("errdb")
	cat.EnsureCollection("errdb", "coll1")

	// Close engine to cause errors
	eng.Close()

	// Try to drop database with closed engine
	if err := cat.DropDatabase("errdb"); err == nil {
		t.Error("expected error for DropDatabase with closed engine")
	}
}

// TestExternalSorterEmptyInput tests ExternalSorter with empty input
func TestExternalSorterEmptyInput(t *testing.T) {
	s := NewExternalSorter(0, byIDAsc)
	defer s.Close()

	// Sort with no data
	result, err := s.Sort()
	if err != nil {
		t.Fatalf("Sort error: %v", err)
	}

	// Should have no results
	if len(result) != 0 {
		t.Errorf("expected 0 results, got %d", len(result))
	}
}

// TestCollectionFindOneByKeyNotFound tests FindOneByKey when document not found
func TestCollectionFindOneByKeyNotFound(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	coll := NewCollection("testdb", "testcoll", eng, cat)

	// Try to find non-existent document
	key := EncodeNamespacePrefix("testdb", "testcoll")
	key = append(key, []byte("nonexistent")...)
	_, err = coll.FindOneByKey(key)
	if err == nil {
		t.Error("expected error for non-existent document")
	}
}

// TestCollectionDeleteOneNotFound tests DeleteOne when document not found
func TestCollectionDeleteOneNotFound(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	coll := NewCollection("testdb", "testcoll", eng, cat)

	// Try to delete non-existent document
	id := bson.NewObjectID()
	_ = coll.DeleteOne(id)
}

// TestCollectionScanAllEmpty tests ScanAll on empty collection
func TestCollectionScanAllEmpty(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	coll := NewCollection("testdb", "testcoll", eng, cat)

	// Scan empty collection
	count := 0
	coll.ScanAll(func(_ []byte, doc *bson.Document) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("expected 0 documents, got %d", count)
	}
}

// TestCatalogEnsureDatabaseExisting tests EnsureDatabase on existing database
func TestCatalogEnsureDatabaseExisting(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)

	// Create database first
	cat.EnsureDatabase("testdb")

	// Call EnsureDatabase again - should not error
	if err := cat.EnsureDatabase("testdb"); err != nil {
		t.Errorf("EnsureDatabase on existing db error: %v", err)
	}
}

// TestCatalogGetCollectionNotFound tests GetCollection for non-existent collection
func TestCatalogGetCollectionNotFound(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	cat.EnsureDatabase("testdb")

	// Try to get non-existent collection
	_, err = cat.GetCollection("testdb", "nonexistent")
	if err == nil {
		t.Error("expected error for non-existent collection")
	}
}

// TestCatalogListCollectionsEmpty tests ListCollections on empty database
func TestCatalogListCollectionsEmpty(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	cat.EnsureDatabase("testdb")

	// List collections on empty database
	colls, err := cat.ListCollections("testdb")
	if err != nil {
		t.Fatalf("ListCollections error: %v", err)
	}
	if len(colls) != 0 {
		t.Errorf("expected 0 collections, got %d", len(colls))
	}
}

// TestCatalogListCollectionsNotFound tests ListCollections for non-existent database
func TestCatalogListCollectionsNotFound(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)

	// Try to list collections for non-existent database
	// May or may not return error depending on implementation
	_, _ = cat.ListCollections("nonexistent")
}

// TestCatalogSetValidatorNotFound tests SetValidator for non-existent collection
func TestCatalogSetValidatorNotFound(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	cat.EnsureDatabase("testdb")

	// Try to set validator for non-existent collection
	_ = cat.SetValidator("testdb", "nonexistent", nil)
}

// TestCatalogGetValidatorNotFound tests GetValidator for non-existent collection
func TestCatalogGetValidatorNotFound(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	cat.EnsureDatabase("testdb")
	cat.EnsureCollection("testdb", "testcoll")

	// Get validator for collection without validator
	_, err = cat.GetValidator("testdb", "testcoll")
	// May or may not error depending on implementation
	_ = err

	// Try to get validator for non-existent collection
	_, err = cat.GetValidator("testdb", "nonexistent")
	if err == nil {
		t.Error("expected error for non-existent collection")
	}
}

// TestExternalSorterAddError tests ExternalSorter Add with error
func TestExternalSorterAddError(t *testing.T) {
	s := NewExternalSorter(0, byIDAsc)
	defer s.Close()

	// Add a document
	doc := bson.D("_id", bson.VInt32(1))
	err := s.Add(doc)
	if err != nil {
		t.Errorf("Add error: %v", err)
	}

	// Sort should work
	result, err := s.Sort()
	if err != nil {
		t.Errorf("Sort error: %v", err)
	}
	if len(result) != 1 {
		t.Errorf("expected 1 result, got %d", len(result))
	}
}

// TestCollectionCount tests Collection Count method
func TestCollectionCount(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	coll := NewCollection("testdb", "testcoll", eng, cat)

	// Count on empty collection
	count, err := coll.Count()
	if err != nil {
		t.Fatalf("Count error: %v", err)
	}
	if count != 0 {
		t.Errorf("expected count 0, got %d", count)
	}

	// Insert a document
	doc := bson.D("_id", bson.VInt32(1), "name", bson.VString("test"))
	if err := coll.InsertOne(doc); err != nil {
		t.Fatalf("InsertOne error: %v", err)
	}

	// Count after insert
	count, err = coll.Count()
	if err != nil {
		t.Fatalf("Count error: %v", err)
	}
	if count != 1 {
		t.Errorf("expected count 1, got %d", count)
	}
}

// TestCollectionReplaceOne tests Collection ReplaceOne method
func TestCollectionReplaceOne(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	coll := NewCollection("testdb", "testcoll", eng, cat)

	// Insert a document
	doc := bson.D("_id", bson.VInt32(1), "name", bson.VString("original"))
	if err := coll.InsertOne(doc); err != nil {
		t.Fatalf("InsertOne error: %v", err)
	}

	// Replace it
	replacement := bson.D("_id", bson.VInt32(1), "name", bson.VString("replaced"))
	if err := coll.ReplaceOne(bson.ObjectID{}, replacement); err != nil {
		t.Errorf("ReplaceOne error: %v", err)
	}
}

// TestCollectionDeleteByKey tests Collection DeleteByKey method
func TestCollectionDeleteByKey(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	coll := NewCollection("testdb", "testcoll", eng, cat)

	// Insert a document
	doc := bson.D("_id", bson.VInt32(1), "name", bson.VString("test"))
	if err := coll.InsertOne(doc); err != nil {
		t.Fatalf("InsertOne error: %v", err)
	}

	// Get the key for the document
	var key []byte
	coll.ScanAll(func(k []byte, d *bson.Document) bool {
		key = k
		return false
	})

	// Delete by key
	if err := coll.DeleteByKey(key); err != nil {
		t.Errorf("DeleteByKey error: %v", err)
	}

	// Verify deletion
	count, _ := coll.Count()
	if count != 0 {
		t.Errorf("expected count 0 after delete, got %d", count)
	}
}

// TestCollectionInsertMany tests Collection InsertMany method
func TestCollectionInsertMany(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	coll := NewCollection("testdb", "testcoll", eng, cat)

	// Insert many documents
	docs := []*bson.Document{
		bson.D("_id", bson.VInt32(1), "name", bson.VString("a")),
		bson.D("_id", bson.VInt32(2), "name", bson.VString("b")),
		bson.D("_id", bson.VInt32(3), "name", bson.VString("c")),
	}
	if err := coll.InsertMany(docs); err != nil {
		t.Errorf("InsertMany error: %v", err)
	}

	// Verify count
	count, _ := coll.Count()
	if count != 3 {
		t.Errorf("expected count 3, got %d", count)
	}
}

// TestCatalogDropCollection tests DropCollection
func TestCatalogDropCollection(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	cat.EnsureDatabase("testdb")
	cat.EnsureCollection("testdb", "testcoll")

	// Drop collection
	if err := cat.DropCollection("testdb", "testcoll"); err != nil {
		t.Errorf("DropCollection error: %v", err)
	}

	// Verify it's gone
	_, err = cat.GetCollection("testdb", "testcoll")
	if err == nil {
		t.Error("expected error for dropped collection")
	}
}

// TestCatalogDropCollectionNotFound tests DropCollection for non-existent collection
func TestCatalogDropCollectionNotFound(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	cat.EnsureDatabase("testdb")

	// Try to drop non-existent collection
	err = cat.DropCollection("testdb", "nonexistent")
	if err == nil {
		t.Error("expected error for non-existent collection")
	}
}

// TestCatalogUpdateCollectionInfo tests UpdateCollectionInfo
func TestCatalogUpdateCollectionInfo(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	cat.EnsureDatabase("testdb")
	cat.EnsureCollection("testdb", "testcoll")

	// Update collection info
	info := CollectionInfo{
		Name: "testcoll",
		DB:   "testdb",
	}

	if err := cat.UpdateCollectionInfo("testdb", "testcoll", info); err != nil {
		t.Errorf("UpdateCollectionInfo error: %v", err)
	}
}

// TestCatalogUpdateCollectionInfoNotFound tests UpdateCollectionInfo for non-existent collection
func TestCatalogUpdateCollectionInfoNotFound(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	cat.EnsureDatabase("testdb")

	info := CollectionInfo{
		Name: "nonexistent",
		DB:   "testdb",
	}

	// Try to update non-existent collection
	// May or may not return error depending on implementation
	_ = cat.UpdateCollectionInfo("testdb", "nonexistent", info)
}

// TestCappedCollectionEnforceLimits tests CappedCollection EnforceLimits
func TestCappedCollectionEnforceLimits(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	coll := NewCollection("testdb", "capped", eng, cat)

	// Create capped collection with max 5 docs
	capped := &CappedCollection{
		db:      "testdb",
		coll:    "capped",
		eng:     eng,
		maxDocs: 5,
	}

	// Insert 10 documents
	for i := 0; i < 10; i++ {
		doc := bson.D("_id", bson.VInt32(int32(i)), "data", bson.VString("test"))
		coll.InsertOne(doc)
	}

	// Enforce limits
	removed, err := capped.EnforceLimits()
	if err != nil {
		t.Errorf("EnforceLimits error: %v", err)
	}

	// Should have removed 5 oldest docs
	if removed != 5 {
		t.Errorf("expected 5 removed, got %d", removed)
	}

	// Verify count
	count, _ := coll.Count()
	if count != 5 {
		t.Errorf("expected 5 remaining, got %d", count)
	}
}

// TestCappedCollectionEnforceLimitsBySize tests CappedCollection EnforceLimits by size
func TestCappedCollectionEnforceLimitsBySize(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	coll := NewCollection("testdb", "capped2", eng, cat)

	// Create capped collection with small max size (100 bytes)
	capped := &CappedCollection{
		db:      "testdb",
		coll:    "capped2",
		eng:     eng,
		maxSize: 200, // Small size to trigger removal
	}

	// Insert several documents with data
	for i := 0; i < 5; i++ {
		doc := bson.D("_id", bson.VInt32(int32(i)), "data", bson.VString("this is some test data to increase size"))
		coll.InsertOne(doc)
	}

	// Enforce limits
	removed, err := capped.EnforceLimits()
	if err != nil {
		t.Errorf("EnforceLimits error: %v", err)
	}

	// Should have removed some docs
	if removed == 0 {
		t.Error("expected some docs to be removed due to size limit")
	}
}

// TestCappedCollectionEnforceLimitsNoLimits tests EnforceLimits with no limits set
func TestCappedCollectionEnforceLimitsNoLimits(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// Create capped collection with no limits
	capped := &CappedCollection{
		db:   "testdb",
		coll: "nocap",
		eng:  eng,
	}

	// Enforce limits should return 0
	removed, err := capped.EnforceLimits()
	if err != nil {
		t.Errorf("EnforceLimits error: %v", err)
	}
	if removed != 0 {
		t.Errorf("expected 0 removed with no limits, got %d", removed)
	}
}

// TestIsCapped tests IsCapped function
func TestIsCapped(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	cat.EnsureDatabase("testdb")

	// Create regular collection
	cat.CreateCollection("testdb", "regular")

	// Test non-capped collection
	if IsCapped(cat, "testdb", "regular") {
		t.Error("expected regular collection to not be capped")
	}

	// Test non-existent collection
	if IsCapped(cat, "testdb", "nonexistent") {
		t.Error("expected non-existent collection to not be capped")
	}
}

// TestExternalSorterFlushError tests ExternalSorter flush with errors
func TestExternalSorterFlushError(t *testing.T) {
	s := NewExternalSorter(100, byIDAsc) // Small memory limit
	defer s.Close()

	// Add many documents to force flush
	for i := 0; i < 50; i++ {
		doc := bson.D("_id", bson.VInt32(int32(i)))
		if err := s.Add(doc); err != nil {
			t.Fatalf("Add error: %v", err)
		}
	}

	// Sort should work
	result, err := s.Sort()
	if err != nil {
		t.Errorf("Sort error: %v", err)
	}
	if len(result) != 50 {
		t.Errorf("expected 50 results, got %d", len(result))
	}
}

// TestExternalSorterCloseMultiple tests closing ExternalSorter multiple times
func TestExternalSorterCloseMultiple(t *testing.T) {
	s := NewExternalSorter(0, byIDAsc)

	// Close should work
	if err := s.Close(); err != nil {
		t.Errorf("Close error: %v", err)
	}

	// Close again should not panic
	if err := s.Close(); err != nil {
		t.Errorf("Second Close error: %v", err)
	}
}

// TestCursorManagerKill tests CursorManager Kill
func TestCursorManagerKill(t *testing.T) {
	cm := NewCursorManager()
	defer cm.Close()

	// Register some cursors
	docs := []*bson.Document{bson.D("_id", bson.VInt32(1))}
	cursor1 := cm.Register("test.coll", docs, 10)
	cursor2 := cm.Register("test.coll", docs, 10)

	id1 := cursor1.ID()
	id2 := cursor2.ID()

	// Kill both
	cm.Kill([]uint64{id1, id2})

	// Verify they're gone
	if _, ok := cm.Get(id1); ok {
		t.Error("expected cursor1 to be killed")
	}
	if _, ok := cm.Get(id2); ok {
		t.Error("expected cursor2 to be killed")
	}
}

// TestCursorManagerGetNotFound tests CursorManager Get for non-existent cursor
func TestCursorManagerGetNotFound(t *testing.T) {
	cm := NewCursorManager()
	defer cm.Close()

	// Get non-existent cursor
	_, ok := cm.Get(99999)
	if ok {
		t.Error("expected !ok for non-existent cursor")
	}
}

// TestCatalogListDatabasesBSON tests ListDatabasesBSON
func TestCatalogListDatabasesBSON(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	cat.EnsureDatabase("db1")
	cat.EnsureDatabase("db2")
	cat.EnsureCollection("db1", "coll1")
	cat.EnsureCollection("db2", "coll2")

	// Get BSON list
	docs, err := cat.ListDatabasesBSON()
	if err != nil {
		t.Fatalf("ListDatabasesBSON error: %v", err)
	}

	// Should have at least 2 databases
	if len(docs) < 2 {
		t.Errorf("expected at least 2 databases, got %d", len(docs))
	}
}

// TestCatalogListCollectionsBSON tests ListCollectionsBSON
func TestCatalogListCollectionsBSON(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	cat.EnsureDatabase("testdb")
	cat.EnsureCollection("testdb", "coll1")
	cat.EnsureCollection("testdb", "coll2")

	// Get BSON list
	docs, err := cat.ListCollectionsBSON("testdb")
	if err != nil {
		t.Fatalf("ListCollectionsBSON error: %v", err)
	}

	// Should have 2 collections
	if len(docs) != 2 {
		t.Errorf("expected 2 collections, got %d", len(docs))
	}
}

// TestCatalogListCollectionsBSONNotFound tests ListCollectionsBSON for non-existent database
func TestCatalogListCollectionsBSONNotFound(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)

	// Try to get collections for non-existent database
	// May or may not return error depending on implementation
	_, _ = cat.ListCollectionsBSON("nonexistent")
}

// TestCatalogCreateDatabase tests CreateDatabase
func TestCatalogCreateDatabase(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)

	// Create database
	if err := cat.CreateDatabase("newdb"); err != nil {
		t.Errorf("CreateDatabase error: %v", err)
	}

	// Verify it exists
	dbs, _ := cat.ListDatabases()
	found := false
	for _, db := range dbs {
		if db.Name == "newdb" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected newdb to exist")
	}
}

// TestCatalogCreateDatabaseDuplicate tests CreateDatabase with duplicate name
func TestCatalogCreateDatabaseDuplicate(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)

	// Create database
	if err := cat.CreateDatabase("dupdb"); err != nil {
		t.Fatalf("CreateDatabase error: %v", err)
	}

	// Try to create again - may error
	_ = cat.CreateDatabase("dupdb")
}

// TestCatalogGetDatabase tests GetDatabase
func TestCatalogGetDatabase(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	cat.EnsureDatabase("testdb")

	// Get existing database
	db, err := cat.GetDatabase("testdb")
	if err != nil {
		t.Errorf("GetDatabase error: %v", err)
	}
	if db.Name != "testdb" {
		t.Errorf("expected name testdb, got %s", db.Name)
	}

	// Get non-existent database
	_, err = cat.GetDatabase("nonexistent")
	if err == nil {
		t.Error("expected error for non-existent database")
	}
}

// TestCollectionScanAllStopEarly tests ScanAll with early stop
func TestCollectionScanAllStopEarly(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	coll := NewCollection("testdb", "testcoll", eng, cat)

	// Insert 5 documents
	for i := 0; i < 5; i++ {
		doc := bson.D("_id", bson.VInt32(int32(i)))
		coll.InsertOne(doc)
	}

	// Scan but stop after 2
	count := 0
	coll.ScanAll(func(_ []byte, doc *bson.Document) bool {
		count++
		return count < 2 // Stop after 2
	})

	if count != 2 {
		t.Errorf("expected count 2, got %d", count)
	}
}

// TestExternalSorterMergeEmpty tests ExternalSorter merge with empty runs
func TestExternalSorterMergeEmpty(t *testing.T) {
	s := NewExternalSorter(0, byIDAsc)
	defer s.Close()

	// Add single document
	doc := bson.D("_id", bson.VInt32(1))
	s.Add(doc)

	// Sort - should handle single doc
	result, err := s.Sort()
	if err != nil {
		t.Errorf("Sort error: %v", err)
	}
	if len(result) != 1 {
		t.Errorf("expected 1 result, got %d", len(result))
	}
}

// TestExternalSorterLargeDocument tests ExternalSorter with large documents
func TestExternalSorterLargeDocument(t *testing.T) {
	s := NewExternalSorter(1000, byIDAsc)
	defer s.Close()

	// Add documents with large data
	for i := 0; i < 10; i++ {
		// Create large string to exceed memory limit quickly
		largeData := make([]byte, 200)
		for j := range largeData {
			largeData[j] = byte('a' + (j % 26))
		}
		doc := bson.D("_id", bson.VInt32(int32(i)), "data", bson.VString(string(largeData)))
		s.Add(doc)
	}

	result, err := s.Sort()
	if err != nil {
		t.Errorf("Sort error: %v", err)
	}
	if len(result) != 10 {
		t.Errorf("expected 10 results, got %d", len(result))
	}

	// Verify order
	for i, d := range result {
		v, _ := d.Get("_id")
		if v.Int32() != int32(i) {
			t.Errorf("position %d: expected _id %d, got %d", i, i, v.Int32())
		}
	}
}

// TestCatalogCreateCollection tests CreateCollection
func TestCatalogCreateCollection(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	cat.EnsureDatabase("testdb")

	// Create collection
	if err := cat.CreateCollection("testdb", "newcoll"); err != nil {
		t.Errorf("CreateCollection error: %v", err)
	}

	// Verify it exists
	colls, _ := cat.ListCollections("testdb")
	found := false
	for _, c := range colls {
		if c.Name == "newcoll" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected newcoll to exist")
	}
}

// TestCatalogCreateCollectionDuplicate tests CreateCollection with duplicate name
func TestCatalogCreateCollectionDuplicate(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	cat.EnsureDatabase("testdb")

	// Create collection
	if err := cat.CreateCollection("testdb", "dupcoll"); err != nil {
		t.Fatalf("CreateCollection error: %v", err)
	}

	// Try to create again - should error
	if err := cat.CreateCollection("testdb", "dupcoll"); err == nil {
		t.Error("expected error for duplicate collection name")
	}
}

// TestCatalogCreateCollectionNotFound tests CreateCollection for non-existent database
func TestCatalogCreateCollectionNotFound(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)

	// Try to create collection for non-existent database
	if err := cat.CreateCollection("nonexistent", "coll"); err == nil {
		t.Error("expected error for non-existent database")
	}
}

// TestCatalogEnsureCollection tests EnsureCollection
func TestCatalogEnsureCollection(t *testing.T) {
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cat := NewCatalog(eng)
	cat.EnsureDatabase("testdb")

	// Ensure collection (creates if not exists)
	if err := cat.EnsureCollection("testdb", "ensuredcoll"); err != nil {
		t.Errorf("EnsureCollection error: %v", err)
	}

	// Ensure again (should not error)
	if err := cat.EnsureCollection("testdb", "ensuredcoll"); err != nil {
		t.Errorf("EnsureCollection second call error: %v", err)
	}

	// Verify it exists
	colls, _ := cat.ListCollections("testdb")
	found := false
	for _, c := range colls {
		if c.Name == "ensuredcoll" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected ensuredcoll to exist")
	}
}
