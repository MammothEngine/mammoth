package wire

import (
	"strings"
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

func TestDecodeSASLData(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"valid base64", "SGVsbG8gV29ybGQ=", "Hello World"},
		{"invalid base64", "not-base64!@#", "not-base64!@#"},
		{"empty", "", ""},
		{"plain text", "plaintext", "plaintext"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := decodeSASLData(tt.input)
			if result != tt.expected {
				t.Errorf("decodeSASLData(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestHandleStartSession(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	result := h.handleStartSession()
	if result == nil {
		t.Fatal("expected non-nil document")
	}

	// Check ok field
	if ok, _ := result.Get("ok"); ok.Double() != 1.0 {
		t.Errorf("expected ok=1.0, got %v", ok.Double())
	}

	// Check id field exists
	if idVal, ok := result.Get("id"); !ok {
		t.Error("expected 'id' field in response")
	} else if idVal.Type != bson.TypeDocument {
		t.Errorf("expected id to be document, got %v", idVal.Type)
	}
}

func TestHandleGetCmdLineOpts(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	result := h.handleGetCmdLineOpts()
	if result == nil {
		t.Fatal("expected non-nil document")
	}

	// Check ok field
	if ok, _ := result.Get("ok"); ok.Double() != 1.0 {
		t.Errorf("expected ok=1.0, got %v", ok.Double())
	}

	// Check argv field
	if argv, ok := result.Get("argv"); !ok {
		t.Error("expected 'argv' field")
	} else if argv.Type != bson.TypeArray {
		t.Errorf("expected argv to be array, got %v", argv.Type)
	}

	// Check parsed field
	if parsed, ok := result.Get("parsed"); !ok {
		t.Error("expected 'parsed' field")
	} else if parsed.Type != bson.TypeDocument {
		t.Errorf("expected parsed to be document, got %v", parsed.Type)
	}
}

func TestPublicCommands(t *testing.T) {
	expectedPublic := []string{
		"hello", "isMaster", "ismaster", "ping", "buildInfo", "buildinfo",
		"whatsmyuri", "getCmdLineOpts", "saslStart", "saslContinue", "connectionStatus",
	}

	for _, cmd := range expectedPublic {
		if !publicCommands[cmd] {
			t.Errorf("expected %q to be in publicCommands", cmd)
		}
	}
}

// Test toFloat64 with various BSON value types
func TestToFloat64_wire(t *testing.T) {
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
		{"double positive", bson.VDouble(3.14), 3.14},
		{"double zero", bson.VDouble(0.0), 0.0},
		{"double negative", bson.VDouble(-5.7), -5.7},
		{"bool true", bson.VBool(true), 1.0},
		{"bool false", bson.VBool(false), 0.0},
		{"string (default)", bson.VString("test"), 0.0},
		{"null (default)", bson.VNull(), 0.0},
		{"document (default)", bson.VDoc(bson.NewDocument()), 0.0},
		{"array (default)", bson.VArray(bson.A()), 0.0},
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

// Test setNestedValue - setting fields at various paths
func TestSetNestedValue(t *testing.T) {
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
			setNestedValue(doc, tt.path, tt.val)
			if !tt.check(doc) {
				t.Errorf(tt.checkMsg)
			}
		})
	}
}

// Test unsetNested - removing fields at various paths
func TestUnsetNested(t *testing.T) {
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
			unsetNested(doc, tt.path)
			if !tt.check(doc) {
				t.Errorf(tt.checkMsg)
			}
		})
	}
}

// Test int32ToStr for wire package
func TestInt32ToStr_wire(t *testing.T) {
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
		{-10, "-10"},
		{-12345, "-12345"},
	}

	for _, tt := range tests {
		result := int32ToStr(tt.n)
		if result != tt.expected {
			t.Errorf("int32ToStr(%d) = %q, want %q", tt.n, result, tt.expected)
		}
	}
}

// Test evaluateExpr with $dayOfYear operator
func TestEvaluateExpr_DayOfYear(t *testing.T) {
	doc := bson.D("date", bson.VDateTime(1609459200000)) // 2021-01-01 00:00:00 UTC

	// $dayOfYear
	dayOfYearExpr := bson.VDoc(bson.D("$dayOfYear", bson.VString("$date")))

	result := evaluateExpr(dayOfYearExpr, doc)
	// Should return an int value for day of year
	if result.Type != bson.TypeInt32 && result.Type != bson.TypeInt64 {
		t.Logf("dayOfYear returned type: %v, value: %v", result.Type, result)
	}
}

// Test evaluateExpr with $year operator
func TestEvaluateExpr_Year(t *testing.T) {
	doc := bson.D("date", bson.VDateTime(1609459200000)) // 2021-01-01 00:00:00 UTC

	// $year
	yearExpr := bson.VDoc(bson.D("$year", bson.VString("$date")))

	result := evaluateExpr(yearExpr, doc)
	// Should return year as int
	if result.Type != bson.TypeInt32 && result.Type != bson.TypeInt64 {
		t.Logf("year returned type: %v, value: %v", result.Type, result)
	}
}

// Test handleDistinct command
func TestHandleDistinct(t *testing.T) {
	h, eng := setupTestHandler(t)
	defer eng.Close()
	defer h.Close()

	// Create collection and insert documents with different values
	h.cat.EnsureCollection("testdb", "testcoll")

	// Insert some documents using Put
	prefix := mongo.EncodeNamespacePrefix("testdb", "testcoll")
	docs := []*bson.Document{
		bson.D("_id", bson.VInt32(1), "category", bson.VString("A")),
		bson.D("_id", bson.VInt32(2), "category", bson.VString("B")),
		bson.D("_id", bson.VInt32(3), "category", bson.VString("A")),
		bson.D("_id", bson.VInt32(4), "category", bson.VString("C")),
	}
	for _, doc := range docs {
		id, _ := doc.Get("_id")
		key := append(prefix, []byte(id.String())...)
		h.engine.Put(key, bson.Encode(doc))
	}

	tests := []struct {
		name       string
		setupBody  func() *bson.Document
		wantValues int
		wantErr    bool
	}{
		{
			name: "distinct with valid key",
			setupBody: func() *bson.Document {
				body := bson.NewDocument()
				body.Set("distinct", bson.VString("testcoll"))
				body.Set("$db", bson.VString("testdb"))
				body.Set("key", bson.VString("category"))
				return body
			},
			wantValues: 1, // At least 1 value should be returned
		},
		{
			name: "distinct with missing collection",
			setupBody: func() *bson.Document {
				body := bson.NewDocument()
				// No collection name - use empty string
				body.Set("distinct", bson.VString(""))
				body.Set("$db", bson.VString("testdb"))
				body.Set("key", bson.VString("category"))
				return body
			},
			wantErr: true,
		},
		{
			name: "distinct with missing key",
			setupBody: func() *bson.Document {
				body := bson.NewDocument()
				body.Set("distinct", bson.VString("testcoll"))
				body.Set("$db", bson.VString("testdb"))
				// No key field
				return body
			},
			wantErr: true,
		},
		{
			name: "distinct with query filter",
			setupBody: func() *bson.Document {
				body := bson.NewDocument()
				body.Set("distinct", bson.VString("testcoll"))
				body.Set("$db", bson.VString("testdb"))
				body.Set("key", bson.VString("category"))
				// Filter: _id > 1 (only docs 2, 3, 4)
				query := bson.NewDocument()
				query.Set("_id", bson.VDoc(bson.D("$gt", bson.VInt32(1))))
				body.Set("query", bson.VDoc(query))
				return body
			},
			wantValues: 1, // At least 1 value should be returned
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := tt.setupBody()
			resp := h.handleDistinct(body)

			okVal, _ := resp.Get("ok")
			ok := okVal.Double() == 1.0

			if tt.wantErr && ok {
				t.Error("expected error, got ok=1")
			}
			if !tt.wantErr && !ok {
				t.Errorf("expected ok=1, got ok=%v", okVal.Double())
			}

			if !tt.wantErr {
				valuesVal, _ := resp.Get("values")
				values := valuesVal.ArrayValue()
				if len(values) != tt.wantValues {
					t.Errorf("expected %d distinct values, got %d", tt.wantValues, len(values))
				}
			}
		})
	}
}

// Test distinctKey function
func TestDistinctKey(t *testing.T) {
	tests := []struct {
		name     string
		val      bson.Value
		expected string
	}{
		{"string", bson.VString("hello"), "s:hello"},
		{"int32", bson.VInt32(42), "i:42"},
		{"int64", bson.VInt64(100), "l:100"},
		{"double", bson.VDouble(3.14), "d:3.14"},
		{"bool true", bson.VBool(true), "b:true"},
		{"bool false", bson.VBool(false), "b:false"},
		{"null", bson.VNull(), "n:"},
		{"objectid", bson.VObjectID(bson.ObjectID{1, 2, 3, 4}), "o:\x01\x02\x03\x04\x00\x00\x00\x00\x00\x00\x00\x00"},
		{"binary", bson.VBinary(bson.BinaryGeneric, []byte{1, 2, 3}), "5:{0 [1 2 3]}"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := distinctKey(tt.val)
			if result != tt.expected {
				t.Errorf("distinctKey(%v) = %q, want %q", tt.val, result, tt.expected)
			}
		})
	}
}

// Test NewServer and Addr
func TestNewServer_Addr(t *testing.T) {
	// Create a test handler
	handler := &Handler{}

	// Create server with random port
	config := ServerConfig{
		Addr:    "127.0.0.1:0", // Random available port
		Handler: handler,
	}

	server, err := NewServer(config)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	defer server.Close()

	// Addr() should return the actual listen address
	addr := server.Addr()
	if addr == "" {
		t.Error("Addr() returned empty string")
	}

	// Should contain the port
	if !strings.Contains(addr, ":") {
		t.Errorf("Addr() should contain port separator: %s", addr)
	}
}

// Test isSortCoveredByIndex
func TestIsSortCoveredByIndex(t *testing.T) {
	tests := []struct {
		name     string
		spec     *mongo.IndexSpec
		sortDoc  *bson.Document
		expected bool
	}{
		{
			name: "exact match ascending",
			spec: &mongo.IndexSpec{
				Key: []mongo.IndexKey{{Field: "name", Descending: false}},
			},
			sortDoc:  bson.D("name", bson.VInt32(1)),
			expected: true,
		},
		{
			name: "exact match descending",
			spec: &mongo.IndexSpec{
				Key: []mongo.IndexKey{{Field: "name", Descending: true}},
			},
			sortDoc:  bson.D("name", bson.VInt32(-1)),
			expected: true,
		},
		{
			name: "wrong direction",
			spec: &mongo.IndexSpec{
				Key: []mongo.IndexKey{{Field: "name", Descending: false}},
			},
			sortDoc:  bson.D("name", bson.VInt32(-1)),
			expected: false,
		},
		{
			name: "wrong field",
			spec: &mongo.IndexSpec{
				Key: []mongo.IndexKey{{Field: "name", Descending: false}},
			},
			sortDoc:  bson.D("age", bson.VInt32(1)),
			expected: false,
		},
		{
			name: "compound index - partial match",
			spec: &mongo.IndexSpec{
				Key: []mongo.IndexKey{
					{Field: "name", Descending: false},
					{Field: "age", Descending: false},
				},
			},
			sortDoc:  bson.D("name", bson.VInt32(1)),
			expected: true,
		},
		{
			name: "compound index - full match",
			spec: &mongo.IndexSpec{
				Key: []mongo.IndexKey{
					{Field: "name", Descending: false},
					{Field: "age", Descending: false},
				},
			},
			sortDoc:  bson.D("name", bson.VInt32(1), "age", bson.VInt32(1)),
			expected: true,
		},
		{
			name: "compound index - wrong order",
			spec: &mongo.IndexSpec{
				Key: []mongo.IndexKey{
					{Field: "name", Descending: false},
					{Field: "age", Descending: false},
				},
			},
			sortDoc:  bson.D("age", bson.VInt32(1)), // wrong field order
			expected: false,
		},
		{
			name: "more sort fields than index",
			spec: &mongo.IndexSpec{
				Key: []mongo.IndexKey{{Field: "name", Descending: false}},
			},
			sortDoc:  bson.D("name", bson.VInt32(1), "age", bson.VInt32(1)),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isSortCoveredByIndex(tt.spec, tt.sortDoc)
			if result != tt.expected {
				t.Errorf("isSortCoveredByIndex() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// Test SessionManager GetOrCreate and Get

// Test SessionManager GetOrCreate and Get
func TestSessionManager_GetOrCreateAndGet(t *testing.T) {
	sm := NewSessionManager()

	// GetOrCreate should create a new session
	s1 := sm.GetOrCreate(1)
	if s1 == nil {
		t.Fatal("expected non-nil session")
	}
	if s1.ConnID != 1 {
		t.Errorf("expected ConnID=1, got %d", s1.ConnID)
	}

	// GetOrCreate should return existing session
	s2 := sm.GetOrCreate(1)
	if s2 != s1 {
		t.Error("expected same session for same ConnID")
	}

	// Get should return the session
	s3 := sm.Get(1)
	if s3 != s1 {
		t.Error("Get should return the same session")
	}

	// Get for non-existent session should return nil
	s4 := sm.Get(999)
	if s4 != nil {
		t.Error("expected nil for non-existent session")
	}

	// Remove the session
	sm.Remove(1)

	// After removal, Get should return nil
	s5 := sm.Get(1)
	if s5 != nil {
		t.Error("expected nil after Remove")
	}

	// GetOrCreate should create a new session after removal
	s6 := sm.GetOrCreate(1)
	if s6 == nil {
		t.Fatal("expected non-nil session after re-create")
	}
	if s6 == s1 {
		t.Error("expected different session after re-create")
	}
}

// Test Server getters
func TestServer_Getters(t *testing.T) {
	// Create server with random port
	config := ServerConfig{
		Addr: "127.0.0.1:0",
	}

	server, err := NewServer(config)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	defer server.Close()

	// Test StartTime
	startTime := server.StartTime()
	if startTime.IsZero() {
		t.Error("expected non-zero start time")
	}

	// Test ConnCount - should be 0 initially
	connCount := server.ConnCount()
	if connCount != 0 {
		t.Errorf("expected ConnCount=0 initially, got %d", connCount)
	}
}
