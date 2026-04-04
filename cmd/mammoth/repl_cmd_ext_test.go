package main

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

func TestExecuteREPL_Help(t *testing.T) {
	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Setup test engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, _ := engine.Open(opts)
	defer eng.Close()
	replEngine = eng
	replCatalog = mongo.NewCatalog(eng)

	executeREPL("help")

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	if !bytes.Contains(buf.Bytes(), []byte("Commands:")) {
		t.Errorf("Expected 'Commands:' in output, got: %s", output)
	}
}

func TestExecuteREPL_Use(t *testing.T) {
	// Setup test engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, _ := engine.Open(opts)
	defer eng.Close()
	replEngine = eng
	replCatalog = mongo.NewCatalog(eng)

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	executeREPL("use testdb")

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)

	if currentDB != "testdb" {
		t.Errorf("Expected currentDB='testdb', got '%s'", currentDB)
	}

	if !bytes.Contains(buf.Bytes(), []byte("switched to db testdb")) {
		t.Errorf("Expected 'switched to db testdb' in output, got: %s", buf.String())
	}
}

func TestExecuteREPL_Unknown(t *testing.T) {
	// Setup test engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, _ := engine.Open(opts)
	defer eng.Close()
	replEngine = eng
	replCatalog = mongo.NewCatalog(eng)

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	executeREPL("unknowncommand")

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)

	if !bytes.Contains(buf.Bytes(), []byte("Unknown command")) {
		t.Errorf("Expected 'Unknown command' in output, got: %s", buf.String())
	}
}

func TestExecuteREPL_Empty(t *testing.T) {
	// Should not panic
	executeREPL("")
	executeREPL("   ")
}

func TestCmdUse_NoArgs(t *testing.T) {
	// Setup
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmdUse([]string{"use"})

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)

	if !bytes.Contains(buf.Bytes(), []byte("Usage:")) {
		t.Errorf("Expected 'Usage:' in output, got: %s", buf.String())
	}
}

func TestCmdShow_NoArgs(t *testing.T) {
	// Setup
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmdShow([]string{"show"})

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)

	if !bytes.Contains(buf.Bytes(), []byte("Usage:")) {
		t.Errorf("Expected 'Usage:' in output, got: %s", buf.String())
	}
}

func TestCmdDB_NoArgs(t *testing.T) {
	// Setup
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmdDB([]string{"db"})

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)

	if !bytes.Contains(buf.Bytes(), []byte("Usage:")) {
		t.Errorf("Expected 'Usage:' in output, got: %s", buf.String())
	}
}

func TestPrintDoc(t *testing.T) {
	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	doc := bson.NewDocument()
	doc.Set("name", bson.VString("test"))
	doc.Set("value", bson.VInt32(42))
	printDoc(doc)

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	if !bytes.Contains(buf.Bytes(), []byte("name:")) {
		t.Errorf("Expected 'name:' in output, got: %s", output)
	}
	if !bytes.Contains(buf.Bytes(), []byte("value:")) {
		t.Errorf("Expected 'value:' in output, got: %s", output)
	}
}

func TestPrintDoc_Empty(t *testing.T) {
	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	doc := bson.NewDocument()
	printDoc(doc)

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	if output != "{}\n" {
		t.Errorf("Expected '{}\\n', got: %s", output)
	}
}

func TestCmdDatabaseOperation_InvalidSyntax(t *testing.T) {
	// Setup test engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, _ := engine.Open(opts)
	defer eng.Close()
	replEngine = eng
	replCatalog = mongo.NewCatalog(eng)
	currentDB = "test"

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Missing db. prefix
	cmdDatabaseOperation([]string{"invalid"})

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)

	if !bytes.Contains(buf.Bytes(), []byte("Unknown command")) {
		t.Errorf("Expected 'Unknown command' in output, got: %s", buf.String())
	}
}

func TestCmdDatabaseOperation_MissingMethod(t *testing.T) {
	// Setup test engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, _ := engine.Open(opts)
	defer eng.Close()
	replEngine = eng
	replCatalog = mongo.NewCatalog(eng)
	currentDB = "test"

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Missing method call syntax
	cmdDatabaseOperation([]string{"db.collection.nomethod"})

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)

	if !bytes.Contains(buf.Bytes(), []byte("Unknown:")) {
		t.Errorf("Expected 'Unknown:' in output, got: %s", buf.String())
	}
}

func TestReplCount(t *testing.T) {
	// Setup test engine with data
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, _ := engine.Open(opts)
	defer eng.Close()
	replEngine = eng
	replCatalog = mongo.NewCatalog(eng)
	currentDB = "testdb"

	// Insert test data
	cat := mongo.NewCatalog(eng)
	cat.EnsureDatabase("testdb")
	cat.EnsureCollection("testdb", "testcoll")
	coll := mongo.NewCollection("testdb", "testcoll", eng, cat)

	for i := 0; i < 5; i++ {
		doc := bson.NewDocument()
		doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
		doc.Set("value", bson.VInt32(int32(i)))
		coll.InsertOne(doc)
	}

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	replCount(coll)

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	if output != "5\n" {
		t.Errorf("Expected count '5', got: %s", output)
	}
}

func TestReplCount_Empty(t *testing.T) {
	// Setup test engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, _ := engine.Open(opts)
	defer eng.Close()
	replEngine = eng
	replCatalog = mongo.NewCatalog(eng)

	cat := mongo.NewCatalog(eng)
	cat.EnsureDatabase("testdb")
	cat.EnsureCollection("testdb", "emptycoll")
	coll := mongo.NewCollection("testdb", "emptycoll", eng, cat)

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	replCount(coll)

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	if output != "0\n" {
		t.Errorf("Expected count '0', got: %s", output)
	}
}

func TestReplInsert_InvalidDoc(t *testing.T) {
	// Setup test engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, _ := engine.Open(opts)
	defer eng.Close()
	replEngine = eng
	replCatalog = mongo.NewCatalog(eng)

	cat := mongo.NewCatalog(eng)
	cat.EnsureDatabase("testdb")
	cat.EnsureCollection("testdb", "testcoll")
	coll := mongo.NewCollection("testdb", "testcoll", eng, cat)

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Invalid document syntax
	replInsert(coll, "{invalid")

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)

	if !bytes.Contains(buf.Bytes(), []byte("Parse error")) {
		t.Errorf("Expected 'Parse error' in output, got: %s", buf.String())
	}
}

func TestReplFind_InvalidFilter(t *testing.T) {
	// Setup test engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, _ := engine.Open(opts)
	defer eng.Close()
	replEngine = eng
	replCatalog = mongo.NewCatalog(eng)

	cat := mongo.NewCatalog(eng)
	cat.EnsureDatabase("testdb")
	cat.EnsureCollection("testdb", "testcoll")
	coll := mongo.NewCollection("testdb", "testcoll", eng, cat)

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Invalid filter syntax
	replFind(coll, "{invalid")

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)

	if !bytes.Contains(buf.Bytes(), []byte("Parse error")) {
		t.Errorf("Expected 'Parse error' in output, got: %s", buf.String())
	}
}

func TestReplUpdate_InvalidArgs(t *testing.T) {
	// Setup test engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, _ := engine.Open(opts)
	defer eng.Close()
	replEngine = eng
	replCatalog = mongo.NewCatalog(eng)

	cat := mongo.NewCatalog(eng)
	cat.EnsureDatabase("testdb")
	cat.EnsureCollection("testdb", "testcoll")
	coll := mongo.NewCollection("testdb", "testcoll", eng, cat)

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Missing update argument
	replUpdate(coll, "{name: 'test'}")

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)

	if !bytes.Contains(buf.Bytes(), []byte("Usage:")) {
		t.Errorf("Expected 'Usage:' in output, got: %s", buf.String())
	}
}

func TestReplUpdate_InvalidFilter(t *testing.T) {
	// Setup test engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, _ := engine.Open(opts)
	defer eng.Close()
	replEngine = eng
	replCatalog = mongo.NewCatalog(eng)

	cat := mongo.NewCatalog(eng)
	cat.EnsureDatabase("testdb")
	cat.EnsureCollection("testdb", "testcoll")
	coll := mongo.NewCollection("testdb", "testcoll", eng, cat)

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Invalid filter syntax
	replUpdate(coll, "{invalid}, {$set: {a: 1}}")

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)

	if !bytes.Contains(buf.Bytes(), []byte("Filter parse error")) {
		t.Errorf("Expected 'Filter parse error' in output, got: %s", buf.String())
	}
}

func TestReplDelete_InvalidFilter(t *testing.T) {
	// Setup test engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, _ := engine.Open(opts)
	defer eng.Close()
	replEngine = eng
	replCatalog = mongo.NewCatalog(eng)

	cat := mongo.NewCatalog(eng)
	cat.EnsureDatabase("testdb")
	cat.EnsureCollection("testdb", "testcoll")
	coll := mongo.NewCollection("testdb", "testcoll", eng, cat)

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Invalid filter syntax
	replDelete(coll, "{invalid")

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)

	if !bytes.Contains(buf.Bytes(), []byte("Parse error")) {
		t.Errorf("Expected 'Parse error' in output, got: %s", buf.String())
	}
}

func TestSplitArgs_Empty(t *testing.T) {
	result := splitArgs("")
	if len(result) != 0 {
		t.Errorf("Expected empty result for empty input, got: %v", result)
	}
}

func TestSplitArgs_Single(t *testing.T) {
	result := splitArgs("{a: 1}")
	if len(result) != 1 {
		t.Errorf("Expected 1 result, got: %v", result)
	}
	if result[0] != "{a: 1}" {
		t.Errorf("Expected '{a: 1}', got: %s", result[0])
	}
}

func TestValueToString_Array(t *testing.T) {
	arr := []bson.Value{bson.VInt32(1), bson.VInt32(2), bson.VInt32(3)}
	v := bson.VArray(arr)
	result := valueToString(v)
	expected := "[1, 2, 3]"
	if result != expected {
		t.Errorf("Expected %s, got: %s", expected, result)
	}
}

func TestValueToString_Document(t *testing.T) {
	doc := bson.NewDocument()
	doc.Set("a", bson.VInt32(1))
	v := bson.VDoc(doc)
	result := valueToString(v)
	if result != "{...}" {
		t.Errorf("Expected '{...}', got: %s", result)
	}
}

func TestValueToString_DateTime(t *testing.T) {
	v := bson.VDateTime(1234567890)
	result := valueToString(v)
	expected := "Date(1234567890)"
	if result != expected {
		t.Errorf("Expected %s, got: %s", expected, result)
	}
}

func TestValueToString_Unknown(t *testing.T) {
	// Create a value with unknown type
	v := bson.Value{Type: 0x99}
	result := valueToString(v)
	expected := "<153>"
	if result != expected {
		t.Errorf("Expected %s, got: %s", expected, result)
	}
}

// Reset globals after tests
func TestMain(m *testing.M) {
	code := m.Run()
	// Reset globals
	replEngine = nil
	replCatalog = nil
	currentDB = "test"
	os.Exit(code)
}
