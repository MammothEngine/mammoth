package main

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

func TestReplDrop(t *testing.T) {
	// Setup test engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, _ := engine.Open(opts)
	defer eng.Close()
	replEngine = eng
	replCatalog = mongo.NewCatalog(eng)
	currentDB = "testdb"

	// Create collection first
	replCatalog.EnsureDatabase("testdb")
	replCatalog.EnsureCollection("testdb", "testcoll")

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cat := mongo.NewCatalog(eng)
	cat.EnsureDatabase("testdb")
	cat.EnsureCollection("testdb", "tocoll")
	coll := mongo.NewCollection("testdb", "tocoll", eng, cat)

	replDrop(coll, "tocoll")

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)

	if !bytes.Contains(buf.Bytes(), []byte("ok")) {
		t.Errorf("Expected 'ok' in output, got: %s", buf.String())
	}
}

func TestCmdShow_Databases(t *testing.T) {
	// Setup test engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, _ := engine.Open(opts)
	defer eng.Close()
	replEngine = eng
	replCatalog = mongo.NewCatalog(eng)

	// Create a database
	replCatalog.EnsureDatabase("testdb1")
	replCatalog.EnsureDatabase("testdb2")

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmdShow([]string{"show", "dbs"})

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	if !strings.Contains(output, "testdb1") && !strings.Contains(output, "testdb2") {
		t.Errorf("Expected database names in output, got: %s", output)
	}
}

func TestCmdShow_Collections(t *testing.T) {
	// Setup test engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, _ := engine.Open(opts)
	defer eng.Close()
	replEngine = eng
	replCatalog = mongo.NewCatalog(eng)
	currentDB = "testdb"

	// Create collections
	replCatalog.EnsureDatabase("testdb")
	replCatalog.EnsureCollection("testdb", "coll1")
	replCatalog.EnsureCollection("testdb", "coll2")

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmdShow([]string{"show", "collections"})

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	if !strings.Contains(output, "coll1") || !strings.Contains(output, "coll2") {
		t.Errorf("Expected collection names in output, got: %s", output)
	}
}

func TestCmdShow_Unknown(t *testing.T) {
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

	cmdShow([]string{"show", "unknown"})

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	if !strings.Contains(output, "Unknown") {
		t.Errorf("Expected 'Unknown' in output, got: %s", output)
	}
}

func TestReplInsert(t *testing.T) {
	// Setup test engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, _ := engine.Open(opts)
	defer eng.Close()
	replEngine = eng
	replCatalog = mongo.NewCatalog(eng)
	currentDB = "testdb"

	// Create collection
	replCatalog.EnsureDatabase("testdb")
	replCatalog.EnsureCollection("testdb", "testcoll")
	coll := mongo.NewCollection("testdb", "testcoll", eng, replCatalog)

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	replInsert(coll, `{name: "test", value: 42}`)

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	if !strings.Contains(output, "Inserted") {
		t.Errorf("Expected 'Inserted' in output, got: %s", output)
	}
}

func TestReplInsert_NoArgs(t *testing.T) {
	// Setup test engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, _ := engine.Open(opts)
	defer eng.Close()
	replEngine = eng
	replCatalog = mongo.NewCatalog(eng)
	currentDB = "testdb"

	coll := mongo.NewCollection("testdb", "testcoll", eng, replCatalog)

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	replInsert(coll, "")

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	if !strings.Contains(output, "Usage") {
		t.Errorf("Expected 'Usage' in output, got: %s", output)
	}
}

func TestReplFind(t *testing.T) {
	// Setup test engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, _ := engine.Open(opts)
	defer eng.Close()
	replEngine = eng
	replCatalog = mongo.NewCatalog(eng)
	currentDB = "testdb"

	// Insert test data
	replCatalog.EnsureDatabase("testdb")
	replCatalog.EnsureCollection("testdb", "testcoll")
	coll := mongo.NewCollection("testdb", "testcoll", eng, replCatalog)

	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("name", bson.VString("alice"))
	coll.InsertOne(doc)

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	replFind(coll, `{name: "alice"}`)

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	if !strings.Contains(output, "alice") {
		t.Errorf("Expected 'alice' in output, got: %s", output)
	}
}

func TestReplFind_Empty(t *testing.T) {
	// Setup test engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, _ := engine.Open(opts)
	defer eng.Close()
	replEngine = eng
	replCatalog = mongo.NewCatalog(eng)
	currentDB = "testdb"

	replCatalog.EnsureDatabase("testdb")
	replCatalog.EnsureCollection("testdb", "emptycoll")
	coll := mongo.NewCollection("testdb", "emptycoll", eng, replCatalog)

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	replFind(coll, "")

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	if !strings.Contains(output, "No documents") {
		t.Errorf("Expected 'No documents' in output, got: %s", output)
	}
}

func TestReplFind_ManyResults(t *testing.T) {
	// Setup test engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, _ := engine.Open(opts)
	defer eng.Close()
	replEngine = eng
	replCatalog = mongo.NewCatalog(eng)
	currentDB = "testdb"

	replCatalog.EnsureDatabase("testdb")
	replCatalog.EnsureCollection("testdb", "testcoll")
	coll := mongo.NewCollection("testdb", "testcoll", eng, replCatalog)

	// Insert 25 documents (more than 20 to trigger "... and X more" message)
	for i := 0; i < 25; i++ {
		doc := bson.NewDocument()
		doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
		doc.Set("num", bson.VInt32(int32(i)))
		coll.InsertOne(doc)
	}

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	replFind(coll, "")

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	if !strings.Contains(output, "and 5 more") {
		t.Errorf("Expected 'and 5 more' in output, got: %s", output)
	}
}

func TestReplUpdate(t *testing.T) {
	// Setup test engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, _ := engine.Open(opts)
	defer eng.Close()
	replEngine = eng
	replCatalog = mongo.NewCatalog(eng)
	currentDB = "testdb"

	replCatalog.EnsureDatabase("testdb")
	replCatalog.EnsureCollection("testdb", "testcoll")
	coll := mongo.NewCollection("testdb", "testcoll", eng, replCatalog)

	// Insert test document
	doc := bson.NewDocument()
	id := bson.NewObjectID()
	doc.Set("_id", bson.VObjectID(id))
	doc.Set("value", bson.VInt32(10))
	coll.InsertOne(doc)

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	replUpdate(coll, `{value: 10}, {$set: {value: 20}}`)

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	if !strings.Contains(output, "Modified") {
		t.Errorf("Expected 'Modified' in output, got: %s", output)
	}
}

func TestReplDelete(t *testing.T) {
	// Setup test engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, _ := engine.Open(opts)
	defer eng.Close()
	replEngine = eng
	replCatalog = mongo.NewCatalog(eng)
	currentDB = "testdb"

	replCatalog.EnsureDatabase("testdb")
	replCatalog.EnsureCollection("testdb", "testcoll")
	coll := mongo.NewCollection("testdb", "testcoll", eng, replCatalog)

	// Insert test document
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("name", bson.VString("delete_me"))
	coll.InsertOne(doc)

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	replDelete(coll, `{name: "delete_me"}`)

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	if !strings.Contains(output, "Deleted") {
		t.Errorf("Expected 'Deleted' in output, got: %s", output)
	}
}

func TestCmdDatabaseOperation_Insert(t *testing.T) {
	// Setup test engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, _ := engine.Open(opts)
	defer eng.Close()
	replEngine = eng
	replCatalog = mongo.NewCatalog(eng)
	currentDB = "testdb"

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmdDatabaseOperation([]string{"db.testcoll.insert({name: \"test\"})"})

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	if !strings.Contains(output, "Inserted") {
		t.Errorf("Expected 'Inserted' in output, got: %s", output)
	}
}

func TestCmdDatabaseOperation_Find(t *testing.T) {
	// Setup test engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, _ := engine.Open(opts)
	defer eng.Close()
	replEngine = eng
	replCatalog = mongo.NewCatalog(eng)
	currentDB = "testdb"

	// Insert test data
	replCatalog.EnsureDatabase("testdb")
	replCatalog.EnsureCollection("testdb", "testcoll")
	coll := mongo.NewCollection("testdb", "testcoll", eng, replCatalog)
	doc := bson.NewDocument()
	doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc.Set("name", bson.VString("findme"))
	coll.InsertOne(doc)

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmdDatabaseOperation([]string{"db.testcoll.find({name: \"findme\"})"})

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	if !strings.Contains(output, "findme") {
		t.Errorf("Expected 'findme' in output, got: %s", output)
	}
}

func TestCmdDatabaseOperation_Count(t *testing.T) {
	// Setup test engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, _ := engine.Open(opts)
	defer eng.Close()
	replEngine = eng
	replCatalog = mongo.NewCatalog(eng)
	currentDB = "testdb"

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmdDatabaseOperation([]string{"db.testcoll.count()"})

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	// Should output a number
	if output == "" {
		t.Errorf("Expected output, got empty")
	}
}

func TestCmdDatabaseOperation_Drop(t *testing.T) {
	// Setup test engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, _ := engine.Open(opts)
	defer eng.Close()
	replEngine = eng
	replCatalog = mongo.NewCatalog(eng)
	currentDB = "testdb"

	replCatalog.EnsureDatabase("testdb")
	replCatalog.EnsureCollection("testdb", "testcoll")

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmdDatabaseOperation([]string{"db.testcoll.drop()"})

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	if !strings.Contains(output, "ok") {
		t.Errorf("Expected 'ok' in output, got: %s", output)
	}
}

func TestCmdDatabaseOperation_UnknownMethod(t *testing.T) {
	// Setup test engine
	dir := t.TempDir()
	opts := engine.DefaultOptions(dir)
	eng, _ := engine.Open(opts)
	defer eng.Close()
	replEngine = eng
	replCatalog = mongo.NewCatalog(eng)
	currentDB = "testdb"

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	cmdDatabaseOperation([]string{"db.testcoll.unknownmethod()"})

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	if !strings.Contains(output, "Unknown method") {
		t.Errorf("Expected 'Unknown method' in output, got: %s", output)
	}
}
