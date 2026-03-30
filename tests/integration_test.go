package tests

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
	"github.com/mammothengine/mammoth/pkg/wire"
)

func setupServer(t *testing.T) (*wire.Server, *engine.Engine) {
	t.Helper()
	eng, err := engine.Open(engine.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	cat := mongo.NewCatalog(eng)
	handler := wire.NewHandler(eng, cat)

	srv, err := wire.NewServer(wire.ServerConfig{
		Addr:    "127.0.0.1:0",
		Handler: handler,
	})
	if err != nil {
		eng.Close()
		t.Fatalf("NewServer: %v", err)
	}

	go srv.Serve()
	return srv, eng
}

func dialServer(t *testing.T, addr string) net.Conn {
	t.Helper()
	var conn net.Conn
	var err error
	for i := 0; i < 10; i++ {
		conn, err = net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	return conn
}

func sendCommand(t *testing.T, conn net.Conn, cmdDoc *bson.Document) *bson.Document {
	t.Helper()
	encoded := bson.Encode(cmdDoc)

	totalLen := 16 + 4 + 1 + len(encoded)
	buf := make([]byte, 0, totalLen)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(totalLen))
	buf = binary.LittleEndian.AppendUint32(buf, 1) // requestID
	buf = binary.LittleEndian.AppendUint32(buf, 0) // responseTo
	buf = binary.LittleEndian.AppendUint32(buf, 2013)
	buf = binary.LittleEndian.AppendUint32(buf, 0) // flagBits
	buf = append(buf, 0)                           // section kind 0
	buf = append(buf, encoded...)

	if _, err := conn.Write(buf); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Read response
	headerBuf := make([]byte, 16)
	if _, err := io.ReadFull(conn, headerBuf); err != nil {
		t.Fatalf("read header: %v", err)
	}

	length := binary.LittleEndian.Uint32(headerBuf[0:4])
	remaining := int(length) - 16
	respBody := make([]byte, remaining)
	if _, err := io.ReadFull(conn, respBody); err != nil {
		t.Fatalf("read body: %v", err)
	}

	// Skip flagBits (4) + section kind (1)
	docBytes := respBody[5:]
	doc, err := bson.Decode(docBytes)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return doc
}

func getCursorDoc(t *testing.T, resp *bson.Document) *bson.Document {
	t.Helper()
	v, ok := resp.Get("cursor")
	if !ok || v.Type != bson.TypeDocument {
		t.Fatal("response missing cursor document")
	}
	return v.DocumentValue()
}

func getBatch(t *testing.T, cursorDoc *bson.Document, key string) bson.Array {
	t.Helper()
	v, ok := cursorDoc.Get(key)
	if !ok || v.Type != bson.TypeArray {
		t.Fatalf("cursor missing %q", key)
	}
	return v.ArrayValue()
}

func TestIntegration_HelloHandshake(t *testing.T) {
	srv, eng := setupServer(t)
	defer eng.Close()
	defer srv.Close()

	conn := dialServer(t, srv.Addr())
	defer conn.Close()

	// hello
	helloDoc := bson.NewDocument()
	helloDoc.Set("hello", bson.VInt32(1))
	helloDoc.Set("$db", bson.VString("admin"))

	resp := sendCommand(t, conn, helloDoc)
	if ok, _ := resp.Get("ok"); ok.Double() != 1.0 {
		t.Errorf("hello ok = %v, want 1.0", ok.Double())
	}
	if v, _ := resp.Get("isWritablePrimary"); !v.Boolean() {
		t.Error("hello should return isWritablePrimary: true")
	}

	// buildInfo
	biDoc := bson.NewDocument()
	biDoc.Set("buildInfo", bson.VInt32(1))
	biDoc.Set("$db", bson.VString("admin"))
	resp = sendCommand(t, conn, biDoc)
	if v, _ := resp.Get("version"); v.String() != "7.0.0" {
		t.Errorf("buildInfo version = %q, want 7.0.0", v.String())
	}

	// ping
	pingDoc := bson.NewDocument()
	pingDoc.Set("ping", bson.VInt32(1))
	pingDoc.Set("$db", bson.VString("admin"))
	resp = sendCommand(t, conn, pingDoc)
	if ok, _ := resp.Get("ok"); ok.Double() != 1.0 {
		t.Errorf("ping ok = %v, want 1.0", ok.Double())
	}
}

func TestIntegration_InsertFind(t *testing.T) {
	srv, eng := setupServer(t)
	defer eng.Close()
	defer srv.Close()

	conn := dialServer(t, srv.Addr())
	defer conn.Close()

	// Insert
	doc := bson.NewDocument()
	doc.Set("name", bson.VString("alice"))
	doc.Set("age", bson.VInt32(30))

	insertDoc := bson.NewDocument()
	insertDoc.Set("insert", bson.VString("users"))
	insertDoc.Set("$db", bson.VString("testdb"))
	insertDoc.Set("documents", bson.VArray(bson.A(bson.VDoc(doc))))

	resp := sendCommand(t, conn, insertDoc)
	if ok, _ := resp.Get("ok"); ok.Double() != 1.0 {
		t.Fatalf("insert ok = %v, want 1.0", ok.Double())
	}

	// Find
	findDoc := bson.NewDocument()
	findDoc.Set("find", bson.VString("users"))
	findDoc.Set("$db", bson.VString("testdb"))

	resp = sendCommand(t, conn, findDoc)
	cursorDoc := getCursorDoc(t, resp)
	batch := getBatch(t, cursorDoc, "firstBatch")
	if len(batch) != 1 {
		t.Fatalf("find batch = %d, want 1", len(batch))
	}
}

func TestIntegration_UpdateDelete(t *testing.T) {
	srv, eng := setupServer(t)
	defer eng.Close()
	defer srv.Close()

	conn := dialServer(t, srv.Addr())
	defer conn.Close()

	// Insert
	doc := bson.NewDocument()
	doc.Set("name", bson.VString("bob"))
	doc.Set("age", bson.VInt32(25))

	insertDoc := bson.NewDocument()
	insertDoc.Set("insert", bson.VString("users"))
	insertDoc.Set("$db", bson.VString("testdb"))
	insertDoc.Set("documents", bson.VArray(bson.A(bson.VDoc(doc))))
	sendCommand(t, conn, insertDoc)

	// Update
	q := bson.NewDocument()
	q.Set("name", bson.VString("bob"))
	setDoc := bson.NewDocument()
	setDoc.Set("age", bson.VInt32(26))
	u := bson.NewDocument()
	u.Set("$set", bson.VDoc(setDoc))
	updateEntry := bson.NewDocument()
	updateEntry.Set("q", bson.VDoc(q))
	updateEntry.Set("u", bson.VDoc(u))

	updateDoc := bson.NewDocument()
	updateDoc.Set("update", bson.VString("users"))
	updateDoc.Set("$db", bson.VString("testdb"))
	updateDoc.Set("updates", bson.VArray(bson.A(bson.VDoc(updateEntry))))

	resp := sendCommand(t, conn, updateDoc)
	if n, _ := resp.Get("n"); n.Int32() != 1 {
		t.Errorf("update n = %d, want 1", n.Int32())
	}

	// Delete
	delQ := bson.NewDocument()
	delQ.Set("name", bson.VString("bob"))
	delEntry := bson.NewDocument()
	delEntry.Set("q", bson.VDoc(delQ))

	deleteDoc := bson.NewDocument()
	deleteDoc.Set("delete", bson.VString("users"))
	deleteDoc.Set("$db", bson.VString("testdb"))
	deleteDoc.Set("deletes", bson.VArray(bson.A(bson.VDoc(delEntry))))

	resp = sendCommand(t, conn, deleteDoc)
	if n, _ := resp.Get("n"); n.Int32() != 1 {
		t.Errorf("delete n = %d, want 1", n.Int32())
	}
}

func TestIntegration_CreateDrop(t *testing.T) {
	srv, eng := setupServer(t)
	defer eng.Close()
	defer srv.Close()

	conn := dialServer(t, srv.Addr())
	defer conn.Close()

	// Create
	createDoc := bson.NewDocument()
	createDoc.Set("create", bson.VString("mycoll"))
	createDoc.Set("$db", bson.VString("testdb"))
	resp := sendCommand(t, conn, createDoc)
	if ok, _ := resp.Get("ok"); ok.Double() != 1.0 {
		t.Fatalf("create ok = %v", ok.Double())
	}

	// Drop
	dropDoc := bson.NewDocument()
	dropDoc.Set("drop", bson.VString("mycoll"))
	dropDoc.Set("$db", bson.VString("testdb"))
	resp = sendCommand(t, conn, dropDoc)
	if ok, _ := resp.Get("ok"); ok.Double() != 1.0 {
		t.Errorf("drop ok = %v", ok.Double())
	}
}

func TestIntegration_Cursors(t *testing.T) {
	srv, eng := setupServer(t)
	defer eng.Close()
	defer srv.Close()

	conn := dialServer(t, srv.Addr())
	defer conn.Close()

	// Insert 5 docs
	for i := 0; i < 5; i++ {
		doc := bson.NewDocument()
		doc.Set("i", bson.VInt32(int32(i)))

		insertDoc := bson.NewDocument()
		insertDoc.Set("insert", bson.VString("nums"))
		insertDoc.Set("$db", bson.VString("testdb"))
		insertDoc.Set("documents", bson.VArray(bson.A(bson.VDoc(doc))))
		sendCommand(t, conn, insertDoc)
	}

	// Find with small batch
	findDoc := bson.NewDocument()
	findDoc.Set("find", bson.VString("nums"))
	findDoc.Set("$db", bson.VString("testdb"))
	findDoc.Set("batchSize", bson.VInt32(2))

	resp := sendCommand(t, conn, findDoc)
	cursorDoc := getCursorDoc(t, resp)
	cursorIDVal, _ := cursorDoc.Get("id")
	var cursorID int64
	if cursorIDVal.Type == bson.TypeInt64 {
		cursorID = cursorIDVal.Int64()
	}

	if cursorID == 0 {
		t.Fatal("cursor should have non-zero ID when results remain")
	}

	// getMore
	getMoreDoc := bson.NewDocument()
	getMoreDoc.Set("getMore", bson.VInt64(cursorID))
	getMoreDoc.Set("collection", bson.VString("nums"))
	getMoreDoc.Set("batchSize", bson.VInt32(2))
	getMoreDoc.Set("$db", bson.VString("testdb"))

	resp = sendCommand(t, conn, getMoreDoc)
	cursorDoc2 := getCursorDoc(t, resp)
	nextBatch := getBatch(t, cursorDoc2, "nextBatch")
	if len(nextBatch) != 2 {
		t.Errorf("getMore batch = %d, want 2", len(nextBatch))
	}

	// killCursors
	killDoc := bson.NewDocument()
	killDoc.Set("killCursors", bson.VString("nums"))
	killDoc.Set("cursors", bson.VArray(bson.A(cursorIDVal)))
	killDoc.Set("$db", bson.VString("testdb"))

	resp = sendCommand(t, conn, killDoc)
	if ok, _ := resp.Get("ok"); ok.Double() != 1.0 {
		t.Errorf("killCursors ok = %v", ok.Double())
	}
}
