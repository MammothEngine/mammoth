package wire

import (
	"encoding/binary"
	"net"
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

func TestWriteOpMsg(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	doc := bson.NewDocument()
	doc.Set("ok", bson.VDouble(1.0))
	doc.Set("message", bson.VString("test"))

	// Write OP_MSG response
	go func() {
		err := WriteOpMsg(client, 42, 99, doc)
		if err != nil {
			t.Errorf("WriteOpMsg: %v", err)
		}
	}()

	// Read header
	headerBuf := make([]byte, headerSize)
	n, err := server.Read(headerBuf)
	if err != nil || n != headerSize {
		t.Fatalf("read header: %v", err)
	}

	length := binary.LittleEndian.Uint32(headerBuf[0:4])
	requestID := binary.LittleEndian.Uint32(headerBuf[4:8])
	responseTo := binary.LittleEndian.Uint32(headerBuf[8:12])
	opCode := binary.LittleEndian.Uint32(headerBuf[12:16])

	if requestID != 99 {
		t.Errorf("RequestID = %d, want 99", requestID)
	}
	if responseTo != 42 {
		t.Errorf("ResponseTo = %d, want 42", responseTo)
	}
	if opCode != OpMsg {
		t.Errorf("OpCode = %d, want %d (OpMsg)", opCode, OpMsg)
	}

	// Read remaining (flagBits + section kind + document)
	remaining := int(length) - headerSize
	body := make([]byte, remaining)
	if _, err := server.Read(body); err != nil {
		t.Fatalf("read body: %v", err)
	}

	// Verify flagBits (should be 0)
	flagBits := binary.LittleEndian.Uint32(body[0:4])
	if flagBits != 0 {
		t.Errorf("FlagBits = %d, want 0", flagBits)
	}

	// Verify section kind (should be 0 for body)
	if body[4] != 0 {
		t.Errorf("Section kind = %d, want 0", body[4])
	}

	// Verify document can be decoded
	decoded, err := bson.Decode(body[5:])
	if err != nil {
		t.Fatalf("Failed to decode document: %v", err)
	}

	if val, ok := decoded.Get("ok"); !ok || val.Double() != 1.0 {
		t.Error("Missing or wrong 'ok' field")
	}
}

func TestWriteOpReply(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	// Create test documents
	doc1 := bson.NewDocument()
	doc1.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc1.Set("name", bson.VString("Item 1"))

	doc2 := bson.NewDocument()
	doc2.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc2.Set("name", bson.VString("Item 2"))

	docs := []*bson.Document{doc1, doc2}

	// Write OP_REPLY
	go func() {
		err := WriteOpReply(client, 42, 99, docs)
		if err != nil {
			t.Errorf("WriteOpReply: %v", err)
		}
	}()

	// Read header
	headerBuf := make([]byte, headerSize)
	n, err := server.Read(headerBuf)
	if err != nil || n != headerSize {
		t.Fatalf("read header: %v", err)
	}

	length := binary.LittleEndian.Uint32(headerBuf[0:4])
	requestID := binary.LittleEndian.Uint32(headerBuf[4:8])
	responseTo := binary.LittleEndian.Uint32(headerBuf[8:12])
	opCode := binary.LittleEndian.Uint32(headerBuf[12:16])

	if requestID != 99 {
		t.Errorf("RequestID = %d, want 99", requestID)
	}
	if responseTo != 42 {
		t.Errorf("ResponseTo = %d, want 42", responseTo)
	}
	if opCode != OpReply {
		t.Errorf("OpCode = %d, want %d (OpReply)", opCode, OpReply)
	}

	// Read remaining body
	remaining := int(length) - headerSize
	body := make([]byte, remaining)
	if _, err := server.Read(body); err != nil {
		t.Fatalf("read body: %v", err)
	}

	// Parse OP_REPLY fields
	pos := 0

	// Response flags
	flags := binary.LittleEndian.Uint32(body[pos:])
	pos += 4
	if flags != 0 {
		t.Errorf("ResponseFlags = %d, want 0", flags)
	}

	// Cursor ID
	cursorID := binary.LittleEndian.Uint64(body[pos:])
	pos += 8
	if cursorID != 0 {
		t.Errorf("CursorID = %d, want 0", cursorID)
	}

	// Starting from
	startingFrom := binary.LittleEndian.Uint32(body[pos:])
	pos += 4
	if startingFrom != 0 {
		t.Errorf("StartingFrom = %d, want 0", startingFrom)
	}

	// Number returned
	numberReturned := binary.LittleEndian.Uint32(body[pos:])
	pos += 4
	if numberReturned != 2 {
		t.Errorf("NumberReturned = %d, want 2", numberReturned)
	}

	// Verify documents
	for i := 0; i < 2; i++ {
		docSize := int(binary.LittleEndian.Uint32(body[pos:]))
		decoded, err := bson.Decode(body[pos : pos+docSize])
		if err != nil {
			t.Fatalf("Failed to decode doc %d: %v", i, err)
		}
		if name, ok := decoded.Get("name"); !ok {
			t.Errorf("Doc %d missing 'name' field", i)
		} else if name.String() != "Item "+string('1'+byte(i)) {
			t.Errorf("Doc %d name = %q, want 'Item %d'", i, name.String(), i+1)
		}
		pos += docSize
	}
}

func TestWriteOpReply_Empty(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	// Write empty OP_REPLY
	go func() {
		err := WriteOpReply(client, 1, 2, []*bson.Document{})
		if err != nil {
			t.Errorf("WriteOpReply: %v", err)
		}
	}()

	// Read header
	headerBuf := make([]byte, headerSize)
	server.Read(headerBuf)

	length := binary.LittleEndian.Uint32(headerBuf[0:4])

	// Read body
	remaining := int(length) - headerSize
	body := make([]byte, remaining)
	server.Read(body)

	// Check number returned is 0
	numberReturned := binary.LittleEndian.Uint32(body[16:20])
	if numberReturned != 0 {
		t.Errorf("NumberReturned = %d, want 0", numberReturned)
	}
}
