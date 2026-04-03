package wire

import (
	"encoding/binary"
	"net"
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

func TestMessage_Command(t *testing.T) {
	body := bson.NewDocument()
	body.Set("ping", bson.VInt32(1))
	body.Set("$db", bson.VString("admin"))

	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg: &OPMsg{
			Sections: []Section{
				{Kind: 0, Body: body},
			},
		},
	}

	if cmd := msg.Command(); cmd != "ping" {
		t.Errorf("Command() = %q, want ping", cmd)
	}
}

func TestMessage_Body(t *testing.T) {
	body := bson.NewDocument()
	body.Set("hello", bson.VBool(true))
	body.Set("$db", bson.VString("admin"))

	msg := &Message{
		Msg: &OPMsg{
			Sections: []Section{
				{Kind: 0, Body: body},
			},
		},
	}

	got := msg.Body()
	if got == nil {
		t.Fatal("Body() returned nil")
	}
	if _, ok := got.Get("hello"); !ok {
		t.Error("Body() missing 'hello' key")
	}
}

func TestMessage_EmptyMsg(t *testing.T) {
	msg := &Message{}
	if cmd := msg.Command(); cmd != "" {
		t.Errorf("Command() on nil msg = %q, want empty", cmd)
	}
	if body := msg.Body(); body != nil {
		t.Errorf("Body() on nil msg = %v, want nil", body)
	}
}

func TestReadWriteMessage_Roundtrip(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	body := bson.NewDocument()
	body.Set("ping", bson.VInt32(1))
	body.Set("$db", bson.VString("admin"))

	// Write from client side (responseTo=0, responseID=42)
	go func() {
		WriteMessage(client, 0, 42, body)
	}()

	// Read from server side
	msg, err := ReadMessage(server)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if msg == nil {
		t.Fatal("ReadMessage returned nil")
	}
	if msg.Header.RequestID != 42 {
		t.Fatalf("RequestID = %d, want 42", msg.Header.RequestID)
	}
	if msg.Header.OpCode != OpMsg {
		t.Errorf("OpCode = %d, want %d", msg.Header.OpCode, OpMsg)
	}
	if cmd := msg.Command(); cmd != "ping" {
		t.Errorf("Command() = %q, want ping", cmd)
	}
}

func TestWriteMessage_Response(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	response := bson.NewDocument()
	response.Set("ok", bson.VDouble(1.0))

	// Write response: responseTo=42, responseID=99
	go func() {
		WriteMessage(server, 42, 99, response)
	}()

	// Read the response
	headerBuf := make([]byte, headerSize)
	n, err := client.Read(headerBuf)
	if err != nil || n != headerSize {
		t.Fatalf("read header: %v", err)
	}

	length := binary.LittleEndian.Uint32(headerBuf[0:4])
	responseTo := binary.LittleEndian.Uint32(headerBuf[8:12])

	if responseTo != 42 {
		t.Errorf("ResponseTo = %d, want 42", responseTo)
	}

	// Read remaining
	remaining := int(length) - headerSize
	body := make([]byte, remaining)
	if _, err := client.Read(body); err != nil {
		t.Fatalf("read body: %v", err)
	}
}

func TestMessage_GetDocumentSequence(t *testing.T) {
	// Create a message with document sequence
	body := bson.NewDocument()
	body.Set("insert", bson.VString("test_collection"))
	body.Set("$db", bson.VString("test_db"))

	// Create document sequence
	doc1 := bson.NewDocument()
	doc1.Set("name", bson.VString("Item 1"))
	doc1.Set("value", bson.VInt32(1))

	doc2 := bson.NewDocument()
	doc2.Set("name", bson.VString("Item 2"))
	doc2.Set("value", bson.VInt32(2))

	rawDoc1 := bson.Encode(doc1)
	rawDoc2 := bson.Encode(doc2)

	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg: &OPMsg{
			FlagBits: 0,
			Sections: []Section{
				{Kind: 0, Body: body},
				{
					Kind:       1,
					Identifier: "documents",
					DocSeq: &DocSequence{
						Size:      int32(len(rawDoc1) + len(rawDoc2) + 4 + 10), // Approximate
						Documents: [][]byte{rawDoc1, rawDoc2},
					},
				},
			},
		},
	}

	// Test GetDocumentSequence
	docs := msg.GetDocumentSequence("documents")
	if docs == nil {
		t.Fatal("GetDocumentSequence returned nil")
	}
	if len(docs) != 2 {
		t.Errorf("Expected 2 documents, got %d", len(docs))
	}

	// Verify first document
	decoded1, err := bson.Decode(docs[0])
	if err != nil {
		t.Fatalf("Failed to decode first doc: %v", err)
	}
	if val, ok := decoded1.Get("name"); !ok || val.String() != "Item 1" {
		t.Error("First document name mismatch")
	}

	// Test non-existent sequence
	nonExistent := msg.GetDocumentSequence("nonexistent")
	if nonExistent != nil {
		t.Error("GetDocumentSequence for non-existent should return nil")
	}

	// Test nil Msg
	msgNoDocSeq := &Message{Header: MsgHeader{OpCode: OpMsg}}
	if msgNoDocSeq.GetDocumentSequence("documents") != nil {
		t.Error("GetDocumentSequence on nil Msg should return nil")
	}
}

func TestMessage_IsHandshake(t *testing.T) {
	tests := []struct {
		name    string
		command string
		want    bool
	}{
		{"hello", "hello", true},
		{"isMaster", "isMaster", true},
		{"ismaster", "ismaster", true},
		{"ping", "ping", false},
		{"find", "find", false},
		{"insert", "insert", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := bson.NewDocument()
			body.Set(tt.command, bson.VInt32(1))

			msg := &Message{
				Msg: &OPMsg{
					Sections: []Section{{Kind: 0, Body: body}},
				},
			}

			if got := msg.IsHandshake(); got != tt.want {
				t.Errorf("IsHandshake() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMessage_ResponseHeader(t *testing.T) {
	msg := &Message{
		Header: MsgHeader{RequestID: 42, ResponseTo: 0},
	}

	respHeader := msg.ResponseHeader(42)

	if respHeader.ResponseTo != 42 {
		t.Errorf("ResponseTo = %d, want 42", respHeader.ResponseTo)
	}
	if respHeader.RequestID != 43 {
		t.Errorf("RequestID = %d, want 43", respHeader.RequestID)
	}
	if respHeader.OpCode != OpReply {
		t.Errorf("OpCode = %d, want %d (OpReply)", respHeader.OpCode, OpReply)
	}
}
