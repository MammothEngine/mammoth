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
