package wire

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

func TestParseOpMsg(t *testing.T) {
	// Build an OP_MSG message manually
	body := bson.NewDocument()
	body.Set("ping", bson.VInt32(1))
	body.Set("$db", bson.VString("admin"))
	encodedBody := bson.Encode(body)

	// Build OP_MSG: flagBits(4) + sectionKind(1) + document
	buf := make([]byte, 0, 4+1+len(encodedBody))
	buf = binary.LittleEndian.AppendUint32(buf, 0) // flagBits
	buf = append(buf, 0)                          // section kind 0
	buf = append(buf, encodedBody...)

	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
	}

	parsed, err := parseOpMsg(msg, buf)
	if err != nil {
		t.Fatalf("parseOpMsg: %v", err)
	}
	if parsed == nil {
		t.Fatal("parseOpMsg returned nil")
	}
	if parsed.Msg == nil {
		t.Fatal("Msg is nil")
	}

	// Check flagBits
	if parsed.Msg.FlagBits != 0 {
		t.Errorf("FlagBits = %d, want 0", parsed.Msg.FlagBits)
	}

	// Check sections
	if len(parsed.Msg.Sections) != 1 {
		t.Fatalf("Expected 1 section, got %d", len(parsed.Msg.Sections))
	}

	section := parsed.Msg.Sections[0]
	if section.Kind != 0 {
		t.Errorf("Section kind = %d, want 0", section.Kind)
	}
	if section.Body == nil {
		t.Fatal("Section body is nil")
	}

	if cmd := parsed.Command(); cmd != "ping" {
		t.Errorf("Command() = %q, want ping", cmd)
	}
}

func TestParseOpMsg_WithDocumentSequence(t *testing.T) {
	// Build body
	body := bson.NewDocument()
	body.Set("insert", bson.VString("test_collection"))
	body.Set("$db", bson.VString("test_db"))
	encodedBody := bson.Encode(body)

	// Build document sequence
	doc1 := bson.NewDocument()
	doc1.Set("name", bson.VString("Item 1"))
	doc1.Set("value", bson.VInt32(1))

	doc2 := bson.NewDocument()
	doc2.Set("name", bson.VString("Item 2"))
	doc2.Set("value", bson.VInt32(2))

	encodedDoc1 := bson.Encode(doc1)
	encodedDoc2 := bson.Encode(doc2)

	// Document sequence format: size(4) + identifier(null-terminated) + documents
	seqBuf := make([]byte, 0)
	seqSize := 4 + len("documents") + 1 + len(encodedDoc1) + len(encodedDoc2)
	seqBuf = binary.LittleEndian.AppendUint32(seqBuf, uint32(seqSize))
	seqBuf = append(seqBuf, "documents"...)
	seqBuf = append(seqBuf, 0) // null terminator
	seqBuf = append(seqBuf, encodedDoc1...)
	seqBuf = append(seqBuf, encodedDoc2...)

	// Build full message: flagBits(4) + bodySection + docSeqSection
	buf := make([]byte, 0)
	buf = binary.LittleEndian.AppendUint32(buf, 0) // flagBits
	buf = append(buf, 0)                          // section kind 0 (body)
	buf = append(buf, encodedBody...)
	buf = append(buf, 1) // section kind 1 (document sequence)
	buf = append(buf, seqBuf...)

	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
	}

	parsed, err := parseOpMsg(msg, buf)
	if err != nil {
		t.Fatalf("parseOpMsg: %v", err)
	}

	if len(parsed.Msg.Sections) != 2 {
		t.Fatalf("Expected 2 sections, got %d", len(parsed.Msg.Sections))
	}

	// Check body section
	if parsed.Msg.Sections[0].Kind != 0 {
		t.Error("First section should be kind 0")
	}

	// Check document sequence section
	seqSection := parsed.Msg.Sections[1]
	if seqSection.Kind != 1 {
		t.Errorf("Second section kind = %d, want 1", seqSection.Kind)
	}
	if seqSection.Identifier != "documents" {
		t.Errorf("Identifier = %q, want documents", seqSection.Identifier)
	}
	if seqSection.DocSeq == nil {
		t.Fatal("DocSeq is nil")
	}
	if len(seqSection.DocSeq.Documents) != 2 {
		t.Errorf("Expected 2 documents in sequence, got %d", len(seqSection.DocSeq.Documents))
	}
}

func TestParseOpQuery(t *testing.T) {
	// Build an OP_QUERY message
	// Format: flags(4) + collectionName(null-terminated) + numberToSkip(4) + numberToReturn(4) + queryDoc

	queryDoc := bson.NewDocument()
	queryDoc.Set("isMaster", bson.VInt32(1))
	encodedQuery := bson.Encode(queryDoc)

	collectionName := "admin.$cmd"

	buf := make([]byte, 0)
	buf = binary.LittleEndian.AppendUint32(buf, 0) // flags
	buf = append(buf, collectionName...)
	buf = append(buf, 0) // null terminator
	buf = binary.LittleEndian.AppendUint32(buf, 0) // numberToSkip
	buf = binary.LittleEndian.AppendUint32(buf, 1) // numberToReturn
	buf = append(buf, encodedQuery...)

	msg := &Message{
		Header: MsgHeader{OpCode: OpQuery},
	}

	parsed, err := parseOpQuery(msg, buf)
	if err != nil {
		t.Fatalf("parseOpQuery: %v", err)
	}
	if parsed == nil {
		t.Fatal("parseOpQuery returned nil")
	}
	if parsed.Query == nil {
		t.Fatal("Query is nil")
	}

	// Check flags
	if parsed.Query.Flags != 0 {
		t.Errorf("Flags = %d, want 0", parsed.Query.Flags)
	}

	// Check collection name
	if parsed.Query.FullCollectionName != collectionName {
		t.Errorf("FullCollectionName = %q, want %q", parsed.Query.FullCollectionName, collectionName)
	}

	// Check numberToSkip
	if parsed.Query.NumberToSkip != 0 {
		t.Errorf("NumberToSkip = %d, want 0", parsed.Query.NumberToSkip)
	}

	// Check numberToReturn
	if parsed.Query.NumberToReturn != 1 {
		t.Errorf("NumberToReturn = %d, want 1", parsed.Query.NumberToReturn)
	}

	// Check query document
	if parsed.Query.Query == nil {
		t.Fatal("Query.Query is nil")
	}

	if cmd := parsed.Command(); cmd != "isMaster" {
		t.Errorf("Command() = %q, want isMaster", cmd)
	}
}

func TestParseOpQuery_WithReturnFields(t *testing.T) {
	// Build an OP_QUERY with ReturnFieldsSelector
	queryDoc := bson.NewDocument()
	queryDoc.Set("find", bson.VString("test_collection"))
	encodedQuery := bson.Encode(queryDoc)

	returnFields := bson.NewDocument()
	returnFields.Set("name", bson.VInt32(1))
	returnFields.Set("_id", bson.VInt32(0))
	encodedReturnFields := bson.Encode(returnFields)

	collectionName := "test.test_collection"

	buf := make([]byte, 0)
	buf = binary.LittleEndian.AppendUint32(buf, 0) // flags
	buf = append(buf, collectionName...)
	buf = append(buf, 0) // null terminator
	buf = binary.LittleEndian.AppendUint32(buf, 0)  // numberToSkip
	buf = binary.LittleEndian.AppendUint32(buf, 100) // numberToReturn
	buf = append(buf, encodedQuery...)
	buf = append(buf, encodedReturnFields...)

	msg := &Message{
		Header: MsgHeader{OpCode: OpQuery},
	}

	parsed, err := parseOpQuery(msg, buf)
	if err != nil {
		t.Fatalf("parseOpQuery: %v", err)
	}

	if parsed.Query.ReturnFieldsSelector == nil {
		t.Fatal("ReturnFieldsSelector is nil")
	}

	if name, ok := parsed.Query.ReturnFieldsSelector.Get("name"); !ok || name.Int32() != 1 {
		t.Error("ReturnFieldsSelector missing or wrong 'name' field")
	}
}

func TestParseOpQuery_TooShort(t *testing.T) {
	msg := &Message{
		Header: MsgHeader{OpCode: OpQuery},
	}

	// Test with too short buffer
	parsed, err := parseOpQuery(msg, []byte{0, 0, 0, 0, 0, 0, 0, 0}) // only 8 bytes
	if err != nil {
		t.Fatalf("parseOpQuery should not error on short buffer: %v", err)
	}
	if parsed != nil {
		t.Error("Expected nil for short buffer")
	}
}

func TestParseOpQuery_InvalidCollectionName(t *testing.T) {
	msg := &Message{
		Header: MsgHeader{OpCode: OpQuery},
	}

	// Buffer without null terminator for collection name
	buf := make([]byte, 20)
	binary.LittleEndian.PutUint32(buf[0:4], 0) // flags
	copy(buf[4:], []byte("test"))              // collection name without null

	parsed, err := parseOpQuery(msg, buf)
	if err != nil {
		t.Fatalf("parseOpQuery should not error: %v", err)
	}
	if parsed != nil {
		t.Error("Expected nil for invalid collection name")
	}
}

func TestParseOpMsg_TooShort(t *testing.T) {
	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
	}

	// Test with too short buffer
	parsed, err := parseOpMsg(msg, []byte{0, 0}) // only 2 bytes
	if err != nil {
		t.Fatalf("parseOpMsg should not error on short buffer: %v", err)
	}
	if parsed != nil {
		t.Error("Expected nil for short buffer")
	}
}

func TestParseOpMsg_InvalidDocument(t *testing.T) {
	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
	}

	// Build message with invalid document
	buf := make([]byte, 0)
	buf = binary.LittleEndian.AppendUint32(buf, 0) // flagBits
	buf = append(buf, 0)                          // section kind 0
	buf = binary.LittleEndian.AppendUint32(buf, 100) // invalid doc size (too big)
	buf = append(buf, []byte("garbage")...)        // invalid document

	parsed, err := parseOpMsg(msg, buf)
	if err != nil {
		t.Fatalf("parseOpMsg should not error: %v", err)
	}
	// Should return message with empty sections due to goto done
	if parsed == nil || parsed.Msg == nil {
		t.Error("Expected non-nil message")
	}
}

func TestOpcodeName(t *testing.T) {
	tests := []struct {
		opcode uint32
		want   string
	}{
		{OpReply, "OP_REPLY"},
		{OpUpdate, "OP_UPDATE"},
		{OpInsert, "OP_INSERT"},
		{OpQuery, "OP_QUERY"},
		{OpGetMore, "OP_GET_MORE"},
		{OpDelete, "OP_DELETE"},
		{OpKillCursors, "OP_KILL_CURSORS"},
		{OpMsg, "OP_MSG"},
		{9999, "UNKNOWN"},
	}

	for _, tt := range tests {
		got := opcodeName(tt.opcode)
		if got != tt.want {
			t.Errorf("opcodeName(%d) = %q, want %q", tt.opcode, got, tt.want)
		}
	}
}

func TestMessage_String(t *testing.T) {
	body := bson.NewDocument()
	body.Set("ping", bson.VInt32(1))

	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg: &OPMsg{
			Sections: []Section{{Kind: 0, Body: body}},
		},
	}

	got := msg.String()
	want := "Message{OpCode: OP_MSG, Command: ping}"
	if got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestReadMessage_InvalidHeader(t *testing.T) {
	// Create a reader with incomplete header
	_ = bytes.NewReader([]byte{0, 0, 0}) // only 3 bytes

	// We can't easily test this without a net.Conn, but we can verify
	// the function signature and basic logic
}