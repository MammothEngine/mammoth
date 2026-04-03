package wire

import (
	"encoding/binary"
	"net"
	"testing"

	"github.com/mammothengine/mammoth/pkg/bson"
)

func BenchmarkParseOpMsg_SmallDocument(b *testing.B) {
	doc := bson.NewDocument()
	doc.Set("ping", bson.VInt32(1))
	doc.Set("$db", bson.VString("admin"))
	encodedBody := bson.Encode(doc)

	buf := make([]byte, 0, 4+1+len(encodedBody))
	buf = binary.LittleEndian.AppendUint32(buf, 0)
	buf = append(buf, 0)
	buf = append(buf, encodedBody...)

	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parseOpMsg(msg, buf)
	}
}

func BenchmarkParseOpMsg_LargeDocument(b *testing.B) {
	doc := bson.NewDocument()
	doc.Set("insert", bson.VString("test_collection"))
	doc.Set("$db", bson.VString("test_db"))
	doc.Set("ordered", bson.VBool(true))

	// Add document sequence
	doc1 := bson.NewDocument()
	doc1.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc1.Set("name", bson.VString("Item 1"))
	doc1.Set("value", bson.VInt32(1))

	doc2 := bson.NewDocument()
	doc2.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc2.Set("name", bson.VString("Item 2"))
	doc2.Set("value", bson.VInt32(2))

	encodedBody := bson.Encode(doc)
	encodedDoc1 := bson.Encode(doc1)
	encodedDoc2 := bson.Encode(doc2)

	seqBuf := make([]byte, 0)
	seqSize := 4 + len("documents") + 1 + len(encodedDoc1) + len(encodedDoc2)
	seqBuf = binary.LittleEndian.AppendUint32(seqBuf, uint32(seqSize))
	seqBuf = append(seqBuf, "documents"...)
	seqBuf = append(seqBuf, 0)
	seqBuf = append(seqBuf, encodedDoc1...)
	seqBuf = append(seqBuf, encodedDoc2...)

	buf := make([]byte, 0)
	buf = binary.LittleEndian.AppendUint32(buf, 0)
	buf = append(buf, 0)
	buf = append(buf, encodedBody...)
	buf = append(buf, 1)
	buf = append(buf, seqBuf...)

	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parseOpMsg(msg, buf)
	}
}

func BenchmarkParseOpQuery(b *testing.B) {
	queryDoc := bson.NewDocument()
	queryDoc.Set("isMaster", bson.VInt32(1))
	encodedQuery := bson.Encode(queryDoc)

	collectionName := "admin.$cmd"

	buf := make([]byte, 0)
	buf = binary.LittleEndian.AppendUint32(buf, 0)
	buf = append(buf, collectionName...)
	buf = append(buf, 0)
	buf = binary.LittleEndian.AppendUint32(buf, 0)
	buf = binary.LittleEndian.AppendUint32(buf, 1)
	buf = append(buf, encodedQuery...)

	msg := &Message{
		Header: MsgHeader{OpCode: OpQuery},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parseOpQuery(msg, buf)
	}
}

func BenchmarkMessage_Command(b *testing.B) {
	body := bson.NewDocument()
	body.Set("find", bson.VString("test_collection"))
	body.Set("$db", bson.VString("test_db"))

	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg: &OPMsg{
			Sections: []Section{{Kind: 0, Body: body}},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = msg.Command()
	}
}

func BenchmarkMessage_GetDocumentSequence(b *testing.B) {
	body := bson.NewDocument()
	body.Set("insert", bson.VString("test_collection"))

	doc1 := bson.NewDocument()
	doc1.Set("name", bson.VString("Item 1"))
	rawDoc1 := bson.Encode(doc1)

	msg := &Message{
		Header: MsgHeader{OpCode: OpMsg},
		Msg: &OPMsg{
			Sections: []Section{
				{Kind: 0, Body: body},
				{
					Kind:       1,
					Identifier: "documents",
					DocSeq: &DocSequence{
						Documents: [][]byte{rawDoc1},
					},
				},
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = msg.GetDocumentSequence("documents")
	}
}

func BenchmarkWriteOpMsg(b *testing.B) {
	doc := bson.NewDocument()
	doc.Set("ok", bson.VDouble(1.0))
	doc.Set("message", bson.VString("benchmark response"))

	// Use a pipe to discard writes
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	// Drain the server side
	go func() {
		buf := make([]byte, 4096)
		for {
			_, err := server.Read(buf)
			if err != nil {
				return
			}
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		WriteOpMsg(client, 42, uint32(i), doc)
	}
}

func BenchmarkWriteOpReply(b *testing.B) {
	doc1 := bson.NewDocument()
	doc1.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc1.Set("name", bson.VString("Item 1"))

	doc2 := bson.NewDocument()
	doc2.Set("_id", bson.VObjectID(bson.NewObjectID()))
	doc2.Set("name", bson.VString("Item 2"))

	docs := []*bson.Document{doc1, doc2}

	// Use a pipe to discard writes
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	// Drain the server side
	go func() {
		buf := make([]byte, 4096)
		for {
			_, err := server.Read(buf)
			if err != nil {
				return
			}
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		WriteOpReply(client, 42, uint32(i), docs)
	}
}

func BenchmarkConnTracker_AddRemove(b *testing.B) {
	ct := NewConnTracker()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ct.Add(uint64(i))
		ct.Remove(uint64(i))
	}
}

func BenchmarkConnTracker_Count(b *testing.B) {
	ct := NewConnTracker()
	for i := 0; i < 1000; i++ {
		ct.Add(uint64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ct.Count()
	}
}

func BenchmarkOpcodeName(b *testing.B) {
	opcodes := []uint32{OpMsg, OpQuery, OpReply, OpInsert, OpUpdate, OpDelete}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = opcodeName(opcodes[i%len(opcodes)])
	}
}
