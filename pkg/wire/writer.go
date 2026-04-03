package wire

import (
	"encoding/binary"
	"net"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// WriteMessage writes an OP_MSG response to a connection.
func WriteMessage(conn net.Conn, responseTo uint32, responseID uint32, doc *bson.Document) error {
	return WriteOpMsg(conn, responseTo, responseID, doc)
}

// WriteOpMsg writes an OP_MSG response to a connection.
func WriteOpMsg(conn net.Conn, responseTo uint32, responseID uint32, doc *bson.Document) error {
	encoded := bson.Encode(doc)

	// OP_MSG layout: header(16) + flagBits(4) + section kind(1) + bson doc
	totalLen := 16 + 4 + 1 + len(encoded)
	buf := make([]byte, 0, totalLen)

	// Header
	buf = binary.LittleEndian.AppendUint32(buf, uint32(totalLen)) // messageLength
	buf = binary.LittleEndian.AppendUint32(buf, responseID)       // requestID
	buf = binary.LittleEndian.AppendUint32(buf, responseTo)       // responseTo
	buf = binary.LittleEndian.AppendUint32(buf, OpMsg)            // opCode

	// FlagBits
	buf = binary.LittleEndian.AppendUint32(buf, 0)

	// Section kind 0 (body)
	buf = append(buf, 0)

	// BSON document
	buf = append(buf, encoded...)

	_, err := conn.Write(buf)
	return err
}

// WriteOpReply writes an OP_REPLY response to a connection.
func WriteOpReply(conn net.Conn, responseTo uint32, responseID uint32, docs []*bson.Document) error {
	// Calculate total size
	var docsSize int
	for _, doc := range docs {
		docsSize += len(bson.Encode(doc))
	}

	// OP_REPLY layout: header(16) + responseFlags(4) + cursorID(8) + startingFrom(4) + numberReturned(4) + documents
	totalLen := 16 + 4 + 8 + 4 + 4 + docsSize
	buf := make([]byte, 0, totalLen)

	// Header
	buf = binary.LittleEndian.AppendUint32(buf, uint32(totalLen)) // messageLength
	buf = binary.LittleEndian.AppendUint32(buf, responseID)       // requestID
	buf = binary.LittleEndian.AppendUint32(buf, responseTo)       // responseTo
	buf = binary.LittleEndian.AppendUint32(buf, OpReply)          // opCode

	// Response flags (0 = no special flags)
	buf = binary.LittleEndian.AppendUint32(buf, 0)

	// Cursor ID (0 for non-cursor responses)
	buf = binary.LittleEndian.AppendUint64(buf, 0)

	// Starting from (0)
	buf = binary.LittleEndian.AppendUint32(buf, 0)

	// Number returned
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(docs)))

	// Documents
	for _, doc := range docs {
		buf = append(buf, bson.Encode(doc)...)
	}

	_, err := conn.Write(buf)
	return err
}
