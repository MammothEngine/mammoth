package wire

import (
	"encoding/binary"
	"net"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// WriteMessage writes an OP_MSG response to a connection.
func WriteMessage(conn net.Conn, responseTo uint32, responseID uint32, doc *bson.Document) error {
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
