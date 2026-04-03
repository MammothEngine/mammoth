package wire

import (
	"encoding/binary"
	"io"
	"net"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// ReadMessage reads a complete MongoDB wire protocol message from a connection.
func ReadMessage(conn net.Conn) (*Message, error) {
	// Read 16-byte header
	headerBuf := make([]byte, headerSize)
	if _, err := io.ReadFull(conn, headerBuf); err != nil {
		return nil, err
	}

	var header MsgHeader
	header.Length = binary.LittleEndian.Uint32(headerBuf[0:4])
	header.RequestID = binary.LittleEndian.Uint32(headerBuf[4:8])
	header.ResponseTo = binary.LittleEndian.Uint32(headerBuf[8:12])
	header.OpCode = binary.LittleEndian.Uint32(headerBuf[12:16])

	// Read remaining bytes
	remaining := int(header.Length) - headerSize
	if remaining < 0 {
		return nil, nil
	}

	var body []byte
	if remaining > 0 {
		body = make([]byte, remaining)
		if _, err := io.ReadFull(conn, body); err != nil {
			return nil, err
		}
	}

	msg := &Message{
		Header: header,
	}

	switch header.OpCode {
	case OpMsg:
		return parseOpMsg(msg, body)
	case OpQuery:
		return parseOpQuery(msg, body)
	default:
		// For other opcodes, return nil (not supported yet)
		return nil, nil
	}
}

func parseOpMsg(msg *Message, body []byte) (*Message, error) {
	if len(body) < 4 {
		return nil, nil
	}

	// Parse OP_MSG
	opMsg := &OPMsg{
		FlagBits: binary.LittleEndian.Uint32(body[0:4]),
	}
	pos := 4

	for pos < len(body) {
		kind := body[pos]
		pos++

		switch kind {
		case 0: // Body section
			if pos+4 > len(body) {
				goto done
			}
			docSize := int(binary.LittleEndian.Uint32(body[pos:]))
			if pos+docSize > len(body) {
				goto done
			}
			doc, err := bson.Decode(body[pos : pos+docSize])
			if err != nil {
				goto done
			}
			opMsg.Sections = append(opMsg.Sections, Section{
				Kind: 0,
				Body: doc,
			})
			pos += docSize

		case 1: // Document sequence
			if pos+4 > len(body) {
				goto done
			}
			seqSize := int(binary.LittleEndian.Uint32(body[pos:]))
			seqEnd := pos + seqSize
			pos += 4
			idStart := pos
			for pos < seqEnd && body[pos] != 0 {
				pos++
			}
			identifier := string(body[idStart:pos])
			if pos < seqEnd {
				pos++ // skip null
			}
			var docs [][]byte
			for pos < seqEnd {
				if pos+4 > seqEnd {
					break
				}
				d := int(binary.LittleEndian.Uint32(body[pos:]))
				if pos+d > seqEnd {
					break
				}
				docs = append(docs, append([]byte{}, body[pos:pos+d]...))
				pos += d
			}
			opMsg.Sections = append(opMsg.Sections, Section{
				Kind:       1,
				Identifier: identifier,
				DocSeq: &DocSequence{
					Size:      int32(seqSize),
					Documents: docs,
				},
			})
			pos = seqEnd
		}
	}

done:
	msg.Msg = opMsg
	return msg, nil
}

func parseOpQuery(msg *Message, body []byte) (*Message, error) {
	if len(body) < 16 {
		return nil, nil
	}

	query := &OPQuery{}
	pos := 0

	// Flags (4 bytes)
	query.Flags = int32(binary.LittleEndian.Uint32(body[pos:]))
	pos += 4

	// Full collection name (null-terminated string)
	nameStart := pos
	for pos < len(body) && body[pos] != 0 {
		pos++
	}
	if pos >= len(body) {
		return nil, nil
	}
	query.FullCollectionName = string(body[nameStart:pos])
	pos++ // skip null

	if pos+8 > len(body) {
		return nil, nil
	}

	// Number to skip (4 bytes)
	query.NumberToSkip = int32(binary.LittleEndian.Uint32(body[pos:]))
	pos += 4

	// Number to return (4 bytes)
	query.NumberToReturn = int32(binary.LittleEndian.Uint32(body[pos:]))
	pos += 4

	// Query document
	if pos+4 > len(body) {
		return nil, nil
	}
	queryDocSize := int(binary.LittleEndian.Uint32(body[pos:]))
	if pos+queryDocSize > len(body) {
		return nil, nil
	}
	queryDoc, err := bson.Decode(body[pos : pos+queryDocSize])
	if err != nil {
		return nil, nil
	}
	query.Query = queryDoc
	pos += queryDocSize

	// Optional: ReturnFieldsSelector
	if pos < len(body) {
		if pos+4 <= len(body) {
			selectorDocSize := int(binary.LittleEndian.Uint32(body[pos:]))
			if pos+selectorDocSize <= len(body) {
				selectorDoc, err := bson.Decode(body[pos : pos+selectorDocSize])
				if err == nil {
					query.ReturnFieldsSelector = selectorDoc
				}
			}
		}
	}

	msg.Query = query
	return msg, nil
}
