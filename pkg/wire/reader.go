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

	if header.OpCode != OpMsg {
		return nil, nil // Skip non-OP_MSG
	}

	// Read remaining bytes
	remaining := int(header.Length) - headerSize
	if remaining <= 0 {
		return nil, nil
	}

	body := make([]byte, remaining)
	if _, err := io.ReadFull(conn, body); err != nil {
		return nil, err
	}

	// Parse OP_MSG
	msg := &OPMsg{
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
			msg.Sections = append(msg.Sections, Section{
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
			msg.Sections = append(msg.Sections, Section{
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
	return &Message{
		Header: header,
		Msg:    msg,
	}, nil
}
