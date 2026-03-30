package wire

import "github.com/mammothengine/mammoth/pkg/bson"

const (
	// OpCode constants
	OpReply   uint32 = 1
	OpUpdate  uint32 = 2001
	OpInsert  uint32 = 2002
	OpQuery   uint32 = 2004
	OpGetMore uint32 = 2005
	OpMsg     uint32 = 2013
)

const headerSize = 16

// MsgHeader is the 16-byte wire protocol message header.
type MsgHeader struct {
	Length     uint32 // Total message size including header
	RequestID  uint32 // Identifier for this message
	ResponseTo uint32 // ResponseTo from original request (0 if not a response)
	OpCode     uint32 // OpCode for the message
}

// Message represents a parsed wire protocol message.
type Message struct {
	Header     MsgHeader
	Msg        *OPMsg
	RemoteAddr string
}

// OPMsg represents an OP_MSG message (MongoDB 3.6+).
type OPMsg struct {
	FlagBits uint32
	Sections []Section
	Checksum uint32
}

// Section is a section within an OP_MSG.
type Section struct {
	Kind       byte
	Body       *bson.Document  // Kind 0: body BSON document
	Identifier string          // Kind 1: document sequence identifier
	DocSeq     *DocSequence    // Kind 1: document sequence
}

// DocSequence represents a document sequence (kind 1 section).
type DocSequence struct {
	Size      int32
	Documents [][]byte // Raw BSON documents
}

// Command returns the command name (first key in the body section).
func (m *Message) Command() string {
	if m.Msg == nil {
		return ""
	}
	for _, s := range m.Msg.Sections {
		if s.Kind == 0 && s.Body != nil {
			keys := s.Body.Keys()
			if len(keys) > 0 {
				return keys[0]
			}
		}
	}
	return ""
}

// Body returns the body document from the first kind-0 section.
func (m *Message) Body() *bson.Document {
	if m.Msg == nil {
		return nil
	}
	for _, s := range m.Msg.Sections {
		if s.Kind == 0 && s.Body != nil {
			return s.Body
		}
	}
	return nil
}
