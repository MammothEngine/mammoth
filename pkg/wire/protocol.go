package wire

import "github.com/mammothengine/mammoth/pkg/bson"

const (
	// OpCode constants
	OpReply       uint32 = 1    // Reply to a client request
	OpUpdate      uint32 = 2001 // Update document
	OpInsert      uint32 = 2002 // Insert document
	OpQuery       uint32 = 2004 // Query a collection
	OpGetMore     uint32 = 2005 // Get more data from a query
	OpDelete      uint32 = 2006 // Delete one or more documents
	OpKillCursors uint32 = 2007 // Notify database that the client has finished with a cursor
	OpMsg         uint32 = 2013 // Message using the extensible wire protocol
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
	Query      *OPQuery
	Reply      *OPReply
	RemoteAddr string
	ConnID     uint64
}

// OPMsg represents an OP_MSG message (MongoDB 3.6+).
type OPMsg struct {
	FlagBits uint32
	Sections []Section
	Checksum uint32
}

// OPQuery represents an OP_QUERY message (legacy, but still used for handshake).
type OPQuery struct {
	Flags                int32
	FullCollectionName   string
	NumberToSkip         int32
	NumberToReturn       int32
	Query                *bson.Document
	ReturnFieldsSelector *bson.Document
}

// OPReply represents an OP_REPLY message (response to OP_QUERY).
type OPReply struct {
	ResponseFlags  int32
	CursorID       int64
	StartingFrom   int32
	NumberReturned int32
	Documents      []*bson.Document
}

// Section is a section within an OP_MSG.
type Section struct {
	Kind       byte
	Body       *bson.Document // Kind 0: body BSON document
	Identifier string         // Kind 1: document sequence identifier
	DocSeq     *DocSequence   // Kind 1: document sequence
}

// DocSequence represents a document sequence (kind 1 section).
type DocSequence struct {
	Size      int32
	Documents [][]byte // Raw BSON documents
}

// Command returns the command name (first key in the body section).
func (m *Message) Command() string {
	if m.Msg != nil {
		for _, s := range m.Msg.Sections {
			if s.Kind == 0 && s.Body != nil {
				keys := s.Body.Keys()
				if len(keys) > 0 {
					return keys[0]
				}
			}
		}
	}
	if m.Query != nil && m.Query.Query != nil {
		keys := m.Query.Query.Keys()
		if len(keys) > 0 {
			return keys[0]
		}
	}
	return ""
}

// Body returns the body document from the first kind-0 section.
func (m *Message) Body() *bson.Document {
	if m.Msg != nil {
		for _, s := range m.Msg.Sections {
			if s.Kind == 0 && s.Body != nil {
				return s.Body
			}
		}
	}
	if m.Query != nil {
		return m.Query.Query
	}
	return nil
}

// GetDocumentSequence returns raw BSON documents from a kind-1 section with the given identifier.
func (m *Message) GetDocumentSequence(identifier string) [][]byte {
	if m.Msg == nil {
		return nil
	}
	for _, s := range m.Msg.Sections {
		if s.Kind == 1 && s.Identifier == identifier && s.DocSeq != nil {
			return s.DocSeq.Documents
		}
	}
	return nil
}

// SetReply sets the OPReply for this message (for building responses).
func (m *Message) SetReply(reply *OPReply) {
	m.Reply = reply
}

// SetMsg sets the OPMsg for this message.
func (m *Message) SetMsg(msg *OPMsg) {
	m.Msg = msg
}

// SetQuery sets the OPQuery for this message.
func (m *Message) SetQuery(query *OPQuery) {
	m.Query = query
}

// IsHandshake returns true if this is a handshake message (isMaster/hello).
func (m *Message) IsHandshake() bool {
	cmd := m.Command()
	return cmd == "hello" || cmd == "isMaster" || cmd == "ismaster"
}

// ResponseHeader creates a response header for this message.
func (m *Message) ResponseHeader(responseTo uint32) MsgHeader {
	return MsgHeader{
		RequestID:  m.Header.RequestID + 1,
		ResponseTo: responseTo,
		OpCode:     OpReply,
	}
}

// String returns a human-readable representation of the message.
func (m *Message) String() string {
	return "Message{OpCode: " + opcodeName(m.Header.OpCode) + ", Command: " + m.Command() + "}"
}

func opcodeName(op uint32) string {
	switch op {
	case OpReply:
		return "OP_REPLY"
	case OpUpdate:
		return "OP_UPDATE"
	case OpInsert:
		return "OP_INSERT"
	case OpQuery:
		return "OP_QUERY"
	case OpGetMore:
		return "OP_GET_MORE"
	case OpDelete:
		return "OP_DELETE"
	case OpKillCursors:
		return "OP_KILL_CURSORS"
	case OpMsg:
		return "OP_MSG"
	default:
		return "UNKNOWN"
	}
}
