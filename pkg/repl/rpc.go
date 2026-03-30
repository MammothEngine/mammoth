package repl

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
)

// RPC message types
const (
	MsgAppendEntries    = 1
	MsgAppendEntriesResp = 2
	MsgRequestVote      = 3
	MsgRequestVoteResp  = 4
	MsgInstallSnapshot  = 5
	MsgInstallSnapshotResp = 6
	MsgPropose          = 7
	MsgProposeResp      = 8
)

// RPCRequest is a generic RPC request.
type RPCRequest struct {
	Type    byte
	From    uint64
	Payload []byte
}

// RPCResponse is a generic RPC response.
type RPCResponse struct {
	Type    byte
	Payload []byte
}

// AppendEntriesRequest is the Raft AppendEntries RPC.
type AppendEntriesRequest struct {
	Term         uint64 `json:"term"`
	LeaderID     uint64 `json:"leader_id"`
	PrevLogIndex uint64 `json:"prev_log_index"`
	PrevLogTerm  uint64 `json:"prev_log_term"`
	LeaderCommit uint64 `json:"leader_commit"`
	Entries      []LogEntry `json:"entries"`
}

// AppendEntriesResponse is the Raft AppendEntries response.
type AppendEntriesResponse struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
	MatchIndex uint64 `json:"match_index"`
}

// RequestVoteRequest is the Raft RequestVote RPC.
type RequestVoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  uint64 `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

// RequestVoteResponse is the Raft RequestVote response.
type RequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

// InstallSnapshotRequest is for snapshot transfer.
type InstallSnapshotRequest struct {
	Term              uint64 `json:"term"`
	LeaderID          uint64 `json:"leader_id"`
	LastIncludedIndex uint64 `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
	Offset            uint64 `json:"offset"`
	Data              []byte `json:"data"`
	Done              bool   `json:"done"`
}

// InstallSnapshotResponse is the snapshot install response.
type InstallSnapshotResponse struct {
	Term uint64 `json:"term"`
}

// ProposeRequest is a client proposal.
type ProposeRequest struct {
	Data []byte `json:"data"`
}

// ProposeResponse is the proposal response.
type ProposeResponse struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Ok    bool   `json:"ok"`
}

// EncodeRPC encodes an RPC request for wire transport.
// Format: type(1) + from(8) + payload_len(4) + payload
func EncodeRPC(req *RPCRequest) []byte {
	buf := make([]byte, 1+8+4+len(req.Payload))
	buf[0] = req.Type
	binary.BigEndian.PutUint64(buf[1:9], req.From)
	binary.BigEndian.PutUint32(buf[9:13], uint32(len(req.Payload)))
	copy(buf[13:], req.Payload)
	return buf
}

// DecodeRPC decodes an RPC request from wire format.
func DecodeRPC(data []byte) (*RPCRequest, error) {
	if len(data) < 13 {
		return nil, ErrInvalidRPC
	}
	req := &RPCRequest{
		Type: data[0],
		From: binary.BigEndian.Uint64(data[1:9]),
	}
	plen := binary.BigEndian.Uint32(data[9:13])
	if len(data) < 13+int(plen) {
		return nil, ErrInvalidRPC
	}
	req.Payload = data[13 : 13+plen]
	return req, nil
}

// EncodeRPCResponse encodes an RPC response for wire transport.
func EncodeRPCResponse(resp *RPCResponse) []byte {
	buf := make([]byte, 1+4+len(resp.Payload))
	buf[0] = resp.Type
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(resp.Payload)))
	copy(buf[5:], resp.Payload)
	return buf
}

// DecodeRPCResponse decodes an RPC response from wire format.
func DecodeRPCResponse(data []byte) (*RPCResponse, error) {
	if len(data) < 5 {
		return nil, ErrInvalidRPC
	}
	resp := &RPCResponse{
		Type: data[0],
	}
	plen := binary.BigEndian.Uint32(data[1:5])
	if len(data) < 5+int(plen) {
		return nil, ErrInvalidRPC
	}
	resp.Payload = data[5 : 5+plen]
	return resp, nil
}

// JSON encode/decode helpers for RPC payloads.
func encodePayload(v interface{}) []byte {
	data, _ := json.Marshal(v)
	return data
}

func decodeAppendEntriesReq(data []byte) (*AppendEntriesRequest, error) {
	var req AppendEntriesRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, err
	}
	return &req, nil
}

func decodeAppendEntriesResp(data []byte) (*AppendEntriesResponse, error) {
	var resp AppendEntriesResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func decodeRequestVoteReq(data []byte) (*RequestVoteRequest, error) {
	var req RequestVoteRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, err
	}
	return &req, nil
}

func decodeRequestVoteResp(data []byte) (*RequestVoteResponse, error) {
	var resp RequestVoteResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// LogEntry represents a single Raft log entry.
type LogEntry struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Data  []byte `json:"data"`
	Type  byte   `json:"type"` // 0 = normal, 1 = config change
}

// HasSamePrefix checks if two byte slices share a prefix.
func HasSamePrefix(a, b []byte, prefixLen int) bool {
	if len(a) < prefixLen || len(b) < prefixLen {
		return false
	}
	return bytes.Equal(a[:prefixLen], b[:prefixLen])
}
