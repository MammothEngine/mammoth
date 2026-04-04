package repl

import (
	"bytes"
	"testing"
)

func TestEncodeRPCResponse(t *testing.T) {
	resp := &RPCResponse{
		Type:    MsgAppendEntriesResp,
		Payload: []byte(`{"term": 1, "success": true}`),
	}

	encoded := EncodeRPCResponse(resp)
	if len(encoded) == 0 {
		t.Error("expected non-empty encoded response")
	}

	// Verify format: type(1) + payload_len(4) + payload
	if encoded[0] != MsgAppendEntriesResp {
		t.Errorf("expected type %d, got %d", MsgAppendEntriesResp, encoded[0])
	}
}

func TestDecodeRPCResponse(t *testing.T) {
	// Create valid encoded response
	resp := &RPCResponse{
		Type:    MsgRequestVoteResp,
		Payload: []byte(`{"term": 2, "vote_granted": true}`),
	}
	encoded := EncodeRPCResponse(resp)

	// Decode it
	decoded, err := DecodeRPCResponse(encoded)
	if err != nil {
		t.Fatalf("DecodeRPCResponse: %v", err)
	}

	if decoded.Type != MsgRequestVoteResp {
		t.Errorf("expected type %d, got %d", MsgRequestVoteResp, decoded.Type)
	}

	if !bytes.Equal(decoded.Payload, resp.Payload) {
		t.Errorf("payload mismatch: got %s, want %s", decoded.Payload, resp.Payload)
	}
}

func TestDecodeRPCResponse_InvalidData(t *testing.T) {
	// Too short
	_, err := DecodeRPCResponse([]byte{1, 2, 3})
	if err == nil {
		t.Error("expected error for data too short")
	}

	// Payload length mismatch (claims 100 bytes but only has 5)
	data := []byte{1, 0, 0, 0, 100, 1, 2, 3, 4, 5}
	_, err = DecodeRPCResponse(data)
	if err == nil {
		t.Error("expected error for payload length mismatch")
	}
}

func TestHasSamePrefix(t *testing.T) {
	tests := []struct {
		name      string
		a         []byte
		b         []byte
		prefixLen int
		expected  bool
	}{
		{"same prefix", []byte("hello world"), []byte("hello there"), 5, true},
		{"different prefix", []byte("hello world"), []byte("goodbye"), 5, false},
		{"a too short", []byte("hi"), []byte("hello"), 5, false},
		{"b too short", []byte("hello"), []byte("hi"), 5, false},
		{"empty", []byte{}, []byte{}, 1, false},
		{"zero length prefix", []byte("abc"), []byte("def"), 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := HasSamePrefix(tt.a, tt.b, tt.prefixLen)
			if result != tt.expected {
				t.Errorf("HasSamePrefix() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestDecodeAppendEntriesReq(t *testing.T) {
	validJSON := `{"term": 1, "leader_id": 2, "prev_log_index": 0, "prev_log_term": 0, "leader_commit": 0, "entries": []}`

	req, err := decodeAppendEntriesReq([]byte(validJSON))
	if err != nil {
		t.Fatalf("decodeAppendEntriesReq: %v", err)
	}

	if req.Term != 1 {
		t.Errorf("expected term 1, got %d", req.Term)
	}
	if req.LeaderID != 2 {
		t.Errorf("expected leader_id 2, got %d", req.LeaderID)
	}

	// Invalid JSON
	_, err = decodeAppendEntriesReq([]byte(`invalid`))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestDecodeAppendEntriesResp(t *testing.T) {
	validJSON := `{"term": 1, "success": true, "match_index": 5}`

	resp, err := decodeAppendEntriesResp([]byte(validJSON))
	if err != nil {
		t.Fatalf("decodeAppendEntriesResp: %v", err)
	}

	if resp.Term != 1 {
		t.Errorf("expected term 1, got %d", resp.Term)
	}
	if !resp.Success {
		t.Error("expected success=true")
	}

	// Invalid JSON
	_, err = decodeAppendEntriesResp([]byte(`invalid`))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestDecodeRequestVoteReq(t *testing.T) {
	validJSON := `{"term": 1, "candidate_id": 2, "last_log_index": 5, "last_log_term": 1}`

	req, err := decodeRequestVoteReq([]byte(validJSON))
	if err != nil {
		t.Fatalf("decodeRequestVoteReq: %v", err)
	}

	if req.Term != 1 {
		t.Errorf("expected term 1, got %d", req.Term)
	}
	if req.CandidateID != 2 {
		t.Errorf("expected candidate_id 2, got %d", req.CandidateID)
	}

	// Invalid JSON
	_, err = decodeRequestVoteReq([]byte(`invalid`))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestDecodeRequestVoteResp(t *testing.T) {
	validJSON := `{"term": 1, "vote_granted": true}`

	resp, err := decodeRequestVoteResp([]byte(validJSON))
	if err != nil {
		t.Fatalf("decodeRequestVoteResp: %v", err)
	}

	if resp.Term != 1 {
		t.Errorf("expected term 1, got %d", resp.Term)
	}
	if !resp.VoteGranted {
		t.Error("expected vote_granted=true")
	}

	// Invalid JSON
	_, err = decodeRequestVoteResp([]byte(`invalid`))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestEncodePayload(t *testing.T) {
	req := &AppendEntriesRequest{
		Term:     1,
		LeaderID: 2,
		Entries:  []LogEntry{},
	}

	data := encodePayload(req)
	if len(data) == 0 {
		t.Error("expected non-empty payload")
	}

	// Verify it can be decoded back
	decoded, err := decodeAppendEntriesReq(data)
	if err != nil {
		t.Fatalf("decodeAppendEntriesReq: %v", err)
	}
	if decoded.Term != 1 {
		t.Errorf("expected term 1, got %d", decoded.Term)
	}
}

// Test DecodeRPC with various invalid inputs
func TestDecodeRPC_InvalidData(t *testing.T) {
	// Too short (less than 13 bytes)
	_, err := DecodeRPC([]byte{1, 2, 3, 4, 5})
	if err == nil {
		t.Error("expected error for data too short (< 13 bytes)")
	}

	// Exactly 13 bytes but payload length claims more
	data := make([]byte, 13)
	data[0] = 1 // Type
	// From: 8 bytes (1-9)
	data[9] = 0
	data[10] = 0
	data[11] = 0
	data[12] = 100 // Payload length = 100 (but only 0 bytes available)
	_, err = DecodeRPC(data)
	if err == nil {
		t.Error("expected error for payload length mismatch")
	}
}

// Test DecodeRPC with valid data
func TestDecodeRPC_Valid(t *testing.T) {
	payload := []byte(`{"term": 1, "success": true}`)
	req := &RPCRequest{
		Type:    MsgAppendEntriesResp,
		From:    42,
		Payload: payload,
	}

	encoded := EncodeRPC(req)
	decoded, err := DecodeRPC(encoded)
	if err != nil {
		t.Fatalf("DecodeRPC: %v", err)
	}

	if decoded.Type != MsgAppendEntriesResp {
		t.Errorf("expected type %d, got %d", MsgAppendEntriesResp, decoded.Type)
	}
	if decoded.From != 42 {
		t.Errorf("expected from 42, got %d", decoded.From)
	}
	if !bytes.Equal(decoded.Payload, payload) {
		t.Errorf("payload mismatch: got %s, want %s", decoded.Payload, payload)
	}
}
