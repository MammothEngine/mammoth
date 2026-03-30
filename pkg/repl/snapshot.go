package repl

import (
	"encoding/json"
)

const maxSnapshotChunkSize = 1024 * 1024 // 1MB

// SnapshotData wraps snapshot bytes with metadata.
type SnapshotData struct {
	LastIncludedIndex uint64 `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
	Data              []byte `json:"data"`
}

// ChunkSnapshot splits a snapshot into chunks for传输.
func ChunkSnapshot(snap SnapshotData, chunkSize int) []InstallSnapshotRequest {
	if chunkSize <= 0 {
		chunkSize = maxSnapshotChunkSize
	}

	var chunks []InstallSnapshotRequest
	offset := uint64(0)

	for offset < uint64(len(snap.Data)) {
		end := offset + uint64(chunkSize)
		if end > uint64(len(snap.Data)) {
			end = uint64(len(snap.Data))
		}
		done := end >= uint64(len(snap.Data))

		chunks = append(chunks, InstallSnapshotRequest{
			LastIncludedIndex: snap.LastIncludedIndex,
			LastIncludedTerm:  snap.LastIncludedTerm,
			Offset:            offset,
			Data:              snap.Data[offset:end],
			Done:              done,
		})
		offset = end
	}

	return chunks
}

// SnapshotBuilder accumulates snapshot chunks on the receiver side.
type SnapshotBuilder struct {
	lastIndex uint64
	lastTerm  uint64
	data      []byte
}

// NewSnapshotBuilder creates a new snapshot builder.
func NewSnapshotBuilder() *SnapshotBuilder {
	return &SnapshotBuilder{}
}

// ApplyChunk applies a snapshot chunk.
// Returns true when the snapshot is complete.
func (b *SnapshotBuilder) ApplyChunk(req InstallSnapshotRequest) bool {
	b.lastIndex = req.LastIncludedIndex
	b.lastTerm = req.LastIncludedTerm
	b.data = append(b.data, req.Data...)
	return req.Done
}

// Build returns the assembled snapshot.
func (b *SnapshotBuilder) Build() SnapshotData {
	return SnapshotData{
		LastIncludedIndex: b.lastIndex,
		LastIncludedTerm:  b.lastTerm,
		Data:              b.data,
	}
}

// EncodeSnapshotData serializes snapshot data.
func EncodeSnapshotData(snap SnapshotData) ([]byte, error) {
	return json.Marshal(snap)
}

// DecodeSnapshotData deserializes snapshot data.
func DecodeSnapshotData(data []byte) (SnapshotData, error) {
	var snap SnapshotData
	if err := json.Unmarshal(data, &snap); err != nil {
		return SnapshotData{}, err
	}
	return snap, nil
}
