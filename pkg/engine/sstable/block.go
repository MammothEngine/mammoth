package sstable

import (
	"encoding/binary"
)

const (
	defaultBlockSize     = 4096
	defaultRestartInterval = 16
)

// BlockBuilder constructs a data block with prefix compression.
type BlockBuilder struct {
	restartInterval int
	buf             []byte
	restarts        []uint32
	counter         int
	prevKey         []byte
}

// NewBlockBuilder creates a new block builder.
func NewBlockBuilder() *BlockBuilder {
	return &BlockBuilder{
		restartInterval: defaultRestartInterval,
	}
}

// Add appends a key-value pair to the block.
func (b *BlockBuilder) Add(key, value []byte) {
	shared := 0
	if b.counter%b.restartInterval == 0 {
		b.restarts = append(b.restarts, uint32(len(b.buf)))
	} else {
		shared = sharedPrefixLen(b.prevKey, key)
	}

	nonShared := len(key) - shared

	// Encode: sharedLen(varint) + nonSharedLen(varint) + valueLen(varint) + key[shared:] + value
	b.encodeVarint(uint32(shared))
	b.encodeVarint(uint32(nonShared))
	b.encodeVarint(uint32(len(value)))
	b.buf = append(b.buf, key[shared:]...)
	b.buf = append(b.buf, value...)

	b.prevKey = append(b.prevKey[:0], key...)
	b.counter++
}

// Finish finalizes the block and returns its contents.
func (b *BlockBuilder) Finish() []byte {
	// Append restart points
	for _, r := range b.restarts {
		var buf [4]byte
		binary.LittleEndian.PutUint32(buf[:], r)
		b.buf = append(b.buf, buf[:]...)
	}
	// Number of restart points
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(len(b.restarts)))
	b.buf = append(b.buf, buf[:]...)

	return b.buf
}

// Reset clears the builder for reuse.
func (b *BlockBuilder) Reset() {
	b.buf = b.buf[:0]
	b.restarts = b.restarts[:0]
	b.counter = 0
	b.prevKey = nil
}

// EstimatedSize returns the approximate block size.
func (b *BlockBuilder) EstimatedSize() int {
	return len(b.buf) + len(b.restarts)*4 + 4
}

// Empty returns true if no entries have been added.
func (b *BlockBuilder) Empty() bool {
	return b.counter == 0
}

func (b *BlockBuilder) encodeVarint(v uint32) {
	var buf [5]byte
	n := binary.PutUvarint(buf[:], uint64(v))
	b.buf = append(b.buf, buf[:n]...)
}

// BlockReader reads entries from a data block.
type BlockReader struct {
	data     []byte
	restarts []uint32
	numRestarts int
}

// NewBlockReader creates a block reader from block data.
func NewBlockReader(data []byte) *BlockReader {
	if len(data) < 4 {
		return &BlockReader{}
	}
	numRestarts := int(binary.LittleEndian.Uint32(data[len(data)-4:]))
	restartStart := len(data) - 4 - numRestarts*4

	restarts := make([]uint32, numRestarts)
	for i := range restarts {
		restarts[i] = binary.LittleEndian.Uint32(data[restartStart+i*4:])
	}

	return &BlockReader{
		data:        data[:restartStart],
		restarts:    restarts,
		numRestarts: numRestarts,
	}
}

// Entry represents a decoded key-value pair from a block.
type Entry struct {
	Key   []byte
	Value []byte
}

// Iter returns all entries in the block.
func (r *BlockReader) Iter(cb func(key, value []byte) bool) {
	pos := 0
	data := r.data
	var prevKey []byte
	for pos < len(data) {
		shared, n := binary.Uvarint(data[pos:])
		pos += n
		nonShared, n := binary.Uvarint(data[pos:])
		pos += n
		valueLen, n := binary.Uvarint(data[pos:])
		pos += n

		key := make([]byte, 0, shared+nonShared)
		key = append(key, prevKey[:shared]...)
		key = append(key, data[pos:pos+int(nonShared)]...)
		pos += int(nonShared)
		value := data[pos : pos+int(valueLen)]
		pos += int(valueLen)

		prevKey = append(prevKey[:0], key...)

		if !cb(key, value) {
			return
		}
	}
}

// NumEntries returns the approximate number of entries.
func (r *BlockReader) NumEntries() int {
	return r.numRestarts
}

func sharedPrefixLen(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	i := 0
	for i < minLen && a[i] == b[i] {
		i++
	}
	return i
}
