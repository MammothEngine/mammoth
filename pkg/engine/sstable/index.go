package sstable

// IndexBlockBuilder builds an index block for an SSTable.
// Each entry is: largest_key in the data block -> block_offset.
type IndexBlockBuilder struct {
	builder *BlockBuilder
}

// NewIndexBlockBuilder creates a new index builder.
func NewIndexBlockBuilder() *IndexBlockBuilder {
	return &IndexBlockBuilder{
		builder: NewBlockBuilder(),
	}
}

// Add adds an index entry: the largest key in a data block and its offset.
func (b *IndexBlockBuilder) Add(largestKey []byte, blockOffset uint64, blockSize uint64) {
	// Encode offset and size as the value
	value := encodeOffsetSize(blockOffset, blockSize)
	b.builder.Add(largestKey, value)
}

// Finish returns the serialized index block.
func (b *IndexBlockBuilder) Finish() []byte {
	return b.builder.Finish()
}

// Reset clears the builder.
func (b *IndexBlockBuilder) Reset() {
	b.builder.Reset()
}

// Empty returns true if no entries added.
func (b *IndexBlockBuilder) Empty() bool {
	return b.builder.Empty()
}

// IndexBlockReader reads index entries.
type IndexBlockReader struct {
	reader *BlockReader
}

// NewIndexBlockReader creates a reader for an index block.
func NewIndexBlockReader(data []byte) *IndexBlockReader {
	return &IndexBlockReader{
		reader: NewBlockReader(data),
	}
}

// Iter iterates over index entries.
func (r *IndexBlockReader) Iter(cb func(largestKey []byte, offset, size uint64) bool) {
	r.reader.Iter(func(key, value []byte) bool {
		offset, size := decodeOffsetSize(value)
		return cb(key, offset, size)
	})
}

func encodeOffsetSize(offset, size uint64) []byte {
	buf := make([]byte, 16)
	_ = buf[15]
	buf[0] = byte(offset)
	buf[1] = byte(offset >> 8)
	buf[2] = byte(offset >> 16)
	buf[3] = byte(offset >> 24)
	buf[4] = byte(offset >> 32)
	buf[5] = byte(offset >> 40)
	buf[6] = byte(offset >> 48)
	buf[7] = byte(offset >> 56)
	buf[8] = byte(size)
	buf[9] = byte(size >> 8)
	buf[10] = byte(size >> 16)
	buf[11] = byte(size >> 24)
	buf[12] = byte(size >> 32)
	buf[13] = byte(size >> 40)
	buf[14] = byte(size >> 48)
	buf[15] = byte(size >> 56)
	return buf
}

func decodeOffsetSize(data []byte) (uint64, uint64) {
	offset := uint64(data[0]) | uint64(data[1])<<8 | uint64(data[2])<<16 | uint64(data[3])<<24 |
		uint64(data[4])<<32 | uint64(data[5])<<40 | uint64(data[6])<<48 | uint64(data[7])<<56
	size := uint64(data[8]) | uint64(data[9])<<8 | uint64(data[10])<<16 | uint64(data[11])<<24 |
		uint64(data[12])<<32 | uint64(data[13])<<40 | uint64(data[14])<<48 | uint64(data[15])<<56
	return offset, size
}
