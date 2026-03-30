package sstable

// Iterator provides sequential scan over an SSTable.
type Iterator struct {
	reader   *Reader
	blockIdx int
	blocks   []blockInfo
	entryIdx int
	entries  []Entry
	valid    bool
}

type blockInfo struct {
	offset uint64
	size   uint64
	key    []byte
}

// NewIterator creates a new SSTable iterator.
func NewIterator(r *Reader) *Iterator {
	var blocks []blockInfo
	r.index.Iter(func(key []byte, offset, size uint64) bool {
		blocks = append(blocks, blockInfo{offset: offset, size: size, key: append([]byte{}, key...)})
		return true
	})

	return &Iterator{
		reader: r,
		blocks: blocks,
	}
}

// Seek positions the iterator at the first key >= target.
func (it *Iterator) Seek(target []byte) {
	// Find the right block
	it.blockIdx = 0
	for i, b := range it.blocks {
		if compareBytes(target, b.key) <= 0 {
			it.blockIdx = i
			break
		}
		if i == len(it.blocks)-1 {
			it.blockIdx = i
		}
	}

	// Load the block
	it.loadBlock()

	// Skip entries until we find one >= target
	for it.valid && compareBytes(it.entries[it.entryIdx].Key, target) < 0 {
		it.Next()
	}
}

// SeekToFirst positions at the first entry.
func (it *Iterator) SeekToFirst() {
	it.blockIdx = 0
	it.loadBlock()
}

// Next advances to the next entry.
func (it *Iterator) Next() {
	it.entryIdx++
	if it.entryIdx < len(it.entries) {
		it.valid = true
		return
	}

	// Move to next block
	it.blockIdx++
	if it.blockIdx >= len(it.blocks) {
		it.valid = false
		return
	}
	it.loadBlock()
}

// Valid returns whether the iterator is positioned at a valid entry.
func (it *Iterator) Valid() bool {
	return it.valid
}

// Key returns the current key.
func (it *Iterator) Key() []byte {
	if !it.valid {
		return nil
	}
	return it.entries[it.entryIdx].Key
}

// Value returns the current value.
func (it *Iterator) Value() []byte {
	if !it.valid {
		return nil
	}
	return it.entries[it.entryIdx].Value
}

func (it *Iterator) loadBlock() {
	if it.blockIdx >= len(it.blocks) {
		it.valid = false
		return
	}

	b := it.blocks[it.blockIdx]
	blockData, err := it.reader.readBlock(b.offset, b.size)
	if err != nil {
		it.valid = false
		return
	}

	it.entries = it.entries[:0]
	reader := NewBlockReader(blockData)
	reader.Iter(func(k, v []byte) bool {
		it.entries = append(it.entries, Entry{
			Key:   append([]byte{}, k...),
			Value: append([]byte{}, v...),
		})
		return true
	})

	it.entryIdx = 0
	it.valid = len(it.entries) > 0
}
