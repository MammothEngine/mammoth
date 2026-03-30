package sstable

import (
	"encoding/binary"
	"os"

	"github.com/mammothengine/mammoth/pkg/engine/bloom"
	"github.com/mammothengine/mammoth/pkg/engine/cache"
	"github.com/mammothengine/mammoth/pkg/engine/compression"
)

// Reader reads an SSTable file.
type Reader struct {
	file       *os.File
	path       string
	footer     Footer
	index      *IndexBlockReader
	bloom      *bloom.Filter
	compressor compression.Compressor
	cache      cache.Cache
	fileSize   uint64
}

// ReaderOptions configures the SSTable reader.
type ReaderOptions struct {
	Cache       cache.Cache
	Compression compression.CompressionType
}

// NewReader opens an SSTable for reading.
func NewReader(path string, opts ReaderOptions) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	size := fi.Size()

	// Read footer
	footerBuf := make([]byte, footerSize)
	if _, err := f.ReadAt(footerBuf, size-int64(footerSize)); err != nil {
		f.Close()
		return nil, err
	}
	footer, err := DecodeFooter(footerBuf)
	if err != nil {
		f.Close()
		return nil, err
	}

	// Read bloom filter
	bloomData := make([]byte, footer.BloomLength)
	if _, err := f.ReadAt(bloomData, int64(footer.BloomOffset)); err != nil {
		f.Close()
		return nil, err
	}
	bf := &bloom.Filter{}
	if err := bf.UnmarshalBinary(bloomData); err != nil {
		f.Close()
		return nil, err
	}

	// Read index block
	indexRaw := make([]byte, footer.IndexLength)
	if _, err := f.ReadAt(indexRaw, int64(footer.IndexOffset)); err != nil {
		f.Close()
		return nil, err
	}

	comp := compression.GetCompressor(opts.Compression)
	if comp.Type() != compression.CompressionNone {
		decompressed, err := comp.Decompress(indexRaw)
		if err != nil {
			f.Close()
			return nil, err
		}
		indexRaw = decompressed
	}

	return &Reader{
		file:       f,
		path:       path,
		footer:     footer,
		index:      NewIndexBlockReader(indexRaw),
		bloom:      bf,
		compressor: comp,
		cache:      opts.Cache,
		fileSize:   uint64(size),
	}, nil
}

// Get looks up a key in the SSTable.
func (r *Reader) Get(key []byte) ([]byte, error) {
	// Check bloom filter first
	if !r.bloom.MayContain(key) {
		return nil, errKeyNotFound
	}

	// Find the block that might contain the key
	blockOffset, blockSize, err := r.findBlock(key)
	if err != nil {
		return nil, err
	}

	// Read and decompress the block
	blockData, err := r.readBlock(blockOffset, blockSize)
	if err != nil {
		return nil, err
	}

	// Search within the block
	var result []byte
	found := false
	reader := NewBlockReader(blockData)
	reader.Iter(func(k, v []byte) bool {
		if compareBytes(k, key) == 0 {
			result = append([]byte{}, v...)
			found = true
			return false
		}
		return true
	})

	if !found {
		return nil, errKeyNotFound
	}
	return result, nil
}

// MayContain checks if the key might be in the SSTable using the bloom filter.
func (r *Reader) MayContain(key []byte) bool {
	return r.bloom.MayContain(key)
}

// Close closes the reader.
func (r *Reader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}

// Path returns the file path.
func (r *Reader) Path() string {
	return r.path
}

// Size returns the file size.
func (r *Reader) Size() uint64 {
	return r.fileSize
}

func (r *Reader) findBlock(key []byte) (offset, size uint64, err error) {
	found := false
	r.index.Iter(func(largestKey []byte, blkOffset, blkSize uint64) bool {
		if compareBytes(key, largestKey) <= 0 {
			offset = blkOffset
			size = blkSize
			found = true
			return false
		}
		return true
	})
	if !found {
		return 0, 0, errKeyNotFound
	}
	return offset, size, nil
}

func (r *Reader) readBlock(offset, size uint64) ([]byte, error) {
	// Read compressed block: 4-byte length + data
	lenBuf := make([]byte, 4)
	if _, err := r.file.ReadAt(lenBuf, int64(offset)); err != nil {
		return nil, err
	}
	compressedLen := binary.LittleEndian.Uint32(lenBuf)

	compressed := make([]byte, compressedLen)
	if _, err := r.file.ReadAt(compressed, int64(offset+4)); err != nil {
		return nil, err
	}

	if r.compressor.Type() != compression.CompressionNone {
		return r.compressor.Decompress(compressed)
	}
	return compressed, nil
}

// Iter iterates over all entries in the SSTable.
func (r *Reader) Iter(cb func(key, value []byte) bool) error {
	r.index.Iter(func(largestKey []byte, offset, size uint64) bool {
		blockData, err := r.readBlock(offset, size)
		if err != nil {
			return false
		}
		reader := NewBlockReader(blockData)
		reader.Iter(func(k, v []byte) bool {
			return cb(k, v)
		})
		return true
	})
	return nil
}

// SmallestKey returns the smallest key (first in first block).
// LargestKey returns the largest key from the footer.
func (r *Reader) LargestKey() []byte {
	var largest []byte
	r.index.Iter(func(key []byte, _, _ uint64) bool {
		largest = key
		return true
	})
	return largest
}

func compareBytes(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := range minLen {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}
