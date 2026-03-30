package sstable

import (
	"encoding/binary"
	"os"
	"path/filepath"

	"github.com/mammothengine/mammoth/pkg/engine/bloom"
	"github.com/mammothengine/mammoth/pkg/engine/compression"
)

// Writer builds an SSTable file.
type Writer struct {
	file          *os.File
	path          string
	blockBuilder  *BlockBuilder
	indexBuilder  *IndexBlockBuilder
	bloomFilter   *bloom.Filter
	compressor    compression.Compressor
	blockSize     int

	offset        uint64
	firstKey      []byte
	lastKey       []byte
	blockEntries  int
	totalEntries  int
}

// WriterOptions configures the SSTable writer.
type WriterOptions struct {
	Path         string
	BlockSize    int
	Compression  compression.CompressionType
	ExpectedKeys int
}

// NewWriter creates a new SSTable writer.
func NewWriter(opts WriterOptions) (*Writer, error) {
	dir := filepath.Dir(opts.Path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	f, err := os.Create(opts.Path)
	if err != nil {
		return nil, err
	}

	bs := opts.BlockSize
	if bs <= 0 {
		bs = defaultBlockSize
	}

	expected := opts.ExpectedKeys
	if expected <= 0 {
		expected = 10000
	}

	return &Writer{
		file:         f,
		path:         opts.Path,
		blockBuilder: NewBlockBuilder(),
		indexBuilder: NewIndexBlockBuilder(),
		bloomFilter:  bloom.NewFilter(expected),
		compressor:   compression.GetCompressor(opts.Compression),
		blockSize:    bs,
	}, nil
}

// Add adds a key-value pair to the SSTable.
func (w *Writer) Add(key, value []byte) error {
	if w.firstKey == nil {
		w.firstKey = append([]byte{}, key...)
	}
	w.lastKey = append(w.lastKey[:0], key...)
	w.bloomFilter.Insert(key)

	w.blockBuilder.Add(key, value)
	w.blockEntries++
	w.totalEntries++

	if w.blockBuilder.EstimatedSize() >= w.blockSize {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}

	return nil
}

// Finish finalizes the SSTable.
func (w *Writer) Finish() (uint64, error) {
	// Flush remaining block
	if !w.blockBuilder.Empty() {
		if err := w.flushBlock(); err != nil {
			return 0, err
		}
	}

	// Write bloom filter
	bloomOffset := w.offset
	bloomData, err := w.bloomFilter.MarshalBinary()
	if err != nil {
		return 0, err
	}
	if _, err := w.file.Write(bloomData); err != nil {
		return 0, err
	}
	w.offset += uint64(len(bloomData))

	// Write index block
	indexOffset := w.offset
	indexData := w.indexBuilder.Finish()
	if w.compressor.Type() != compression.CompressionNone {
		indexData, err = w.compressor.Compress(indexData)
		if err != nil {
			return 0, err
		}
	}
	if _, err := w.file.Write(indexData); err != nil {
		return 0, err
	}
	w.offset += uint64(len(indexData))

	// Write footer
	footer := Footer{
		BloomOffset: bloomOffset,
		BloomLength: uint64(len(bloomData)),
		IndexOffset: indexOffset,
		IndexLength: uint64(len(indexData)),
		MetaOffset:  w.offset,
	}
	if _, err := w.file.Write(footer.Encode()); err != nil {
		return 0, err
	}
	w.offset += footerSize

	if err := w.file.Sync(); err != nil {
		return 0, err
	}

	return w.offset, w.Close()
}

// Close closes the writer.
func (w *Writer) Close() error {
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

// Entries returns the total number of entries written.
func (w *Writer) Entries() int {
	return w.totalEntries
}

func (w *Writer) flushBlock() error {
	blockData := w.blockBuilder.Finish()
	blockOffset := w.offset

	// Compress
	var compressed []byte
	var err error
	if w.compressor.Type() != compression.CompressionNone {
		compressed, err = w.compressor.Compress(blockData)
		if err != nil {
			return err
		}
	} else {
		compressed = blockData
	}

	// Write block: length(4) + compressed_data
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(compressed)))
	if _, err := w.file.Write(lenBuf[:]); err != nil {
		return err
	}
	if _, err := w.file.Write(compressed); err != nil {
		return err
	}
	w.offset += 4 + uint64(len(compressed))

	// Add to index
	w.indexBuilder.Add(w.lastKey, blockOffset, uint64(len(blockData)))

	// Reset block builder
	w.blockBuilder.Reset()
	w.blockEntries = 0

	return nil
}
