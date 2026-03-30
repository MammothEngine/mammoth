package wal

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const (
	defaultBlockSize    = 32 * 1024 // 32KB
	defaultMaxSegSize   = 64 * 1024 * 1024 // 64MB
	segmentFilePattern  = "wal_%06d.log"
)

// Segment manages a single WAL segment file.
type Segment struct {
	mu     sync.Mutex
	file   *os.File
	path   string
	index  int
	offset int64
	size   int64
}

// CreateSegment creates a new segment file.
func CreateSegment(dir string, index int) (*Segment, error) {
	path := filepath.Join(dir, fmt.Sprintf(segmentFilePattern, index))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal: create segment: %w", err)
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: stat segment: %w", err)
	}
	return &Segment{
		file:  f,
		path:  path,
		index: index,
		size:  fi.Size(),
	}, nil
}

// OpenSegment opens an existing segment for reading.
func OpenSegment(path string) (*Segment, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("wal: open segment: %w", err)
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: stat segment: %w", err)
	}
	return &Segment{
		file: f,
		path: path,
		size: fi.Size(),
	}, nil
}

// WriteRecord writes a record with block alignment padding.
func (s *Segment) WriteRecord(rec *Record, sync bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data := rec.Encode()

	// Check if we need block padding
	blockOffset := s.offset % int64(defaultBlockSize)
	remaining := int64(defaultBlockSize) - blockOffset

	if int64(len(data)) > remaining && remaining > 0 && remaining < int64(RecordSize(0)) {
		// Pad remaining with zeros
		pad := make([]byte, remaining)
		if _, err := s.file.Write(pad); err != nil {
			return fmt.Errorf("wal: write padding: %w", err)
		}
		s.offset += remaining
		s.size += remaining
	}

	n, err := s.file.Write(data)
	if err != nil {
		return fmt.Errorf("wal: write record: %w", err)
	}
	s.offset += int64(n)
	s.size += int64(n)

	if sync {
		if err := s.file.Sync(); err != nil {
			return fmt.Errorf("wal: sync segment: %w", err)
		}
	}

	return nil
}

// Close closes the segment file.
func (s *Segment) Close() error {
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}

// Sync flushes data to disk.
func (s *Segment) Sync() error {
	if s.file != nil {
		return s.file.Sync()
	}
	return nil
}

// Size returns current file size.
func (s *Segment) Size() int64 {
	return s.size
}

// Path returns the file path.
func (s *Segment) Path() string {
	return s.path
}

// Index returns the segment index.
func (s *Segment) Index() int {
	return s.index
}

// ReadRecords reads all records from a segment file.
func ReadRecords(path string) ([]Record, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("wal: read segment: %w", err)
	}

	var records []Record
	pos := 0

	for pos < len(data) {
		// Skip block padding (zeros at end of block)
		if data[pos] == 0 {
			blockEnd := ((pos / defaultBlockSize) + 1) * defaultBlockSize
			if blockEnd > len(data) {
				blockEnd = len(data)
			}
			// Check if rest of block is all zeros
			allZero := true
			for i := pos; i < blockEnd; i++ {
				if data[i] != 0 {
					allZero = false
					break
				}
			}
			if allZero {
				pos = blockEnd
				continue
			}
		}

		rec, consumed, err := DecodeRecord(data[pos:])
		if err != nil {
			// Corruption - skip to next block boundary
			blockEnd := ((pos / defaultBlockSize) + 1) * defaultBlockSize
			if blockEnd <= pos {
				blockEnd = pos + defaultBlockSize
			}
			if blockEnd > len(data) {
				break
			}
			pos = blockEnd
			continue
		}

		records = append(records, rec)
		pos += consumed
	}

	return records, nil
}

// ListSegments lists segment files in order.
func ListSegments(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".log" {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	paths := make([]string, len(names))
	for i, n := range names {
		paths[i] = filepath.Join(dir, n)
	}
	return paths, nil
}

// ParseSegmentIndex extracts the index from a segment filename.
func ParseSegmentIndex(name string) (int, error) {
	var idx int
	_, err := fmt.Sscanf(name, segmentFilePattern, &idx)
	return idx, err
}

// uitoa converts uint to string.
func uitoa(v uint) string {
	return fmt.Sprintf("%06d", v)
}

// putUint32LE writes uint32 little-endian.
func putUint32LE(b []byte, v uint32) {
	binary.LittleEndian.PutUint32(b, v)
}
