package manifest

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
)

const manifestFileName = "MANIFEST"

// EditType defines manifest edit operations.
type EditType uint8

const (
	EditAddFile    EditType = 1
	EditRemoveFile EditType = 2
)

// ManifestEdit represents a single edit to the version state.
type ManifestEdit struct {
	Type     EditType
	Level    int
	FileNum  uint64
	FileSize uint64
	SmallestKey []byte
	LargestKey  []byte
}

// Manifest manages version state persistently.
type Manifest struct {
	mu       sync.RWMutex
	dir      string
	file     *os.File
	version  *Version
}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Open opens or creates a manifest in the given directory.
func Open(dir string) (*Manifest, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	m := &Manifest{
		dir:     dir,
		version: NewVersion(),
	}

	path := filepath.Join(dir, manifestFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("manifest: open: %w", err)
	}
	m.file = f

	// Replay existing edits
	if err := m.replay(); err != nil {
		f.Close()
		return nil, fmt.Errorf("manifest: replay: %w", err)
	}

	return m, nil
}

// LogEdit appends an edit to the manifest log.
func (m *Manifest) LogEdit(edit ManifestEdit) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	data := m.encodeEdit(edit)

	// Write: length(4) + data + crc(4)
	buf := make([]byte, 4+len(data)+4)
	binary.LittleEndian.PutUint32(buf, uint32(len(data)))
	copy(buf[4:], data)
	crc := crc32.Checksum(buf[:4+len(data)], crcTable)
	binary.LittleEndian.PutUint32(buf[4+len(data):], crc)

	if _, err := m.file.Write(buf); err != nil {
		return err
	}
	if err := m.file.Sync(); err != nil {
		return err
	}

	// Apply edit to current version
	m.applyEdit(edit)

	return nil
}

// LogBatch atomically logs multiple edits.
func (m *Manifest) LogBatch(edits []ManifestEdit) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, edit := range edits {
		data := m.encodeEdit(edit)
		buf := make([]byte, 4+len(data)+4)
		binary.LittleEndian.PutUint32(buf, uint32(len(data)))
		copy(buf[4:], data)
		crc := crc32.Checksum(buf[:4+len(data)], crcTable)
		binary.LittleEndian.PutUint32(buf[4+len(data):], crc)

		if _, err := m.file.Write(buf); err != nil {
			return err
		}
		m.applyEdit(edit)
	}

	return m.file.Sync()
}

// CurrentVersion returns a snapshot of the current version.
func (m *Manifest) CurrentVersion() *Version {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.version.Clone()
}

// Close closes the manifest.
func (m *Manifest) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.file != nil {
		return m.file.Close()
	}
	return nil
}

func (m *Manifest) applyEdit(edit ManifestEdit) {
	switch edit.Type {
	case EditAddFile:
		m.version.AddFile(edit.Level, FileMetadata{
			FileNum:     edit.FileNum,
			Size:        edit.FileSize,
			SmallestKey: edit.SmallestKey,
			LargestKey:  edit.LargestKey,
		})
	case EditRemoveFile:
		m.version.RemoveFile(edit.Level, edit.FileNum)
	}
}

func (m *Manifest) encodeEdit(edit ManifestEdit) []byte {
	// type(1) + level(1) + fileNum(8) + fileSize(8) + smallestLen(4) + smallest + largestLen(4) + largest
	size := 1 + 1 + 8 + 8 + 4 + len(edit.SmallestKey) + 4 + len(edit.LargestKey)
	buf := make([]byte, size)

	buf[0] = byte(edit.Type)
	buf[1] = byte(edit.Level)
	binary.LittleEndian.PutUint64(buf[2:], edit.FileNum)
	binary.LittleEndian.PutUint64(buf[10:], edit.FileSize)
	binary.LittleEndian.PutUint32(buf[18:], uint32(len(edit.SmallestKey)))
	copy(buf[22:], edit.SmallestKey)
	off := 22 + len(edit.SmallestKey)
	binary.LittleEndian.PutUint32(buf[off:], uint32(len(edit.LargestKey)))
	copy(buf[off+4:], edit.LargestKey)

	return buf
}

func (m *Manifest) decodeEdit(data []byte) (ManifestEdit, error) {
	if len(data) < 22 {
		return ManifestEdit{}, fmt.Errorf("manifest: edit too short")
	}

	edit := ManifestEdit{
		Type:     EditType(data[0]),
		Level:    int(data[1]),
		FileNum:  binary.LittleEndian.Uint64(data[2:]),
		FileSize: binary.LittleEndian.Uint64(data[10:]),
	}

	smallestLen := int(binary.LittleEndian.Uint32(data[18:]))
	if len(data) < 22+smallestLen+4 {
		return ManifestEdit{}, fmt.Errorf("manifest: smallest key truncated")
	}
	edit.SmallestKey = make([]byte, smallestLen)
	copy(edit.SmallestKey, data[22:22+smallestLen])

	off := 22 + smallestLen
	largestLen := int(binary.LittleEndian.Uint32(data[off:]))
	if len(data) < off+4+largestLen {
		return ManifestEdit{}, fmt.Errorf("manifest: largest key truncated")
	}
	edit.LargestKey = make([]byte, largestLen)
	copy(edit.LargestKey, data[off+4:])

	return edit, nil
}

func (m *Manifest) replay() error {
	data, err := os.ReadFile(filepath.Join(m.dir, manifestFileName))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	pos := 0
	for pos < len(data) {
		if pos+4 > len(data) {
			break
		}
		editLen := int(binary.LittleEndian.Uint32(data[pos:]))
		if pos+4+editLen+4 > len(data) {
			break
		}

		// Verify CRC
		expected := binary.LittleEndian.Uint32(data[pos+4+editLen:])
		actual := crc32.Checksum(data[pos:pos+4+editLen], crcTable)
		if expected != actual {
			break // Stop at corruption
		}

		edit, err := m.decodeEdit(data[pos+4 : pos+4+editLen])
		if err != nil {
			break
		}
		m.applyEdit(edit)
		pos += 4 + editLen + 4
	}

	return nil
}
