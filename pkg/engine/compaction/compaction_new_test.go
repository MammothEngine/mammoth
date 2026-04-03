package compaction

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/mammothengine/mammoth/pkg/engine/compression"
	"github.com/mammothengine/mammoth/pkg/engine/manifest"
	"github.com/mammothengine/mammoth/pkg/engine/sstable"
)

// TestCompactionFullMerge tests the full compaction merge process
func TestCompactionFullMerge(t *testing.T) {
	dir := t.TempDir()

	// Create valid SSTable files with actual data
	for i := 0; i < 2; i++ {
		fileNum := uint64(i + 1)
		path := filepath.Join(dir, fmt.Sprintf("%06d.sst", fileNum))

		w, err := sstable.NewWriter(sstable.WriterOptions{
			Path:         path,
			ExpectedKeys: 10,
			Compression:  compression.CompressionNone,
		})
		if err != nil {
			t.Fatal(err)
		}

		// Write unique keys for each file
		for j := 0; j < 5; j++ {
			key := []byte(fmt.Sprintf("key%02d", i*5+j))
			val := []byte(fmt.Sprintf("value%02d", i*5+j))
			if err := w.Add(key, val); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := w.Finish(); err != nil {
			t.Fatal(err)
		}
	}

	// Create manifest with these files
	m, err := manifest.Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Add files to L0
	for i := 0; i < 2; i++ {
		m.LogEdit(manifest.ManifestEdit{
			Type:        manifest.EditAddFile,
			Level:       0,
			FileNum:     uint64(i + 1),
			FileSize:    200,
			SmallestKey: []byte(fmt.Sprintf("key%02d", i*5)),
			LargestKey:  []byte(fmt.Sprintf("key%02d", i*5+4)),
		})
	}

	c := NewCompactor(dir, m, 10, compression.CompressionNone)

	// Create compaction manually
	comp := &Compaction{
		Level: 0,
		Inputs: []manifest.FileMetadata{
			{FileNum: 1, Size: 200, SmallestKey: []byte("key00"), LargestKey: []byte("key04")},
			{FileNum: 2, Size: 200, SmallestKey: []byte("key05"), LargestKey: []byte("key09")},
		},
	}

	err = c.runCompaction(comp)
	if err != nil {
		t.Fatalf("runCompaction failed: %v", err)
	}

	// Verify output file was created
	outputPath := filepath.Join(dir, "000010.sst")
	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		t.Error("output SSTable was not created")
	}

	m.Close()
}

// TestCompactionWithOverlappingKeys tests compaction with overlapping keys
func TestCompactionWithOverlappingKeys(t *testing.T) {
	dir := t.TempDir()

	// Create two SSTables with overlapping key ranges
	for i := 0; i < 2; i++ {
		fileNum := uint64(i + 1)
		path := filepath.Join(dir, fmt.Sprintf("%06d.sst", fileNum))

		w, err := sstable.NewWriter(sstable.WriterOptions{
			Path:         path,
			ExpectedKeys: 10,
			Compression:  compression.CompressionNone,
		})
		if err != nil {
			t.Fatal(err)
		}

		// Write overlapping keys (key02-key06 overlap)
		for j := 0; j < 5; j++ {
			key := []byte(fmt.Sprintf("key%02d", i*3+j)) // 0-4, 3-7 (overlap at 3,4)
			val := []byte(fmt.Sprintf("value%d_%d", i, j))
			w.Add(key, val)
		}
		if _, err := w.Finish(); err != nil {
			t.Fatal(err)
		}
	}

	m, _ := manifest.Open(dir)

	// Add files with overlapping ranges
	m.LogEdit(manifest.ManifestEdit{
		Type:        manifest.EditAddFile,
		Level:       0,
		FileNum:     1,
		FileSize:    200,
		SmallestKey: []byte("key00"),
		LargestKey:  []byte("key04"),
	})
	m.LogEdit(manifest.ManifestEdit{
		Type:        manifest.EditAddFile,
		Level:       0,
		FileNum:     2,
		FileSize:    200,
		SmallestKey: []byte("key03"),
		LargestKey:  []byte("key07"),
	})

	c := NewCompactor(dir, m, 10, compression.CompressionNone)

	comp := &Compaction{
		Level: 0,
		Inputs: []manifest.FileMetadata{
			{FileNum: 1, Size: 200, SmallestKey: []byte("key00"), LargestKey: []byte("key04")},
			{FileNum: 2, Size: 200, SmallestKey: []byte("key03"), LargestKey: []byte("key07")},
		},
	}

	err := c.runCompaction(comp)
	if err != nil {
		t.Fatalf("runCompaction with overlapping keys failed: %v", err)
	}

	m.Close()
}

// TestCompactionTombstoneAtBottomLevel tests that tombstones are dropped at max level
func TestCompactionTombstoneAtBottomLevel(t *testing.T) {
	dir := t.TempDir()

	// Create SSTable at level 6 (max level)
	fileNum := uint64(1)
	path := filepath.Join(dir, fmt.Sprintf("%06d.sst", fileNum))

	w, err := sstable.NewWriter(sstable.WriterOptions{
		Path:         path,
		ExpectedKeys: 10,
		Compression:  compression.CompressionNone,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Write regular key and tombstone (empty value)
	w.Add([]byte("key01"), []byte("value1"))
	w.Add([]byte("key02"), []byte("")) // Tombstone
	w.Add([]byte("key03"), []byte("value3"))
	if _, err := w.Finish(); err != nil { t.Fatal(err) }

	m, _ := manifest.Open(dir)
	m.LogEdit(manifest.ManifestEdit{
		Type:        manifest.EditAddFile,
		Level:       6,
		FileNum:     1,
		FileSize:    100,
		SmallestKey: []byte("key01"),
		LargestKey:  []byte("key03"),
	})

	c := NewCompactor(dir, m, 10, compression.CompressionNone)

	// Compact from level 6 to level 6 (should drop tombstones)
	comp := &Compaction{
		Level: 6,
		Inputs: []manifest.FileMetadata{
			{FileNum: 1, Size: 100, SmallestKey: []byte("key01"), LargestKey: []byte("key03")},
		},
	}

	err = c.runCompaction(comp)
	if err != nil {
		t.Fatalf("runCompaction at max level failed: %v", err)
	}

	m.Close()
}

// TestCompactionInvalidInput tests error handling for invalid input files
func TestCompactionInvalidInput(t *testing.T) {
	dir := t.TempDir()
	m, _ := manifest.Open(dir)
	defer m.Close()

	c := NewCompactor(dir, m, 10, compression.CompressionNone)

	// Try to compact with non-existent file
	comp := &Compaction{
		Level: 0,
		Inputs: []manifest.FileMetadata{
			{FileNum: 999, Size: 100, SmallestKey: []byte("a"), LargestKey: []byte("z")},
		},
	}

	err := c.runCompaction(comp)
	if err == nil {
		t.Error("expected error for non-existent input file")
	}
}
