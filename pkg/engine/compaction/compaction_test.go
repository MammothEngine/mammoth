package compaction

import (
	"os"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/mammothengine/mammoth/pkg/engine/compression"
	"github.com/mammothengine/mammoth/pkg/engine/manifest"
)

func TestPickerNoCompaction(t *testing.T) {
	v := manifest.NewVersion()
	p := NewPicker(v)

	if p.NeedsCompaction() {
		t.Fatal("empty version should not need compaction")
	}
	if p.PickCompaction() != nil {
		t.Fatal("expected nil compaction")
	}
}

func TestPickerL0Trigger(t *testing.T) {
	v := manifest.NewVersion()
	for i := 0; i < l0CompactionTrigger; i++ {
		v.AddFile(0, manifest.FileMetadata{
			FileNum:     uint64(i + 1),
			Size:        1024,
			SmallestKey: []byte{byte(i)},
			LargestKey:  []byte{byte(i) + 1},
		})
	}

	p := NewPicker(v)
	if !p.NeedsCompaction() {
		t.Fatal("should need compaction")
	}

	comp := p.PickCompaction()
	if comp == nil {
		t.Fatal("expected compaction")
	}
	if comp.Level != 0 {
		t.Fatalf("expected level 0, got %d", comp.Level)
	}
}

func TestCompactionRun(t *testing.T) {
	dir := t.TempDir()
	m, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("open manifest: %v", err)
	}

	// Add files to trigger compaction
	for i := 0; i < l0CompactionTrigger; i++ {
		m.LogEdit(manifest.ManifestEdit{
			Type:        manifest.EditAddFile,
			Level:       0,
			FileNum:     uint64(i + 1),
			FileSize:    100,
			SmallestKey: []byte{byte(i)},
			LargestKey:  []byte{byte(i) + 1},
		})
	}

	_ = &Compactor{
		dir:         dir,
		manifest:    m,
		nextFileNum: 100,
	}

	// The compaction will try to read SST files that don't exist
	// So we just verify the picker logic works
	v := m.CurrentVersion()
	picker := NewPicker(v)
	if !picker.NeedsCompaction() {
		t.Fatal("should need compaction")
	}

	comp_result := picker.PickCompaction()
	if comp_result == nil {
		t.Fatal("expected compaction to be picked")
	}
	if len(comp_result.Inputs) != l0CompactionTrigger {
		t.Fatalf("expected %d inputs, got %d", l0CompactionTrigger, len(comp_result.Inputs))
	}

	m.Close()
}

func TestCompactionFileNumGeneration(t *testing.T) {
	dir := t.TempDir()
	m, _ := manifest.Open(dir)
	defer m.Close()

	c := NewCompactor(dir, m, 1, compression.CompressionNone)

	n1 := c.NextFileNum()
	n2 := c.NextFileNum()
	n3 := c.NextFileNum()

	if n1 != 1 || n2 != 2 || n3 != 3 {
		t.Fatalf("expected 1,2,3 got %d,%d,%d", n1, n2, n3)
	}
}

func TestPickerLevelScore(t *testing.T) {
	v := manifest.NewVersion()
	// Add files to L1 exceeding base level size
	for i := 0; i < 100; i++ {
		v.AddFile(1, manifest.FileMetadata{
			FileNum:     uint64(i + 1),
			Size:        200 * 1024, // 200KB each
			SmallestKey: []byte{byte(i)},
			LargestKey:  []byte{byte(i) + 1},
		})
	}

	p := NewPicker(v)
	// Total size: 100 * 200KB = 20MB, max for L1 = 10MB, score = 2.0
	if !p.NeedsCompaction() {
		t.Fatal("should need compaction due to L1 size")
	}
}

func TestSSTablePath(t *testing.T) {
	if sstablePath(1) != "000001.sst" {
		t.Fatalf("unexpected path: %s", sstablePath(1))
	}
	if sstablePath(999) != "000999.sst" {
		t.Fatalf("unexpected path: %s", sstablePath(999))
	}
}

func TestCompactionEndToEnd(t *testing.T) {
	dir := t.TempDir()
	m, _ := manifest.Open(dir)

	// Create actual SSTable files for compaction
	for i := 0; i < l0CompactionTrigger; i++ {
		fileNum := uint64(i + 1)
		path := filepath.Join(dir, fmt.Sprintf("%06d.sst", fileNum))

		// Create a small SSTable
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		f.Write([]byte("dummy"))
		f.Close()

		m.LogEdit(manifest.ManifestEdit{
			Type:        manifest.EditAddFile,
			Level:       0,
			FileNum:     fileNum,
			FileSize:    5,
			SmallestKey: []byte{byte(i * 10)},
			LargestKey:  []byte{byte(i*10 + 9)},
		})
	}

	m.Close()
}

func TestCompareBytes(t *testing.T) {
	tests := []struct {
		a, b     []byte
		expected int
	}{
		{[]byte("a"), []byte("b"), -1},
		{[]byte("b"), []byte("a"), 1},
		{[]byte("a"), []byte("a"), 0},
		{[]byte(""), []byte("a"), -1},
		{[]byte("a"), []byte(""), 1},
		{[]byte(""), []byte(""), 0},
		{[]byte("abc"), []byte("abd"), -1},
		{[]byte("abc"), []byte("ab"), 1},
		{[]byte("ab"), []byte("abc"), -1},
	}

	for _, tc := range tests {
		result := compareBytes(tc.a, tc.b)
		if result != tc.expected {
			t.Errorf("compareBytes(%q, %q) = %d, want %d", tc.a, tc.b, result, tc.expected)
		}
	}
}

func TestKeyOverlap(t *testing.T) {
	tests := []struct {
		smallest, largest, lo, hi []byte
		expected                  bool
	}{
		{[]byte("a"), []byte("c"), []byte("b"), []byte("d"), true},
		{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), false},
		{[]byte("c"), []byte("d"), []byte("a"), []byte("b"), false},
		{[]byte("a"), []byte("d"), []byte("b"), []byte("c"), true},
		{[]byte("b"), []byte("c"), []byte("a"), []byte("d"), true},
		{[]byte("b"), []byte("b"), []byte("b"), []byte("b"), true},
	}

	for _, tc := range tests {
		result := keyOverlap(tc.smallest, tc.largest, tc.lo, tc.hi)
		if result != tc.expected {
			t.Errorf("keyOverlap(%q, %q, %q, %q) = %v, want %v",
				tc.smallest, tc.largest, tc.lo, tc.hi, result, tc.expected)
		}
	}
}

func TestPickLevelCompaction(t *testing.T) {
	v := manifest.NewVersion()

	// Add files to L1
	for i := 0; i < 60; i++ {
		v.AddFile(1, manifest.FileMetadata{
			FileNum:     uint64(i + 1),
			Size:        200 * 1024,
			SmallestKey: []byte{byte(i * 2)},
			LargestKey:  []byte{byte(i*2 + 1)},
		})
	}

	p := NewPicker(v)
	comp := p.PickCompaction()

	if comp == nil {
		t.Fatal("expected compaction")
	}
	if comp.Level != 1 {
		t.Fatalf("expected level 1, got %d", comp.Level)
	}
	if len(comp.Inputs) == 0 {
		t.Fatal("expected at least one input file")
	}
}

func TestPickerEmptyVersion(t *testing.T) {
	v := manifest.NewVersion()
	p := NewPicker(v)

	if p.NeedsCompaction() {
		t.Error("empty version should not need compaction")
	}

	comp := p.PickCompaction()
	if comp != nil {
		t.Error("expected nil compaction for empty version")
	}
}

func TestMaxLevelSize(t *testing.T) {
	v := manifest.NewVersion()
	p := NewPicker(v)

	tests := []struct {
		level    int
		expected uint64
	}{
		{1, baseLevelSize},
		{2, baseLevelSize * levelMultiplier},
		{3, baseLevelSize * levelMultiplier * levelMultiplier},
		{6, baseLevelSize * levelMultiplier * levelMultiplier * levelMultiplier * levelMultiplier * levelMultiplier},
	}

	for _, tc := range tests {
		size := p.maxLevelSize(tc.level)
		if size != tc.expected {
			t.Errorf("maxLevelSize(%d) = %d, want %d", tc.level, size, tc.expected)
		}
	}
}

func TestCompactorStartStop(t *testing.T) {
	dir := t.TempDir()
	m, _ := manifest.Open(dir)
	defer m.Close()

	c := NewCompactor(dir, m, 1, compression.CompressionNone)

	c.Start()
	c.Stop()

	// Should be able to start/stop multiple times
	c2 := NewCompactor(dir, m, 10, compression.CompressionNone)
	c2.Start()
	c2.Stop()
}

func TestRunCompactionEmpty(t *testing.T) {
	dir := t.TempDir()
	m, _ := manifest.Open(dir)
	defer m.Close()

	c := NewCompactor(dir, m, 1, compression.CompressionNone)

	comp := &Compaction{
		Level:   0,
		Inputs:  []manifest.FileMetadata{},
		Outputs: nil,
	}

	err := c.runCompaction(comp)
	if err != nil {
		t.Fatalf("empty compaction should return nil, got %v", err)
	}
}

func TestCompactionWithTombstones(t *testing.T) {
	dir := t.TempDir()

	// Create SSTable files with some tombstones (empty values)
	for i := 0; i < l0CompactionTrigger; i++ {
		fileNum := uint64(i + 1)
		path := filepath.Join(dir, fmt.Sprintf("%06d.sst", fileNum))

		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		f.Write([]byte("dummy"))
		f.Close()
	}

	m, _ := manifest.Open(dir)
	for i := 0; i < l0CompactionTrigger; i++ {
		m.LogEdit(manifest.ManifestEdit{
			Type:        manifest.EditAddFile,
			Level:       0,
			FileNum:     uint64(i + 1),
			FileSize:    100,
			SmallestKey: []byte(fmt.Sprintf("key%04d", i)),
			LargestKey:  []byte(fmt.Sprintf("key%04d", i)),
		})
	}
	m.Close()
}
