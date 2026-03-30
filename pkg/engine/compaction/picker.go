package compaction

import (
	"github.com/mammothengine/mammoth/pkg/engine/manifest"
)

const (
	maxLevel     = 6
	l0CompactionTrigger = 4
	baseLevelSize = 10 * 1024 * 1024 // 10MB
	levelMultiplier  = 10
)

// Picker selects files for compaction.
type Picker struct {
	version *manifest.Version
}

// NewPicker creates a new compaction picker.
func NewPicker(v *manifest.Version) *Picker {
	return &Picker{version: v}
}

// PickCompaction selects the best compaction to perform.
// Returns nil if no compaction is needed.
func (p *Picker) PickCompaction() *Compaction {
	// Check L0 first
	if p.version.NumFiles(0) >= l0CompactionTrigger {
		return p.pickL0Compaction()
	}

	// Check other levels
	for level := 1; level <= maxLevel; level++ {
		score := p.levelScore(level)
		if score >= 1.0 {
			return p.pickLevelCompaction(level)
		}
	}

	return nil
}

// NeedsCompaction returns true if compaction should be triggered.
func (p *Picker) NeedsCompaction() bool {
	if p.version.NumFiles(0) >= l0CompactionTrigger {
		return true
	}
	for level := 1; level <= maxLevel; level++ {
		if p.levelScore(level) >= 1.0 {
			return true
		}
	}
	return false
}

func (p *Picker) levelScore(level int) float64 {
	totalSize := uint64(0)
	for _, f := range p.version.Files(level) {
		totalSize += f.Size
	}
	maxSize := p.maxLevelSize(level)
	if maxSize == 0 {
		return 0
	}
	return float64(totalSize) / float64(maxSize)
}

func (p *Picker) maxLevelSize(level int) uint64 {
	size := uint64(baseLevelSize)
	for i := 1; i < level; i++ {
		size *= levelMultiplier
	}
	return size
}

func (p *Picker) pickL0Compaction() *Compaction {
	files := p.version.Files(0)
	if len(files) == 0 {
		return nil
	}

	// Compact all L0 files into L1
	inputs := make([]manifest.FileMetadata, len(files))
	copy(inputs, files)

	return &Compaction{
		Level:   0,
		Inputs:  inputs,
		Outputs: nil,
	}
}

func (p *Picker) pickLevelCompaction(level int) *Compaction {
	files := p.version.Files(level)
	if len(files) == 0 {
		return nil
	}

	// Pick first file for compaction (simplified strategy)
	inputs := []manifest.FileMetadata{files[0]}

	// Extend to include overlapping files in the same level
	smallest := files[0].SmallestKey
	largest := files[0].LargestKey

	for _, f := range files[1:] {
		if keyOverlap(f.SmallestKey, f.LargestKey, smallest, largest) {
			inputs = append(inputs, f)
		}
	}

	return &Compaction{
		Level:   level,
		Inputs:  inputs,
		Outputs: nil,
	}
}

func keyOverlap(smallest, largest, lo, hi []byte) bool {
	return compareBytes(smallest, hi) <= 0 && compareBytes(largest, lo) >= 0
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
