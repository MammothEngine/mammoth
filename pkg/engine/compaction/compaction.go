package compaction

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/mammothengine/mammoth/pkg/engine/manifest"
	"github.com/mammothengine/mammoth/pkg/engine/sstable"

	"github.com/mammothengine/mammoth/pkg/engine/compression"
)

// Compaction represents an in-progress compaction.
type Compaction struct {
	Level   int
	Inputs  []manifest.FileMetadata
	Outputs []string // output SSTable paths
}

// Compactor performs background compaction.
type Compactor struct {
	mu          sync.Mutex
	dir         string
	manifest    *manifest.Manifest
	nextFileNum uint64
	compression compression.CompressionType
	stopCh      chan struct{}
	doneCh      chan struct{}
}

// NewCompactor creates a new compactor.
func NewCompactor(dir string, m *manifest.Manifest, nextFileNum uint64, comp compression.CompressionType) *Compactor {
	return &Compactor{
		dir:         dir,
		manifest:    m,
		nextFileNum: nextFileNum,
		compression: comp,
		stopCh:      make(chan struct{}),
		doneCh:      make(chan struct{}),
	}
}

// Start begins background compaction.
func (c *Compactor) Start() {
	go c.run()
}

// Stop stops the compactor.
func (c *Compactor) Stop() {
	close(c.stopCh)
	<-c.doneCh
}

// NextFileNum returns the next available file number.
func (c *Compactor) NextFileNum() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := c.nextFileNum
	c.nextFileNum++
	return n
}

// MaybeCompact checks if compaction is needed and runs it.
func (c *Compactor) MaybeCompact() error {
	v := c.manifest.CurrentVersion()
	picker := NewPicker(v)

	if !picker.NeedsCompaction() {
		return nil
	}

	comp := picker.PickCompaction()
	if comp == nil {
		return nil
	}

	return c.runCompaction(comp)
}

func (c *Compactor) run() {
	defer close(c.doneCh)
	for {
		select {
		case <-c.stopCh:
			return
		default:
			if err := c.MaybeCompact(); err != nil {
				// Log and continue
				_ = err
			}
			return // One-shot for now, engine will trigger as needed
		}
	}
}

func (c *Compactor) runCompaction(comp *Compaction) error {
	if len(comp.Inputs) == 0 {
		return nil
	}

	outputLevel := comp.Level + 1
	if outputLevel > maxLevel {
		outputLevel = maxLevel
	}

	// Open all input SSTables and merge
	var iters []*sstable.Iterator
	for _, f := range comp.Inputs {
		path := filepath.Join(c.dir, sstablePath(f.FileNum))
		r, err := sstable.NewReader(path, sstable.ReaderOptions{
			Compression: c.compression,
		})
		if err != nil {
			return fmt.Errorf("compaction: open input: %w", err)
		}
		defer r.Close()
		iters = append(iters, sstable.NewIterator(r))
	}

	// Create output SSTable
	outNum := c.NextFileNum()
	outPath := filepath.Join(c.dir, sstablePath(outNum))

	w, err := sstable.NewWriter(sstable.WriterOptions{
		Path:        outPath,
		ExpectedKeys: 10000,
		Compression: c.compression,
	})
	if err != nil {
		return fmt.Errorf("compaction: create output: %w", err)
	}

	// Merge and write
	merge := sstable.NewMergeIterator(iters)
	var smallestKey, largestKey []byte

	for merge.Valid() {
		key := merge.Key()
		value := merge.Value()

		if len(smallestKey) == 0 || compareBytes(key, smallestKey) < 0 {
			smallestKey = append([]byte{}, key...)
		}
		if len(largestKey) == 0 || compareBytes(key, largestKey) > 0 {
			largestKey = append([]byte{}, key...)
		}

		// Skip tombstones (nil values) at the bottom level
		if outputLevel == maxLevel && len(value) == 0 {
			merge.Next()
			continue
		}

		if err := w.Add(key, value); err != nil {
			w.Close()
			os.Remove(outPath)
			return err
		}
		merge.Next()
	}

	if _, err := w.Finish(); err != nil {
		os.Remove(outPath)
		return err
	}

	// Get output file size
	fi, err := os.Stat(outPath)
	if err != nil {
		os.Remove(outPath)
		return err
	}

	// Update manifest atomically
	var edits []manifest.ManifestEdit

	// Add new file
	edits = append(edits, manifest.ManifestEdit{
		Type:        manifest.EditAddFile,
		Level:       outputLevel,
		FileNum:     outNum,
		FileSize:    uint64(fi.Size()),
		SmallestKey: smallestKey,
		LargestKey:  largestKey,
	})

	// Remove old files
	for _, f := range comp.Inputs {
		edits = append(edits, manifest.ManifestEdit{
			Type:    manifest.EditRemoveFile,
			Level:   comp.Level,
			FileNum: f.FileNum,
		})
	}

	if err := c.manifest.LogBatch(edits); err != nil {
		os.Remove(outPath)
		return err
	}

	// Clean up old SSTable files
	for _, f := range comp.Inputs {
		os.Remove(filepath.Join(c.dir, sstablePath(f.FileNum)))
	}

	return nil
}

func sstablePath(fileNum uint64) string {
	return fmt.Sprintf("%06d.sst", fileNum)
}
