package mongo

import (
	"container/heap"
	"encoding/binary"
	"os"
	"sort"

	"github.com/mammothengine/mammoth/pkg/bson"
)

const defaultMemLimit int64 = 100 * 1024 * 1024 // 100 MB

// ExternalSorter implements an external merge sort for BSON documents.
// Documents accumulate in memory and are flushed to sorted temp-file runs
// when the memory limit is exceeded. Sort performs a K-way merge of all runs.
type ExternalSorter struct {
	memLimit int64
	runs     []*os.File
	less     func(a, b *bson.Document) bool
	buf      []*bson.Document
	bufSize  int64
	tmpDir   string
}

// NewExternalSorter creates a new external sorter. If memLimit <= 0 it
// defaults to 100 MB. The less function defines the sort order.
func NewExternalSorter(memLimit int64, less func(a, b *bson.Document) bool) *ExternalSorter {
	if memLimit <= 0 {
		memLimit = defaultMemLimit
	}
	return &ExternalSorter{
		memLimit: memLimit,
		less:     less,
		buf:      make([]*bson.Document, 0),
	}
}

// Add appends a document to the in-memory buffer. When the buffer size reaches
// memLimit the buffer is sorted and flushed to a temporary run file.
func (s *ExternalSorter) Add(doc *bson.Document) error {
	s.buf = append(s.buf, doc)
	s.bufSize += int64(len(bson.Encode(doc)))
	if s.bufSize >= s.memLimit {
		return s.flush()
	}
	return nil
}

// Sort returns all added documents in sorted order. If only in-memory data
// exists it is sorted and returned directly. Otherwise a K-way merge of all
// on-disk runs (plus any remaining in-memory buffer) is performed.
func (s *ExternalSorter) Sort() ([]*bson.Document, error) {
	// Pure in-memory path: no runs were ever created.
	if len(s.runs) == 0 {
		sort.Slice(s.buf, func(i, j int) bool {
			return s.less(s.buf[i], s.buf[j])
		})
		result := s.buf
		s.buf = nil
		s.bufSize = 0
		return result, nil
	}

	// Flush whatever remains in memory as the final run.
	if len(s.buf) > 0 {
		if err := s.flush(); err != nil {
			return nil, err
		}
	}

	// K-way merge using a heap.
	return s.merge()
}

// Close removes all temporary run files.
func (s *ExternalSorter) Close() error {
	var firstErr error
	for _, f := range s.runs {
		name := f.Name()
		f.Close()
		if err := os.Remove(name); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.runs = nil
	s.buf = nil
	s.bufSize = 0
	return firstErr
}

// flush sorts the current in-memory buffer and writes it as a new run file.
func (s *ExternalSorter) flush() error {
	sort.Slice(s.buf, func(i, j int) bool {
		return s.less(s.buf[i], s.buf[j])
	})

	dir := s.tmpDir
	if dir == "" {
		dir = os.TempDir()
	}

	f, err := os.CreateTemp(dir, "mammoth_extsort_*")
	if err != nil {
		return err
	}

	for _, doc := range s.buf {
		if err := writeDoc(f, doc); err != nil {
			f.Close()
			os.Remove(f.Name())
			return err
		}
	}

	// Seek back to start so the merge reader can read from the beginning.
	if _, err := f.Seek(0, 0); err != nil {
		f.Close()
		os.Remove(f.Name())
		return err
	}

	s.runs = append(s.runs, f)
	s.buf = s.buf[:0]
	s.bufSize = 0
	return nil
}

// merge performs a K-way merge over all run files using a min-heap.
func (s *ExternalSorter) merge() ([]*bson.Document, error) {
	items := make([]*mergeItem, 0, len(s.runs))
	for i, f := range s.runs {
		doc, err := readDoc(f)
		if err != nil {
			return nil, err
		}
		if doc != nil {
			items = append(items, &mergeItem{
				doc:  doc,
				run:  i,
				file: f,
			})
		}
	}

	h := &mergeHeap{
		items: items,
		less:  s.less,
	}
	heap.Init(h)

	var result []*bson.Document
	for h.Len() > 0 {
		top := heap.Pop(h).(*mergeItem)
		result = append(result, top.doc)

		doc, err := readDoc(top.file)
		if err != nil {
			return nil, err
		}
		if doc != nil {
			heap.Push(h, &mergeItem{
				doc:  doc,
				run:  top.run,
				file: top.file,
			})
		}
	}

	return result, nil
}

// --- Run file format helpers ---

// writeDoc appends one document to the run file as [length:4 LE][bson_bytes].
func writeDoc(f *os.File, doc *bson.Document) error {
	data := bson.Encode(doc)
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(data)))
	if _, err := f.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err := f.Write(data)
	return err
}

// readDoc reads the next document from a run file. Returns (nil, nil) on EOF.
func readDoc(f *os.File) (*bson.Document, error) {
	var lenBuf [4]byte
	n, err := f.Read(lenBuf[:])
	if err != nil {
		return nil, nil // EOF or error treated as end of run
	}
	if n < 4 {
		return nil, nil
	}

	size := int(binary.LittleEndian.Uint32(lenBuf[:]))
	data := make([]byte, size)
	totalRead := 0
	for totalRead < size {
		rn, err := f.Read(data[totalRead:])
		if err != nil {
			return nil, err
		}
		totalRead += rn
	}

	return bson.Decode(data)
}

// --- Merge heap ---

type mergeItem struct {
	doc  *bson.Document
	run  int
	file *os.File
}

type mergeHeap struct {
	items []*mergeItem
	less  func(a, b *bson.Document) bool
}

func (h *mergeHeap) Len() int { return len(h.items) }

func (h *mergeHeap) Less(i, j int) bool {
	return h.less(h.items[i].doc, h.items[j].doc)
}

func (h *mergeHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *mergeHeap) Push(x interface{}) {
	h.items = append(h.items, x.(*mergeItem))
}

func (h *mergeHeap) Pop() interface{} {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[:n-1]
	return item
}
