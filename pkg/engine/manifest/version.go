package manifest

import "sort"

// FileMetadata holds metadata about an SSTable file.
type FileMetadata struct {
	FileNum       uint64
	Size          uint64
	SmallestKey   []byte
	LargestKey    []byte
	Level         int
}

// Version represents a snapshot of the SSTable files at each level.
type Version struct {
	files [7][]FileMetadata // 7 levels (L0-L6)
}

// NewVersion creates a new empty version.
func NewVersion() *Version {
	return &Version{}
}

// Files returns the files at the given level.
func (v *Version) Files(level int) []FileMetadata {
	if level < 0 || level >= 7 {
		return nil
	}
	return v.files[level]
}

// AddFile adds a file to a level.
func (v *Version) AddFile(level int, meta FileMetadata) {
	if level < 0 || level >= 7 {
		return
	}
	meta.Level = level
	v.files[level] = append(v.files[level], meta)
	v.sortLevel(level)
}

// RemoveFile removes a file from a level.
func (v *Version) RemoveFile(level int, fileNum uint64) {
	if level < 0 || level >= 7 {
		return
	}
	files := v.files[level]
	for i, f := range files {
		if f.FileNum == fileNum {
			v.files[level] = append(files[:i], files[i+1:]...)
			return
		}
	}
}

// NumFiles returns the number of files at a level.
func (v *Version) NumFiles(level int) int {
	if level < 0 || level >= 7 {
		return 0
	}
	return len(v.files[level])
}

// TotalSize returns the total size of all files.
func (v *Version) TotalSize() uint64 {
	var total uint64
	for level := range v.files {
		for _, f := range v.files[level] {
			total += f.Size
		}
	}
	return total
}

// Clone creates a deep copy of the version.
func (v *Version) Clone() *Version {
	nv := NewVersion()
	for level := range v.files {
		nv.files[level] = make([]FileMetadata, len(v.files[level]))
		copy(nv.files[level], v.files[level])
	}
	return nv
}

func (v *Version) sortLevel(level int) {
	sort.Slice(v.files[level], func(i, j int) bool {
		return compareBytes(v.files[level][i].SmallestKey, v.files[level][j].SmallestKey) < 0
	})
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
