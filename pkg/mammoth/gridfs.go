package mammoth

import (
	"crypto/md5"
	"errors"
	"fmt"
	"hash"
	"io"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

const (
	// DefaultChunkSize is the default GridFS chunk size (255KB).
	DefaultChunkSize = 255 * 1024
	// MaxChunkSize is the maximum allowed chunk size (16MB - 1MB overhead).
	MaxChunkSize = 15 * 1024 * 1024
	// MinChunkSize is the minimum allowed chunk size.
	MinChunkSize = 1024
)

// ErrFileNotFound is returned when a GridFS file is not found.
var ErrFileNotFound = errors.New("gridfs: file not found")

// ErrInvalidChunkSize is returned when an invalid chunk size is specified.
var ErrInvalidChunkSize = errors.New("gridfs: invalid chunk size")

// GridFSBucket manages a GridFS bucket for storing large files.
type GridFSBucket struct {
	db         *Database
	name       string // bucket name (default: "fs")
	chunkSize  int32
	cat        *mongo.Catalog

	// Collections
	filesColl   *Collection
	chunksColl  *Collection
}

// GridFSFile represents metadata for a stored file.
type GridFSFile struct {
	ID          interface{}            `json:"_id" bson:"_id"`
	Filename    string                 `json:"filename" bson:"filename"`
	Length      int64                  `json:"length" bson:"length"`
	ChunkSize   int32                  `json:"chunkSize" bson:"chunkSize"`
	UploadDate  time.Time              `json:"uploadDate" bson:"uploadDate"`
	Metadata    map[string]interface{} `json:"metadata,omitempty" bson:"metadata,omitempty"`
	ContentType string                 `json:"contentType,omitempty" bson:"contentType,omitempty"`
	Aliases     []string               `json:"aliases,omitempty" bson:"aliases,omitempty"`
	MD5         string                 `json:"md5" bson:"md5"`
}

// UploadOptions configures file upload behavior.
type UploadOptions struct {
	ChunkSizeBytes int32
	Metadata       map[string]interface{}
	ContentType    string
	Aliases        []string
}

// GridFSUploadStream is a write stream for uploading files to GridFS.
type GridFSUploadStream struct {
	bucket      *GridFSBucket
	filename    string
	id          interface{}
	chunkSize   int32
	metadata    map[string]interface{}
	contentType string
	aliases     []string

	// Write state
	buffer      []byte
	chunkIndex  int32
	length      int64
	hasher      hash.Hash
	closed      bool
	aborted     bool

	// Transaction support
	tx          *Transaction
}

// GridFSDownloadStream is a read stream for downloading files from GridFS.
type GridFSDownloadStream struct {
	bucket     *GridFSBucket
	file       *GridFSFile
	position   int64
	chunkCache []byte
	chunkIndex int32
	closed     bool
}

// OpenBucket opens a GridFS bucket with the given options.
func (db *Database) OpenBucket(opts *BucketOptions) (*GridFSBucket, error) {
	if opts == nil {
		opts = &BucketOptions{}
	}

	name := opts.Name
	if name == "" {
		name = "fs"
	}

	chunkSize := opts.ChunkSizeBytes
	if chunkSize == 0 {
		chunkSize = DefaultChunkSize
	}
	if chunkSize < MinChunkSize || chunkSize > MaxChunkSize {
		return nil, ErrInvalidChunkSize
	}

	// Ensure collections exist
	filesCollName := name + ".files"
	chunksCollName := name + ".chunks"

	filesColl, err := db.Collection(filesCollName)
	if err != nil {
		return nil, fmt.Errorf("gridfs: create files collection: %w", err)
	}

	chunksColl, err := db.Collection(chunksCollName)
	if err != nil {
		return nil, fmt.Errorf("gridfs: create chunks collection: %w", err)
	}

	bucket := &GridFSBucket{
		db:         db,
		name:       name,
		chunkSize:  chunkSize,
		filesColl:  filesColl,
		chunksColl: chunksColl,
	}

	// Create indexes for efficient lookups
	if err := bucket.createIndexes(); err != nil {
		return nil, fmt.Errorf("gridfs: create indexes: %w", err)
	}

	return bucket, nil
}

// BucketOptions configures GridFS bucket creation.
type BucketOptions struct {
	Name           string
	ChunkSizeBytes int32
}

// createIndexes creates required indexes for GridFS.
func (b *GridFSBucket) createIndexes() error {
	// Files collection: unique index on filename + uploadDate
	filesIdx := map[string]interface{}{
		"filename":   1,
		"uploadDate": 1,
	}
	if _, err := b.db.CreateIndex(b.name+".files", filesIdx, IndexOptions{Name: "filename_1_uploadDate_1"}); err != nil {
		// Index may already exist, continue
	}

	// Chunks collection: unique compound index on files_id + n
	chunksIdx := map[string]interface{}{
		"files_id": 1,
		"n":        1,
	}
	if _, err := b.db.CreateIndex(b.name+".chunks", chunksIdx, IndexOptions{Name: "files_id_1_n_1", Unique: true}); err != nil {
		// Index may already exist, continue
	}

	return nil
}

// OpenUploadStream opens a stream for writing a file to GridFS.
func (b *GridFSBucket) OpenUploadStream(filename string, opts ...UploadOptions) (*GridFSUploadStream, error) {
	id := bson.NewObjectID()
	return b.OpenUploadStreamWithID(id, filename, opts...)
}

// OpenUploadStreamWithID opens a stream for writing with a specific file ID.
func (b *GridFSBucket) OpenUploadStreamWithID(id interface{}, filename string, opts ...UploadOptions) (*GridFSUploadStream, error) {
	opt := UploadOptions{}
	if len(opts) > 0 {
		opt = opts[0]
	}

	chunkSize := opt.ChunkSizeBytes
	if chunkSize == 0 {
		chunkSize = b.chunkSize
	}

	h := md5.New()

	return &GridFSUploadStream{
		bucket:      b,
		filename:    filename,
		id:          id,
		chunkSize:   chunkSize,
		metadata:    opt.Metadata,
		contentType: opt.ContentType,
		aliases:     opt.Aliases,
		buffer:      make([]byte, 0, chunkSize),
		hasher:      h,
	}, nil
}

// UploadFromStream uploads data from a reader to GridFS.
func (b *GridFSBucket) UploadFromStream(filename string, source io.Reader, opts ...UploadOptions) (interface{}, error) {
	stream, err := b.OpenUploadStream(filename, opts...)
	if err != nil {
		return nil, err
	}
	defer stream.Close()

	if _, err := io.Copy(stream, source); err != nil {
		stream.Abort()
		return nil, err
	}

	return stream.FileID(), nil
}

// Write writes data to the upload stream.
func (us *GridFSUploadStream) Write(p []byte) (int, error) {
	if us.closed {
		return 0, errors.New("gridfs: write to closed stream")
	}
	if us.aborted {
		return 0, errors.New("gridfs: write to aborted stream")
	}

	written := 0

	for len(p) > 0 {
		// Calculate how much we can buffer
		space := int(us.chunkSize) - len(us.buffer)
		if space > len(p) {
			space = len(p)
		}

		// Add to buffer
		us.buffer = append(us.buffer, p[:space]...)
		us.hasher.Write(p[:space])
		p = p[space:]
		written += space
		us.length += int64(space)

		// If buffer is full, flush chunk
		if len(us.buffer) >= int(us.chunkSize) {
			if err := us.flushChunk(); err != nil {
				return written, err
			}
		}
	}

	return written, nil
}

// flushChunk writes the current buffer as a chunk.
func (us *GridFSUploadStream) flushChunk() error {
	if len(us.buffer) == 0 {
		return nil
	}

	chunkDoc := map[string]interface{}{
		"files_id": us.id,
		"n":        us.chunkIndex,
		"data":     us.buffer,
	}

	if _, err := us.bucket.chunksColl.InsertOne(chunkDoc); err != nil {
		return fmt.Errorf("gridfs: write chunk %d: %w", us.chunkIndex, err)
	}

	us.chunkIndex++
	us.buffer = us.buffer[:0] // Reset buffer

	return nil
}

// Close flushes remaining data and writes file metadata.
func (us *GridFSUploadStream) Close() error {
	if us.closed {
		return nil
	}
	if us.aborted {
		return errors.New("gridfs: cannot close aborted stream")
	}

	// Flush final chunk
	if err := us.flushChunk(); err != nil {
		return err
	}

	// Write file metadata
	fileDoc := map[string]interface{}{
		"_id":         us.id,
		"filename":    us.filename,
		"length":      us.length,
		"chunkSize":   us.chunkSize,
		"uploadDate":  time.Now().UnixMilli(),
		"md5":         fmt.Sprintf("%x", us.hasher.Sum(nil)),
	}

	if us.contentType != "" {
		fileDoc["contentType"] = us.contentType
	}
	if len(us.aliases) > 0 {
		fileDoc["aliases"] = us.aliases
	}
	if us.metadata != nil {
		fileDoc["metadata"] = us.metadata
	}

	if _, err := us.bucket.filesColl.InsertOne(fileDoc); err != nil {
		return fmt.Errorf("gridfs: write file metadata: %w", err)
	}

	us.closed = true
	return nil
}

// Abort cancels the upload and deletes any written chunks.
func (us *GridFSUploadStream) Abort() error {
	if us.closed {
		return errors.New("gridfs: cannot abort closed stream")
	}
	if us.aborted {
		return nil
	}

	us.aborted = true

	// Delete any chunks that were written
	for i := int32(0); i < us.chunkIndex; i++ {
		filter := map[string]interface{}{
			"files_id": us.id,
			"n":        i,
		}
		us.bucket.chunksColl.DeleteOne(filter)
	}

	return nil
}

// FileID returns the file ID for this upload stream.
func (us *GridFSUploadStream) FileID() interface{} {
	return us.id
}

// SetTransaction associates this upload with a transaction.
func (us *GridFSUploadStream) SetTransaction(tx *Transaction) {
	us.tx = tx
}

// OpenDownloadStream opens a stream for reading a file from GridFS by ID.
func (b *GridFSBucket) OpenDownloadStream(fileID interface{}) (*GridFSDownloadStream, error) {
	file, err := b.FindOne(fileID)
	if err != nil {
		return nil, err
	}

	return &GridFSDownloadStream{
		bucket:     b,
		file:       file,
		chunkIndex: -1,
	}, nil
}

// OpenDownloadStreamByName opens a stream for reading a file by name.
// If multiple files have the same name, returns the most recent upload.
func (b *GridFSBucket) OpenDownloadStreamByName(filename string) (*GridFSDownloadStream, error) {
	file, err := b.FindOneByName(filename)
	if err != nil {
		return nil, err
	}

	return &GridFSDownloadStream{
		bucket:     b,
		file:       file,
		chunkIndex: -1,
	}, nil
}

// DownloadToStream downloads a file to a writer.
func (b *GridFSBucket) DownloadToStream(fileID interface{}, dest io.Writer) (int64, error) {
	stream, err := b.OpenDownloadStream(fileID)
	if err != nil {
		return 0, err
	}
	defer stream.Close()

	return io.Copy(dest, stream)
}

// Read reads data from the download stream.
func (ds *GridFSDownloadStream) Read(p []byte) (int, error) {
	if ds.closed {
		return 0, errors.New("gridfs: read from closed stream")
	}

	if ds.position >= ds.file.Length {
		return 0, io.EOF
	}

	read := 0
	for read < len(p) && ds.position < ds.file.Length {
		// Ensure we have the right chunk loaded
		chunkIdx := int32(ds.position / int64(ds.file.ChunkSize))
		if ds.chunkIndex != chunkIdx {
			if err := ds.loadChunk(chunkIdx); err != nil {
				return read, err
			}
		}

		// Calculate offset within chunk
		offset := int(ds.position % int64(ds.file.ChunkSize))
		available := len(ds.chunkCache) - offset
		if available <= 0 {
			return read, io.EOF
		}

		// Copy data
		toCopy := len(p) - read
		if toCopy > available {
			toCopy = available
		}
		copy(p[read:], ds.chunkCache[offset:offset+toCopy])
		read += toCopy
		ds.position += int64(toCopy)
	}

	return read, nil
}

// loadChunk loads a specific chunk into the cache.
func (ds *GridFSDownloadStream) loadChunk(index int32) error {
	filter := map[string]interface{}{
		"files_id": ds.file.ID,
		"n":        index,
	}

	doc, err := ds.bucket.chunksColl.FindOne(filter)
	if err != nil {
		if err == ErrNotFound {
			return fmt.Errorf("gridfs: chunk %d not found", index)
		}
		return err
	}

	data, ok := doc["data"].([]byte)
	if !ok {
		return fmt.Errorf("gridfs: chunk %d has no data", index)
	}

	ds.chunkCache = data
	ds.chunkIndex = index
	return nil
}

// Seek implements io.Seeker for range reads.
func (ds *GridFSDownloadStream) Seek(offset int64, whence int) (int64, error) {
	if ds.closed {
		return 0, errors.New("gridfs: seek on closed stream")
	}

	var newPos int64
	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = ds.position + offset
	case io.SeekEnd:
		newPos = ds.file.Length + offset
	default:
		return 0, errors.New("gridfs: invalid seek whence")
	}

	if newPos < 0 {
		return 0, errors.New("gridfs: seek to negative position")
	}

	ds.position = newPos
	return ds.position, nil
}

// Close closes the download stream.
func (ds *GridFSDownloadStream) Close() error {
	ds.closed = true
	ds.chunkCache = nil
	return nil
}

// GetFile returns the file metadata for this stream.
func (ds *GridFSDownloadStream) GetFile() *GridFSFile {
	return ds.file
}

// FindOne finds a file by ID.
func (b *GridFSBucket) FindOne(fileID interface{}) (*GridFSFile, error) {
	doc, err := b.filesColl.FindOne(map[string]interface{}{"_id": fileID})
	if err != nil {
		if err == ErrNotFound {
			return nil, ErrFileNotFound
		}
		return nil, err
	}

	return b.docToFile(doc)
}

// FindOneByName finds the most recent file with the given name.
func (b *GridFSBucket) FindOneByName(filename string) (*GridFSFile, error) {
	// Find all files with this name and sort by uploadDate
	cursor, err := b.filesColl.FindWithOptions(FindOptions{
		Filter: map[string]interface{}{"filename": filename},
		Sort:   map[string]interface{}{"uploadDate": -1},
	})
	if err != nil {
		return nil, err
	}
	defer cursor.Close()

	if !cursor.Next() {
		return nil, ErrFileNotFound
	}

	var doc map[string]interface{}
	if err := cursor.Decode(&doc); err != nil {
		return nil, err
	}

	return b.docToFile(doc)
}

// Find finds files matching the filter.
func (b *GridFSBucket) Find(filter map[string]interface{}) ([]*GridFSFile, error) {
	cursor, err := b.filesColl.Find(filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()

	var files []*GridFSFile
	for cursor.Next() {
		var doc map[string]interface{}
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}
		file, err := b.docToFile(doc)
		if err != nil {
			return nil, err
		}
		files = append(files, file)
	}

	return files, nil
}

// Delete deletes a file and all its chunks by ID.
func (b *GridFSBucket) Delete(fileID interface{}) error {
	// Delete chunks first
	_, err := b.chunksColl.DeleteMany(map[string]interface{}{"files_id": fileID})
	if err != nil {
		return fmt.Errorf("gridfs: delete chunks: %w", err)
	}

	// Delete file metadata
	_, err = b.filesColl.DeleteOne(map[string]interface{}{"_id": fileID})
	if err != nil {
		return fmt.Errorf("gridfs: delete file: %w", err)
	}

	return nil
}

// DeleteByName deletes all files with the given name.
func (b *GridFSBucket) DeleteByName(filename string) error {
	files, err := b.Find(map[string]interface{}{"filename": filename})
	if err != nil {
		return err
	}

	for _, file := range files {
		if err := b.Delete(file.ID); err != nil {
			return err
		}
	}

	return nil
}

// Rename renames a file.
func (b *GridFSBucket) Rename(fileID interface{}, newFilename string) error {
	_, err := b.filesColl.UpdateOne(
		map[string]interface{}{"_id": fileID},
		map[string]interface{}{"$set": map[string]interface{}{"filename": newFilename}},
	)
	return err
}

// Drop deletes the entire bucket (all files and chunks).
func (b *GridFSBucket) Drop() error {
	if err := b.db.DropCollection(b.name + ".chunks"); err != nil {
		return err
	}
	if err := b.db.DropCollection(b.name + ".files"); err != nil {
		return err
	}
	return nil
}

// docToFile converts a document to GridFSFile.
func (b *GridFSBucket) docToFile(doc map[string]interface{}) (*GridFSFile, error) {
	// Convert ID back to ObjectID if it's a string representation
	id := doc["_id"]
	if idStr, ok := id.(string); ok && len(idStr) == 24 {
		if oid, err := bson.ParseObjectID(idStr); err == nil {
			id = oid
		}
	}

	file := &GridFSFile{
		ID:         id,
		Filename:   getString(doc, "filename"),
		Length:     getInt64(doc, "length"),
		ChunkSize:  int32(getInt64(doc, "chunkSize")),
		UploadDate: getTime(doc, "uploadDate"),
		MD5:        getString(doc, "md5"),
		Metadata:   getMap(doc, "metadata"),
	}

	if _, ok := doc["contentType"]; ok {
		file.ContentType = getString(doc, "contentType")
	}

	if aliases, ok := doc["aliases"]; ok {
		if arr, ok := aliases.([]interface{}); ok {
			file.Aliases = make([]string, len(arr))
			for i, a := range arr {
				file.Aliases[i] = fmt.Sprintf("%v", a)
			}
		}
	}

	return file, nil
}

// Helper functions for type conversion

func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		return fmt.Sprintf("%v", v)
	}
	return ""
}

func getInt64(m map[string]interface{}, key string) int64 {
	if v, ok := m[key]; ok {
		switch val := v.(type) {
		case int:
			return int64(val)
		case int32:
			return int64(val)
		case int64:
			return val
		case float64:
			return int64(val)
		}
	}
	return 0
}

func getTime(m map[string]interface{}, key string) time.Time {
	if v, ok := m[key]; ok {
		switch val := v.(type) {
		case time.Time:
			return val
		case int64:
			return time.UnixMilli(val)
		case float64:
			return time.UnixMilli(int64(val))
		}
	}
	return time.Time{}
}

func getMap(m map[string]interface{}, key string) map[string]interface{} {
	if v, ok := m[key]; ok {
		if m, ok := v.(map[string]interface{}); ok {
			return m
		}
	}
	return nil
}

// Ensure GridFSDownloadStream implements io.Seeker
var _ io.Seeker = (*GridFSDownloadStream)(nil)
