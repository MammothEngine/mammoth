package mongo

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"sort"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

const defaultChunkSize = 255 * 1024 // 255 KB

// GridFS stores large files by splitting them into chunks backed by the KV engine.
type GridFS struct {
	db        string
	eng       *engine.Engine
	cat       *Catalog
	chunkSize int
}

// FileInfo holds metadata for a stored file.
type FileInfo struct {
	ID         bson.ObjectID
	Filename   string
	Length     int64
	ChunkSize  int
	UploadDate time.Time
	MD5        string
}

// NewGridFS creates a GridFS instance for the given database.
func NewGridFS(db string, eng *engine.Engine, cat *Catalog) *GridFS {
	return &GridFS{
		db:        db,
		eng:       eng,
		cat:       cat,
		chunkSize: defaultChunkSize,
	}
}

// chunkKey builds the engine key for a single chunk:
//
//	{ns_prefix_fs.chunks}{files_id_12bytes}{n_4bytes_bigendian}
func (g *GridFS) chunkKey(id bson.ObjectID, n int) []byte {
	prefix := EncodeNamespacePrefix(g.db, "fs.chunks")
	key := make([]byte, 0, len(prefix)+12+4)
	key = append(key, prefix...)
	key = append(key, id[:]...)
	key = binary.BigEndian.AppendUint32(key, uint32(n))
	return key
}

// chunksPrefix returns the prefix used to scan all chunks belonging to a file.
func (g *GridFS) chunksPrefix(id bson.ObjectID) []byte {
	prefix := EncodeNamespacePrefix(g.db, "fs.chunks")
	key := make([]byte, 0, len(prefix)+12)
	key = append(key, prefix...)
	key = append(key, id[:]...)
	return key
}

// metaKey builds the engine key for file metadata:
//
//	{ns_prefix_fs.files}{filename}
func (g *GridFS) metaKey(filename string) []byte {
	prefix := EncodeNamespacePrefix(g.db, "fs.files")
	key := make([]byte, 0, len(prefix)+len(filename))
	key = append(key, prefix...)
	key = append(key, filename...)
	return key
}

// metaPrefix returns the prefix used to scan all file metadata entries.
func (g *GridFS) metaPrefix() []byte {
	return EncodeNamespacePrefix(g.db, "fs.files")
}

// UploadFile splits data into chunks and stores metadata.
// Returns the generated ObjectID for the file.
func (g *GridFS) UploadFile(filename string, data []byte) (bson.ObjectID, error) {
	id := bson.NewObjectID()
	length := int64(len(data))

	// Compute MD5
	hash := md5.Sum(data)
	md5hex := fmt.Sprintf("%x", hash[:])

	// Split into chunks and store each one
	offset := 0
	for n := 0; offset < len(data); n++ {
		end := offset + g.chunkSize
		if end > len(data) {
			end = len(data)
		}
		chunkData := data[offset:end]

		// Store as BSON document: {files_id, n, data}
		chunkDoc := bson.NewDocument()
		chunkDoc.Set("files_id", bson.VObjectID(id))
		chunkDoc.Set("n", bson.VInt32(int32(n)))
		chunkDoc.Set("data", bson.VBinary(bson.BinaryGeneric, chunkData))

		encoded := bson.Encode(chunkDoc)
		key := g.chunkKey(id, n)
		if err := g.eng.Put(key, encoded); err != nil {
			// Attempt cleanup of already-written chunks
			g.deleteChunks(id, n)
			return bson.ObjectID{}, fmt.Errorf("gridfs: write chunk %d: %w", n, err)
		}
		offset = end
	}

	// Build and store file metadata
	metaDoc := bson.NewDocument()
	metaDoc.Set("_id", bson.VObjectID(id))
	metaDoc.Set("filename", bson.VString(filename))
	metaDoc.Set("length", bson.VInt64(length))
	metaDoc.Set("chunkSize", bson.VInt32(int32(g.chunkSize)))
	metaDoc.Set("uploadDate", bson.VDateTime(time.Now().UnixMilli()))
	metaDoc.Set("md5", bson.VString(md5hex))

	metaKey := g.metaKey(filename)
	if err := g.eng.Put(metaKey, bson.Encode(metaDoc)); err != nil {
		g.deleteChunks(id, offset/g.chunkSize)
		return bson.ObjectID{}, fmt.Errorf("gridfs: write metadata: %w", err)
	}

	return id, nil
}

// DownloadFile reads all chunks for a file and reassembles the data.
func (g *GridFS) DownloadFile(id bson.ObjectID) ([]byte, error) {
	type chunkEntry struct {
		n    int
		data []byte
	}

	prefix := g.chunksPrefix(id)
	var chunks []chunkEntry

	err := g.eng.Scan(prefix, func(key, value []byte) bool {
		// The key is: prefix(12 bytes of files_id) + n(4 bytes BE)
		// We already know the prefix matches, so the last 4 bytes are n.
		if len(key) < 4 {
			return true
		}
		n := int(binary.BigEndian.Uint32(key[len(key)-4:]))

		// Decode chunk BSON document to extract data field
		chunkDoc, err := bson.Decode(value)
		if err != nil {
			return true
		}
		dataVal, ok := chunkDoc.Get("data")
		if !ok {
			return true
		}
		bin := dataVal.Binary()

		chunks = append(chunks, chunkEntry{n: n, data: bin.Data})
		return true
	})
	if err != nil {
		return nil, fmt.Errorf("gridfs: scan chunks: %w", err)
	}

	if len(chunks) == 0 {
		return nil, fmt.Errorf("gridfs: no chunks found for file %s", id)
	}

	// Sort by sequence number
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].n < chunks[j].n
	})

	// Concatenate
	totalSize := 0
	for _, c := range chunks {
		totalSize += len(c.data)
	}
	result := make([]byte, 0, totalSize)
	for _, c := range chunks {
		result = append(result, c.data...)
	}

	return result, nil
}

// DeleteFile removes all chunks and metadata for a file.
func (g *GridFS) DeleteFile(id bson.ObjectID) error {
	// Find the filename by scanning metadata
	filename, err := g.findFilenameByID(id)
	if err != nil {
		return err
	}

	// Delete all chunks
	if err := g.deleteChunks(id, -1); err != nil {
		return err
	}

	// Delete metadata
	metaKey := g.metaKey(filename)
	return g.eng.Delete(metaKey)
}

// ListFiles returns metadata for all stored files.
func (g *GridFS) ListFiles() ([]FileInfo, error) {
	prefix := g.metaPrefix()
	var files []FileInfo

	err := g.eng.Scan(prefix, func(key, value []byte) bool {
		metaDoc, err := bson.Decode(value)
		if err != nil {
			return true
		}

		fi := FileInfo{}

		if v, ok := metaDoc.Get("_id"); ok {
			fi.ID = v.ObjectID()
		}
		if v, ok := metaDoc.Get("filename"); ok {
			fi.Filename = v.String()
		}
		if v, ok := metaDoc.Get("length"); ok {
			fi.Length = v.Int64()
		}
		if v, ok := metaDoc.Get("chunkSize"); ok {
			fi.ChunkSize = int(v.Int32())
		}
		if v, ok := metaDoc.Get("uploadDate"); ok {
			fi.UploadDate = time.UnixMilli(v.DateTime())
		}
		if v, ok := metaDoc.Get("md5"); ok {
			fi.MD5 = v.String()
		}

		files = append(files, fi)
		return true
	})
	return files, err
}

// deleteChunks removes chunk entries for a file. If maxN < 0, all chunks are removed.
// If maxN >= 0, only chunks with n <= maxN are removed (for cleanup on partial upload).
// Keys are collected during the scan first, then deleted separately to avoid holding
// the engine's read lock while attempting writes.
func (g *GridFS) deleteChunks(id bson.ObjectID, maxN int) error {
	prefix := g.chunksPrefix(id)
	var keys [][]byte
	err := g.eng.Scan(prefix, func(key, _ []byte) bool {
		if maxN >= 0 && len(key) >= 4 {
			n := int(binary.BigEndian.Uint32(key[len(key)-4:]))
			if n > maxN {
				return true // skip, don't delete chunks beyond maxN
			}
		}
		keys = append(keys, append([]byte(nil), key...))
		return true
	})
	if err != nil {
		return err
	}
	for _, k := range keys {
		if err := g.eng.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

// findFilenameByID scans all metadata entries to find the filename for a given file ID.
func (g *GridFS) findFilenameByID(id bson.ObjectID) (string, error) {
	prefix := g.metaPrefix()
	var filename string
	var found bool

	err := g.eng.Scan(prefix, func(key, value []byte) bool {
		metaDoc, err := bson.Decode(value)
		if err != nil {
			return true
		}
		idVal, ok := metaDoc.Get("_id")
		if !ok {
			return true
		}
		if idVal.ObjectID() == id {
			nameVal, ok := metaDoc.Get("filename")
			if ok {
				filename = nameVal.String()
				found = true
				return false // stop scanning
			}
		}
		return true
	})
	if err != nil {
		return "", fmt.Errorf("gridfs: find filename: %w", err)
	}
	if !found {
		return "", fmt.Errorf("gridfs: file %s not found", id)
	}
	return filename, nil
}
