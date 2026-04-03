// Package backup provides backup and restore functionality for Mammoth Engine.
package backup

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// Format represents the backup format type.
type Format string

const (
	// FormatBSON uses BSON binary format (MongoDB-compatible).
	FormatBSON Format = "bson"
	// FormatJSON uses JSON format (human-readable).
	FormatJSON Format = "json"
)

// Backup represents a database backup operation.
type Backup struct {
	catalog    Catalog
	format     Format
	compress   bool
	oplogStore OplogStore
}

// Catalog provides access to database collections.
type Catalog interface {
	ListCollections() ([]string, error)
	GetCollection(name string) (Collection, error)
}

// Collection represents a database collection for backup.
type Collection interface {
	Name() string
	FindAll(ctx context.Context) (DocumentIterator, error)
	Count() (int64, error)
}

// DocumentIterator iterates over collection documents.
type DocumentIterator interface {
	Next() (*bson.Document, error)
	Close() error
}

// OplogStore provides access to the oplog for incremental backups.
type OplogStore interface {
	GetTimestamp() (int64, error)
	FindSince(ctx context.Context, timestamp int64) (OplogIterator, error)
}

// OplogIterator iterates over oplog entries.
type OplogIterator interface {
	Next() (*OplogEntry, error)
	Close() error
}

// OplogEntry represents an oplog operation.
type OplogEntry struct {
	Timestamp   int64                  `bson:"ts" json:"ts"`
	Operation   string                 `bson:"op" json:"op"`
	Namespace   string                 `bson:"ns" json:"ns"`
	DocumentID  interface{}            `bson:"o2,omitempty" json:"o2,omitempty"`
	Document    map[string]interface{} `bson:"o,omitempty" json:"o,omitempty"`
}

// Metadata contains backup metadata.
type Metadata struct {
	Version       string            `json:"version"`
	CreatedAt     time.Time         `json:"created_at"`
	Format        Format            `json:"format"`
	Compressed    bool              `json:"compressed"`
	Database      string            `json:"database"`
	Collections   []CollectionMeta  `json:"collections"`
	OplogStart    int64             `json:"oplog_start,omitempty"`
	OplogEnd      int64             `json:"oplog_end,omitempty"`
	Incremental   bool              `json:"incremental"`
	Checksum      string            `json:"checksum"`
}

// CollectionMeta contains metadata for a backed-up collection.
type CollectionMeta struct {
	Name      string `json:"name"`
	DocumentCount int64  `json:"document_count"`
	Size      int64  `json:"size"`
	Checksum  string `json:"checksum"`
}

// Options configures backup behavior.
type Options struct {
	Format      Format
	Compress    bool
	Incremental bool
	Collections []string // Empty = all collections
	OutputDir   string
}

// New creates a new Backup instance.
func New(catalog Catalog, oplog OplogStore, opts Options) *Backup {
	if opts.Format == "" {
		opts.Format = FormatBSON
	}
	return &Backup{
		catalog:    catalog,
		format:     opts.Format,
		compress:   opts.Compress,
		oplogStore: oplog,
	}
}

// Create performs a full backup of the database.
func (b *Backup) Create(ctx context.Context, database string) (*Metadata, error) {
	// Create backup directory
	timestamp := time.Now().UTC().Format("20060102_150405")
	backupDir := filepath.Join(database, timestamp)
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return nil, fmt.Errorf("create backup directory: %w", err)
	}

	metadata := &Metadata{
		Version:     "1.0",
		CreatedAt:   time.Now().UTC(),
		Format:      b.format,
		Compressed:  b.compress,
		Database:    database,
		Incremental: false,
	}

	// Get collections to backup
	collections, err := b.catalog.ListCollections()
	if err != nil {
		return nil, fmt.Errorf("list collections: %w", err)
	}

	// Backup each collection
	for _, collName := range collections {
		coll, err := b.catalog.GetCollection(collName)
		if err != nil {
			return nil, fmt.Errorf("get collection %s: %w", collName, err)
		}

		collMeta, err := b.backupCollection(ctx, coll, backupDir)
		if err != nil {
			return nil, fmt.Errorf("backup collection %s: %w", collName, err)
		}

		metadata.Collections = append(metadata.Collections, *collMeta)
	}

	// Calculate overall checksum
	metadata.Checksum = b.calculateMetadataChecksum(metadata)

	// Write metadata file
	if err := b.writeMetadata(metadata, backupDir); err != nil {
		return nil, fmt.Errorf("write metadata: %w", err)
	}

	return metadata, nil
}

// CreateIncremental performs an incremental backup using oplog.
func (b *Backup) CreateIncremental(ctx context.Context, database string, since int64) (*Metadata, error) {
	// Create backup directory
	timestamp := time.Now().UTC().Format("20060102_150405")
	backupDir := filepath.Join(database, "incremental", timestamp)
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return nil, fmt.Errorf("create backup directory: %w", err)
	}

	// Get current oplog timestamp
	endTs, err := b.oplogStore.GetTimestamp()
	if err != nil {
		return nil, fmt.Errorf("get oplog timestamp: %w", err)
	}

	metadata := &Metadata{
		Version:     "1.0",
		CreatedAt:   time.Now().UTC(),
		Format:      b.format,
		Compressed:  b.compress,
		Database:    database,
		Incremental: true,
		OplogStart:  since,
		OplogEnd:    endTs,
	}

	// Export oplog entries
	if err := b.backupOplog(ctx, since, endTs, backupDir); err != nil {
		return nil, fmt.Errorf("backup oplog: %w", err)
	}

	// Calculate checksum
	metadata.Checksum = b.calculateMetadataChecksum(metadata)

	// Write metadata
	if err := b.writeMetadata(metadata, backupDir); err != nil {
		return nil, fmt.Errorf("write metadata: %w", err)
	}

	return metadata, nil
}

func (b *Backup) backupCollection(ctx context.Context, coll Collection, backupDir string) (*CollectionMeta, error) {
	filename := coll.Name()
	switch b.format {
	case FormatBSON:
		filename += ".bson"
	case FormatJSON:
		filename += ".json"
	}
	if b.compress {
		filename += ".gz"
	}

	filepath := filepath.Join(backupDir, filename)
	file, err := os.Create(filepath)
	if err != nil {
		return nil, fmt.Errorf("create backup file: %w", err)
	}
	defer file.Close()

	var writer io.Writer = file
	if b.compress {
		gw := gzip.NewWriter(file)
		defer gw.Close()
		writer = gw
	}

	hasher := md5.New()
	writer = io.MultiWriter(writer, hasher)

	// Get document count
	count, err := coll.Count()
	if err != nil {
		return nil, fmt.Errorf("count documents: %w", err)
	}

	// Export documents
	docSize, err := b.exportDocuments(ctx, coll, writer)
	if err != nil {
		return nil, fmt.Errorf("export documents: %w", err)
	}

	return &CollectionMeta{
		Name:          coll.Name(),
		DocumentCount: count,
		Size:          docSize,
		Checksum:      hex.EncodeToString(hasher.Sum(nil)),
	}, nil
}

func (b *Backup) exportDocuments(ctx context.Context, coll Collection, w io.Writer) (int64, error) {
	iterator, err := coll.FindAll(ctx)
	if err != nil {
		return 0, err
	}
	defer iterator.Close()

	var totalSize int64

	switch b.format {
	case FormatBSON:
		return b.exportBSON(iterator, w)
	case FormatJSON:
		return b.exportJSON(iterator, w)
	default:
		return 0, fmt.Errorf("unsupported format: %s", b.format)
	}

	return totalSize, nil
}

func (b *Backup) exportBSON(iterator DocumentIterator, w io.Writer) (int64, error) {
	var totalSize int64
	bw := bufio.NewWriter(w)
	defer bw.Flush()

	for {
		doc, err := iterator.Next()
		if err != nil {
			return totalSize, err
		}
		if doc == nil {
			break
		}

		// Encode BSON document
		data := bson.Encode(doc)

		// Write document
		if _, err := bw.Write(data); err != nil {
			return totalSize, err
		}

		totalSize += int64(len(data))
	}

	return totalSize, nil
}

func (b *Backup) exportJSON(iterator DocumentIterator, w io.Writer) (int64, error) {
	var totalSize int64
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")

	// Write array start
	if _, err := w.Write([]byte("[\n")); err != nil {
		return 0, err
	}

	first := true
	for {
		doc, err := iterator.Next()
		if err != nil {
			return totalSize, err
		}
		if doc == nil {
			break
		}

		if !first {
			if _, err := w.Write([]byte(",\n")); err != nil {
				return totalSize, err
			}
		}
		first = false

		// Convert BSON to map for JSON encoding
		m := bsonDocumentToMap(doc)
		if err := encoder.Encode(m); err != nil {
			return totalSize, fmt.Errorf("encode document: %w", err)
		}
	}

	// Write array end
	if _, err := w.Write([]byte("\n]")); err != nil {
		return totalSize, err
	}

	return totalSize, nil
}

func (b *Backup) backupOplog(ctx context.Context, startTs, endTs int64, backupDir string) error {
	filename := "oplog.bson"
	if b.compress {
		filename += ".gz"
	}

	filepath := filepath.Join(backupDir, filename)
	file, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("create oplog file: %w", err)
	}
	defer file.Close()

	var writer io.Writer = file
	if b.compress {
		gw := gzip.NewWriter(file)
		defer gw.Close()
		writer = gw
	}

	iterator, err := b.oplogStore.FindSince(ctx, startTs)
	if err != nil {
		return fmt.Errorf("find oplog entries: %w", err)
	}
	defer iterator.Close()

	bw := bufio.NewWriter(writer)
	defer bw.Flush()

	for {
		entry, err := iterator.Next()
		if err != nil {
			return err
		}
		if entry == nil {
			break
		}

		// Convert to BSON
		doc := bson.NewDocument()
		doc.Set("ts", bson.VInt64(entry.Timestamp))
		doc.Set("op", bson.VString(entry.Operation))
		doc.Set("ns", bson.VString(entry.Namespace))
		if entry.DocumentID != nil {
			doc.Set("o2", interfaceToBSONValue(entry.DocumentID))
		}
		if entry.Document != nil {
			doc.Set("o", interfaceToBSONValue(entry.Document))
		}

		data := bson.Encode(doc)

		if _, err := bw.Write(data); err != nil {
			return err
		}
	}

	return nil
}

func (b *Backup) writeMetadata(metadata *Metadata, backupDir string) error {
	filepath := filepath.Join(backupDir, "metadata.json")
	file, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(metadata)
}

func (b *Backup) calculateMetadataChecksum(m *Metadata) string {
	h := md5.New()
	data, _ := json.Marshal(m)
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}

// Helper functions

func writeInt32(w io.Writer, v int32) error {
	buf := make([]byte, 4)
	buf[0] = byte(v)
	buf[1] = byte(v >> 8)
	buf[2] = byte(v >> 16)
	buf[3] = byte(v >> 24)
	_, err := w.Write(buf)
	return err
}

func bsonDocumentToMap(doc *bson.Document) map[string]interface{} {
	result := make(map[string]interface{})
	for _, elem := range doc.Elements() {
		result[elem.Key] = bsonValueToInterface(elem.Value)
	}
	return result
}

func bsonValueToInterface(v bson.Value) interface{} {
	switch v.Type {
	case bson.TypeNull:
		return nil
	case bson.TypeBoolean:
		return v.Boolean()
	case bson.TypeInt32:
		return v.Int32()
	case bson.TypeInt64:
		return v.Int64()
	case bson.TypeDouble:
		return v.Double()
	case bson.TypeString:
		return v.String()
	case bson.TypeObjectID:
		return v.ObjectID().String()
	case bson.TypeArray:
		arr := v.ArrayValue()
		result := make([]interface{}, len(arr))
		for i, elem := range arr {
			result[i] = bsonValueToInterface(elem)
		}
		return result
	case bson.TypeDocument:
		// Nested document
		doc := v.DocumentValue()
		if doc == nil {
			return nil
		}
		return bsonDocumentToMap(doc)
	default:
		return v.Interface()
	}
}

func interfaceToBSONValue(v interface{}) bson.Value {
	switch val := v.(type) {
	case nil:
		return bson.VNull()
	case bool:
		return bson.VBool(val)
	case int:
		return bson.VInt32(int32(val))
	case int32:
		return bson.VInt32(val)
	case int64:
		return bson.VInt64(val)
	case float64:
		return bson.VDouble(val)
	case string:
		return bson.VString(val)
	case map[string]interface{}:
		doc := bson.NewDocument()
		for k, v := range val {
			doc.Set(k, interfaceToBSONValue(v))
		}
		return bson.VDoc(doc)
	case []interface{}:
		arr := make(bson.Array, len(val))
		for i, elem := range val {
			arr[i] = interfaceToBSONValue(elem)
		}
		return bson.VArray(arr)
	default:
		return bson.VNull()
	}
}
