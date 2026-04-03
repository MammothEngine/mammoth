package backup

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/mammothengine/mammoth/pkg/bson"
)

// Restore represents a database restore operation.
type Restore struct {
	catalog CatalogWriter
}

// CatalogWriter provides write access to database collections.
type CatalogWriter interface {
	CreateCollection(name string) (CollectionWriter, error)
	DropCollection(name string) error
	ListCollections() ([]string, error)
}

// CollectionWriter provides write access to a collection.
type CollectionWriter interface {
	InsertOne(doc *bson.Document) error
	CreateIndexes(indexes []IndexInfo) error
}

// IndexInfo represents index metadata.
type IndexInfo struct {
	Name   string
	Keys   map[string]interface{}
	Unique bool
}

// RestoreOptions configures restore behavior.
type RestoreOptions struct {
	DropBeforeRestore bool // Drop collections before restoring
	Upsert            bool // Update existing documents
}

// NewRestore creates a new Restore instance.
func NewRestore(catalog CatalogWriter) *Restore {
	return &Restore{catalog: catalog}
}

// RestoreFromDir restores a database from a backup directory.
func (r *Restore) RestoreFromDir(ctx context.Context, backupDir string, opts RestoreOptions) error {
	// Read metadata
	metadata, err := r.readMetadata(backupDir)
	if err != nil {
		return fmt.Errorf("read metadata: %w", err)
	}

	// Verify checksum
	if err := r.verifyChecksum(metadata); err != nil {
		return fmt.Errorf("checksum verification failed: %w", err)
	}

	// Handle incremental backup
	if metadata.Incremental {
		return r.restoreIncremental(ctx, metadata, backupDir, opts)
	}

	// Restore full backup
	return r.restoreFull(ctx, metadata, backupDir, opts)
}

// RestoreCollection restores a single collection from a backup file.
func (r *Restore) RestoreCollection(ctx context.Context, collName string, backupFile string, format Format, compressed bool) error {
	// Open backup file
	file, err := os.Open(backupFile)
	if err != nil {
		return fmt.Errorf("open backup file: %w", err)
	}
	defer file.Close()

	var reader io.Reader = file
	if compressed {
		gr, err := gzip.NewReader(file)
		if err != nil {
			return fmt.Errorf("create gzip reader: %w", err)
		}
		defer gr.Close()
		reader = gr
	}

	// Create collection
	coll, err := r.catalog.CreateCollection(collName)
	if err != nil {
		return fmt.Errorf("create collection: %w", err)
	}

	// Import documents
	switch format {
	case FormatBSON:
		return r.importBSON(reader, coll)
	case FormatJSON:
		return r.importJSON(reader, coll)
	default:
		return fmt.Errorf("unsupported format: %s", format)
	}
}

func (r *Restore) restoreFull(ctx context.Context, metadata *Metadata, backupDir string, opts RestoreOptions) error {
	for _, collMeta := range metadata.Collections {
		// Drop collection if requested
		if opts.DropBeforeRestore {
			if err := r.catalog.DropCollection(collMeta.Name); err != nil {
				return fmt.Errorf("drop collection %s: %w", collMeta.Name, err)
			}
		}

		// Determine backup filename
		filename := collMeta.Name
		switch metadata.Format {
		case FormatBSON:
			filename += ".bson"
		case FormatJSON:
			filename += ".json"
		}
		if metadata.Compressed {
			filename += ".gz"
		}

		backupFile := filepath.Join(backupDir, filename)
		if err := r.RestoreCollection(ctx, collMeta.Name, backupFile, metadata.Format, metadata.Compressed); err != nil {
			return fmt.Errorf("restore collection %s: %w", collMeta.Name, err)
		}
	}

	return nil
}

func (r *Restore) restoreIncremental(ctx context.Context, metadata *Metadata, backupDir string, opts RestoreOptions) error {
	// Read and apply oplog entries
	filename := "oplog.bson"
	if metadata.Compressed {
		filename += ".gz"
	}

	oplogFile := filepath.Join(backupDir, filename)
	file, err := os.Open(oplogFile)
	if err != nil {
		return fmt.Errorf("open oplog file: %w", err)
	}
	defer file.Close()

	var reader io.Reader = file
	if metadata.Compressed {
		gr, err := gzip.NewReader(file)
		if err != nil {
			return fmt.Errorf("create gzip reader: %w", err)
		}
		defer gr.Close()
		reader = gr
	}

	return r.applyOplog(reader)
}

func (r *Restore) importBSON(reader io.Reader, coll CollectionWriter) error {
	br := bufio.NewReader(reader)

	for {
		// Read length prefix
		lengthBuf := make([]byte, 4)
		if _, err := io.ReadFull(br, lengthBuf); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("read length prefix: %w", err)
		}

		length := int32(lengthBuf[0]) |
			int32(lengthBuf[1])<<8 |
			int32(lengthBuf[2])<<16 |
			int32(lengthBuf[3])<<24

		if length < 5 {
			return fmt.Errorf("invalid document length: %d", length)
		}

		// Read document data
		data := make([]byte, length)
		copy(data, lengthBuf)
		if _, err := io.ReadFull(br, data[4:]); err != nil {
			return fmt.Errorf("read document data: %w", err)
		}

		// Parse BSON document
		doc, err := bson.Decode(data)
		if err != nil {
			return fmt.Errorf("decode document: %w", err)
		}

		// Insert document
		if err := coll.InsertOne(doc); err != nil {
			return fmt.Errorf("insert document: %w", err)
		}
	}

	return nil
}

func (r *Restore) importJSON(reader io.Reader, coll CollectionWriter) error {
	decoder := json.NewDecoder(reader)

	// Expect array start
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("read JSON start: %w", err)
	}
	if token != json.Delim('[') {
		return fmt.Errorf("expected JSON array, got %v", token)
	}

	// Read documents
	for decoder.More() {
		var m map[string]interface{}
		if err := decoder.Decode(&m); err != nil {
			return fmt.Errorf("decode document: %w", err)
		}

		doc := mapToBSONDocument(m)
		if err := coll.InsertOne(doc); err != nil {
			return fmt.Errorf("insert document: %w", err)
		}
	}

	// Expect array end
	token, err = decoder.Token()
	if err != nil {
		return fmt.Errorf("read JSON end: %w", err)
	}
	if token != json.Delim(']') {
		return fmt.Errorf("expected array end, got %v", token)
	}

	return nil
}

func (r *Restore) applyOplog(reader io.Reader) error {
	br := bufio.NewReader(reader)

	for {
		// Read length prefix
		lengthBuf := make([]byte, 4)
		if _, err := io.ReadFull(br, lengthBuf); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("read length prefix: %w", err)
		}

		length := int32(lengthBuf[0]) |
			int32(lengthBuf[1])<<8 |
			int32(lengthBuf[2])<<16 |
			int32(lengthBuf[3])<<24

		if length < 5 {
			return fmt.Errorf("invalid document length: %d", length)
		}

		// Read document data
		data := make([]byte, length)
		copy(data, lengthBuf)
		if _, err := io.ReadFull(br, data[4:]); err != nil {
			return fmt.Errorf("read oplog entry: %w", err)
		}

		// Parse oplog entry
		doc, err := bson.Decode(data)
		if err != nil {
			return fmt.Errorf("decode oplog entry: %w", err)
		}

		// Extract operation details
		opVal, _ := doc.Get("op")
		operation := opVal.String()

		nsVal, _ := doc.Get("ns")
		namespace := nsVal.String()

		// Extract collection name from namespace
		collName := extractCollectionName(namespace)
		if collName == "" {
			continue
		}

		// Get or create collection
		coll, err := r.catalog.CreateCollection(collName)
		if err != nil {
			return fmt.Errorf("create collection %s: %w", collName, err)
		}

		// Apply operation
		switch operation {
		case "i": // Insert
			oVal, _ := doc.Get("o")
			if oDoc := oVal.DocumentValue(); oDoc != nil {
				if err := coll.InsertOne(oDoc); err != nil {
					return fmt.Errorf("apply insert: %w", err)
				}
			}
		case "u": // Update
			// For updates, we need to apply the update to an existing document
			// This is a simplified implementation - full oplog replay would need more logic
			oVal, _ := doc.Get("o")
			if oDoc := oVal.DocumentValue(); oDoc != nil {
				// Try to insert or replace
				_ = coll.InsertOne(oDoc)
			}
		case "d": // Delete
			// Deletes require finding and removing documents
			// Simplified: skip for now
		}
	}

	return nil
}

func (r *Restore) readMetadata(backupDir string) (*Metadata, error) {
	filepath := filepath.Join(backupDir, "metadata.json")
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("open metadata file: %w", err)
	}
	defer file.Close()

	var metadata Metadata
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&metadata); err != nil {
		return nil, fmt.Errorf("decode metadata: %w", err)
	}

	return &metadata, nil
}

func (r *Restore) verifyChecksum(metadata *Metadata) error {
	// In production, this would verify the actual file checksums
	// For now, we just check the metadata checksum structure
	if metadata.Checksum == "" {
		return fmt.Errorf("missing checksum")
	}

	// Verify collection checksums match
	for _, coll := range metadata.Collections {
		if coll.Checksum == "" {
			return fmt.Errorf("missing checksum for collection %s", coll.Name)
		}
	}

	return nil
}

// Helper functions

func extractCollectionName(namespace string) string {
	// Namespace format: "database.collection"
	// Find the first dot
	for i, c := range namespace {
		if c == '.' {
			if i+1 < len(namespace) {
				return namespace[i+1:]
			}
			return ""
		}
	}
	return namespace
}

func mapToBSONDocument(m map[string]interface{}) *bson.Document {
	doc := bson.NewDocument()
	for k, v := range m {
		doc.Set(k, interfaceToBSONValue(v))
	}
	return doc
}

// Additional helper for restore
func hexToObjectID(hex string) (bson.ObjectID, error) {
	if len(hex) != 24 {
		return bson.ObjectID{}, fmt.Errorf("invalid ObjectID length")
	}
	return bson.ParseObjectID(hex)
}

// IsValidBackupDir checks if a directory contains a valid backup.
func IsValidBackupDir(backupDir string) bool {
	metadataFile := filepath.Join(backupDir, "metadata.json")
	_, err := os.Stat(metadataFile)
	return err == nil
}

// ListBackups returns a list of available backups in a directory.
func ListBackups(baseDir string) ([]string, error) {
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return nil, fmt.Errorf("read directory: %w", err)
	}

	var backups []string
	for _, entry := range entries {
		if entry.IsDir() {
			backupDir := filepath.Join(baseDir, entry.Name())
			if IsValidBackupDir(backupDir) {
				backups = append(backups, entry.Name())
			}
		}
	}

	return backups, nil
}
