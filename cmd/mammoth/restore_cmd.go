package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

func restoreCmd(args []string) {
	fs := flag.NewFlagSet("restore", flag.ExitOnError)
	dataDir := fs.String("data-dir", "./data", "data directory")
	backupDir := fs.String("dir", "", "backup directory to restore from")
	fs.Parse(args)

	if *backupDir == "" {
		fmt.Fprintln(os.Stderr, "Error: --dir is required")
		fs.Usage()
		os.Exit(1)
	}

	// Open engine for writes
	opts := engine.DefaultOptions(*dataDir)
	eng, err := engine.Open(opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening engine: %v\n", err)
		os.Exit(1)
	}
	defer eng.Close()

	cat := mongo.NewCatalog(eng)

	// Find all .bak files
	entries, err := os.ReadDir(*backupDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading backup dir: %v\n", err)
		os.Exit(1)
	}

	totalDocs := 0
	totalColls := 0

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".bak") {
			continue
		}

		// Parse "db.collection.bak"
		name := strings.TrimSuffix(entry.Name(), ".bak")
		parts := strings.SplitN(name, ".", 2)
		if len(parts) != 2 {
			fmt.Fprintf(os.Stderr, "Skipping malformed backup file: %s\n", entry.Name())
			continue
		}
		dbName, collName := parts[0], parts[1]

		count, err := restoreCollection(eng, cat, dbName, collName, filepath.Join(*backupDir, entry.Name()))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error restoring %s.%s: %v\n", dbName, collName, err)
			continue
		}

		fmt.Printf("  %s.%s: %d documents restored\n", dbName, collName, count)
		totalDocs += count
		totalColls++
	}

	fmt.Printf("Restore complete: %d collections, %d documents from %s\n",
		totalColls, totalDocs, *backupDir)
}

func restoreCollection(eng *engine.Engine, cat *mongo.Catalog, dbName, collName, filePath string) (int, error) {
	// Ensure namespace exists
	if err := cat.EnsureDatabase(dbName); err != nil {
		return 0, fmt.Errorf("create db: %w", err)
	}
	if err := cat.EnsureCollection(dbName, collName); err != nil {
		return 0, fmt.Errorf("create collection: %w", err)
	}

	f, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	count := 0
	batch := eng.NewBatch()
	batchSize := 0

	for {
		// Read length prefix
		var lenBuf [4]byte
		if _, err := io.ReadFull(f, lenBuf[:]); err != nil {
			if err == io.EOF {
				break
			}
			return count, err
		}

		docLen := binary.LittleEndian.Uint32(lenBuf[:])
		if docLen == 0 || docLen > 16*1024*1024 { // sanity: max 16MB
			return count, fmt.Errorf("invalid document size: %d", docLen)
		}

		doc := make([]byte, docLen)
		if _, err := io.ReadFull(f, doc); err != nil {
			return count, fmt.Errorf("reading document: %w", err)
		}

		// Generate key from BSON doc's _id field
		key, err := extractDocKey(dbName, collName, doc)
		if err != nil {
			return count, fmt.Errorf("extracting key: %w", err)
		}

		batch.Put(key, doc)
		batchSize++
		count++

		// Flush batch every 1000 docs
		if batchSize >= 1000 {
			if err := batch.Commit(); err != nil {
				return count, err
			}
			batch = eng.NewBatch()
			batchSize = 0
		}
	}

	// Flush remaining
	if batchSize > 0 {
		if err := batch.Commit(); err != nil {
			return count, err
		}
	}

	return count, nil
}

func extractDocKey(dbName, collName string, bsonDoc []byte) ([]byte, error) {
	// Extract _id from BSON document
	// BSON format: [size:4][type:1][name\0][value]...[0x00]
	if len(bsonDoc) < 5 {
		return nil, fmt.Errorf("document too short")
	}

	var id []byte
	pos := 4 // skip doc size
	for pos < len(bsonDoc)-1 {
		btype := bsonDoc[pos]
		pos++
		if btype == 0 {
			break // end marker
		}

		// Read field name
		nameStart := pos
		for pos < len(bsonDoc) && bsonDoc[pos] != 0 {
			pos++
		}
		fieldName := string(bsonDoc[nameStart:pos])
		pos++ // skip null terminator

		if fieldName != "_id" {
			// Skip this field's value
			next := skipBSONValue(bsonDoc, pos, btype)
			if next < 0 {
				break // malformed, stop parsing
			}
			pos = next
			continue
		}

		// Found _id — extract based on type
		switch btype {
		case 0x07: // ObjectId
			if pos+12 <= len(bsonDoc) {
				id = make([]byte, 12)
				copy(id, bsonDoc[pos:pos+12])
			}
		case 0x02: // String
			if pos+4 <= len(bsonDoc) {
				strLen := int(binary.LittleEndian.Uint32(bsonDoc[pos : pos+4]))
				id = bsonDoc[pos : pos+4+strLen] // include length + string bytes
			}
		case 0x10: // Int32
			if pos+4 <= len(bsonDoc) {
				id = bsonDoc[pos : pos+4]
			}
		case 0x12: // Int64
			if pos+8 <= len(bsonDoc) {
				id = bsonDoc[pos : pos+8]
			}
		default:
			// Fallback: use raw bytes as id
			id = bsonDoc[pos : pos+12]
		}
		break
	}

	if id == nil {
		// Generate synthetic key from position info
		id = fmt.Appendf(nil, "%s.%s.%d", dbName, collName, len(bsonDoc))
	}

	return mongo.EncodeDocumentKey(dbName, collName, id), nil
}
