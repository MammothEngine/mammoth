package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

func backupCmd(args []string) {
	fs := flag.NewFlagSet("backup", flag.ExitOnError)
	dataDir := fs.String("data-dir", "./data", "data directory")
	outDir := fs.String("dir", "", "output directory for backup files")
	fs.Parse(args)

	if *outDir == "" {
		*outDir = fmt.Sprintf("backup_%s", time.Now().Format("20060102_150405"))
	}

	// Open engine
	opts := engine.DefaultOptions(*dataDir)
	eng, err := engine.Open(opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening engine: %v\n", err)
		os.Exit(1)
	}
	defer eng.Close()

	cat := mongo.NewCatalog(eng)

	// Create output directory
	if err := os.MkdirAll(*outDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating backup dir: %v\n", err)
		os.Exit(1)
	}

	dbs, err := cat.ListDatabases()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error listing databases: %v\n", err)
		os.Exit(1)
	}

	totalDocs := 0
	totalColls := 0

	for _, db := range dbs {
		colls, err := cat.ListCollections(db.Name)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error listing collections in %s: %v\n", db.Name, err)
			continue
		}

		for _, coll := range colls {
			count, err := backupCollection(eng, db.Name, coll.Name, *outDir)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error backing up %s.%s: %v\n", db.Name, coll.Name, err)
				continue
			}
			fmt.Printf("  %s.%s: %d documents\n", db.Name, coll.Name, count)
			totalDocs += count
			totalColls++
		}
	}

	// Write metadata
	meta := fmt.Sprintf("version=%s\ntimestamp=%s\ndatabases=%d\ncollections=%d\ndocuments=%d\n",
		version, time.Now().UTC().Format(time.RFC3339), len(dbs), totalColls, totalDocs)
	if err := os.WriteFile(filepath.Join(*outDir, "BACKUP_META"), []byte(meta), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing metadata: %v\n", err)
	}

	fmt.Printf("Backup complete: %d databases, %d collections, %d documents -> %s\n",
		len(dbs), totalColls, totalDocs, *outDir)
}

func backupCollection(eng *engine.Engine, dbName, collName, outDir string) (int, error) {
	prefix := mongo.EncodeNamespacePrefix(dbName, collName)

	f, err := os.Create(filepath.Join(outDir, fmt.Sprintf("%s.%s.bak", dbName, collName)))
	if err != nil {
		return 0, err
	}
	defer f.Close()

	count := 0
	it := eng.NewPrefixIterator(prefix)
	defer it.Close()

	for it.Next() {
		bsonBytes := it.Value()

		// Write [length:4][bson_bytes]
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(bsonBytes)))
		if _, err := f.Write(lenBuf[:]); err != nil {
			return count, err
		}
		if _, err := f.Write(bsonBytes); err != nil {
			return count, err
		}
		count++
	}
	if it.Err() != nil {
		return count, it.Err()
	}

	// Write count metadata
	countFile := filepath.Join(outDir, fmt.Sprintf("%s.%s.count", dbName, collName))
	_ = os.WriteFile(countFile, []byte(fmt.Sprintf("%d", count)), 0644)

	return count, nil
}
