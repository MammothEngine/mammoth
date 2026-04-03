package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"os"

	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

type collStats struct {
	DB       string
	Coll     string
	Docs     int
	DataSize int64
	Indexes  int
}

func statsCmd(args []string) {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	dataDir := fs.String("data-dir", "./data", "data directory")
	dbName := fs.String("db", "", "database name (empty = all)")
	collName := fs.String("coll", "", "collection name (requires --db)")
	fs.Parse(args)

	if *collName != "" && *dbName == "" {
		fmt.Fprintln(os.Stderr, "Error: --coll requires --db")
		os.Exit(1)
	}

	opts := engine.DefaultOptions(*dataDir)
	eng, err := engine.Open(opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening engine: %v\n", err)
		os.Exit(1)
	}
	defer eng.Close()

	cat := mongo.NewCatalog(eng)

	// Engine-level stats
	engStats := eng.Stats()
	fmt.Println("ENGINE STATS")
	fmt.Println("============")
	fmt.Printf("  Memtables: %d (%s)\n", engStats.MemtableCount, formatSize(int64(engStats.MemtableSizeBytes)))
	fmt.Printf("  SSTables:  %d (%s)\n", engStats.SSTableCount, formatSize(int64(engStats.SSTableTotalBytes)))
	fmt.Printf("  Compactions: %d\n", engStats.CompactionCount)
	fmt.Printf("  Sequence: %d\n", engStats.SequenceNumber)
	fmt.Printf("  Operations: puts=%d gets=%d deletes=%d scans=%d\n",
		engStats.PutCount, engStats.GetCount, engStats.DeleteCount, engStats.ScanCount)
	fmt.Println()

	// Collection-level stats
	var collections []collStats
	indexCat := mongo.NewIndexCatalog(eng, cat)

	if *dbName != "" && *collName != "" {
		cs := getCollStats(eng, indexCat, *dbName, *collName)
		collections = append(collections, cs)
	} else if *dbName != "" {
		colls, err := cat.ListCollections(*dbName)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error listing collections: %v\n", err)
			os.Exit(1)
		}
		for _, c := range colls {
			cs := getCollStats(eng, indexCat, *dbName, c.Name)
			collections = append(collections, cs)
		}
	} else {
		dbs, err := cat.ListDatabases()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error listing databases: %v\n", err)
			os.Exit(1)
		}
		for _, db := range dbs {
			colls, err := cat.ListCollections(db.Name)
			if err != nil {
				continue
			}
			for _, c := range colls {
				cs := getCollStats(eng, indexCat, db.Name, c.Name)
				collections = append(collections, cs)
			}
		}
	}

	// Print table
	fmt.Println("DATABASE   COLLECTION       DOCS       SIZE      INDEXES")
	fmt.Println("--------   -----------      ----       ----      -------")
	for _, cs := range collections {
		fmt.Printf("%-10s %-16s %-10d %-9s %d\n",
			cs.DB, cs.Coll, cs.Docs, formatSize(cs.DataSize), cs.Indexes)
	}
}

func getCollStats(eng *engine.Engine, indexCat *mongo.IndexCatalog, dbName, collName string) collStats {
	prefix := mongo.EncodeNamespacePrefix(dbName, collName)
	cs := collStats{DB: dbName, Coll: collName}

	it := eng.NewPrefixIterator(prefix)
	defer it.Close()

	for it.Next() {
		cs.Docs++
		val := it.Value()
		if len(val) >= 4 {
			cs.DataSize += int64(binary.LittleEndian.Uint32(val[:4]))
		}
	}

	// Get index count
	indexes, _ := indexCat.ListIndexes(dbName, collName)
	cs.Indexes = len(indexes)

	return cs
}

func formatSize(bytes int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)
	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.1f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.1f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.1f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}
