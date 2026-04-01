package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"os"

	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

type validateResult struct {
	DB      string
	Coll    string
	Scanned int
	Valid   int
	Errors  int
}

func validateCmd(args []string) {
	fs := flag.NewFlagSet("validate", flag.ExitOnError)
	dataDir := fs.String("data-dir", "./data", "data directory")
	dbName := fs.String("db", "", "database name (empty = all)")
	collName := fs.String("collection", "", "collection name (requires --db)")
	fs.Parse(args)

	if *collName != "" && *dbName == "" {
		fmt.Fprintln(os.Stderr, "Error: --collection requires --db")
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

	var results []validateResult

	if *dbName != "" && *collName != "" {
		// Validate specific collection
		r := validateCollection(eng, *dbName, *collName)
		results = append(results, r)
	} else if *dbName != "" {
		// Validate all collections in a database
		colls, err := cat.ListCollections(*dbName)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error listing collections: %v\n", err)
			os.Exit(1)
		}
		for _, c := range colls {
			r := validateCollection(eng, *dbName, c.Name)
			results = append(results, r)
		}
	} else {
		// Validate all databases
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
				r := validateCollection(eng, db.Name, c.Name)
				results = append(results, r)
			}
		}
	}

	// Print results
	fmt.Println("\nVALIDATION REPORT")
	fmt.Println("=================")
	totalScanned := 0
	totalValid := 0
	totalErrors := 0
	for _, r := range results {
		fmt.Printf("%s.%s: scanned=%d, valid=%d, errors=%d\n",
			r.DB, r.Coll, r.Scanned, r.Valid, r.Errors)
		totalScanned += r.Scanned
		totalValid += r.Valid
		totalErrors += r.Errors
	}
	fmt.Println("---")
	fmt.Printf("TOTAL: scanned=%d, valid=%d, errors=%d\n", totalScanned, totalValid, totalErrors)
	if totalErrors > 0 {
		os.Exit(1)
	}
}

func validateCollection(eng *engine.Engine, dbName, collName string) validateResult {
	prefix := mongo.EncodeNamespacePrefix(dbName, collName)
	r := validateResult{DB: dbName, Coll: collName}

	it := eng.NewPrefixIterator(prefix)
	defer it.Close()

	for it.Next() {
		r.Scanned++
		val := it.Value()

		// Check BSON document validity
		if len(val) < 5 {
			r.Errors++
			continue
		}

		// Check declared size matches actual data
		declaredSize := int(binary.LittleEndian.Uint32(val[:4]))
		if declaredSize != len(val) {
			r.Errors++
			continue
		}

		// Try to walk the document structure
		if isValidBSON(val) {
			r.Valid++
		} else {
			r.Errors++
		}
	}
	if it.Err() != nil {
		r.Errors++
	}

	return r
}

func isValidBSON(doc []byte) bool {
	if len(doc) < 5 {
		return false
	}

	// Verify declared size
	size := int(binary.LittleEndian.Uint32(doc[:4]))
	if size != len(doc) {
		return false
	}

	// Walk fields
	pos := 4
	for pos < len(doc)-1 {
		btype := doc[pos]
		pos++
		if btype == 0 {
			break
		}

		// Read field name
		for pos < len(doc) && doc[pos] != 0 {
			pos++
		}
		if pos >= len(doc) {
			return false
		}
		pos++ // skip null

		// Skip value based on type
		pos = skipBSONValue(doc, pos, btype)
		if pos < 0 {
			return false
		}
	}

	return pos <= len(doc)
}
