// Package mammoth provides an embedded document database for Go applications.
//
// Mammoth Engine is a MongoDB-compatible document store built on a custom
// LSM-tree storage engine. This package exposes a clean Go API for
// applications that want to embed a document database without running
// a separate server process.
//
// # Quick Start
//
// Open a database, create a collection, and insert a document:
//
//	db, err := mammoth.Open("/data/mydb")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer db.Close()
//
//	coll, err := db.Collection("users")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	id, err := coll.InsertOne(map[string]interface{}{
//	    "name":  "Alice",
//	    "email": "alice@example.com",
//	    "age":   30,
//	})
//
// # Querying
//
// Documents are queried using filter maps that support MongoDB-style
// query operators:
//
//	doc, err := coll.FindOne(map[string]interface{}{
//	    "name": "Alice",
//	})
//
//	cursor, err := coll.Find(map[string]interface{}{
//	    "age": map[string]interface{}{"$gt": 25},
//	})
//	for cursor.Next() {
//	    var result map[string]interface{}
//	    cursor.Decode(&result)
//	}
//	cursor.Close()
//
// # Configuration
//
// Use OpenWithOptions for fine-grained control over engine parameters:
//
//	db, err := mammoth.OpenWithOptions(mammoth.Options{
//	    DataDir:      "/data/mydb",
//	    MemtableSize: 8 * 1024 * 1024, // 8 MB
//	    CacheSize:    2000,
//	    LogLevel:     "info",
//	})
package mammoth
