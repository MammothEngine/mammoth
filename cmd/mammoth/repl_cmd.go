package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

var (
	currentDB   = "test"
	replEngine  *engine.Engine
	replCatalog *mongo.Catalog
)

func replCmd(args []string) {
	dir := "./data"
	if len(args) > 0 {
		dir = args[0]
	}

	// Open engine
	opts := engine.DefaultOptions(dir)
	eng, err := engine.Open(opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open engine: %v\n", err)
		os.Exit(1)
	}
	defer eng.Close()

	replEngine = eng
	replCatalog = mongo.NewCatalog(eng)

	fmt.Printf("Mammoth Engine v%s REPL\n", version)
	fmt.Println("Type 'help' for commands, 'exit' to quit.")

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 0), 1024*1024) // 1MB buffer

	for {
		fmt.Printf("%s> ", currentDB)
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if line == "exit" || line == "quit" {
			break
		}
		executeREPL(line)
	}
}

func executeREPL(line string) {
	// Parse the command
	tokens := tokenizeREPL(line)
	if len(tokens) == 0 {
		return
	}

	switch strings.ToLower(tokens[0]) {
	case "help":
		printHelp()
	case "use":
		cmdUse(tokens)
	case "show":
		cmdShow(tokens)
	case "db":
		cmdDB(tokens)
	case "exit", "quit":
		fmt.Println("Bye!")
	default:
		// Try to parse as db.collection.operation(...)
		cmdDatabaseOperation(tokens)
	}
}

func printHelp() {
	fmt.Println("Commands:")
	fmt.Println("  use <db>                 - Switch database")
	fmt.Println("  show dbs               - List databases")
	fmt.Println("  show collections        - List collections in current database")
	fmt.Println("  db.<coll>.insert({doc})  - Insert document")
	fmt.Println("  db.<coll>.find({query}) - Find documents")
	fmt.Println("  db.<coll>.update({q}, {u}) - Update documents")
	fmt.Println("  db.<coll>.delete({query}) - Delete documents")
	fmt.Println("  db.<coll>.count()         - Count documents")
	fmt.Println("  db.<coll>.drop()         - Drop collection")
	fmt.Println("  help                    - Show this help")
	fmt.Println("  exit                    - Exit REPL")
}

func cmdUse(tokens []string) {
	if len(tokens) < 2 {
		fmt.Println("Usage: use <database>")
		return
	}
	currentDB = tokens[1]
	_ = replCatalog.EnsureDatabase(currentDB)
	fmt.Printf("switched to db %s\n", currentDB)
}

func cmdShow(tokens []string) {
	if len(tokens) < 2 {
		fmt.Println("Usage: show dbs | show collections")
		return
	}
	switch tokens[1] {
	case "dbs":
		dbs, err := replCatalog.ListDatabases()
		if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
		}
		for _, db := range dbs {
			fmt.Printf("  %s\n", db.Name)
		}
	case "collections":
		colls, err := replCatalog.ListCollections(currentDB)
		if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
		}
		for _, c := range colls {
			fmt.Printf("  %s\n", c.Name)
		}
	default:
		fmt.Printf("Unknown: show %s\n", tokens[1])
	}
}

func cmdDB(tokens []string) {
	if len(tokens) < 2 {
		fmt.Println("Usage: db.<collection>.<operation>(...)")
		return
	}
}

func cmdDatabaseOperation(tokens []string) {
	// Parse: db.collection.operation(args)
	input := strings.Join(tokens, " ")

	// Parse db.coll.method(args)
	if !strings.HasPrefix(input, "db.") {
		fmt.Printf("Unknown command: %s\n", tokens[0])
		return
	}

	// Extract collection and rest
	rest := input[3:] // remove "db."

	// Split on first '.'
	parts := strings.SplitN(rest, ".", 2)
	if len(parts) < 2 {
		fmt.Println("Usage: db.<collection>.<operation>(args)")
		return
	}
	collName := parts[0]
	operation := parts[1]

	// Parse method and arguments
	// find operation name and arguments
	opEnd := strings.Index(operation, "(")
	if opEnd < 0 {
		fmt.Printf("Unknown: db.%s.%s\n", collName, operation)
		return
	}
	method := operation[:opEnd]
	argsStr := operation[opEnd+1:]
	if len(argsStr) > 0 && argsStr[len(argsStr)-1] == ')' {
		argsStr = argsStr[:len(argsStr)-1]
	}

	_ = replCatalog.EnsureCollection(currentDB, collName)
	coll := mongo.NewCollection(currentDB, collName, replEngine, replCatalog)

	switch method {
	case "insert":
		replInsert(coll, argsStr)
	case "find":
		replFind(coll, argsStr)
	case "update":
		replUpdate(coll, argsStr)
	case "delete":
		replDelete(coll, argsStr)
	case "count":
		replCount(coll)
	case "drop":
		replDrop(coll, collName)
	default:
		fmt.Printf("Unknown method: %s\n", method)
	}
}

func replInsert(coll *mongo.Collection, argsStr string) {
	if argsStr == "" {
		fmt.Println("Usage: db.coll.insert({field: value, ...})")
		return
	}
	doc, err := parseDocument(argsStr)
	if err != nil {
		fmt.Printf("Parse error: %v\n", err)
		return
	}
	err = coll.InsertOne(doc)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	idVal, _ := doc.Get("_id")
	fmt.Printf("Inserted 1 document (_id: %v)\n", idVal)
}

func replFind(coll *mongo.Collection, argsStr string) {
	filter := bson.NewDocument()
	if argsStr != "" {
		doc, err := parseDocument(argsStr)
		if err != nil {
			fmt.Printf("Parse error: %v\n", err)
			return
		}
		filter = doc
	}

	matcher := mongo.NewMatcher(filter)
	prefix := mongo.EncodeNamespacePrefix(coll.DB(), coll.Name())
	var results []*bson.Document
	replEngine.Scan(prefix, func(key, value []byte) bool {
		doc, err := bson.Decode(value)
		if err != nil {
			return true
		}
		if matcher.Match(doc) {
			results = append(results, doc)
		}
		return true
	})

	if len(results) == 0 {
		fmt.Println("No documents found.")
		return
	}
	for i, doc := range results {
		if i > 20 {
			fmt.Printf("... and %d more\n", len(results)-20)
			break
		}
		printDoc(doc)
	}
}

func replUpdate(coll *mongo.Collection, argsStr string) {
	// Parse: {filter}, {update}
	parts := splitArgs(argsStr)
	if len(parts) < 2 {
		fmt.Println("Usage: db.coll.update({filter}, {update})")
		return
	}
	filter, err := parseDocument(parts[0])
	if err != nil {
		fmt.Printf("Filter parse error: %v\n", err)
		return
	}
	update, err := parseDocument(parts[1])
	if err != nil {
		fmt.Printf("Update parse error: %v\n", err)
		return
	}

	matcher := mongo.NewMatcher(filter)
	prefix := mongo.EncodeNamespacePrefix(coll.DB(), coll.Name())
	var modified int

	replEngine.Scan(prefix, func(key, value []byte) bool {
		doc, err := bson.Decode(value)
		if err != nil {
			return true
		}
		if matcher.Match(doc) {
			newDoc := mongo.ApplyUpdate(doc, update, false)
			if idVal, ok := doc.Get("_id"); ok {
				newDoc.Set("_id", idVal)
				if err := coll.ReplaceOne(idVal.ObjectID(), newDoc); err == nil {
					modified++
				}
			}
		}
		return true
	})
	fmt.Printf("Modified %d document(s)\n", modified)
}

func replDelete(coll *mongo.Collection, argsStr string) {
	filter := bson.NewDocument()
	if argsStr != "" {
		doc, err := parseDocument(argsStr)
		if err != nil {
			fmt.Printf("Parse error: %v\n", err)
			return
		}
		filter = doc
	}

	matcher := mongo.NewMatcher(filter)
	prefix := mongo.EncodeNamespacePrefix(coll.DB(), coll.Name())
	var keys [][]byte
	replEngine.Scan(prefix, func(key, value []byte) bool {
		doc, err := bson.Decode(value)
		if err != nil {
			return true
		}
		if matcher.Match(doc) {
			keys = append(keys, append([]byte{}, key...))
		}
		return true
	})

	var deleted int
	for _, k := range keys {
		if err := replEngine.Delete(k); err == nil {
			deleted++
		}
	}
	fmt.Printf("Deleted %d document(s)\n", deleted)
}

func replCount(coll *mongo.Collection) {
	prefix := mongo.EncodeNamespacePrefix(coll.DB(), coll.Name())
	var count int
	replEngine.Scan(prefix, func(_, _ []byte) bool {
		count++
		return true
	})
	fmt.Println(count)
}

func replDrop(coll *mongo.Collection, collName string) {
	if err := replCatalog.DropCollection(coll.DB(), collName); err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Println("ok")
}

func printDoc(doc *bson.Document) {
	fmt.Print("{")
	first := true
	for _, e := range doc.Elements() {
		if !first {
			fmt.Print(", ")
		}
		first = false
		fmt.Printf("%s: %s", e.Key, valueToString(e.Value))
	}
	fmt.Println("}")
}

func valueToString(v bson.Value) string {
	switch v.Type {
	case bson.TypeString:
		return fmt.Sprintf("%q", v.String())
	case bson.TypeInt32:
		return fmt.Sprintf("%d", v.Int32())
	case bson.TypeInt64:
		return fmt.Sprintf("%d", v.Int64())
	case bson.TypeDouble:
		return fmt.Sprintf("%f", v.Double())
	case bson.TypeBoolean:
		return fmt.Sprintf("%t", v.Boolean())
	case bson.TypeNull:
		return "null"
	case bson.TypeObjectID:
		return fmt.Sprintf("ObjectId(%q)", v.ObjectID().String())
	case bson.TypeArray:
		arr := v.ArrayValue()
		parts := make([]string, len(arr))
		for i, elem := range arr {
			parts[i] = valueToString(elem)
		}
		return fmt.Sprintf("[%s]", strings.Join(parts, ", "))
	case bson.TypeDocument:
		return "{...}"
	case bson.TypeDateTime:
		return fmt.Sprintf("Date(%d)", v.DateTime())
	default:
		return fmt.Sprintf("<%d>", v.Type)
	}
}

func tokenizeREPL(line string) []string {
	return strings.Fields(line)
}

func splitArgs(s string) []string {
	// Split on }, { boundary
	var parts []string
	depth := 0
	current := strings.Builder{}
	for _, ch := range s {
		switch ch {
		case '{':
			depth++
			current.WriteRune(ch)
		case '}':
			depth--
			current.WriteRune(ch)
			if depth == 0 {
				parts = append(parts, current.String())
				current.Reset()
			}
		case ',':
			if depth == 0 {
				continue
			}
			current.WriteRune(ch)
		default:
			current.WriteRune(ch)
		}
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	return parts
}
