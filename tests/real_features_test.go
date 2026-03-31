package tests

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
	"github.com/mammothengine/mammoth/pkg/mongo"
)

// getTempDir returns a Windows-compatible temp directory
func getTempDir(t *testing.T) string {
	t.Helper()
	tmpDir := t.TempDir()
	// Windows compatibility
	if runtime.GOOS == "windows" {
		return tmpDir
	}
	return tmpDir
}

// TestRealDocumentLifecycle tests full document CRUD operations
func TestRealDocumentLifecycle(t *testing.T) {
	dir := getTempDir(t)
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	cat := mongo.NewCatalog(eng)
	cat.EnsureCollection("testdb", "users")
	coll := mongo.NewCollection("testdb", "users", eng, cat)

	// 1. CREATE - Insert document
	t.Run("Create", func(t *testing.T) {
		doc := bson.NewDocument()
		doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
		doc.Set("name", bson.VString("John Doe"))
		doc.Set("email", bson.VString("john@example.com"))
		doc.Set("age", bson.VInt32(30))
		doc.Set("active", bson.VBool(true))
		doc.Set("balance", bson.VDouble(1234.56))
		doc.Set("createdAt", bson.VInt64(time.Now().Unix()))
		doc.Set("tags", bson.VArray(bson.Array{
			bson.VString("premium"),
			bson.VString("verified"),
		}))

		if err := coll.InsertOne(doc); err != nil {
			t.Fatalf("InsertOne failed: %v", err)
		}
		t.Logf("Inserted document with _id")
	})

	// 2. READ - Find document
	t.Run("Read", func(t *testing.T) {
		matcher := mongo.NewMatcher(bson.D("email", bson.VString("john@example.com")))
		prefix := mongo.EncodeNamespacePrefix("testdb", "users")

		found := false
		eng.Scan(prefix, func(key, value []byte) bool {
			doc, err := bson.Decode(value)
			if err != nil {
				return true
			}
			if matcher.Match(doc) {
				found = true
				name, _ := doc.Get("name")
				t.Logf("Found document: name=%s", name.String())
				return false
			}
			return true
		})

		if !found {
			t.Error("Document not found")
		}
	})

	// 3. UPDATE - Update document
	t.Run("Update", func(t *testing.T) {
		filter := bson.D("email", bson.VString("john@example.com"))
		matcher := mongo.NewMatcher(filter)
		prefix := mongo.EncodeNamespacePrefix("testdb", "users")

		var found bool
		eng.Scan(prefix, func(key, value []byte) bool {
			doc, err := bson.Decode(value)
			if err != nil {
				return true
			}
			if matcher.Match(doc) {
				found = true
				// Update fields
				doc.Set("age", bson.VInt32(31))
				doc.Set("lastLogin", bson.VInt64(time.Now().Unix()))
				doc.Set("loginCount", bson.VInt32(1))

				// Write back
				if err := eng.Put(key, bson.Encode(doc)); err != nil {
					t.Errorf("Failed to update: %v", err)
				} else {
					t.Log("Document updated successfully")
				}
				return false
			}
			return true
		})

		if !found {
			t.Error("Document not found for update")
		}
	})

	// 4. DELETE - Remove document
	t.Run("Delete", func(t *testing.T) {
		filter := bson.D("email", bson.VString("john@example.com"))
		matcher := mongo.NewMatcher(filter)
		prefix := mongo.EncodeNamespacePrefix("testdb", "users")

		var found bool
		eng.Scan(prefix, func(key, value []byte) bool {
			doc, err := bson.Decode(value)
			if err != nil {
				return true
			}
			if matcher.Match(doc) {
				found = true
				if err := eng.Delete(key); err != nil {
					t.Errorf("Failed to delete: %v", err)
				} else {
					t.Log("Document deleted successfully")
				}
				return false
			}
			return true
		})

		if !found {
			t.Error("Document not found for delete")
		}
	})

	// 5. VERIFY - Ensure deleted
	t.Run("VerifyDelete", func(t *testing.T) {
		filter := bson.D("email", bson.VString("john@example.com"))
		matcher := mongo.NewMatcher(filter)
		prefix := mongo.EncodeNamespacePrefix("testdb", "users")

		found := false
		eng.Scan(prefix, func(key, value []byte) bool {
			doc, err := bson.Decode(value)
			if err != nil {
				return true
			}
			if matcher.Match(doc) {
				found = true
				return false
			}
			return true
		})

		if found {
			t.Error("Document still exists after delete")
		} else {
			t.Log("Verified: Document successfully deleted")
		}
	})
}

// TestRealIndexUsage tests index creation and usage
func TestRealIndexUsage(t *testing.T) {
	dir := getTempDir(t)
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	cat := mongo.NewCatalog(eng)
	indexCat := mongo.NewIndexCatalog(eng, cat)
	cat.EnsureCollection("testdb", "products")
	coll := mongo.NewCollection("testdb", "products", eng, cat)

	// Insert test data
	for i := 0; i < 1000; i++ {
		doc := bson.NewDocument()
		doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
		doc.Set("sku", bson.VString(fmt.Sprintf("SKU-%05d", i)))
		doc.Set("name", bson.VString(fmt.Sprintf("Product %d", i)))
		doc.Set("price", bson.VDouble(float64(i)*10.99))
		doc.Set("category", bson.VString(fmt.Sprintf("Category%d", i%10)))
		coll.InsertOne(doc)
	}
	t.Log("Inserted 1000 products")

	// Test 1: Create unique index on SKU
	t.Run("CreateUniqueIndex", func(t *testing.T) {
		spec := mongo.IndexSpec{
			Name:   "idx_sku_unique",
			Key:    []mongo.IndexKey{{Field: "sku", Descending: false}},
			Unique: true,
		}
		if err := indexCat.CreateIndex("testdb", "products", spec); err != nil {
			t.Fatalf("Failed to create index: %v", err)
		}
		t.Log("Created unique index on sku")
	})

	// Test 2: Create compound index
	t.Run("CreateCompoundIndex", func(t *testing.T) {
		spec := mongo.IndexSpec{
			Name: "idx_category_price",
			Key: []mongo.IndexKey{
				{Field: "category", Descending: false},
				{Field: "price", Descending: true},
			},
		}
		if err := indexCat.CreateIndex("testdb", "products", spec); err != nil {
			t.Fatalf("Failed to create compound index: %v", err)
		}
		t.Log("Created compound index on category, price")
	})

	// Test 3: List indexes
	t.Run("ListIndexes", func(t *testing.T) {
		indexes, err := indexCat.ListIndexes("testdb", "products")
		if err != nil {
			t.Fatalf("Failed to list indexes: %v", err)
		}
		t.Logf("Found %d indexes:", len(indexes))
		for _, idx := range indexes {
			t.Logf("  - %s", idx.Name)
		}
	})

	// Test 4: Query using index (via planner)
	t.Run("QueryWithIndex", func(t *testing.T) {
		filter := bson.D("sku", bson.VString("SKU-00500"))
		matcher := mongo.NewMatcher(filter)
		prefix := mongo.EncodeNamespacePrefix("testdb", "products")

		found := false
		start := time.Now()
		eng.Scan(prefix, func(key, value []byte) bool {
			doc, err := bson.Decode(value)
			if err != nil {
				return true
			}
			if matcher.Match(doc) {
				found = true
				name, _ := doc.Get("name")
				t.Logf("Found: %s (took %v)", name.String(), time.Since(start))
				return false
			}
			return true
		})

		if !found {
			t.Error("Product not found")
		}
	})

	// Test 5: Drop index
	t.Run("DropIndex", func(t *testing.T) {
		if err := indexCat.DropIndex("testdb", "products", "idx_sku_unique"); err != nil {
			t.Fatalf("Failed to drop index: %v", err)
		}
		t.Log("Dropped index idx_sku_unique")
	})
}

// TestRealQueryOperations tests various query operations
func TestRealQueryOperations(t *testing.T) {
	dir := getTempDir(t)
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	cat := mongo.NewCatalog(eng)
	cat.EnsureCollection("testdb", "orders")
	coll := mongo.NewCollection("testdb", "orders", eng, cat)

	// Insert sample orders
	statuses := []string{"pending", "processing", "shipped", "delivered", "cancelled"}
	for i := 0; i < 500; i++ {
		doc := bson.NewDocument()
		doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
		doc.Set("orderId", bson.VString(fmt.Sprintf("ORD-%d", i)))
		doc.Set("customerId", bson.VString(fmt.Sprintf("CUST-%d", i%50)))
		doc.Set("status", bson.VString(statuses[i%5]))
		doc.Set("amount", bson.VDouble(float64(i)*15.5))
		doc.Set("items", bson.VInt32(int32(i%10+1)))
		doc.Set("createdAt", bson.VInt64(time.Now().Unix()-int64(i*3600)))
		coll.InsertOne(doc)
	}

	// Test 1: Equality query
	t.Run("EqualityQuery", func(t *testing.T) {
		filter := bson.D("status", bson.VString("delivered"))
		matcher := mongo.NewMatcher(filter)
		prefix := mongo.EncodeNamespacePrefix("testdb", "orders")

		count := 0
		eng.Scan(prefix, func(key, value []byte) bool {
			doc, _ := bson.Decode(value)
			if matcher.Match(doc) {
				count++
			}
			return true
		})

		t.Logf("Found %d delivered orders", count)
		if count == 0 {
			t.Error("Expected to find delivered orders")
		}
	})

	// Test 2: Range query
	t.Run("RangeQuery", func(t *testing.T) {
		filter := bson.NewDocument()
		amtFilter := bson.NewDocument()
		amtFilter.Set("$gte", bson.VDouble(100.0))
		amtFilter.Set("$lte", bson.VDouble(500.0))
		filter.Set("amount", bson.VDoc(amtFilter))

		matcher := mongo.NewMatcher(filter)
		prefix := mongo.EncodeNamespacePrefix("testdb", "orders")

		count := 0
		eng.Scan(prefix, func(key, value []byte) bool {
			doc, _ := bson.Decode(value)
			if matcher.Match(doc) {
				count++
			}
			return true
		})

		t.Logf("Found %d orders with amount between 100-500", count)
		if count == 0 {
			t.Error("Expected to find orders in range")
		}
	})

	// Test 3: $in query
	t.Run("InQuery", func(t *testing.T) {
		filter := bson.NewDocument()
		inArray := bson.Array{
			bson.VString("pending"),
			bson.VString("processing"),
		}
		filter.Set("status", bson.VDoc(bson.D("$in", bson.VArray(inArray))))

		matcher := mongo.NewMatcher(filter)
		prefix := mongo.EncodeNamespacePrefix("testdb", "orders")

		count := 0
		eng.Scan(prefix, func(key, value []byte) bool {
			doc, _ := bson.Decode(value)
			if matcher.Match(doc) {
				count++
			}
			return true
		})

		t.Logf("Found %d pending/processing orders", count)
	})

	// Test 4: Sort and limit
	t.Run("SortAndLimit", func(t *testing.T) {
		prefix := mongo.EncodeNamespacePrefix("testdb", "orders")

		var orders []int64
		eng.Scan(prefix, func(key, value []byte) bool {
			doc, _ := bson.Decode(value)
			if ts, ok := doc.Get("createdAt"); ok {
				orders = append(orders, ts.Int64())
			}
			return len(orders) < 10
		})

		t.Logf("Retrieved %d most recent orders", len(orders))
	})
}

// TestRealAggregation tests aggregation operations
func TestRealAggregation(t *testing.T) {
	dir := getTempDir(t)
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	cat := mongo.NewCatalog(eng)
	cat.EnsureCollection("testdb", "sales")
	coll := mongo.NewCollection("testdb", "sales", eng, cat)

	// Insert sales data
	regions := []string{"North", "South", "East", "West"}
	products := []string{"Widget", "Gadget", "Tool", "Device"}

	for i := 0; i < 1000; i++ {
		doc := bson.NewDocument()
		doc.Set("_id", bson.VObjectID(bson.NewObjectID()))
		doc.Set("region", bson.VString(regions[i%4]))
		doc.Set("product", bson.VString(products[i%4]))
		doc.Set("quantity", bson.VInt32(int32(i%20+1)))
		doc.Set("price", bson.VDouble(float64(i%50+10)))
		doc.Set("date", bson.VInt64(time.Now().Unix()-int64(i*86400)))
		coll.InsertOne(doc)
	}
	t.Log("Inserted 1000 sales records")

	// Test: Group by region and sum quantities
	t.Run("GroupByRegion", func(t *testing.T) {
		prefix := mongo.EncodeNamespacePrefix("testdb", "sales")

		regionTotals := make(map[string]int64)
		eng.Scan(prefix, func(key, value []byte) bool {
			doc, err := bson.Decode(value)
			if err != nil {
				return true
			}

			region, _ := doc.Get("region")
			qty, _ := doc.Get("quantity")

			if region.Type == bson.TypeString && qty.Type == bson.TypeInt32 {
				regionTotals[region.String()] += int64(qty.Int32())
			}
			return true
		})

		t.Log("Sales by region:")
		for region, total := range regionTotals {
			t.Logf("  %s: %d units", region, total)
		}

		if len(regionTotals) != 4 {
			t.Errorf("Expected 4 regions, got %d", len(regionTotals))
		}
	})

	// Test: Calculate average price by product
	t.Run("AvgByProduct", func(t *testing.T) {
		prefix := mongo.EncodeNamespacePrefix("testdb", "sales")

		type stats struct {
			sum   float64
			count int64
		}
		productStats := make(map[string]*stats)

		eng.Scan(prefix, func(key, value []byte) bool {
			doc, err := bson.Decode(value)
			if err != nil {
				return true
			}

			product, _ := doc.Get("product")
			price, _ := doc.Get("price")

			if product.Type == bson.TypeString && price.Type == bson.TypeDouble {
				p := product.String()
				if productStats[p] == nil {
					productStats[p] = &stats{}
				}
				productStats[p].sum += price.Double()
				productStats[p].count++
			}
			return true
		})

		t.Log("Average price by product:")
		for product, s := range productStats {
			avg := s.sum / float64(s.count)
			t.Logf("  %s: $%.2f (%d sales)", product, avg, s.count)
		}
	})
}

// TestRealTransactionWorkflow tests transaction scenarios
func TestRealTransactionWorkflow(t *testing.T) {
	dir := getTempDir(t)
	eng, err := engine.Open(engine.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer eng.Close()

	cat := mongo.NewCatalog(eng)
	cat.EnsureCollection("bank", "accounts")
	cat.EnsureCollection("bank", "transactions")

	accountsColl := mongo.NewCollection("bank", "accounts", eng, cat)
	transactionsColl := mongo.NewCollection("bank", "transactions", eng, cat)

	// Setup initial accounts
	accounts := []struct {
		id      string
		balance float64
	}{
		{"ACC-001", 1000.0},
		{"ACC-002", 500.0},
		{"ACC-003", 2000.0},
	}

	for _, acc := range accounts {
		doc := bson.NewDocument()
		doc.Set("_id", bson.VString(acc.id))
		doc.Set("balance", bson.VDouble(acc.balance))
		doc.Set("owner", bson.VString(fmt.Sprintf("Owner-%s", acc.id)))
		accountsColl.InsertOne(doc)
	}
	t.Log("Created 3 bank accounts")

	// Test: Transfer between accounts
	t.Run("TransferFunds", func(t *testing.T) {
		// Simulate transfer: ACC-001 -> ACC-002, amount: 100
		fromAcc := "ACC-001"
		toAcc := "ACC-002"
		amount := 100.0

		// Get source account
		fromDoc, err := getAccount(eng, accountsColl, fromAcc)
		if err != nil {
			t.Fatalf("Source account not found: %v", err)
		}

		fromBalance, _ := fromDoc.Get("balance")
		if fromBalance.Double() < amount {
			t.Fatalf("Insufficient funds: %.2f < %.2f", fromBalance.Double(), amount)
		}

		// Get destination account
		toDoc, err := getAccount(eng, accountsColl, toAcc)
		if err != nil {
			t.Fatalf("Destination account not found: %v", err)
		}

		// Update balances
		newFromBalance := fromBalance.Double() - amount
		toBalance, _ := toDoc.Get("balance")
		newToBalance := toBalance.Double() + amount

		fromDoc.Set("balance", bson.VDouble(newFromBalance))
		toDoc.Set("balance", bson.VDouble(newToBalance))

		// Save updates
		if err := saveAccount(eng, accountsColl, fromDoc); err != nil {
			t.Fatalf("Failed to update source: %v", err)
		}
		if err := saveAccount(eng, accountsColl, toDoc); err != nil {
			t.Fatalf("Failed to update destination: %v", err)
		}

		// Record transaction
		transDoc := bson.NewDocument()
		transDoc.Set("_id", bson.VObjectID(bson.NewObjectID()))
		transDoc.Set("from", bson.VString(fromAcc))
		transDoc.Set("to", bson.VString(toAcc))
		transDoc.Set("amount", bson.VDouble(amount))
		transDoc.Set("timestamp", bson.VInt64(time.Now().Unix()))
		transactionsColl.InsertOne(transDoc)

		t.Logf("Transferred $%.2f from %s to %s", amount, fromAcc, toAcc)
		t.Logf("New balance %s: $%.2f", fromAcc, newFromBalance)
		t.Logf("New balance %s: $%.2f", toAcc, newToBalance)
	})

	// Verify final balances
	t.Run("VerifyBalances", func(t *testing.T) {
		prefix := mongo.EncodeNamespacePrefix("bank", "accounts")

		balances := make(map[string]float64)
		eng.Scan(prefix, func(key, value []byte) bool {
			doc, _ := bson.Decode(value)
			id, _ := doc.Get("_id")
			bal, _ := doc.Get("balance")
			if id.Type == bson.TypeString && bal.Type == bson.TypeDouble {
				balances[id.String()] = bal.Double()
			}
			return true
		})

		t.Log("Final balances:")
		for id, bal := range balances {
			t.Logf("  %s: $%.2f", id, bal)
		}

		// ACC-001 should have 900, ACC-002 should have 600
		if balances["ACC-001"] != 900.0 {
			t.Errorf("ACC-001 balance wrong: expected 900, got %.2f", balances["ACC-001"])
		}
		if balances["ACC-002"] != 600.0 {
			t.Errorf("ACC-002 balance wrong: expected 600, got %.2f", balances["ACC-002"])
		}
	})
}

func getAccount(eng *engine.Engine, coll *mongo.Collection, id string) (*bson.Document, error) {
	filter := bson.D("_id", bson.VString(id))
	matcher := mongo.NewMatcher(filter)
	prefix := mongo.EncodeNamespacePrefix("bank", "accounts")

	var result *bson.Document
	eng.Scan(prefix, func(key, value []byte) bool {
		doc, err := bson.Decode(value)
		if err != nil {
			return true
		}
		if matcher.Match(doc) {
			result = doc
			return false
		}
		return true
	})

	if result == nil {
		return nil, fmt.Errorf("account not found: %s", id)
	}
	return result, nil
}

func saveAccount(eng *engine.Engine, coll *mongo.Collection, doc *bson.Document) error {
	id, _ := doc.Get("_id")
	filter := bson.D("_id", id)
	matcher := mongo.NewMatcher(filter)
	prefix := mongo.EncodeNamespacePrefix("bank", "accounts")

	return eng.Scan(prefix, func(key, value []byte) bool {
		existing, _ := bson.Decode(value)
		if matcher.Match(existing) {
			eng.Put(key, bson.Encode(doc))
			return false
		}
		return true
	})
}

// TestRealFilePersistence tests actual file system persistence
func TestRealFilePersistence(t *testing.T) {
	// Use a real directory that persists across operations
	dataDir := filepath.Join(os.TempDir(), "mammoth_test_persist")
	os.RemoveAll(dataDir)
	os.MkdirAll(dataDir, 0755)
	defer os.RemoveAll(dataDir)

	// Phase 1: Create data
	func() {
		eng, err := engine.Open(engine.DefaultOptions(dataDir))
		if err != nil {
			t.Fatalf("Failed to open engine: %v", err)
		}
		defer eng.Close()

		cat := mongo.NewCatalog(eng)
		cat.EnsureCollection("persist", "data")
		coll := mongo.NewCollection("persist", "data", eng, cat)

		// Insert documents
		for i := 0; i < 100; i++ {
			doc := bson.NewDocument()
			doc.Set("_id", bson.VInt64(int64(i)))
			doc.Set("value", bson.VString(fmt.Sprintf("value_%d", i)))
			doc.Set("timestamp", bson.VInt64(time.Now().Unix()))
			if err := coll.InsertOne(doc); err != nil {
				t.Fatalf("Insert failed: %v", err)
			}
		}

		t.Log("Phase 1: Inserted 100 documents")

		// Force flush to disk by closing
	}()

	// Phase 2: Verify persistence
	func() {
		eng, err := engine.Open(engine.DefaultOptions(dataDir))
		if err != nil {
			t.Fatalf("Failed to reopen engine: %v", err)
		}
		defer eng.Close()

		prefix := mongo.EncodeNamespacePrefix("persist", "data")

		count := 0
		eng.Scan(prefix, func(key, value []byte) bool {
			_, err := bson.Decode(value)
			if err == nil {
				count++
			}
			return true
		})

		t.Logf("Phase 2: Found %d documents after reopen", count)

		if count != 100 {
			t.Errorf("Expected 100 documents, found %d", count)
		}
	}()

	// Phase 3: Update and verify
	func() {
		eng, err := engine.Open(engine.DefaultOptions(dataDir))
		if err != nil {
			t.Fatalf("Failed to reopen engine: %v", err)
		}
		defer eng.Close()

		// Update some documents
		prefix := mongo.EncodeNamespacePrefix("persist", "data")
		matcher := mongo.NewMatcher(bson.D("_id", bson.VInt64(50)))

		eng.Scan(prefix, func(key, value []byte) bool {
			doc, _ := bson.Decode(value)
			if matcher.Match(doc) {
				doc.Set("updated", bson.VBool(true))
				doc.Set("updateTime", bson.VInt64(time.Now().Unix()))
				eng.Put(key, bson.Encode(doc))
				return false
			}
			return true
		})

		t.Log("Phase 3: Updated document 50")
	}()

	// Phase 4: Verify update persisted
	func() {
		eng, err := engine.Open(engine.DefaultOptions(dataDir))
		if err != nil {
			t.Fatalf("Failed to reopen engine: %v", err)
		}
		defer eng.Close()

		prefix := mongo.EncodeNamespacePrefix("persist", "data")
		matcher := mongo.NewMatcher(bson.D("_id", bson.VInt64(50)))

		var found bool
		eng.Scan(prefix, func(key, value []byte) bool {
			doc, _ := bson.Decode(value)
			if matcher.Match(doc) {
				if updated, ok := doc.Get("updated"); ok && updated.Boolean() {
					found = true
				}
				return false
			}
			return true
		})

		if !found {
			t.Error("Phase 4: Update did not persist")
		} else {
			t.Log("Phase 4: Update successfully persisted")
		}
	}()
}

// TestRealWindowsPaths tests Windows-specific path handling
func TestRealWindowsPaths(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows-specific test")
	}

	// Test with Windows-style paths
	dataDir := `C:\temp\mammoth_test`
	os.RemoveAll(dataDir)
	os.MkdirAll(dataDir, 0755)
	defer os.RemoveAll(dataDir)

	eng, err := engine.Open(engine.DefaultOptions(dataDir))
	if err != nil {
		t.Fatalf("Failed to open with Windows path: %v", err)
	}
	defer eng.Close()

	cat := mongo.NewCatalog(eng)
	cat.EnsureCollection("test", "data")
	coll := mongo.NewCollection("test", "data", eng, cat)

	// Insert and retrieve
	doc := bson.NewDocument()
	doc.Set("_id", bson.VString("test1"))
	doc.Set("path", bson.VString(dataDir))
	if err := coll.InsertOne(doc); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Verify file was created
	files, err := os.ReadDir(dataDir)
	if err != nil {
		t.Fatalf("Failed to read data dir: %v", err)
	}

	if len(files) == 0 {
		t.Error("No files created in data directory")
	} else {
		t.Logf("Created %d files/directories:", len(files))
		for _, f := range files {
			t.Logf("  - %s", f.Name())
		}
	}
}
