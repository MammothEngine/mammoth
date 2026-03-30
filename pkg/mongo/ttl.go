package mongo

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/mammothengine/mammoth/pkg/bson"
	"github.com/mammothengine/mammoth/pkg/engine"
)

// TTLWorker removes expired documents based on TTL indexes.
type TTLWorker struct {
	engine *engine.Engine
	cat    *Catalog
	indexCat *IndexCatalog
	done   chan struct{}
	mu     sync.Mutex
	running bool
}

// NewTTLWorker creates a new TTL background worker.
func NewTTLWorker(eng *engine.Engine, cat *Catalog, indexCat *IndexCatalog) *TTLWorker {
	return &TTLWorker{
		engine:   eng,
		cat:      cat,
		indexCat: indexCat,
		done:     make(chan struct{}),
	}
}

// Start begins the TTL background goroutine.
func (w *TTLWorker) Start() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.running {
		return
	}
	w.running = true
	go w.run()
}

// Stop terminates the TTL background goroutine.
func (w *TTLWorker) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.running {
		return
	}
	close(w.done)
	w.running = false
}

func (w *TTLWorker) run() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-w.done:
			return
		case <-ticker.C:
			w.expireDocs()
		}
	}
}

func (w *TTLWorker) expireDocs() {
	// Collect all TTL indexes first to avoid nested scan/delete deadlock
	prefix := append([]byte(catalogPrefix), catalogTypeIndex)
	var ttlIndexes []IndexSpec
	w.engine.Scan(prefix, func(_, value []byte) bool {
		var spec IndexSpec
		if err := json.Unmarshal(value, &spec); err != nil {
			return true
		}
		if spec.ExpireAfterSeconds > 0 && len(spec.Key) > 0 {
			ttlIndexes = append(ttlIndexes, spec)
		}
		return true
	})

	// Now expire docs for each TTL index (outside the scan lock)
	for _, spec := range ttlIndexes {
		w.expireForIndex(spec)
	}
}

func (w *TTLWorker) expireForIndex(spec IndexSpec) {
	if len(spec.Key) == 0 {
		return
	}
	ttlField := spec.Key[0].Field
	expireMs := int64(spec.ExpireAfterSeconds) * 1000
	now := time.Now().UnixMilli()

	// We don't know the exact db/coll from the spec alone.
	// Scan all databases and collections to find which ones have this index.
	dbs, _ := w.cat.ListDatabases()
	for _, db := range dbs {
		colls, _ := w.cat.ListCollections(db.Name)
		for _, coll := range colls {
			idx, err := w.indexCat.GetIndex(db.Name, coll.Name, spec.Name)
			if err != nil {
				continue
			}
			w.expireDocsInCollection(db.Name, coll.Name, idx, ttlField, now, expireMs)
		}
	}
}

func (w *TTLWorker) expireDocsInCollection(db, coll string, idx *Index, field string, now, expireMs int64) {
	prefix := EncodeNamespacePrefix(db, coll)
	var keysToDelete [][]byte
	var docsToDelete []*bson.Document

	// Collect expired docs first to avoid deadlock (Scan holds read lock, Delete needs write lock)
	w.engine.Scan(prefix, func(key, value []byte) bool {
		doc, err := bson.Decode(value)
		if err != nil {
			return true
		}

		v, found := ResolveField(doc, field)
		if !found || v.Type != bson.TypeDateTime {
			return true
		}

		dateMs := v.DateTime()
		if now-dateMs > expireMs {
			keysToDelete = append(keysToDelete, append([]byte{}, key...))
			docsToDelete = append(docsToDelete, doc)
		}
		return true
	})

	for i, k := range keysToDelete {
		if err := w.engine.Delete(k); err == nil {
			w.indexCat.OnDocumentDelete(db, coll, docsToDelete[i])
		}
	}
}
