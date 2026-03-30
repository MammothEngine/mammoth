package engine

// Batch represents an atomic batch of writes.
type Batch struct {
	engine  *Engine
	writes  []batchEntry
	commited bool
}

type batchEntry struct {
	key   []byte
	value []byte
	delete bool
}

// Put adds a put operation to the batch.
func (b *Batch) Put(key, value []byte) {
	if b.commited {
		return
	}
	b.writes = append(b.writes, batchEntry{key: key, value: value})
}

// Delete adds a delete operation to the batch.
func (b *Batch) Delete(key []byte) {
	if b.commited {
		return
	}
	b.writes = append(b.writes, batchEntry{key: key, delete: true})
}

// Commit applies all operations in the batch atomically.
func (b *Batch) Commit() error {
	if b.commited {
		return errBatchAlreadyCommitted
	}
	b.commited = true
	return b.engine.applyBatch(b)
}

// Len returns the number of operations in the batch.
func (b *Batch) Len() int {
	return len(b.writes)
}
